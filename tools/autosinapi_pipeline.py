"""
autosinapi_pipeline.py: Script Principal para Execução do Pipeline ETL do AutoSINAPI.

Este script atua como o orquestrador central para o processo de Extração,
Transformação e Carga (ETL) dos dados do SINAPI. Ele é responsável por:

1.  **Configuração:** Carregar as configurações de execução (ano, mês, tipo de
    caderno, etc.) a partir de um arquivo JSON ou variáveis de ambiente.
2.  **Download:** Utilizar o módulo `autosinapi.core.downloader` para obter
    os arquivos brutos do SINAPI.
3.  **Processamento:** Empregar o módulo `autosinapi.core.processor` para
    transformar e limpar os dados brutos em um formato estruturado.
4.  **Carga:** Usar o módulo `autosinapi.core.database` para carregar os dados
    processados no banco de dados PostgreSQL.
5.  **Logging:** Configurar e gerenciar o sistema de logging para registrar
    o progresso e quaisquer erros durante a execução do pipeline.

Este script suporta diferentes modos de operação (local e servidor) e é a
interface principal para a execução do AutoSINAPI como uma ferramenta CLI.
"""
import json
import logging
import argparse
import os
import zipfile
from pathlib import Path
import pandas as pd
from autosinapi.config import Config
from autosinapi.core.downloader import Downloader
from autosinapi.core.processor import Processor
from autosinapi.core.database import Database
from autosinapi.exceptions import AutoSinapiError

# Configuração do logger principal
logger = logging.getLogger("autosinapi")

def setup_logging(debug_mode=False):
    """Configura o sistema de logging de forma centralizada."""
    level = logging.DEBUG if debug_mode else logging.INFO
    log_file_path = Path("./logs/etl_pipeline.log")
    log_file_path.parent.mkdir(parents=True, exist_ok=True)

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    stream_formatter_info = logging.Formatter('[%(levelname)s] %(message)s')
    stream_formatter_debug = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')

    file_handler = logging.FileHandler(log_file_path, mode='w')
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(level)

    stream_handler = logging.StreamHandler()
    if debug_mode:
        stream_handler.setFormatter(stream_formatter_debug)
    else:
        stream_handler.setFormatter(stream_formatter_info)
    stream_handler.setLevel(level)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.setLevel(level)

    if not debug_mode:
        logging.getLogger("urllib3").setLevel(logging.WARNING)

class Pipeline:
    def __init__(self, config_path: str = None):
        self.logger = logging.getLogger("autosinapi.pipeline")
        self.config = self._load_config(config_path)
        self.db_config = self._get_db_config()
        self.sinapi_config = self._get_sinapi_config()

    def _load_config(self, config_path: str):
        self.logger.debug(f"Tentando carregar configuração. Caminho fornecido: {config_path}")
        if config_path:
            self.logger.info(f"Carregando configuração do arquivo: {config_path}")
            try:
                with open(config_path, 'r') as f:
                    return json.load(f)
            except FileNotFoundError:
                self.logger.error(f"Arquivo de configuração não encontrado: {config_path}", exc_info=True)
                raise
            except json.JSONDecodeError:
                self.logger.error(f"Erro ao decodificar o arquivo JSON de configuração: {config_path}", exc_info=True)
                raise
        else:
            self.logger.info("Carregando configuração a partir de variáveis de ambiente.")
            return {
                "secrets_path": os.getenv("AUTOSINAPI_SECRETS_PATH", "tools/sql_access.secrets"),
                "default_year": os.getenv("AUTOSINAPI_YEAR"),
                "default_month": os.getenv("AUTOSINAPI_MONTH"),
                "workbook_type_name": os.getenv("AUTOSINAPI_TYPE", "REFERENCIA"),
                "duplicate_policy": os.getenv("AUTOSINAPI_POLICY", "substituir"),
            }

    def _get_db_config(self):
        self.logger.debug("Extraindo configurações do banco de dados.")
        if os.getenv("DOCKER_ENV"):
            self.logger.info("Modo Docker detectado. Lendo configuração do DB a partir de variáveis de ambiente.")
            required_vars = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
            missing_vars = [v for v in required_vars if not os.getenv(v)]
            if missing_vars:
                raise AutoSinapiError(
                    f"Variáveis de ambiente para o banco de dados não encontradas: {missing_vars}. "
                    f"Verifique se o arquivo 'tools/docker/.env' existe e está preenchido corretamente."
                )
            return {
                'host': os.getenv("POSTGRES_HOST", "db"),
                'port': os.getenv("POSTGRES_PORT", 5432),
                'database': os.getenv("POSTGRES_DB"),
                'user': os.getenv("POSTGRES_USER"),
                'password': os.getenv("POSTGRES_PASSWORD"),
            }
        try:
            secrets_path = self.config['secrets_path']
            with open(secrets_path, 'r') as f:
                content = f.read()
            
            db_config = {}
            for line in content.splitlines():
                if '=' in line:
                    key, value = line.split('=', 1)
                    db_config[key.strip()] = value.strip().strip("'")
            
            return {
                'host': db_config['DB_HOST'],
                'port': db_config['DB_PORT'],
                'database': db_config['DB_NAME'],
                'user': db_config['DB_USER'],
                'password': db_config['DB_PASSWORD'],
            }
        except Exception as e:
            self.logger.error(f"Erro CRÍTICO ao ler ou processar o arquivo de secrets '{secrets_path}'. Detalhes: {e}", exc_info=True)
            raise

    def _get_sinapi_config(self):
        return {
            'state': self.config.get('default_state', 'BR'),
            'year': self.config['default_year'],
            'month': self.config['default_month'],
            'type': self.config.get('workbook_type_name', 'REFERENCIA'),
            'file_format': self.config.get('default_format', 'XLSX'),
            'duplicate_policy': self.config.get('duplicate_policy', 'substituir'),
            'mode': os.getenv('AUTOSINAPI_MODE', 'local') # Add this line
        }

    def _find_and_normalize_zip(self, download_path: Path, standardized_name: str) -> Path:
        self.logger.info(f"Procurando por arquivo .zip em: {download_path}")
        for file in download_path.glob('*.zip'):
            self.logger.info(f"Arquivo .zip encontrado: {file.name}")
            if file.name.upper() != standardized_name.upper():
                new_path = download_path / standardized_name
                self.logger.info(f"Renomeando '{file.name}' para o padrão: '{standardized_name}'")
                file.rename(new_path)
                return new_path
            return file
        self.logger.warning("Nenhum arquivo .zip encontrado localmente.")
        return None

    def _unzip_file(self, zip_path: Path) -> Path:
        extraction_path = zip_path.parent / zip_path.stem
        self.logger.info(f"Descompactando '{zip_path.name}' para: {extraction_path}")
        extraction_path.mkdir(parents=True, exist_ok=True)
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extraction_path)
            self.logger.info("Arquivo descompactado com sucesso.")
            return extraction_path
        except zipfile.BadZipFile:
            self.logger.error(f"O arquivo '{zip_path.name}' não é um zip válido ou está corrompido.", exc_info=True)
            raise

    def _run_pre_processing(self):
        self.logger.info("FASE PRE: Iniciando pré-processamento de planilhas para CSV.")
        script_path = "tools/pre_processador.py"
        try:
            if not os.path.exists(script_path):
                raise FileNotFoundError(f"Script de pré-processamento não encontrado em '{script_path}'")
            
            result = os.system(f"python {script_path}")
            if result != 0:
                raise AutoSinapiError(f"O script de pré-processamento '{script_path}' falhou com código de saída {result}.")
            self.logger.info("Pré-processamento de planilhas concluído com sucesso.")
        except Exception as e:
            self.logger.error(f"Erro ao executar o script de pré-processamento: {e}", exc_info=True)
            raise

    def _sync_catalog_status(self, db: Database):
        self.logger.info("Iniciando Fase 2: Sincronização de Status dos Catálogos.")
        sql_update = """
        WITH latest_maintenance AS (
            SELECT
                item_codigo,
                tipo_item,
                tipo_manutencao,
                ROW_NUMBER() OVER(PARTITION BY item_codigo, tipo_item ORDER BY data_referencia DESC) as rn
            FROM manutencoes_historico
        )
        UPDATE {table}
        SET status = 'DESATIVADO'
        WHERE codigo IN (
            SELECT item_codigo FROM latest_maintenance
            WHERE rn = 1 AND tipo_item = '{item_type}' AND tipo_manutencao ILIKE '%DESATIVAÇÃO%'
        );
        """
        try:
            num_insumos_updated = db.execute_non_query(sql_update.format(table="insumos", item_type="INSUMO"))
            self.logger.info(f"Status do catálogo de insumos sincronizado. Itens desativados: {num_insumos_updated}")
            num_composicoes_updated = db.execute_non_query(sql_update.format(table="composicoes", item_type="COMPOSICAO"))
            self.logger.info(f"Status do catálogo de composições sincronizado. Itens desativados: {num_composicoes_updated}")
        except Exception as e:
            self.logger.error(f"Erro ao sincronizar status dos catálogos: {e}", exc_info=True)
            raise AutoSinapiError(f"Erro em '_sync_catalog_status': {e}")

    def run(self):
        self.logger.info("======================================================")
        self.logger.info("=========   INICIANDO PIPELINE AUTOSINAPI   =========")
        self.logger.info("======================================================")
        try:
            config = Config(db_config=self.db_config, sinapi_config=self.sinapi_config, mode=self.sinapi_config['mode'])
            self.logger.info("Configuração validada com sucesso.")
            self.logger.debug(f"Configurações SINAPI para esta execução: {config.sinapi_config}")

            downloader = Downloader(config.sinapi_config, config.mode)
            processor = Processor(config.sinapi_config)
            db = Database(config.db_config)

            self.logger.info("Recriando tabelas do banco de dados para garantir conformidade.")
            db.create_tables()

            year = config.sinapi_config['year']
            month = config.sinapi_config['month']
            data_referencia = f"{year}-{month}-01"
            
            download_path = Path(f"./downloads/{year}_{month}")
            download_path.mkdir(parents=True, exist_ok=True)
            standardized_name = f"SINAPI-{year}-{month}-formato-xlsx.zip"
            local_zip_path = self._find_and_normalize_zip(download_path, standardized_name)
            
            if not local_zip_path:
                self.logger.info("Arquivo não encontrado localmente. Iniciando download...")
                file_content = downloader.get_sinapi_data(save_path=download_path)
                local_zip_path = download_path / standardized_name
                with open(local_zip_path, 'wb') as f:
                    f.write(file_content.getbuffer())
                self.logger.info(f"Download concluído e salvo em: {local_zip_path}")
            
            extraction_path = self._unzip_file(local_zip_path)
            
            # --- PRÉ-PROCESSAMENTO PARA CSV ---
            self._run_pre_processing()
            # --- FIM DO PRÉ-PROCESSAMENTO ---

            all_excel_files = list(extraction_path.glob('*.xlsx'))
            if not all_excel_files:
                raise FileNotFoundError(f"Nenhum arquivo .xlsx encontrado em {extraction_path}")

            manutencoes_file_path = next((f for f in all_excel_files if "Manuten" in f.name), None)
            referencia_file_path = next((f for f in all_excel_files if "Referência" in f.name), None)

            if manutencoes_file_path:
                self.logger.info(f"FASE 1: Processamento de Manutenções ({manutencoes_file_path.name})")
                manutencoes_df = processor.process_manutencoes(str(manutencoes_file_path))
                db.save_data(manutencoes_df, 'manutencoes_historico', policy='append')
                self.logger.info("Histórico de manutenções carregado com sucesso.")
                self._sync_catalog_status(db) # FASE 2
            else:
                self.logger.warning("Arquivo de Manutenções não encontrado. Pulando Fases 1 e 2.")

            if not referencia_file_path:
                self.logger.warning("Arquivo de Referência não encontrado. Finalizando pipeline.")
                return

            self.logger.info(f"FASE 3: Processamento do Arquivo de Referência ({referencia_file_path.name})")
            self.logger.info("Processando catálogos, dados mensais e estrutura de composições...")
            processed_data = processor.process_catalogo_e_precos(str(referencia_file_path))
            structure_dfs = processor.process_composicao_itens(str(referencia_file_path))

            if 'insumos' in processed_data:
                existing_insumos_df = processed_data['insumos']
            else:
                existing_insumos_df = pd.DataFrame(columns=['codigo', 'descricao', 'unidade'])

            all_child_insumo_codes = structure_dfs['composicao_insumos']['insumo_filho_codigo'].unique()
            existing_insumo_codes_set = set(existing_insumos_df['codigo'].values)
            missing_insumo_codes = [code for code in all_child_insumo_codes if code not in existing_insumo_codes_set]

            if missing_insumo_codes:
                self.logger.warning(f"Encontrados {len(missing_insumo_codes)} insumos na estrutura que não estão no catálogo. Criando placeholders com detalhes...")
                insumo_details_df = structure_dfs['child_item_details'][
                    (structure_dfs['child_item_details']['codigo'].isin(missing_insumo_codes)) &
                    (structure_dfs['child_item_details']['tipo'] == 'INSUMO')
                ].drop_duplicates(subset=['codigo']).set_index('codigo')

                missing_insumos_data = {
                    'codigo': missing_insumo_codes,
                    'descricao': [insumo_details_df.loc[code, 'descricao'] if code in insumo_details_df.index else f"INSUMO_DESCONHECIDO_{code}" for code in missing_insumo_codes],
                    'unidade': [insumo_details_df.loc[code, 'unidade'] if code in insumo_details_df.index else "UN" for code in missing_insumo_codes]
                }
                missing_insumos_df = pd.DataFrame(missing_insumos_data)
                processed_data['insumos'] = pd.concat([existing_insumos_df, missing_insumos_df], ignore_index=True)

            if 'composicoes' in processed_data:
                existing_composicoes_df = processed_data['composicoes']
            else:
                existing_composicoes_df = pd.DataFrame(columns=['codigo', 'descricao', 'unidade'])

            parent_codes = structure_dfs['parent_composicoes_details'].set_index('codigo')
            child_codes = structure_dfs['child_item_details'][
                structure_dfs['child_item_details']['tipo'] == 'COMPOSICAO'
            ].drop_duplicates(subset=['codigo']).set_index('codigo')
            
            all_composicao_codes_in_structure = set(parent_codes.index) | set(child_codes.index)
            existing_composicao_codes_set = set(existing_composicoes_df['codigo'].values)
            missing_composicao_codes = list(all_composicao_codes_in_structure - existing_composicao_codes_set)

            if missing_composicao_codes:
                self.logger.warning(f"Encontradas {len(missing_composicao_codes)} composições (pai/filha) na estrutura que não estão no catálogo. Criando placeholders com detalhes...")
                
                def get_detail(code, column):
                    if code in parent_codes.index: return parent_codes.loc[code, column]
                    if code in child_codes.index: return child_codes.loc[code, column]
                    return f"COMPOSICAO_DESCONHECIDA_{code}" if column == 'descricao' else 'UN'

                missing_composicoes_df = pd.DataFrame({
                    'codigo': missing_composicao_codes,
                    'descricao': [get_detail(code, 'descricao') for code in missing_composicao_codes],
                    'unidade': [get_detail(code, 'unidade') for code in missing_composicao_codes]
                })
                processed_data['composicoes'] = pd.concat([existing_composicoes_df, missing_composicoes_df], ignore_index=True)

            self.logger.info("Iniciando carga de dados no banco de dados na ordem correta...")

            if 'insumos' in processed_data and not processed_data['insumos'].empty:
                db.save_data(processed_data['insumos'], 'insumos', policy='upsert', pk_columns=['codigo'])
                self.logger.info("Catálogo de insumos (incluindo placeholders) carregado.")
            if 'composicoes' in processed_data and not processed_data['composicoes'].empty:
                db.save_data(processed_data['composicoes'], 'composicoes', policy='upsert', pk_columns=['codigo'])
                self.logger.info("Catálogo de composições (incluindo placeholders) carregado.")
            
            db.truncate_table('composicao_insumos')
            db.truncate_table('composicao_subcomposicoes')
            db.save_data(structure_dfs['composicao_insumos'], 'composicao_insumos', policy='append')
            db.save_data(structure_dfs['composicao_subcomposicoes'], 'composicao_subcomposicoes', policy='append')
            self.logger.info("Estrutura de composições carregada com sucesso.")

            precos_carregados = False
            if 'precos_insumos_mensal' in processed_data and not processed_data['precos_insumos_mensal'].empty:
                processed_data['precos_insumos_mensal']['data_referencia'] = pd.to_datetime(data_referencia)
                db.save_data(processed_data['precos_insumos_mensal'], 'precos_insumos_mensal', policy='append')
                precos_carregados = True
            else:
                self.logger.warning("Nenhum dado de PREÇOS DE INSUMOS foi encontrado ou processado. Pulando esta etapa.")

            custos_carregados = False
            if 'custos_composicoes_mensal' in processed_data and not processed_data['custos_composicoes_mensal'].empty:
                processed_data['custos_composicoes_mensal']['data_referencia'] = pd.to_datetime(data_referencia)
                db.save_data(processed_data['custos_composicoes_mensal'], 'custos_composicoes_mensal', policy='append')
                custos_carregados = True
            else:
                self.logger.warning("Nenhum dado de CUSTOS DE COMPOSIÇÕES foi encontrado ou processado. Pulando esta etapa.")

            if precos_carregados or custos_carregados:
                self.logger.info("Dados mensais (preços/custos) carregados com sucesso.")
            else:
                self.logger.warning("Nenhuma informação de preços ou custos foi carregada nesta execução.")
            
            self.logger.info("Pipeline AutoSINAPI concluído com sucesso!")

        except AutoSinapiError as e:
            self.logger.error(f"Erro de negócio no pipeline AutoSINAPI: {e}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Ocorreu um erro inesperado e fatal no pipeline: {e}", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description="Pipeline de ETL para dados do SINAPI.")
    parser.add_argument('--config', type=str, help='Caminho para o arquivo de configuração JSON.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Habilita logging em nível DEBUG.')
    args = parser.parse_args()

    setup_logging(debug_mode=True)

    try:
        pipeline = Pipeline(config_path=args.config)
        pipeline.run()
    except Exception:
        logger.critical("Pipeline encerrado devido a um erro fatal.")

if __name__ == "__main__":
    main()
