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

# Configuração básica de logging
log_file_path = Path("./logs/etl_debug.log")
log_file_path.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

class Pipeline:
    def __init__(self, config_path: str = None):
        self.logger = logging.getLogger("autosinapi.pipeline")
        self.config = self._load_config(config_path)
        self.db_config = self._get_db_config()
        self.sinapi_config = self._get_sinapi_config()

    def _load_config(self, config_path: str):
        """Carrega a configuração de um arquivo JSON ou de variáveis de ambiente."""
        if config_path:
            self.logger.info(f"Carregando configuração do arquivo: {config_path}")
            try:
                with open(config_path, 'r') as f:
                    return json.load(f)
            except FileNotFoundError:
                self.logger.error(f"Arquivo de configuração não encontrado: {config_path}")
                raise
            except json.JSONDecodeError:
                self.logger.error(f"Erro ao decodificar o arquivo JSON: {config_path}")
                raise
        else:
            self.logger.info("Carregando configuração das variáveis de ambiente.")
            return {
                "secrets_path": os.getenv("AUTOSINAPI_SECRETS_PATH", "tools/sql_access.secrets"),
                "default_year": os.getenv("AUTOSINAPI_YEAR"),
                "default_month": os.getenv("AUTOSINAPI_MONTH"),
                "workbook_type_name": os.getenv("AUTOSINAPI_TYPE", "REFERENCIA"),
                "duplicate_policy": os.getenv("AUTOSINAPI_POLICY", "substituir"),
            }

    def _get_db_config(self):
        """Extrai as configurações do banco de dados."""
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
            self.logger.error(f"Erro ao ler ou processar o arquivo de secrets: {e}")
            raise

    def _get_sinapi_config(self):
        """Extrai as configurações do SINAPI."""
        return {
            'state': self.config.get('default_state', 'BR'),
            'year': self.config['default_year'],
            'month': self.config['default_month'],
            'type': self.config.get('workbook_type_name', 'REFERENCIA'),
            'file_format': self.config.get('default_format', 'XLSX'),
            'duplicate_policy': self.config.get('duplicate_policy', 'substituir')
        }

    def _find_and_normalize_zip(self, download_path: Path, standardized_name: str) -> Path:
        """Encontra, renomeia e retorna o caminho de um arquivo .zip em um diretório."""
        for file in download_path.glob('*.zip'):
            self.logger.info(f"Arquivo .zip encontrado: {file.name}")
            if file.name.upper() != standardized_name.upper():
                new_path = download_path / standardized_name
                self.logger.info(f"Renomeando arquivo para o padrão: {standardized_name}")
                file.rename(new_path)
                return new_path
            return file
        return None

    def _unzip_file(self, zip_path: Path) -> Path:
        """Descompacta um arquivo .zip e retorna o caminho da pasta de extração."""
        extraction_path = zip_path.parent / zip_path.stem
        extraction_path.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extraction_path)
        self.logger.info(f"Arquivo descompactado em: {extraction_path}")
        return extraction_path

    def _sync_catalog_status(self, db: Database):
        """Usa o histórico de manutenções para atualizar o status dos itens nos catálogos."""
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
            # Captura o número de linhas afetadas e o adiciona ao log
            num_insumos_updated = db.execute_non_query(sql_update.format(table="insumos", item_type="INSUMO"))
            self.logger.info(f"Status do catálogo de insumos sincronizado. Itens desativados: {num_insumos_updated}")
            
            # Faz o mesmo para as composições
            num_composicoes_updated = db.execute_non_query(sql_update.format(table="composicoes", item_type="COMPOSICAO"))
            self.logger.info(f"Status do catálogo de composições sincronizado. Itens desativados: {num_composicoes_updated}")

        except Exception as e:
            raise AutoSinapiError(f"Erro ao sincronizar status dos catálogos: {e}")

    def run(self):
        """Executa o pipeline de ETL do SINAPI seguindo o DataModel."""
        self.logger.info("Iniciando pipeline AutoSINAPI...")
        try:
            # ... (toda a parte inicial de configuração, download e Fases 1 e 2 permanece a mesma) ...
            config = Config(db_config=self.db_config, sinapi_config=self.sinapi_config, mode='local')
            self.logger.info("Configuração validada com sucesso.")

            downloader = Downloader(config.sinapi_config, config.mode)
            processor = Processor(config.sinapi_config)
            db = Database(config.db_config)

            self.logger.info("Recriando tabelas do banco de dados para garantir conformidade com o modelo...")
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
                with open(download_path / standardized_name, 'wb') as f:
                    f.write(file_content.getbuffer())
                local_zip_path = download_path / standardized_name
                self.logger.info(f"Download concluído e salvo em: {local_zip_path}")
            
            extraction_path = self._unzip_file(local_zip_path)
            all_excel_files = list(extraction_path.glob('*.xlsx'))
            if not all_excel_files:
                raise FileNotFoundError(f"Nenhum arquivo .xlsx encontrado em {extraction_path}")

            manutencoes_file_path = next((f for f in all_excel_files if "Manuten" in f.name), None)
            referencia_file_path = next((f for f in all_excel_files if "Referência" in f.name), None)

            # FASE 1: Processamento de Manutenções
            if manutencoes_file_path:
                self.logger.info(f"Iniciando Fase 1: Processamento de Manutenções ({manutencoes_file_path.name})")
                manutencoes_df = processor.process_manutencoes(str(manutencoes_file_path))
                db.save_data(manutencoes_df, 'manutencoes_historico', policy='append')
                self.logger.info("Histórico de manutenções carregado com sucesso.")
            else:
                self.logger.warning("Arquivo de Manutenções não encontrado. Pulando Fases 1 e 2.")

            # FASE 2: Sincronização de Status
            if manutencoes_file_path:
                self._sync_catalog_status(db)

            # FASE 3: Processamento do Arquivo de Referência
            if not referencia_file_path:
                self.logger.warning("Arquivo de Referência não encontrado. Pulando Fase 3.")
                return

            self.logger.info(f"Iniciando Fase 3: Processamento do Arquivo de Referência ({referencia_file_path.name})")
            
            # 1. Processar TODOS os dados de referência em memória PRIMEIRO
            self.logger.info("Processando catálogos, dados mensais e estrutura de composições...")
            processed_data = processor.process_catalogo_e_precos(str(referencia_file_path))
            structure_dfs = processor.process_composicao_itens(str(referencia_file_path))

            # 2. Garantir a existência de TODOS os itens da estrutura nos catálogos
            
            # 2.1. Lidar com INSUMOS ausentes usando os detalhes da estrutura
            if 'insumos' in processed_data:
                existing_insumos_df = processed_data['insumos']
            else:
                existing_insumos_df = pd.DataFrame(columns=['codigo', 'descricao', 'unidade'])

            all_child_insumo_codes = structure_dfs['composicao_insumos']['insumo_filho_codigo'].unique()
            existing_insumo_codes_set = set(existing_insumos_df['codigo'].values)
            missing_insumo_codes = [code for code in all_child_insumo_codes if code not in existing_insumo_codes_set]

            if missing_insumo_codes:
                self.logger.warning(f"Encontrados {len(missing_insumo_codes)} insumos na estrutura que não estão no catálogo. Criando placeholders com detalhes...")
                
                # Pega os detalhes dos insumos ausentes do novo DataFrame 'child_item_details'
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

            # 2.2. Lidar com COMPOSIÇÕES (pais e filhas) ausentes
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

            # 3. Salvar no banco NA ORDEM CORRETA...
            self.logger.info("Iniciando carga de dados no banco de dados na ordem correta...")

            # 3.1. Carregar Catálogos (UPSERT) - Agora completos com todos os placeholders
            if 'insumos' in processed_data and not processed_data['insumos'].empty:
                db.save_data(processed_data['insumos'], 'insumos', policy='upsert', pk_columns=['codigo'])
                self.logger.info("Catálogo de insumos (incluindo placeholders) carregado.")
            if 'composicoes' in processed_data and not processed_data['composicoes'].empty:
                db.save_data(processed_data['composicoes'], 'composicoes', policy='upsert', pk_columns=['codigo'])
                self.logger.info("Catálogo de composições (incluindo placeholders) carregado.")
            
            # 3.2. Recarregar Estrutura (TRUNCATE/INSERT) - Agora é seguro
            db.truncate_table('composicao_insumos')
            db.truncate_table('composicao_subcomposicoes')
            db.save_data(structure_dfs['composicao_insumos'], 'composicao_insumos', policy='append')
            db.save_data(structure_dfs['composicao_subcomposicoes'], 'composicao_subcomposicoes', policy='append')
            self.logger.info("Estrutura de composições carregada com sucesso.")

            # 3.3. Carregar Dados Mensais (APPEND) com Logs Detalhados
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
            self.logger.error(f"Erro no pipeline AutoSINAPI: {e}")
        except Exception as e:
            self.logger.error(f"Ocorreu um erro inesperado: {e}", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description="Pipeline de ETL para dados do SINAPI.")
    parser.add_argument(
        '--config',
        type=str,
        default=None,
        help='Caminho para o arquivo de configuração JSON. Se não fornecido, usa variáveis de ambiente.'
    )
    parser.add_argument('-v', '--verbose', action='store_true', help='Habilita logging em nível DEBUG')
    args = parser.parse_args()

    # --- ALTERAÇÃO AQUI ---
    # Força o nível de logging para DEBUG para esta execução de diagnóstico
    logging.getLogger().setLevel(logging.DEBUG)
    logging.info("--- MODO DE DEPURAÇÃO DE COLUNAS ATIVADO ---")

    # A flag --verbose ainda funciona se você quiser usá-la no futuro
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    pipeline = Pipeline(config_path=args.config)
    pipeline.run()

if __name__ == "__main__":
    main()
