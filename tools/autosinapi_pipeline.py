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
            db.execute_non_query(sql_update.format(table="insumos", item_type="INSUMO"))
            self.logger.info("Status do catálogo de insumos sincronizado.")
            db.execute_non_query(sql_update.format(table="composicoes", item_type="COMPOSICAO"))
            self.logger.info("Status do catálogo de composições sincronizado.")
        except Exception as e:
            raise AutoSinapiError(f"Erro ao sincronizar status dos catálogos: {e}")

    def run(self):
        """Executa o pipeline de ETL do SINAPI seguindo o DataModel."""
        self.logger.info("Iniciando pipeline AutoSINAPI...")
        try:
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

            if manutencoes_file_path:
                self.logger.info(f"Iniciando Fase 1: Processamento de Manutenções ({manutencoes_file_path.name})")
                manutencoes_df = processor.process_manutencoes(str(manutencoes_file_path))
                db.save_data(manutencoes_df, 'manutencoes_historico', policy='append')
                self.logger.info("Histórico de manutenções carregado com sucesso.")
            else:
                self.logger.warning("Arquivo de Manutenções não encontrado. Pulando Fases 1 e 2.")

            if manutencoes_file_path:
                self._sync_catalog_status(db)

            if referencia_file_path:
                self.logger.info(f"Iniciando Fase 3: Processamento do Arquivo de Referência ({referencia_file_path.name})")
                
                self.logger.info("Processando estrutura de composições (Analítico)...")
                structure_dfs = processor.process_composicao_itens(str(referencia_file_path))
                db.truncate_table('composicao_insumos')
                db.truncate_table('composicao_subcomposicoes')
                db.save_data(structure_dfs['composicao_insumos'], 'composicao_insumos', policy='append')
                db.save_data(structure_dfs['composicao_subcomposicoes'], 'composicao_subcomposicoes', policy='append')
                self.logger.info("Estrutura de composições carregada com sucesso.")

                self.logger.info("Processando catálogos e dados mensais (preços/custos)...")
                processed_data = processor.process_catalogo_e_precos(str(referencia_file_path))

                if 'precos_insumos_mensal' in processed_data:
                    processed_data['precos_insumos_mensal']['data_referencia'] = pd.to_datetime(data_referencia)
                if 'custos_composicoes_mensal' in processed_data:
                    processed_data['custos_composicoes_mensal']['data_referencia'] = pd.to_datetime(data_referencia)

                for table_name, df in processed_data.items():
                    if table_name in ['insumos', 'composicoes']:
                        db.save_data(df, table_name, policy='upsert', pk_columns=['codigo'])
                    else:
                        db.save_data(df, table_name, policy='append')
                self.logger.info("Catálogos e dados mensais carregados com sucesso.")

            else:
                self.logger.warning(f"Arquivo de Referência ({referencia_file_path.name}) não encontrado. Pulando Fase 3.")

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

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    pipeline = Pipeline(config_path=args.config)
    pipeline.run()

if __name__ == "__main__":
    main()