
import json
import logging
import argparse
import os
import zipfile
from pathlib import Path
from autosinapi.config import Config
from autosinapi.core.downloader import Downloader
from autosinapi.core.processor import Processor
from autosinapi.core.database import Database
from autosinapi.exceptions import AutoSinapiError

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Pipeline:
    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)
        self.db_config = self._get_db_config()
        self.sinapi_config = self._get_sinapi_config()

    def _load_config(self, config_path: str):
        """Carrega a configuração de um arquivo JSON ou de variáveis de ambiente."""
        if config_path:
            logging.info(f"Carregando configuração do arquivo: {config_path}")
            try:
                with open(config_path, 'r') as f:
                    return json.load(f)
            except FileNotFoundError:
                logging.error(f"Arquivo de configuração não encontrado: {config_path}")
                raise
            except json.JSONDecodeError:
                logging.error(f"Erro ao decodificar o arquivo JSON: {config_path}")
                raise
        else:
            logging.info("Carregando configuração das variáveis de ambiente.")
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
            logging.error(f"Erro ao ler ou processar o arquivo de secrets: {e}")
            raise

    def _get_sinapi_config(self):
        """Extrai as configurações do SINAPI."""
        return {
            'state': self.config.get('default_state', 'BR'),
            'year': self.config['default_year'],
            'month': self.config['default_month'],
            'type': self.config.get('workbook_type_name', 'REFERENCIA'),
            'duplicate_policy': self.config.get('duplicate_policy', 'substituir')
        }

    def _find_and_normalize_zip(self, download_path: Path, standardized_name: str) -> Path:
        """Encontra, renomeia e retorna o caminho de um arquivo .zip em um diretório."""
        for file in download_path.glob('*.zip'):
            logging.info(f"Arquivo .zip encontrado: {file.name}")
            if file.name.upper() != standardized_name.upper():
                new_path = download_path / standardized_name
                logging.info(f"Renomeando arquivo para o padrão: {standardized_name}")
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
        logging.info(f"Arquivo descompactado em: {extraction_path}")
        return extraction_path

    def run(self):
        """Executa o pipeline de ETL do SINAPI."""
        logging.info(f"Iniciando pipeline AutoSINAPI...")
        try:
            config = Config(db_config=self.db_config, sinapi_config=self.sinapi_config, mode='local')
            logging.info("Configuração validada com sucesso.")

            downloader = Downloader(config.sinapi_config, config.mode)
            ano = config.sinapi_config['year']
            mes = config.sinapi_config['month']
            tipo = config.sinapi_config['type'].upper()
            download_path = Path(f"./downloads/{ano}_{mes}")
            download_path.mkdir(parents=True, exist_ok=True)

            standardized_name = f"SINAPI_{tipo}_{mes}_{ano}.zip"
            local_zip_path = self._find_and_normalize_zip(download_path, standardized_name)

            if not local_zip_path:
                logging.info("Arquivo não encontrado localmente. Iniciando download...")
                file_content = downloader.get_sinapi_data(save_path=download_path)
                with open(download_path / standardized_name, 'wb') as f:
                    f.write(file_content.getbuffer())
                local_zip_path = download_path / standardized_name
                logging.info(f"Download concluído e salvo em: {local_zip_path}")

            extraction_path = self._unzip_file(local_zip_path)
            
            all_excel_files = list(extraction_path.glob('*.xlsx'))
            if not all_excel_files:
                raise FileNotFoundError(f"Nenhum arquivo .xlsx encontrado em {extraction_path}")

            processor = Processor(config.sinapi_config)
            db = Database(config.db_config)
            db.create_tables() # Cria as tabelas uma única vez

            processed_any_file = False

            # Processar Manutenções primeiro (se existir)
            manutencoes_file_path = None
            for f in all_excel_files:
                if "Manutenções" in f.name:
                    manutencoes_file_path = f
                    break

            if manutencoes_file_path:
                logging.info(f"Processando arquivo de Manutenções: {manutencoes_file_path.name}")
                processor.process_manutencoes(str(manutencoes_file_path), db._engine)
                processed_any_file = True

            # Processar Referência (preços, custos, insumos, composições e estrutura)
            referencia_file_path = None
            for f in all_excel_files:
                if "Referência" in f.name:
                    referencia_file_path = f
                    break

            if referencia_file_path:
                logging.info(f"Processando arquivo de Referência: {referencia_file_path.name}")
                
                # Processar preços e custos mensais (SINAPI_mao_de_obra e SINAPI_Referência sheets)
                processor.process_precos_e_custos(str(referencia_file_path), db._engine)
                logging.info("Preços e custos mensais processados.")

                # Processar dados de catálogo de Insumos (assumindo sheet "Insumos")
                try:
                    insumos_df = processor.process(str(referencia_file_path), sheet_name="Insumos")
                    if not insumos_df.empty:
                        # A política de duplicatas para catálogos deve ser UPSERT. 
                        # Assumindo que 'substituir' aqui significa um UPSERT ou que a tabela é limpa antes.
                        # O DataModel.md sugere UPSERT para catálogos.
                        db.save_data(insumos_df, "insumos", "substituir", ano, mes) 
                        logging.info(f"Dados de insumos (catálogo) salvos: {len(insumos_df)} registros.")
                except Exception as e:
                    logging.warning(f"Não foi possível processar a aba 'Insumos' do arquivo de Referência: {e}")

                # Processar dados de catálogo de Composições (assumindo sheet "Composicoes")
                try:
                    composicoes_df = processor.process(str(referencia_file_path), sheet_name="Composicoes")
                    if not composicoes_df.empty:
                        db.save_data(composicoes_df, "composicoes", "substituir", ano, mes) 
                        logging.info(f"Dados de composições (catálogo) salvos: {len(composicoes_df)} registros.")
                except Exception as e:
                    logging.warning(f"Não foi possível processar a aba 'Composicoes' do arquivo de Referência: {e}")

                # Processar composicao_itens (estrutura analítica) - assumindo sheet "Analítico"
                try:
                    # O método process_composicao_itens já lida com a leitura e salvamento no DB
                    processor.process_composicao_itens(str(referencia_file_path), db._engine)
                    logging.info(f"Dados de composicao_itens (estrutura) salvos.")
                except Exception as e:
                    logging.warning(f"Não foi possível processar a aba 'Analítico' do arquivo de Referência para composicao_itens: {e}")

                processed_any_file = True

            if not processed_any_file:
                logging.warning("Nenhum arquivo .xlsx reconhecido foi processado. Verifique os nomes dos arquivos e as abas.")

            logging.info("Processamento de arquivos concluído.")

            logging.info("Pipeline AutoSINAPI concluído com sucesso!")

        except AutoSinapiError as e:
            logging.error(f"Erro no pipeline AutoSINAPI: {e}")
        except Exception as e:
            logging.error(f"Ocorreu um erro inesperado: {e}")

def main():
    parser = argparse.ArgumentParser(description="Pipeline de ETL para dados do SINAPI.")
    parser.add_argument(
        '--config',
        type=str,
        default=None,
        help='Caminho para o arquivo de configuração JSON. Se não fornecido, usa variáveis de ambiente.'
    )
    args = parser.parse_args()

    pipeline = Pipeline(config_path=args.config)
    pipeline.run()

if __name__ == "__main__":
    main()
