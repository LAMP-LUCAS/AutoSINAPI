import json
import logging
import argparse
from pathlib import Path
from autosinapi.config import Config
from autosinapi.core.downloader import Downloader
from autosinapi.core.processor import Processor
from autosinapi.core.database import Database
from autosinapi.exceptions import AutoSinapiError

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Pipeline:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self.db_config = self._get_db_config()
        self.sinapi_config = self._get_sinapi_config()

    def _load_config(self):
        """Carrega o arquivo de configuração JSON."""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logging.error(f"Arquivo de configuração não encontrado: {self.config_path}")
            raise
        except json.JSONDecodeError:
            logging.error(f"Erro ao decodificar o arquivo JSON: {self.config_path}")
            raise

    def _get_db_config(self):
        """Extrai as configurações do banco de dados do arquivo de secrets."""
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
        """Extrai as configurações do SINAPI do config.json."""
        return {
            'state': self.config.get('default_state', 'BR'),
            'year': self.config['default_year'],
            'month': self.config['default_month'],
            'type': self.config.get('workbook_type_name', 'REFERENCIA'),
            'duplicate_policy': self.config.get('duplicate_policy', 'substituir')
        }

    def run(self):
        """Executa o pipeline de ETL do SINAPI."""
        logging.info(f"Iniciando pipeline AutoSINAPI com a configuração: {self.config_path}")
        try:
            config = Config(db_config=self.db_config, sinapi_config=self.sinapi_config, mode='local')
            logging.info("Configuração validada com sucesso.")

            downloader = Downloader(config.sinapi_config, config.mode)
            download_path = Path(f"./downloads/{config.sinapi_config['year']}_{config.sinapi_config['month']}")
            download_path.mkdir(parents=True, exist_ok=True)

            ano = config.sinapi_config['year']
            mes = config.sinapi_config['month']
            tipo = config.sinapi_config['type']
            local_zip_path = download_path / f"SINAPI_{tipo}_{mes}_{ano}.zip"

            if local_zip_path.exists():
                logging.info(f"Arquivo encontrado localmente: {local_zip_path}")
                file_content = downloader.get_sinapi_data(file_path=local_zip_path)
            else:
                logging.info("Arquivo não encontrado localmente. Iniciando download...")
                file_content = downloader.get_sinapi_data(save_path=download_path)
                logging.info(f"Download concluído com sucesso.")

            processor = Processor(config.sinapi_config)
            temp_file_path = download_path / "temp_sinapi_file.zip"
            with open(temp_file_path, 'wb') as f:
                f.write(file_content.getbuffer())

            processed_data = processor.process(str(temp_file_path))
            logging.info(f"Processamento concluído. {len(processed_data)} registros processados.")

            db = Database(config.db_config)
            db.create_tables()
            table_name = f"sinapi_{config.sinapi_config['year']}_{config.sinapi_config['month']}"
            
            policy = self.sinapi_config['duplicate_policy']
            db.save_data(processed_data, table_name, policy, ano, mes)
            logging.info(f"Dados salvos com sucesso na tabela '{table_name}' com a política '{policy}'.")

            temp_file_path.unlink()

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
        default='tools/CONFIG.example.json',
        help='Caminho para o arquivo de configuração JSON.'
    )
    args = parser.parse_args()

    pipeline = Pipeline(config_path=args.config)
    pipeline.run()

if __name__ == "__main__":
    main()