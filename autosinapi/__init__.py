"""
Interface pública do AutoSINAPI Toolkit.
"""
from typing import Dict, Any
from datetime import datetime
from .config import Config
from .core.downloader import Downloader
from .core.processor import Processor
from .core.database import Database
from .exceptions import AutoSINAPIError

def run_etl(db_config: Dict[str, Any], sinapi_config: Dict[str, Any], mode: str = 'server') -> Dict[str, Any]:
    """
    Executa o pipeline ETL do SINAPI.
    
    Args:
        db_config: Configurações do banco de dados
        sinapi_config: Configurações do SINAPI
        mode: Modo de operação ('server' ou 'local')
    
    Returns:
        Dict com status da operação:
        {
            'status': 'success' ou 'error',
            'message': Mensagem descritiva,
            'details': {
                'rows_processed': número de linhas,
                'tables_updated': lista de tabelas,
                'timestamp': data/hora da execução
            }
        }
    
    Raises:
        AutoSINAPIError: Se houver qualquer erro no processo
    """
    try:
        # Valida configurações
        config = Config(db_config, sinapi_config, mode)
        
        # Executa pipeline
        with Downloader(config.sinapi_config, config.mode) as downloader:
            # Tenta usar arquivo local primeiro, se fornecido na configuração
            local_file = config.sinapi_config.get('input_file')
            excel_file = downloader.get_sinapi_data(file_path=local_file)
        
        processor = Processor(config.sinapi_config)
        data = processor.process(excel_file)
        
        with Database(config.db_config) as db:
            table_name = f"sinapi_{config.sinapi_config['state'].lower()}"
            db.save_data(data, table_name)
        
        return {
            'status': 'success',
            'message': 'Pipeline ETL executado com sucesso',
            'details': {
                'rows_processed': len(data),
                'tables_updated': [table_name],
                'timestamp': datetime.now().isoformat()
            }
        }
        
    except AutoSINAPIError as e:
        return {
            'status': 'error',
            'message': str(e),
            'details': {}
        }
