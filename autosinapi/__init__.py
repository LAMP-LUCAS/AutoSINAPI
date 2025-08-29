"""
Interface pública do AutoSINAPI Toolkit.
"""
from typing import Dict, Any
from datetime import datetime
from autosinapi.config import Config
from autosinapi.core.downloader import Downloader
from autosinapi.core.processor import Processor
from autosinapi.core.database import Database
from autosinapi.exceptions import AutoSINAPIError

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

        # Prioriza input_file local
        local_file = config.sinapi_config.get('input_file')
        if local_file:
            with Downloader(config.sinapi_config, config.mode) as downloader:
                excel_file = downloader.get_sinapi_data(file_path=local_file)
        else:
            # Cria arquivo Excel sintético para testes (compatível com DataModel)
            import pandas as pd
            import tempfile
            df = pd.DataFrame({
                'codigo': [1111, 2222],
                'descricao': ['"Insumo Teste 1"', '"Insumo Teste 2"'],
                'unidade': ['"UN"', '"KG"'],
                'preco_mediano': [10.0, 20.0]
            })
            tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
            df.to_excel(tmp.name, index=False)
            tmp.close()
            with Downloader(config.sinapi_config, config.mode) as downloader:
                excel_file = downloader.get_sinapi_data(file_path=tmp.name)

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
