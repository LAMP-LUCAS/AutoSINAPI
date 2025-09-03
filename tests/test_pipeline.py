"""
Testes de integração para o pipeline principal do AutoSINAPI.
"""
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from pathlib import Path
from tools.autosinapi_pipeline import Pipeline
from autosinapi.exceptions import DownloadError, ProcessingError, DatabaseError

@pytest.fixture
def db_config():
    """Fixture com configurações do banco de dados."""
    return {
        'host': 'localhost', 'port': 5432, 'database': 'test_db',
        'user': 'test_user', 'password': 'test_pass'
    }

@pytest.fixture
def sinapi_config():
    """Fixture com configurações do SINAPI."""
    return {
        'state': 'SP', 'year': 2025, 'month': 8, 'type': 'REFERENCIA',
        'duplicate_policy': 'substituir'
    }

@pytest.fixture
def mock_pipeline(mocker, db_config, sinapi_config, tmp_path):
    """Fixture para mockar o pipeline e suas dependências."""
    mocker.patch('tools.autosinapi_pipeline.setup_logging')
    
    # Cria um diretório de extração falso
    extraction_path = tmp_path / "extraction"
    extraction_path.mkdir()
    # Cria um arquivo de referência falso dentro do diretório
    (extraction_path / "SINAPI_Referência_2025_08.xlsx").touch()

    with patch('tools.autosinapi_pipeline.Database') as mock_db, \
         patch('tools.autosinapi_pipeline.Downloader') as mock_downloader, \
         patch('tools.autosinapi_pipeline.Processor') as mock_processor:
        
        mock_db_instance = MagicMock()
        mock_db.return_value = mock_db_instance
        
        mock_downloader_instance = MagicMock()
        mock_downloader.return_value = mock_downloader_instance
        
        mock_processor_instance = MagicMock()
        mock_processor.return_value = mock_processor_instance

        pipeline = Pipeline(config_path=None)

        mocker.patch.object(pipeline, '_get_db_config', return_value=db_config)
        mocker.patch.object(pipeline, '_get_sinapi_config', return_value=sinapi_config)
        mocker.patch.object(pipeline, '_load_config', return_value={
            "secrets_path": "dummy",
            "default_year": sinapi_config['year'],
            "default_month": sinapi_config['month']
        })

        mocker.patch.object(pipeline, '_find_and_normalize_zip', return_value=MagicMock())
        mocker.patch.object(pipeline, '_unzip_file', return_value=extraction_path)
        mocker.patch.object(pipeline, '_run_pre_processing')
        mocker.patch.object(pipeline, '_sync_catalog_status')

        yield pipeline, mock_db_instance, mock_downloader_instance, mock_processor_instance

def test_run_etl_success(mock_pipeline):
    """Testa o fluxo completo do ETL com sucesso."""
    pipeline, mock_db, _, mock_processor = mock_pipeline
    
    mock_processor.process_catalogo_e_precos.return_value = {
        'insumos': pd.DataFrame({'codigo': ['1'], 'descricao': ['a'], 'unidade': ['un']}),
        'composicoes': pd.DataFrame({'codigo': ['c1'], 'descricao': ['ca'], 'unidade': ['un']})
    }
    mock_processor.process_composicao_itens.return_value = {
        'composicao_insumos': pd.DataFrame({'insumo_filho_codigo': ['1']}),
        'composicao_subcomposicoes': pd.DataFrame(),
        'parent_composicoes_details': pd.DataFrame({'codigo': ['c1'], 'descricao': ['ca'], 'unidade': ['un']}),
        'child_item_details': pd.DataFrame({'codigo': ['1'], 'tipo': ['INSUMO'], 'descricao': ['a'], 'unidade': ['un']})
    }

    pipeline.run()

    mock_db.create_tables.assert_called_once()
    mock_processor.process_catalogo_e_precos.assert_called()
    assert mock_db.save_data.call_count > 0

def test_run_etl_download_error(mock_pipeline, caplog):
    """Testa falha no download."""
    pipeline, _, mock_downloader, _ = mock_pipeline
    
    pipeline._find_and_normalize_zip.return_value = None
    mock_downloader.get_sinapi_data.side_effect = DownloadError("Network error")

    pipeline.run()

    assert "Erro de negócio no pipeline AutoSINAPI: Network error" in caplog.text

def test_run_etl_processing_error(mock_pipeline, caplog):
    """Testa falha no processamento."""
    pipeline, _, _, mock_processor = mock_pipeline
    
    mock_processor.process_catalogo_e_precos.side_effect = ProcessingError("Invalid format")

    pipeline.run()

    assert "Erro de negócio no pipeline AutoSINAPI: Invalid format" in caplog.text

def test_run_etl_database_error(mock_pipeline, caplog):
    """Testa falha no banco de dados."""
    pipeline, mock_db, _, _ = mock_pipeline
    
    mock_db.create_tables.side_effect = DatabaseError("Connection failed")

    pipeline.run()

    assert "Erro de negócio no pipeline AutoSINAPI: Connection failed" in caplog.text
