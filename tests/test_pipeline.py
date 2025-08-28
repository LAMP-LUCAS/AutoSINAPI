"""
Testes de integração para o pipeline principal do AutoSINAPI.
"""
import pytest
from unittest.mock import Mock, patch
import pandas as pd
from autosinapi import run_etl
from autosinapi.core.downloader import Downloader
from autosinapi.core.processor import Processor
from autosinapi.core.database import Database
from autosinapi.exceptions import (
    AutoSINAPIError,
    DownloadError,
    ProcessingError,
    DatabaseError
)

@pytest.fixture
def db_config():
    """Fixture com configurações do banco de dados."""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass'
    }

@pytest.fixture
def sinapi_config():
    """Fixture com configurações do SINAPI."""
    return {
        'year': 2025,
        'month': 8,
        'type': 'REFERENCIA',
        'duplicate_policy': 'substituir'
    }

@pytest.fixture
def mock_data():
    """Fixture com dados de exemplo."""
    return pd.DataFrame({
        'CODIGO': ['1234', '5678'],
        'DESCRICAO': ['Item A', 'Item B'],
        'PRECO': [100.0, 200.0]
    })

def test_run_etl_success(db_config, sinapi_config, mock_data):
    """Testa o fluxo completo do ETL com sucesso."""
    # Mock das classes principais
    with patch('autosinapi.Downloader') as mock_downloader, \
         patch('autosinapi.Processor') as mock_processor, \
         patch('autosinapi.Database') as mock_db:
        
        # Configura os mocks
        mock_downloader_instance = Mock()
        mock_downloader_instance.download.return_value = b'fake_excel_data'
        mock_downloader.return_value = mock_downloader_instance
        
        mock_processor_instance = Mock()
        mock_processor_instance.process.return_value = mock_data
        mock_processor.return_value = mock_processor_instance
        
        mock_db_instance = Mock()
        mock_db_instance.save_data.return_value = None
        mock_db.return_value = mock_db_instance
        
        # Executa o pipeline
        result = run_etl(db_config, sinapi_config, mode='server')
        
        # Verifica o resultado
        assert result['status'] == 'success'
        assert isinstance(result['message'], str)
        assert 'tables_updated' in result['details']
        
        # Verifica se os métodos foram chamados corretamente
        mock_downloader_instance.download.assert_called_once()
        mock_processor_instance.process.assert_called_once()
        mock_db_instance.save_data.assert_called_once()

def test_run_etl_download_error(db_config, sinapi_config):
    """Testa falha no download."""
    with patch('autosinapi.Downloader') as mock_downloader:
        mock_downloader_instance = Mock()
        mock_downloader_instance.download.side_effect = DownloadError("Erro no download")
        mock_downloader.return_value = mock_downloader_instance
        
        result = run_etl(db_config, sinapi_config, mode='server')
        
        assert result['status'] == 'error'
        assert 'download' in result['message'].lower()

def test_run_etl_processing_error(db_config, sinapi_config):
    """Testa falha no processamento."""
    with patch('autosinapi.Downloader') as mock_downloader, \
         patch('autosinapi.Processor') as mock_processor:
        
        mock_downloader_instance = Mock()
        mock_downloader_instance.download.return_value = b'fake_excel_data'
        mock_downloader.return_value = mock_downloader_instance
        
        mock_processor_instance = Mock()
        mock_processor_instance.process.side_effect = ProcessingError("Erro no processamento")
        mock_processor.return_value = mock_processor_instance
        
        result = run_etl(db_config, sinapi_config, mode='server')
        
        assert result['status'] == 'error'
        assert 'processamento' in result['message'].lower()

def test_run_etl_database_error(db_config, sinapi_config, mock_data):
    """Testa falha no banco de dados."""
    with patch('autosinapi.Downloader') as mock_downloader, \
         patch('autosinapi.Processor') as mock_processor, \
         patch('autosinapi.Database') as mock_db:
        
        mock_downloader_instance = Mock()
        mock_downloader_instance.download.return_value = b'fake_excel_data'
        mock_downloader.return_value = mock_downloader_instance
        
        mock_processor_instance = Mock()
        mock_processor_instance.process.return_value = mock_data
        mock_processor.return_value = mock_processor_instance
        
        mock_db_instance = Mock()
        mock_db_instance.save_data.side_effect = DatabaseError("Erro no banco de dados")
        mock_db.return_value = mock_db_instance
        
        result = run_etl(db_config, sinapi_config, mode='server')
        
        assert result['status'] == 'error'
        assert 'banco de dados' in result['message'].lower()

def test_run_etl_invalid_mode(db_config, sinapi_config):
    """Testa modo de operação inválido."""
    result = run_etl(db_config, sinapi_config, mode='invalid')
    
    assert result['status'] == 'error'
    assert 'modo' in result['message'].lower()

def test_run_etl_invalid_config(db_config, sinapi_config):
    """Testa configurações inválidas."""
    # Remove campo obrigatório
    del db_config['host']
    
    result = run_etl(db_config, sinapi_config, mode='server')
    
    assert result['status'] == 'error'
    assert 'configuração' in result['message'].lower()
