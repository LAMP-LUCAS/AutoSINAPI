"""
Testes unitários para o módulo de download.
"""
import pytest
from unittest.mock import Mock, patch
from pathlib import Path
from io import BytesIO
import requests
from autosinapi.core.downloader import Downloader
from autosinapi.exceptions import DownloadError

# Fixtures
@pytest.fixture
def sinapi_config():
    return {
        'state': 'SP',
        'month': '01',
        'year': '2023',
        'type': 'REFERENCIA'
    }

@pytest.fixture
def mock_response():
    response = Mock()
    response.content = b'test content'
    response.raise_for_status = Mock()
    return response

# Testes de URL Building
def test_build_url_referencia(sinapi_config):
    """Testa construção de URL para planilha referencial."""
    downloader = Downloader(sinapi_config, 'server')
    url = downloader._build_url()
    
    assert 'SINAPI_REFERENCIA_01_2023.zip' in url
    assert url.startswith('https://www.caixa.gov.br/Downloads/sinapi-a-vista-composicoes')

def test_build_url_desonerado():
    """Testa construção de URL para planilha desonerada."""
    config = {
        'state': 'SP',
        'month': '12',
        'year': '2023',
        'type': 'DESONERADO'
    }
    downloader = Downloader(config, 'server')
    url = downloader._build_url()
    
    assert 'SINAPI_DESONERADO_12_2023.zip' in url

def test_build_url_invalid_type():
    """Testa erro ao construir URL com tipo inválido."""
    config = {
        'state': 'SP',
        'month': '01',
        'year': '2023',
        'type': 'INVALIDO'
    }
    downloader = Downloader(config, 'server')
    
    with pytest.raises(ValueError, match="Tipo de planilha inválido"):
        downloader._build_url()

def test_build_url_zero_padding():
    """Testa padding com zeros nos números."""
    config = {
        'state': 'SP',
        'month': 1,  # Número sem zero
        'year': 2023,
        'type': 'REFERENCIA'
    }
    downloader = Downloader(config, 'server')
    url = downloader._build_url()
    
    assert 'SINAPI_REFERENCIA_01_2023.zip' in url

# Testes
@patch('autosinapi.core.downloader.requests.Session')
def test_successful_download(mock_session, sinapi_config, mock_response):
    """Deve realizar download com sucesso."""
    # Configura o mock
    session = Mock()
    session.get.return_value = mock_response
    mock_session.return_value = session
    
    # Executa o download
    downloader = Downloader(sinapi_config, 'server')
    result = downloader.download()
    
    # Verifica o resultado
    assert isinstance(result, BytesIO)
    assert result.getvalue() == b'test content'
    session.get.assert_called_once()

@patch('autosinapi.core.downloader.requests.Session')
def test_download_network_error(mock_session, sinapi_config):
    """Deve tratar erro de rede corretamente."""
    # Configura o mock para simular erro
    session = Mock()
    session.get.side_effect = requests.ConnectionError('Network error')
    mock_session.return_value = session
    
    # Verifica se levanta a exceção correta
    with pytest.raises(DownloadError) as exc_info:
        downloader = Downloader(sinapi_config, 'server')
        downloader.download()
    
    assert 'Network error' in str(exc_info.value)

@patch('autosinapi.core.downloader.requests.Session')
def test_local_mode_save(mock_session, sinapi_config, mock_response, tmp_path):
    """Deve salvar arquivo localmente em modo local."""
    # Configura o mock
    session = Mock()
    session.get.return_value = mock_response
    mock_session.return_value = session
    
    # Cria caminho temporário para teste
    save_path = tmp_path / 'test.xlsx'
    
    # Executa o download em modo local
    downloader = Downloader(sinapi_config, 'local')
    result = downloader.download(save_path)
    
    # Verifica se salvou o arquivo
    assert save_path.exists()
    assert save_path.read_bytes() == b'test content'
    
    # Verifica se também retornou o conteúdo em memória
    assert isinstance(result, BytesIO)
    assert result.getvalue() == b'test content'

def test_context_manager(sinapi_config):
    """Deve funcionar corretamente como context manager."""
    with Downloader(sinapi_config, 'server') as downloader:
        assert isinstance(downloader, Downloader)
        # A sessão será fechada automaticamente ao sair do contexto
