def test_real_excel_input(tmp_path):
    """Testa o pipeline com um arquivo Excel real do SINAPI."""
    import shutil
    from autosinapi import run_etl
    # Copia um arquivo real para o tmp_path para simular input do usuário
    src_file = 'tools/downloads/2025_07/SINAPI-2025-07-formato-xlsx/SINAPI_mao_de_obra_2025_07.xlsx'
    test_file = tmp_path / 'SINAPI_mao_de_obra_2025_07.xlsx'
    shutil.copy(src_file, test_file)

    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass'
    }
    sinapi_config = {
        'state': 'SP',
        'month': '07',
        'year': '2025',
        'type': 'insumos',
        'input_file': str(test_file)
    }
    result = run_etl(db_config, sinapi_config, mode='server')
    if result['status'] != 'success':
        print('Erro no pipeline:', result)
    assert result['status'] == 'success'
    assert isinstance(result['details'].get('rows_processed', 1), int)
"""
Testes do módulo de download com suporte a input direto de arquivo.
"""
import pytest
from pathlib import Path
import pandas as pd
from autosinapi import run_etl

def test_direct_file_input(tmp_path):
    """Testa o pipeline com input direto de arquivo."""
    # Cria um arquivo XLSX de teste
    test_file = tmp_path / "test_sinapi.xlsx"
    df = pd.DataFrame({
        'codigo': [1234, 5678],
        'descricao': ['Item 1', 'Item 2'],
        'unidade': ['un', 'kg'],
        'preco': [10.5, 20.75]
    })
    df.to_excel(test_file, index=False)
    
    # Configura o teste
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass'
    }
    
    sinapi_config = {
        'state': 'SP',
        'month': '01',
        'year': '2023',
        'type': 'insumos',
        'input_file': str(test_file)  # Usa arquivo local
    }
    
    # Executa o pipeline
    result = run_etl(db_config, sinapi_config, mode='server')
    
    # Verifica o resultado
    assert result['status'] == 'success'
    assert result['details']['rows_processed'] == 2
    assert isinstance(result['details']['timestamp'], str)

def test_fallback_to_download(mocker):
    """Testa o fallback para download quando arquivo não é fornecido."""
    # Mock do downloader
    mock_download = mocker.patch('autosinapi.core.downloader.Downloader._download_file')
    mock_download.return_value = mocker.Mock()
    
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass'
    }
    
    sinapi_config = {
        'state': 'SP',
        'month': '01',
        'year': '2023',
        'type': 'insumos'
        # Sem input_file, deve tentar download
    }
    
    # Executa o pipeline
    result = run_etl(db_config, sinapi_config, mode='server')
    
    # Verifica se o download foi tentado
    mock_download.assert_called_once()

def test_invalid_input_file():
    """Testa erro ao fornecer arquivo inválido."""
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass'
    }
    
    sinapi_config = {
        'state': 'SP',
        'month': '01',
        'year': '2023',
        'type': 'insumos',
        'input_file': 'arquivo_inexistente.xlsx'
    }
    
    # Executa o pipeline
    result = run_etl(db_config, sinapi_config, mode='server')
    
    # Verifica se retornou erro
    assert result['status'] == 'error'
    assert 'Arquivo não encontrado' in result['message']
