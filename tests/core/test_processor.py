"""
Testes unitários para o módulo processor.py
"""
import pytest
import pandas as pd
import numpy as np
from autosinapi.core.processor import Processor
from autosinapi.exceptions import ProcessingError

@pytest.fixture
def processor():
    """Fixture que cria um processador com configurações básicas."""
    config = {
        'year': 2025,
        'month': 8,
        'type': 'REFERENCIA'
    }
    return Processor(config)

@pytest.fixture
def sample_insumos_df():
    """Fixture que cria um DataFrame de exemplo para insumos."""
    return pd.DataFrame({
        'CODIGO': ['1234', '5678', '9012'],
        'DESCRICAO': ['AREIA MÉDIA', 'CIMENTO PORTLAND', 'TIJOLO CERÂMICO'],
        'UNIDADE': ['M3', 'KG', 'UN'],
        'PRECO_MEDIANO': [120.50, 0.89, 1.25]
    })

@pytest.fixture
def sample_composicoes_df():
    """Fixture que cria um DataFrame de exemplo para composições."""
    return pd.DataFrame({
        'CODIGO_COMPOSICAO': ['87453', '87522', '87890'],
        'DESCRICAO_COMPOSICAO': [
            'ALVENARIA DE VEDAÇÃO',
            'REVESTIMENTO CERÂMICO',
            'CONTRAPISO'
        ],
        'UNIDADE': ['M2', 'M2', 'M2'],
        'CUSTO_TOTAL': [89.90, 45.75, 32.80]
    })

def test_clean_data_remove_empty(processor):
    """Testa se a limpeza remove linhas e colunas vazias."""
    df = pd.DataFrame({
        'A': [1, np.nan, 3],
        'B': [np.nan, np.nan, np.nan],
        'C': ['x', 'y', 'z']
    })
    
    result = processor._clean_data(df)
    
    assert 'B' not in result.columns
    assert len(result) == 3
    assert result['A'].isna().sum() == 1

def test_clean_data_normalize_columns(processor):
    """Testa a normalização dos nomes das colunas."""
    df = pd.DataFrame({
        'Código do Item': [1, 2, 3],
        'Descrição': ['a', 'b', 'c'],
        'Preço Unitário': [10, 20, 30]
    })
    
    result = processor._clean_data(df)
    
    assert 'CODIGO_DO_ITEM' in result.columns
    assert 'DESCRICAO' in result.columns
    assert 'PRECO_UNITARIO' in result.columns

def test_clean_data_normalize_text(processor):
    """Testa a normalização de textos."""
    df = pd.DataFrame({
        'DESCRICAO': ['Areia  Média ', 'CIMENTO portland', 'Tijolo  Cerâmico']
    })
    
    result = processor._clean_data(df)
    
    assert result['DESCRICAO'].tolist() == ['AREIA MEDIA', 'CIMENTO PORTLAND', 'TIJOLO CERAMICO']

def test_transform_insumos(processor, sample_insumos_df):
    """Testa transformação de dados de insumos."""
    result = processor._transform_insumos(sample_insumos_df)
    
    assert 'CODIGO_INSUMO' in result.columns
    assert 'DESCRICAO_INSUMO' in result.columns
    assert 'PRECO_UNITARIO' in result.columns
    assert result['PRECO_UNITARIO'].dtype in ['float64', 'float32']

def test_transform_composicoes(processor, sample_composicoes_df):
    """Testa transformação de dados de composições."""
    result = processor._transform_composicoes(sample_composicoes_df)
    
    assert 'CODIGO' in result.columns
    assert 'DESCRICAO' in result.columns
    assert 'PRECO_UNITARIO' in result.columns
    assert result['PRECO_UNITARIO'].dtype in ['float64', 'float32']

def test_validate_data_empty_df(processor):
    """Testa validação com DataFrame vazio."""
    df = pd.DataFrame()
    
    with pytest.raises(ProcessingError, match="DataFrame está vazio"):
        processor._validate_data(df)

def test_validate_data_invalid_codes(processor, sample_insumos_df):
    """Testa validação de códigos inválidos."""
    sample_insumos_df.loc[0, 'CODIGO'] = 'ABC'  # Código inválido
    
    result = processor._validate_data(sample_insumos_df)
    
    assert len(result) == 2  # Deve remover a linha com código inválido
    assert 'ABC' not in result['CODIGO'].values

def test_validate_data_negative_prices(processor, sample_insumos_df):
    """Testa validação de preços negativos."""
    sample_insumos_df.loc[0, 'PRECO_MEDIANO'] = -10.0
    
    result = processor._validate_data(sample_insumos_df)
    
    assert pd.isna(result.loc[0, 'PRECO_UNITARIO'])

def test_validate_insumos_code_length(processor):
    """Testa validação do tamanho dos códigos de insumos."""
    df = pd.DataFrame({
        'CODIGO_INSUMO': ['123', '1234', '12345'],  # Primeiro código inválido
        'DESCRICAO_INSUMO': ['A', 'B', 'C']
    })
    
    result = processor._validate_insumos(df)
    
    assert len(result) == 2
    assert '123' not in result['CODIGO_INSUMO'].values

def test_validate_composicoes_code_length(processor):
    """Testa validação do tamanho dos códigos de composições."""
    df = pd.DataFrame({
        'CODIGO': ['1234', '12345', '123456'],  # Primeiro código inválido
        'DESCRICAO': ['A', 'B', 'C']
    })
    
    result = processor._validate_composicoes(df)
    
    assert len(result) == 2
    assert '1234' not in result['CODIGO'].values
