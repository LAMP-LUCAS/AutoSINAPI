def test_read_xlsx_planilhas_reais():
    """Testa se o parser lê corretamente todas as planilhas relevantes dos arquivos xlsx reais do SINAPI."""
    import os
    from autosinapi.core.processor import read_sinapi_file
    arquivos_planilhas = [
        ('tools/downloads/2025_07/SINAPI-2025-07-formato-xlsx/SINAPI_mao_de_obra_2025_07.xlsx', None),
        ('tools/downloads/2025_07/SINAPI-2025-07-formato-xlsx/SINAPI_Referência_2025_07.xlsx', None),
        ('tools/downloads/2025_07/SINAPI-2025-07-formato-xlsx/SINAPI_Manutenções_2025_07.xlsx', None),
    ]
    for arquivo, planilha in arquivos_planilhas:
        assert os.path.exists(arquivo), f"Arquivo não encontrado: {arquivo}"
        import pandas as pd
        xls = pd.ExcelFile(arquivo)
        for sheet in xls.sheet_names:
            df = read_sinapi_file(arquivo, sheet_name=sheet, dtype=str)
            assert isinstance(df, pd.DataFrame)
            # O parser deve conseguir ler todas as planilhas não vazias
            if df.shape[0] > 0:
                print(f"Arquivo {os.path.basename(arquivo)} - Planilha '{sheet}': {df.shape[0]} linhas, {df.shape[1]} colunas")
"""
Testes unitários para o módulo processor.py
"""
import pytest
from unittest.mock import Mock
import pandas as pd
import numpy as np
import logging
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
    p = Processor(config)
    p.logger.setLevel(logging.DEBUG)
    return p

@pytest.fixture
def sample_insumos_df():
    """Fixture que cria um DataFrame de exemplo para insumos."""
    df = pd.DataFrame({
        'CODIGO': ['1234', '5678', '9012'],
        'DESCRICAO': ['AREIA MEDIA', 'CIMENTO PORTLAND', 'TIJOLO CERAMICO'],
        'UNIDADE': ['M3', 'KG', 'UN'],
        'PRECO_MEDIANO': [120.50, 0.89, 1.25]
    })
    df.index = range(3)  # Garante índices sequenciais
    return df

@pytest.fixture
def sample_composicoes_df():
    """Fixture que cria um DataFrame de exemplo para composições."""
    return pd.DataFrame({
        'CODIGO_COMPOSICAO': ['87453', '87522', '87890'],
        'DESCRICAO_COMPOSICAO': [
            'ALVENARIA DE VEDACAO',
            'REVESTIMENTO CERAMICO',
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
    processor.logger.debug(f"Test clean_data_remove_empty - input columns: {list(df.columns)}")
    result = processor._clean_data(df)
    processor.logger.debug(f"Test clean_data_remove_empty - output columns: {list(result.columns)}")
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
    processor.logger.debug(f"Test clean_data_normalize_columns - input columns: {list(df.columns)}")
    result = processor._clean_data(df)
    processor.logger.debug(f"Test clean_data_normalize_columns - output columns: {list(result.columns)}")
    # Após normalização, os nomes devem ser compatíveis com o DataModel
    # Aceita 'codigo' (catálogo) ou 'item_codigo' (estrutura)
    assert 'descricao' in result.columns
    assert 'preco_mediano' in result.columns
    assert any(col in result.columns for col in ['codigo', 'item_codigo'])

def test_clean_data_normalize_text(processor):
    """Testa a normalização de textos."""
    df = pd.DataFrame({
        'DESCRICAO': ['Areia  Média ', 'CIMENTO portland', 'Tijolo  Cerâmico']
    })
    processor.logger.debug(f"Test clean_data_normalize_text - input: {df['DESCRICAO'].tolist()}")
    result = processor._clean_data(df)
    processor.logger.debug(f"Test clean_data_normalize_text - output: {result['descricao'].tolist()}")
    # Agora as descrições devem estar encapsuladas por aspas duplas e manter acentuação
    assert all(x.startswith('"') and x.endswith('"') for x in result['descricao'])

def test_transform_insumos(processor, sample_insumos_df):
    """Testa transformação de dados de insumos."""
    result = processor._transform_insumos(sample_insumos_df)
    assert 'CODIGO_INSUMO' in result.columns
    assert 'DESCRICAO_INSUMO' in result.columns
    assert 'PRECO_MEDIANO' in result.columns
    assert result['PRECO_MEDIANO'].dtype in ['float64', 'float32']

def test_transform_composicoes(processor, sample_composicoes_df):
    """Testa transformação de dados de composições."""
    result = processor._transform_composicoes(sample_composicoes_df)
    assert 'CODIGO' in result.columns
    assert 'DESCRICAO' in result.columns
    assert 'CUSTO_TOTAL' in result.columns
    assert result['CUSTO_TOTAL'].dtype in ['float64', 'float32']

def test_validate_data_empty_df(processor):
    """Testa validação com DataFrame vazio."""
    df = pd.DataFrame()
    
    with pytest.raises(ProcessingError, match="DataFrame está vazio"):
        processor._validate_data(df)

def test_validate_data_invalid_codes(processor, sample_insumos_df):
    """Testa validação de códigos inválidos."""
    # Cria uma cópia para não afetar o fixture
    df = sample_insumos_df.copy()
    df.loc[0, 'CODIGO'] = 'ABC'  # Código inválido
    # Ajusta para compatibilidade com o novo mapeamento
    df = df.rename(columns={'CODIGO': 'codigo', 'DESCRICAO': 'descricao', 'UNIDADE': 'unidade', 'PRECO_MEDIANO': 'preco_mediano'})
    result = processor._validate_data(df)
    # Só deve restar linhas com código numérico
    assert all(result['codigo'].str.isnumeric())

def test_validate_data_negative_prices(processor, sample_insumos_df):
    """Testa validação de preços negativos."""
    # Cria uma cópia para não afetar o fixture
    df = sample_insumos_df.copy()
    df.loc[0, 'PRECO_MEDIANO'] = -10.0
    df = df.rename(columns={'CODIGO': 'codigo', 'DESCRICAO': 'descricao', 'UNIDADE': 'unidade', 'PRECO_MEDIANO': 'preco_mediano'})
    result = processor._validate_data(df)
    # Se houver linhas, o preço negativo deve ser None
    if not result.empty:
        assert result['preco_mediano'].isnull().iloc[0]

def test_validate_insumos_code_length(processor):
    """Testa validação do tamanho dos códigos de insumos."""
    df = pd.DataFrame({
        'CODIGO_INSUMO': ['123', '1234', '12345'],  # Primeiro código inválido
        'DESCRICAO_INSUMO': ['A', 'B', 'C']
    })
    result = processor._validate_insumos(df)
    # Aceita códigos com 4 ou mais dígitos
    assert len(result) == 2
    assert set(result['CODIGO_INSUMO']) == {'1234', '12345'}

def test_validate_composicoes_code_length(processor):
    """Testa validação do tamanho dos códigos de composições."""
    df = pd.DataFrame({
        'codigo': ['1234', '12345', '123456'],  # Primeiro código inválido
        'descricao': ['A', 'B', 'C']
    })
    result = processor._validate_composicoes(df)
    # Aceita códigos com exatamente 6 dígitos
    assert all(result['codigo'].str.len() == 6)
    assert set(result['codigo']) == {'123456'}


def test_process_composicao_itens(tmp_path):
    """Testa o processamento da estrutura das composições e inserção na tabela composicao_itens."""
    import pandas as pd
    from sqlalchemy.engine import create_engine, Connection, Engine
    from sqlalchemy import text
    # Cria DataFrame simulado
    df = pd.DataFrame({
        'CÓDIGO DA COMPOSIÇÃO': [1001, 1001, 1002],
        'CÓDIGO DO ITEM': [2001, 2002, 2003],
        'TIPO ITEM': ['INSUMO', 'COMPOSICAO', 'INSUMO'],
        'COEFICIENTE': ['1,5', '2.0', '0,75']
    })
    # Salva como xlsx temporário
    xlsx_path = tmp_path / 'analitico.xlsx'
    with pd.ExcelWriter(xlsx_path) as writer:
        df.to_excel(writer, index=False, sheet_name='Analítico')

    # Cria engine SQLite em memória para teste
    engine = create_engine('sqlite:///:memory:')
    
    # Cria tabela composicao_itens
    with engine.connect() as conn:
        conn.execute(text('''CREATE TABLE composicao_itens (
            composicao_pai_codigo INTEGER,
            item_codigo INTEGER,
            tipo_item TEXT,
            coeficiente REAL
        )'''))
        conn.commit()

    # Processa os dados
    processor = Processor({'year': 2025, 'month': 8, 'type': 'REFERENCIA'})
    processor.process_composicao_itens(str(xlsx_path), engine)

    # Verifica se os dados foram inseridos corretamente
    result = pd.read_sql('SELECT * FROM composicao_itens ORDER BY composicao_pai_codigo', engine)
    assert len(result) == 3
    assert set(result['tipo_item']) == {'INSUMO', 'COMPOSICAO'}


