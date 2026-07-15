"""
Testes unitários para o módulo processor.py
"""

import logging

import pandas as pd
import pytest

from autosinapi.config import Config
from autosinapi.core.processor import Processor


@pytest.fixture
def db_config():
    """Fixture com configuração de teste do banco de dados."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass",
    }


@pytest.fixture
def sinapi_config():
    """Fixture com configuração SINAPI mínima para testes."""
    return {"state": "SP", "month": 8, "year": 2025, "type": "REFERENCIA"}


@pytest.fixture
def processor(db_config, sinapi_config):
    """Fixture que cria um processador com configurações completas."""
    config = Config(db_config, sinapi_config, mode="server")
    p = Processor(config)
    p.logger.setLevel(logging.DEBUG)
    return p


@pytest.fixture
def sample_insumos_df():
    """Fixture que cria um DataFrame de exemplo para insumos."""
    return pd.DataFrame(
        {
            "CODIGO": ["1234", "5678", "9012"],
            "DESCRICAO": ["AREIA MEDIA", "CIMENTO PORTLAND", "TIJOLO CERAMICO"],
            "UNIDADE": ["M3", "KG", "UN"],
            "PRECO_MEDIANO": [120.50, 0.89, 1.25],
        }
    )


@pytest.fixture
def sample_composicoes_df():
    """Fixture que cria um DataFrame de exemplo para composições."""
    return pd.DataFrame(
        {
            "CODIGO_COMPOSICAO": ["87453", "87522", "87890"],
            "DESCRICAO_COMPOSICAO": [
                "ALVENARIA DE VEDACAO",
                "REVESTIMENTO CERAMICO",
                "CONTRAPISO",
            ],
            "UNIDADE": ["M2", "M2", "M2"],
            "CUSTO_TOTAL": [89.90, 45.75, 32.80],
        }
    )


def test_normalize_cols(processor):
    """Testa a normalização dos nomes das colunas."""
    df = pd.DataFrame(
        {
            "Código do Item": [1, 2, 3],
            "Descrição": ["a", "b", "c"],
            "Preço Unitário": [10, 20, 30],
        }
    )
    result = processor._normalize_cols(df)
    assert "CODIGO_DO_ITEM" in result.columns
    assert "DESCRICAO" in result.columns
    assert "PRECO_UNITARIO" in result.columns


def test_csv_custos_path_resolves_correctly(processor, tmp_path):
    """
    Issue 1: Verifica que _process_custos_sheet busca CSV no diretório
    SINAPI-*/csv_temp/ (parent/csv_temp) e NÃO em downloads/YYYY_MM/csv_temp/
    (parent.parent/csv_temp).
    """
    import pandas as pd
    from pathlib import Path

    # Simula xlsx em: tmp_path/2026_05/SINAPI-2026-05-formato-xlsx/ref.xlsx
    xlsx_dir = tmp_path / "2026_05" / "SINAPI-2026-05-formato-xlsx"
    xlsx_dir.mkdir(parents=True)
    csv_dir_correct = xlsx_dir / "csv_temp"
    csv_dir_correct.mkdir()
    csv_path = csv_dir_correct / "CSD.csv"
    # Cria CSV dummy com estrutura mínima
    pd.DataFrame({"col": ["CODIGO", "DESCRICAO", "UNIDADE", "CUSTO_AC"]}).to_csv(
        csv_path, index=False, header=False, sep=";"
    )
    xlsx_file = xlsx_dir / "ref.xlsx"
    xlsx_file.touch()

    # O CSV está em xlsx_dir.parent / csv_temp ⇒ deve ser encontrado
    # Se o bug existir (parent.parent), o path seria tmp_path/2026_05/csv_temp/
    # que não existe — então levantaria FileNotFoundError

    try:
        processor._process_custos_sheet(str(xlsx_file), "CSD")
        found = True
    except FileNotFoundError as e:
        found = False
        err_path = str(e)

    assert found, (
        f"CSV não encontrado! O bug parent.parent busca em 'downloads/YYYY_MM/csv_temp/'.\n"
        f"Caminho correto seria: {csv_dir_correct}\n"
        f"Erro: {err_path}"
    )


def test_familias_header_row_uses_correct_index(processor, tmp_path):
    """
    Issue 2: Verifica que process_familias_e_coeficientes usa header=5
    (não header=4). Cria xlsx com header real na linha 5 (0-indexed)
    e colunas esperadas: CODIGO_DA_FAMILIA, CODIGO_DO_INSUMO, CATEGORIA.
    """
    import pandas as pd
    from pathlib import Path

    test_file = tmp_path / "test_familias.xlsx"

    # Conteúdo real: header na linha 5 (0-indexed)
    # header=4 pegaria a linha de "Coeficientes:" que é label, não cabeçalho
    rows = [
        ["SINAPI - Sistema Nacional..."],
        ["RELATÓRIO DE FAMÍLIAS E COEFICIENTES"],
        ["Mês de Referência:", "05/2026"],
        ["Data de emissão:", "01/06/2026"],
        [],  # ← header=4 pegaria esta linha VAZIA → todas colunas Unnamed
        ["Código da Família", "Código do Insumo", "Descrição", "Unidade", "Categoria", "AC", "AL"],
        [1001, 5001, "Areia", "M3", "MATERIAL", 1.5, 1.3],
        [1001, 5002, "Cimento", "KG", "MATERIAL", 0.8, 0.9],
    ]
    df_write = pd.DataFrame(rows)
    writer = pd.ExcelWriter(test_file, engine="xlsxwriter")
    df_write.to_excel(writer, index=False, header=False, sheet_name="Sheet1")
    writer.close()

    result = processor.process_familias_e_coeficientes(str(test_file))

    # Se header=4 (bug): retorna {} vazio porque colunas não existem
    # Se header=5 (fix): retorna dict com 'insumos_familias' e 'coeficientes_familia_mensal'
    assert "insumos_familias" in result, (
        "BUG: process_familias_e_coeficientes retornou vazio! "
        "Provavelmente header=4 não encontra as colunas esperadas. "
        "Resultado esperado deve conter 'insumos_familias' e 'coeficientes_familia_mensal'."
    )
    assert "coeficientes_familia_mensal" in result
    assert len(result["insumos_familias"]) > 0
    assert "codigo_familia" in result["insumos_familias"].columns


def test_mao_de_obra_header_row_uses_correct_index(processor, tmp_path):
    """
    Issue 2: Verifica que process_mao_de_obra usa header=5.
    Cria xlsx com header real na linha 5 e coluna CODIGO_DA_COMPOSICAO.
    """
    import pandas as pd

    test_file = tmp_path / "test_mao_de_obra.xlsx"

    rows = [
        ["SINAPI - Sistema Nacional..."],
        ["RELATÓRIO DE PERCENTUAL DE MÃO DE OBRA"],
        ["Mês de Referência:", "05/2026"],
        ["Data de emissão:", "01/06/2026"],
        [],  # ← header=4 pegaria linha vazia
        ["Grupo", "Código da Composição", "Descrição", "Unidade", "AC", "AL"],
        ["A", 87453, "ALVENARIA", "M2", 35.0, 32.5],
        ["A", 87454, "REBOCO", "M2", 42.0, 38.0],
    ]
    df_write = pd.DataFrame(rows)
    writer = pd.ExcelWriter(test_file, engine="xlsxwriter")
    df_write.to_excel(writer, index=False, header=False, sheet_name="Sheet1")
    writer.close()

    result = processor.process_mao_de_obra(str(test_file))

    # Se header=4 (bug): retorna DataFrame vazio
    # Se header=5 (fix): retorna DataFrame com composicao_codigo
    assert not result.empty, (
        "BUG: process_mao_de_obra retornou DataFrame vazio! "
        "Provavelmente header=4 não encontra a coluna CODIGO_DA_COMPOSICAO."
    )
    assert "composicao_codigo" in result.columns


def test_process_composicao_itens(processor, tmp_path):
    """Testa o processamento da estrutura das composições."""
    # Cria um arquivo XLSX de teste
    test_file = tmp_path / "test_sinapi.xlsx"
    df = pd.DataFrame(
        {
            "GRUPO": ["A", "A"],
            "CODIGO_DA_COMPOSICAO": ["87453", "87453"],
            "TIPO_ITEM": ["INSUMO", "COMPOSICAO"],
            "CODIGO_DO_ITEM": ["1234", "5678"],
            "COEFICIENTE": ["1,0", "2,5"],
            "DESCRICAO": ["INSUMO A", "COMPOSICAO B"],
            "UNIDADE": ["UN", "M2"],
        }
    )
    # Adiciona linha de cabeçalho e outras linhas para simular o arquivo real
    writer = pd.ExcelWriter(test_file, engine="xlsxwriter")
    df.to_excel(writer, index=False, header=True, sheet_name="Analítico", startrow=9)
    writer.close()

    result = processor.process_composicao_itens(str(test_file))

    assert "composicao_insumos" in result
    assert "composicao_subcomposicoes" in result
    assert len(result["composicao_insumos"]) == 1
    assert len(result["composicao_subcomposicoes"]) == 1
    assert result["composicao_insumos"].iloc[0]["insumo_filho_codigo"] == 1234