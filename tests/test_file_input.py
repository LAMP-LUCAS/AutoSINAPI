"""
Testes do módulo de download com suporte a input direto de arquivo.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch
from io import BytesIO

import pandas as pd
import pytest

from autosinapi.etl_pipeline import PipelineETL


@pytest.fixture
def mock_pipeline(mocker, tmp_path):
    """Fixture para mockar o pipeline e suas dependências."""
    mocker.patch("autosinapi.etl_pipeline.setup_logging")

    # Mock do objeto Config
    mock_config = MagicMock()
    mock_config.DOWNLOAD_DIR = tmp_path / "downloads"
    mock_config.YEAR = "2023"
    mock_config.MONTH = "01"
    mock_config.STATE = "SP"
    mock_config.TYPE = "insumos"
    mock_config.DB_HOST = "localhost"
    mock_config.DB_PORT = 5432
    mock_config.DB_NAME = "test_db"
    mock_config.DB_USER = "test_user"
    mock_config.DB_PASSWORD = "test_pass"
    mock_config.REFERENCE_FILE_KEYWORD = "Referencia"
    mock_config.MAINTENANCE_FILE_KEYWORD = "Manuten"
    mock_config.MAINTENANCE_DEACTIVATION_KEYWORD = "%DESATIVAÇÃO%"
    mock_config.DB_TABLE_MANUTENCOES = "manutencoes_historico"
    mock_config.DB_TABLE_INSUMOS = "insumos"
    mock_config.DB_TABLE_COMPOSICOES = "composicoes"
    mock_config.DB_TABLE_COMPOSICAO_INSUMOS = "composicao_insumos"
    mock_config.DB_TABLE_COMPOSICAO_SUBCOMPOSICOES = "composicao_subcomposicoes"
    mock_config.DB_TABLE_PRECOS_INSUMOS = "precos_insumos_mensal"
    mock_config.DB_TABLE_CUSTOS_COMPOSICOES = "custos_composicoes_mensal"
    mock_config.ITEM_TYPE_INSUMO = "INSUMO"
    mock_config.ITEM_TYPE_COMPOSICAO = "COMPOSICAO"
    mock_config.SHEETS_TO_CONVERT = ['CSD', 'CCD', 'CSE']
    mock_config.sinapi_config = {"state": "SP", "month": "01", "year": "2023", "type": "insumos"} 
    mock_config.STATUS_SUCCESS = "SUCESSO"
    mock_config.STATUS_FAILURE = "FALHA"
    mock_config.VERSION = "1.2.0"

    # Patch para que PipelineETL use o mock_config
    mocker.patch("autosinapi.etl_pipeline.Config", return_value=mock_config)
    mocker.patch("autosinapi.etl_pipeline.PipelineETL._get_db_config", return_value={})
    mocker.patch("autosinapi.etl_pipeline.PipelineETL._get_sinapi_config", return_value={})
    mocker.patch("autosinapi.etl_pipeline.PipelineETL._load_base_config", return_value={})

    # Cria um diretório de extração falso
    extraction_path = tmp_path / "extraction"
    extraction_path.mkdir()
    # Cria um arquivo de referência falso dentro do diretório
    referencia_file_name = f"SINAPI_{mock_config.REFERENCE_FILE_KEYWORD}_20_23_01.xlsx"
    referencia_file_path = extraction_path / referencia_file_name
    
    with patch("autosinapi.etl_pipeline.Database") as mock_db_class, patch(
        "autosinapi.etl_pipeline.Downloader") as mock_downloader_class, patch(
        "autosinapi.etl_pipeline.Processor") as mock_processor_class, patch(
        "autosinapi.etl_pipeline.convert_excel_sheets_to_csv") as mock_convert:

        mock_db_instance = MagicMock()
        mock_db_class.return_value = mock_db_instance
        mock_db_instance.__enter__.return_value = mock_db_instance

        mock_downloader_instance = MagicMock()
        mock_downloader_class.return_value = mock_downloader_instance
        mock_downloader_instance.get_sinapi_data.return_value = (str(referencia_file_path), {})

        mock_processor_instance = MagicMock()
        mock_processor_class.return_value = mock_processor_instance

        pipeline = PipelineETL(run_id="test-run", config_path=None) 

        yield (
            pipeline,
            mock_db_instance,
            mock_downloader_instance,
            mock_processor_instance,
            mock_convert,
            referencia_file_path,
            mock_config
        )


def test_direct_file_input(tmp_path, mock_pipeline):
    """Testa o pipeline com input direto de arquivo."""
    pipeline, mock_db, mock_downloader, mock_processor, mock_convert, referencia_file_path, mock_config = mock_pipeline

    test_file = tmp_path / "test_sinapi.xlsx"
    df = pd.DataFrame(
        {
            "codigo": [1234, 5678],
            "descricao": ["Item 1", "Item 2"],
            "unidade": ["un", "kg"],
            "preco": [10.5, 20.75],
            "classificacao": ["c1", "c2"]
        }
    )
    mock_downloader.get_sinapi_data.return_value = (str(test_file), {})

    mock_processor.process_catalogo_e_precos.return_value = {"insumos": df}
    mock_processor.process_composicao_itens.return_value = {
        "composicao_insumos": pd.DataFrame(columns=["composicao_pai_codigo", "insumo_filho_codigo"]),
        "composicao_subcomposicoes": pd.DataFrame(columns=["composicao_pai_codigo", "composicao_filho_codigo"]),
        "parent_composicoes_details": pd.DataFrame(
            columns=["codigo", "descricao", "unidade", "grupo"]
        ),
        "child_item_details": pd.DataFrame(
            columns=["codigo", "tipo", "descricao", "unidade"]
        ),
    }

    result = pipeline.run(input_file_path=str(test_file)) 

    assert result["status"] == "SUCESSO"
    assert mock_db.save_data.called


def test_invalid_input_file(mock_pipeline):
    """Testa erro ao fornecer arquivo inválido."""
    pipeline, mock_db, mock_downloader, _, _, _, _ = mock_pipeline

    mock_downloader.get_sinapi_data.side_effect = Exception("Erro de download")

    result = pipeline.run()

    assert result["status"] == "FALHA"
    assert "Erro de download" in result["message"]
