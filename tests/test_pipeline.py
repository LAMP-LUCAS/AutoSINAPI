"""
Testes de integração para o pipeline principal do AutoSINAPI.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from autosinapi.exceptions import DatabaseError, DownloadError, ProcessingError
from autosinapi.etl_pipeline import PipelineETL


@pytest.fixture
def db_config():
    """Fixture com configurações do banco de dados."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass",
    }


@pytest.fixture
def sinapi_config():
    """Fixture com configurações do SINAPI."""
    return {
        "state": "SP",
        "year": 2025,
        "month": 8,
        "type": "REFERENCIA",
        "duplicate_policy": "substituir",
    }


@pytest.fixture
def mock_pipeline(mocker, db_config, sinapi_config, tmp_path):
    """Fixture para mockar o pipeline e suas dependências."""
    mocker.patch("autosinapi.etl_pipeline.setup_logging")

    # Cria um diretório de extração falso
    extraction_path = tmp_path / "extraction"
    extraction_path.mkdir()
    # Cria um arquivo de referência falso dentro do diretório
    referencia_file_path = extraction_path / "SINAPI_Referência_2025_08.xlsx"
    referencia_file_path.touch()

    with patch("autosinapi.core.database.Database") as mock_db, patch(
        "autosinapi.core.downloader.Downloader"
    ) as mock_downloader, patch(
        "autosinapi.core.processor.Processor"
    ) as mock_processor, patch(
        "autosinapi.core.pre_processor.convert_excel_sheets_to_csv"
    ) as mock_convert_excel_sheets_to_csv: # New mock for the new pre_processor function

        mock_db_instance = MagicMock()
        mock_db.return_value = mock_db_instance

        mock_downloader_instance = MagicMock()
        mock_downloader.return_value = mock_downloader_instance

        mock_processor_instance = MagicMock()
        mock_processor.return_value = mock_processor_instance

        pipeline = PipelineETL(config_path=None) # Changed to PipelineETL

        mocker.patch.object(pipeline, "_get_db_config", return_value=db_config)
        mocker.patch.object(pipeline, "_get_sinapi_config", return_value=sinapi_config)
        mocker.patch.object(
            pipeline,
            "_load_config",
            return_value={
                "secrets_path": "dummy",
                "default_year": sinapi_config["year"],
                "default_month": sinapi_config["month"],
            },
        )

        mocker.patch.object(
            pipeline, "_find_and_normalize_zip", return_value=MagicMock()
        )
        mocker.patch.object(pipeline, "_unzip_file", return_value=extraction_path)
        # The _run_pre_processing method now calls convert_excel_sheets_to_csv,
        # so we mock the underlying function directly.
        # We also need to ensure _run_pre_processing is called with the correct arguments.
        # For simplicity, we'll mock the method itself and ensure it's called.
        mocker.patch.object(pipeline, "_run_pre_processing") # Keep this mock for the method call
        mocker.patch.object(pipeline, "_sync_catalog_status")

        yield (
            pipeline,
            mock_db_instance,
            mock_downloader_instance,
            mock_processor_instance,
            mock_convert_excel_sheets_to_csv, # Yield the new mock
            referencia_file_path # Yield the path for assertions
        )


def test_run_etl_success(mock_pipeline):
    """Testa o fluxo completo do ETL com sucesso."""
    pipeline, mock_db, _, mock_processor, mock_convert_excel_sheets_to_csv, referencia_file_path = mock_pipeline

    mock_processor.process_catalogo_e_precos.return_value = {
        "insumos": pd.DataFrame(
            {"codigo": ["1"], "descricao": ["a"], "unidade": ["un"]}
        ),
        "composicoes": pd.DataFrame(
            {"codigo": ["c1"], "descricao": ["ca"], "unidade": ["un"]}
        ),
    }
    mock_processor.process_composicao_itens.return_value = {
        "composicao_insumos": pd.DataFrame({"insumo_filho_codigo": ["1"]}),
        "composicao_subcomposicoes": pd.DataFrame(),
        "parent_composicoes_details": pd.DataFrame(
            {"codigo": ["c1"], "descricao": ["ca"], "unidade": ["un"]}
        ),
        "child_item_details": pd.DataFrame(
            {"codigo": ["1"], "tipo": ["INSUMO"], "descricao": ["a"], "unidade": ["un"]}
        ),
    }

    result = pipeline.run() # Capture the result

    mock_db.create_tables.assert_called_once()
    mock_processor.process_catalogo_e_precos.assert_called()
    assert mock_db.save_data.call_count > 0
    mock_convert_excel_sheets_to_csv.assert_called_once_with(
        xlsx_full_path=referencia_file_path,
        sheets_to_convert=['CSD', 'CCD', 'CSE'],
        output_dir=referencia_file_path.parent.parent / "csv_temp" # Adjust path as per etl_pipeline.py
    )

    assert result["status"] == "success"
    assert "populados com sucesso" in result["message"]
    assert "insumos" in result["tables_updated"]
    assert "composicoes" in result["tables_updated"]
    assert "composicao_insumos" in result["tables_updated"]
    assert "composicao_subcomposicoes" in result["tables_updated"]
    assert result["records_inserted"] > 0


def test_run_etl_download_error(mock_pipeline):
    """Testa falha no download."""
    pipeline, _, mock_downloader, _, _, _ = mock_pipeline # Unpack all yielded values

    pipeline._find_and_normalize_zip.return_value = None
    mock_downloader.get_sinapi_data.side_effect = DownloadError("Network error")

    result = pipeline.run() # Capture the result

    assert result["status"] == "failed"
    assert "Network error" in result["message"]
    assert result["tables_updated"] == []
    assert result["records_inserted"] == 0


def test_run_etl_processing_error(mock_pipeline):
    """Testa falha no processamento."""
    pipeline, _, _, mock_processor, _, _ = mock_pipeline # Unpack all yielded values

    mock_processor.process_catalogo_e_precos.side_effect = ProcessingError(
        "Invalid format"
    )

    result = pipeline.run() # Capture the result

    assert result["status"] == "failed"
    assert "Invalid format" in result["message"]
    assert result["tables_updated"] == []
    assert result["records_inserted"] == 0


def test_run_etl_database_error(mock_pipeline):
    """Testa falha no banco de dados."""
    pipeline, mock_db, _, _, _, _ = mock_pipeline # Unpack all yielded values

    mock_db.create_tables.side_effect = DatabaseError("Connection failed")

    result = pipeline.run() # Capture the result

    assert result["status"] == "failed"
    assert "Connection failed" in result["message"]
    assert result["tables_updated"] == []
    assert result["records_inserted"] == 0
