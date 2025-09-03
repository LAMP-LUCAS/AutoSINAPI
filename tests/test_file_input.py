"""
Testes do módulo de download com suporte a input direto de arquivo.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from autosinapi.etl_pipeline import PipelineETL


@pytest.fixture
def mock_pipeline(mocker, tmp_path):
    """Fixture para mockar o pipeline e suas dependências."""
    mocker.patch("autosinapi.etl_pipeline.setup_logging")

    # Cria um diretório de extração falso
    extraction_path = tmp_path / "extraction"
    extraction_path.mkdir()
    # Cria um arquivo de referência falso dentro do diretório
    referencia_file_path = extraction_path / "SINAPI_Referência_20_23_01.xlsx"
    referencia_file_path.touch()

    with patch("autosinapi.core.database.Database") as mock_db, patch(
        "autosinapi.core.downloader.Downloader"
    ) as mock_downloader, patch(
        "autosinapi.core.processor.Processor"
    ) as mock_processor, patch(
        "autosinapi.core.pre_processor.convert_excel_sheets_to_csv"
    ) as mock_convert_excel_sheets_to_csv:

        mock_db_instance = MagicMock()
        mock_db.return_value = mock_db_instance

        mock_downloader_instance = MagicMock()
        mock_downloader.return_value = mock_downloader_instance

        mock_processor_instance = MagicMock()
        mock_processor.return_value = mock_processor_instance

        pipeline = PipelineETL(config_path=None)

        mocker.patch.object(pipeline, "_run_pre_processing")
        mocker.patch.object(pipeline, "_sync_catalog_status")
        mocker.patch.object(
            pipeline, "_unzip_file", return_value=extraction_path
        )
        mocker.patch.object(
            pipeline, "_find_and_normalize_zip", return_value=Path("mocked.zip")
        )

        yield (
            pipeline,
            mock_db_instance,
            mock_downloader_instance,
            mock_processor_instance,
            mock_convert_excel_sheets_to_csv,
            referencia_file_path
        )


def test_direct_file_input(tmp_path, mock_pipeline):
    """Testa o pipeline com input direto de arquivo."""
    pipeline, mock_db, _, mock_processor, mock_convert_excel_sheets_to_csv, referencia_file_path = mock_pipeline

    test_file = tmp_path / "test_sinapi.xlsx"
    df = pd.DataFrame(
        {
            "codigo": [1234, 5678],
            "descricao": ["Item 1", "Item 2"],
            "unidade": ["un", "kg"],
            "preco": [10.5, 20.75],
        }
    )
    df.to_excel(test_file, index=False)

    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass",
    }
    sinapi_config = {
        "state": "SP",
        "month": "01",
        "year": "2023",
        "type": "insumos",
        "input_file": str(test_file),
    }

    with patch.object(
        pipeline,
        "_load_config",
        return_value={
            "secrets_path": "dummy_path",
            "default_year": "2023",
            "default_month": "01",
        },
    ):
        with patch.object(pipeline, "_get_db_config", return_value=db_config):
            with patch.object(
                pipeline, "_get_sinapi_config", return_value=sinapi_config
            ):
                mock_processor.process_catalogo_e_precos.return_value = {"insumos": df}
                mock_processor.process_composicao_itens.return_value = {
                    "composicao_insumos": pd.DataFrame(columns=["insumo_filho_codigo"]),
                    "composicao_subcomposicoes": pd.DataFrame(),
                    "parent_composicoes_details": pd.DataFrame(
                        columns=["codigo", "descricao", "unidade"]
                    ),
                    "child_item_details": pd.DataFrame(
                        columns=["codigo", "tipo", "descricao", "unidade"]
                    ),
                }

                result = pipeline.run() # Capture the result

    mock_processor.process_catalogo_e_precos.assert_called()
    mock_db.save_data.assert_called()
    assert result["status"] == "success"
    assert "populados com sucesso" in result["message"]
    assert result["records_inserted"] > 0
    mock_convert_excel_sheets_to_csv.assert_called_once_with(
        xlsx_full_path=referencia_file_path,
        sheets_to_convert=['CSD', 'CCD', 'CSE'],
        output_dir=referencia_file_path.parent.parent / "csv_temp"
    )


def test_fallback_to_download(mock_pipeline):
    """Testa o fallback para download quando arquivo não é fornecido."""
    pipeline, _, mock_downloader, _, _, _ = mock_pipeline

    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass",
    }
    sinapi_config = {"state": "SP", "month": "01", "year": "2023", "type": "insumos"}

    with patch.object(
        pipeline,
        "_load_config",
        return_value={
            "secrets_path": "dummy_path",
            "default_year": "2023",
            "default_month": "01",
        },
    ):
        with patch.object(pipeline, "_get_db_config", return_value=db_config):
            with patch.object(
                pipeline, "_get_sinapi_config", return_value=sinapi_config
            ):
                pipeline._find_and_normalize_zip.return_value = None

                result = pipeline.run() # Capture the result

    mock_downloader.get_sinapi_data.assert_called_once()
    assert result["status"] == "success"
    assert "populados com sucesso" in result["message"]
    assert result["records_inserted"] > 0


def test_invalid_input_file(mock_pipeline):
    """Testa erro ao fornecer arquivo inválido."""
    pipeline, _, _, _, _, _ = mock_pipeline

    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass",
    }
    sinapi_config = {
        "state": "SP",
        "month": "01",
        "year": "2023",
        "type": "insumos",
        "input_file": "arquivo_inexistente.xlsx",
    }

    with patch.object(
        pipeline,
        "_load_config",
        return_value={
            "secrets_path": "dummy_path",
            "default_year": "2023",
            "default_month": "01",
        },
    ):
        with patch.object(pipeline, "_get_db_config", return_value=db_config):
            with patch.object(
                pipeline, "_get_sinapi_config", return_value=sinapi_config
            ):
                pipeline._unzip_file.side_effect = FileNotFoundError(
                    "Arquivo não encontrado"
                )

                result = pipeline.run() # Capture the result

    assert result["status"] == "failed"
    assert "Arquivo não encontrado" in result["message"]
    assert result["tables_updated"] == []
    assert result["records_inserted"] == 0


def test_fallback_to_download(mock_pipeline):
    """Testa o fallback para download quando arquivo não é fornecido."""
    pipeline, _, mock_downloader, _ = mock_pipeline

    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass",
    }
    sinapi_config = {"state": "SP", "month": "01", "year": "2023", "type": "insumos"}

    with patch.object(
        pipeline,
        "_load_config",
        return_value={
            "secrets_path": "dummy_path",
            "default_year": "2023",
            "default_month": "01",
        },
    ):
        with patch.object(pipeline, "_get_db_config", return_value=db_config):
            with patch.object(
                pipeline, "_get_sinapi_config", return_value=sinapi_config
            ):
                pipeline._find_and_normalize_zip.return_value = None

                pipeline.run()

    mock_downloader.get_sinapi_data.assert_called_once()


def test_invalid_input_file(mock_pipeline, caplog):
    """Testa erro ao fornecer arquivo inválido."""
    pipeline, _, _, _ = mock_pipeline

    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass",
    }
    sinapi_config = {
        "state": "SP",
        "month": "01",
        "year": "2023",
        "type": "insumos",
        "input_file": "arquivo_inexistente.xlsx",
    }

    with patch.object(
        pipeline,
        "_load_config",
        return_value={
            "secrets_path": "dummy_path",
            "default_year": "2023",
            "default_month": "01",
        },
    ):
        with patch.object(pipeline, "_get_db_config", return_value=db_config):
            with patch.object(
                pipeline, "_get_sinapi_config", return_value=sinapi_config
            ):
                pipeline._unzip_file.side_effect = FileNotFoundError(
                    "Arquivo não encontrado"
                )

                pipeline.run()

    assert "Arquivo não encontrado" in caplog.text
