"""
Testes de integracao para o pipeline principal do AutoSINAPI.
"""
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
from autosinapi.exceptions import DatabaseError, DownloadError, ProcessingError
from autosinapi.etl_pipeline import PipelineETL


@pytest.fixture
def mock_pipeline(mocker, tmp_path):
    """Fixture para mockar o pipeline e suas dependencias."""
    mocker.patch("autosinapi.etl_pipeline.setup_logging")
    extraction_path = tmp_path / "extraction"
    extraction_path.mkdir()

    with patch("autosinapi.etl_pipeline.Database") as mock_db, \
         patch("autosinapi.etl_pipeline.Downloader") as mock_downloader, \
         patch("autosinapi.etl_pipeline.Processor") as mock_processor, \
         patch("autosinapi.etl_pipeline.convert_excel_sheets_to_csv") as mock_convert:

        mock_db_instance = MagicMock()
        mock_db.return_value = mock_db_instance
        # Correctly mock context manager
        mock_db_instance.__enter__.return_value = mock_db_instance

        mocker.patch("autosinapi.etl_pipeline.PipelineETL._get_db_config",
                      return_value={"host": "localhost", "port": 5432, "database": "test_db",
                                    "user": "test_user", "password": "test_pass"})
        mocker.patch("autosinapi.etl_pipeline.PipelineETL._get_sinapi_config",
                      return_value={"state": "SP", "year": 2025, "month": 8, "type": "REFERENCIA"})
        mocker.patch("autosinapi.etl_pipeline.PipelineETL._load_base_config",
                      return_value={"secrets_path": "dummy", "default_year": 2025, "default_month": 8})

        pipeline = PipelineETL(run_id="test-run", config_path=None)

        yield pipeline, mock_db_instance, mock_processor, mock_downloader, mock_convert, extraction_path


class TestSinapiVersionExtraction:
    def test_extract_version_from_reference_file(self, mock_pipeline):
        pipeline, _, _, _, _, _ = mock_pipeline
        assert pipeline.extract_sinapi_version("SINAPI_Referencia_2024_01.xlsx") == "2024.01"

    def test_extract_version_fallback(self, mock_pipeline):
        pipeline, _, _, _, _, _ = mock_pipeline
        pipeline.config.YEAR = 2023
        pipeline.config.MONTH = 12
        assert pipeline.extract_sinapi_version("invalido.txt") == "2023.12"


class TestRunETL:
    def test_run_etl_success(self, mock_pipeline):
        pipeline, mock_db, mock_processor, mock_downloader, mock_convert, extraction_path = mock_pipeline

        # Create reference file so pipeline finds it
        ref_file = extraction_path / "SINAPI_Referência_2025_08.xlsx"
        ref_file.touch()
        
        mock_downloader.return_value.get_sinapi_data.return_value = (str(ref_file), {})

        mock_processor.return_value.process_catalogo_e_precos.return_value = {
            "insumos": pd.DataFrame({"codigo": [1], "descricao": ["a"], "unidade": ["un"], "classificacao": ["c"]}),
            "composicoes": pd.DataFrame({"codigo": [2], "descricao": ["b"], "unidade": ["un"], "grupo": ["g"]}),
        }
        mock_processor.return_value.process_composicao_itens.return_value = {
            "composicao_insumos": pd.DataFrame({"composicao_pai_codigo": [2], "insumo_filho_codigo": [1]}),
            "composicao_subcomposicoes": pd.DataFrame(columns=["composicao_pai_codigo", "composicao_filho_codigo"]),
            "parent_composicoes_details": pd.DataFrame({"codigo": [2], "descricao": ["b"], "unidade": ["un"], "grupo": ["g"]}),
            "child_item_details": pd.DataFrame({"codigo": [1], "tipo": ["INSUMO"], "descricao": ["a"], "unidade": ["un"]}),
        }

        result = pipeline.run()
        
        assert result["status"] == pipeline.config.STATUS_SUCCESS
        assert mock_db.save_data.called
        mock_db.register_audit_log.assert_called_once()

    def test_run_etl_processing_error(self, mock_pipeline):
        pipeline, _, mock_processor, mock_downloader, _, extraction_path = mock_pipeline
        
        ref_file = extraction_path / "SINAPI_Referência_2025_08.xlsx"
        ref_file.touch()
        mock_downloader.return_value.get_sinapi_data.return_value = (str(ref_file), {})
        mock_processor.return_value.process_catalogo_e_precos.side_effect = ProcessingError("Invalid")

        result = pipeline.run()
        assert result["status"] == pipeline.config.STATUS_FAILURE
        assert "Invalid" in result["message"]

    def test_run_etl_database_error(self, mock_pipeline):
        pipeline, mock_db, _, mock_downloader, _, extraction_path = mock_pipeline
        
        ref_file = extraction_path / "SINAPI_Referência_2025_08.xlsx"
        ref_file.touch()
        mock_downloader.return_value.get_sinapi_data.return_value = (str(ref_file), {})
        mock_db.check_tables.side_effect = DatabaseError("Connection failed")

        result = pipeline.run()
        assert result["status"] == pipeline.config.STATUS_FAILURE
        assert "Connection failed" in result["message"]
