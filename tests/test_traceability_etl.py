"""
Testes de traceability para o ETL Pipeline.
"""
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
from autosinapi.etl_pipeline import PipelineETL


@pytest.fixture
def mock_pipeline(mocker, tmp_path):
    """Fixture para mockar o pipeline."""
    mocker.patch("autosinapi.etl_pipeline.setup_logging")
    extraction_path = tmp_path / "extraction"
    extraction_path.mkdir()

    with patch("autosinapi.etl_pipeline.Database") as mock_db, \
         patch("autosinapi.etl_pipeline.Downloader") as mock_downloader, \
         patch("autosinapi.etl_pipeline.Processor") as mock_processor, \
         patch("autosinapi.etl_pipeline.convert_excel_sheets_to_csv"):

        mock_db_instance = MagicMock()
        mock_db.return_value = mock_db_instance
        mock_db_instance.__enter__.return_value = mock_db_instance

        mocker.patch(
            "autosinapi.etl_pipeline.PipelineETL._get_db_config",
            return_value={"host": "test", "port": 5432, "database": "test", "user": "test", "password": "test"}
        )
        mocker.patch(
            "autosinapi.etl_pipeline.PipelineETL._get_sinapi_config",
            return_value={"state": "SP", "year": 2024, "month": 1, "type": "REFERENCIA"}
        )
        mocker.patch(
            "autosinapi.etl_pipeline.PipelineETL._load_base_config",
            return_value={"secrets_path": "dummy", "default_year": 2024, "default_month": 1}
        )

        pipeline = PipelineETL(run_id="test-run", config_path=None)

        # Mock Downloader to skip real download
        mock_downloader_instance = mock_downloader.return_value
        mock_downloader_instance.get_sinapi_data.return_value = (str(extraction_path / "SINAPI_Ref.xlsx"), {})

        yield pipeline, mock_db_instance, mock_processor, extraction_path


class TestDeleteByPeriod:
    def test_execute_phase_3_uses_correct_save_calls(self, mock_pipeline):
        pipeline, mock_db, mock_processor, extraction_path = mock_pipeline

        # Create dummy reference file
        ref_file = extraction_path / "SINAPI_Referência_2024_01.xlsx"
        ref_file.touch()

        mock_processor.return_value.process_catalogo_e_precos.return_value = {
            "insumos": pd.DataFrame({"codigo": [1001], "descricao": ["A"], "unidade": ["m3"], "classificacao": ["c"]}),
            "precos_insumos_mensal": pd.DataFrame(),
            "custos_composicoes_mensal": pd.DataFrame(),
        }

        mock_processor.return_value.process_composicao_itens.return_value = {
            "composicao_insumos": pd.DataFrame({
                "composicao_pai_codigo": [2001], "insumo_filho_codigo": [1001],
                "coeficiente": [1.5],
            }),
            "composicao_subcomposicoes": pd.DataFrame(columns=["composicao_pai_codigo", "composicao_filho_codigo"]),
            "parent_composicoes_details": pd.DataFrame({"codigo": [2001], "descricao": ["Comp"], "unidade": ["UN"], "grupo": ["g"]}),
            "child_item_details": pd.DataFrame({"codigo": [1001], "tipo": ["INSUMO"], "descricao": ["A"], "unidade": ["m3"]}),
        }

        pipeline.run()

        # Check if save_data was called for main tables
        assert mock_db.save_data.called
        # Check audit log registration
        mock_db.register_audit_log.assert_called_once()


class TestExtractSinapiVersion:
    def test_extract_version_from_filename(self, mock_pipeline):
        pipeline, _, _, _ = mock_pipeline
        assert pipeline.extract_sinapi_version("SINAPI_Referencia_2024_01.xlsx") == "2024.01"

    def test_extract_version_fallback(self, mock_pipeline):
        pipeline, _, _, _ = mock_pipeline
        pipeline.config.YEAR = 2023
        pipeline.config.MONTH = 12
        assert pipeline.extract_sinapi_version("arquivo.txt") == "2023.12"
