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

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tools.autosinapi_pipeline import Pipeline


@pytest.fixture
def mock_pipeline(mocker, tmp_path):
    """Fixture para mockar o pipeline e suas dependências."""
    mocker.patch("tools.autosinapi_pipeline.setup_logging")

    # Cria um diretório de extração falso
    extraction_path = tmp_path / "extraction"
    extraction_path.mkdir()
    # Cria um arquivo de referência falso dentro do diretório
    (extraction_path / "SINAPI_Referência_2023_01.xlsx").touch()

    with patch("tools.autosinapi_pipeline.Database") as mock_db:
        mock_db_instance = MagicMock()
        mock_db.return_value = mock_db_instance

        with patch("tools.autosinapi_pipeline.Downloader") as mock_downloader:
            mock_downloader_instance = MagicMock()
            mock_downloader.return_value = mock_downloader_instance

            with patch("tools.autosinapi_pipeline.Processor") as mock_processor:
                mock_processor_instance = MagicMock()
                mock_processor.return_value = mock_processor_instance

                pipeline = Pipeline(config_path=None)

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
                )


def test_direct_file_input(tmp_path, mock_pipeline):
    """Testa o pipeline com input direto de arquivo."""
    pipeline, mock_db, _, mock_processor = mock_pipeline

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

                pipeline.run()

    mock_processor.process_catalogo_e_precos.assert_called()
    mock_db.save_data.assert_called()


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
