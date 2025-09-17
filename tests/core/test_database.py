"""
Testes unitários para o módulo database.py
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy.exc import SQLAlchemyError

from autosinapi.config import Config
from autosinapi.core.database import Database
from autosinapi.exceptions import DatabaseError


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
    return {"state": "SP", "month": "01", "year": "2023", "type": "REFERENCIA"}


@pytest.fixture
def database(db_config, sinapi_config):
    """Fixture que cria uma instância do Database com engine mockada."""
    with patch("autosinapi.core.database.create_engine") as mock_create_engine:
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        config = Config(db_config, sinapi_config, mode="server")
        db = Database(config)
        db._engine = mock_engine
        yield db, mock_engine


@pytest.fixture
def sample_df():
    """Fixture que cria um DataFrame de exemplo."""
    return pd.DataFrame(
        {
            "CODIGO": ["1234", "5678"],
            "DESCRICAO": ["Produto A", "Produto B"],
            "PRECO": [100.0, 200.0],
        }
    )


def test_connect_success(db_config, sinapi_config):
    """Testa conexão bem-sucedida com o banco."""
    with patch("autosinapi.core.database.create_engine") as mock_create_engine:
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        config = Config(db_config, sinapi_config, mode="server")
        db = Database(config)
        assert db._engine is not None
        mock_create_engine.assert_called_once()


def test_connect_failure(db_config, sinapi_config):
    """Testa falha na conexão com o banco."""
    with patch("autosinapi.core.database.create_engine") as mock_create_engine:
        mock_create_engine.side_effect = SQLAlchemyError("Connection failed")
        with pytest.raises(DatabaseError, match="Erro ao conectar"):
            config = Config(db_config, sinapi_config, mode="server")
            Database(config)


def test_save_data_success(database, sample_df):
    """Testa salvamento bem-sucedido de dados."""
    db, mock_engine = database
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    db.save_data(sample_df, "test_table", policy="append")

    assert mock_conn.execute.call_count > 0


@pytest.mark.filterwarnings("ignore:pandas only supports SQLAlchemy")
def test_save_data_failure(database, sample_df):
    """Testa falha no salvamento de dados."""
    db, mock_engine = database
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = SQLAlchemyError("Insert failed")
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    with pytest.raises(DatabaseError, match="Erro ao inserir dados"):
        db.save_data(sample_df, "test_table", policy="append")