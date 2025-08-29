"""
Testes unitários para o módulo database.py
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from autosinapi.core.database import Database
from autosinapi.exceptions import DatabaseError

@pytest.fixture
def db_config():
    """Fixture com configuração de teste do banco de dados."""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass'
    }

@pytest.fixture
def database(db_config):
    """Fixture que cria uma instância do Database com engine mockada."""
    with patch('sqlalchemy.create_engine') as mock_create_engine:
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        db = Database(db_config)
        db._engine = mock_engine
        yield db

@pytest.fixture
def sample_df():
    """Fixture que cria um DataFrame de exemplo."""
    return pd.DataFrame({
        'CODIGO': ['1234', '5678'],
        'DESCRICAO': ['Produto A', 'Produto B'],
        'PRECO': [100.0, 200.0]
    })

def test_connect_success(db_config):
    """Testa conexão bem-sucedida com o banco."""
    with patch('sqlalchemy.create_engine') as mock_create_engine:
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        db = Database(db_config)
        assert db._engine is not None
        mock_create_engine.assert_called_once()

def test_connect_failure(db_config):
    """Testa falha na conexão com o banco."""
    with patch('sqlalchemy.create_engine') as mock_create_engine:
        mock_create_engine.side_effect = SQLAlchemyError("Connection failed")
        with pytest.raises(DatabaseError, match="Erro ao conectar"):
            Database(db_config)

def test_save_data_success(database, sample_df):
    """Testa salvamento bem-sucedido de dados."""
    mock_conn = Mock()
    database._engine.connect.return_value.__enter__.return_value = mock_conn
    database.save_data(sample_df, 'test_table')
    mock_conn.execute.assert_called()

def test_save_data_failure(database, sample_df):
    """Testa falha no salvamento de dados."""
    mock_conn = Mock()
    mock_conn.execute.side_effect = SQLAlchemyError("Insert failed")
    database._engine.connect.return_value.__enter__.return_value = mock_conn
    with pytest.raises(DatabaseError, match="Erro ao salvar dados"):
        database.save_data(sample_df, 'test_table')
