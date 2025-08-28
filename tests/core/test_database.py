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
        db.engine = mock_engine
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
        
        assert db.engine is not None
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
    database.engine.connect.return_value.__enter__.return_value = mock_conn
    
    database.save_data(sample_df, 'test_table')
    
    mock_conn.execute.assert_called()
    assert mock_conn.execute.call_count >= 1

def test_save_data_failure(database, sample_df):
    """Testa falha no salvamento de dados."""
    mock_conn = Mock()
    mock_conn.execute.side_effect = SQLAlchemyError("Insert failed")
    database.engine.connect.return_value.__enter__.return_value = mock_conn
    
    with pytest.raises(DatabaseError, match="Erro ao salvar dados"):
        database.save_data(sample_df, 'test_table')

def test_infer_sql_types(database):
    """Testa inferência de tipos SQL."""
    df = pd.DataFrame({
        'int_col': [1, 2, 3],
        'float_col': [1.1, 2.2, 3.3],
        'str_col': ['a', 'b', 'c'],
        'bool_col': [True, False, True]
    })
    
    result = database._infer_sql_types(df)
    
    assert any('INTEGER' in t for t in result)
    assert any('NUMERIC' in t for t in result)
    assert any('VARCHAR' in t for t in result)
    assert len(result) == 4

def test_create_table(database):
    """Testa criação de tabela."""
    df = pd.DataFrame({
        'id': [1, 2],
        'name': ['A', 'B']
    })
    
    database.create_table('test_table', df)
    
    database.engine.execute.assert_called()
    call_args = database.engine.execute.call_args[0][0]
    assert 'CREATE TABLE' in str(call_args)
    assert 'test_table' in str(call_args)

def test_validate_data_new_table(database, sample_df):
    """Testa validação de dados para tabela nova."""
    database.table_exists = Mock(return_value=False)
    
    result = database.validate_data(sample_df, 'test_table')
    
    assert result is sample_df
    database.table_exists.assert_called_once()

def test_validate_data_existing_table_replace(database, sample_df):
    """Testa validação de dados com política de substituição."""
    database.table_exists = Mock(return_value=True)
    mock_existing_df = pd.DataFrame({'CODIGO': ['1234'], 'DESCRICAO': ['Old Product']})
    database.get_existing_data = Mock(return_value=mock_existing_df)
    
    result = database.validate_data(sample_df, 'test_table', policy='replace')
    
    assert len(result) == len(sample_df)
    database.get_existing_data.assert_called_once()

def test_validate_data_existing_table_append(database, sample_df):
    """Testa validação de dados com política de anexação."""
    database.table_exists = Mock(return_value=True)
    mock_existing_df = pd.DataFrame({'CODIGO': ['1234'], 'DESCRICAO': ['Old Product']})
    database.get_existing_data = Mock(return_value=mock_existing_df)
    
    result = database.validate_data(sample_df, 'test_table', policy='append')
    
    assert len(result) < len(sample_df)  # Deve remover registros duplicados
    database.get_existing_data.assert_called_once()

def test_backup_table(database):
    """Testa backup de tabela."""
    mock_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    database.get_table_data = Mock(return_value=mock_df)
    
    with patch('pandas.DataFrame.to_csv') as mock_to_csv:
        database.backup_table('test_table', '/backup/path')
        
        mock_to_csv.assert_called_once()
        database.get_table_data.assert_called_once_with('test_table')
