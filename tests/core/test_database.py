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
    return {"host": "localhost", "port": 5432, "database": "test_db", "user": "test_user", "password": "test_pass"}

@pytest.fixture
def sinapi_config():
    return {"state": "SP", "month": "01", "year": "2023", "type": "REFERENCIA"}

@pytest.fixture
def database(db_config, sinapi_config):
    with patch("autosinapi.core.database.create_engine") as mock_ce:
        mock_engine = MagicMock()
        mock_ce.return_value = mock_engine
        config = Config(db_config, sinapi_config, mode="server")
        db = Database(config)
        db._engine = mock_engine
        yield db, mock_engine

@pytest.fixture
def sample_df():
    return pd.DataFrame({"CODIGO": [1234, 5678], "DESCRICAO": ["Produto A", "Produto B"], "PRECO": [100.0, 200.0]})

@pytest.fixture
def sample_df_with_trace():
    return pd.DataFrame({
        "codigo": [1001, 1002], "descricao": ["Insumo A", "Insumo B"],
        "unidade": ["m3", "kg"], "sinapi_versao": [None, None],
        "etl_run_id": [None, None], "created_at": [None, None], "updated_at": [None, None],
    })

def test_connect_success(db_config, sinapi_config):
    with patch("autosinapi.core.database.create_engine") as mock_ce:
        mock_engine = MagicMock()
        mock_ce.return_value = mock_engine
        config = Config(db_config, sinapi_config, mode="server")
        db = Database(config)
        assert db._engine is not None
        mock_ce.assert_called_once()

def test_connect_failure(db_config, sinapi_config):
    with patch("autosinapi.core.database.create_engine") as mock_ce:
        mock_ce.side_effect = SQLAlchemyError("Connection failed")
        with pytest.raises(DatabaseError, match="Erro ao conectar"):
            config = Config(db_config, sinapi_config, mode="server")
            Database(config)

def test_save_data_success(database, sample_df):
    db, mock_engine = database
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    # Mocking pandas to_sql to avoid engine issues
    with patch("pandas.DataFrame.to_sql"):
        db.save_data(sample_df, "test_table", policy="append")
        # Just check it didn't crash and attempted some database interaction if policy needed it
        # Since _append_data calls to_sql, we check if to_sql was called
        assert pd.DataFrame.to_sql.called

class TestTraceabilityPropagation:
    def test_save_data_propagates_sinapi_versao(self, database, sample_df_with_trace):
        db, mock_engine = database
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        df = sample_df_with_trace.copy()
        with patch("pandas.DataFrame.to_sql"):
            db.save_data(df, "insumos", policy="append", sinapi_versao="2024.01", etl_run_id="test-run-123")
            assert df["sinapi_versao"].iloc[0] == "2024.01"
            # etl_run_id is converted to UUID object string
            assert "3ac0759c-5a1d-5d31-b450-df6bfb133a37" in str(df["etl_run_id"].iloc[0])

    def test_save_data_adds_missing_traceability_columns(self, database):
        db, mock_engine = database
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        df = pd.DataFrame({"codigo": [1001], "descricao": ["Insumo A"]})
        with patch("pandas.DataFrame.to_sql"):
            db.save_data(df, "insumos", policy="append", sinapi_versao="2024.01", etl_run_id="test-run-123")
            assert "sinapi_versao" in df.columns
            assert df["sinapi_versao"].iloc[0] == "2024.01"

class TestAuditLog:
    def test_register_audit_log_inserts_correctly(self, database):
        db, mock_engine = database
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        db.register_audit_log(
            run_id="test-run-123", data_ref="2024.01", 
            records=100, tables=["insumos", "precos"]
        )

        assert mock_conn.execute.called
        call_str = str(mock_conn.execute.call_args[0][0])
        assert "sinapi_audit_log" in call_str

    def test_register_audit_log_uses_etl_run_log_schema(self, database):
        """
        Issue 3: Verifica que o INSERT usa o schema ETL-run-log
        (run_id, data_referencia, records_inserted, tables_updated)
        e NÃO o schema audit-trail (id, table_name, record_pk, operation).
        """
        db, mock_engine = database
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        db.register_audit_log(
            run_id="run-abc-123", data_ref="2026.05",
            records=362891, tables=["insumos", "precos_insumos_mensal"]
        )

        assert mock_conn.execute.called
        call_str = str(mock_conn.execute.call_args[0][0])

        # Colunas DO ETL-run-log schema (devem estar presentes)
        assert "run_id" in call_str, "ETL-run-log schema requer coluna 'run_id'"
        assert "data_referencia" in call_str, "ETL-run-log schema requer 'data_referencia'"
        assert "records_inserted" in call_str, "ETL-run-log schema requer 'records_inserted'"
        assert "tables_updated" in call_str, "ETL-run-log schema requer 'tables_updated'"

        # Colunas do audit-trail schema (NÃO devem estar presentes)
        assert "record_pk" not in call_str, "Schema incorreto: não deve ter 'record_pk' (é audit-trail)"
        assert "old_values" not in call_str, "Schema incorreto: não deve ter 'old_values' (é audit-trail)"
        assert "new_values" not in call_str, "Schema incorreto: não deve ter 'new_values' (é audit-trail)"
        assert "table_name" not in call_str, "Schema incorreto: não deve ter 'table_name' (é audit-trail)"
        assert "motivo_manutencao" not in call_str, "Schema incorreto: não deve ter 'motivo_manutencao'"
        assert "operation" not in call_str, "Schema incorreto: não deve ter 'operation' (é audit-trail)"

    @patch("pandas.DataFrame.to_sql")
    def test_upsert_precos_is_idempotent(self, mock_to_sql, database):
        """
        Issue UPSERT: Verifica que save_data com policy UPSERT em
        precos_insumos_mensal não levanta PK violation na segunda execução.
        """
        import pandas as pd
        db, mock_engine = database
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Simula execução do UPSERT (o mock do to_sql + conn.execute)
        mock_conn.execute.return_value = None

        df = pd.DataFrame({
            "insumo_codigo": [1, 2],
            "uf": ["SP", "SP"],
            "data_referencia": pd.Timestamp("2026-05-01"),
            "regime": ["NAO_DESONERADO", "NAO_DESONERADO"],
            "preco_mediano": [100.0, 200.0],
        })

        # Primeira execução (UPSERT - deve inserir)
        db.save_data(df.copy(), "precos_insumos_mensal", policy="upsert",
                     pk_columns=["insumo_codigo", "uf", "data_referencia", "regime"],
                     sinapi_versao="2026.05", etl_run_id="run-001")
        assert mock_to_sql.called

        # Segunda execução com os mesmos dados (UPSERT - deve atualizar, sem erro)
        mock_to_sql.reset_mock()
        try:
            db.save_data(df.copy(), "precos_insumos_mensal", policy="upsert",
                         pk_columns=["insumo_codigo", "uf", "data_referencia", "regime"],
                         sinapi_versao="2026.05", etl_run_id="run-002")
            idempotent = True
        except Exception:
            idempotent = False

        assert idempotent, (
            "UPSERT não é idempotente! Segunda execução com mesmos PKs "
            "não deveria levantar exceção."
        )
