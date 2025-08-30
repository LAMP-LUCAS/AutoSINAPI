"""
Módulo responsável pelas operações de banco de dados.
"""
from typing import Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from autosinapi.exceptions import DatabaseError

class Database:
    def create_tables(self):
        """
        Cria as tabelas do modelo de dados do SINAPI no banco PostgreSQL.
        """
        ddl = """
        CREATE TABLE IF NOT EXISTS insumos (
            codigo INTEGER PRIMARY KEY,
            descricao TEXT NOT NULL,
            unidade VARCHAR(20),
            status VARCHAR(20) DEFAULT 'ATIVO'
        );

        CREATE TABLE IF NOT EXISTS composicoes (
            codigo INTEGER PRIMARY KEY,
            descricao TEXT NOT NULL,
            unidade VARCHAR(20),
            grupo VARCHAR(50),
            status VARCHAR(20) DEFAULT 'ATIVO'
        );

        CREATE TABLE IF NOT EXISTS manutencoes_historico (
            item_codigo INTEGER NOT NULL,
            tipo_item VARCHAR(20) NOT NULL,
            data_referencia DATE NOT NULL,
            tipo_manutencao VARCHAR(30) NOT NULL,
            descricao_nova TEXT,
            PRIMARY KEY (item_codigo, tipo_item, data_referencia, tipo_manutencao)
        );

        CREATE TABLE IF NOT EXISTS composicao_itens (
            composicao_pai_codigo INTEGER NOT NULL,
            item_codigo INTEGER NOT NULL,
            tipo_item VARCHAR(20) NOT NULL,
            coeficiente NUMERIC,
            PRIMARY KEY (composicao_pai_codigo, item_codigo, tipo_item),
            FOREIGN KEY (composicao_pai_codigo) REFERENCES composicoes(codigo)
        );

        CREATE TABLE IF NOT EXISTS precos_insumos_mensal (
            insumo_codigo INTEGER NOT NULL,
            uf VARCHAR(2) NOT NULL,
            data_referencia DATE NOT NULL,
            desonerado BOOLEAN NOT NULL,
            preco_mediano NUMERIC,
            PRIMARY KEY (insumo_codigo, uf, data_referencia, desonerado),
            FOREIGN KEY (insumo_codigo) REFERENCES insumos(codigo)
        );

        CREATE TABLE IF NOT EXISTS custos_composicoes_mensal (
            composicao_codigo INTEGER NOT NULL,
            uf VARCHAR(2) NOT NULL,
            data_referencia DATE NOT NULL,
            desonerado BOOLEAN NOT NULL,
            custo_total NUMERIC,
            percentual_mao_de_obra NUMERIC,
            PRIMARY KEY (composicao_codigo, uf, data_referencia, desonerado)
        );
        """
        try:
            with self._engine.connect() as conn:
                for stmt in ddl.split(';'):
                    if stmt.strip():
                        conn.execute(text(stmt))
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Erro ao criar tabelas: {str(e)}")

    def __init__(self, db_config: Dict[str, Any]):
        self.config = db_config
        self._engine = self._create_engine()
    
    def _create_engine(self) -> Engine:
        try:
            url = (f"postgresql://{self.config['user']}:{self.config['password']}"
                  f"@{self.config['host']}:{self.config['port']}"
                  f"/{self.config['database']}")
            return create_engine(url)
        except Exception as e:
            raise DatabaseError("Erro ao conectar com o banco de dados")
    
    def save_data(self, data: pd.DataFrame, table_name: str, policy: str, year: str, month: str) -> None:
        """
        Salva os dados no banco, aplicando a política de duplicatas.
        """
        if policy.lower() == 'substituir':
            self._replace_data(data, table_name, year, month)
        elif policy.lower() == 'append':
            self._append_data(data, table_name)
        else:
            raise DatabaseError(f"Política de duplicatas desconhecida: {policy}")

    def _append_data(self, data: pd.DataFrame, table_name: str):
        try:
            data.to_sql(name=table_name, con=self._engine, if_exists='append', index=False)
        except Exception as e:
            raise DatabaseError(f"Erro ao salvar dados: {str(e)}")

    def _replace_data(self, data: pd.DataFrame, table_name: str, year: str, month: str):
        """Substitui os dados de um determinado período."""
        # Adiciona a data de referência para o delete
        data_referencia = f'{year}-{month}-01'
        delete_query = text(f"DELETE FROM {table_name} WHERE TO_CHAR(data_referencia, 'YYYY-MM') = :ref")
        
        with self._engine.connect() as conn:
            trans = conn.begin()
            try:
                conn.execute(delete_query, {"ref": f"{year}-{month}"})
                data.to_sql(name=table_name, con=conn, if_exists='append', index=False)
                trans.commit()
            except Exception as e:
                trans.rollback()
                raise DatabaseError(f"Erro ao substituir dados: {str(e)}")

    def execute_query(self, query: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            raise DatabaseError(f"Erro ao executar query: {str(e)}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._engine.dispose()