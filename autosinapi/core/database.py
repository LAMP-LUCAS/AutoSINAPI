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
        As tabelas são criadas na ordem correta para garantir integridade referencial.
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
        except Exception as e:
            raise DatabaseError(f"Erro ao criar tabelas: {str(e)}")
    # ...existing code...
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        Inicializa a conexão com o banco de dados.
        
        Args:
            db_config: Configurações do banco de dados
        """
        self.config = db_config
        self._engine = self._create_engine()
    
    def _create_engine(self) -> Engine:
        """Cria a engine do SQLAlchemy."""
        try:
            url = (f"postgresql://{self.config['user']}:{self.config['password']}"
                  f"@{self.config['host']}:{self.config['port']}"
                  f"/{self.config['database']}")
            return create_engine(url)
        except Exception as e:
            raise DatabaseError("Erro ao conectar com o banco de dados")
    
    def save_data(self, data: pd.DataFrame, table_name: str) -> None:
        """
        Salva os dados no banco.
        
        Args:
            data: DataFrame com os dados a serem salvos
            table_name: Nome da tabela
        
        Raises:
            DatabaseError: Se houver erro na operação
        """
        try:
            data.to_sql(
                name=table_name,
                con=self._engine,
                if_exists='append',
                index=False
            )
        except Exception as e:
            raise DatabaseError(f"Erro ao salvar dados: {str(e)}")
    
    def execute_query(self, query: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        """
        Executa uma query no banco.
        
        Args:
            query: Query SQL
            params: Parâmetros da query
        
        Returns:
            DataFrame: Resultado da query
        
        Raises:
            DatabaseError: Se houver erro na execução
        """
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            raise DatabaseError(f"Erro ao executar query: {str(e)}")
    
    def __enter__(self):
        """Permite uso do contexto 'with'."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fecha a conexão ao sair do contexto."""
        self._engine.dispose()
