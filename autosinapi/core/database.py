"""
Módulo responsável pelas operações de banco de dados.
"""
from typing import Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from .exceptions import DatabaseError

class Database:
    """Classe responsável pelas operações de banco de dados."""
    
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
            raise DatabaseError(f"Erro ao criar conexão: {str(e)}")
    
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
