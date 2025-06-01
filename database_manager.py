"""
Módulo para gerenciamento do banco de dados SINAPI
"""
import sqlalchemy
from sqlalchemy import create_engine, text
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime, timedelta
import time
from tqdm import tqdm
from sinapi_utils_new import SinapiLogger
from pathlib import Path

class DatabaseManager:
    """Classe para gerenciar operações de banco de dados"""
    
    def __init__(self, connection_string: str, log_level: str = 'info'):
        """
        Inicializa o gerenciador de banco de dados
        Args:
            connection_string: String de conexão SQLAlchemy
            log_level: Nível de log
        """
        self.engine = create_engine(connection_string)
        self.logger = SinapiLogger("DatabaseManager", log_level)
    
    def create_database(self, db_name: str) -> bool:
        """
        Cria um banco de dados se não existir
        Args:
            db_name: Nome do banco de dados
        Returns:
            bool: True se criado/existente com sucesso
        """
        self.logger.log('info', f"Verificando banco de dados '{db_name}'...")
        try:
            with self.engine.connect() as conn:
                conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                    text(f"CREATE DATABASE {db_name}")
                )
            self.logger.log('info', f"Banco '{db_name}' criado com sucesso")
            return True
        except sqlalchemy.exc.ProgrammingError as e:
            if 'already exists' in str(e):
                self.logger.log('info', f"Banco '{db_name}' já existe")
                return True
            self.logger.log('error', f"Erro ao criar banco '{db_name}': {e}")
            raise
        except Exception as e:
            self.logger.log('error', f"Erro inesperado: {e}")
            return False

    def create_schemas(self, schemas: List[str]) -> None:
        """
        Cria esquemas se não existirem
        Args:
            schemas: Lista de esquemas
        """
        self.logger.log('info', "Verificando esquemas...")
        for schema in schemas:
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                    conn.commit()
                self.logger.log('debug', f"Schema '{schema}' verificado")
            except Exception as e:
                self.logger.log('error', f"Erro no schema '{schema}': {e}")
        self.logger.log('info', f"Esquemas verificados: {', '.join(schemas)}")

    def create_table(self, schema: str, table: str, columns: List[str], types: List[str]) -> None:
        """
        Cria uma tabela se não existir
        Args:
            schema: Nome do esquema
            table: Nome da tabela
            columns: Lista de colunas
            types: Lista de tipos SQL
        """
        table_name = f"{schema}.{table}"
        self.logger.log('info', f"Verificando tabela '{table_name}'...")
        
        ddl = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(f'{col} {typ}' for col, typ in zip(columns, types))}
        )"""
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(ddl))
                conn.commit()
            self.logger.log('info', f"Tabela '{table}' verificada")
        except Exception as e:
            self.logger.log('error', f"Erro na tabela '{table}': {e}")
            raise

    def insert_data(self, schema: str, table: str, df: pd.DataFrame, batch_size: int = 1000) -> None:
        """
        Insere dados em uma tabela com validação e progresso
        Args:
            schema: Nome do esquema
            table: Nome da tabela
            df: DataFrame com os dados
            batch_size: Tamanho do lote
        """
        table_name = f"{schema}.{table}"
        total_rows = len(df)
        start_time = time.time()
        
        self.logger.log('info', f"Inserindo {total_rows:,} registros em {table_name}...")
        
        with self.engine.connect() as conn:
            with tqdm(total=total_rows, desc="Inserindo dados", unit="reg") as pbar:
                for i in range(0, total_rows, batch_size):
                    batch_start = time.time()
                    batch_df = df.iloc[i:i + batch_size]
                    
                    try:
                        self._insert_batch(conn, table_name, batch_df)
                        conn.commit()
                    except Exception as e:
                        self.logger.log('error', f"Erro no lote {i//batch_size + 1}: {e}")
                        continue
                    
                    batch_time = time.time() - batch_start
                    records = len(batch_df)
                    elapsed = time.time() - start_time
                    rate = records / batch_time if batch_time > 0 else 0
                    
                    pbar.update(records)
                    pbar.set_postfix({
                        "Tempo lote": f"{batch_time:.1f}s",
                        "Total": f"{elapsed:.1f}s",
                        "Reg/s": f"{rate:.0f}"
                    })
        
        total_time = timedelta(seconds=int(time.time() - start_time))
        self.logger.log('info', f"Inserção concluída em {total_time}")

    def _insert_batch(self, conn: sqlalchemy.engine.Connection, table_name: str, 
                     batch_df: pd.DataFrame) -> None:
        """
        Insere um lote de dados na tabela
        Args:
            conn: Conexão SQLAlchemy
            table_name: Nome completo da tabela (schema.tabela)
            batch_df: DataFrame com os dados do lote
        """
        # Prepara os dados para inserção
        data = batch_df.to_dict(orient='records')
        if not data:
            return
            
        # Constrói a query de inserção
        columns = list(data[0].keys())
        placeholders = ', '.join([':' + col for col in columns])
        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({placeholders})
        """
        
        # Executa a inserção
        conn.execute(text(query), data)
    
    def execute_query(self, query: str, params: Dict = None) -> pd.DataFrame:
        """
        Executa uma query SQL e retorna os resultados
        Args:
            query: Query SQL
            params: Parâmetros da query
        Returns:
            DataFrame: Resultados da query
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            self.logger.log('error', f"Erro na query: {e}")
            raise
    
    def backup_table(self, schema: str, table: str, backup_dir: Path) -> None:
        """
        Faz backup dos dados de uma tabela em CSV
        Args:
            schema: Nome do esquema
            table: Nome da tabela
            backup_dir: Diretório para salvar o backup
        """
        table_name = f"{schema}.{table}"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_dir / f"{schema}_{table}_{timestamp}.csv"
        
        try:
            # Cria diretório se não existir
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Recupera os dados
            query = f"SELECT * FROM {table_name}"
            df = self.execute_query(query)
            
            # Salva em CSV
            df.to_csv(backup_file, index=False)
            self.logger.log('info', f"Backup salvo em {backup_file}")
            
        except Exception as e:
            self.logger.log('error', f"Erro no backup de {table_name}: {e}")
            raise
