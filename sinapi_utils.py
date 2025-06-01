from typing import List, Dict, Optional, Any
import datetime
import time
from pathlib import Path
import unicodedata
import logging
import sqlalchemy
from sqlalchemy import create_engine, text
import pandas as pd
from tqdm import tqdm
import re

def logger_config(log_level: Optional[str] = None) -> logging.Logger:
    """
    Configura o logger para controle de logs no terminal.
    Args:
        log_level (str): 'debug', 'info' ou 'off'. Padrão é 'off' (desativado).
    Returns:
        logging.Logger: Logger configurado
    """
    logger = logging.getLogger("sinapi_logger")
    logger.propagate = False

    log_levels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'off': logging.CRITICAL + 1
    }
    
    logger.setLevel(log_levels.get(log_level.lower() if log_level else 'off', log_levels['off']))

    if logger.hasHandlers():
        logger.handlers.clear()
        
    if log_level and log_level.lower() != 'off':
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        handler.setLevel(logger.level)
        logger.addHandler(handler)
        
    return logger

def normalize_text(text: str) -> str:
    """
    Normaliza um texto removendo acentos e convertendo para maiúsculo.
    Args:
        text (str): Texto a ser normalizado
    Returns:
        str: Texto normalizado
    """
    if not text:
        return ''
        
    text = str(text).strip().upper()
    text = text.replace('\n', ' ')
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    return text.replace(' ', '_').replace('-', '_').replace('  ', ' ').strip()

def normalize_files(path: Path, extension: str = 'xlsx') -> List[str]:
    """
    Normaliza os nomes dos arquivos em um diretório.
    Args:
        path (Path): Caminho do diretório
        extension (str): Extensão dos arquivos a serem normalizados
    Returns:
        List[str]: Lista com os nomes normalizados
    """
    logger = logger_config()
    extension = extension.strip().lower().lstrip('*.')
    files = list(path.glob(f'*.{extension}'))
    normalized_names = []
    
    for file in files:
        new_name = normalize_text(file.name)
        new_path = file.parent / new_name
        if file != new_path:
            file.rename(new_path)
            logger.info(f'Arquivo renomeado: {file} -> {new_path}')
        normalized_names.append(new_name)
        
    return normalized_names

def clean_string(value: Any) -> str:
    """
    Limpa e normaliza uma string para inserção no banco de dados.
    Args:
        value (Any): Valor a ser limpo
    Returns:
        str: String limpa e normalizada
    """
    if pd.isna(value) or value == '':
        return 'NULL'
        
    text = str(value).upper()
    text = ' '.join(text.split())
    text = text.replace("'", "").replace('"', '')
    text = text.replace('R$', 'RS').replace('$', 'S')
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    text = text.replace('\n', ' ')
    
    return text

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza os nomes das colunas do DataFrame adicionando aspas.
    Args:
        df (pd.DataFrame): DataFrame a ser normalizado
    Returns:
        pd.DataFrame: DataFrame com colunas normalizadas
    """
    logger = logger_config()
    df = df.copy()
    df.columns = [f'"{col.strip()}"' for col in df.columns]
    df.columns = [col.replace('  ', ' ') for col in df.columns]
    
    # Remove aspas duplicadas
    df.columns = [
        '"' + col.strip().lstrip('"').rstrip('"') + '"'
        if col.startswith('"') and col.endswith('"')
        else col.strip()
        for col in df.columns
    ]
    
    logger.debug(f"Colunas normalizadas: {df.columns.tolist()}")
    return df

def improve_dataframe(df: pd.DataFrame, type_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Melhora o DataFrame convertendo tipos e limpando strings.
    Args:
        df (pd.DataFrame): DataFrame original
        type_mapping (Dict[str, str]): Mapeamento de tipos SQL para Pandas
    Returns:
        pd.DataFrame: DataFrame melhorado
    """
    logger = logger_config()
    logger.info("Iniciando tratamento dos dados...")
    df = df.copy()
    
    # Converte tipos
    for col in df.columns:
        sql_type = type_mapping.get(str(df[col].dtype))
        pandas_type = type_mapping.get(sql_type)
        
        if pandas_type:
            try:
                df[col] = df[col].astype(pandas_type)
                logger.debug(f"Coluna '{col}' convertida para {pandas_type}")
            except Exception as e:
                logger.warning(f"Falha ao converter coluna '{col}': {e}")
    
    # Limpa strings
    str_columns = df.select_dtypes(include=['object', 'string']).columns
    for col in str_columns:
        df[col] = df[col].apply(clean_string)
    
    logger.info("Tratamento dos dados concluído")
    return df

class DatabaseManager:
    """Classe para gerenciar operações de banco de dados"""
    
    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine
        #self.logger = logging.getLogger("sinapi_logger") #não entendi o motivo disso.
        self.logger = logger_config()

    def create_database(self, db_name: str) -> bool:
        """
        Cria um banco de dados se não existir.
        Args:
            db_name (str): Nome do banco de dados
        Returns:
            bool: True se criado/existente com sucesso
        """
        self.logger.info(f"Verificando banco de dados '{db_name}'...")
        try:
            with self.engine.connect() as conn:
                conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                    text(f"CREATE DATABASE {db_name}")
                )
            self.logger.info(f"Banco de dados '{db_name}' criado")
            return True
        except sqlalchemy.exc.ProgrammingError as e:
            if 'already exists' in str(e):
                self.logger.debug(f"Banco '{db_name}' já existe")
                return True
            self.logger.error(f"Erro ao criar banco '{db_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Erro: {e}")
            return False

    def create_schemas(self, schemas: List[str]) -> None:
        """
        Cria esquemas se não existirem.
        Args:
            schemas (List[str]): Lista de esquemas
        """
        self.logger.info("Verificando esquemas...")
        for schema in schemas:
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                    conn.commit()
                self.logger.debug(f"Schema '{schema}' verificado")
            except Exception as e:
                self.logger.error(f"Erro no schema '{schema}': {e}")
        self.logger.info(f"Esquemas verificados: {', '.join(schemas)}")

    def create_table(self, schema: str, table: str, columns: List[str], types: List[str]) -> None:
        """
        Cria uma tabela se não existir.
        Args:
            schema (str): Nome do esquema
            table (str): Nome da tabela
            columns (List[str]): Lista de colunas
            types (List[str]): Lista de tipos
        """
        table_name = f"{schema}.{table}"
        self.logger.info(f"Verificando tabela '{table_name}'...")
        
        ddl = f"CREATE TABLE {table_name} ({', '.join(f'{col} {typ}' for col, typ in zip(columns, types))})"
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(ddl))
                conn.commit()
            self.logger.info(f"Tabela '{table}' criada")
        except sqlalchemy.exc.ProgrammingError as e:
            if 'already exists' in str(e):
                self.logger.info(f"Tabela '{table}' já existe")
            else:
                self.logger.error(f"Erro na tabela '{table}': {e}")
        except Exception as e:
            self.logger.error(f"Erro: {e}")

    def insert_data(self, schema: str, table: str, df: pd.DataFrame, batch_size: int) -> None:
        """
        Insere dados em uma tabela com validação.
        Args:
            schema (str): Nome do esquema
            table (str): Nome da tabela
            df (DataFrame): DataFrame com os dados
            batch_size (int): Tamanho do lote
        """
        total_rows = len(df)
        table_name = f"{schema}.{table}"
        start_time = time.time()
        
        self.logger.info(f"Inserindo {total_rows} registros em {table_name}...")
        df = normalize_column_names(df)
        col_types = df.dtypes.to_dict()
        
        with self.engine.connect() as conn:
            with tqdm(total=total_rows, desc="Inserindo dados", unit="registro") as pbar:
                for i in range(0, total_rows, batch_size):
                    batch_start = time.time()
                    batch_df = df[i:i + batch_size]
                    
                    try:
                        self._insert_batch(conn, table_name, batch_df, col_types)
                        conn.commit()
                    except Exception as e:
                        self.logger.error(f"Erro no lote: {e}")
                        continue
                    
                    batch_time = time.time() - batch_start
                    records = len(batch_df)
                    elapsed = time.time() - start_time
                    rate = records / batch_time if batch_time > 0 else 0
                    
                    pbar.update(records)
                    pbar.set_postfix({
                        "Tempo lote": f"{batch_time:.2f}s",
                        "Tempo total": f"{elapsed:.2f}s",
                        "Reg/s": f"{rate:.2f}"
                    })
                    self.logger.info(f"Lote de {records} registros em {batch_time:.2f}s")
        
        total_time = datetime.timedelta(seconds=time.time() - start_time)
        self.logger.info(f"Tempo total: {total_time}")

    def _insert_batch(self, conn: sqlalchemy.engine.Connection, table_name: str, 
                     batch_df: pd.DataFrame, col_types: Dict[str, Any]) -> None:
        """
        Insere um lote de dados na tabela.
        Args:
            conn (Connection): Conexão do SQLAlchemy
            table_name (str): Nome completo da tabela (schema.tabela)
            batch_df (DataFrame): DataFrame com os dados do lote
            col_types (Dict[str, Any]): Tipos das colunas
        """
        for _, row in batch_df.iterrows():
            row_dict = row.to_dict()
            columns = ', '.join(row_dict.keys())
            
            values = []
            conditions = []
            
            for col, val in row_dict.items():
                col_type = col_types[col]
                
                if val == 'NULL' or pd.isna(val):
                    values.append('NULL')
                    conditions.append(f"{col} IS NULL")
                elif col_type == object or isinstance(val, str):
                    clean_val = clean_string(val)
                    values.append(f"'{clean_val}'")
                    conditions.append(f"{col} = '{clean_val}'")
                elif col_type in ['float64', 'float32'] or isinstance(val, float):
                    str_val = str(val).replace(',', '.')
                    values.append(str_val)
                    conditions.append(f"{col} = {str_val}")
                else:
                    int_val = int(val)
                    values.append(str(int_val))
                    conditions.append(f"{col} = {int_val}")
            
            values_str = ', '.join(values)
            where_clause = ' AND '.join(conditions)
            
            # Verifica se registro existe
            exists = conn.execute(
                text(f"SELECT EXISTS (SELECT 1 FROM {table_name} WHERE {where_clause})")
            ).scalar()
            
            if not exists:
                insert_sql = text(f"INSERT INTO {table_name} ({columns}) VALUES ({values_str})")
                conn.execute(insert_sql)
                self.logger.debug(f"Registro inserido: {row_dict}")
            else:
                self.logger.debug(f"Registro já existe: {row_dict}")
