"""
sql_sinapi_insert.py

Este módulo realiza a leitura, tratamento e inserção de dados do SINAPI (Sistema Nacional de Pesquisa de Custos e Índices da Construção Civil) a partir de arquivos Excel para um banco de dados PostgreSQL. Ele inclui funções para:

- Configuração de logging.
- Limpeza e padronização de strings.
- Conversão de tipos de dados entre pandas e SQL.
- Criação de banco de dados, schemas e tabelas se não existirem.
- Inserção de dados com verificação de duplicidade.
- Leitura de arquivos Excel do SINAPI.
- Execução via linha de comando.

Entradas principais:
- Arquivo Excel do SINAPI.
- Parâmetros de conexão com o banco de dados PostgreSQL.
- Tipo de base (insumos, composicao, analitico).

Saídas principais:
- Dados inseridos no banco PostgreSQL, em schemas e tabelas apropriados.

Uso:
    python sql_sinapi_insert.py --arquivo_xlsx <caminho> --tipo_base <tipo> --user <usuario> --password <senha> --host <host> --port <porta> --dbname <nome_db>
"""

import datetime
import time
import pandas as pd
import re
import sqlalchemy
from sqlalchemy import create_engine, text
import logging
from tqdm import tqdm

# Mapeamento de tipos SQL para pandas
SQL_TO_PANDAS_TYPE = {
    'TEXT': 'str',
    'BIGINT': 'int64',
    'DECIMAL': 'float64',
    'DATE': 'datetime64[ns]',
    'BOOLEAN': 'bool',
    'REAL': 'float32',
    'INTEGER': 'int32',
    'SMALLINT': 'int16',
    'TINYINT': 'int8',
    'INTERVAL': 'timedelta64[ns]',
}

def logger_config(log_level='off'):
    """
    Configura e retorna um logger para o processo.

    Args:
        log_level (str): Nível de log ('off', 'info', 'debug').

    Returns:
        logging.Logger: Logger configurado.
    """
    logger = logging.getLogger("sinapi_logger")
    logger.propagate = False
    if log_level is None or log_level.lower() == 'off':
        logger.setLevel(logging.CRITICAL + 1)
    elif log_level.lower() == 'debug':
        logger.setLevel(logging.DEBUG)
    elif log_level.lower() == 'info':
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.CRITICAL + 1)
    if logger.hasHandlers():
        logger.handlers.clear()
    if log_level is not None and log_level.lower() != 'off':
        stream_handler = logging.StreamHandler()
        stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(stream_formatter)
        stream_handler.setLevel(logger.level)
        logger.addHandler(stream_handler)
    return logger

def sql_access(secrets_file_path):
    """
    Lê e retorna as credenciais SQL do arquivo .secrets.

    Args:
        secrets_file_path (str): Caminho do arquivo de segredos com as credenciais do banco.
    Returns:
        tuple: (user, password, host, port, dbname, initial_db)
    Raises:
        FileNotFoundError: Se o arquivo de segredos não for encontrado.
        ValueError: Se as credenciais estiverem incompletas.
    """

    credentials = {}
    try:
        with open(secrets_file_path, 'r') as f:
            for line in f:
                match = re.match(r"\s*([A-Z_]+)\s*=\s*'([^']*)'", line)
                if match:
                    key, value = match.groups()
                    credentials[key] = value

        user = credentials.get('DB_USER')
        password = credentials.get('DB_PASSWORD')
        host = credentials.get('DB_HOST')
        port = credentials.get('DB_PORT')
        dbname = credentials.get('DB_NAME')
        initial_db = credentials.get('DB_INITIAL_DB', 'postgres')

        if None in [user, password, host, port, dbname]:
            raise ValueError("Credenciais incompletas no arquivo de segredos.")

        return user, password, host, port, dbname, initial_db

    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo '{secrets_file_path}' não encontrado.")
    except Exception as e:
        raise RuntimeError(f"Erro ao ler o arquivo '{secrets_file_path}': {e}")

def clean_string(s):
    """
    Limpa e padroniza uma string para inserção no banco de dados.

    Args:
        s (str): String de entrada.

    Returns:
        str: String limpa ou 'NULL' se vazia/nula.
    """
    if pd.isna(s) or s == '':
        return 'NULL'
    s = str(s)
    s = s.upper()
    s = ' '.join(s.split())
    s = s.replace("'", "")
    s = s.replace('"', '')
    s = s.replace(r'$', 'S')
    s = s.replace(r'R$', 'RS')
    s = re.sub(r'[^a-zA-Z0-9\s]', '', s)
    return s

def aspas_column_names(df_input):
    """
    Adiciona aspas duplas aos nomes das colunas de um DataFrame.

    Args:
        df_input (pd.DataFrame): DataFrame de entrada.

    Returns:
        pd.DataFrame: DataFrame com nomes de colunas entre aspas.
    """
    df_improove = df_input.copy()
    df_improove.columns = [f'"{col.strip()}"' for col in df_improove.columns]
    return df_improove

def df_improove(df, type_mapping=SQL_TO_PANDAS_TYPE, logger=None):
    """
    Converte tipos de colunas do DataFrame conforme mapeamento e limpa strings.

    Args:
        df (pd.DataFrame): DataFrame de entrada.
        type_mapping (dict): Mapeamento de tipos SQL para pandas.
        logger (logging.Logger, opcional): Logger para mensagens.

    Returns:
        pd.DataFrame: DataFrame tratado.
    """
    processed_df = df.copy()
    for col in processed_df.columns:
        # Descobre o tipo SQL e converte para pandas
        target_sql_type = type_mapping.get(str(processed_df[col].dtype), None)
        target_pandas_type = SQL_TO_PANDAS_TYPE.get(target_sql_type, None)
        if target_pandas_type:
            try:
                processed_df[col] = processed_df[col].astype(target_pandas_type)
                if logger:
                    logger.debug(f"Coluna '{col}' convertida para {target_pandas_type}")
            except Exception as e:
                if logger:
                    logger.warning(f"Não foi possível converter coluna '{col}' para {target_pandas_type}: {e}")
    # Limpeza de strings
    for col in processed_df.select_dtypes(include=['object', 'string']).columns:
        processed_df[col] = processed_df[col].apply(clean_string)
    if logger:
        logger.info("Tratamento dos dados concluído.")
    return processed_df

def create_database_if_not_exists(engine, db_name, logger=None):
    """
    Cria o banco de dados se não existir.

    Args:
        engine (sqlalchemy.Engine): Engine conectado ao banco inicial.
        db_name (str): Nome do banco a ser criado.
        logger (logging.Logger, opcional): Logger para mensagens.

    Returns:
        bool: True se criado/conectado, False em erro.
    """
    if logger:
        logger.info(f"Verificando/criando banco de dados '{db_name}' no engine '{engine}'...")
    try:
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(text(f"CREATE DATABASE {db_name}"))
            conn.commit()
        if logger:
            logger.debug(f"Banco de dados '{db_name}' criado com sucesso.")
        return True
    except sqlalchemy.exc.ProgrammingError as e:
        if 'already exists' in str(e):
            if logger:
                logger.debug(f"Banco de dados '{db_name}' já existe e foi conectado com sucesso.")
            return True
        else:
            if logger:
                logger.error(f"Erro ao criar o banco de dados '{db_name}': {e}")
            raise
    except Exception as e:
        if logger:
            logger.error(f"Erro ao criar o banco de dados '{db_name}': {e}")
        return False

def create_schemas_if_not_exists(engine, schemas, logger=None):
    """
    Cria schemas no banco de dados se não existirem.

    Args:
        engine (sqlalchemy.Engine): Engine conectada ao banco.
        schemas (list): Lista de nomes de schemas.
        logger (logging.Logger, opcional): Logger para mensagens.
    """
    if logger:
        logger.info("Verificando/criando esquemas...")
    for schema in schemas:
        if logger:
            logger.debug(f"Verificando/criando esquema '{schema}'...")
        try:
            with engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                conn.commit()
                if logger:
                    logger.debug(f"  Schema '{schema}' verificado/criado com sucesso!")
        except Exception as e:
            if logger:
                logger.error(f"Erro ao criar o schema '{schema}': {e}")
    if logger:
        logger.info(f"Todos os esquemas [{', '.join(schemas)}] foram verificados/criados.")

def create_tables_if_not_exists(engine, schema_name, table_name, columns, types, logger=None):
    """
    Cria tabela no schema especificado se não existir.

    Args:
        engine (sqlalchemy.Engine): Engine conectada ao banco.
        schema_name (str): Nome do schema.
        table_name (str): Nome da tabela.
        columns (list): Lista de nomes de colunas.
        types (list): Lista de tipos SQL das colunas.
        logger (logging.Logger, opcional): Logger para mensagens.
    """
    NomeEsquema = '.'.join([str(schema_name), str(table_name)])
    if logger:
        logger.info(f"Verificando/criando tabela '{table_name}' no esquema '{schema_name}'...")
    ddl = f"CREATE TABLE IF NOT EXISTS {NomeEsquema} ({', '.join([f'{col} {typ}' for col, typ in zip(columns, types)])});"
    try:
        with engine.connect() as conn:
            conn.execute(text(ddl))
            conn.commit()
        if logger:
            logger.info(f"Tabela '{table_name}' criada com sucesso.")
    except sqlalchemy.exc.ProgrammingError as e:
        if 'already exists' in str(e):
            if logger:
                logger.info(f"Tabela '{table_name}' já existe.")
        else:
            if logger:
                logger.error(f"Erro ao criar a tabela '{table_name}': {e}")
    except Exception as e:
        if logger:
            logger.error(f"Erro inesperado ao criar a tabela '{table_name}': {e}")

def insert_data_with_validation(engine, schema_name, table_name, dataframe, batch_size, logger=None):
    """
    Insere dados no banco de dados em lotes, validando duplicidade.

    Args:
        engine (sqlalchemy.Engine): Engine conectada ao banco.
        schema_name (str): Nome do schema.
        table_name (str): Nome da tabela.
        dataframe (pd.DataFrame): Dados a inserir.
        batch_size (int): Tamanho do lote de inserção.
        logger (logging.Logger, opcional): Logger para mensagens.
    """
    total_rows = len(dataframe)
    start_time = time.time()
    if logger:
        logger.info(f"Iniciando a inserção de {total_rows} registros na tabela {schema_name}.{table_name}...")
    dataframe = aspas_column_names(dataframe)
    col_types = dataframe.dtypes.to_dict()
    with tqdm(total=total_rows, desc="Inserindo dados", unit="registro") as pbar:
        for i in range(0, total_rows, batch_size):
            batch_start_time = time.time()
            batch = dataframe[i:i + batch_size]
            nameTable = f'{schema_name}.{table_name}'
            for index, row in batch.iterrows():
                row_dict = row.to_dict()
                columns = ', '.join(row_dict.keys())
                values_list = []
                placeholders_list = []
                for k, v in row_dict.items():
                    col_type = col_types[k]
                    if v == 'NULL' or pd.isna(v):
                        values_list.append('NULL')
                        placeholders_list.append(f"{k} IS NULL")
                    elif col_type == object or isinstance(v, str):
                        v_clean = clean_string(v)
                        values_list.append(f"'{v_clean}'")
                        placeholders_list.append(f"{k} = '{v_clean}'")
                    elif col_type in ['float64', 'float32'] or isinstance(v, float):
                        values_list.append(str(v).replace(',', '.'))
                        placeholders_list.append(f"{k} = {str(v).replace(',', '.')}")
                    elif col_type in ['int64', 'int32', 'int16', 'int8'] or isinstance(v, int):
                        values_list.append(str(int(v)))
                        placeholders_list.append(f"{k} = {int(v)}")
                    else:
                        v_clean = clean_string(str(v))
                        values_list.append(f"'{v_clean}'")
                        placeholders_list.append(f"{k} = '{v_clean}'")
                values = ', '.join(values_list)
                placeholders = ' AND '.join(placeholders_list)
                check_sql = text(f"SELECT EXISTS (SELECT 1 FROM {nameTable} WHERE {placeholders})")
                try:
                    with engine.connect() as conn:
                        result = conn.execute(check_sql).scalar()
                        if not result:
                            insert_sql = text(f"INSERT INTO {nameTable} ({columns}) VALUES ({values})")
                            if logger:
                                logger.debug(f'Inserindo linha: {row_dict}')
                            conn.execute(insert_sql)
                            conn.commit()
                        else:
                            if logger:
                                logger.debug(f"Linha já existe: {row_dict}")
                except Exception as e:
                    if logger:
                        logger.error(f"Erro ao inserir linha: {e}")
                    continue
            batch_end_time = time.time()
            batch_time = batch_end_time - batch_start_time
            inserted_rows = len(batch)
            elapsed_time = time.time() - start_time
            rows_per_second = inserted_rows / batch_time if batch_time > 0 else 0
            pbar.update(inserted_rows)
            if logger:
                logger.info(f"Lote de {inserted_rows} registros inserido em {batch_time:.2f}s.")
    end_time = time.time()
    total_time = end_time - start_time
    total_time_delta = datetime.timedelta(seconds=total_time)
    if logger:
        logger.info(f"Tempo total de execução da inserção: {total_time_delta}")

def load_total_excel_sinapi(filepath, tipo_base, logger=None):
    """
    Carrega arquivo Excel do SINAPI conforme o tipo de base.

    Args:
        filepath (str): Caminho do arquivo Excel.
        tipo_base (str): Tipo da base ('insumos', 'composicao', 'analitico').
        logger (logging.Logger, opcional): Logger para mensagens.

    Returns:
        pd.DataFrame: DataFrame carregado.
    """
    if tipo_base == 'insumos':
        df = pd.read_excel(filepath, header=6)
    elif tipo_base == 'composicao':
        df = pd.read_excel(filepath, header=4)
    elif tipo_base == 'analitico':
        df = pd.read_excel(filepath, header=4)
    else:
        raise ValueError("Tipo de base inválido. Use 'insumos', 'composicao' ou 'analitico'.")
    if logger:
        logger.info(f"Arquivo {filepath} carregado para tipo {tipo_base}.")
    return df

def clean_data(df_insert):
    # Limpeza genérica de dados: strings para uppercase, remoção de espaços e NaN para None
    df = df_insert.copy()
    df = df.applymap(lambda x: x.strip().upper() if isinstance(x, str) else x)
    df = df.where(pd.notnull(df), None)
    return df

def read_and_clean_excel(filepath, sheet_name, header=None):
    # Leitura da planilha Excel
    df = pd.read_excel(filepath, sheet_name=sheet_name, header=header)
    # Limpeza dos dados
    df = clean_data(df)
    return df

def load_especific_excel_sinapi(filepath, tipo_base, logger=None):
    """
    Carrega planilhas específicas do arquivo Excel do SINAPI conforme o tipo de base.

    Args:
        filepath (str): Caminho do arquivo Excel.
        tipo_base (str): Tipo da base ('coeficientes', 'manutencoes', 'analitico').
        logger (logging.Logger, opcional): Logger para mensagens.

    Returns:
        pd.DataFrame: DataFrame carregado ou None se o tipo_base for inválido.
    """
    if tipo_base == 'coeficientes':
        try:
            df = read_and_clean_excel(filepath, sheet_name='Coeficientes', header=5)

            if logger:
                logger.info(f"Planilha 'Coeficientes' carregada do arquivo {filepath}.")
            return df
        except Exception as e:
            if logger:
                logger.error(f"Erro ao carregar planilha 'Coeficientes' de {filepath}: {e}")
            return None
    elif tipo_base == 'manutencoes':
        try:
            df = read_and_clean_excel(filepath, sheet_name='Manutenções', header=5)
            if logger:
                logger.info(f"Planilha 'Manutenções' carregada do arquivo {filepath}.")
            return df
        except Exception as e:
            if logger:
                logger.error(f"Erro ao carregar planilha 'Manutenções' de {filepath}: {e}")
            return None
    elif tipo_base == 'analitico':
        try:
            df = read_and_clean_excel(filepath, sheet_name='Analítico', header=9)
            if logger:
                logger.info(f"Planilha 'Analítico' carregada do arquivo {filepath}.")
            return df
        except Exception as e:
            if logger:
                logger.error(f"Erro ao carregar planilha 'Analítico' de {filepath}: {e}")
            return None
    else:
        if logger:
            logger.warning(f"Tipo de base inválido: {tipo_base}. Use 'coeficientes', 'manutencoes' ou 'analitico'.")
        return None

def treat_coeficientes(df_insert, logger=None):
    """
    Trata os dados da planilha 'Coeficientes'.

    Args:
        df (pd.DataFrame): DataFrame da planilha 'Coeficientes'.
        logger (logging.Logger, opcional): Logger para mensagens.

    Returns:
        pd.DataFrame: DataFrame tratado.
    """
    df = df_insert.copy()
    # Implementar tratamento específico para 'Coeficientes'
    df = df.drop_duplicates()
    df.columns = [clean_string(col) for col in df.columns]
    df = clean_data(df)
    # uma lista com o nome das primeiras 5 colunas
    df_idvars = df.  # Mantém apenas as primeiras 5 colunas fixas

    # função que pivota as colunas de estados para linhas
    df = pd.melt(df, id_vars=['mes/ref', 'cod familia', 'cod_insumo', 'descricao', 'unidade'],
                    var_name='estado', value_name='coeficiente')
    
    if logger:
        logger.info("Dados da planilha 'Coeficientes' tratados.")
    return df

def treat_manutencoes(df, logger=None):
    """
    Trata os dados da planilha 'Manutenções'.

    Args:
        df (pd.DataFrame): DataFrame da planilha 'Manutenções'.
        logger (logging.Logger, opcional): Logger para mensagens.

    Returns:
        pd.DataFrame: DataFrame tratado.
    """
    # Implementar tratamento específico para 'Manutenções'
    df = df.drop_duplicates()
    if logger:
        logger.info("Dados da planilha 'Manutenções' tratados.")
    return df

def treat_analitico(df, logger=None):
    """
    Trata os dados da planilha 'Analítico'.

    Args:
        df (pd.DataFrame): DataFrame da planilha 'Analítico'.
        logger (logging.Logger, opcional): Logger para mensagens.

    Returns:
        pd.DataFrame: DataFrame tratado.
    """
    # Implementar tratamento específico para 'Analítico'
    df = df.drop_duplicates()
    if logger:
        logger.info("Dados da planilha 'Analítico' tratados.")
    return df

def create_prices_table(df_coeficientes, df_manutencoes, df_analitico, logger=None):
    """
    Cria a tabela de preços consolidada.

    Args:
        df_coeficientes (pd.DataFrame): DataFrame da planilha 'Coeficientes'.
        df_manutencoes (pd.DataFrame): DataFrame da planilha 'Manutenções'.
        df_analitico (pd.DataFrame): DataFrame da planilha 'Analítico'.
        logger (logging.Logger, opcional): Logger para mensagens.

    Returns:
        pd.DataFrame: DataFrame da tabela de preços consolidada.
    """
    # Implementar a lógica para criar a tabela de preços
    if logger:
        logger.info("Tabela de preços consolidada criada.")
    return None

# Função principal para uso como script
def main(
    arquivo_xlsx,
    tipo_base,
    user,
    password,
    host,
    port,
    dbname,
    initial_db='postgres',
    batch_size=1000,
    log_level='off'
    ):
    """
    Função principal para execução do script via linha de comando.

    Args:
        arquivo_xlsx (str): Caminho do arquivo Excel.
        tipo_base (str): Tipo da base ('insumos', 'composicao', 'analitico').
        user (str): Usuário do banco.
        password (str): Senha do banco.
        host (str): Host do banco.
        port (str/int): Porta do banco.
        dbname (str): Nome do banco de dados.
        initial_db (str): Banco inicial para conexão (default: 'postgres').
        batch_size (int): Tamanho do lote de inserção.
        log_level (str): Nível de log ('off', 'info', 'debug').
    """
    
    logger = logger_config(log_level)
    base = load_especific_excel_sinapi(arquivo_xlsx, tipo_base, logger)
    df = df_improove(base, logger=logger)
    if df is None:
        logger.error("Não foi possível carregar os dados do Excel.")
        return
    
    if tipo_base == 'coeficientes':
        df = treat_coeficientes(df, logger)
    elif tipo_base == 'manutencoes':
        df = treat_manutencoes(df, logger)
    elif tipo_base == 'analitico':
        df = treat_analitico(df, logger)

    if df is not None:
        df = df_improove(df, logger=logger)
        schema_name = f'sinapi_{tipo_base}_data'
        table_name = f'{tipo_base}_data'
        engine_base = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{initial_db}", connect_args={'connect_timeout': 30})
        if create_database_if_not_exists(engine_base, dbname, logger):
            engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
            schemas_to_create = ["sinapi_coeficientes_data", "sinapi_manutencoes_data", "sinapi_analitico_data"]
            create_schemas_if_not_exists(engine, schemas_to_create, logger)
            columns = aspas_column_names(df).columns.tolist()
            types = [SQL_TO_PANDAS_TYPE.get(str(df[col].dtype), 'TEXT') for col in df.columns]
            create_tables_if_not_exists(engine, schema_name, table_name, columns, types, logger)
            insert_data_with_validation(engine, schema_name, table_name, df, batch_size, logger)
            engine.dispose()
        engine_base.dispose()

    schema_name = f'sinapi_{tipo_base}_data'
    table_name = f'{tipo_base}_data'
    engine_base = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{initial_db}", connect_args={'connect_timeout': 30})
    if create_database_if_not_exists(engine_base, dbname, logger):
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
        schemas_to_create = ["sinapi_composicao_data", "sinapi_composicao_precos", "sinapi_insumos_data", "sinapi_insumos_precos"]
        create_schemas_if_not_exists(engine, schemas_to_create, logger)
        columns = aspas_column_names(df).columns.tolist()
        types = [SQL_TO_PANDAS_TYPE.get(str(df[col].dtype), 'TEXT') for col in df.columns]
        create_tables_if_not_exists(engine, schema_name, table_name, columns, types, logger)
        insert_data_with_validation(engine, schema_name, table_name, df, batch_size, logger)
        engine.dispose()
    engine_base.dispose()

if __name__ == "__main__":
    # Ponto de entrada do script para execução via linha de comando ou arquivo de configuração.

    import argparse
    import sys
    import os

    def parse_args():
        parser = argparse.ArgumentParser(description="Insere dados SINAPI no PostgreSQL.")
        parser.add_argument('--arquivo_xlsx', help='Caminho do arquivo Excel SINAPI')
        parser.add_argument('--tipo_base', choices=['insumos', 'composicao', 'analitico'], help='Tipo de base')
        parser.add_argument('--user', help='Usuário do banco')
        parser.add_argument('--password', help='Senha do banco')
        parser.add_argument('--host', help='Host do banco')
        parser.add_argument('--port', help='Porta do banco')
        parser.add_argument('--dbname', help='Nome do banco de dados')
        parser.add_argument('--initial_db', default='postgres', help='Banco inicial para conexão')
        parser.add_argument('--batch_size', type=int, default=1000, help='Tamanho do lote de inserção')
        parser.add_argument('--log_level', default='off', choices=['off', 'info', 'debug'], help='Nível de log')
        parser.add_argument('--config', help='Arquivo de configuração .secrets')
        return parser.parse_args()
    # Inicializa parâmetros com None
    params = {
        'arquivo_xlsx': None,
        'tipo_base': None,
        'user': None,
        'password': None,
        'host': None,
        'port': None,
        'dbname': None,
        'initial_db': 'postgres',
        'batch_size': 1000,
        'log_level': 'off'
    }
    
    try: # Tenta analisar os argumentos da linha de comando
        
         args = parse_args()
    except:
        pass
    
    if args is None: # Se não houver argumentos da linha de comando, tenta ler do arquivo de configuração
        try:
            #tenta buscar o arquivo sql_access.secrets no diretório atual
            secrets_file = args.config if args.config else 'sql_access.secrets'
            if os.path.isfile(secrets_file):
                secrets_file = os.path.join(os.getcwd(), secrets_file)
                #sobrescreve os parametros com os valores do arquivo de configuração
                user, password, host, port, dbname, initial_db = sql_access(secrets_file)
                params.update({
                    'user': user,
                    'password': password,
                    'host': host,
                    'port': port,
                    'dbname': dbname,
                    'initial_db': initial_db
                })
            
        except FileNotFoundError as e:
            print(f"Arquivo de configuração '{secrets_file}' não encontrado.")
            pass  
    else: # Se arquivo de configuração fornecido na linha de comando, carrega valores
        if args.config:
            if not os.path.isfile(args.config):
                print(f"Arquivo de configuração '{args.config}' não encontrado.", file=sys.stderr)
                sys.exit(1)
            try:
                user, password, host, port, dbname, initial_db = sql_access(args.config)
                params.update({
                    'user': user,
                    'password': password,
                    'host': host,
                    'port': port,
                    'dbname': dbname,
                    'initial_db': initial_db
                })
            except Exception as e:
                print(f"Erro ao ler arquivo de configuração '{args.config}': {e}", file=sys.stderr)
                sys.exit(1)
    
        # Sobrescreve com argumentos da linha de comando, se fornecidos
        for key in params.keys():
            arg_val = getattr(args, key, None)
            if arg_val is not None:
                params[key] = arg_val

    # Validação obrigatória
    obrigatorios = ['arquivo_xlsx', 'tipo_base', 'user', 'password', 'host', 'port', 'dbname']
    faltando = [k for k in obrigatorios if not params[k]]
    
    if faltando:
        print(f"Argumentos obrigatórios faltando: {', '.join(faltando)}", file=sys.stderr)
        print("Use --help para detalhes de uso.", file=sys.stderr)
        sys.exit(2)

    try:
        main(
            arquivo_xlsx=params['arquivo_xlsx'],
            tipo_base=params['tipo_base'],
            user=params['user'],
            password=params['password'],
            host=params['host'],
            port=params['port'],
            dbname=params['dbname'],
            initial_db=params.get('initial_db', 'postgres'),
            batch_size=int(params.get('batch_size', 1000)),
            log_level=params.get('log_level', 'off')
        )
    except Exception as e:
        print(f"Erro na execução do script: {e}", file=sys.stderr)
        sys.exit(3)
