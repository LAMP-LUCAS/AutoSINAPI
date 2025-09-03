"""
Módulo de Banco de Dados do AutoSINAPI.

Este módulo encapsula toda a interação com o banco de dados PostgreSQL.
Ele é responsável por:
- Criar a conexão com o banco de dados usando SQLAlchemy.
- Definir e criar o esquema de tabelas e views (DDL).
- Salvar os dados processados (DataFrames) nas tabelas, com diferentes
  políticas de inserção (append, upsert, replace).
- Executar queries de consulta e de modificação de forma segura.

A classe `Database` abstrai a complexidade do SQL e do SQLAlchemy, fornecendo
uma interface clara e de alto nível para o restante da aplicação.
"""
import logging
from typing import Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from autosinapi.exceptions import DatabaseError

class Database:
    def __init__(self, db_config: Dict[str, Any]):
        self.logger = logging.getLogger("autosinapi.database")
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter('[%(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.config = db_config
        self._engine = self._create_engine()
    
    def _create_engine(self) -> Engine:
        try:
            url = (f"postgresql://{self.config['user']}:{self.config['password']}"
                  f"@{self.config['host']}:{self.config['port']}"
                  f"/{self.config['database']}")
            self.logger.info(f"Tentando conectar ao banco de dados em: postgresql://{self.config['user']}:***@{self.config['host']}:{self.config['port']}/{self.config['database']}")
            return create_engine(url)
        except Exception as e:
            self.logger.error("----------------- ERRO ORIGINAL DE CONEXÃO -----------------")
            self.logger.error(f"TIPO DE ERRO: {type(e).__name__}")
            self.logger.error(f"MENSAGEM: {e}")
            self.logger.error("------------------------------------------------------------")
            raise DatabaseError("Erro ao conectar com o banco de dados")

    def create_tables(self):
        """
        Cria as tabelas do modelo de dados do SINAPI no banco PostgreSQL, recriando-as para garantir conformidade com o modelo.
        """
        # Drop all related objects to ensure a clean slate
        drop_statements = """
        DROP VIEW IF EXISTS vw_composicao_itens_unificados;
        DROP TABLE IF EXISTS composicao_subcomposicoes CASCADE;
        DROP TABLE IF EXISTS composicao_insumos CASCADE;
        DROP TABLE IF EXISTS custos_composicoes_mensal CASCADE;
        DROP TABLE IF EXISTS precos_insumos_mensal CASCADE;
        DROP TABLE IF EXISTS manutencoes_historico CASCADE;
        DROP TABLE IF EXISTS composicoes CASCADE;
        DROP TABLE IF EXISTS insumos CASCADE;
        DROP TABLE IF EXISTS composicao_itens CASCADE;
        """

        ddl = """
        CREATE TABLE insumos (
            codigo INTEGER PRIMARY KEY,
            descricao TEXT NOT NULL,
            unidade VARCHAR,
            classificacao TEXT,
            status VARCHAR DEFAULT 'ATIVO'
        );

        CREATE TABLE composicoes (
            codigo INTEGER PRIMARY KEY,
            descricao TEXT NOT NULL,
            unidade VARCHAR,
            grupo VARCHAR,
            status VARCHAR DEFAULT 'ATIVO'
        );

        CREATE TABLE precos_insumos_mensal (
            insumo_codigo INTEGER NOT NULL,
            uf CHAR(2) NOT NULL,
            data_referencia DATE NOT NULL,
            regime VARCHAR NOT NULL,
            preco_mediano NUMERIC,
            PRIMARY KEY (insumo_codigo, uf, data_referencia, regime),
            FOREIGN KEY (insumo_codigo) REFERENCES insumos(codigo) ON DELETE CASCADE
        );

        CREATE TABLE custos_composicoes_mensal (
            composicao_codigo INTEGER NOT NULL,
            uf CHAR(2) NOT NULL,
            data_referencia DATE NOT NULL,
            regime VARCHAR NOT NULL,
            custo_total NUMERIC,
            PRIMARY KEY (composicao_codigo, uf, data_referencia, regime),
            FOREIGN KEY (composicao_codigo) REFERENCES composicoes(codigo) ON DELETE CASCADE
        );

        CREATE TABLE composicao_insumos (
            composicao_pai_codigo INTEGER NOT NULL,
            insumo_filho_codigo INTEGER NOT NULL,
            coeficiente NUMERIC,
            PRIMARY KEY (composicao_pai_codigo, insumo_filho_codigo),
            FOREIGN KEY (composicao_pai_codigo) REFERENCES composicoes(codigo) ON DELETE CASCADE,
            FOREIGN KEY (insumo_filho_codigo) REFERENCES insumos(codigo) ON DELETE CASCADE
        );

        CREATE TABLE composicao_subcomposicoes (
            composicao_pai_codigo INTEGER NOT NULL,
            composicao_filho_codigo INTEGER NOT NULL,
            coeficiente NUMERIC,
            PRIMARY KEY (composicao_pai_codigo, composicao_filho_codigo),
            FOREIGN KEY (composicao_pai_codigo) REFERENCES composicoes(codigo) ON DELETE CASCADE,
            FOREIGN KEY (composicao_filho_codigo) REFERENCES composicoes(codigo) ON DELETE CASCADE
        );

        CREATE TABLE manutencoes_historico (
            item_codigo INTEGER NOT NULL,
            tipo_item VARCHAR NOT NULL,
            data_referencia DATE NOT NULL,
            tipo_manutencao TEXT NOT NULL,
            descricao_item TEXT,
            PRIMARY KEY (item_codigo, tipo_item, data_referencia, tipo_manutencao)
        );

        CREATE OR REPLACE VIEW vw_composicao_itens_unificados AS
        SELECT
            composicao_pai_codigo,
            insumo_filho_codigo AS item_codigo,
            'INSUMO' AS tipo_item,
            coeficiente
        FROM
            composicao_insumos
        UNION ALL
        SELECT
            composicao_pai_codigo,
            composicao_filho_codigo AS item_codigo,
            'COMPOSICAO' AS tipo_item,
            coeficiente
        FROM
            composicao_subcomposicoes;
        """
        try:
            with self._engine.connect() as conn:
                trans = conn.begin()
                self.logger.info("Recriando o esquema do banco de dados...")
                # Drop old tables and view
                for stmt in drop_statements.split(';'):
                    if stmt.strip():
                        conn.execute(text(stmt))
                # Create new tables and view
                for stmt in ddl.split(';'):
                    if stmt.strip():
                        conn.execute(text(stmt))
                trans.commit()
            self.logger.info("Esquema do banco de dados recriado com sucesso.")
        except Exception as e:
            trans.rollback()
            raise DatabaseError(f"Erro ao recriar as tabelas: {str(e)}")

    def save_data(self, data: pd.DataFrame, table_name: str, policy: str, **kwargs) -> None:
        """
        Salva os dados no banco, aplicando a política de duplicatas.
        """
        if data.empty:
            self.logger.warning(f"DataFrame para a tabela '{table_name}' está vazio. Nenhum dado será salvo.")
            return

        if policy.lower() == 'substituir':
            year = kwargs.get('year')
            month = kwargs.get('month')
            if not year or not month:
                raise DatabaseError("Política 'substituir' requer 'year' e 'month'.")
            self._replace_data(data, table_name, year, month)
        elif policy.lower() == 'append':
            self._append_data(data, table_name)
        elif policy.lower() == 'upsert':
            pk_columns = kwargs.get('pk_columns')
            if not pk_columns:
                raise DatabaseError("Política 'upsert' requer 'pk_columns'.")
            self._upsert_data(data, table_name, pk_columns)
        else:
            raise DatabaseError(f"Política de duplicatas desconhecida: {policy}")

    def _append_data(self, data: pd.DataFrame, table_name: str):
        """Insere dados, ignorando conflitos de chave primária."""
        self.logger.info(f"Inserindo {len(data)} registros em '{table_name}' (política: append/ignore)." )
        
        with self._engine.connect() as conn:
            data.to_sql(name=f"temp_{table_name}", con=conn, if_exists='replace', index=False)
            
            pk_cols_query = text(f"""
                SELECT a.attname
                FROM   pg_index i
                JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                 AND a.attnum = ANY(i.indkey)
                WHERE  i.indrelid = '"{table_name}"'::regclass
                AND    i.indisprimary;
            """)
            
            trans = conn.begin()
            try:
                pk_cols_result = conn.execute(pk_cols_query).fetchall()
                if not pk_cols_result:
                    raise DatabaseError(f"Nenhuma chave primária encontrada para a tabela {table_name}.")
                pk_cols = [row[0] for row in pk_cols_result]
                pk_cols_str = ", ".join(pk_cols)
                
                cols = ", ".join([f'"{c}"' for c in data.columns])
                
                insert_query = f"""
                INSERT INTO "{table_name}" ({cols})
                SELECT {cols} FROM "temp_{table_name}"
                ON CONFLICT ({pk_cols_str}) DO NOTHING;
                """
                conn.execute(text(insert_query))
                conn.execute(text(f'DROP TABLE "temp_{table_name}" CASCADE'))
                trans.commit()
            except Exception as e:
                trans.rollback()
                raise DatabaseError(f"Erro ao inserir dados em {table_name}: {str(e)}")

    def _replace_data(self, data: pd.DataFrame, table_name: str, year: str, month: str):
        """Substitui os dados de um determinado período."""
        self.logger.info(f"Substituindo dados em '{table_name}' para o período {year}-{month}.")
        delete_query = text(f'''DELETE FROM "{table_name}" WHERE TO_CHAR(data_referencia, 'YYYY-MM') = :ref''')
        
        with self._engine.connect() as conn:
            trans = conn.begin()
            try:
                conn.execute(delete_query, {"ref": f"{year}-{month}"})
                data.to_sql(name=table_name, con=conn, if_exists='append', index=False)
                trans.commit()
            except Exception as e:
                trans.rollback()
                raise DatabaseError(f"Erro ao substituir dados: {str(e)}")

    def _upsert_data(self, data: pd.DataFrame, table_name: str, pk_columns: list):
        """Executa um UPSERT (INSERT ON CONFLICT UPDATE)."""
        self.logger.info(f"Executando UPSERT de {len(data)} registros em '{table_name}'.")
        
        with self._engine.connect() as conn:
            data.to_sql(name=f"temp_{table_name}", con=conn, if_exists='replace', index=False)

            cols = ", ".join([f'"{c}"' for c in data.columns])
            pk_cols_str = ", ".join(pk_columns)
            update_cols = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in data.columns if c not in pk_columns])

            if not update_cols:
                self._append_data(data, table_name)
                return

            query = f"""
            INSERT INTO "{table_name}" ({cols})
            SELECT {cols} FROM "temp_{table_name}"
            ON CONFLICT ({pk_cols_str}) DO UPDATE SET {update_cols};
            """
            
            trans = conn.begin()
            try:
                conn.execute(text(query))
                conn.execute(text(f'DROP TABLE "temp_{table_name}" CASCADE'))
                trans.commit()
            except Exception as e:
                trans.rollback()
                raise DatabaseError(f"Erro no UPSERT para {table_name}: {str(e)}")

    def truncate_table(self, table_name: str):
        """Executa TRUNCATE em uma tabela para limpá-la antes de uma nova carga."""
        self.logger.info(f"Limpando tabela: {table_name}")
        try:
            with self._engine.connect() as conn:
                trans = conn.begin()
                conn.execute(text(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE'))
                trans.commit()
        except Exception as e:
            trans.rollback()
            raise DatabaseError(f"Erro ao truncar a tabela {table_name}: {str(e)}")

    def execute_query(self, query: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            self.logger.error("----------------- ERRO ORIGINAL DE EXECUÇÃO (QUERY) -----------------")
            self.logger.error(f"TIPO DE ERRO: {type(e).__name__}")
            self.logger.error(f"MENSAGEM: {e}")
            self.logger.error(f"QUERY: {query}")
            self.logger.error("---------------------------------------------------------------------")
            raise DatabaseError(f"Erro ao executar query: {str(e)}")
    
    def execute_non_query(self, query: str, params: Dict[str, Any] = None) -> int:
        """
        Executa uma query que não retorna resultados (INSERT, UPDATE, DELETE, DDL).
        Retorna o número de linhas afetadas.
        """
        try:
            with self._engine.connect() as conn:
                trans = conn.begin()
                result = conn.execute(text(query), params or {})
                trans.commit()
                return result.rowcount
        except Exception as e:
            trans.rollback()
            self.logger.error("----------------- ERRO ORIGINAL DE EXECUÇÃO (NON-QUERY) -----------------")
            self.logger.error(f"TIPO DE ERRO: {type(e).__name__}")
            self.logger.error(f"MENSAGEM: {e}")
            self.logger.error(f"QUERY: {query}")
            self.logger.error("-----------------------------------------------------------------------")
            raise DatabaseError(f"Erro ao executar non-query: {str(e)}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._engine.dispose()
    