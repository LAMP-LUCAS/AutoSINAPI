"""
Utilitários centralizados para o sistema SINAPI
Este módulo contém todas as funções e classes comuns utilizadas pelos outros módulos do sistema.
"""
import logging
import unicodedata
import pandas as pd
import os
import zipfile
import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Union
from pathlib import Path
import sqlalchemy
from sqlalchemy import create_engine, text
from tqdm import tqdm
from openpyxl import load_workbook
import json

class SinapiLogger:
    """Classe para gerenciar logs do sistema SINAPI"""
    
    def __init__(self, nome: str, level: str = 'info'):
        """
        Inicializa o logger com nome e nível específicos
        Args:
            nome (str): Nome do logger
            level (str): Nível de log ('debug', 'info', 'warning', 'error', 'critical')
        """
        self.logger = logging.getLogger(nome)
        self.configure(level)
    
    def configure(self, level: str = 'info') -> None:
        """Configura o logger com o nível especificado"""
        levels = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
            'critical': logging.CRITICAL
        }
        
        self.logger.setLevel(levels.get(level.lower(), logging.INFO))
        
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
            
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def log(self, level: str, message: str) -> None:
        """Registra uma mensagem no nível especificado"""
        getattr(self.logger, level.lower())(message)

class ExcelProcessor:
    """Classe para processamento de arquivos Excel do SINAPI"""
    
    def __init__(self):
        self.logger = SinapiLogger("ExcelProcessor")
    
    def scan_directory(self, diretorio: str = None, formato: str = 'xlsx', data: bool = False) -> Dict:
        """
        Escaneia um diretório em busca de arquivos Excel
        Args:
            diretorio (str): Caminho do diretório
            formato (str): Formato dos arquivos ('xlsx', 'xls', etc)
            data (bool): Se True, processa os dados das planilhas
        Returns:
            Dict: Resultados do processamento
        """
        if not diretorio:
            diretorio = os.getcwd()
        diretorio = Path(diretorio).resolve()
        
        self.logger.log('info', f'Escaneando o diretório: {diretorio}')
        resultado = {}
        
        try:
            for arquivo in os.listdir(diretorio):
                if not arquivo.lower().endswith(formato.lower()):
                    continue
                    
                caminho = diretorio / arquivo
                self.logger.log('info', f'Processando: {arquivo}')
                
                if data:
                    try:
                        wb = load_workbook(caminho, read_only=True)
                        planilhas_info = []
                        for nome_planilha in wb.sheetnames:
                            ws = wb[nome_planilha]
                            dados = self.get_sheet_data(ws)
                            planilhas_info.append((nome_planilha, dados))
                        resultado[arquivo] = planilhas_info
                        wb.close()
                    except Exception as e:
                        self.logger.log('error', f"Erro ao processar {arquivo}: {str(e)}")
                else:
                    resultado[arquivo] = str(caminho)
                    
        except Exception as e:
            self.logger.log('error', f"Erro ao escanear diretório: {str(e)}")
            
        self.logger.log('info', f'Encontrados {len(resultado)} arquivos')
        return resultado

    def get_sheet_data(self, ws) -> List[int]:
        """
        Extrai informações básicas de uma planilha Excel
        Args:
            ws: Worksheet do openpyxl
        Returns:
            List[int]: [total_cells, n_rows, n_cols]
        """
        if not any(ws.cell(row=1, column=col).value for col in [1,2]) and not ws.cell(row=2, column=1).value:
            return [0, 0, 0]
        
        total_cells = 0
        for row in ws.iter_rows(min_row=ws.min_row, max_row=ws.max_row,
                              min_col=ws.min_column, max_col=ws.max_column,
                              values_only=True):
            total_cells += sum(1 for cell in row if cell is not None and (not isinstance(cell, str) or cell.strip()))
        
        return [total_cells, ws.max_row - ws.min_row + 1, ws.max_column - ws.min_column + 1]

class FileManager:
    """Classe para gerenciamento de arquivos do SINAPI"""
    
    def __init__(self):
        self.logger = SinapiLogger("FileManager")
    
    def normalize_text(self, text: str) -> str:
        """
        Normaliza um texto removendo acentos e convertendo para maiúsculo
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

    def normalize_files(self, path: Union[str, Path], extension: str = 'xlsx') -> List[str]:
        """
        Normaliza nomes de arquivos em um diretório
        Args:
            path: Caminho do diretório
            extension (str): Extensão dos arquivos
        Returns:
            List[str]: Lista de nomes normalizados
        """
        path = Path(path)
        extension = extension.strip().lower().lstrip('*.')
        normalized_names = []
        
        for file in path.glob(f'*.{extension}'):
            new_name = self.normalize_text(file.name)
            new_path = file.parent / new_name
            
            if file != new_path:
                try:
                    file.rename(new_path)
                    self.logger.log('info', f'Arquivo renomeado: {file} -> {new_path}')
                except Exception as e:
                    self.logger.log('error', f'Erro ao renomear {file}: {str(e)}')
                    
            normalized_names.append(new_name)
            
        return normalized_names

class SinapiDownloader:
    """Classe para download de arquivos do SINAPI"""
    
    def __init__(self, cache_minutes: int = 10):
        self.logger = SinapiLogger("SinapiDownloader")
        self.cache_minutes = cache_minutes
        self.log_file = "sinap_webscraping_download_log.json"
    
    def download_file(self, ano: str, mes: str, formato: str = 'xlsx') -> Optional[str]:
        """
        Baixa arquivo do SINAPI se necessário
        Args:
            ano (str): Ano de referência (YYYY)
            mes (str): Mês de referência (MM)
            formato (str): Formato do arquivo ('xlsx' ou 'pdf')
        Returns:
            Optional[str]: Caminho do arquivo baixado ou None se falhou
        """
        if not self._validar_parametros(ano, mes, formato):
            return None
            
        if not self._pode_baixar(ano, mes):
            return None
            
        url = f'https://www.caixa.gov.br/Downloads/sinapi-relatorios-mensais/SINAPI-{ano}-{mes}-formato-{formato}.zip'
        folder_name = f'{ano}_{mes}'
        zip_path = Path(folder_name) / f'SINAPI-{ano}-{mes}-formato-{formato}.zip'
        
        if zip_path.exists():
            self.logger.log('info', f'Arquivo já existe: {zip_path}')
            return str(zip_path)
            
        try:
            os.makedirs(folder_name, exist_ok=True)
            self._download_with_retry(url, zip_path)
            self._registrar_download(ano, mes)
            return str(zip_path)
        except Exception as e:
            self.logger.log('error', f'Erro no download: {str(e)}')
            return None

    def unzip_file(self, zip_path: Union[str, Path]) -> Optional[str]:
        """
        Extrai um arquivo ZIP do SINAPI
        Args:
            zip_path: Caminho do arquivo ZIP
        Returns:
            Optional[str]: Caminho da pasta extraída ou None se falhou
        """
        zip_path = Path(zip_path)
        if not zip_path.exists():
            self.logger.log('error', f'Arquivo não existe: {zip_path}')
            return None
            
        extraction_path = zip_path.parent / zip_path.stem
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extraction_path)
            self.logger.log('info', f'Arquivos extraídos em: {extraction_path}')
            return str(extraction_path)
        except Exception as e:
            self.logger.log('error', f'Erro ao extrair: {str(e)}')
            return None

    def _validar_parametros(self, ano: str, mes: str, formato: str) -> bool:
        """Valida os parâmetros de entrada"""
        try:
            if len(ano) != 4 or len(mes) != 2:
                raise ValueError("Ano deve ter 4 dígitos e mês deve ter 2 dígitos")
            if int(mes) < 1 or int(mes) > 12:
                raise ValueError("Mês deve estar entre 01 e 12")
            if formato not in ['xlsx', 'pdf']:
                raise ValueError("Formato deve ser 'xlsx' ou 'pdf'")
            return True
        except Exception as e:
            self.logger.log('error', f'Parâmetros inválidos: {str(e)}')
            return False

    def _pode_baixar(self, ano: str, mes: str) -> bool:
        """Verifica se já passou o tempo mínimo desde o último download"""
        chave = f"{ano}_{mes}"
        agora = datetime.now()
        
        if not os.path.exists(self.log_file):
            return True
            
        try:
            with open(self.log_file, "r") as f:
                log = json.load(f)
            ultimo = log.get(chave)
            if ultimo:
                ultimo_dt = datetime.fromisoformat(ultimo)
                if agora - ultimo_dt < timedelta(minutes=self.cache_minutes):
                    tempo_restante = timedelta(minutes=self.cache_minutes) - (agora - ultimo_dt)
                    self.logger.log('warning', 
                        f"Download recente detectado. Aguarde {tempo_restante} antes de tentar novamente.")
                    return False
        except Exception as e:
            self.logger.log('error', f'Erro ao ler log: {str(e)}')
            
        return True

    def _registrar_download(self, ano: str, mes: str) -> None:
        """Registra a data/hora do download no log"""
        chave = f"{ano}_{mes}"
        agora = datetime.now().isoformat()
        log = {}
        
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, "r") as f:
                    log = json.load(f)
            except Exception:
                pass
                
        log[chave] = agora
        
        with open(self.log_file, "w") as f:
            json.dump(log, f)

    def _download_with_retry(self, url: str, zip_path: Path, max_retries: int = 3) -> None:
        """Faz o download com retry em caso de falha"""
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=max_retries)
        session.mount('https://', adapter)
        
        try:
            response = session.get(url, timeout=30, allow_redirects=True)
            response.raise_for_status()
            
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            self.logger.log('info', f'Download concluído: {zip_path}')
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.logger.log('error', 'Arquivo não encontrado no servidor')
            else:
                self.logger.log('error', f'Erro HTTP: {str(e)}')
            raise
            
        except Exception as e:
            self.logger.log('error', f'Erro no download: {str(e)}')
            raise

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

class SinapiProcessor:
    """Classe para processamento específico das planilhas SINAPI"""
    
    def __init__(self):
        self.logger = SinapiLogger("SinapiProcessor")
        self.file_manager = FileManager()
    
    def process_excel(self, file_path: Union[str, Path], sheet_name: str, header_id: int, split_id: int = 0) -> pd.DataFrame:
        """
        Processa uma planilha SINAPI, normalizando colunas e realizando transformações necessárias
        Args:
            file_path: Caminho do arquivo Excel
            sheet_name: Nome da planilha
            header_id: Índice da linha de cabeçalho
            split_id: Índice para split de colunas (melt)
        Returns:
            DataFrame: Dados processados
        """
        try:
            self.logger.log('info', f'Processando planilha {sheet_name} do arquivo {file_path}')
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_id)
            
            # Normaliza nomes das colunas
            df.columns = [self.file_manager.normalize_text(col) for col in df.columns]
            
            # Se necessário fazer melt (unpivot)
            if split_id > 0:
                self.logger.log('debug', f'Realizando melt com split_id={split_id}')
                df = pd.melt(
                    df,
                    id_vars=df.columns[:split_id],
                    value_vars=df.columns[split_id:],
                    var_name='ESTADO',
                    value_name='COEFICIENTE'
                )
                
            return df
            
        except Exception as e:
            self.logger.log('error', f'Erro ao processar planilha: {str(e)}')
            raise
    
    def identify_sheet_type(self, sheet_name: str, table_names: List[str] = None) -> Dict[str, int]:
        """
        Identifica o tipo de planilha SINAPI e retorna suas configurações
        Args:
            sheet_name: Nome da planilha
            table_names: Lista de nomes de tabelas conhecidas
        Returns:
            Dict: Configurações da planilha (split_id, header_id)
        """
        sheet_name = self.file_manager.normalize_text(sheet_name)
        
        # Configurações padrão por tipo de planilha
        configs = {
            'ISD': {'split_id': 5, 'header_id': 9},
            'CSD': {'split_id': 4, 'header_id': 9},
            'ANALITICO': {'split_id': 0, 'header_id': 9},
            'COEFICIENTES': {'split_id': 5, 'header_id': 5},
            'MANUTENCOES': {'split_id': 0, 'header_id': 5},
            'MAO_DE_OBRA': {'split_id': 4, 'header_id': 5}
        }
        
        # Verifica correspondências diretas
        for type_name, config in configs.items():
            if type_name in sheet_name:
                self.logger.log('info', f'Planilha identificada como {type_name}')
                return config
        
        # Verifica correspondências com table_names se fornecido
        if table_names:
            for i, table in enumerate(table_names):
                table = self.file_manager.normalize_text(table)
                if table in sheet_name:
                    if i == 0:  # Insumos Coeficiente
                        return {'split_id': 5, 'header_id': 5}
                    elif i == 1:  # Códigos Manutenções
                        return {'split_id': 0, 'header_id': 5}
                    elif i == 2:  # Mão de Obra
                        return {'split_id': 4, 'header_id': 5}
        
        self.logger.log('warning', f'Tipo de planilha não identificado: {sheet_name}')
        return {'split_id': 0, 'header_id': 0}
    
    def validate_data(self, df: pd.DataFrame, expected_columns: List[str] = None) -> bool:
        """
        Valida os dados de uma planilha SINAPI
        Args:
            df: DataFrame a ser validado
            expected_columns: Lista de colunas esperadas
        Returns:
            bool: True se válido, False caso contrário
        """
        try:
            # Verifica se há dados
            if df.empty:
                self.logger.log('error', 'DataFrame está vazio')
                return False
            
            # Verifica colunas esperadas
            if expected_columns:
                missing = set(expected_columns) - set(df.columns)
                if missing:
                    self.logger.log('error', f'Colunas ausentes: {missing}')
                    return False
            
            # Verifica valores nulos em colunas críticas
            critical_cols = [col for col in df.columns if 'COD' in col or 'ID' in col]
            for col in critical_cols:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    self.logger.log('warning', f'Coluna {col} tem {null_count} valores nulos')
            
            return True
            
        except Exception as e:
            self.logger.log('error', f'Erro na validação: {str(e)}')
            return False
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa e padroniza os dados de uma planilha SINAPI
        Args:
            df: DataFrame a ser limpo
        Returns:
            DataFrame: Dados limpos e padronizados
        """
        try:
            df = df.copy()
            
            # Remove linhas totalmente vazias
            df = df.dropna(how='all')
            
            # Limpa strings
            str_columns = df.select_dtypes(include=['object']).columns
            for col in str_columns:
                df[col] = df[col].apply(lambda x: self.file_manager.normalize_text(str(x)) if pd.notnull(x) else x)
            
            # Converte colunas numéricas
            num_columns = df.select_dtypes(include=['float64', 'int64']).columns
            for col in num_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Remove caracteres especiais de colunas específicas
            cod_columns = [col for col in df.columns if 'COD' in col]
            for col in cod_columns:
                df[col] = df[col].astype(str).str.replace(r'[^0-9]', '', regex=True)
            
            return df
            
        except Exception as e:
            self.logger.log('error', f'Erro na limpeza dos dados: {str(e)}')
            raise

