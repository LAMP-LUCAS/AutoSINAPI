"""
Módulo responsável pelo processamento dos dados SINAPI.
"""
from typing import Dict, Any, BinaryIO
import pandas as pd
from io import BytesIO
import re
import unicodedata
from ..exceptions import ProcessingError

class Processor:
    """Classe responsável pelo processamento dos dados SINAPI."""
    
    def __init__(self, sinapi_config: Dict[str, Any]):
        """
        Inicializa o processador.
        
        Args:
            sinapi_config: Configurações do SINAPI
        """
        self.config = sinapi_config
    
    def process(self, excel_file: BinaryIO) -> pd.DataFrame:
        """
        Processa o arquivo Excel do SINAPI.
        
        Args:
            excel_file: Arquivo Excel em memória
        
        Returns:
            DataFrame: Dados processados
        
        Raises:
            ProcessingError: Se houver erro no processamento
        """
        try:
            # Lê o arquivo Excel
            df = pd.read_excel(excel_file)
            
            # Aplica transformações
            df = self._clean_data(df)
            df = self._transform_data(df)
            df = self._validate_data(df)
            
            return df
            
        except Exception as e:
            raise ProcessingError(f"Erro no processamento: {str(e)}")
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove dados inconsistentes e padroniza formatos.
        
        Args:
            df: DataFrame a ser limpo
            
        Returns:
            DataFrame: Dados limpos e padronizados
        """
        # Copia o DataFrame para não modificar o original
        df = df.copy()
        
        # Remove linhas completamente vazias
        df.dropna(how='all', inplace=True)
        
        # Remove colunas completamente vazias
        df.dropna(axis=1, how='all', inplace=True)
        
        # Normaliza nomes das colunas
        df.columns = [self._normalize_column_name(col) for col in df.columns]
        
        # Remove espaços extras e converte para maiúsculo
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].apply(lambda x: self._normalize_text(x) if pd.notna(x) else x)
        
        # Converte colunas numéricas
        numeric_columns = [col for col in df.columns if 'PRECO' in col or 'VALOR' in col or 'CUSTO' in col]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')
        
        return df
        
    def _normalize_column_name(self, column: str) -> str:
        """Normaliza o nome de uma coluna."""
        if not column:
            return column
            
        column = str(column).strip().upper()
        column = unicodedata.normalize('NFKD', column).encode('ASCII', 'ignore').decode('utf-8')
        column = re.sub(r'[^A-Z0-9_]+', '_', column)
        column = re.sub(r'_+', '_', column)
        return column.strip('_')
        
    def _normalize_text(self, text: str) -> str:
        """Normaliza um texto removendo acentos e padronizando formato."""
        if not isinstance(text, str):
            return text
            
        text = text.strip().upper()
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
        text = re.sub(r'\s+', ' ', text)
        return text
    
    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformações específicas nos dados SINAPI.
        
        Args:
            df: DataFrame a ser transformado
            
        Returns:
            DataFrame: Dados transformados
        """
        # Copia o DataFrame para não modificar o original
        df = df.copy()
        
        # Adiciona colunas de metadados
        df['ANO_REFERENCIA'] = self.config.get('year')
        df['MES_REFERENCIA'] = self.config.get('month')
        df['TIPO_TABELA'] = self.config.get('type', 'REFERENCIA')
        
        # Identifica o tipo de planilha baseado nas colunas
        sheet_type = self._identify_sheet_type(df)
        
        if sheet_type == 'ISD':  # Insumos
            df = self._transform_insumos(df)
        elif sheet_type == 'CSD':  # Composições
            df = self._transform_composicoes(df)
        
        return df
    
    def _identify_sheet_type(self, df: pd.DataFrame) -> str:
        """Identifica o tipo de planilha baseado nas colunas presentes."""
        columns = set(df.columns)
        
        if {'CODIGO', 'DESCRICAO', 'UNIDADE', 'PRECO_MEDIANO'}.issubset(columns):
            return 'ISD'
        elif {'CODIGO_COMPOSICAO', 'DESCRICAO_COMPOSICAO', 'UNIDADE', 'CUSTO_TOTAL'}.issubset(columns):
            return 'CSD'
        else:
            return 'UNKNOWN'
    
    def _transform_insumos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformações específicas para planilhas de insumos."""
        # Renomeia colunas para padrão
        column_map = {
            'CODIGO': 'CODIGO_INSUMO',
            'DESCRICAO': 'DESCRICAO_INSUMO',
            'PRECO_MEDIANO': 'PRECO_UNITARIO'
        }
        df = df.rename(columns=column_map)
        
        # Garante tipos de dados corretos
        df['CODIGO_INSUMO'] = df['CODIGO_INSUMO'].astype(str)
        df['PRECO_UNITARIO'] = pd.to_numeric(df['PRECO_UNITARIO'], errors='coerce')
        
        return df
    
    def _transform_composicoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformações específicas para planilhas de composições."""
        # Renomeia colunas para padrão
        column_map = {
            'CODIGO_COMPOSICAO': 'CODIGO',
            'DESCRICAO_COMPOSICAO': 'DESCRICAO',
            'CUSTO_TOTAL': 'PRECO_UNITARIO'
        }
        df = df.rename(columns=column_map)
        
        # Garante tipos de dados corretos
        df['CODIGO'] = df['CODIGO'].astype(str)
        df['PRECO_UNITARIO'] = pd.to_numeric(df['PRECO_UNITARIO'], errors='coerce')
        
        return df
    
    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Valida os dados processados, removendo ou corrigindo registros inválidos.
        
        Args:
            df: DataFrame a ser validado
            
        Returns:
            DataFrame: Dados validados
            
        Raises:
            ProcessingError: Se houver erros críticos nos dados
        """
        # Copia o DataFrame para não modificar o original
        df = df.copy()
        
        # 1. Validações básicas
        if df.empty:
            raise ProcessingError("DataFrame está vazio após processamento")
            
        # 2. Remove linhas onde campos críticos são nulos
        critical_fields = ['CODIGO', 'DESCRICAO', 'UNIDADE', 'PRECO_UNITARIO']
        df.dropna(subset=critical_fields, how='any', inplace=True)
        
        # 3. Valida códigos
        invalid_codes = df[~df['CODIGO'].str.match(r'^\d+$', na=False)]
        if not invalid_codes.empty:
            self.logger.log('warning', f"Removendo {len(invalid_codes)} registros com códigos inválidos")
            df = df[df['CODIGO'].str.match(r'^\d+$', na=False)]
        
        # 4. Valida preços
        df.loc[df['PRECO_UNITARIO'] < 0, 'PRECO_UNITARIO'] = None
        
        # 5. Valida campos de texto
        text_columns = ['DESCRICAO', 'UNIDADE']
        for col in text_columns:
            if col in df.columns:
                # Remove linhas com descrições muito curtas ou vazias
                df = df[df[col].str.len() > 2]
        
        # 6. Validações específicas por tipo de planilha
        sheet_type = self._identify_sheet_type(df)
        if sheet_type == 'ISD':
            df = self._validate_insumos(df)
        elif sheet_type == 'CSD':
            df = self._validate_composicoes(df)
        
        # Se após todas as validações o DataFrame estiver vazio, é um erro
        if df.empty:
            raise ProcessingError("DataFrame vazio após validações")
            
        return df
    
    def _validate_insumos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validações específicas para planilhas de insumos."""
        # Verifica se os códigos de insumos seguem o padrão esperado
        if 'CODIGO_INSUMO' in df.columns:
            valid_mask = df['CODIGO_INSUMO'].str.len() >= 4
            if not valid_mask.all():
                self.logger.log('warning', f"Removendo {(~valid_mask).sum()} insumos com códigos inválidos")
                df = df[valid_mask]
        return df
    
    def _validate_composicoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validações específicas para planilhas de composições."""
        # Verifica se os códigos de composição seguem o padrão esperado
        if 'CODIGO' in df.columns:
            valid_mask = df['CODIGO'].str.len() >= 5
            if not valid_mask.all():
                self.logger.log('warning', f"Removendo {(~valid_mask).sum()} composições com códigos inválidos")
                df = df[valid_mask]
        return df
