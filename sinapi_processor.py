"""
Módulo para processamento específico das planilhas SINAPI
"""
from typing import Dict, List, Union
from pathlib import Path
import pandas as pd
from sinapi_utils_new import SinapiLogger, FileManager

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
