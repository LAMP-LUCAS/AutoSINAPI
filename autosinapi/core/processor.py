import os
import zipfile
import pandas as pd
def read_sinapi_file(filepath, sheet_name=None, **kwargs):
    """Lê arquivos .csv, .xlsx ou .zip (com .csv/.xlsx dentro) de forma flexível."""
    ext = os.path.splitext(filepath)[-1].lower()
    if ext == '.csv':
        return pd.read_csv(filepath, **kwargs)
    elif ext == '.xlsx':
        return pd.read_excel(filepath, sheet_name=sheet_name, **kwargs)
    elif ext == '.zip':
        # Procura o primeiro arquivo .csv ou .xlsx dentro do zip
        with zipfile.ZipFile(filepath) as z:
            for name in z.namelist():
                if name.lower().endswith('.csv'):
                    with z.open(name) as f:
                        return pd.read_csv(f, **kwargs)
                elif name.lower().endswith('.xlsx'):
                    with z.open(name) as f:
                        return pd.read_excel(f, sheet_name=sheet_name, **kwargs)
        raise ValueError('Nenhum arquivo .csv ou .xlsx encontrado no zip: ' + filepath)
    else:
        raise ValueError('Formato de arquivo não suportado: ' + ext)
"""
Module responsible for processing SINAPI data.
"""
from typing import Dict, Any
import logging
import pandas as pd
from sqlalchemy import text
from ..exceptions import ProcessingError

class Processor:
    def __init__(self, sinapi_config: Dict[str, Any]):
        """Initialize processor."""
        self.config = sinapi_config
        self.logger = logging.getLogger("autosinapi.processor")
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter('[%(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        
    def process(self, file_path: str, sheet_name=None) -> pd.DataFrame:
        """Processa dados SINAPI a partir de arquivo CSV, XLSX ou ZIP."""
        try:
            df = read_sinapi_file(file_path, sheet_name=sheet_name)
            self.logger.debug(f"Colunas originais: {list(df.columns)}")
            df = self._clean_data(df)
            self.logger.debug(f"Colunas após limpeza: {list(df.columns)}")
            df = self._validate_data(df)
            self.logger.debug(f"Registros válidos: {len(df)}")
            return df
        except Exception as e:
            raise ProcessingError(f"Erro ao processar dados: {str(e)}")

    def process_precos_e_custos(self, xlsx_path: str, engine) -> None:
        """Process prices and costs worksheets."""
        # Preços dos insumos
        precos = pd.read_excel(xlsx_path, sheet_name='SINAPI_mao_de_obra')
        precos.columns = [str(col).strip().upper() for col in precos.columns]
        precos = precos.rename(columns={
            'CÓDIGO': 'insumo_codigo',
            'UF': 'uf',
            'DATA REFERÊNCIA': 'data_referencia',
            'DESONERADO': 'desonerado',
            'PREÇO MEDIANO': 'preco_mediano'
        })
        precos['data_referencia'] = pd.to_datetime(precos['data_referencia'], errors='coerce')
        precos['desonerado'] = precos['desonerado'].astype(bool)
        precos = precos[['insumo_codigo', 'uf', 'data_referencia', 'desonerado', 'preco_mediano']]
        
        try:
            with engine.connect() as conn:
                conn.execute(text('DELETE FROM precos_insumos_mensal'))
            precos.to_sql('precos_insumos_mensal', con=engine, if_exists='append', index=False, method='multi')
        except Exception as e:
            raise ProcessingError(f"Erro ao inserir precos_insumos_mensal: {str(e)}")

        # Custos das composições
        custos = pd.read_excel(xlsx_path, sheet_name='SINAPI_Referência')
        custos.columns = [str(col).strip().upper() for col in custos.columns]
        custos = custos.rename(columns={
            'CÓDIGO': 'composicao_codigo',
            'UF': 'uf',
            'DATA REFERÊNCIA': 'data_referencia',
            'DESONERADO': 'desonerado',
            'CUSTO TOTAL': 'custo_total',
            'PERC. MÃO DE OBRA': 'percentual_mao_de_obra'
        })
        custos['data_referencia'] = pd.to_datetime(custos['data_referencia'], errors='coerce')
        custos['desonerado'] = custos['desonerado'].astype(bool)
        custos = custos[['composicao_codigo', 'uf', 'data_referencia', 'desonerado', 'custo_total', 'percentual_mao_de_obra']]
        
        try:
            with engine.connect() as conn:
                conn.execute(text('DELETE FROM custos_composicoes_mensal'))
            custos.to_sql('custos_composicoes_mensal', con=engine, if_exists='append', index=False, method='multi')
        except Exception as e:
            raise ProcessingError(f"Erro ao inserir custos_composicoes_mensal: {str(e)}")

    def _transform_insumos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform input data for supplies."""
        # Rename columns to standard
        column_map = {
            'CODIGO': 'CODIGO_INSUMO',
            'DESCRICAO': 'DESCRICAO_INSUMO',
            'PRECO_MEDIANO': 'PRECO_MEDIANO'
        }
        df = df.rename(columns=column_map)
        
        # Ensure correct data types
        df['CODIGO_INSUMO'] = df['CODIGO_INSUMO'].astype(str)
        df['PRECO_MEDIANO'] = pd.to_numeric(df['PRECO_MEDIANO'], errors='coerce')
        
        return df
    
    def _transform_composicoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform input data for compositions."""
        # Rename columns to standard
        column_map = {
            'CODIGO_COMPOSICAO': 'CODIGO',
            'DESCRICAO_COMPOSICAO': 'DESCRICAO',
            'CUSTO_TOTAL': 'CUSTO_TOTAL'
        }
        df = df.rename(columns=column_map)
        
        # Ensure correct data types
        df['CODIGO'] = df['CODIGO'].astype(str)
        df['CUSTO_TOTAL'] = pd.to_numeric(df['CUSTO_TOTAL'], errors='coerce')
        
        return df
    
    def process_composicao_itens(self, xlsx_path: str, engine) -> None:
        """Process composition structure."""
        # Read Analítico worksheet
        df = pd.read_excel(xlsx_path, sheet_name=0)
        df.columns = [str(col).strip().upper() for col in df.columns]
        
        # Filter subitems
        subitens = df[df['TIPO ITEM'].str.upper().isin(['INSUMO', 'COMPOSICAO'])].copy()
        subitens['composicao_pai_codigo'] = pd.to_numeric(subitens['CÓDIGO DA COMPOSIÇÃO'], errors='coerce').astype('Int64')
        subitens['item_codigo'] = pd.to_numeric(subitens['CÓDIGO DO ITEM'], errors='coerce').astype('Int64')
        subitens['tipo_item'] = subitens['TIPO ITEM'].str.upper().str.strip()

        # Handle coefficient (may come with comma)
        subitens['coeficiente'] = pd.to_numeric(subitens['COEFICIENTE'].astype(str).str.replace(',', '.'), errors='coerce')

        # Remove duplicates
        subitens = subitens.drop_duplicates(subset=['composicao_pai_codigo', 'item_codigo', 'tipo_item'])

        # Select final columns
        final = subitens[['composicao_pai_codigo', 'item_codigo', 'tipo_item', 'coeficiente']]

        # Insert into database
        try:
            with engine.connect() as conn:
                conn.execute(text('DELETE FROM composicao_itens'))
            final.to_sql('composicao_itens', con=engine, if_exists='append', index=False, method='multi')
        except Exception as e:
            raise ProcessingError(f"Erro ao inserir composicao_itens: {str(e)}")
    
    def process_manutencoes(self, xlsx_path: str, engine) -> dict:
        """Process maintenance worksheet and return status dict."""
        # Read maintenance worksheet
        df = pd.read_excel(xlsx_path, sheet_name=0)
        df.columns = [str(col).strip().upper() for col in df.columns]
        col_map = {
            'REFERENCIA': 'data_referencia',
            'TIPO': 'tipo_item',
            'CÓDIGO': 'item_codigo',
            'CODIGO': 'item_codigo',
            'DESCRIÇÃO': 'descricao_nova',
            'DESCRICAO': 'descricao_nova',
            'MANUTENÇÃO': 'tipo_manutencao',
            'MANUTENCAO': 'tipo_manutencao'
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        # Convert data types
        df['data_referencia'] = pd.to_datetime(df['data_referencia'], errors='coerce').dt.date
        df['item_codigo'] = pd.to_numeric(df['item_codigo'], errors='coerce').astype('Int64')
        df['tipo_item'] = df['tipo_item'].str.upper().str.strip()
        df['tipo_manutencao'] = df['tipo_manutencao'].str.upper().str.strip()

        # Insert into database
        try:
            df.to_sql('manutencoes_historico', con=engine, if_exists='append', index=False, method='multi')
        except Exception as e:
            raise ProcessingError(f"Erro ao inserir manutenções: {str(e)}")

        # Generate latest status
        status_dict = {}
        df_sorted = df.sort_values('data_referencia')
        for _, row in df_sorted.iterrows():
            key = (row['tipo_item'], row['item_codigo'])
            if row['tipo_manutencao'] == 'DESATIVAÇÃO':
                status_dict[key] = 'DESATIVADO'
            elif row['tipo_manutencao'] == 'INCLUSÃO':
                status_dict[key] = 'ATIVO'
            elif row['tipo_manutencao'] == 'ALTERACAO DE DESCRICAO':
                if key not in status_dict:
                    status_dict[key] = 'ATIVO'
        return status_dict

    def process(self, excel_data: bytes) -> pd.DataFrame:
        """Process SINAPI data from Excel file."""
        try:
            # Convert excel_data into a DataFrame
            df = pd.read_excel(excel_data)
            
            # Clean data
            df = self._clean_data(df)
            
            # Basic validation
            df = self._validate_data(df)
            
            # Return processed DataFrame
            return df
            
        except Exception as e:
            raise ProcessingError(f"Erro ao processar dados: {str(e)}")
            
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpa e normaliza os dados, mapeando colunas dinamicamente conforme DataModel."""
        import re
        df = df.copy()
        self.logger.debug("Iniciando limpeza de dados")
        # Remove linhas e colunas totalmente vazias
        df.dropna(how='all', inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
        # Normaliza apenas os títulos das colunas (remove acentos, espaços, caixa alta, caracteres especiais)
        def normalize_col(col):
            import unicodedata
            col = str(col).strip()
            col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('ASCII')
            col = re.sub(r'[^A-Za-z0-9 ]', '', col)
            col = col.replace(' ', '_').upper()
            return col
        df.columns = [normalize_col(col) for col in df.columns]
        # Mapeamento dinâmico para DataModel
        col_map = {
            # Catálogo
            'CODIGO': 'codigo', 'CODIGO_DO_ITEM': 'codigo', 'CODIGO_ITEM': 'codigo',
            'DESCRICAO': 'descricao', 'DESCRICAO_ITEM': 'descricao',
            'UNIDADE': 'unidade', 'UNIDADE_DE_MEDIDA': 'unidade',
            # Preço
            'PRECO_UNITARIO': 'preco_mediano', 'PRECO_MEDIANO': 'preco_mediano',
            # Custos
            'CUSTO_TOTAL': 'custo_total',
            'PERC_MAO_DE_OBRA': 'percentual_mao_de_obra', 'PERC_MAO_OBRA': 'percentual_mao_de_obra',
            # Estrutura
            'CODIGO_DA_COMPOSICAO': 'composicao_pai_codigo',
            'TIPO_ITEM': 'tipo_item',
            'COEFICIENTE': 'coeficiente',
            'CODIGO_DO_ITEM': 'item_codigo',
            # Manutencoes
            'REFERENCIA': 'data_referencia',
            'TIPO': 'tipo_item',
            'MANUTENCAO': 'tipo_manutencao',
            'MANUTENCAO_TIPO': 'tipo_manutencao',
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        # Encapsula descrições/textos em aspas duplas, sem normalizar
        if 'descricao' in df.columns:
            df['descricao'] = df['descricao'].astype(str).apply(lambda x: f'"{x.strip()}"' if not (x.startswith('"') and x.endswith('"')) else x)
        if 'unidade' in df.columns:
            df['unidade'] = df['unidade'].astype(str).apply(lambda x: f'"{x.strip()}"' if not (x.startswith('"') and x.endswith('"')) else x)
        # Converte valores numéricos
        for col in ['preco_mediano', 'custo_total', 'coeficiente']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')
        self.logger.debug(f"Colunas após mapeamento: {list(df.columns)}")
        self.logger.debug("Limpeza de dados concluída")
        return df
            
    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida os dados processados conforme o DataModel, sem remover registros válidos por erro de mapeamento."""
        df = df.copy()
        self.logger.debug("Iniciando validação de dados")
        # Validação básica
        if df.empty:
            raise ProcessingError("DataFrame está vazio após processamento")
        # Campos obrigatórios conforme DataModel
        critical_fields = ['codigo', 'descricao', 'unidade']
        missing_fields = [f for f in critical_fields if f not in df.columns]
        if missing_fields:
            raise ProcessingError(f"Campos obrigatórios ausentes: {missing_fields}")
        df.dropna(subset=critical_fields, how='any', inplace=True)
        # Valida códigos: apenas dígitos, mas não remove se for string numérica válida
        df['codigo'] = df['codigo'].astype(str)
        invalid_codes = df[~df['codigo'].str.match(r'^\d+$', na=False)]
        if not invalid_codes.empty:
            self.logger.warning(f"Removendo {len(invalid_codes)} registros com códigos inválidos")
        df = df[df['codigo'].str.match(r'^\d+$', na=False)].copy()
        # Valida preços se existir
        if 'preco_mediano' in df.columns:
            df['preco_mediano'] = pd.to_numeric(df['preco_mediano'], errors='coerce')
            df.loc[df['preco_mediano'] < 0, 'preco_mediano'] = None
        # Valida textos: mantém descrições encapsuladas, mas remove se for muito curta
        for col in ['descricao', 'unidade']:
            df = df[df[col].astype(str).str.len() > 2].copy()
        df = df.reset_index(drop=True)
        self.logger.debug("Validação de dados concluída")
        return df
        
    def _validate_insumos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate supply data."""
        df = df.copy()
        
        # Validate code length (4-6 digits)
        df['CODIGO_INSUMO'] = df['CODIGO_INSUMO'].astype(str)
        invalid_codes = df[~df['CODIGO_INSUMO'].str.match(r'^\d{4,6}$', na=False)]
        if not invalid_codes.empty:
            self.logger.warning(f"Removendo {len(invalid_codes)} insumos com códigos inválidos")
        df = df[df['CODIGO_INSUMO'].str.match(r'^\d{4,6}$', na=False)]
        
        return df
        
    def _validate_composicoes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida dados de composições, aceitando coluna 'codigo' (minúsculo)."""
        df = df.copy()
        col = 'codigo' if 'codigo' in df.columns else 'CODIGO'
        # Valida código com 6 dígitos
        df[col] = df[col].astype(str)
        invalid_codes = df[~df[col].str.match(r'^\d{6}$', na=False)]
        if not invalid_codes.empty:
            self.logger.warning(f"Removendo {len(invalid_codes)} composições com códigos inválidos")
        df = df[df[col].str.match(r'^\d{6}$', na=False)]
        return df
