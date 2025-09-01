import pandas as pd
from typing import Dict, Any, List
import logging
import re
import unicodedata

from ..exceptions import ProcessingError

class Processor:
    def __init__(self, sinapi_config: Dict[str, Any]):
        self.config = sinapi_config
        self.logger = logging.getLogger("autosinapi.processor")
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter('[%(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def _find_header_row(self, df: pd.DataFrame, keywords: List[str]) -> int:
        """Encontra a linha do cabeçalho em um DataFrame procurando por palavras-chave."""
        def normalize_text(text_val):
            s = str(text_val).strip()
            s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
            s = re.sub(r'[^A-Z0-9_]', '', s.upper().replace(' ', '_'))
            return s

        for i, row in df.iterrows():
            if i > 20: 
                break
            normalized_row_values = [normalize_text(cell) for cell in row.values]
            row_str = ' '.join(normalized_row_values)

            normalized_keywords = [normalize_text(k) for k in keywords]

            if all(nk in row_str for nk in normalized_keywords):
                return i
        return None

    def _normalize_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza os nomes das colunas para um padrão ASCII, maiúsculo e com underscores."""
        new_cols = {}
        for col in df.columns:
            s = str(col).strip()
            s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
            s = re.sub(r'[^A-Z0-9_]', '', s.upper().replace(' ', '_'))
            new_cols[col] = s
        return df.rename(columns=new_cols)

    def _unpivot_data(self, df: pd.DataFrame, id_vars: List[str], value_name: str) -> pd.DataFrame:
        """Transforma o DataFrame de formato largo (UFs em colunas) para longo."""
        uf_cols = [col for col in df.columns if col not in id_vars]
        long_df = df.melt(id_vars=id_vars, value_vars=uf_cols, var_name='uf', value_name=value_name)
        long_df = long_df.dropna(subset=[value_name])
        long_df[value_name] = pd.to_numeric(long_df[value_name], errors='coerce')
        return long_df

    def process_manutencoes(self, xlsx_path: str) -> pd.DataFrame:
        """Lê e processa a planilha de manutenções, retornando um DataFrame limpo."""
        self.logger.info(f"Processando arquivo de manutenções: {xlsx_path}")
        df_raw = pd.read_excel(xlsx_path, sheet_name=0, header=None)
        header_row = self._find_header_row(df_raw, ['REFERENCIA', 'TIPO', 'CODIGO', 'DESCRICAO', 'MANUTENCAO'])
        if header_row is None:
            raise ProcessingError(f"Cabeçalho não encontrado no arquivo de manutenções: {xlsx_path}")

        df = pd.read_excel(xlsx_path, sheet_name=0, header=header_row)
        df = self._normalize_cols(df)

        col_map = {
            'REFERENCIA': 'data_referencia',
            'TIPO': 'tipo_item',
            'CODIGO': 'item_codigo',
            'DESCRICAO': 'descricao_item',
            'MANUTENCAO': 'tipo_manutencao'
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        df['data_referencia'] = pd.to_datetime(df['data_referencia'], errors='coerce').dt.date
        df['item_codigo'] = pd.to_numeric(df['item_codigo'], errors='coerce').astype('Int64')
        df['tipo_item'] = df['tipo_item'].str.upper().str.strip()
        df['tipo_manutencao'] = df['tipo_manutencao'].str.upper().str.strip()

        return df[list(col_map.values())]

    def process_composicao_itens(self, xlsx_path: str) -> Dict[str, pd.DataFrame]:
        """Processa a estrutura de composições (aba analítico)."""

        xls = pd.ExcelFile(xlsx_path)
        
        SINAPI_sheet_names=[]
        for sheet_name in xls.sheet_names:
            SINAPI_sheet_names.append(sheet_name)

        if 'Analítico' in SINAPI_sheet_names:
            sheet_SINAPI_name = 'Analítico'
        else:
            raise ProcessingError(f"Aba 'Analítico' não encontrada no arquivo: {xlsx_path}")

        self.logger.info(f"Processando estrutura de composições de: {xlsx_path}")
        # According to DataModel.md, header for 'Analítico' sheet is on row 9 (index 8)
        df = pd.read_excel(xlsx_path, sheet_name=sheet_SINAPI_name, header=8)
        df = self._normalize_cols(df)

        subitens = df[df['TIPO_ITEM'].str.upper().isin(['INSUMO', 'COMPOSICAO'])].copy()
        subitens['composicao_pai_codigo'] = pd.to_numeric(subitens['CODIGO_DA_COMPOSICAO'], errors='coerce').astype('Int64')
        subitens['item_codigo'] = pd.to_numeric(subitens['CODIGO_DO_ITEM'], errors='coerce').astype('Int64')
        subitens['tipo_item'] = subitens['TIPO_ITEM'].str.upper().str.strip()
        subitens['coeficiente'] = pd.to_numeric(subitens['COEFICIENTE'].astype(str).str.replace(',', '.'), errors='coerce')

        subitens.dropna(subset=['composicao_pai_codigo', 'item_codigo', 'tipo_item'], inplace=True)
        subitens = subitens.drop_duplicates(subset=['composicao_pai_codigo', 'item_codigo', 'tipo_item'])

        insumos_df = subitens[subitens['tipo_item'] == 'INSUMO']
        composicoes_df = subitens[subitens['tipo_item'] == 'COMPOSICAO']

        composicao_insumos = insumos_df[['composicao_pai_codigo', 'item_codigo', 'coeficiente']].rename(columns={'item_codigo': 'insumo_filho_codigo'})
        composicao_subcomposicoes = composicoes_df[['composicao_pai_codigo', 'item_codigo', 'coeficiente']].rename(columns={'item_codigo': 'composicao_filho_codigo'})

        return {
            "composicao_insumos": composicao_insumos,
            "composicao_subcomposicoes": composicao_subcomposicoes
        }

    def process_catalogo_e_precos(self, xlsx_path: str) -> Dict[str, pd.DataFrame]:
        """Processa o arquivo de referência para extrair catálogos e preços/custos."""
        xls = pd.ExcelFile(xlsx_path)
        all_dfs = {}

        sheet_map = {
            "Catálogo de Insumos": "catalogo_insumos",
            "Catálogo de Composições": "catalogo_composicoes",
            "Preços de Insumos": "precos",
            "Custos de Composições": "custos"
        }

        for sheet_name in xls.sheet_names:
            process_type = next((v for k, v in sheet_map.items() if k in sheet_name), None)
            if not process_type:
                continue

            self.logger.info(f"Processando aba: {sheet_name} (tipo: {process_type})")
            df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            header_row = self._find_header_row(df_raw, ['CÓDIGO', 'DESCRIÇÃO'])
            if header_row is None:
                self.logger.warning(f"Cabeçalho não encontrado na aba {sheet_name}. Pulando.")
                continue
            
            df = pd.read_excel(xls, sheet_name=sheet_name, header=header_row)
            df = self._normalize_cols(df)

            if process_type == "catalogo_insumos":
                df = df.rename(columns={'CODIGO': 'codigo', 'DESCRICAO': 'descricao', 'UNIDADE': 'unidade'})
                all_dfs['insumos'] = df[['codigo', 'descricao', 'unidade']]

            elif process_type == "catalogo_composicoes":
                df = df.rename(columns={'CODIGO': 'codigo', 'DESCRICAO_DA_COMPOSICAO': 'descricao', 'UNIDADE': 'unidade'})
                all_dfs['composicoes'] = df[['codigo', 'descricao', 'unidade']]

            elif process_type in ["precos", "custos"]:
                if "DESONERADO" in sheet_name.upper(): regime = "DESONERADO"
                elif "NAO DESONERADO" in sheet_name.upper(): regime = "NAO_DESONERADO"
                else: regime = "SEM_ENCARGOS"
                
                id_vars = ['CODIGO', 'DESCRICAO', 'UNIDADE']
                value_name = 'preco_mediano' if process_type == "precos" else 'custo_total'
                
                long_df = self._unpivot_data(df, id_vars, value_name)
                long_df['regime'] = regime
                
                code_col = 'insumo_codigo' if process_type == "precos" else 'composicao_codigo'
                long_df = long_df.rename(columns={'CODIGO': code_col})
                
                table_name = 'precos_insumos_mensal' if process_type == "precos" else 'custos_composicoes_mensal'
                
                final_cols = [code_col, 'uf', 'regime', value_name]
                if table_name not in all_dfs: all_dfs[table_name] = []
                all_dfs[table_name].append(long_df[final_cols])

        # Concatena os dataframes de preços e custos de diferentes regimes
        if 'precos_insumos_mensal' in all_dfs: 
            all_dfs['precos_insumos_mensal'] = pd.concat(all_dfs['precos_insumos_mensal'], ignore_index=True)
        if 'custos_composicoes_mensal' in all_dfs: 
            all_dfs['custos_composicoes_mensal'] = pd.concat(all_dfs['custos_composicoes_mensal'], ignore_index=True)
            
        return all_dfs
