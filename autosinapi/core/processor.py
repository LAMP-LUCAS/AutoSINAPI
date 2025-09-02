
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

        def normalize_text(text_val):

            s = str(text_val).strip()

            s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

            s = re.sub(r'[^A-Z0-9_]', '', s.upper().replace(' ', '_').replace('\n', '_'))

            return s

        for i, row in df.iterrows():

            if i > 20: break

            normalized_row_values = [normalize_text(cell) for cell in row.values]

            row_str = ' '.join(normalized_row_values)

            normalized_keywords = [normalize_text(k) for k in keywords]

            if all(nk in row_str for nk in normalized_keywords): return i

        return None



    def _normalize_cols(self, df: pd.DataFrame) -> pd.DataFrame:

        new_cols = {}

        for col in df.columns:

            s = str(col).strip()

            s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

            s = s.upper()

            s = re.sub(r'[\s\n]+', '_', s)

            s = re.sub(r'[^A-Z0-9_]', '', s)

            new_cols[col] = s

        return df.rename(columns=new_cols)



    def _unpivot_data(self, df: pd.DataFrame, id_vars: List[str], value_name: str) -> pd.DataFrame:

        uf_cols = [col for col in df.columns if len(str(col)) == 2 and str(col).isalpha()]

        if not uf_cols:

            self.logger.warning(f"Nenhuma coluna de UF foi identificada para o unpivot na planilha de {value_name}. O DataFrame pode ficar vazio.")

            return pd.DataFrame(columns=id_vars + ['uf', value_name])

        long_df = df.melt(id_vars=id_vars, value_vars=uf_cols, var_name='uf', value_name=value_name)

        long_df = long_df.dropna(subset=[value_name])

        long_df[value_name] = pd.to_numeric(long_df[value_name], errors='coerce')

        return long_df



    def _standardize_id_columns(self, df: pd.DataFrame) -> pd.DataFrame:

        rename_map = {

            'CODIGO_DO_INSUMO': 'CODIGO', 'DESCRICAO_DO_INSUMO': 'DESCRICAO',

            'CODIGO_DA_COMPOSICAO': 'CODIGO', 'DESCRICAO_DA_COMPOSICAO': 'DESCRICAO',

        }

        actual_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}

        return df.rename(columns=actual_rename_map)



    def process_manutencoes(self, xlsx_path: str) -> pd.DataFrame:

        self.logger.info(f"Processando arquivo de manutenções: {xlsx_path}")

        df_raw = pd.read_excel(xlsx_path, sheet_name=0, header=None)

        header_row = self._find_header_row(df_raw, ['REFERENCIA', 'TIPO', 'CODIGO', 'DESCRICAO', 'MANUTENCAO'])

        if header_row is None: raise ProcessingError(f"Cabeçalho não encontrado no arquivo de manutenções: {xlsx_path}")

        df = pd.read_excel(xlsx_path, sheet_name=0, header=header_row)

        df = self._normalize_cols(df)

        col_map = {

            'REFERENCIA': 'data_referencia', 'TIPO': 'tipo_item', 'CODIGO': 'item_codigo',

            'DESCRICAO': 'descricao_item', 'MANUTENCAO': 'tipo_manutencao'

        }

        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        df['data_referencia'] = pd.to_datetime(df['data_referencia'], errors='coerce', format='%m/%Y').dt.date

        df['item_codigo'] = pd.to_numeric(df['item_codigo'], errors='coerce').astype('Int64')

        df['tipo_item'] = df['tipo_item'].str.upper().str.strip()

        df['tipo_manutencao'] = df['tipo_manutencao'].str.upper().str.strip()

        return df[list(col_map.values())]



    def process_composicao_itens(self, xlsx_path: str) -> Dict[str, pd.DataFrame]:

        xls = pd.ExcelFile(xlsx_path)

        sheet_SINAPI_name = next((s for s in xls.sheet_names if 'Analítico' in s), None)

        if not sheet_SINAPI_name: raise ProcessingError(f"Aba 'Analítico' não encontrada no arquivo: {xlsx_path}")

        self.logger.info(f"Processando estrutura de composições de: {xlsx_path} > {sheet_SINAPI_name}")

        df = pd.read_excel(xlsx_path, sheet_name=sheet_SINAPI_name, header=9)

        df = self._normalize_cols(df)

        subitens = df[df['TIPO_ITEM'].str.upper().isin(['INSUMO', 'COMPOSICAO'])].copy()

        subitens['composicao_pai_codigo'] = pd.to_numeric(subitens['CODIGO_DA_COMPOSICAO'], errors='coerce').astype('Int64')

        subitens['item_codigo'] = pd.to_numeric(subitens['CODIGO_DO_ITEM'], errors='coerce').astype('Int64')

        subitens['tipo_item'] = subitens['TIPO_ITEM'].str.upper().str.strip()

        subitens['coeficiente'] = pd.to_numeric(subitens['COEFICIENTE'].astype(str).str.replace(',', '.'), errors='coerce')

        subitens.rename(columns={'DESCRICAO': 'item_descricao', 'UNIDADE': 'item_unidade'}, inplace=True)

        subitens.dropna(subset=['composicao_pai_codigo', 'item_codigo', 'tipo_item'], inplace=True)

        subitens = subitens.drop_duplicates(subset=['composicao_pai_codigo', 'item_codigo', 'tipo_item'])

        insumos_df = subitens[subitens['tipo_item'] == 'INSUMO']

        composicoes_df = subitens[subitens['tipo_item'] == 'COMPOSICAO']

        composicao_insumos = insumos_df[['composicao_pai_codigo', 'item_codigo', 'coeficiente']].rename(columns={'item_codigo': 'insumo_filho_codigo'})

        composicao_subcomposicoes = composicoes_df[['composicao_pai_codigo', 'item_codigo', 'coeficiente']].rename(columns={'item_codigo': 'composicao_filho_codigo'})

        parent_composicoes_df = df[df['CODIGO_DA_COMPOSICAO'].notna() & ~df['TIPO_ITEM'].str.upper().isin(['INSUMO', 'COMPOSICAO'])].copy()

        parent_composicoes_df = parent_composicoes_df.rename(columns={'CODIGO_DA_COMPOSICAO': 'codigo', 'DESCRICAO': 'descricao', 'UNIDADE': 'unidade'})

        parent_composicoes_df = parent_composicoes_df[['codigo', 'descricao', 'unidade']].drop_duplicates(subset=['codigo'])

        child_item_details = subitens[['item_codigo', 'tipo_item', 'item_descricao', 'item_unidade']].copy()

        child_item_details.rename(columns={'item_codigo': 'codigo', 'tipo_item': 'tipo', 'item_descricao': 'descricao', 'item_unidade': 'unidade'}, inplace=True)

        child_item_details = child_item_details.drop_duplicates(subset=['codigo', 'tipo'])

        return {

            "composicao_insumos": composicao_insumos, "composicao_subcomposicoes": composicao_subcomposicoes,

            "parent_composicoes_details": parent_composicoes_df, "child_item_details": child_item_details

        }



    def process_catalogo_e_precos(self, xlsx_path: str) -> Dict[str, pd.DataFrame]:

        xls = pd.ExcelFile(xlsx_path)

        all_dfs = {}

        sheet_map = {

            'ISD': ('precos', 'NAO_DESONERADO'), 'ICD': ('precos', 'DESONERADO'), 'ISE': ('precos', 'SEM_ENCARGOS'),

            'CSD': ('custos', 'NAO_DESONERADO'), 'CCD': ('custos', 'DESONERADO'), 'CSE': ('custos', 'SEM_ENCARGOS'),

            'Catálogo de Insumos': ('catalogo_insumos', None),

            'Catálogo de Composições': ('catalogo_composicoes', None)

        }

        temp_insumos = []

        temp_composicoes = []



        for sheet_name in xls.sheet_names:

            process_key = next((key for key in sheet_map if key in sheet_name), None)

            if not process_key: continue

           

            try:

                process_type, regime = sheet_map[process_key]

                self.logger.info(f"Processando aba: '{sheet_name}' (tipo: {process_type}, regime: {regime or 'N/A'})")



                if process_type in ["precos", "catalogo_insumos"]:

                    df = pd.read_excel(xls, sheet_name=sheet_name, header=9)

                elif process_type in ["custos", "catalogo_composicoes"]:

                    df = pd.read_excel(xls, sheet_name=sheet_name, header=[8, 9])

                   

                    # --- LOG DE DEPURAÇÃO 1: COLUNAS ORIGINAIS ---

                    self.logger.debug(f"Colunas originais (MultiIndex) da aba '{sheet_name}': {df.columns.to_list()}")



                    new_cols = []

                    last_uf = ''

                    for col in df.columns:

                        level0, level1 = str(col[0]), str(col[1])

                        # A UF está na primeira parte do tupla

                        uf_match = re.match(r'^[A-Z]{2}$', level0)

                        if uf_match:

                            last_uf = uf_match.group(0)

                        # As colunas de ID começam com 'Unnamed' no primeiro nível

                        elif 'Unnamed' in level0:

                            last_uf = '' # Reseta para não contaminar as colunas de ID

                       

                        if last_uf:

                            new_cols.append(f"{last_uf}_{level1}")

                        else:

                            new_cols.append(level1)

                   

                    df.columns = new_cols

                    # Remove linhas completamente vazias que podem ter sido lidas

                    df.dropna(how='all', inplace=True)

               

                # --- LOG DE DEPURAÇÃO 2: COLUNAS APÓS ACHATAMENTO/LEITURA ---

                self.logger.debug(f"Colunas da aba '{sheet_name}' após primeira leitura/achatamento: {df.columns.to_list()}")

               

                df = self._normalize_cols(df)

                # --- LOG DE DEPURAÇÃO 3: COLUNAS APÓS NORMALIZAÇÃO ---

                self.logger.debug(f"Colunas da aba '{sheet_name}' após normalização: {df.columns.to_list()}")

               

                df = self._standardize_id_columns(df)

                # --- LOG DE DEPURAÇÃO 4: COLUNAS APÓS PADRONIZAÇÃO DE IDS ---

                self.logger.debug(f"Colunas da aba '{sheet_name}' após padronização de IDs: {df.columns.to_list()}")

               

                if process_type in ["precos", "catalogo_insumos"]:

                    if 'CODIGO' in df.columns and 'DESCRICAO' in df.columns:

                        temp_insumos.append(df[['CODIGO', 'DESCRICAO', 'UNIDADE']].copy())

                    if process_type == "precos":

                        long_df = self._unpivot_data(df, ['CODIGO'], 'preco_mediano')

                elif process_type in ["custos", "catalogo_composicoes"]:

                    if 'CODIGO' in df.columns and 'DESCRICAO' in df.columns:

                        temp_composicoes.append(df[['CODIGO', 'DESCRICAO', 'UNIDADE']].copy())

                    if process_type == "custos":

                        cost_cols = {col.split('_')[0]: col for col in df.columns if 'CUSTO' in col and len(col.split('_')[0]) == 2}

                        if 'CODIGO' in df.columns:

                            df_costs = df[['CODIGO'] + list(cost_cols.values())].copy()

                            df_costs = df_costs.rename(columns=lambda x: x.split('_')[0] if 'CUSTO' in x else x)

                            long_df = self._unpivot_data(df_costs, ['CODIGO'], 'custo_total')

                        else:

                            self.logger.warning(f"Coluna 'CODIGO' não encontrada na aba '{sheet_name}' após processamento. Pulando extração de custos.")

                            continue



                if process_type in ["precos", "custos"]:

                    long_df['regime'] = regime

                    code_col = 'insumo_codigo' if process_type == "precos" else 'composicao_codigo'

                    long_df = long_df.rename(columns={'CODIGO': code_col})

                    table_name = 'precos_insumos_mensal' if process_type == "precos" else 'custos_composicoes_mensal'

                    if table_name not in all_dfs: all_dfs[table_name] = []

                    all_dfs[table_name].append(long_df)

            except Exception as e:

                self.logger.error(f"Falha CRÍTICA ao processar a aba '{sheet_name}'. Esta aba será ignorada. Erro: {e}", exc_info=True)

                continue



        if temp_insumos:

            all_insumos = pd.concat(temp_insumos, ignore_index=True).drop_duplicates(subset=['CODIGO'])

            all_dfs['insumos'] = all_insumos.rename(columns={'CODIGO': 'codigo', 'DESCRICAO': 'descricao', 'UNIDADE': 'unidade'})

        if temp_composicoes:

            all_composicoes = pd.concat(temp_composicoes, ignore_index=True).drop_duplicates(subset=['CODIGO'])

            all_dfs['composicoes'] = all_composicoes.rename(columns={'CODIGO': 'codigo', 'DESCRICAO': 'descricao', 'UNIDADE': 'unidade'})

        if 'precos_insumos_mensal' in all_dfs:

            all_dfs['precos_insumos_mensal'] = pd.concat(all_dfs['precos_insumos_mensal'], ignore_index=True)

        if 'custos_composicoes_mensal' in all_dfs:

            all_dfs['custos_composicoes_mensal'] = pd.concat(all_dfs['custos_composicoes_mensal'], ignore_index=True)

           

        return all_dfs

