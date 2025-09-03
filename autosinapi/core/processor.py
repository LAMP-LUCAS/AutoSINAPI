"""
Módulo de Processamento do AutoSINAPI.

Este módulo é responsável por todas as etapas de transformação e limpeza dos dados
brutos do SINAPI, obtidos pelo módulo `downloader`. Ele lida com a leitura de
arquivos Excel, padronização de nomes de colunas, tratamento de valores ausentes,
e a estruturação dos dados em DataFrames do Pandas para que estejam prontos
para inserção no banco de dados pelo módulo `database`.

A classe `Processor` encapsula a lógica de negócio para interpretar as planilhas
do SINAPI, extrair informações relevantes e aplicar as regras de negócio
necessárias para a consistência dos dados.
"""

import logging
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from ..exceptions import ProcessingError

# Configuração do logger para este módulo
logger = logging.getLogger(__name__)


class Processor:
    def __init__(self, sinapi_config: Dict[str, Any]):
        self.config = sinapi_config
        self.logger = logger
        self.logger.info("[__init__] Processador inicializado.")

    def _find_header_row(self, df: pd.DataFrame, keywords: List[str]) -> int:
        self.logger.debug(
            f"[_find_header_row] Procurando cabeçalho com keywords: {keywords}"
        )

        def normalize_text(text_val):
            s = str(text_val).strip()
            s = "".join(
                c
                for c in unicodedata.normalize("NFD", s)
                if unicodedata.category(c) != "Mn"
            )
            s = re.sub(
                r"[^A-Z0-9_]", "", s.upper().replace(" ", "_").replace("\n", "_")
            )
            return s

        for i, row in df.iterrows():
            if i > 20:  # Limite de busca para evitar varrer o arquivo inteiro
                self.logger.warning(
                    "[_find_header_row] Limite de busca por cabeçalho (20 linhas)"
                    "atingido. Cabeçalho não encontrado."
                )
                break

            try:
                row_values = [
                    str(cell) if pd.notna(cell) else "" for cell in row.values
                ]
                normalized_row_values = [normalize_text(cell) for cell in row_values]
                row_str = " ".join(normalized_row_values)
                normalized_keywords = [normalize_text(k) for k in keywords]

                self.logger.debug(
                    f"[_find_header_row] Linha {i} normalizada para busca: {row_str}"
                )

                if all(nk in row_str for nk in normalized_keywords):
                    self.logger.info(
                        f"[_find_header_row] Cabeçalho encontrado na linha {i}."
                    )
                    return i
            except Exception as e:
                self.logger.error(
                    f"[_find_header_row] Erro ao processar a linha {i} "
                    f"para encontrar o cabeçalho: {e}",
                    exc_info=True,
                )
                continue

        self.logger.error(
            f"[_find_header_row] Cabeçalho com as keywords {keywords} "
            f"não foi encontrado."
        )
        return None

    def _normalize_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.debug("[_normalize_cols] Normalizando nomes das colunas...")
        new_cols = {}
        for col in df.columns:
            s = str(col).strip()
            s = "".join(
                c
                for c in unicodedata.normalize("NFD", s)
                if unicodedata.category(c) != "Mn"
            )
            s = s.upper()
            s = re.sub(r"[\s\n]+", "_", s)
            s = re.sub(r"[^A-Z0-9_]", "", s)
            new_cols[col] = s

        self.logger.debug(
            f"[_normalize_cols] Mapeamento de colunas normalizadas: {new_cols}"
        )
        return df.rename(columns=new_cols)

    def _unpivot_data(
        self, df: pd.DataFrame, id_vars: List[str], value_name: str
    ) -> pd.DataFrame:
        self.logger.debug(
            f"[_unpivot_data] Iniciando unpivot para '{value_name}' "
            f"com id_vars: {id_vars}"
        )

        uf_cols = [
            col for col in df.columns if len(str(col)) == 2 and str(col).isalpha()
        ]
        if not uf_cols:
            self.logger.warning(
                f"[_unpivot_data] Nenhuma coluna de UF foi identificada "
                f"para o unpivot na planilha de {value_name}."
                f" O DataFrame pode ficar vazio."
            )
            return pd.DataFrame(columns=id_vars + ["uf", value_name])

        self.logger.debug(
            f"[_unpivot_data] Colunas de UF identificadas para unpivot: {uf_cols}"
        )

        long_df = df.melt(
            id_vars=id_vars, value_vars=uf_cols, var_name="uf", value_name=value_name
        )
        long_df = long_df.dropna(subset=[value_name])
        long_df[value_name] = pd.to_numeric(long_df[value_name], errors="coerce")

        self.logger.debug(
            f"[_unpivot_data] DataFrame após unpivot. Head:\n{long_df.head().to_string()}"
        )
        return long_df

    def _standardize_id_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.debug(
            "[_standardize_id_columns] Padronizando colunas de ID (CODIGO, DESCRICAO)..."
        )
        rename_map = {
            "CODIGO_DO_INSUMO": "CODIGO",
            "DESCRICAO_DO_INSUMO": "DESCRICAO",
            "CODIGO_DA_COMPOSICAO": "CODIGO",
            "DESCRICAO_DA_COMPOSICAO": "DESCRICAO",
        }
        actual_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
        if actual_rename_map:
            self.logger.debug(
                f"[_standardize_id_columns] Mapeamento de renomeação de ID aplicado: {actual_rename_map}"
            )
        return df.rename(columns=actual_rename_map)

    def process_manutencoes(self, xlsx_path: str) -> pd.DataFrame:
        self.logger.info(
            f"[process_manutencoes] Processando arquivo de manutenções: {xlsx_path}"
        )
        try:
            df_raw = pd.read_excel(xlsx_path, sheet_name=0, header=None)
            header_row = self._find_header_row(
                df_raw, ["REFERENCIA", "TIPO", "CODIGO", "DESCRICAO", "MANUTENCAO"]
            )
            if header_row is None:
                raise ProcessingError(
                    f"Cabeçalho não encontrado no arquivo de manutenções: {xlsx_path}"
                )

            df = pd.read_excel(xlsx_path, sheet_name=0, header=header_row)
            df = self._normalize_cols(df)

            col_map = {
                "REFERENCIA": "data_referencia",
                "TIPO": "tipo_item",
                "CODIGO": "item_codigo",
                "DESCRICAO": "descricao_item",
                "MANUTENCAO": "tipo_manutencao",
            }
            df = df.rename(
                columns={k: v for k, v in col_map.items() if k in df.columns}
            )

            df["data_referencia"] = pd.to_datetime(
                df["data_referencia"], errors="coerce", format="%m/%Y"
            ).dt.date
            df["item_codigo"] = pd.to_numeric(
                df["item_codigo"], errors="coerce"
            ).astype("Int64")
            df["tipo_item"] = df["tipo_item"].str.upper().str.strip()
            df["tipo_manutencao"] = df["tipo_manutencao"].str.upper().str.strip()

            self.logger.info(
                "[process_manutencoes] Processamento de manutenções concluído com sucesso."
            )
            return df[list(col_map.values())]
        except Exception as e:
            self.logger.error(
                f"[process_manutencoes] Falha crítica ao processar arquivo de manutenções. Erro: {e}",
                exc_info=True,
            )
            raise ProcessingError(f"Erro em 'process_manutencoes': {e}")

    def process_composicao_itens(self, xlsx_path: str) -> Dict[str, pd.DataFrame]:
        self.logger.info(
            f"[process_composicao_itens] Processando estrutura de itens de composição de: {xlsx_path}"
        )
        try:
            xls = pd.ExcelFile(xlsx_path)
            sheet_SINAPI_name = next(
                (s for s in xls.sheet_names if "Analítico" in s and "Custo" not in s),
                None,
            )
            if not sheet_SINAPI_name:
                raise ProcessingError(
                    f"Aba 'Analítico' não encontrada no arquivo: {xlsx_path}"
                )

            self.logger.info(
                f"[process_composicao_itens] Lendo aba: {sheet_SINAPI_name}"
            )
            df = pd.read_excel(xlsx_path, sheet_name=sheet_SINAPI_name, header=9)
            df = self._normalize_cols(df)

            subitens = df[
                df["TIPO_ITEM"].str.upper().isin(["INSUMO", "COMPOSICAO"])
            ].copy()

            subitens["composicao_pai_codigo"] = pd.to_numeric(
                subitens["CODIGO_DA_COMPOSICAO"], errors="coerce"
            ).astype("Int64")
            subitens["item_codigo"] = pd.to_numeric(
                subitens["CODIGO_DO_ITEM"], errors="coerce"
            ).astype("Int64")
            subitens["tipo_item"] = subitens["TIPO_ITEM"].str.upper().str.strip()
            subitens["coeficiente"] = pd.to_numeric(
                subitens["COEFICIENTE"].astype(str).str.replace(",", "."),
                errors="coerce",
            )
            subitens.rename(
                columns={"DESCRICAO": "item_descricao", "UNIDADE": "item_unidade"},
                inplace=True,
            )

            subitens.dropna(
                subset=["composicao_pai_codigo", "item_codigo", "tipo_item"],
                inplace=True,
            )
            subitens = subitens.drop_duplicates(
                subset=["composicao_pai_codigo", "item_codigo", "tipo_item"]
            )

            insumos_df = subitens[subitens["tipo_item"] == "INSUMO"]
            composicoes_df = subitens[subitens["tipo_item"] == "COMPOSICAO"]

            self.logger.info(
                f"[process_composicao_itens] Encontrados {len(insumos_df)} links insumo-composição e {len(composicoes_df)} links subcomposição-composição."
            )

            composicao_insumos = insumos_df[
                ["composicao_pai_codigo", "item_codigo", "coeficiente"]
            ].rename(columns={"item_codigo": "insumo_filho_codigo"})
            composicao_subcomposicoes = composicoes_df[
                ["composicao_pai_codigo", "item_codigo", "coeficiente"]
            ].rename(columns={"item_codigo": "composicao_filho_codigo"})

            parent_composicoes_df = df[
                df["CODIGO_DA_COMPOSICAO"].notna()
                & ~df["TIPO_ITEM"].str.upper().isin(["INSUMO", "COMPOSICAO"])
            ].copy()
            parent_composicoes_df = parent_composicoes_df.rename(
                columns={
                    "CODIGO_DA_COMPOSICAO": "codigo",
                    "DESCRICAO": "descricao",
                    "UNIDADE": "unidade",
                }
            )
            parent_composicoes_df = parent_composicoes_df[
                ["codigo", "descricao", "unidade"]
            ].drop_duplicates(subset=["codigo"])

            child_item_details = subitens[
                ["item_codigo", "tipo_item", "item_descricao", "item_unidade"]
            ].copy()
            child_item_details.rename(
                columns={
                    "item_codigo": "codigo",
                    "tipo_item": "tipo",
                    "item_descricao": "descricao",
                    "item_unidade": "unidade",
                },
                inplace=True,
            )
            child_item_details = child_item_details.drop_duplicates(
                subset=["codigo", "tipo"]
            )

            return {
                "composicao_insumos": composicao_insumos,
                "composicao_subcomposicoes": composicao_subcomposicoes,
                "parent_composicoes_details": parent_composicoes_df,
                "child_item_details": child_item_details,
            }
        except Exception as e:
            self.logger.error(
                f"[process_composicao_itens] Falha crítica ao processar estrutura de composições. Erro: {e}",
                exc_info=True,
            )
            raise ProcessingError(f"Erro em 'process_composicao_itens': {e}")

    def _process_precos_sheet(
        self, xls: pd.ExcelFile, sheet_name: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Processa uma aba de preços de insumos ou catálogo de insumos."""
        df = pd.read_excel(xls, sheet_name=sheet_name, header=9)
        df = self._normalize_cols(df)
        df = self._standardize_id_columns(df)

        catalogo_df = pd.DataFrame()
        if "CODIGO" in df.columns and "DESCRICAO" in df.columns:
            catalogo_df = df[["CODIGO", "DESCRICAO", "UNIDADE"]].copy()

        long_df = self._unpivot_data(df, ["CODIGO"], "preco_mediano")
        return long_df, catalogo_df

    def _process_custos_sheet(
        self, xlsx_path: str, process_key: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Processa uma aba de custos de composição a partir de um CSV."""
        csv_dir = Path(xlsx_path).parent.parent / "csv_temp"
        csv_path = csv_dir / f"{process_key}.csv"
        self.logger.info(
            f"Lendo dados de custo do arquivo CSV pré-processado: {csv_path}"
        )
        if not csv_path.exists():
            raise FileNotFoundError(f"Arquivo CSV não encontrado: {csv_path}.")

        df_raw = pd.read_csv(csv_path, header=None, low_memory=False, sep=";")
        header_row = self._find_header_row(
            df_raw, ["Código da Composição", "Descrição", "Unidade"]
        )
        if header_row is None:
            self.logger.warning(f"Cabeçalho não encontrado em {csv_path.name}. Pulando.")
            return pd.DataFrame(), pd.DataFrame()

        # Constrói o cabeçalho multi-nível e lê os dados
        header_df = df_raw.iloc[header_row - 1 : header_row + 1].copy()

        def clean_level0(val):
            s_val = str(val)
            return s_val if len(s_val) == 2 and s_val.isalpha() else pd.NA

        header_df.iloc[0] = header_df.iloc[0].apply(clean_level0).ffill()
        new_cols = [
            f"{h0}_{h1}" if pd.notna(h0) else str(h1)
            for h0, h1 in zip(header_df.iloc[0], header_df.iloc[1])
        ]
        df = df_raw.iloc[header_row + 1 :].copy()
        df.columns = new_cols
        df.dropna(how="all", inplace=True)

        # Normalização e extração de código
        df = self._normalize_cols(df)
        df = self._standardize_id_columns(df)
        if "CODIGO" in df.columns:
            df["CODIGO"] = df["CODIGO"].astype(str).str.extract(r",(\d+)\)$")[0]
            df["CODIGO"] = pd.to_numeric(df["CODIGO"], errors="coerce")
            df.dropna(subset=["CODIGO"], inplace=True)
            if not df.empty:
                df["CODIGO"] = df["CODIGO"].astype("Int64")

        # Extração de catálogo e custos
        catalogo_df = pd.DataFrame()
        if "CODIGO" in df.columns and "DESCRICAO" in df.columns:
            catalogo_df = df[["CODIGO", "DESCRICAO", "UNIDADE"]].copy()

        cost_cols = {
            col.split("_")[0]: col
            for col in df.columns
            if "CUSTO" in col and len(col.split("_")[0]) == 2
        }
        if "CODIGO" in df.columns and cost_cols:
            df_costs = df[["CODIGO"] + list(cost_cols.values())].copy()
            df_costs = df_costs.rename(
                columns=lambda x: x.split("_")[0] if "CUSTO" in x else x
            )
            long_df = self._unpivot_data(df_costs, ["CODIGO"], "custo_total")
            return long_df, catalogo_df

        self.logger.warning(f"Não foi possível extrair custos da aba '{process_key}'.")
        return pd.DataFrame(), pd.DataFrame()

    def _aggregate_final_dataframes(
        self, all_dfs: Dict, temp_insumos: List, temp_composicoes: List
    ) -> Dict:
        """Agrega os DataFrames temporários nos resultados finais."""
        self.logger.info("Agregando e finalizando DataFrames...")
        if temp_insumos:
            all_insumos = pd.concat(
                temp_insumos, ignore_index=True
            ).drop_duplicates(subset=["CODIGO"])
            all_dfs["insumos"] = all_insumos.rename(
                columns={
                    "CODIGO": "codigo", "DESCRICAO": "descricao", "UNIDADE": "unidade"
                }
            )
            self.logger.info(
                f"Catálogo de insumos finalizado com {len(all_insumos)} registros únicos."
            )
        if temp_composicoes:
            all_composicoes = pd.concat(
                temp_composicoes, ignore_index=True
            ).drop_duplicates(subset=["CODIGO"])
            all_dfs["composicoes"] = all_composicoes.rename(
                columns={
                    "CODIGO": "codigo", "DESCRICAO": "descricao", "UNIDADE": "unidade"
                }
            )
            self.logger.info(
                f"Catálogo de composições finalizado com {len(all_composicoes)} registros únicos."
            )

        # Concatena dados mensais
        if "precos_insumos_mensal" in all_dfs:
            df_concat = pd.concat(all_dfs["precos_insumos_mensal"], ignore_index=True)
            all_dfs["precos_insumos_mensal"] = df_concat
            self.logger.info(
                f"Tabela de preços mensais finalizada com {len(df_concat)} registros."
            )
        if "custos_composicoes_mensal" in all_dfs:
            df_concat = pd.concat(all_dfs["custos_composicoes_mensal"], ignore_index=True)
            all_dfs["custos_composicoes_mensal"] = df_concat
            self.logger.info(
                f"Tabela de custos mensais finalizada com {len(df_concat)} registros."
            )
        return all_dfs

    def process_catalogo_e_precos(self, xlsx_path: str) -> Dict[str, pd.DataFrame]:
        self.logger.info(
            f"Iniciando processamento completo de catálogos e preços de: {xlsx_path}"
        )
        xls = pd.ExcelFile(xlsx_path)
        all_dfs = {}
        sheet_map = {
            "ISD": ("precos", "NAO_DESONERADO"),
            "ICD": ("precos", "DESONERADO"),
            "ISE": ("precos", "SEM_ENCARGOS"),
            "CSD": ("custos", "NAO_DESONERADO"),
            "CCD": ("custos", "DESONERADO"),
            "CSE": ("custos", "SEM_ENCARGOS"),
        }
        temp_insumos, temp_composicoes = [], []

        for sheet_name in xls.sheet_names:
            process_key = next((k for k in sheet_map if k in sheet_name), None)
            if not process_key:
                continue

            try:
                process_type, regime = sheet_map[process_key]
                self.logger.info(
                    f"Processando aba: '{sheet_name}' (tipo: {process_type}, regime: {regime})"
                )

                long_df, catalogo_df = pd.DataFrame(), pd.DataFrame()
                if process_type == "precos":
                    long_df, catalogo_df = self._process_precos_sheet(xls, sheet_name)
                    if not catalogo_df.empty:
                        temp_insumos.append(catalogo_df)
                
                elif process_type == "custos":
                    long_df, catalogo_df = self._process_custos_sheet(
                        xlsx_path, process_key
                    )
                    if not catalogo_df.empty:
                        temp_composicoes.append(catalogo_df)

                # Adiciona dados mensais processados ao dicionário
                if not long_df.empty:
                    long_df["regime"] = regime
                    table, code = (
                        ("precos_insumos_mensal", "insumo_codigo")
                        if process_type == "precos"
                        else ("custos_composicoes_mensal", "composicao_codigo")
                    )
                    long_df.rename(columns={"CODIGO": code}, inplace=True)
                    all_dfs.setdefault(table, []).append(long_df)
                    self.logger.info(f"Dados da aba '{sheet_name}' adicionados à chave '{table}'.")

            except Exception as e:
                self.logger.error(
                    f"Falha CRÍTICA ao processar a aba '{sheet_name}'. "
                    f"Esta aba será ignorada. Erro: {e}",
                    exc_info=True,
                )
        
        return self._aggregate_final_dataframes(all_dfs, temp_insumos, temp_composicoes)

