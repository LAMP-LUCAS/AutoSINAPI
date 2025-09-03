"""
pre_processador.py: Script para Pré-processamento de Planilhas SINAPI.

Este script é responsável por pré-processar planilhas específicas dos arquivos
Excel do SINAPI, convertendo-as para o formato CSV. O objetivo principal é
garantir que os dados, especialmente aqueles que contêm fórmulas, sejam lidos
como texto simples, evitando problemas de interpretação e garantindo a
integridade dos dados antes do processamento principal pelo `Processor`.

Ele identifica as planilhas necessárias, lê o conteúdo do Excel e salva as
informações em arquivos CSV temporários, que serão posteriormente consumidos
pelo pipeline ETL do AutoSINAPI.
"""
import pandas as pd
import os
import logging

# Configuração básica do logger
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# --- CONFIGURAÇÃO ---
# Caminho base para os arquivos descompactados
BASE_PATH = "downloads/2025_07/SINAPI-2025-07-formato-xlsx"
# Arquivo XLSX de referência
XLSX_FILENAME = "SINAPI_Referência_2025_07.xlsx"
# Planilhas que precisam de pré-processamento
SHEETS_TO_CONVERT = ['CSD', 'CCD', 'CSE']
# Diretório de saída para os CSVs
OUTPUT_DIR = os.path.join(BASE_PATH, "..", "csv_temp")

def pre_process_sheets():
    """
    Converte planilhas específicas de um arquivo XLSX para CSV, garantindo que as fórmulas sejam lidas como texto.
    """
    xlsx_full_path = os.path.join(BASE_PATH, XLSX_FILENAME)
    logging.info(f"Iniciando pré-processamento do arquivo: {xlsx_full_path}")

    if not os.path.exists(xlsx_full_path):
        logging.error(f"Arquivo XLSX não encontrado. Abortando.")
        return

    # Cria o diretório de saída se não existir
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logging.info(f"Diretório de saída para CSVs: {OUTPUT_DIR}")

    for sheet in SHEETS_TO_CONVERT:
        try:
            logging.info(f"Processando planilha: '{sheet}'...")
            # Lê a planilha específica, forçando a leitura de fórmulas como texto
            df = pd.read_excel(
                xlsx_full_path,
                sheet_name=sheet,
                header=None,
                engine='openpyxl',
                engine_kwargs={'data_only': False}
            )

            # Define o caminho de saída para o CSV
            csv_output_path = os.path.join(OUTPUT_DIR, f"{sheet}.csv")
            
            # Salva o DataFrame como CSV usando ponto e vírgula como separador
            df.to_csv(csv_output_path, index=False, header=False, sep=';')
            logging.info(f"Planilha '{sheet}' convertida com sucesso para '{csv_output_path}' (separador: ;)")

        except Exception as e:
            logging.error(f"Falha ao processar a planilha '{sheet}'. Erro: {e}")

    logging.info("Pré-processamento concluído.")

if __name__ == "__main__":
    pre_process_sheets()