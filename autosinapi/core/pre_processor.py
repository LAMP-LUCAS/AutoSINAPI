import pandas as pd
import os
import logging
from pathlib import Path
from autosinapi.exceptions import ProcessingError

logger = logging.getLogger(__name__)

def convert_excel_sheets_to_csv(
    xlsx_full_path: Path,
    sheets_to_convert: list[str],
    output_dir: Path
):
    """
    Converts specific sheets from an XLSX file to CSV, ensuring formulas are read as text.

    Args:
        xlsx_full_path (Path): The full path to the XLSX file.
        sheets_to_convert (list[str]): A list of sheet names to convert.
        output_dir (Path): The directory where the CSV files will be saved.
    """
    logger.info(f"Iniciando pré-processamento do arquivo: {xlsx_full_path}")

    if not xlsx_full_path.exists():
        raise ProcessingError(f"Arquivo XLSX não encontrado: {xlsx_full_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Diretório de saída para CSVs: {output_dir}")

    for sheet in sheets_to_convert:
        try:
            logger.info(f"Processando planilha: '{sheet}'...")
            df = pd.read_excel(
                xlsx_full_path,
                sheet_name=sheet,
                header=None,
                engine='openpyxl',
                engine_kwargs={'data_only': False}
            )

            csv_output_path = output_dir / f"{sheet}.csv"
            df.to_csv(csv_output_path, index=False, header=False, sep=';')
            logger.info(f"Planilha '{sheet}' convertida com sucesso para '{csv_output_path}' (separador: ;)")

        except Exception as e:
            raise ProcessingError(f"Falha ao processar a planilha '{sheet}'. Erro: {e}") from e

if __name__ == "__main__":
    # This part is for testing the module directly
    # Example usage (will not be used by etl_pipeline.py directly)
    # You would need to set up a dummy Excel file and output directory for this to run.
    DUMMY_BASE_PATH = Path("./downloads/2025_07/SINAPI-2025-07-formato-xlsx")
    DUMMY_XLSX_FILENAME = "SINAPI_Referência_2025_07.xlsx"
    DUMMY_SHEETS_TO_CONVERT = ['CSD', 'CCD', 'CSE']
    DUMMY_OUTPUT_DIR = DUMMY_BASE_PATH / ".." / "csv_temp"

    # Create dummy files/dirs for testing if needed
    # DUMMY_BASE_PATH.mkdir(parents=True, exist_ok=True)
    # (Create a dummy SINAPI_Referência_2025_07.xlsx here for testing)

    try:
        convert_excel_sheets_to_csv(
            DUMMY_BASE_PATH / DUMMY_XLSX_FILENAME,
            DUMMY_SHEETS_TO_CONVERT,
            DUMMY_OUTPUT_DIR
        )
        print("Pré-processamento de teste concluído com sucesso.")
    except ProcessingError as e:
        print(f"Erro durante o pré-processamento de teste: {e}")
