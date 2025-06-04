# sinap_webscraping.py
import json
from datetime import datetime, timedelta
import os
from tools.rastreador_xlsx import scan_excel_directory, format_output
from pathlib import Path

LOG_FILE = "sinap_webscraping_download_log.json"
TEMPO_ESPERA_MINUTOS = 10  # tempo mínimo entre downloads do mesmo arquivo

def pode_fazer_download(ano, mes):
    """Verifica se já passou o tempo mínimo desde o último download desse arquivo."""
    chave = f"{ano}_{mes}"
    agora = datetime.now()
    if not os.path.exists(LOG_FILE):
        return True
    try:
        with open(LOG_FILE, "r") as f:
            log = json.load(f)
        ultimo = log.get(chave)
        if ultimo:
            ultimo_dt = datetime.fromisoformat(ultimo)
            if agora - ultimo_dt < timedelta(minutes=TEMPO_ESPERA_MINUTOS):
                tempo_restante = timedelta(minutes=TEMPO_ESPERA_MINUTOS) - (agora - ultimo_dt)
                print(f"Download recente detectado para {chave}. Aguarde {tempo_restante} antes de tentar novamente.")
                return False
    except Exception as e:
        print(f"Erro ao ler log: {e}")
    return True

def registra_download(ano, mes):
    """Registra a data/hora do último download desse arquivo."""
    chave = f"{ano}_{mes}"
    agora = datetime.now().isoformat()
    log = {}
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r") as f:
                log = json.load(f)
        except Exception:
            pass
    log[chave] = agora
    with open(LOG_FILE, "w") as f:
        json.dump(log, f)

def download_sinapi_zip(ano, mes, formato='xlsx'):
    import requests
    import os
    import time

    if not pode_fazer_download(ano, mes):
        return

    # Remove espaços em branco antes/depois
    ano = ano.strip()
    mes = mes.strip()
    # Valida se são números
    try:
        int(ano)
        int(mes)
    except ValueError:
        raise ValueError("Ano e mês devem ser números.")
    if len(ano) != 4 or len(mes) != 2:
        raise ValueError("Ano deve ter 4 dígitos e mês deve ter 2 dígitos.")
    if int(mes) < 1 or int(mes) > 12:
        raise ValueError("Mês deve estar entre 1 e 12.")
    if int(ano) < 2025:
        raise ValueError("Ano deve ser maior que 2025.")

    url = f'https://www.caixa.gov.br/Downloads/sinapi-relatorios-mensais/SINAPI-{ano}-{mes}-formato-{formato}.zip'
    folder_name = f'{ano}_{mes}'
    # https://www.caixa.gov.br/Downloads/sinapi-relatorios-mensais/SINAPI-2025-03-formato-xlsx.zip
    # https://www.caixa.gov.br/Downloads/sinapi-relatorios-mensais/SINAPI-2025-03-formato-xlsx.zip
    # https://www.caixa.gov.br/Downloads/sinapi-relatorios-mensais/SINAPI-2025-02-formato-xlsx.zip
    # Verifica se a pasta existe, se não existir, cria a pasta
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    
    zip_file_path = os.path.join(folder_name, f'SINAPI-{ano}-{mes}-formato-{formato}.zip')
    if os.path.exists(zip_file_path):
        print(f'Arquivo já existe: {zip_file_path}')
        return

    try:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        session.mount('https://', adapter)
        response = session.get(url, timeout=30, allow_redirects=True)
        if response.status_code == 200:
            with open(zip_file_path, 'wb') as file:
                file.write(response.content)
            print(f'Download completed: {zip_file_path}')
            registra_download(ano, mes)
        else:
            print(f'Arquivo não encontrado ou erro: {response.status_code}')
    except requests.exceptions.TooManyRedirects:
        print('Muitos redirecionamentos. O arquivo pode não existir ou o servidor está protegendo contra acessos repetidos.')
    except requests.exceptions.RequestException as e:
        print(f'Erro de rede: {e}')
        s = 30
        print('Aguardando {s} segundos antes de tentar novamente...')
        time.sleep(s)
        print(f'Failed to download file: {response.status_code}')

def unzip_sinapi_file(ano, mes,formato='xlsx'):
    import zipfile
    import os

    nome_zip = f'SINAPI-{ano}-{mes}-formato-{formato}.zip'
    nome_pasta = f'SINAPI-{ano}-{mes}-formato-{formato}'
    base_folder_name = f'{ano}_{mes}'
    extraction_path = os.path.join(base_folder_name, nome_pasta)
    zip_file_path = os.path.join(base_folder_name, nome_zip)

    #print(f'Variáveis definidas: zip_file_path={zip_file_path}, base_folder_name={base_folder_name}, extraction_path={extraction_path}, nome_zip={nome_zip}, nome_pasta={nome_pasta}')
    
    if not os.path.exists(zip_file_path):
        print(f'Zip file does not exist: {zip_file_path}')
        return
    
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extraction_path)
        print(f'Unzipped files to: {extraction_path}')
    
    return extraction_path

def xlsx_file_name_normalization(path,format='xlsx'):
    import unicodedata
    """Normalizes the file name by removing accents and converting to uppercase.
    Args:
        name (str): The file name to be normalized.
    Returns:
        str: The normalized file name.
    """
    format = format.strip().lower()
    if "*." in format:
        format = format.split("*.")[1]
    if not format:
        raise ValueError("Formato não pode ser vazio.")
    names = [arquivo.name for arquivo in path.glob(f'*.{format}')]
    normalized_names = []
    for name in names:
        normalized_name = name_normalization(name)
        normalized_names.append(normalized_name)
        
    
    #renomeando arquivos
    for i, name in enumerate(normalized_names):
        old_file_path = path / names[i]
        new_file_path = path / name.upper()
        os.rename(old_file_path, new_file_path)
        if old_file_path != new_file_path:
            os.rename(old_file_path, new_file_path)
            print(f'Renamed {old_file_path} to {new_file_path}')
    return normalized_names

def name_normalization(name):
    """Normalizes the name by removing accents and converting to uppercase.
    Args:
        path (str): The path to the directory containing the files.
        names (list): List of file names to be normalized.
        """
    import unicodedata
    normalized_name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')
    return normalized_name.upper().replace(' ', '_').replace('-', '_').strip()

def main():
    planilhas = ['familias_e_coeficientes', 'Manutenções', 'mao_de_obra', 'Referência']
    formatos = ['xlsx', 'pdf']
    # 1. Carregar o arquivo Excel
    file_path = f'SINAPI-{ano}{mes}-formato-{formato}/SINAPI_{planilha}_{ano}_{mes}.xlsx'
    ano = input("Enter year (YYYY): ")
    mes = input("Enter month (MM): ")
    planilha = input(f"Enter planilha ({', '.join(planilhas)}): ")
    formato = input(f"Enter formato ({', '.join(formatos)}): ")
    if planilha not in planilhas or formato not in formatos:
        print("Planilha ou formato inválido.")
        return
         
    download_sinapi_zip(ano, mes)
    extraction_path = unzip_sinapi_file(ano, mes)

    names = [arquivo.name for arquivo in extraction_path.glob('*.xlsx')]
    
    xlsx_file_name_normalization(extraction_path,'xlsx')

    # Rastreia os arquivos Excel no diretório
    resultado = scan_excel_directory(file_path)
    formatted_output = format_output(resultado)
    
    # Imprime o resultado formatado
    print(formatted_output)

if __name__ == "__main__":
    main()

# unzip_sinapi_file('2025', '02', 'xlsx')