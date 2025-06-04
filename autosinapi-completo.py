# IMPORTAÇÕES
import os
import sinapi_utils as sinapi

sinapiDowloader = sinapi.SinapiDownloader(cache_minutes=90)
sinapiFileManager = sinapi.FileManager()
sinapiLogger = sinapi.SinapiLogger(nome='loging',level='info')
sinapiProcessor = sinapi.SinapiProcessor()
sinapiExcel = sinapi.ExcelProcessor()



#FUNÇÃO MAIN

def main():
    # COLETA DE INFORMAÇÕES COM O USUÁRIO

    ## perguntar o ano e mes de referencia da base sinapi para insersão no banco de dados

    data_ano_referencia = input(f'Digite o ano (YYYY): ')
    data_mes_referencia = input(f'Digite o mes (MM): ')
    formato_arquivos = input(f'Digite o formato dos arquivos (1 - xlsx, 2 - pdf): ')
    
    # TRATAMENTO DAS INFORMAÇÕES COLETADAS

    if formato_arquivos:
        formato_arquivos = formato_arquivos.strip()
        try:
            if formato_arquivos in ['1', '2']: 
                if formato_arquivos == '1':
                    formato = 'xlsx'
                elif formato_arquivos == '2':
                    formato = 'pdf'
            elif formato_arquivos in ['xlsx', 'pdf']:
                if formato_arquivos == 'xlsx':
                    formato = 'xlsx'
                elif formato_arquivos == 'pdf':
                    formato = 'pdf'
        except ValueError:
                print(f'Valor {formato_arquivos} inválido para o formato do arquivo')
                return
    else:
        formato = 'xlsx'

    mes = str(data_mes_referencia).strip()
    ano = str(data_ano_referencia).strip()
    formato = str(formato).strip()
    
    if not sinapiDowloader._validar_parametros(ano=ano, mes=mes, formato=formato):
        print(f'Erro ao validar os dados inseridos...')
        return
    else:
        print(f'    Os parametros ano:{ano}, mes:{mes}, formato:{formato}, foram verificados e validados...')
        pass
        
    # VERIFICAÇÃO DO DIRETÓRIO

    ## verificando existem arquivos na pasta, listando-os ou perguntando se o usuário quer fazer o download da base.
    diretorio_atual = os.getcwd()
    diretorio_referencia = f'{diretorio_atual}/{ano}_{mes}'.replace('/', '\\')


    ## Conferindo a pasta zip ou arquivo zip
    try:
        ### Verificando se tem o arquivo zip ou pasta descompactada
        print('Verificando se tem o arquivo zip ou pasta descompactada...')
        if not os.path.exists(diretorio_referencia):
            os.makedirs(diretorio_referencia)

        print('Verificando arquivos e subpastas...')        
        while True:
            filefinder = sinapiDowloader._zip_filefinder(diretorio_referencia,ano,mes,formato)
            if filefinder is not None:
                zipFiles = filefinder[0]
                selectFile = filefinder[1]
            if isinstance(filefinder,tuple):
                if (zipFiles is not None and len(zipFiles) > 0) and (selectFile is not None and len(selectFile) > 0):
                    print(f'Foram encontrados {len(zipFiles)} arquivos zip e {len(selectFile)} arquivo correspondente ao ano e mes de referencia:  {list(selectFile.keys())[0]}')
                    break

            # CONDICIONAL PARA COLETAR BASE FALTANTE
            if filefinder is None or len(selectFile) == 0:
                print(f'A pasta {diretorio_referencia} está vazia...')
                ### realizando o download ou coleta do arquivo
                coletar = input('Deseja realizar o download da base de dados? (S/N): ')
                if coletar.upper() == 'S':
                    try:
                        caminhoDownload = sinapiDowloader.download_file(ano, mes, formato,sleeptime=1,count=3)
                        if caminhoDownload is None:
                            print(f'\nErro ao realizar o download (caminhoDownload: {caminhoDownload})...')
                            return
                        else:
                            print(f'A base está disponível no caminho: {caminhoDownload}')
                            break
                    except Exception as e:
                        print(f'Erro acontecido: {e}')
                        return
                else:
                    print("Coleta da base não realizada")
                    return

        ### Verificando se o zip já foi descompactado e descompactando se solicitado
        print('Verificando se o zip já foi descompactado e descompactando se solicitado')
        if len(selectFile) == 1:            
            chave = list(selectFile.keys())[0]
            valor = list(selectFile.values())[0]
            scandir = f'{diretorio_atual}/{ano}_{mes}/{chave.split('.')[0]}'
            ciclo = True
            print(f'Verificando se há pastas ou se o arquivo {chave} já foi descompactado para a pasta "{chave.split('.')[0]}"')
            while ciclo == True:
                if f'SINAPI-{ano}-{mes}-formato-{formato}' == chave.split('.')[0] and os.path.exists(scandir):
                    print(f'\n    A pasta {chave} já foi descompactada...')
                    try:                    
                        resultado = sinapiExcel.scan_directory(scandir,formato,False)
                        ciclo = False
                        break
                    except Exception as e:
                        print(f'Erro ao procurar arquivos xlsx no diretório {scandir}:\n    {e}')
                else:
                    print(f'\n    A pasta {chave} não foi descompactada...')
                    descompactar = input(f'    Deseja descompactar o arquivo {chave} (S/N)? ')
                    if descompactar.upper() == 'S':
                        try:
                            sinapiDowloader.unzip_file(valor)
                        except Exception as e:
                            print(f'Erro ao descompactar o arquivo {valor}:\n    {e}')
                            ciclo = False
                            break

    except Exception as e:
        print(f'Erro ao processar o diretório {diretorio_referencia}:\n    {e}')
        #print(f'Erro ao validar o arquivo {f'SINAPI-{ano}-{mes}-formato-{formato}'} em relação a chave {chave.split('.')[0]}')
        
    if resultado:
        planilhas = list(resultado.keys())
        print('\n\n==================================================\n')
        print('PLANILHAS DISPONÍVEIS:')
        [print(f'    {i} - {planilhas[i]}') for i in range(len(planilhas))]
        print('\n==================================================\n')
    
    # DEFININDO BASE DE COLETA
    while True:
        base = input(f'Qual a planilha escolhida para a base de dados? (Escolha entre 0 e {len(planilhas)-1}) ')
        try:
            base = int(base)            
            if base >= len(planilhas)-1:
                print(f'\nPlanilha escolhida: {planilhas[base]}\n')
                break
            else:
                raise
        except Exception as e:
            print(f'Erro ao escolher a base: ({e})')

    # ACESSO AO BANCO DE DADOS
    params = {
        'arquivo_xlsx': None,
        'tipo_base': None,
        'user': None,
        'password': None,
        'host': None,
        'port': None,
        'dbname': None,
        'initial_db': 'postgres',
        'batch_size': 1000,
        'log_level': 'off'
        }

    try:
        sql_secrets = 'sql_access.secrets'
        print(f'Local dos dados secrets: {sql_secrets}')    
        if os.path.isfile(sql_secrets):
            print(os.path.isfile(sql_secrets))
            sql_secrets_path = os.path.join(os.getcwd(), sql_secrets)
            print(f'Caminho completo: {sql_secrets}')
            user, password, host, port, dbname, initial_db = sinapi.DatabaseManager.sql_secrets_access(secrets_file_path=sql_secrets_path)
            params.update({
                'user': user,
                'password': password,
                'host': host,
                'port': port,
                'dbname': dbname,
                'initial_db': initial_db
            })
            [print(f'   {data}') for data in params.items()]
    except Exception as e:
        print(f'Erro ao acessar o banco de dados:\n    {e}\n')
        raise

    ## conectando ao db
    try:
        dbEngine = sinapi.DatabaseManager.database_conect(
            user=params['user'],
            password=params['password'],
            host=params['host'],
            port=params['port'],
            dbname=params['initial_db']
            )
        print(f'Resultado da conexão: {dbEngine}')
    except Exception as e:
        print(f'Erro ocorrido na conexão ao banco de dados: {e}\n')


    # VERIFICAÇÃO DO ESQUEMA E TABELAS DO BANCO



    # VERIFICAÇÃO DOS DADOS EXISTENTES E CONDICIONAL SOBRE A DUPLICIDADE, AGREGAÇÃO OU DECLÍNIO DA INSERSÃO DOS DADOS COLETADOS

    # TRATAMENTO DOS DADOS E INSERSÃO NAS TABELAS CORRETAS

    # FINALIZAÇÃO COM A EXPORTAÇÃO "BYPASS" DOS DADOS INSERIDOS NAS TABELAS EM FORMATO CSV NO DIRETÓRIO DA BASE

    # EXPORTAÇÃO DO LOG TOTAL DO SCRIPT


if __name__ == "__main__":
    prog = False
    while prog == False:
        print('\n===============================================================')
        try:
            main()
        except KeyboardInterrupt:
            print('\n\nFinalização do programa pelo usuário\n')
            break
        except Exception as e:
            choice = input(f'\n{e}\n\nDeseja continuar o Programa (S/N)? ')
            if choice.upper() == 'S':
                prog = True
            else:
                prog = False
                break
