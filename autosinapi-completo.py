# IMPORTAÇÕES
import os
import sys
import pandas as pd
import sinapi_utils as sinapi
from sinapi_utils import make_conn_str,create_db_manager

sinapiDowloader = sinapi.SinapiDownloader(cache_minutes=90)
sinapiFileManager = sinapi.FileManager()
sinapiLogger = sinapi.SinapiLogger(nome='loging',level='INFO')
sinapiProcessor = sinapi.SinapiProcessor()
sinapiExcel = sinapi.ExcelProcessor()



#FUNÇÃO MAIN

def main():
    # COLETA DE INFORMAÇÕES COM O USUÁRIO

    ## perguntar o ano e mes de referencia da base sinapi para insersão no banco de dados
    data_ano_referencia = input(f'Digite o ano (YYYY): ')
    ano = str(data_ano_referencia).strip()
    if not ano.isdigit() or len(ano) != 4:
        print("Ano inválido!")
        return  
    
    data_mes_referencia = input(f'Digite o mes (MM): ')
    mes = str(data_mes_referencia).strip()
    if not mes.isdigit() or len(mes) != 2:
        print("Mês inválido!")
        return
    
    formato_arquivos = input(f'Digite o formato dos arquivos (1 - xlsx, 2 - pdf): ')
    
    # TRATAMENTO DAS INFORMAÇÕES COLETADAS

    formato_arquivos = str(formato_arquivos).strip()

    if formato_arquivos == '1':
        formato = 'xlsx'
    elif formato_arquivos == '2':
        formato = 'pdf'
    elif formato_arquivos in ['xlsx', 'pdf']:
        formato = formato_arquivos
    else:
        print(f'Valor {formato_arquivos} inválido para o formato do arquivo')
        return
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
            filefinder = sinapiDowloader._zip_filefinder(diretorio_referencia, ano, mes, formato)

            # CONDICIONAL PARA COLETAR BASE FALTANTE
            if filefinder is None:
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
            elif isinstance(filefinder, tuple) and len(filefinder) == 2:
                zipFiles, selectFile = filefinder
                print(f'Foram encontrados {len(zipFiles)} arquivos zip e {len(selectFile)} arquivo correspondente ao ano e mes de referencia:  {list(selectFile.keys())[0]}')
                break

            elif isinstance(filefinder, str):
                # Caso específico quando encontrou um arquivo específico
                zip_path = filefinder
                selectFile = {os.path.basename(zip_path): zip_path}
                zipFiles = selectFile
                print(f'Foram encontrados {len(zipFiles)} em formato STRING e zip e {len(selectFile)} arquivo correspondente ao ano e mes de referencia:  {list(selectFile.keys())[0]}')
                break

            else:
                print('Tipo de retorno inesperado')
                break
                # Tratar erro
            
        ### Verificando se o zip já foi descompactado e descompactando se solicitado
        print('Verificando se o zip já foi descompactado e descompactando se solicitado')
        
        if len(selectFile) == 1:
            resultado={}
            chave = list(selectFile.keys())[0]
            valor = list(selectFile.values())[0]
            scandir = f'{diretorio_atual}/{ano}_{mes}/{chave.split('.')[0]}'
            ciclo = True
            print(f'Verificando se há pastas ou se o arquivo {chave} já foi descompactado para a pasta "{chave.split('.')[0]}"')
        else:
            selectFile = None
            raise
            

        while ciclo == True:
            if f'SINAPI-{ano}-{mes}-formato-{formato}' == chave.split('.')[0] and os.path.exists(scandir):
                print(f'\n    A pasta {chave} já foi descompactada...')
                try:                    
                    resultado = sinapiExcel.scan_directory(scandir,formato,False)
                    ciclo = False
                    break
                except Exception as e:
                    print(f'Erro ao procurar arquivos no diretório {scandir}:\n    {e}')
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
    planilhas = []
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
            if 0 <= base < len(planilhas):
                planilha_name = planilhas[base]
                planilha = resultado[planilha_name]
                print(f'\nPlanilha escolhida: {planilhas[base]}\nResultado: {planilha}\n')
                
                break
            else:
                raise
        except Exception as e:
            print(f'Erro ao escolher a base: ({e})')

    # CRIAÇÃO/ACESSO AO BANCO DE DADOS E VERIFICAÇÃO DO ESQUEMA
    try:
        # Pergunta e valida o caminho do arquivo de secrets
        secrets_path = input('Deseja utilizar o arquivo "sql_access.secrets" (S/N)? ').strip()
        if secrets_path.upper() == 'S' or secrets_path == '':
            secrets_path = 'sql_access.secrets'
        else:
            secrets_path = input('Digite o caminho do arquivo "sql_access.secrets": ').strip()

        if not os.path.exists(secrets_path):
            print('Erro ao encontrar o arquivo "sql_access.secrets"')
            raise FileNotFoundError('Arquivo de secrets não encontrado.')

        # Utiliza função utilitária para ler configurações do banco
        secrets=secrets_path
        credentials = sinapiFileManager.read_sql_secrets(secrets)
        db_user, db_password, db_host, db_port, db_name, db_initial = credentials

        print(f"""
        Configurações do Banco:
        Usuário: {db_user} | Host: {db_host}:{db_port}
        Banco Principal: {db_name} | Banco Inicial: {db_initial}
        Senha: {db_password}
        """)

        if not db_name:
            print('Nome do banco de dados não encontrado no arquivo de secrets.')
            raise ValueError('Nome do banco de dados não encontrado.')

        # Cria o gerenciador de banco usando função utilitária
        db_manager = create_db_manager(secrets_path=secrets_path, log_level='info',output='target')

        # Garante existência dos schemas necessários
        db_manager.create_schemas(['public', 'sinapi'])

        # Testa conexão e mostra o banco conectado
        query = "SELECT current_database()"
        # params = {"categoria": "eletrônicos"}
        db_name_check = db_manager.execute_query('SELECT current_database()')
        print(f"Banco de dados conectado:\n{db_name_check}\n\n")

    except ConnectionError as e:
        print(f"ERRO DE CONEXÃO: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERRO INESPERADO: {e}")
        sys.exit(2)

    # return
    # VERIFICAÇÃO DAS TABELAS DO BANCO
    # try:        
    #     # Definir estrutura das tabelas (exemplo para composições)
    #     tabelas = {
    #         'composicoes': {
    #             'colunas': [
    #                 'id SERIAL PRIMARY KEY',
    #                 'codigo_composicao VARCHAR(20) NOT NULL',
    #                 'descricao TEXT',
    #                 'unidade VARCHAR(10)',
    #                 'valor NUMERIC(15,2)',
    #                 'ano_referencia INT',
    #                 'mes_referencia INT'
    #             ]
    #         },
    #         # Adicionar outras tabelas conforme necessário
    #     }
        
    #     # Criar tabelas se não existirem
    #     for tabela, estrutura in tabelas.items():
    #         if not db_manager.table_exists('sinapi', tabela):
    #             colunas_str = ", ".join(estrutura['colunas'])
    #             db_manager.execute_query(
    #                 f"CREATE TABLE sinapi.{tabela} ({colunas_str})"
    #             )
    #             print(f"Tabela criada: sinapi.{tabela}")

    # except Exception as e:
    #     sinapiLogger.log('error', f"Erro na criação de tabelas: {e}")
    #     sys.exit(3)

    file_path = planilha.split(planilha_name)[0]
    file_path = file_path.replace('\\', '/')
    #print(f'\nNormalizando arquivos "{str(list(resultado.keys())).replace("'", "").replace("[", "").replace("]", "").replace(",", ", ")}" do diretório :{file_path}\n\n')
    sinapiFileManager.normalize_files(file_path, extension='xlsx')

    #escanear e localizar e extrair as tabelas da planilha escolhida --- normalizar nomes e caminhos dos arquivos
    sheet = {planilha_name : file_path}

    #sinapiFileManager.normalize_text(planilha_name)
    print(f'\n\n    file_path: {file_path}\n    formato: {formato}\n    Planilha_name: {planilha_name}\n    Sheet: {sheet}\n\n\n')
    workbook = sinapiExcel.scan_directory(diretorio=file_path,formato=formato.upper(),data=True,sheet=sheet)
    print(f"\n    Planilhas disponíveis no arquivo: {list(workbook.keys())}\n\n{workbook}\n\n")

    #tableNames = ('COEFICIENTES', 'MANUTENCOES', 'SEM_DESONERACAO', 'ANALITICO', 'PRECOS')
    #print(f"TableNames: {tableNames}\n")

    for sheet in list(workbook.keys()):
        sheet_type = str(sheet).split('SINAPI_')[-1].split(f'_{ano}_{mes}.')[0]
        #sheet_type = sheet.replace('_', ' ').replace('-', ' ').upper().strip()
        print(f'    sheet: {sheet}\n    sheet_type: {sheet_type}\n    workbook keys: {list(workbook.keys())}\n    total: {len(list(workbook[sheet]))}')
        
        sheet_data_type = []
        match_data_type = {}
        for sheet_name in list(workbook[sheet]):            
            #Verificando as planilhas do workbook
            #print(f'            sheet_name: {sheet_name[0]}\n')
            #print(f'            workbook[sheet]: {list(workbook[sheet])}\n')
            sheet_data_type = sinapiProcessor.identify_sheet_type(sheet_name[0])
            #print(f'            dataFiles: {sheet_data_type}')
            if sheet_data_type is not None:
                match_data_type[sheet_name[0]]=sheet_data_type

        print(f'Total de match_data_type: {len(match_data_type)}:\n {match_data_type}')
        
        #processando dados, validando e entregando o dataframe filtrado
        for sheet in match_data_type.items():
            #print(f'sheet: {sheet} / items: {match_data_type.items()}')
            sheet_name = sheet[0]
            sheet_type = sheet[1]
            df = sinapiProcessor.process_excel(file_path,sheet_name,sheet_type['header_id'],sheet_type['split_id'])
            print(df.header())

            
        
        
        # normalized_sheet = sinapiFileManager.normalize_text(sheet.upper())
        # print(f'    Planilha: {normalized_sheet}')
        
        # if isinstance(matched_sheet, list):
        #     tableReferenciaName = ['ISD','CSD','ANALITICO']

        # # Se for lista, verificar se todos os itens de tableNames estão na sheet
        # if all(item in normalized_sheet for item in tableReferenciaName):
        #     matched_sheet.append(sheet)
        #     print(f'        Todas as tabelas {tableReferenciaName} encontradas em: {sheet}')
        # else:
        #     # Caso contrário, procurar por cada tabela individualmente
        #     for table in tableNames:
        #         if table in normalized_sheet:
        #             matched_sheet = sheet
        #             print(f'        Planilha encontrada: {sheet} / {table} - matched_sheet = {matched_sheet}')
        #             break
        # if matched_sheet:
        #     break

    return

    table_configs = {
    'ISD': {
        'split_id': 5,
        'header_id': 9
    },
    'CSD': {
        'split_id': 4,
        'header_id': 9
    },
    'ANALITICO': {
        'split_id': 0,
        'header_id': 9
    },
    'tableNames_mapping': {
        0: {  # Insumos Coeficiente
            'split_id': 5,
            'header_id': 5
        },
        1: {  # Códigos Manutenções
            'split_id': 0,
            'header_id': 5
        },
        2: {  # Mão de Obra
            'split_id': 4,
            'header_id': 5
        }
    },
    'default': {
        'split_id': 0,
        'header_id': 0
    }
    }

    # Para a primeira condição (quando tableReferenciaName existe)
    if tableReferenciaName:
        for i, table in enumerate(tableReferenciaName):
            table_norm = normalize_text(table)
            if table_norm in table_configs:
                config = table_configs[table_norm]
                split_id = config['split_id']
                header_id = config['header_id']
            else:
                # Configuração padrão se a tabela não for encontrada
                config = table_configs['default']
                split_id = config['split_id']
                header_id = config['header_id']
    # Para a segunda condição (quando tableReferenciaName não existe)
    else:
        try:
            for idx, table_name in enumerate(tableNames):
                if table_name in normalize_text(matched_sheet):
                    config = table_configs['tableNames_mapping'][idx]
                    split_id = config['split_id']
                    header_id = config['header_id']
                    break
            else:
                # Configuração padrão se nenhuma correspondência for encontrada
                config = table_configs['default']
                split_id = config['split_id']
                header_id = config['header_id']
        except Exception as e:
            print(f'Filtro de colunas não encontrado - {e}')
            exit()

    # TRATAMENTO DOS DADOS DATAFRAME PARA INSERSÃO NAS TABELAS CORRETAS

    # VERIFICAÇÃO DOS DADOS EXISTENTES NO BANCO DE DADOS COM OS DO DATAFRAME E CONDICIONAL SOBRE A DUPLICIDADE, AGREGAÇÃO OU DECLÍNIO DA INSERSÃO DOS DADOS COLETADOS

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
