# IMPORTAÇÕES
import os
import sys
import json
import pandas as pd
from datetime import datetime
import sinapi_utils as sinapi
from sinapi_utils import create_db_manager

class SinapiPipeline:
    def __init__(self, config_path="CONFIG.json"):
        self.config = self.load_config(config_path)
        self.logger = sinapi.SinapiLogger("SinapiPipeline", self.config.get('log_level', 'info'))
        self.downloader = sinapi.SinapiDownloader(cache_minutes=90)
        self.file_manager = sinapi.FileManager()
        self.processor = sinapi.SinapiProcessor()
        self.excel_processor = sinapi.ExcelProcessor()
        self.db_manager = None

    def load_config(self, config_path):
        """Carrega o arquivo de configuração"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.log('critical', f'Erro ao carregar configuração: {e}')
            raise

    def get_parameters(self):
        """Obtém parâmetros de execução do config ou do usuário"""
        params = {
            'ano': self.config.get('default_year'),
            'mes': self.config.get('default_month'),
            'formato': self.config.get('default_format', 'xlsx'),
            'workbook': self.config.get('default_workbook', 'REFERENCIA'),
        }
        
        # Preenche valores faltantes via input do usuário
        if not params['ano']:
            params['ano'] = input('Digite o ano (YYYY): ').strip()
        if not params['mes']:
            params['mes'] = input('Digite o mês (MM): ').strip()
        
        # Validação básica
        if not params['ano'].isdigit() or len(params['ano']) != 4:
            raise ValueError("Ano inválido!")
        if not params['mes'].isdigit() or len(params['mes']) != 2:
            raise ValueError("Mês inválido!")
        
        return params

    def setup_database(self):
        """Configura a conexão com o banco de dados"""
        secrets_path = self.config['secrets_path']
        
        if not os.path.exists(secrets_path):
            raise FileNotFoundError(f'Arquivo de secrets não encontrado: {secrets_path}')
        
        self.db_manager = create_db_manager(
            secrets_path=secrets_path,
            log_level=self.config.get('log_level', 'info'),
            output='target'
        )
        
        # Garante existência dos schemas
        self.db_manager.create_schemas(['public', 'sinapi'])

    def download_and_extract_files(self, params):
        """Gerencia download e extração de arquivos"""
        ano = params['ano']
        mes = params['mes']
        formato = params['formato']
        diretorio_referencia = f"./{ano}_{mes}"
        
        # Cria diretório se necessário
        if not os.path.exists(diretorio_referencia):
            os.makedirs(diretorio_referencia)
        
        # Verifica se o arquivo já existe
        filefinder_result = self.downloader._zip_filefinder(diretorio_referencia, ano, mes, formato)
        
        # Trata diferentes tipos de retorno
        if isinstance(filefinder_result, tuple) and len(filefinder_result) == 2:
            # Caso retorne (zipFiles, selectFile)
            zip_files, select_file = filefinder_result
            if select_file:
                zip_path = list(select_file.values())[0]
            else:
                zip_path = None
        elif isinstance(filefinder_result, str):
            # Caso retorne diretamente o caminho
            zip_path = filefinder_result
        else:
            zip_path = None
        
        # Faz download se necessário
        if not zip_path:
            zip_path = self.downloader.download_file(ano, mes, formato, sleeptime=1, count=3)
            if not zip_path:
                raise RuntimeError("Falha no download do arquivo")
        
        # Extrai arquivos
        extraction_path = self.downloader.unzip_file(zip_path)
        return extraction_path

    def process_spreadsheets(self, extraction_path, planilha_name):
        """Processa as planilhas usando padrão Strategy"""
        # Normaliza nomes de arquivos
        self.file_manager.normalize_files(extraction_path, extension='xlsx')
        
        # Identifica planilhas disponíveis
        workbook = self.excel_processor.scan_directory(
            diretorio=extraction_path,
            formato='xlsx',
            data=True,
            sheet={planilha_name: extraction_path}
        )
        
        df_list = {}
        for sheet_name in workbook.get(planilha_name, []):
            sheet_config = self.config['sheet_processors'].get(
                sheet_name[0],
                self.processor.identify_sheet_type(sheet_name[0])
            )
            
            if not sheet_config:
                self.logger.log('warning', f'Configuração não encontrada para: {sheet_name[0]}')
                continue
                
            # Processa a planilha
            df = self.processor.process_excel(
                f'{extraction_path}/{planilha_name}',
                sheet_name[0],
                sheet_config['header_id'],
                sheet_config['split_id']
            )
            
            # Limpa e normaliza dados
            clean_df = self.processor.clean_data(df)
            table_name = self.file_manager.normalize_text(
                f'{planilha_name.split(".")[0]}_{sheet_name[0]}'
            )
            df_list[table_name] = clean_df
        
        return df_list

    def handle_database_operations(self, df_list):
        """Gerencia operações de banco de dados"""
        duplicate_policy = self.config.get('duplicate_policy', 'substituir').upper()
        backup_dir = self.config.get('backup_dir', './backups')
        
        for table_name, df in df_list.items():
            full_table_name = f"sinapi.{table_name}"
            
            try:
                # Valida e prepara dados para inserção
                df_to_insert = self.db_manager.validate_data(
                    full_table_name=full_table_name,
                    df=df,
                    backup_dir=backup_dir,
                    policy=duplicate_policy  # Novo parâmetro
                )
                
                if df_to_insert is not None and not df_to_insert.empty:
                    self.db_manager.insert_data('sinapi', table_name, df_to_insert)
                elif df_to_insert is not None and df_to_insert.empty:
                    self.logger.log('info', f"Nenhum dado novo para inserir na tabela {table_name} após validação.")
            except Exception as e:
                self.logger.log('error', f'Erro crítico ao processar a tabela {full_table_name}: {e}', exc_info=True)
                # Re-levanta a exceção para parar o pipeline, pois um erro de DB é crítico
                raise

    def export_results(self, df_list, params):
        """Exporta resultados para CSV"""
        ano = params['ano']
        mes = params['mes']
        diretorio_referencia = f"./{ano}_{mes}"
        
        for table_name, df in df_list.items():
            csv_filename = f"{table_name}_{ano}_{mes}.csv"
            csv_path = os.path.join(diretorio_referencia, csv_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            self.logger.log('info', f"DataFrame exportado para: {csv_path}")

    def run(self):
        """Executa o pipeline completo"""
        try:
            # Passo 1: Obter parâmetros
            params = self.get_parameters()
            
            # Passo 2: Configurar banco de dados
            self.setup_database()
            
            # Passo 3: Download e extração
            extraction_path = self.download_and_extract_files(params)
            
            # Passo 4: Identificar planilha principal
            planilha_base_name = self.file_manager.normalize_text(f'SINAPI_{params["workbook"]}_{params["ano"]}_{params["mes"]}')
            planilha_name = f'{planilha_base_name}.xlsx'
            
            # Passo 5: Processar planilhas
            df_list = self.process_spreadsheets(extraction_path, planilha_name)
            
            # Passo 6: Operações de banco
            if df_list:
                self.handle_database_operations(df_list)
            else:
                self.logger.log('warning', "Nenhuma planilha foi processada, pulando operações de banco de dados.")

            # Passo 7: Exportar resultados
            self.export_results(df_list, params)
            
            self.logger.log('info', 'Processo concluído com sucesso!')
            
        except Exception as e:
            self.logger.log('error', f'Erro durante o processo: {e}', exc_info=True)
            #self.logger.log('critical', f'O pipeline falhou devido a um erro crítico: {e}', exc_info=True)
            raise

if __name__ == "__main__":
    pipeline = SinapiPipeline()
    pipeline.run()