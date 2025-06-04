"""
Módulo para gerenciamento de dados do SINAPI, incluindo verificação de arquivos,
processamento de dados e controle de inserção no banco de dados.
"""
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
import pandas as pd
from datetime import datetime

from autosinapi import (
    SinapiProcessor,
    DatabaseManager,
    FileManager,
    ExcelProcessor,
    SinapiLogger
)

class SinapiDataManager:
    """
    Gerencia o processamento e inserção de dados SINAPI, incluindo verificações
    de duplicidade e controle de versões.
    """
    
    def __init__(self, connection_string: str):
        """
        Inicializa o gerenciador de dados.
        
        Args:
            connection_string: String de conexão com o banco de dados
        """
        self.logger = SinapiLogger("SinapiDataManager")
        self.processor = SinapiProcessor()
        self.file_manager = FileManager()
        self.excel_processor = ExcelProcessor()
        self.db_manager = DatabaseManager(connection_string)
        
    def scan_sinapi_directory(self, directory: Union[str, Path]) -> Dict[str, List[Path]]:
        """
        Escaneia um diretório em busca de arquivos SINAPI válidos.
        
        Args:
            directory: Diretório a ser escaneado
            
        Returns:
            Dict com categorias de arquivos encontrados
        """
        directory = Path(directory)
        self.logger.log('info', f'Escaneando diretório: {directory}')
        
        file_categories = {
            'coeficientes': [],
            'manutencoes': [],
            'mao_de_obra': [],
            'referencia': [],
            'outros': []
        }
        
        try:
            excel_files = list(directory.glob('**/*.xlsx'))
            
            for file in excel_files:
                file_name = file.name.upper()
                if 'COEFICIENTES' in file_name:
                    file_categories['coeficientes'].append(file)
                elif 'MANUTENCOES' in file_name:
                    file_categories['manutencoes'].append(file)
                elif 'MAO_DE_OBRA' in file_name:
                    file_categories['mao_de_obra'].append(file)
                elif 'REFERENCIA' in file_name:
                    file_categories['referencia'].append(file)
                else:
                    file_categories['outros'].append(file)
                    
            return file_categories
            
        except Exception as e:
            self.logger.log('error', f'Erro ao escanear diretório: {e}')
            raise
            
    def verify_existing_data(self, schema: str, table: str, ano: int, mes: int) -> bool:
        """
        Verifica se já existem dados para o período especificado.
        
        Args:
            schema: Schema do banco de dados
            table: Tabela a ser verificada
            ano: Ano de referência
            mes: Mês de referência
            
        Returns:
            True se existem dados, False caso contrário
        """
        try:
            query = f"""
                SELECT COUNT(*) as count 
                FROM {schema}.{table}
                WHERE ano_ref = :ano AND mes_ref = :mes
            """
            
            result = self.db_manager.execute_query(
                query,
                params={'ano': ano, 'mes': mes}
            )
            
            return result.iloc[0]['count'] > 0
            
        except Exception as e:
            self.logger.log('error', f'Erro ao verificar dados existentes: {e}')
            raise
            
    def process_excel_file(self, file_path: Path) -> Tuple[pd.DataFrame, Dict]:
        """
        Processa um arquivo Excel do SINAPI e retorna os dados normalizados.
        
        Args:
            file_path: Caminho do arquivo Excel
            
        Returns:
            DataFrame com dados processados e dicionário com metadados
        """
        try:
            # Identifica tipo do arquivo e configurações
            file_name = file_path.name.upper()
            sheet_name = None
            
            workbook = pd.ExcelFile(file_path)
            sheet_names = workbook.sheet_names
            
            # Escolhe a planilha adequada
            if len(sheet_names) == 1:
                sheet_name = sheet_names[0]
            else:
                # Se houver múltiplas planilhas, tenta identificar a correta
                for name in sheet_names:
                    if 'DADOS' in name.upper():
                        sheet_name = name
                        break
                if not sheet_name:
                    sheet_name = sheet_names[0]
            
            # Identifica configurações baseado no nome do arquivo
            if 'REFERENCIA' in file_name:
                config = {'header_id': 9, 'split_id': 0}
            elif 'COEFICIENTES' in file_name:
                config = {'header_id': 5, 'split_id': 5}
            elif 'MANUTENCOES' in file_name:
                config = {'header_id': 5, 'split_id': 0}
            elif 'MAO_DE_OBRA' in file_name:
                config = {'header_id': 5, 'split_id': 4}
            else:
                config = {'header_id': 0, 'split_id': 0}
            
            # Processa os dados
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=config['header_id'])
            df.columns = [self.file_manager.normalize_text(col) for col in df.columns]
            
            # Realiza melt se necessário
            if config['split_id'] > 0:
                id_vars = df.columns[:config['split_id']].tolist()
                value_vars = df.columns[config['split_id']:].tolist()
                
                df = pd.melt(
                    df,
                    id_vars=id_vars,
                    value_vars=value_vars,
                    var_name='ESTADO',
                    value_name='VALOR'
                )
            
            # Adiciona colunas de controle
            df['DATA_IMPORTACAO'] = datetime.now()
            df['ARQUIVO_ORIGEM'] = str(file_path)
            
            return df, config
            
        except Exception as e:
            self.logger.log('error', f'Erro ao processar arquivo {file_path}: {e}')
            raise
            
    def insert_data(self, df: pd.DataFrame, schema: str, table: str, 
                    ano: int, mes: int, if_exists: str = 'ask') -> bool:
        """
        Insere dados no banco, controlando duplicidades.
        
        Args:
            df: DataFrame com dados a serem inseridos
            schema: Schema do banco de dados
            table: Tabela de destino
            ano: Ano de referência
            mes: Mês de referência
            if_exists: Como proceder se existirem dados ('replace', 'append', 'skip', 'ask')
            
        Returns:
            True se dados foram inseridos com sucesso
        """
        try:
            # Verifica dados existentes
            has_data = self.verify_existing_data(schema, table, ano, mes)
            
            if has_data:
                if if_exists == 'ask':
                    print(f'\nJá existem dados para {mes}/{ano} na tabela {schema}.{table}')
                    print('Como deseja proceder?')
                    print('1 - Substituir dados existentes')
                    print('2 - Adicionar aos dados existentes')
                    print('3 - Pular inserção')
                    
                    choice = input('\nEscolha uma opção (1-3): ').strip()
                    
                    if choice == '1':
                        if_exists = 'replace'
                    elif choice == '2':
                        if_exists = 'append'
                    else:
                        if_exists = 'skip'
                
                if if_exists == 'skip':
                    self.logger.log('info', 'Inserção pulada pelo usuário')
                    return False
                    
                if if_exists == 'replace':
                    # Remove dados existentes
                    self.db_manager.execute_query(
                        f"DELETE FROM {schema}.{table} WHERE ano_ref = :ano AND mes_ref = :mes",
                        params={'ano': ano, 'mes': mes}
                    )
            
            # Adiciona colunas de referência
            df['ANO_REF'] = ano
            df['MES_REF'] = mes
            
            # Insere dados
            self.db_manager.insert_data(schema, table, df)
            
            self.logger.log('info', f'Dados inseridos com sucesso em {schema}.{table}')
            return True
            
        except Exception as e:
            self.logger.log('error', f'Erro ao inserir dados: {e}')
            raise
