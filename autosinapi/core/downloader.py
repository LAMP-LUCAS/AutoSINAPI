"""
Módulo de Download do AutoSINAPI.

Este módulo é responsável por obter os arquivos de dados do SINAPI. Ele abstrai
a origem dos dados, que pode ser tanto um download direto do site da Caixa
Econômica Federal quanto um arquivo local fornecido pelo usuário.

A classe `Downloader` gerencia a sessão HTTP, constrói as URLs de download
com base nas configurações e trata os erros de rede, garantindo que o pipeline
receba um stream de bytes do arquivo a ser processado.
"""
from typing import Dict, Optional, BinaryIO, Union
import requests
from io import BytesIO
from pathlib import Path
from ..exceptions import DownloadError

class Downloader:
    """
    Classe responsável por obter os arquivos SINAPI, seja por download ou input direto.
    
    Suporta dois modos de obtenção:
    1. Download direto do servidor SINAPI
    2. Leitura de arquivo local fornecido pelo usuário
    """
    
    def __init__(self, sinapi_config: Dict[str, str], mode: str):
        """
        Inicializa o downloader.
        
        Args:
            sinapi_config: Configurações do SINAPI
            mode: Modo de operação ('server' ou 'local')
        """
        self.config = sinapi_config
        self.mode = mode
        self._session = requests.Session()
        
    def get_sinapi_data(self, 
                       file_path: Optional[Union[str, Path]] = None, 
                       save_path: Optional[Path] = None) -> BinaryIO:
        """
        Obtém os dados do SINAPI, seja por download ou arquivo local.
        
        Args:
            file_path: Caminho opcional para arquivo XLSX local
            save_path: Caminho opcional para salvar o arquivo baixado (modo local)
            
        Returns:
            BytesIO: Stream com o conteúdo do arquivo
            
        Raises:
            DownloadError: Se houver erro no download ou leitura do arquivo
        """
        if file_path:
            return self._read_local_file(file_path)
        return self._download_file(save_path)
    
    def _read_local_file(self, file_path: Union[str, Path]) -> BinaryIO:
        """Lê um arquivo XLSX local."""
        try:
            path = Path(file_path)
            if not path.exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {path}")
            if path.suffix.lower() not in {'.xlsx', '.xls'}:
                raise ValueError(f"Formato inválido. Use arquivos .xlsx ou .xls")
            return BytesIO(path.read_bytes())
        except Exception as e:
            raise DownloadError(f"Erro ao ler arquivo local: {str(e)}")
    
    def _download_file(self, save_path: Optional[Path] = None) -> BinaryIO:
        """
        Realiza o download do arquivo SINAPI.
        
        Args:
            save_path: Caminho para salvar o arquivo (apenas em modo local)
        
        Returns:
            BytesIO: Stream com o conteúdo do arquivo
        
        Raises:
            DownloadError: Se houver erro no download
        """
        try:
            url = self._build_url()
            response = self._session.get(url, timeout=30)
            response.raise_for_status()
            
            content = BytesIO(response.content)
            
            if self.mode == 'local' and save_path:
                save_path.write_bytes(response.content)
            
            return content
            
        except requests.RequestException as e:
            raise DownloadError(f"Erro no download: {str(e)}")
    
    def _build_url(self) -> str:
        """
        Constrói a URL do arquivo SINAPI com base nas configurações.
        
        Returns:
            str: URL completa para download do arquivo
        """
        base_url = "https://www.caixa.gov.br/Downloads/sinapi-a-vista-composicoes"
        
        # Formata ano e mês com zeros à esquerda
        ano = str(self.config['year']).zfill(4)
        mes = str(self.config['month']).zfill(2)
        
        # Determina o tipo de planilha
        tipo = self.config.get('type', 'REFERENCIA').upper()
        if tipo not in ['REFERENCIA', 'DESONERADO']:
            raise ValueError(f"Tipo de planilha inválido: {tipo}")
        
        # Constrói a URL
        file_name = f"SINAPI_{tipo}_{mes}_{ano}"
        url = f"{base_url}/{file_name}.zip"
        
        return url
    
    def __enter__(self):
        """Permite uso do contexto 'with'."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fecha a sessão HTTP ao sair do contexto."""
        self._session.close()
