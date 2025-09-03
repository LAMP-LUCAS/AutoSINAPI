"""
Módulo de configuração do AutoSINAPI.

Este módulo define a classe `Config`, responsável por centralizar, validar e gerenciar
todas as configurações necessárias para a execução do pipeline de ETL.

A classe garante que todas as chaves obrigatórias para a conexão com o banco de dados
e para os parâmetros do SINAPI sejam fornecidas, levantando um erro claro em caso de
configurações ausentes.
"""

from typing import Any, Dict

from .exceptions import ConfigurationError


class Config:
    """Gerenciador de configurações do AutoSINAPI."""

    REQUIRED_DB_KEYS = {"host", "port", "database", "user", "password"}
    REQUIRED_SINAPI_KEYS = {"state", "month", "year", "type"}
    OPTIONAL_SINAPI_KEYS = {"input_file"}  # Arquivo XLSX local opcional

    def __init__(
        self, db_config: Dict[str, Any], sinapi_config: Dict[str, Any], mode: str
    ):
        """
        Inicializa as configurações do AutoSINAPI.

        Args:
            db_config: Configurações do banco de dados
            sinapi_config: Configurações do SINAPI
            mode: Modo de operação ('server' ou 'local')

        Raises:
            ConfigurationError: Se as configurações forem inválidas
        """
        self.mode = self._validate_mode(mode)
        self.db_config = self._validate_db_config(db_config)
        self.sinapi_config = self._validate_sinapi_config(sinapi_config)

    def _validate_mode(self, mode: str) -> str:
        """Valida o modo de operação."""
        if mode not in ("server", "local"):
            raise ConfigurationError(f"Modo inválido: {mode}. Use 'server' ou 'local'")
        return mode

    def _validate_db_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Valida as configurações do banco de dados."""
        missing = self.REQUIRED_DB_KEYS - set(config.keys())
        if missing:
            raise ConfigurationError(f"Configurações de banco ausentes: {missing}")
        return config

    def _validate_sinapi_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Valida as configurações do SINAPI."""
        missing = self.REQUIRED_SINAPI_KEYS - set(config.keys())
        if missing:
            raise ConfigurationError(f"Configurações do SINAPI ausentes: {missing}")
        return config

    @property
    def is_server_mode(self) -> bool:
        """Retorna True se o modo for 'server'."""
        return self.mode == "server"

    @property
    def is_local_mode(self) -> bool:
        """Retorna True se o modo for 'local'."""
        return self.mode == "local"
