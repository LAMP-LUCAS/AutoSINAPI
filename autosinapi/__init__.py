"""
AutoSINAPI: Um toolkit para automação de dados do SINAPI.

Este arquivo é o ponto de entrada do pacote `autosinapi`. Ele define a interface
pública da biblioteca, expondo as principais classes e exceções para serem
utilizadas por outras aplicações.

O `__all__` define explicitamente quais nomes são exportados quando um cliente
usa `from autosinapi import *`.
"""

__version__ = "0.1.0"  # A ser gerenciado pelo setuptools-scm

from autosinapi.config import Config
from autosinapi.core.database import Database
from autosinapi.core.downloader import Downloader
from autosinapi.core.processor import Processor
from autosinapi.exceptions import (AutoSinapiError, ConfigurationError,
                                   DatabaseError, DownloadError,
                                   ProcessingError)

__all__ = [
    "Config",
    "Database",
    "Downloader",
    "Processor",
    "AutoSinapiError",
    "ConfigurationError",
    "DownloadError",
    "ProcessingError",
    "DatabaseError",
    "run_etl"
]

import os
import logging
from contextlib import contextmanager
from typing import Dict, Any

from .etl_pipeline import PipelineETL, setup_logging


# Configure a logger for this module
logger = logging.getLogger(__name__)

@contextmanager
def set_env_vars(env_vars: Dict[str, str]):
    """Temporarily sets environment variables."""
    original_env = {key: os.getenv(key) for key in env_vars}
    for key, value in env_vars.items():
        os.environ[key] = str(value) # Ensure value is string for env vars
    try:
        yield
    finally:
        for key, value in original_env.items():
            if value is None:
                del os.environ[key]
            else:
                os.environ[key] = value

def run_etl(db_config: Dict[str, Any], sinapi_config: Dict[str, Any], mode: str = 'local', log_level: str = 'INFO'):
    """
    Executa o pipeline ETL do AutoSINAPI.

    Args:
        db_config (Dict[str, Any]): Dicionário de configuração do banco de dados.
        sinapi_config (Dict[str, Any]): Dicionário de configuração do SINAPI.
        mode (str): Modo de operação do pipeline ('local' ou 'server'). Padrão é 'local'.
        log_level (str): Nível de log para a execução do pipeline (e.g., 'INFO', 'DEBUG', 'WARNING'). Padrão é 'INFO'.
    """
    # Validate inputs
    if not isinstance(db_config, dict):
        return {
            "status": "failed",
            "message": "Erro de validação: db_config deve ser um dicionário.",
            "tables_updated": [],
            "records_inserted": 0
        }
    if not isinstance(sinapi_config, dict):
        return {
            "status": "failed",
            "message": "Erro de validação: sinapi_config deve ser um dicionário.",
            "tables_updated": [],
            "records_inserted": 0
        }
    if mode not in ['local', 'server']:
        return {
            "status": "failed",
            "message": "Erro de validação: mode deve ser 'local' ou 'server'.",
            "tables_updated": [],
            "records_inserted": 0
        }
    if log_level.upper() not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        return {
            "status": "failed",
            "message": f"Erro de validação: log_level inválido: {log_level}. Use 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'.",
            "tables_updated": [],
            "records_inserted": 0
        }

    # Prepare environment variables
    env_vars_to_set = {
        'DOCKER_ENV': 'true', # Assuming API runs in a docker-like environment
        'POSTGRES_HOST': db_config.get('host'),
        'POSTGRES_PORT': db_config.get('port'),
        'POSTGRES_DB': db_config.get('database'),
        'POSTGRES_USER': db_config.get('user'),
        'POSTGRES_PASSWORD': db_config.get('password'),
        'AUTOSINAPI_YEAR': sinapi_config.get('year'),
        'AUTOSINAPI_MONTH': sinapi_config.get('month'),
        'AUTOSINAPI_TYPE': sinapi_config.get('type', 'REFERENCIA'),
        'AUTOSINAPI_POLICY': sinapi_config.get('duplicate_policy', 'substituir'),
        'AUTOSINAPI_MODE': mode # Pass the mode
    }

    # Filter out None values
    env_vars_to_set = {k: v for k, v in env_vars_to_set.items() if v is not None}

    # Set up logging for the pipeline run
    # The setup_logging function in autosinapi_pipeline.py takes debug_mode.
    # We need to map log_level to debug_mode.
    debug_mode = (log_level.upper() == 'DEBUG')
    setup_logging(debug_mode=debug_mode)

    try:
        with set_env_vars(env_vars_to_set):
            logger.info(f"Iniciando execução do pipeline com modo: {mode}"
                        f"e nível de log: {log_level}")
            pipeline = PipelineETL() # Pipeline will read from env vars
            result = pipeline.run()
            logger.info("Pipeline executado com sucesso.")
            return result
    except Exception as e:
        logger.error(f"Erro ao executar o pipeline: {e}", exc_info=True)
        # Re-raise the exception to indicate task failure, or return a structured error
        # based on the user's request for run_etl to return a dictionary on failure.
        # Since pipeline.run() already returns a dictionary on failure,
        # this outer exception block should only catch errors *before* pipeline.run() is called
        # or unexpected errors not caught by pipeline.run().
        # For consistency, we'll return a structured error here too.
        return {
            "status": "failed",
            "message": f"Erro inesperado antes ou durante a inicialização do pipeline: {e}",
            "tables_updated": [],
            "records_inserted": 0
        }

