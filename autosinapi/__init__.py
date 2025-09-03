"""
AutoSINAPI: Um toolkit para automação de dados do SINAPI.
"""

__version__ = "0.1.0"  # A ser gerenciado pelo setuptools-scm

from autosinapi.config import Config
from autosinapi.core.database import Database
from autosinapi.core.downloader import Downloader
from autosinapi.core.processor import Processor
from autosinapi.exceptions import AutoSinapiError, ConfigurationError, DownloadError, ProcessingError, DatabaseError

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
]