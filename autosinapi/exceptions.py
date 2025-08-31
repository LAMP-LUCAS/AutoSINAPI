"""
Módulo de exceções customizadas para o AutoSINAPI.
"""

class AutoSinapiError(Exception):
    """Exceção base para todos os erros do AutoSINAPI."""
    pass

class ConfigurationError(AutoSinapiError):
    """Erro relacionado a configurações inválidas."""
    pass

class DownloadError(AutoSinapiError):
    """Erro durante o download de arquivos."""
    pass

class ProcessingError(AutoSinapiError):
    """Erro durante o processamento dos dados."""
    pass

class DatabaseError(AutoSinapiError):
    """Erro relacionado a operações de banco de dados."""
    pass