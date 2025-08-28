"""
Exceções customizadas para o AutoSINAPI Toolkit.
Todas as exceções são derivadas de AutoSINAPIError para facilitar o tratamento específico.
"""

class AutoSINAPIError(Exception):
    """Exceção base para todos os erros do AutoSINAPI."""
    pass

class DownloadError(AutoSINAPIError):
    """Exceção levantada quando há problemas no download de arquivos SINAPI."""
    pass

class ProcessingError(AutoSINAPIError):
    """Exceção levantada quando há problemas no processamento das planilhas."""
    pass

class DatabaseError(AutoSINAPIError):
    """Exceção levantada quando há problemas com operações no banco de dados."""
    pass

class ConfigurationError(AutoSINAPIError):
    """Exceção levantada quando há problemas com as configurações."""
    pass

class ValidationError(AutoSINAPIError):
    """Exceção levantada quando há problemas com validação de dados."""
    pass
