"""CNPJ Data Pipeline - Modular data processing for Brazilian CNPJ files."""

__version__ = "1.0.0"

from .config import Config
from .downloader import Downloader
from .processor import Processor

__all__ = ["Config", "Downloader", "Processor"]
