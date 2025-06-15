"""
Database abstraction layer for CNPJ Data Pipeline.

Provides a consistent interface for different database backends while
maintaining backward compatibility with existing PostgreSQL implementation.
"""

from .factory import create_database_adapter
from .base import DatabaseAdapter

__all__ = ["create_database_adapter", "DatabaseAdapter"]
