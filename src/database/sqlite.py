"""SQLite adapter placeholder.

This is a placeholder implementation for SQLite support.
The interface is defined but methods raise NotImplementedError.
"""

import polars as pl
from typing import Set
from .base import DatabaseAdapter


class SQLiteAdapter(DatabaseAdapter):
    """SQLite adapter placeholder implementation.

    TODO: Implement SQLite-specific database operations.
    For now, all methods raise NotImplementedError to indicate
    that SQLite support is planned but not yet available.

    Note: SQLite is primarily intended for development and testing,
    not for production CNPJ data loading due to performance limitations.
    """

    def __init__(self, config):
        super().__init__(config)
        raise NotImplementedError(
            "SQLite support is not yet implemented. "
            "Please use PostgreSQL for now, or contribute SQLite implementation."
        )

    def connect(self):
        """Establish SQLite connection."""
        raise NotImplementedError("SQLite support not yet implemented")

    def disconnect(self):
        """Close SQLite connection."""
        raise NotImplementedError("SQLite support not yet implemented")

    def bulk_upsert(self, df: pl.DataFrame, table: str, **kwargs):
        """Bulk upsert data into SQLite table."""
        raise NotImplementedError("SQLite support not yet implemented")

    def get_processed_files(self, directory: str) -> Set[str]:
        """Get set of processed files for a directory."""
        raise NotImplementedError("SQLite support not yet implemented")

    def mark_processed(self, directory: str, filename: str):
        """Mark file as processed."""
        raise NotImplementedError("SQLite support not yet implemented")

    def is_processed(self, directory: str, filename: str) -> bool:
        """Check if a file has already been processed."""
        raise NotImplementedError("SQLite support not yet implemented")
