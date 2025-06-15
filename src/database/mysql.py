"""MySQL adapter placeholder.

This is a placeholder implementation for MySQL support.
The interface is defined but methods raise NotImplementedError.
"""

import polars as pl
from typing import Set
from .base import DatabaseAdapter


class MySQLAdapter(DatabaseAdapter):
    """MySQL adapter placeholder implementation.

    TODO: Implement MySQL-specific database operations.
    For now, all methods raise NotImplementedError to indicate
    that MySQL support is planned but not yet available.
    """

    def __init__(self, config):
        super().__init__(config)
        raise NotImplementedError(
            "MySQL support is not yet implemented. "
            "Please use PostgreSQL for now, or contribute MySQL implementation."
        )

    def connect(self):
        """Establish MySQL connection."""
        raise NotImplementedError("MySQL support not yet implemented")

    def disconnect(self):
        """Close MySQL connection."""
        raise NotImplementedError("MySQL support not yet implemented")

    def bulk_upsert(self, df: pl.DataFrame, table: str, **kwargs):
        """Bulk upsert data into MySQL table."""
        raise NotImplementedError("MySQL support not yet implemented")

    def get_processed_files(self, directory: str) -> Set[str]:
        """Get set of processed files for a directory."""
        raise NotImplementedError("MySQL support not yet implemented")

    def mark_processed(self, directory: str, filename: str):
        """Mark file as processed."""
        raise NotImplementedError("MySQL support not yet implemented")

    def is_processed(self, directory: str, filename: str) -> bool:
        """Check if a file has already been processed."""
        raise NotImplementedError("MySQL support not yet implemented")
