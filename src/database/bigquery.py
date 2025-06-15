"""BigQuery adapter placeholder.

This is a placeholder implementation for BigQuery support.
The interface is defined but methods raise NotImplementedError.
"""

import polars as pl
from typing import Set
from .base import DatabaseAdapter


class BigQueryAdapter(DatabaseAdapter):
    """BigQuery adapter placeholder implementation.

    TODO: Implement BigQuery-specific database operations.
    For now, all methods raise NotImplementedError to indicate
    that BigQuery support is planned but not yet available.
    """

    def __init__(self, config):
        super().__init__(config)
        raise NotImplementedError(
            "BigQuery support is not yet implemented. "
            "Please use PostgreSQL for now, or contribute BigQuery implementation."
        )

    def connect(self):
        """Initialize BigQuery client."""
        raise NotImplementedError("BigQuery support not yet implemented")

    def disconnect(self):
        """BigQuery client doesn't need explicit disconnect."""
        raise NotImplementedError("BigQuery support not yet implemented")

    def bulk_upsert(self, df: pl.DataFrame, table: str, **kwargs):
        """Bulk upsert data into BigQuery table."""
        raise NotImplementedError("BigQuery support not yet implemented")

    def get_processed_files(self, directory: str) -> Set[str]:
        """Get set of processed files for a directory."""
        raise NotImplementedError("BigQuery support not yet implemented")

    def mark_processed(self, directory: str, filename: str):
        """Mark file as processed."""
        raise NotImplementedError("BigQuery support not yet implemented")

    def is_processed(self, directory: str, filename: str) -> bool:
        """Check if a file has already been processed."""
        raise NotImplementedError("BigQuery support not yet implemented")
