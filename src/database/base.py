from abc import ABC, abstractmethod
from typing import List, Set
import polars as pl
from pathlib import Path


class DatabaseAdapter(ABC):
    """Abstract base class for database adapters.

    Provides a consistent interface for different database backends while
    allowing each implementation to optimize for its specific database features.
    """

    def __init__(self, config):
        self.config = config
        self._pk_cache = {}

    @abstractmethod
    def connect(self):
        """Establish database connection."""
        pass

    @abstractmethod
    def disconnect(self):
        """Close database connection."""
        pass

    @abstractmethod
    def bulk_upsert(self, df: pl.DataFrame, table: str, **kwargs):
        """Bulk upsert data into table.

        This is the main method used by existing code and must be preserved
        for backward compatibility.
        """
        pass

    def bulk_insert(self, df: pl.DataFrame, table: str, **kwargs):
        """Bulk insert data into table.

        Default implementation delegates to bulk_upsert for compatibility.
        Subclasses can override for optimization.
        """
        return self.bulk_upsert(df, table, **kwargs)

    @abstractmethod
    def get_processed_files(self, directory: str) -> Set[str]:
        """Get set of processed files for a directory."""
        pass

    @abstractmethod
    def mark_processed(self, directory: str, filename: str):
        """Mark file as processed."""
        pass

    @abstractmethod
    def is_processed(self, directory: str, filename: str) -> bool:
        """Check if a file has already been processed."""
        pass

    def ensure_tracking_table(self):
        """Ensure processed files tracking table exists.

        Default implementation - can be overridden by subclasses.
        """
        pass

    # Optional methods from reference implementation
    def supports_upsert(self) -> bool:
        """Check if database supports native UPSERT."""
        return True  # Assume most databases support some form of upsert

    def table_exists(self, table: str) -> bool:
        """Check if table exists."""
        # Default implementation - should be overridden
        return True

    def get_primary_keys(self, table: str) -> List[str]:
        """Get primary key columns for a table."""
        # Default implementation - should be overridden
        return []

    def execute_schema(self, schema_path: Path):
        """Execute schema creation script."""
        # Default implementation - should be overridden
        pass
