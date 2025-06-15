from typing import Dict, Type
from .base import DatabaseAdapter
from .postgres import PostgreSQLAdapter


# Registry of available adapters
ADAPTERS: Dict[str, Type[DatabaseAdapter]] = {
    "postgresql": PostgreSQLAdapter,
    # Placeholder for future implementations
    # "mysql": MySQLAdapter,
    # "bigquery": BigQueryAdapter,
    # "sqlite": SQLiteAdapter,
}


def create_database_adapter(config) -> DatabaseAdapter:
    """Factory function to create appropriate database adapter."""
    backend = config.database_backend.value

    adapter_class = ADAPTERS.get(backend)
    if not adapter_class:
        available_backends = ", ".join(ADAPTERS.keys())
        raise ValueError(
            f"Unsupported database backend: {backend}. Available: {available_backends}"
        )

    return adapter_class(config)


def list_available_backends() -> list:
    """List available database backends."""
    return list(ADAPTERS.keys())
