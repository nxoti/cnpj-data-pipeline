import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import psutil


def _get_int_env(key: str, default: str) -> int:
    """Get integer from environment variable, handling empty strings."""
    value = os.getenv(key, default)
    return int(default if value == "" else value)


class DatabaseBackend(Enum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    BIGQUERY = "bigquery"
    SQLITE = "sqlite"


class ProcessingStrategy(Enum):
    AUTO = "auto"
    MEMORY_CONSTRAINED = "memory_constrained"
    HIGH_MEMORY = "high_memory"
    DISTRIBUTED = "distributed"


@dataclass
class Config:
    """Configuration for CNPJ data pipeline with multi-database support and intelligent processing."""

    # Core settings
    debug: bool = field(
        default_factory=lambda: os.getenv("DEBUG", "false").lower() == "true"
    )
    base_url: str = (
        "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"
    )
    temp_dir: str = field(default_factory=lambda: os.getenv("TEMP_DIR", "./temp"))
    batch_size: int = field(default_factory=lambda: _get_int_env("BATCH_SIZE", "50000"))

    # Database configuration
    database_backend: DatabaseBackend = field(
        default_factory=lambda: DatabaseBackend(
            os.getenv("DATABASE_BACKEND", "postgresql")
        )
    )

    # Processing configuration
    processing_strategy: ProcessingStrategy = field(
        default_factory=lambda: ProcessingStrategy(
            os.getenv("PROCESSING_STRATEGY", "auto")
        )
    )
    max_memory_percent: float = field(
        default_factory=lambda: float(os.getenv("MAX_MEMORY_PERCENT", "80") or "80")
    )

    # PostgreSQL settings
    db_host: str = field(
        default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost")
    )
    db_port: int = field(default_factory=lambda: _get_int_env("POSTGRES_PORT", "5432"))
    db_name: str = field(default_factory=lambda: os.getenv("POSTGRES_DB", "cnpj"))
    db_user: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", "postgres"))
    db_password: str = field(
        default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "postgres")
    )

    # PostgreSQL-specific settings
    pg_work_mem: str = field(default_factory=lambda: os.getenv("PG_WORK_MEM", "256MB"))

    # MySQL settings (placeholder for future use)
    mysql_host: str = field(
        default_factory=lambda: os.getenv("MYSQL_HOST", "localhost")
    )
    mysql_port: int = field(default_factory=lambda: _get_int_env("MYSQL_PORT", "3306"))
    mysql_database: str = field(
        default_factory=lambda: os.getenv("MYSQL_DATABASE", "cnpj")
    )
    mysql_user: str = field(default_factory=lambda: os.getenv("MYSQL_USER", "root"))
    mysql_password: str = field(default_factory=lambda: os.getenv("MYSQL_PASSWORD", ""))

    # BigQuery settings (placeholder for future use)
    bq_project_id: Optional[str] = field(
        default_factory=lambda: os.getenv("BQ_PROJECT_ID")
    )
    bq_dataset: str = field(default_factory=lambda: os.getenv("BQ_DATASET", "cnpj"))
    bq_location: str = field(default_factory=lambda: os.getenv("BQ_LOCATION", "US"))

    # Retry configuration
    retry_attempts: int = field(
        default_factory=lambda: _get_int_env("RETRY_ATTEMPTS", "3")
    )
    retry_delay: int = field(default_factory=lambda: _get_int_env("RETRY_DELAY", "5"))

    # Timeout configuration
    connect_timeout: int = field(
        default_factory=lambda: _get_int_env("CONNECT_TIMEOUT", "30")
    )
    read_timeout: int = field(
        default_factory=lambda: _get_int_env("READ_TIMEOUT", "300")
    )

    # Memory optimization
    max_file_size_mb: int = field(
        default_factory=lambda: _get_int_env("MAX_FILE_SIZE_MB", "500")
    )
    encoding_chunk_size: int = field(
        default_factory=lambda: _get_int_env("ENCODING_CHUNK_SIZE", "52428800")
    )

    # Performance options
    download_strategy: str = field(
        default_factory=lambda: os.getenv("DOWNLOAD_STRATEGY", "sequential")
    )
    download_workers: int = field(
        default_factory=lambda: _get_int_env("DOWNLOAD_WORKERS", "4")
    )

    # File management options
    keep_downloaded_files: bool = field(
        default_factory=lambda: os.getenv("KEEP_DOWNLOADED_FILES", "false").lower()
        == "true"
    )

    def __post_init__(self):
        """Validate configuration and create directories."""
        os.makedirs(self.temp_dir, exist_ok=True)

        # Auto-detect processing strategy if needed
        if self.processing_strategy == ProcessingStrategy.AUTO:
            self.processing_strategy = self._detect_strategy()

    def _detect_strategy(self) -> ProcessingStrategy:
        """Auto-detect optimal processing strategy based on resources."""
        memory_gb = psutil.virtual_memory().total / (1024**3)
        cpu_count = psutil.cpu_count()

        if memory_gb < 8:
            return ProcessingStrategy.MEMORY_CONSTRAINED
        elif memory_gb < 32 or cpu_count < 8:
            return ProcessingStrategy.HIGH_MEMORY
        else:
            return ProcessingStrategy.DISTRIBUTED

    @property
    def optimal_chunk_size(self) -> int:
        """Calculate optimal chunk size based on available memory and strategy."""
        memory_gb = psutil.virtual_memory().total / (1024**3)

        # Rough estimates for CNPJ data processing
        if self.processing_strategy == ProcessingStrategy.MEMORY_CONSTRAINED:
            if memory_gb < 4:
                return 100_000
            elif memory_gb < 8:
                return 250_000
            else:
                return 500_000
        elif self.processing_strategy == ProcessingStrategy.HIGH_MEMORY:
            return 2_000_000
        else:  # DISTRIBUTED
            return 5_000_000

    @property
    def db_url(self) -> str:
        """Get database connection URL."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
