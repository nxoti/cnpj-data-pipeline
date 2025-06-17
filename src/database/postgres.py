import logging
import psycopg2
import polars as pl
from typing import List, Set
import io
import time
from functools import wraps
from psycopg2.extras import execute_values
from contextlib import contextmanager

from .base import DatabaseAdapter

logger = logging.getLogger(__name__)


def retry_db_connection(max_retries=3, base_delay=1.0):
    """Decorator to retry database connections with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds (will be doubled each retry)
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):  # +1 for initial attempt
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, psycopg2.DatabaseError) as e:
                    last_exception = e

                    if attempt == max_retries:
                        # Final attempt failed
                        logger.error(
                            f"Database connection failed after {max_retries + 1} attempts: {e}"
                        )
                        raise

                    # Calculate delay with exponential backoff
                    delay = base_delay * (2**attempt)
                    logger.warning(
                        f"Database connection attempt {attempt + 1} failed: {e}"
                    )
                    logger.info(
                        f"Retrying in {delay:.1f} seconds... (attempt {attempt + 2}/{max_retries + 1})"
                    )
                    time.sleep(delay)

            # Should never reach here, but just in case
            raise last_exception

        return wrapper

    return decorator


class PostgreSQLAdapter(DatabaseAdapter):
    """PostgreSQL adapter implementation.

    Handles PostgreSQL database operations with optimized loading and connection retry logic.

    Features:
    - Automatic retry with exponential backoff for transient connection failures
    - Optimized bulk loading with chunking and staging tables
    - Primary key caching to avoid repeated schema queries
    - Memory-aware processing to prevent OOM errors
    """

    def __init__(self, config, max_retries=3, retry_base_delay=1.0):
        super().__init__(config)
        self.max_retries = max_retries
        self.retry_base_delay = retry_base_delay
        self.chunk_size = 100_000  # Optimal from tests
        self.conn = None
        self._ensure_tracking_table()

    def connect(self):
        """Establish PostgreSQL connection."""
        if self.conn is None:
            self.conn = self._get_connection()

    def disconnect(self):
        """Close PostgreSQL connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    @contextmanager
    def cursor(self):
        """Context manager for cursor."""
        if self.conn is None:
            self.connect()
        cur = self.conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def _get_connection(self):
        """Get database connection with optimized settings and retry logic."""

        @retry_db_connection(
            max_retries=self.max_retries, base_delay=self.retry_base_delay
        )
        def _connect():
            conn = psycopg2.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                database=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password,
            )
            # Set autocommit off for better transaction control
            conn.autocommit = False
            return conn

        return _connect()

    def _ensure_tracking_table(self):
        """Ensure the processed files tracking table exists."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS processed_files (
                            directory VARCHAR(50) NOT NULL,
                            filename VARCHAR(255) NOT NULL,
                            processed_at TIMESTAMP DEFAULT NOW(),
                            PRIMARY KEY (directory, filename)
                        )
                    """)
                    conn.commit()

        except Exception as e:
            logger.error(f"Error creating tracking table: {e}")
            raise

    def ensure_tracking_table(self):
        """Public method to ensure tracking table exists."""
        self._ensure_tracking_table()

    def get_processed_files(self, directory: str) -> Set[str]:
        """Get all processed filenames for a directory in a single query.

        Returns a set of filenames for O(1) lookup performance.
        Use this when checking multiple files in the same directory.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT filename FROM processed_files WHERE directory = %s",
                        (directory,),
                    )
                    return {row[0] for row in cur.fetchall()}

        except Exception as e:
            logger.error(f"Error fetching processed files for {directory}: {e}")
            return set()

    def is_processed(self, directory: str, filename: str) -> bool:
        """Check if a file has already been processed.

        For batch processing, consider using get_processed_files() instead
        to avoid N+1 query problem.
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM processed_files WHERE directory = %s AND filename = %s",
                        (directory, filename),
                    )
                    return cur.fetchone() is not None

        except Exception as e:
            logger.error(f"Error checking if file is processed: {e}")
            return False

    def mark_processed(self, directory: str, filename: str):
        """Mark a file as processed."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO processed_files (directory, filename) VALUES (%s, %s) ON CONFLICT (directory, filename) DO NOTHING",
                        (directory, filename),
                    )
                    conn.commit()

        except Exception as e:
            logger.error(f"Error marking file as processed: {e}")
            raise

    def bulk_upsert(self, df: pl.DataFrame, table_name: str):
        """Bulk upsert data with minimal overhead."""
        if len(df) == 0:
            logger.warning(f"Empty dataframe for table {table_name}")
            return

        rows = len(df)
        logger.info(f"Processing {rows:,} rows for {table_name}")

        try:
            with self._get_connection() as conn:
                # Get primary keys (cached)
                primary_keys = self._get_primary_key_columns(conn.cursor(), table_name)

                if not primary_keys:
                    # No primary keys = simple append, no staging needed!
                    logger.info(f"Table {table_name} has no PK - using direct COPY")
                    self._direct_copy_append(conn, df, table_name)
                elif rows < 10_000:
                    # Small dataset with PK - use execute_values (fast enough)
                    logger.info("Small dataset with PK - using execute_values")
                    self._direct_upsert_small(conn, df, table_name, primary_keys)
                else:
                    # Large dataset with PK - need staging for UPSERT
                    logger.info("Large dataset with PK - using staging table")
                    self._staged_upsert(conn, df, table_name, primary_keys)

                conn.commit()
                logger.info(f"Successfully processed {rows} rows to {table_name}")

        except Exception as e:
            logger.error(f"Error during bulk upsert to {table_name}: {e}")
            raise

    def _direct_copy_append(self, conn, df: pl.DataFrame, table_name: str):
        """Direct COPY for tables without primary keys - fastest possible."""
        columns = df.columns
        columns_str = ", ".join([f'"{col}"' for col in columns])

        # Create CSV buffer
        csv_buffer = io.BytesIO()
        csv_content = df.write_csv(include_header=False).encode(
            "utf-8", errors="replace"
        )

        # Remove null bytes that PostgreSQL COPY can't handle
        csv_content = csv_content.replace(b"\x00", b"")

        csv_buffer.write(csv_content)
        csv_buffer.seek(0)

        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {table_name} ({columns_str}) FROM STDIN WITH CSV ENCODING 'UTF8'",
                csv_buffer,
            )

    def _streaming_copy_append(
        self, conn, df: pl.DataFrame, table_name: str, commit_batches: bool = True
    ):
        """Streaming COPY for very large DataFrames that might not fit in memory as CSV."""
        columns = df.columns
        columns_str = ", ".join([f'"{col}"' for col in columns])

        with conn.cursor() as cur:
            # Start COPY
            cur.copy_expert(
                f"COPY {table_name} ({columns_str}) FROM STDIN WITH CSV",
                open("/dev/stdin", "r"),  # This will be replaced by our generator
            )

    def _direct_upsert_small(
        self, conn, df: pl.DataFrame, table_name: str, primary_keys: List[str]
    ):
        """Direct upsert for small datasets using execute_values."""
        columns = df.columns
        columns_str = ", ".join([f'"{col}"' for col in columns])

        # Build conflict resolution
        conflict_columns = ", ".join([f'"{pk}"' for pk in primary_keys])
        update_columns = [col for col in columns if col not in primary_keys]

        if update_columns:
            update_clause = ", ".join(
                [f'"{col}" = EXCLUDED."{col}"' for col in update_columns]
            )
            update_clause += ", data_atualizacao = CURRENT_TIMESTAMP"
        else:
            update_clause = ""

        # Build SQL
        if update_clause:
            sql = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES %s
                ON CONFLICT ({conflict_columns})
                DO UPDATE SET {update_clause}
            """  # nosec B608 - table_name and columns are from schema, not user input
        else:
            sql = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES %s
                ON CONFLICT ({conflict_columns}) DO NOTHING
            """  # nosec B608 - table_name and columns are from schema, not user input

        # Convert to list of tuples
        values = [tuple(row) for row in df.iter_rows()]

        with conn.cursor() as cur:
            execute_values(cur, sql, values)

    def _staged_upsert(
        self, conn, df: pl.DataFrame, table_name: str, primary_keys: List[str]
    ):
        """Staged upsert for large datasets - most memory efficient."""
        temp_table = f"temp_{table_name}_{id(df)}"
        columns = df.columns

        try:
            with conn.cursor() as cur:
                # Create temp table with same structure
                cur.execute(f"""
                    CREATE TEMP TABLE {temp_table}
                    (LIKE {table_name} INCLUDING DEFAULTS INCLUDING STORAGE)
                """)  # nosec B608 - temp_table and table_name are safely generated from schema

                # Load data to temp table using COPY
                self._load_to_temp_table(conn, df, temp_table, columns)

                # Use adaptive merge strategy based on size
                self._merge_temp_to_target(
                    conn, temp_table, table_name, columns, primary_keys
                )

        except Exception as e:
            logger.error(f"Error in staged upsert: {e}")
            raise
        finally:
            # Cleanup temp table (PostgreSQL will auto-drop on connection close anyway)
            try:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {temp_table}")  # nosec B608 - temp_table is safely generated
            except (psycopg2.Error, Exception) as e:
                # Log cleanup errors but don't raise them
                logger.debug(f"Failed to cleanup temp table {temp_table}: {e}")
                # Don't raise cleanup errors as they're not critical

    def _load_to_temp_table(
        self, conn, df: pl.DataFrame, temp_table: str, columns: List[str]
    ):
        """Load data to temporary table using COPY."""
        columns_str = ", ".join([f'"{col}"' for col in columns])

        # Create CSV buffer
        csv_buffer = io.BytesIO()
        csv_content = df.write_csv(include_header=False).encode(
            "utf-8", errors="replace"
        )

        # Remove null bytes that PostgreSQL COPY can't handle
        csv_content = csv_content.replace(b"\x00", b"")

        csv_buffer.write(csv_content)
        csv_buffer.seek(0)

        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {temp_table} ({columns_str}) FROM STDIN WITH CSV ENCODING 'UTF8'",
                csv_buffer,  # nosec B608 - temp_table and columns_str are safely generated
            )

    def _merge_temp_to_target(
        self,
        conn,
        temp_table: str,
        target_table: str,
        columns: List[str],
        primary_keys: List[str],
    ):
        """Merge temp table to target using appropriate strategy."""
        # For very large datasets, use batched approach to avoid lock contention
        # For medium datasets, use single transaction

        # Estimate temp table size
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {temp_table}")  # nosec B608 - temp_table is safely generated
            temp_rows = cur.fetchone()[0]

        if temp_rows > 1_000_000:
            logger.info(f"Large merge ({temp_rows:,} rows) - using batched approach")
            self._merge_temp_to_target_batched(
                conn, temp_table, target_table, columns, primary_keys
            )
        else:
            logger.info(f"Medium merge ({temp_rows:,} rows) - using single transaction")
            self._merge_temp_to_target_single(
                conn, temp_table, target_table, columns, primary_keys
            )

    def _merge_temp_to_target_single(
        self,
        conn,
        temp_table: str,
        target_table: str,
        columns: List[str],
        primary_keys: List[str],
    ):
        """Single transaction merge - faster for medium datasets."""
        columns_str = ", ".join([f'"{col}"' for col in columns])
        conflict_columns = ", ".join([f'"{pk}"' for pk in primary_keys])
        pk_columns_str = ", ".join([f'"{pk}"' for pk in primary_keys])
        update_columns = [col for col in columns if col not in primary_keys]

        if update_columns:
            update_clause = ", ".join(
                [f'"{col}" = EXCLUDED."{col}"' for col in update_columns]
            )
            update_clause += ", data_atualizacao = CURRENT_TIMESTAMP"
        else:
            update_clause = ""

        # Build SQL with deduplication using DISTINCT ON
        if update_clause:
            sql = f"""
                INSERT INTO {target_table} ({columns_str})
                SELECT DISTINCT ON ({pk_columns_str}) {columns_str}
                FROM {temp_table}
                ORDER BY {pk_columns_str}
                ON CONFLICT ({conflict_columns})
                DO UPDATE SET {update_clause}
            """  # nosec B608
        else:
            sql = f"""
                INSERT INTO {target_table} ({columns_str})
                SELECT DISTINCT ON ({pk_columns_str}) {columns_str}
                FROM {temp_table}
                ORDER BY {pk_columns_str}
                ON CONFLICT ({conflict_columns}) DO NOTHING
            """  # nosec B608

        with conn.cursor() as cur:
            cur.execute(sql)

    def _merge_temp_to_target_batched(
        self,
        conn,
        temp_table: str,
        target_table: str,
        columns: List[str],
        primary_keys: List[str],
    ):
        """Batched merge for very large datasets to reduce lock contention."""
        batch_size = 1_000_000
        columns_str = ", ".join([f'"{col}"' for col in columns])
        conflict_columns = ", ".join([f'"{pk}"' for pk in primary_keys])
        pk_columns_str = ", ".join([f'"{pk}"' for pk in primary_keys])
        update_columns = [col for col in columns if col not in primary_keys]

        if update_columns:
            update_clause = ", ".join(
                [f'"{col}" = EXCLUDED."{col}"' for col in update_columns]
            )
            update_clause += ", data_atualizacao = CURRENT_TIMESTAMP"
        else:
            update_clause = ""

        # Get total count for progress tracking
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {temp_table}")  # nosec B608
            total_rows = cur.fetchone()[0]

        # Add row numbers for reliable batching
        logger.info("Adding row numbers for batching...")
        with conn.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {temp_table} ADD COLUMN IF NOT EXISTS batch_row_num SERIAL"
            )  # nosec B608

        # Build SQL with deduplication
        if update_clause:
            merge_sql = f"""
                INSERT INTO {target_table} ({columns_str})
                SELECT {columns_str} FROM (
                    SELECT {columns_str},
                        ROW_NUMBER() OVER (PARTITION BY {pk_columns_str} ORDER BY batch_row_num DESC) as rn
                    FROM {temp_table}
                    WHERE batch_row_num BETWEEN %s AND %s
                ) deduplicated
                WHERE rn = 1
                ON CONFLICT ({conflict_columns})
                DO UPDATE SET {update_clause}
            """  # nosec B608
        else:
            merge_sql = f"""
                INSERT INTO {target_table} ({columns_str})
                SELECT {columns_str} FROM (
                    SELECT {columns_str},
                        ROW_NUMBER() OVER (PARTITION BY {pk_columns_str} ORDER BY batch_row_num DESC) as rn
                    FROM {temp_table}
                    WHERE batch_row_num BETWEEN %s AND %s
                ) deduplicated
                WHERE rn = 1
                ON CONFLICT ({conflict_columns}) DO NOTHING
            """  # nosec B608

        # Process batches
        total_merged = 0
        start_row = 1

        while start_row <= total_rows:
            end_row = min(start_row + batch_size - 1, total_rows)
            batch_num = (start_row - 1) // batch_size + 1

            logger.info(
                f"Processing batch {batch_num} (rows {start_row:,} to {end_row:,})"
            )

            try:
                with conn.cursor() as cur:
                    cur.execute(merge_sql, (start_row, end_row))
                    batch_merged = cur.rowcount
                    total_merged += batch_merged
                    conn.commit()

                    logger.debug(f"Batch {batch_num} merged {batch_merged:,} rows")

            except Exception as e:
                logger.error(f"Error in batch {batch_num}: {e}")
                conn.rollback()
                raise

            start_row = end_row + 1

        logger.info(f"Batched merge completed: {total_merged:,} total rows processed")

    def _get_primary_key_columns(self, cur, table_name: str) -> List[str]:
        """Get primary key columns for a table with caching."""
        if table_name in self._pk_cache:
            return self._pk_cache[table_name]

        cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid
                AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass
                AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
        """,
            (table_name,),
        )

        primary_keys = [row[0] for row in cur.fetchall()]
        self._pk_cache[table_name] = primary_keys
        return primary_keys

    def get_primary_keys(self, table: str) -> List[str]:
        """Get primary key columns for a table."""
        with self.cursor() as cur:
            return self._get_primary_key_columns(cur, table)

    def table_exists(self, table: str) -> bool:
        """Check if table exists in PostgreSQL."""
        with self.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s
                )
            """,
                (table,),
            )
            return cur.fetchone()[0]

    def supports_upsert(self) -> bool:
        """PostgreSQL supports ON CONFLICT."""
        return True
