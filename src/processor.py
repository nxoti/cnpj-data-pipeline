import logging
import polars as pl
from pathlib import Path
from typing import Tuple, Optional, List
import tempfile
import psutil
import os
import gc

logger = logging.getLogger(__name__)

# Simple mapping of file patterns to table names
FILE_MAPPINGS = {
    "CNAECSV": "cnaes",
    "MOTICSV": "motivos",
    "MUNICCSV": "municipios",
    "NATJUCSV": "naturezas_juridicas",
    "PAISCSV": "paises",
    "QUALSCSV": "qualificacoes_socios",
    "EMPRECSV": "empresas",
    "ESTABELE": "estabelecimentos",
    "SOCIOCSV": "socios",
    "SIMPLESCSV": "dados_simples",
}

# Column mappings for different file types
COLUMN_MAPPINGS = {
    "CNAECSV": {0: "codigo", 1: "descricao"},
    "MOTICSV": {0: "codigo", 1: "descricao"},
    "MUNICCSV": {0: "codigo", 1: "descricao"},
    "NATJUCSV": {0: "codigo", 1: "descricao"},
    "PAISCSV": {0: "codigo", 1: "descricao"},
    "QUALSCSV": {0: "codigo", 1: "descricao"},
    "EMPRECSV": {
        0: "cnpj_basico",
        1: "razao_social",
        2: "natureza_juridica",
        3: "qualificacao_responsavel",
        4: "capital_social",
        5: "porte",
        6: "ente_federativo_responsavel",
    },
    "ESTABELE": {
        0: "cnpj_basico",
        1: "cnpj_ordem",
        2: "cnpj_dv",
        3: "identificador_matriz_filial",
        4: "nome_fantasia",
        5: "situacao_cadastral",
        6: "data_situacao_cadastral",
        7: "motivo_situacao_cadastral",
        8: "nome_cidade_exterior",
        9: "pais",
        10: "data_inicio_atividade",
        11: "cnae_fiscal_principal",
        12: "cnae_fiscal_secundaria",
        13: "tipo_logradouro",
        14: "logradouro",
        15: "numero",
        16: "complemento",
        17: "bairro",
        18: "cep",
        19: "uf",
        20: "municipio",
        21: "ddd_1",
        22: "telefone_1",
        23: "ddd_2",
        24: "telefone_2",
        25: "ddd_fax",
        26: "fax",
        27: "correio_eletronico",
        28: "situacao_especial",
        29: "data_situacao_especial",
    },
    "SOCIOCSV": {
        0: "cnpj_basico",
        1: "identificador_de_socio",
        2: "nome_socio",
        3: "cnpj_cpf_do_socio",
        4: "qualificacao_do_socio",
        5: "data_entrada_sociedade",
        6: "pais",
        7: "representante_legal",
        8: "nome_do_representante",
        9: "qualificacao_do_representante_legal",
        10: "faixa_etaria",
    },
    "SIMPLESCSV": {
        0: "cnpj_basico",
        1: "opcao_pelo_simples",
        2: "data_opcao_pelo_simples",
        3: "data_exclusao_do_simples",
        4: "opcao_pelo_mei",
        5: "data_opcao_pelo_mei",
        6: "data_exclusao_do_mei",
    },
}

# Numeric columns that need comma-to-point conversion
NUMERIC_COLUMNS = {
    "EMPRECSV": ["capital_social"],
    "ESTABELE": [],
    "SIMPLESCSV": [],
    "SOCIOCSV": [],
}

# Date columns that need cleaning
DATE_COLUMNS = {
    "EMPRECSV": [],
    "ESTABELE": [
        "data_situacao_cadastral",
        "data_inicio_atividade",
        "data_situacao_especial",
    ],
    "SIMPLESCSV": [
        "data_opcao_pelo_simples",
        "data_exclusao_do_simples",
        "data_opcao_pelo_mei",
        "data_exclusao_do_mei",
    ],
    "SOCIOCSV": ["data_entrada_sociedade"],
}


class Processor:
    """Handles processing and transforming CSV files."""

    def __init__(self, config):
        self.config = config
        self.debug = config.debug

    def _log_memory_usage(self, context: str):
        """Log current memory usage if debug mode is enabled."""
        if self.debug:
            memory = psutil.virtual_memory()
            process = psutil.Process(os.getpid())
            process_memory = process.memory_info()

            logger.debug(f"[{context}] Memory Status:")
            logger.debug(
                f"  System: {memory.percent:.1f}% used ({memory.used / 1024**3:.2f}GB / {memory.total / 1024**3:.2f}GB)"
            )
            logger.debug(f"  Process RSS: {process_memory.rss / 1024**3:.2f}GB")
            logger.debug(f"  Process VMS: {process_memory.vms / 1024**3:.2f}GB")

    def _get_file_size_mb(self, file_path: Path) -> float:
        """Get file size in MB."""
        return file_path.stat().st_size / (1024 * 1024)

    def _get_file_type(self, filename: str) -> Optional[str]:
        """Determine file type from filename."""
        filename_upper = filename.upper()

        for pattern in FILE_MAPPINGS.keys():
            if pattern in filename_upper:
                if self.debug:
                    logger.debug(f"File type detected: {pattern} for {filename}")
                return pattern

        logger.warning(f"Unknown file type for: {filename}")
        return None

    def _check_memory_available(self, required_mb: float) -> bool:
        """Check if enough memory is available."""
        memory = psutil.virtual_memory()
        available_mb = memory.available / (1024 * 1024)

        # Leave at least 20% system memory free
        safety_margin_mb = (memory.total * 0.2) / (1024 * 1024)

        return available_mb > (required_mb + safety_margin_mb)

    def _convert_file_encoding_chunked(
        self, input_file: Path, output_file: Optional[Path] = None
    ) -> Path:
        """Convert file encoding from ISO-8859-1 to UTF-8 using chunked reading for large files."""
        if output_file is None:
            temp_fd, temp_path = tempfile.mkstemp(suffix=".utf8.csv")
            output_file = Path(temp_path)
            os.close(temp_fd)

        file_size_mb = self._get_file_size_mb(input_file)
        logger.info(f"Converting encoding for {input_file.name} ({file_size_mb:.2f}MB)")

        if self.debug:
            logger.debug(
                f"Using chunk size: {self.config.encoding_chunk_size / 1024**2:.2f}MB"
            )

        self._log_memory_usage("Before encoding conversion")

        try:
            with open(
                input_file,
                "r",
                encoding="ISO-8859-1",
                buffering=self.config.encoding_chunk_size,
            ) as infile:
                with open(
                    output_file,
                    "w",
                    encoding="UTF-8",
                    buffering=self.config.encoding_chunk_size,
                ) as outfile:
                    chunk_count = 0
                    while True:
                        chunk = infile.read(self.config.encoding_chunk_size)
                        if not chunk:
                            break

                        outfile.write(chunk)
                        chunk_count += 1

                        if self.debug and chunk_count % 10 == 0:
                            logger.debug(
                                f"Processed {chunk_count * self.config.encoding_chunk_size / 1024**2:.2f}MB"
                            )
                            self._log_memory_usage(
                                f"During encoding (chunk {chunk_count})"
                            )

            converted_size_mb = self._get_file_size_mb(output_file)
            logger.info(f"Encoding conversion complete: {converted_size_mb:.2f}MB")
            self._log_memory_usage("After encoding conversion")

            return output_file

        except Exception as e:
            logger.error(f"Error converting file encoding: {str(e)}")
            if output_file.exists():
                output_file.unlink()
            raise

    def _read_csv_lazy(self, file_path: Path, file_type: str) -> pl.LazyFrame:
        """Read CSV file using lazy evaluation for better memory management."""
        logger.info(f"Reading CSV file lazily: {file_path.name}")

        if self.debug:
            logger.debug("Using lazy frame evaluation to minimize memory usage")

        # Start with lazy reading
        lf = pl.scan_csv(
            file_path,
            separator=";",
            encoding="utf8",
            has_header=False,
            null_values=[""],
            ignore_errors=True,
            infer_schema_length=0,
            low_memory=True,
        )

        return lf

    def _get_actual_column_names(self, lf: pl.LazyFrame) -> List[str]:
        """Get the actual column names from a lazy frame."""
        # Get schema to see actual column names
        schema = lf.schema
        return list(schema.keys())

    def _apply_transformations_lazy(
        self, lf: pl.LazyFrame, file_type: str
    ) -> pl.LazyFrame:
        """Apply transformations to a lazy frame."""
        try:
            col_mapping = COLUMN_MAPPINGS.get(file_type, {})

            if col_mapping:
                # Get actual column names from the lazy frame
                actual_columns = self._get_actual_column_names(lf)

                if self.debug:
                    logger.debug(
                        f"Actual columns in lazy frame: {actual_columns[:5]}..."
                    )
                    logger.debug(f"Expected mapping has {len(col_mapping)} columns")

                # Create mapping from actual column names to desired names
                rename_mapping = {}
                for i, actual_col in enumerate(actual_columns):
                    if i in col_mapping:
                        rename_mapping[actual_col] = col_mapping[i]
                    else:
                        # Keep columns that aren't in the mapping
                        rename_mapping[actual_col] = actual_col

                if self.debug:
                    logger.debug(
                        f"Rename mapping (first 5): {dict(list(rename_mapping.items())[:5])}"
                    )

                # Apply renaming
                lf = lf.rename(rename_mapping)

            # Convert numeric columns
            numeric_cols = NUMERIC_COLUMNS.get(file_type, [])
            for col in numeric_cols:
                # Check if column exists in the lazy frame
                if col in lf.columns:
                    lf = lf.with_columns(
                        pl.col(col).str.replace(",", ".").cast(pl.Float64, strict=False)
                    )

            # Clean date columns
            date_cols = DATE_COLUMNS.get(file_type, [])
            for col in date_cols:
                # Check if column exists in the lazy frame
                if col in lf.columns:
                    lf = lf.with_columns(
                        pl.when(pl.col(col) == "0")
                        .then(None)
                        .otherwise(pl.col(col))
                        .alias(col)
                    )

            return lf

        except Exception as e:
            logger.error(f"Error applying transformations to {file_type}: {e}")
            logger.error(f"Schema: {lf.schema}")
            raise

    def process_file(self, file_path: Path) -> Tuple[pl.DataFrame, str]:
        """Process a single CSV file and return dataframe and table name."""
        utf8_file = None
        try:
            file_size_mb = self._get_file_size_mb(file_path)
            logger.info(f"Processing file: {file_path.name} ({file_size_mb:.2f}MB)")

            self._log_memory_usage("Start of process_file")

            # Determine file type
            file_type = self._get_file_type(file_path.name)
            if not file_type:
                raise ValueError(f"Cannot determine file type for {file_path.name}")

            # Convert file encoding
            if self.debug:
                logger.debug("Starting encoding conversion...")
            utf8_file = self._convert_file_encoding_chunked(file_path)

            # Force garbage collection after encoding conversion
            gc.collect()
            self._log_memory_usage("After GC post-encoding")

            # Check if file is too large for direct loading
            utf8_size_mb = self._get_file_size_mb(utf8_file)

            if utf8_size_mb > self.config.max_file_size_mb:
                logger.warning(
                    f"File size ({utf8_size_mb:.2f}MB) exceeds max_file_size_mb ({self.config.max_file_size_mb}MB)"
                )
                logger.info("Using chunked processing approach...")

                # For very large files, process in chunks
                return self._process_large_file_chunked(utf8_file, file_type)
            else:
                # Use regular processing for moderate files
                if self.debug:
                    logger.debug("Starting CSV parsing...")

                # Read with normal polars for files under the limit
                df = pl.read_csv(
                    utf8_file,
                    separator=";",
                    encoding="utf8",
                    has_header=False,
                    null_values=[""],
                    ignore_errors=True,
                    infer_schema_length=0,
                    low_memory=False,
                )

                # Apply transformations directly to dataframe
                df = self._apply_transformations(df, file_type)

                self._log_memory_usage("After processing")

                # Get table name
                table_name = FILE_MAPPINGS[file_type]

                logger.info(f"Processed {len(df)} rows for table {table_name}")
                return df, table_name

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            self._log_memory_usage("Error in process_file")
            raise
        finally:
            # Clean up the UTF-8 converted file
            if utf8_file and utf8_file.exists():
                try:
                    utf8_file.unlink()
                    logger.debug(f"Deleted converted file: {utf8_file}")
                except Exception as e:
                    logger.warning(f"Could not delete temporary file {utf8_file}: {e}")

    def _apply_transformations(self, df: pl.DataFrame, file_type: str) -> pl.DataFrame:
        """Apply necessary transformations to the dataframe (non-lazy version)."""
        try:
            # Get column mapping for this file type
            col_mapping = COLUMN_MAPPINGS.get(file_type, {})

            # Rename columns
            if col_mapping:
                # Create new column names list
                new_columns = []
                for i in range(len(df.columns)):
                    new_columns.append(col_mapping.get(i, f"column_{i}"))
                df = df.rename(dict(zip(df.columns, new_columns)))

            # Convert numeric columns (comma to point)
            numeric_cols = NUMERIC_COLUMNS.get(file_type, [])
            for col in numeric_cols:
                if col in df.columns:
                    df = df.with_columns(
                        pl.col(col).str.replace(",", ".").cast(pl.Float64, strict=False)
                    )

            # Clean date columns
            date_cols = DATE_COLUMNS.get(file_type, [])
            for col in date_cols:
                if col in df.columns:
                    df = df.with_columns(
                        pl.when(pl.col(col) == "0")
                        .then(None)
                        .otherwise(pl.col(col))
                        .alias(col)
                    )

            return df

        except Exception as e:
            logger.error(f"Error applying transformations to {file_type}: {e}")
            raise

    def _process_large_file_chunked(
        self, file_path: Path, file_type: str
    ) -> Tuple[None, str]:
        """Process very large files in chunks, loading directly to database."""
        logger.info("Processing large file in chunks with direct database loading...")

        chunk_size = 1_000_000  # Smaller chunks
        table_name = FILE_MAPPINGS[file_type]

        # We need database access here
        from src.database.factory import create_database_adapter

        db = create_database_adapter(self.config)

        try:
            # First, get a small sample to understand the structure
            sample_df = pl.read_csv(
                file_path,
                separator=";",
                encoding="utf8",
                has_header=False,
                null_values=[""],
                ignore_errors=True,
                infer_schema_length=0,
                n_rows=100,
            )

            # Apply transformations to understand the schema
            sample_df = self._apply_transformations(sample_df, file_type)
            expected_columns = sample_df.columns

            if self.debug:
                logger.debug(f"Expected columns: {expected_columns}")

            # Process file in chunks
            offset = 0
            batch_num = 0
            total_processed = 0

            while True:
                batch_num += 1
                logger.info(f"Processing batch {batch_num} (offset: {offset:,})")
                self._log_memory_usage(f"Before batch {batch_num}")

                # Read a chunk
                chunk_df = pl.read_csv(
                    file_path,
                    separator=";",
                    encoding="utf8",
                    has_header=False,
                    null_values=[""],
                    ignore_errors=True,
                    infer_schema_length=0,
                    skip_rows=offset,
                    n_rows=chunk_size,
                )

                if len(chunk_df) == 0:
                    break

                # Apply transformations
                chunk_df = self._apply_transformations(chunk_df, file_type)

                # Load this chunk directly to database
                logger.info(
                    f"Loading batch {batch_num} to database ({len(chunk_df):,} rows)"
                )
                db.bulk_upsert(chunk_df, table_name)

                total_processed += len(chunk_df)
                offset += len(chunk_df)

                # Explicitly delete the chunk and force garbage collection
                del chunk_df

                # Periodic GC
                if batch_num % 3 == 0:
                    logger.debug(f"Running aggressive GC after batch {batch_num}")
                    gc.collect()
                    gc.collect()  # Second pass
                else:
                    gc.collect()  # Single pass for other batches

                self._log_memory_usage(f"After batch {batch_num} (post GC)")

                # Break if we read less than chunk_size (end of file)
                if offset % chunk_size != 0:
                    break

            logger.info(
                f"Completed chunked processing: {total_processed:,} total rows processed"
            )

            # Return None for dataframe since we already loaded to DB
            return None

        except Exception as e:
            logger.error(f"Error in chunked processing: {e}")
            raise
