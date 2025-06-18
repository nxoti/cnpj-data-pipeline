"""
Parallel download strategy for CNPJ data pipeline.

Downloads multiple files concurrently using a thread pool to improve performance.
This strategy is more complex but can significantly reduce download times.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterator, List

from tqdm import tqdm

from .base import DownloadStrategy

logger = logging.getLogger(__name__)


class ParallelDownloadStrategy(DownloadStrategy):
    """
    Parallel download strategy.

    Downloads multiple files concurrently using a configurable number of worker threads.
    This can significantly improve performance but adds complexity.

    **Trade-offs:**
    - Performance: Multiple downloads in parallel
    - Complex: Thread management and error handling
    - Robust: Graceful degradation when workers fail
    - Edge Cases: Respects dependency order for reference tables
    """

    def __init__(self, config):
        super().__init__(config)
        self.max_workers = getattr(config, "download_workers", 4)
        logger.debug(
            f"Initialized parallel download strategy with {self.max_workers} workers"
        )

    def download_files(self, directory: str, files: List[str]) -> Iterator[Path]:
        """
        Download files in parallel while respecting dependencies.

        Reference tables are downloaded first sequentially, then data files
        are downloaded in parallel to avoid dependency issues.

        Args:
            directory: Directory path containing the files
            files: List of filenames to download

        Yields:
            Path: Extracted CSV file paths as they become available
        """
        if not files:
            logger.info("No files to download")
            return

        self.stats["start_time"] = time.time()
        logger.debug(
            f"Starting parallel download of {len(files)} files from {directory}"
        )
        logger.debug(f"Using {self.max_workers} worker threads")

        try:
            # Split files into reference tables and data files for dependency management
            reference_files, data_files = self._categorize_files(files)

            # Process reference files first (sequentially to maintain simplicity)
            if reference_files:
                logger.debug(
                    f"Processing {len(reference_files)} reference files sequentially"
                )
                with tqdm(
                    total=len(reference_files),
                    desc="Downloading reference files",
                    unit="file",
                ) as pbar:
                    for filename in reference_files:
                        pbar.set_description(f"Downloading {filename}")
                        try:
                            extracted_files = self.download_single_file(
                                directory, filename
                            )
                            for csv_file in extracted_files:
                                yield csv_file
                            pbar.update(1)
                        except Exception as e:
                            error_msg = (
                                f"Failed to process reference file {filename}: {e}"
                            )
                            logger.error(error_msg)
                            self.stats["errors"].append(error_msg)
                            pbar.update(1)

            # Process data files in parallel
            if data_files:
                logger.debug(f"Processing {len(data_files)} data files in parallel")
                yield from self._download_files_parallel(directory, data_files)

        finally:
            self.stats["end_time"] = time.time()

            # Log summary statistics
            if self.stats["start_time"]:
                duration = self.stats["end_time"] - self.stats["start_time"]
                logger.debug("Parallel download completed:")
                logger.debug(f"  Files downloaded: {self.stats['files_downloaded']}")
                logger.debug(f"  Total bytes: {self.stats['total_bytes']:,}")
                logger.debug(f"  Duration: {duration:.1f}s")
                logger.debug(f"  Workers: {self.max_workers}")
                logger.debug(f"  Errors: {len(self.stats['errors'])}")

                if self.stats["files_downloaded"] > 0:
                    avg_speed = (
                        self.stats["total_bytes"] / duration / 1024 / 1024
                    )  # MB/s
                    logger.debug(f"  Average speed: {avg_speed:.1f} MB/s")

    def _categorize_files(self, files: List[str]) -> tuple[List[str], List[str]]:
        """
        Categorize files into reference tables and data files.

        Reference tables should be processed first to satisfy dependencies.
        """
        reference_tables = {
            "Cnaes.zip",
            "Motivos.zip",
            "Municipios.zip",
            "Naturezas.zip",
            "Paises.zip",
            "Qualificacoes.zip",
        }

        reference_files = [f for f in files if f in reference_tables]
        data_files = [f for f in files if f not in reference_tables]

        return reference_files, data_files

    def _download_files_parallel(
        self, directory: str, files: List[str]
    ) -> Iterator[Path]:
        """
        Download data files in parallel using ThreadPoolExecutor.
        """
        if not files:
            return

        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Log which files will be downloaded in parallel
            logger.debug(
                f"Starting parallel download of {len(files)} files: {', '.join(files)}"
            )

            # Submit all download tasks
            future_to_filename = {
                executor.submit(
                    self.download_single_file, directory, filename
                ): filename
                for filename in files
            }

            # Process completed downloads as they finish
            for future in as_completed(future_to_filename):
                filename = future_to_filename[future]

                try:
                    extracted_files = future.result()

                    # Yield each extracted CSV file
                    for csv_file in extracted_files:
                        yield csv_file

                    logger.debug(f"✅ Completed parallel download: {filename}")

                except Exception as e:
                    error_msg = f"Failed to process {filename} in parallel: {e}"
                    logger.error(error_msg)
                    self.stats["errors"].append(error_msg)
                    # Continue with other files
                    continue

    def get_strategy_name(self) -> str:
        """Get the name of this strategy."""
        return f"parallel({self.max_workers})"
