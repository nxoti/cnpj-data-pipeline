"""
Sequential download strategy for CNPJ data pipeline.

Downloads files one at a time in the order specified. This is the default
strategy that maintains backward compatibility with the original implementation.
"""

import logging
import time
from pathlib import Path
from typing import Iterator, List

from tqdm import tqdm

from .base import DownloadStrategy

logger = logging.getLogger(__name__)


class SequentialDownloadStrategy(DownloadStrategy):
    """
    Sequential download strategy.

    Downloads and processes files one at a time in the order specified.
    This is the safest strategy and matches the original implementation.

    **Trade-offs:**
    - Simple: No concurrency complexity
    - Robust: Easy to debug and handle errors
    - Corners Cut: Not optimized for speed, single-threaded downloads
    """

    def __init__(self, config):
        super().__init__(config)
        logger.debug("Initialized sequential download strategy")

    def download_files(self, directory: str, files: List[str]) -> Iterator[Path]:
        """
        Download files sequentially.

        Args:
            directory: Directory path containing the files
            files: List of filenames to download in order

        Yields:
            Path: Extracted CSV file paths as each file is completed
        """
        if not files:
            logger.info("No files to download")
            return

        self.stats["start_time"] = time.time()
        logger.info(
            f"Starting sequential download of {len(files)} files from {directory}"
        )

        try:
            # Create progress bar for overall progress
            with tqdm(total=len(files), desc="Downloading files", unit="file") as pbar:
                for i, filename in enumerate(files, 1):
                    pbar.set_description(f"Downloading {filename}")

                    try:
                        # Download and extract this file
                        extracted_files = self.download_single_file(directory, filename)

                        # Yield each extracted CSV file
                        for csv_file in extracted_files:
                            yield csv_file

                        logger.debug(f"✅ Completed {filename} ({i}/{len(files)})")
                        pbar.update(1)

                    except Exception as e:
                        error_msg = f"Failed to process {filename}: {e}"
                        logger.error(error_msg)
                        self.stats["errors"].append(error_msg)
                        pbar.update(1)
                        # Continue with next file instead of failing completely
                        continue

        finally:
            self.stats["end_time"] = time.time()

            # Log summary statistics
            if self.stats["start_time"]:
                duration = self.stats["end_time"] - self.stats["start_time"]
                logger.debug("Sequential download completed:")
                logger.debug(f"  Files downloaded: {self.stats['files_downloaded']}")
                logger.debug(f"  Total bytes: {self.stats['total_bytes']:,}")
                logger.debug(f"  Duration: {duration:.1f}s")
                logger.debug(f"  Errors: {len(self.stats['errors'])}")

                if self.stats["files_downloaded"] > 0:
                    avg_speed = (
                        self.stats["total_bytes"] / duration / 1024 / 1024
                    )  # MB/s
                    logger.debug(f"  Average speed: {avg_speed:.1f} MB/s")

    def get_strategy_name(self) -> str:
        """Get the name of this strategy."""
        return "sequential"
