"""
Base download strategy for CNPJ data pipeline.

Defines the interface that all download strategies must implement.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterator, List

logger = logging.getLogger(__name__)


class DownloadStrategy(ABC):
    """
    Abstract base class for download strategies.

    Implements the Strategy pattern to allow different download approaches
    (sequential, parallel, etc.) while maintaining the same interface.
    """

    def __init__(self, config):
        """
        Initialize the download strategy.

        Args:
            config: Configuration object with download settings
        """
        self.config = config
        self.temp_path = Path(config.temp_dir)
        self.temp_path.mkdir(exist_ok=True)
        self.stats = {
            "files_downloaded": 0,
            "total_bytes": 0,
            "start_time": None,
            "end_time": None,
            "errors": [],
        }

    @abstractmethod
    def download_files(self, directory: str, files: List[str]) -> Iterator[Path]:
        """
        Download and extract multiple files.

        Args:
            directory: Directory path containing the files
            files: List of filenames to download

        Yields:
            Path: Extracted CSV file paths as they become available
        """
        pass

    def download_single_file(self, directory: str, filename: str) -> List[Path]:
        """
        Download and extract a single file.

        This is the core download logic that will be shared between strategies.

        Args:
            directory: Directory path containing the file
            filename: Filename to download

        Returns:
            List of extracted CSV file paths
        """
        import time
        import zipfile

        import requests

        # Check for existing extracted files if keeping downloads
        if self.config.keep_downloaded_files:
            existing_files = self._check_existing_csv_files(filename)
            if existing_files:
                logger.info(
                    f"Found existing CSV files for {filename}, skipping download"
                )
                return existing_files

        url = f"{self.config.base_url}/{directory}/{filename}"
        zip_path = self.temp_path / filename

        # Download with retries
        for attempt in range(self.config.retry_attempts):
            try:
                logger.debug(f"Downloading {filename} (attempt {attempt + 1})")

                response = requests.get(
                    url,
                    stream=True,
                    timeout=(self.config.connect_timeout, self.config.read_timeout),
                )
                response.raise_for_status()

                bytes_downloaded = 0
                with open(zip_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        bytes_downloaded += len(chunk)

                logger.debug(f"Downloaded {filename} ({bytes_downloaded} bytes)")
                self.stats["files_downloaded"] += 1
                self.stats["total_bytes"] += bytes_downloaded
                break

            except Exception as e:
                error_msg = f"Download attempt {attempt + 1} failed for {filename}: {e}"
                logger.warning(error_msg)
                self.stats["errors"].append(error_msg)

                if attempt < self.config.retry_attempts - 1:
                    time.sleep(self.config.retry_delay)
                else:
                    raise

        # Extract files
        extracted_files = []
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # Known CNPJ file patterns
                known_patterns = [
                    "CNAECSV",
                    "MOTICSV",
                    "MUNICCSV",
                    "NATJUCSV",
                    "PAISCSV",
                    "QUALSCSV",
                    "EMPRECSV",
                    "ESTABELE",
                    "SOCIOCSV",
                    "SIMPLESCSV",
                ]

                for member in zip_ref.namelist():
                    # Check if member ends with any known CNPJ file pattern
                    member_upper = member.upper()
                    is_cnpj_file = any(
                        member_upper.endswith(pattern) for pattern in known_patterns
                    )

                    if is_cnpj_file:
                        extract_path = self.temp_path / member
                        zip_ref.extract(member, self.temp_path)
                        extracted_files.append(extract_path)
                        logger.debug(f"Extracted CNPJ file: {member}")

            logger.debug(f"Extracted {len(extracted_files)} CSV files from {filename}")

        except Exception as e:
            logger.error(f"Error extracting {filename}: {e}")
            raise
        finally:
            # Only cleanup zip file if not keeping downloaded files
            if zip_path.exists() and not self.config.keep_downloaded_files:
                zip_path.unlink()

        return extracted_files

    def _check_existing_csv_files(self, zip_filename: str) -> List[Path]:
        """
        Check if CSV files from this ZIP already exist in temp directory.

        Args:
            zip_filename: Name of the ZIP file (e.g., "Empresas0.zip")

        Returns:
            List of existing CSV file paths, empty if none found
        """
        # Known CNPJ file patterns that would be extracted from this ZIP
        known_patterns = [
            "CNAECSV",
            "MOTICSV",
            "MUNICCSV",
            "NATJUCSV",
            "PAISCSV",
            "QUALSCSV",
            "EMPRECSV",
            "ESTABELE",
            "SOCIOCSV",
            "SIMPLESCSV",
        ]

        existing_files = []

        # Look for CSV files that could have come from this ZIP
        for csv_file in self.temp_path.glob("*.csv"):
            csv_name_upper = csv_file.name.upper()

            # Check if this CSV matches any known pattern
            is_cnpj_file = any(
                csv_name_upper.endswith(pattern) for pattern in known_patterns
            )

            if is_cnpj_file:
                # Simple heuristic: if the CSV file exists and the ZIP filename
                # contains similar pattern, assume it's from this ZIP
                zip_base = zip_filename.replace(".zip", "").upper()

                # Check if the ZIP base name is related to this CSV
                # This is a simple check - could be more sophisticated
                if any(
                    pattern in zip_base
                    for pattern in known_patterns
                    if pattern in csv_name_upper
                ):
                    existing_files.append(csv_file)
                    logger.debug(f"Found existing CSV file: {csv_file.name}")

        return existing_files

    def get_stats(self) -> Dict[str, Any]:
        """Get download statistics."""
        return self.stats.copy()

    def reset_stats(self):
        """Reset download statistics."""
        self.stats = {
            "files_downloaded": 0,
            "total_bytes": 0,
            "start_time": None,
            "end_time": None,
            "errors": [],
        }

    def cleanup(self):
        """Clean up temporary files."""
        if self.config.keep_downloaded_files:
            logger.info(
                "Keeping downloaded files as requested (KEEP_DOWNLOADED_FILES=true)"
            )
            return

        try:
            for file in self.temp_path.glob("*"):
                if file.is_file():
                    file.unlink()
            logger.debug("Temporary files cleaned up")
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")


class DownloadError(Exception):
    """Custom exception for download-related errors."""

    pass
