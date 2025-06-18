import logging
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from bs4 import BeautifulSoup

from .download_strategies import create_download_strategy

logger = logging.getLogger(__name__)


class Downloader:
    """Handles downloading and extracting CNPJ files with configurable strategies."""

    def __init__(self, config):
        self.config = config
        self.temp_path = Path(config.temp_dir)
        self.temp_path.mkdir(exist_ok=True)

        # Initialize download strategy
        self.strategy = create_download_strategy(config)
        logger.debug(
            f"Downloader initialized with {self.strategy.get_strategy_name()} strategy"
        )

    def get_latest_directories(self) -> List[str]:
        """Get all available CNPJ data directories, sorted by date (newest first)."""
        try:
            response = requests.get(
                self.config.base_url,
                timeout=(self.config.connect_timeout, self.config.read_timeout),
            )
            if response.status_code != 200:
                logger.error(f"Failed to access base URL: {response.status_code}")
                return []

            soup = BeautifulSoup(response.text, "html.parser")
            dirs = []

            for link in soup.find_all("a"):
                href = link.get("href")
                text = link.text.strip()

                # Match directories in YYYY-MM format
                if href and text.strip("/").count("-") == 1:
                    dir_name = text.strip("/")
                    dirs.append(dir_name)

            dirs.sort(reverse=True)  # Newest first
            return dirs

        except Exception as e:
            logger.error(f"Error getting directories: {e}")
            return []

    def get_directory_files(self, directory: str) -> List[str]:
        """Get list of files in a specific directory."""
        try:
            url = f"{self.config.base_url}/{directory}/"
            response = requests.get(
                url, timeout=(self.config.connect_timeout, self.config.read_timeout)
            )

            if response.status_code != 200:
                logger.error(
                    f"Failed to access directory {directory}: {response.status_code}"
                )
                return []

            soup = BeautifulSoup(response.text, "html.parser")
            files = []

            for link in soup.find_all("a"):
                href = link.get("href")
                if href and href.endswith(".zip"):
                    files.append(href)

            return files

        except Exception as e:
            logger.error(f"Error getting files from {directory}: {e}")
            return []

    def organize_files_by_dependencies(
        self, files: List[str]
    ) -> Tuple[List[str], Dict[str, List[str]]]:
        """
        Organize files by their database dependencies.

        Returns:
            Tuple of (ordered_files_list, categorization_info)

        The categorization_info dict contains details about how files were organized,
        useful for logging and debugging.
        """
        # Reference tables first (no foreign keys)
        REFERENCE_TABLES = {
            "Cnaes.zip",
            "Motivos.zip",
            "Municipios.zip",
            "Naturezas.zip",
            "Paises.zip",
            "Qualificacoes.zip",
        }

        # Files to process after references, in dependency order
        ORDERED_PATTERNS = [
            # 1. Empresas (depends on naturezas_juridicas)
            "Empresas",
            # 2. Estabelecimentos (depends on empresas, municipios, motivos)
            "Estabelecimentos",
            # 3. Socios and Simples (depend on empresas)
            "Socios",
            "Simples",
        ]

        # Separate files into categories
        reference_files = []
        data_files = {pattern: [] for pattern in ORDERED_PATTERNS}
        unmatched_files = []

        for filename in files:
            if filename in REFERENCE_TABLES:
                reference_files.append(filename)
            else:
                # Check which pattern this file matches
                matched = False
                for pattern in ORDERED_PATTERNS:
                    if filename.startswith(pattern):
                        data_files[pattern].append(filename)
                        matched = True
                        break

                if not matched:
                    unmatched_files.append(filename)

        # Sort each category for consistent processing
        reference_files.sort()
        for pattern in ORDERED_PATTERNS:
            data_files[pattern].sort()

        # Build final processing order
        ordered_files = reference_files[:]  # Copy to avoid modifying original
        for pattern in ORDERED_PATTERNS:
            ordered_files.extend(data_files[pattern])

        # Add unmatched files at the end (edge case handling)
        ordered_files.extend(sorted(unmatched_files))

        # Build categorization info for logging/debugging
        categorization_info = {
            "reference_files": reference_files,
            "data_files": data_files,
            "unmatched_files": unmatched_files,
            "total_files": len(files),
            "ordered_count": len(ordered_files),
        }

        return ordered_files, categorization_info

    def download_and_extract(self, directory: str, filename: str) -> List[Path]:
        """
        Download and extract a single file using the configured strategy.

        This method provides backward compatibility with the existing interface
        while using the new strategy pattern under the hood.
        """
        return self.strategy.download_single_file(directory, filename)

    def download_files_batch(self, directory: str, files: List[str]) -> List[Path]:
        """
        Download and extract multiple files using the configured strategy.

        This is the new method that leverages the strategy pattern for
        potentially parallel downloads.

        Args:
            directory: Directory containing the files
            files: List of filenames to download

        Returns:
            List of all extracted CSV file paths
        """
        extracted_files = []

        # Use the strategy to download files (sequential or parallel)
        for csv_file_path in self.strategy.download_files(directory, files):
            extracted_files.append(csv_file_path)

        return extracted_files

    def cleanup(self):
        """Clean up temporary files."""
        self.strategy.cleanup()

    def get_download_stats(self) -> dict:
        """Get download statistics from the current strategy."""
        return self.strategy.get_stats()
