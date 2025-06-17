import logging
import requests
import zipfile
import time
from pathlib import Path
from bs4 import BeautifulSoup
from typing import List, Dict, Tuple

logger = logging.getLogger(__name__)


class Downloader:
    """Handles downloading and extracting CNPJ files."""

    def __init__(self, config):
        self.config = config
        self.temp_path = Path(config.temp_dir)
        self.temp_path.mkdir(exist_ok=True)

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
        """Download and extract a file, return list of extracted CSV paths."""
        url = f"{self.config.base_url}/{directory}/{filename}"
        zip_path = self.temp_path / filename

        # Download with retries
        for attempt in range(self.config.retry_attempts):
            try:
                logger.info(f"Downloading {filename} (attempt {attempt + 1})")

                response = requests.get(
                    url,
                    stream=True,
                    timeout=(self.config.connect_timeout, self.config.read_timeout),
                )
                response.raise_for_status()

                with open(zip_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                logger.info(f"Downloaded {filename} ({zip_path.stat().st_size} bytes)")
                break

            except Exception as e:
                logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < self.config.retry_attempts - 1:
                    time.sleep(self.config.retry_delay)
                else:
                    raise

        # Extract files
        extracted_files = []
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # Known CNPJ file patterns from processor.py
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
                        logger.info(f"Extracted CNPJ file: {member}")

            logger.info(f"Extracted {len(extracted_files)} CSV files")

        except Exception as e:
            logger.error(f"Error extracting {filename}: {e}")
            raise
        finally:
            # Cleanup zip file
            if zip_path.exists():
                zip_path.unlink()

        return extracted_files

    def cleanup(self):
        """Clean up temporary files."""
        try:
            for file in self.temp_path.glob("*"):
                if file.is_file():
                    file.unlink()

            logger.info("Temporary files cleaned up")

        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
