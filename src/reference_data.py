import logging
import time
import requests
from pathlib import Path
from typing import Optional, Set
import polars as pl

logger = logging.getLogger(__name__)


class ReferenceDataManager:
    """Manages reference data from multiple sources, handling gaps in official data."""

    URLS = {
        "motivos": "https://bcadastros.serpro.gov.br/documentacao/dominios/pj/motivo_situacao_cadastral.csv",
        # Future: Add other missing reference data URLs here
    }

    def __init__(self, config):
        self.config = config
        self.cache_dir = Path(config.temp_dir) / "reference_cache"
        self.cache_dir.mkdir(exist_ok=True)

    def download_reference(self, ref_type: str) -> Optional[Path]:
        """Download reference data from SERPRO if needed."""
        if ref_type not in self.URLS:
            return None

        url = self.URLS[ref_type]
        cache_file = self.cache_dir / f"serpro_{ref_type}.csv"

        # Use cached version if recent (within 30 days)
        if cache_file.exists():
            age_days = (time.time() - cache_file.stat().st_mtime) / 86400
            if age_days < 30:
                logger.info(
                    f"Using cached SERPRO {ref_type} data ({age_days:.1f} days old)"
                )
                return cache_file

        try:
            logger.info(f"Downloading SERPRO {ref_type} reference data...")
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            # SERPRO uses UTF-8 with BOM sometimes
            content = response.content
            if content.startswith(b"\xef\xbb\xbf"):
                content = content[3:]

            with open(cache_file, "wb") as f:
                f.write(content)

            logger.info(f"Downloaded SERPRO {ref_type} data to {cache_file}")
            return cache_file

        except Exception as e:
            logger.error(f"Failed to download SERPRO {ref_type} data: {e}")
            # Return cached version if exists, even if old
            if cache_file.exists():
                logger.warning("Using stale cache due to download failure")
                return cache_file
            return None

    def diff_motivos_data(self, existing_codes: Set[str]) -> Optional[pl.DataFrame]:
        """Get only the motivos codes that are missing from official data."""
        serpro_file = self.download_reference("motivos")
        if not serpro_file:
            return None

        try:
            # Read SERPRO data with correct separator
            serpro_df = pl.read_csv(
                serpro_file,
                separator=";",  # SERPRO uses semicolon
                encoding="utf-8",
                has_header=True,
                truncate_ragged_lines=True,
            )

            # Standardize column names
            column_mapping = {
                "Código": "codigo",
                "Descrição": "descricao",
            }

            for old_name, new_name in column_mapping.items():
                if old_name in serpro_df.columns:
                    serpro_df = serpro_df.rename({old_name: new_name})

            # Ensure we have required columns
            if "codigo" not in serpro_df.columns:
                logger.error(
                    f"SERPRO file missing 'codigo' column. Found: {serpro_df.columns}"
                )
                return None

            # Select and clean columns
            serpro_df = serpro_df.select(
                [
                    pl.col("codigo").cast(pl.Utf8).str.strip_chars(),
                    pl.col("descricao").cast(pl.Utf8).str.strip_chars()
                    if "descricao" in serpro_df.columns
                    else pl.lit("DESCRIÇÃO INDISPONÍVEL"),
                ]
            )

            # Pad single-digit codes with leading zero to match official format
            serpro_df = serpro_df.with_columns(
                [
                    pl.when(
                        (pl.col("codigo").str.len_chars() == 1)
                        & (pl.col("codigo").str.contains(r"^\d$"))
                    )
                    .then(pl.concat_str([pl.lit("0"), pl.col("codigo")]))
                    .otherwise(pl.col("codigo"))
                    .alias("codigo")
                ]
            )

            # Normalize descriptions: uppercase and remove accents
            import unicodedata

            def remove_accents(text: str) -> str:
                """Remove accents from text."""
                if text is None:
                    return text
                # NFD normalization splits accented chars into base + accent
                # Then encode to ASCII ignoring non-ASCII chars
                return (
                    unicodedata.normalize("NFD", text)
                    .encode("ascii", "ignore")
                    .decode("utf-8")
                )

            # Apply normalization
            serpro_df = serpro_df.with_columns(
                [
                    pl.col("descricao")
                    .str.to_uppercase()  # First uppercase
                    .map_elements(
                        remove_accents, return_dtype=pl.Utf8
                    )  # Then remove accents
                    .alias("descricao")
                ]
            )

            # Remove duplicates that may have been created by padding (keep first occurrence)
            serpro_df = serpro_df.unique(subset=["codigo"], keep="first")

            # Filter to only missing codes
            missing_df = serpro_df.filter(
                (pl.col("codigo").is_not_null())
                & (pl.col("codigo") != "")
                & (~pl.col("codigo").is_in(existing_codes))
            )

            if len(missing_df) > 0:
                logger.info(
                    f"Found {len(missing_df)} missing motivos codes in SERPRO data"
                )

            return missing_df

        except Exception as e:
            logger.error(f"Failed to process SERPRO motivos data: {e}")
            return None
