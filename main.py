#!/usr/bin/env python3
"""
CNPJ Data Pipeline - Main entry point

Configurable, efficient processor for CNPJ data files with database abstraction.
Supports multiple databases and intelligent processing strategies.

Usage: python main.py
"""

import logging
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

from src.config import Config
from src.downloader import Downloader
from src.processor import Processor
from src.database.factory import create_database_adapter

# Load environment variables
load_dotenv()


def setup_logging(config: Config):
    """Configure logging with enhanced output."""
    log_level = logging.DEBUG if config.debug else logging.INFO

    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "cnpj_loader.log"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger(__name__)


def main():
    """Main processing function."""
    # Initialize basic logging first (before config in case config fails)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    logger = logging.getLogger(__name__)

    try:
        # Initialize configuration
        config = Config()

        # Setup enhanced logging with config
        logger = setup_logging(config)

        logger.info("=" * 60)
        logger.info("CNPJ Data Pipeline Starting")
        logger.info(f"Database: {config.database_backend.value}")
        logger.info(f"Processing Strategy: {config.processing_strategy.value}")
        logger.info(f"Batch Size: {config.batch_size:,}")
        logger.info(f"Optimal Chunk Size: {config.optimal_chunk_size:,}")
        logger.info("=" * 60)

        # Initialize components
        downloader = Downloader(config)
        processor = Processor(config)
        db = create_database_adapter(config)

        # Connect to database and setup
        logger.info(f"Connecting to {config.database_backend.value}...")
        db.connect()
        db.ensure_tracking_table()

        logger.info("Starting CNPJ data processing")

        # Get latest directory
        directories = downloader.get_latest_directories()
        if not directories:
            logger.error("No directories found")
            return

        latest_dir = directories[0]
        logger.info(f"Processing directory: {latest_dir}")

        # Get files in latest directory
        files = downloader.get_directory_files(latest_dir)
        if not files:
            logger.error(f"No files found in {latest_dir}")
            return

        # Get all processed files for this directory
        # Note: it won't check files that might be processed later during same run
        processed_files = db.get_processed_files(latest_dir)
        logger.info(
            f"Found {len(processed_files)} already processed files in {latest_dir}"
        )

        # Organize files by database dependencies
        ordered_files, categorization = downloader.organize_files_by_dependencies(files)

        logger.info("Processing files in dependency order:")
        logger.info(
            f"  Reference tables: {len(categorization['reference_files'])} files"
        )
        for pattern, pattern_files in categorization["data_files"].items():
            if pattern_files:
                logger.info(f"  {pattern}: {len(pattern_files)} files")

        if categorization["unmatched_files"]:
            logger.warning(
                f"  Unmatched files: {len(categorization['unmatched_files'])} files"
            )
            logger.warning(f"    Files: {categorization['unmatched_files']}")

        # Process each file
        total_start = time.time()
        files_processed = 0
        total_rows = 0

        for filename in ordered_files:
            if filename in processed_files:
                logger.info(f"Skipping already processed file: {latest_dir}/{filename}")
                continue

            logger.info(f"\nProcessing file: {latest_dir}/{filename}")
            file_start = time.time()

            try:
                # Download and extract
                extracted_files = downloader.download_and_extract(latest_dir, filename)

                for csv_file in extracted_files:
                    result = processor.process_file(csv_file)

                    if result is None:
                        # File was processed in chunks directly to DB
                        continue

                    df, table_name = result

                    # Insert/upsert data using bulk_upsert method
                    # The PostgreSQL adapter automatically handles upsert vs insert logic
                    db.bulk_upsert(df, table_name)

                    total_rows += len(df) if df is not None else 0

                # Calculate duration
                duration = time.time() - file_start

                # Mark as processed
                db.mark_processed(latest_dir, filename)

                # Cleanup
                downloader.cleanup()

                files_processed += 1
                logger.info(f"✅ Completed {filename} in {duration:.1f}s")

            except Exception as e:
                logger.error(f"❌ Error processing {filename}: {e}")
                if config.debug:
                    logger.exception("Full traceback:")
                continue

        # Summary
        total_duration = time.time() - total_start
        logger.info("\n" + "=" * 60)
        logger.info("CNPJ Data Loading Complete!")
        logger.info(f"Files processed: {files_processed}")
        logger.info(f"Total rows: {total_rows:,}")
        logger.info(f"Total time: {total_duration:.1f}s")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        if "config" in locals() and config.debug:
            logger.exception("Full traceback:")
        sys.exit(1)
    finally:
        if "db" in locals():
            db.disconnect()


if __name__ == "__main__":
    main()
