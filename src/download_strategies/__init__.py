"""
Download strategies for CNPJ data pipeline.

Implements the Strategy pattern for different download approaches:
- Sequential: Downloads files one at a time (default, safe)
- Parallel: Downloads multiple files concurrently (faster, more complex)
"""

import logging
from typing import Dict, Type

from .base import DownloadError, DownloadStrategy
from .parallel import ParallelDownloadStrategy
from .sequential import SequentialDownloadStrategy

logger = logging.getLogger(__name__)

# Registry of available strategies
STRATEGIES: Dict[str, Type[DownloadStrategy]] = {
    "sequential": SequentialDownloadStrategy,
    "parallel": ParallelDownloadStrategy,
}


def create_download_strategy(config) -> DownloadStrategy:
    """
    Create a download strategy based on configuration.

    Args:
        config: Configuration object with download_strategy attribute

    Returns:
        DownloadStrategy instance

    Raises:
        ValueError: If strategy name is not recognized
    """
    strategy_name = getattr(config, "download_strategy", "sequential").lower()

    if strategy_name not in STRATEGIES:
        logger.warning(
            f"Unknown download strategy '{strategy_name}', falling back to sequential"
        )
        strategy_name = "sequential"

    strategy_class = STRATEGIES[strategy_name]
    logger.debug(f"Creating {strategy_name} download strategy")

    return strategy_class(config)


def get_available_strategies() -> list[str]:
    """Get list of available download strategy names."""
    return list(STRATEGIES.keys())


__all__ = [
    "DownloadStrategy",
    "DownloadError",
    "SequentialDownloadStrategy",
    "ParallelDownloadStrategy",
    "create_download_strategy",
    "get_available_strategies",
    "STRATEGIES",
]
