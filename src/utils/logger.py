"""
Structured Logger
==================
WHY: print() is NOT logging. In production pipelines, you need:
  1. Timestamps on every message (for debugging data lag issues)
  2. Severity levels (DEBUG/INFO/WARNING/ERROR) to filter noise
  3. Consistent format across all modules (producer, consumer, etc.)
  4. Easy redirection to files or log aggregation systems (e.g., ELK)

This module sets up a reusable logger factory so every module in the
project gets a properly configured logger with one line:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
"""

import logging
import sys
from pathlib import Path


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Create and return a configured logger instance.

    Args:
        name: Logger name (typically __name__ of the calling module).
        level: Logging level (default: INFO).

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if logger already exists
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # ── Console Handler ──
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    # ── Format: timestamp | level | module | message ──
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Prevent log propagation to root logger (avoids duplicate messages)
    logger.propagate = False

    return logger
