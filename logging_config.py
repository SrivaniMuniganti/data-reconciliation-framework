"""
logging_config.py
-----------------
Logging configuration for DataSync Audit.

This module configures Python's standard ``logging`` library with sensible
defaults: timestamped console output and a rotating file handler.

It is optional — the ``RunLogger`` class in ``orchestration/run_logger.py``
provides a richer, framework-specific logger used by the main pipeline.
This module is provided for integrations that prefer the standard
``logging`` interface.

Usage
-----
    from logging_config import configure_logging
    import logging

    configure_logging(log_dir="outputs/logs", level=logging.DEBUG)
    logger = logging.getLogger("datasync_audit")
    logger.info("Reconciliation started")
"""

import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path


def configure_logging(
    log_dir: str = "outputs/logs",
    level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,   # 10 MB per file
    backup_count: int = 5,
) -> logging.Logger:
    """
    Configure and return the root ``datasync_audit`` logger.

    A console handler and a rotating file handler are attached.
    Calling this function a second time is safe — handlers are not duplicated.

    Parameters
    ----------
    log_dir      : str   Directory for log files.
    level        : int   Minimum log level (default: ``logging.INFO``).
    max_bytes    : int   Max bytes per log file before rotation.
    backup_count : int   Number of rotated log files to retain.

    Returns
    -------
    logging.Logger
        Configured ``datasync_audit`` logger.
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"datasync_audit_{timestamp}.log")

    fmt = logging.Formatter(
        fmt="[%(asctime)s] %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger("datasync_audit")

    # Avoid duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    # Rotating file handler
    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    logger.info(f"Logging initialised — file: {log_file}")
    return logger
