"""
orchestration/run_logger.py
-----------------------------
Structured logger that writes simultaneously to the console and a
timestamped log file.

Features
--------
- Every message is prefixed with an ISO-style timestamp.
- Log file is created automatically under a configurable directory.
- Convenience methods for common severity levels and structural markers.
- ``DualWriter`` helper redirects ``print()`` output to the log file.

Usage
-----
    logger = RunLogger("outputs/logs")
    logger.info("Starting reconciliation run...")
    logger.success("All datasets processed.")
    logger.close()
"""

import os
import sys
from datetime import datetime
from pathlib import Path


class RunLogger:
    """
    Writes structured log messages to both ``stdout`` and a timestamped file.

    Parameters
    ----------
    log_dir    : str   Directory for log files (created if absent).
    log_prefix : str   Prefix used in the log filename (default: ``run``).
    """

    def __init__(self, log_dir: str = "outputs/logs", log_prefix: str = "run"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = self.log_dir / f"{log_prefix}_{timestamp}.log"

        self._init_file()
        print(f"📝 Log file: {self.log_file}")

    # -------------------------------------------------------------------------
    # Initialisation
    # -------------------------------------------------------------------------

    def _init_file(self) -> None:
        header = (
            f"\n{'=' * 90}\n"
            f"DATASYNC AUDIT — CROSS-SYSTEM DATA RECONCILIATION FRAMEWORK\n"
            f"{'=' * 90}\n"
            f"Log File : {self.log_file.name}\n"
            f"Started  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"{'=' * 90}\n\n"
        )
        self.log_file.write_text(header, encoding="utf-8")

    # -------------------------------------------------------------------------
    # Core write
    # -------------------------------------------------------------------------

    def _timestamp(self) -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _append(self, line: str) -> None:
        with open(self.log_file, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def log(self, message: str, *, add_timestamp: bool = True) -> None:
        """Write ``message`` to console and log file."""
        formatted = f"[{self._timestamp()}] {message}" if add_timestamp else message
        print(formatted)
        self._append(formatted)

    # -------------------------------------------------------------------------
    # Severity helpers
    # -------------------------------------------------------------------------

    def info(self, message: str) -> None:
        """Log an informational message."""
        self.log(f"ℹ️  {message}")

    def success(self, message: str) -> None:
        """Log a success message."""
        self.log(f"✅ {message}")

    def warning(self, message: str) -> None:
        """Log a warning message."""
        self.log(f"⚠️  {message}")

    def error(self, message: str) -> None:
        """Log an error message."""
        self.log(f"❌ {message}")

    def debug(self, message: str) -> None:
        """Log a debug message."""
        self.log(f"🔍 {message}")

    # -------------------------------------------------------------------------
    # Structural markers
    # -------------------------------------------------------------------------

    def section(self, title: str) -> None:
        """Log a section divider with a title."""
        sep = "-" * 90
        self.log(sep, add_timestamp=False)
        self.log(f"📌 {title}")
        self.log(sep, add_timestamp=False)

    def banner(self, title: str) -> None:
        """Log a prominent banner."""
        sep = "=" * 90
        self.log(sep, add_timestamp=False)
        self.log(f"🚀 {title}")
        self.log(sep, add_timestamp=False)

    def step(self, step_num: int, total: int, description: str) -> None:
        """Log a numbered pipeline step."""
        self.log(f"📍 STEP {step_num}/{total}: {description}")

    def dataset_start(self, dataset_name: str) -> None:
        """Log the beginning of dataset processing."""
        self.log("")
        self.section(f"Processing Dataset: {dataset_name}")

    def dataset_end(self, dataset_name: str, *, success: bool = True) -> None:
        """Log the completion of dataset processing."""
        if success:
            self.success(f"Dataset complete: {dataset_name}")
        else:
            self.error(f"Dataset failed: {dataset_name}")
        self.log("")

    def summary(self, title: str, items: dict) -> None:
        """Log a key-value summary block."""
        self.log("")
        self.log(f"📊 {title}", add_timestamp=False)
        self.log("=" * 60, add_timestamp=False)
        for key, value in items.items():
            self.log(f"  {key:30s}: {value}", add_timestamp=False)
        self.log("=" * 60, add_timestamp=False)
        self.log("")

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def close(self) -> None:
        """Write closing footer to the log file."""
        footer = (
            f"\n{'=' * 90}\n"
            f"RUN COMPLETE\n"
            f"Finished : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Log path : {self.log_file}\n"
            f"{'=' * 90}\n"
        )
        self._append(footer)
        print(f"\n📝 Full log saved to: {self.log_file}")


class DualWriter:
    """
    Redirects ``print()`` statements to both the terminal and a ``RunLogger``.

    Usage
    -----
        sys.stdout = DualWriter(logger)
        # ... code that uses print() ...
        sys.stdout = sys.__stdout__
    """

    def __init__(self, logger: RunLogger):
        self._logger = logger
        self._terminal = sys.stdout

    def write(self, message: str) -> None:
        self._terminal.write(message)
        if message.strip():
            self._logger._append(
                f"[{self._logger._timestamp()}] {message.rstrip()}"
            )

    def flush(self) -> None:
        self._terminal.flush()
