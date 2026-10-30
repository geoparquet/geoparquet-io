"""
Logging configuration for geoparquet-io.

This module provides centralized logging that works for both CLI and library usage:
- CLI mode: Clean output with colors, no timestamps by default
- Library mode: Standard Python logging, users configure their own handlers

Usage in core modules:
    from geoparquet_io.core.logging_config import success, warn, error, info, debug, progress

    success("Operation completed")  # Green
    warn("Something to note")       # Yellow
    error("Something went wrong")   # Red
    info("Informational message")   # Cyan
    debug("Debug details")          # Only shown when verbose
    progress("Processing...")       # Plain text

Usage in CLI:
    from geoparquet_io.core.logging_config import setup_cli_logging
    setup_cli_logging(verbose=True, show_timestamps=False)
"""

import logging
import sys
from contextlib import contextmanager

from geoparquet_io.core.exceptions import redact_credentials

# Package-level logger
logger = logging.getLogger("geoparquet_io")

# ANSI color codes for terminal output
COLORS = {
    "green": "\033[32m",
    "yellow": "\033[33m",
    "red": "\033[31m",
    "cyan": "\033[36m",
    "reset": "\033[0m",
    "bold": "\033[1m",
}

# Color markers embedded in log messages
COLOR_MARKERS = {
    "[SUCCESS]": "green",
    "[INFO]": "cyan",
}


class CLIFormatter(logging.Formatter):
    """
    Formatter for CLI output that mimics click.echo() behavior.

    Features:
    - No log level prefix by default (just the message)
    - Optional timestamp support via show_timestamps parameter
    - Color support for WARNING/ERROR levels and embedded markers
    - Strips colors for non-TTY output
    """

    def __init__(self, show_timestamps: bool = False, use_colors: bool | None = None):
        """
        Initialize CLI formatter.

        Args:
            show_timestamps: If True, prepend timestamps to messages
            use_colors: If True, apply ANSI colors. If None, auto-detect TTY.
        """
        self.show_timestamps = show_timestamps
        self.use_colors = use_colors if use_colors is not None else sys.stdout.isatty()

        if show_timestamps:
            fmt = "%(asctime)s %(message)s"
            datefmt = "%Y-%m-%d %H:%M:%S"
        else:
            fmt = "%(message)s"
            datefmt = None

        super().__init__(fmt=fmt, datefmt=datefmt)

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with optional colors."""
        message = super().format(record)

        if self.use_colors:
            message = self._apply_colors(message, record)
        else:
            message = self._strip_color_markers(message)

        return message

    def _apply_colors(self, message: str, record: logging.LogRecord) -> str:
        """Apply ANSI colors based on log level and markers."""
        # Level-based coloring for WARNING and ERROR
        if record.levelno >= logging.ERROR:
            return f"{COLORS['red']}{message}{COLORS['reset']}"
        elif record.levelno >= logging.WARNING:
            return f"{COLORS['yellow']}{message}{COLORS['reset']}"

        # Check for embedded color markers
        for marker, color in COLOR_MARKERS.items():
            if marker in message:
                clean_message = message.replace(marker, "")
                return f"{COLORS[color]}{clean_message}{COLORS['reset']}"

        return message

    def _strip_color_markers(self, message: str) -> str:
        """Remove color markers for non-TTY output."""
        for marker in COLOR_MARKERS:
            message = message.replace(marker, "")
        return message


class LibraryFormatter(logging.Formatter):
    """Standard formatter for library usage with timestamps and levels."""

    def __init__(self):
        super().__init__(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


class DynamicStreamHandler(logging.StreamHandler):
    """
    A StreamHandler that works correctly with Click's CliRunner and piping.

    Click's CliRunner substitutes sys.stdout with a wrapper. To ensure
    output is captured by CliRunner, we dynamically select the stream.

    When stdout is piped (for binary data like Arrow IPC), logs go to stderr.
    When stdout is a terminal, logs go to stdout (standard CLI behavior).
    """

    def __init__(self):
        # Initialize with None stream - we'll handle output dynamically
        super().__init__(stream=None)

    def emit(self, record):
        try:
            # When stdout is piped (not a TTY), write to stderr to avoid
            # corrupting binary data streams (like Arrow IPC).
            # When stdout is a terminal, write to stdout for normal CLI output.
            if sys.stdout.isatty():
                self.stream = sys.stdout
            else:
                self.stream = sys.stderr
            super().emit(record)
        except Exception:
            self.handleError(record)


def setup_cli_logging(
    verbose: bool = False, show_timestamps: bool = False, use_colors: bool | None = None
) -> None:
    """
    Configure logging for CLI usage.

    This sets up a handler with CLIFormatter that produces clean output
    similar to click.echo().

    Args:
        verbose: If True, set level to DEBUG; otherwise INFO
        show_timestamps: If True, include timestamps in output
        use_colors: If True, use ANSI colors. If None, auto-detect TTY.
    """
    handler = DynamicStreamHandler()
    handler.setFormatter(CLIFormatter(show_timestamps=show_timestamps, use_colors=use_colors))

    level = logging.DEBUG if verbose else logging.INFO

    logger.setLevel(level)
    logger.handlers.clear()
    logger.addHandler(handler)
    # Keep propagate True for pytest log capture compatibility
    # Our handler will still output to stdout
    logger.propagate = True


def get_logger(name: str | None = None) -> logging.Logger:
    """
    Get a logger for a specific module.

    Args:
        name: Module name (e.g., "geoparquet_io.core.convert")
              If None, returns the root package logger

    Returns:
        logging.Logger instance
    """
    if name is None:
        return logger
    return logging.getLogger(name)


@contextmanager
def verbose_logging():
    """
    Context manager to temporarily enable verbose (DEBUG) logging.

    Usage:
        with verbose_logging():
            debug("This will be shown")
    """
    original_level = logger.level
    logger.setLevel(logging.DEBUG)
    try:
        yield
    finally:
        logger.setLevel(original_level)


def _bootstrap_default_handler(verbose: bool) -> None:
    """Attach a default handler for non-CLI usage without hijacking the host's.

    ``setup_cli_logging()`` installs a stream handler *and* sets a level, which
    is right at a CLI entry point but wrong when the first thing that touches
    logging is a library call inside somebody else's application.

    Two cases:

    * The host has already configured logging (the root logger has handlers).
      Library convention then applies: attach a :class:`logging.NullHandler`
      only, so this bootstrap does not run again, and let records propagate to
      the host's handlers exactly once at whatever level the host chose.
      Installing a second stream handler here made every gpio message appear
      twice in an embedding application.
    * Nothing has configured logging at all — a bare script importing gpio.
      Install the CLI handler so output is not silently dropped, but put back a
      level the caller explicitly chose for the ``geoparquet_io`` logger.
    """
    if logging.getLogger().handlers:
        logger.addHandler(logging.NullHandler())
        return

    chosen_level = logger.level  # NOTSET unless something explicitly set it
    setup_cli_logging(verbose=verbose, show_timestamps=False, use_colors=False)
    if chosen_level != logging.NOTSET:
        logger.setLevel(chosen_level)


def configure_verbose(verbose: bool) -> None:
    """
    Raise the package logger to DEBUG for a verbose call.

    Call this at the start of a function that has a verbose parameter.
    Also ensures a handler is attached if none exists (for non-CLI usage).

    Deliberately one-way: this is called from ~60 nested core functions, nearly
    all of which default to ``verbose=False``. Lowering the level on a
    non-verbose call would (a) truncate an outer ``--verbose`` run the moment it
    reached a nested default call, and (b) stamp gpio's idea of a level onto an
    embedding application's logger on every single default call. Use
    :func:`verbose_logging` when a raise has to be undone again.

    Args:
        verbose: If True, set logger to DEBUG level
    """
    # Ensure at least a basic handler exists for non-CLI usage (e.g., tests, library usage)
    if not logger.handlers:
        _bootstrap_default_handler(verbose)
    if verbose:
        logger.setLevel(logging.DEBUG)


# ============================================================================
# Helper functions for styled output
# These map to the current click.echo patterns used throughout the codebase
# ============================================================================


def success(message: str) -> None:
    """
    Log a success message (displayed in green in CLI).

    Use for: Completed operations, passed validations, successful writes.
    """
    logger.info(f"[SUCCESS]{message}")


def warn(message: str) -> None:
    """
    Log a warning message (displayed in yellow in CLI).

    Use for: Non-critical issues, recommendations, deprecation notices.

    Credential values (``api_key=...``, ``user:password@host``) are redacted here,
    once, so a caller can interpolate a caught exception without cleaning it first.
    """
    logger.warning(redact_credentials(message))


def error(message: str) -> None:
    """
    Log an error message (displayed in red in CLI).

    Use for: Failed operations, validation failures, critical issues.
    """
    logger.error(redact_credentials(message))


def info(message: str) -> None:
    """
    Log an info message (displayed in cyan in CLI).

    Use for: Informational details, tips, context about operations.
    """
    logger.info(f"[INFO]{message}")


def debug(message: str) -> None:
    """
    Log a debug message (only shown when verbose=True).

    Use for: Detailed diagnostic output, internal state, SQL queries.
    """
    logger.debug(message)


def progress(message: str) -> None:
    """
    Log a progress message (no special formatting).

    Use for: Progress updates, status messages, general output.
    """
    logger.info(message)
