"""Tests for the logging configuration module."""

import logging
from unittest.mock import MagicMock, patch

import pytest

from geoparquet_io.core.logging_config import (
    COLORS,
    CLIFormatter,
    DynamicStreamHandler,
    LibraryFormatter,
    configure_verbose,
    debug,
    error,
    get_logger,
    info,
    progress,
    setup_cli_logging,
    success,
    verbose_logging,
    warn,
)


class TestCLIFormatter:
    """Tests for the CLIFormatter class."""

    def test_message_without_timestamps(self):
        """Messages should not include timestamps by default."""
        formatter = CLIFormatter(show_timestamps=False, use_colors=False)
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert formatted == "Test message"
        assert ":" not in formatted  # No timestamp

    def test_message_with_timestamps(self):
        """Messages should include timestamps when show_timestamps=True."""
        formatter = CLIFormatter(show_timestamps=True, use_colors=False)
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "Test message" in formatted
        # Should have timestamp format YYYY-MM-DD HH:MM:SS
        assert "-" in formatted  # Date separator
        assert ":" in formatted  # Time separator

    def test_success_marker_produces_green(self):
        """Messages with [SUCCESS] marker should be green."""
        formatter = CLIFormatter(show_timestamps=False, use_colors=True)
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="[SUCCESS]Operation completed",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert COLORS["green"] in formatted
        assert COLORS["reset"] in formatted
        assert "[SUCCESS]" not in formatted  # Marker should be stripped

    def test_info_marker_produces_cyan(self):
        """Messages with [INFO] marker should be cyan."""
        formatter = CLIFormatter(show_timestamps=False, use_colors=True)
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="[INFO]Informational message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert COLORS["cyan"] in formatted
        assert COLORS["reset"] in formatted
        assert "[INFO]" not in formatted  # Marker should be stripped

    def test_warning_level_produces_yellow(self):
        """WARNING level messages should be yellow."""
        formatter = CLIFormatter(show_timestamps=False, use_colors=True)
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="Warning message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert COLORS["yellow"] in formatted
        assert COLORS["reset"] in formatted

    def test_error_level_produces_red(self):
        """ERROR level messages should be red."""
        formatter = CLIFormatter(show_timestamps=False, use_colors=True)
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="Error message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert COLORS["red"] in formatted
        assert COLORS["reset"] in formatted

    def test_no_colors_strips_markers(self):
        """When use_colors=False, markers should be stripped."""
        formatter = CLIFormatter(show_timestamps=False, use_colors=False)
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="[SUCCESS]Operation completed",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "[SUCCESS]" not in formatted
        assert COLORS["green"] not in formatted
        assert formatted == "Operation completed"


class TestSetupCliLogging:
    """Tests for the setup_cli_logging function."""

    def test_setup_creates_handler(self):
        """setup_cli_logging should configure the logger with a handler."""
        setup_cli_logging(verbose=False, show_timestamps=False)
        logger = get_logger()
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0].formatter, CLIFormatter)

    def test_verbose_sets_debug_level(self):
        """verbose=True should set logger to DEBUG level."""
        setup_cli_logging(verbose=True)
        logger = get_logger()
        assert logger.level == logging.DEBUG

    def test_non_verbose_sets_info_level(self):
        """verbose=False should set logger to INFO level."""
        setup_cli_logging(verbose=False)
        logger = get_logger()
        assert logger.level == logging.INFO

    def test_clears_existing_handlers(self):
        """setup_cli_logging should clear existing handlers."""
        logger = get_logger()
        # Add a dummy handler
        logger.addHandler(logging.StreamHandler())
        setup_cli_logging()
        assert len(logger.handlers) == 1  # Only the new handler


class TestHelperFunctions:
    """Tests for the logging helper functions."""

    @pytest.fixture(autouse=True)
    def setup_logging(self):
        """Set up logging for each test with propagation enabled for caplog."""
        logger = get_logger()
        original_propagate = logger.propagate
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()
        logger.propagate = True  # Enable propagation for caplog to work
        yield
        # Clean up
        logger.handlers.clear()
        logger.propagate = original_propagate

    def test_success_logs_info_with_marker(self, caplog):
        """success() should log at INFO level with SUCCESS marker."""
        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            success("Operation completed")
        assert "Operation completed" in caplog.text
        assert "[SUCCESS]" in caplog.text

    def test_warn_logs_warning(self, caplog):
        """warn() should log at WARNING level."""
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            warn("Something to note")
        assert "Something to note" in caplog.text

    def test_error_logs_error(self, caplog):
        """error() should log at ERROR level."""
        with caplog.at_level(logging.ERROR, logger="geoparquet_io"):
            error("Something went wrong")
        assert "Something went wrong" in caplog.text

    def test_info_logs_info_with_marker(self, caplog):
        """info() should log at INFO level with INFO marker."""
        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            info("Informational message")
        assert "Informational message" in caplog.text
        assert "[INFO]" in caplog.text

    def test_debug_logs_at_debug_level(self, caplog):
        """debug() should log at DEBUG level."""
        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            debug("Debug details")
        assert "Debug details" in caplog.text
        # Verify it's at DEBUG level
        assert any(r.levelno == logging.DEBUG for r in caplog.records)

    def test_progress_logs_plain_info(self, caplog):
        """progress() should log at INFO level without markers."""
        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            progress("Processing...")
        assert "Processing..." in caplog.text
        # Should not have markers
        assert "[SUCCESS]" not in caplog.text
        assert "[INFO]" not in caplog.text


class TestConfigureVerbose:
    """Tests for the configure_verbose function."""

    def test_verbose_true_sets_debug(self):
        """configure_verbose(True) should set logger to DEBUG."""
        setup_cli_logging(verbose=False)
        configure_verbose(True)
        logger = get_logger()
        assert logger.level == logging.DEBUG

    def test_verbose_false_no_change(self):
        """configure_verbose(False) should not change level."""
        setup_cli_logging(verbose=False)
        original_level = get_logger().level
        configure_verbose(False)
        assert get_logger().level == original_level


class TestVerboseLoggingContext:
    """Tests for the verbose_logging context manager."""

    def test_verbose_logging_temporarily_enables_debug(self):
        """verbose_logging should temporarily enable DEBUG level."""
        setup_cli_logging(verbose=False)
        logger = get_logger()
        assert logger.level == logging.INFO

        with verbose_logging():
            assert logger.level == logging.DEBUG

        assert logger.level == logging.INFO  # Restored


class TestGetLogger:
    """Tests for the get_logger function."""

    def test_get_logger_without_name_returns_package_logger(self):
        """get_logger() should return the package logger."""
        logger = get_logger()
        assert logger.name == "geoparquet_io"

    def test_get_logger_with_name_returns_child_logger(self):
        """get_logger(name) should return a child logger."""
        logger = get_logger("geoparquet_io.core.convert")
        assert logger.name == "geoparquet_io.core.convert"


class TestLibraryFormatter:
    """Tests for LibraryFormatter class (lines 115-119)."""

    def test_init(self):
        """Test LibraryFormatter initialization."""
        formatter = LibraryFormatter()
        assert formatter is not None
        # Verify formatter works by formatting a test record
        record = logging.LogRecord(
            name="geoparquet_io.test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Init test",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        # Verify expected elements are in formatted output
        assert "geoparquet_io.test" in result
        assert "INFO" in result
        assert "Init test" in result

    def test_format(self):
        """Test LibraryFormatter formats correctly."""
        formatter = LibraryFormatter()
        record = logging.LogRecord(
            name="geoparquet_io.test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        assert "geoparquet_io.test" in result
        assert "INFO" in result
        assert "Test message" in result


class TestDynamicStreamHandler:
    """Tests for DynamicStreamHandler class."""

    def test_emit_success(self, capsys):
        """Test emit writes to stderr when stdout is piped (not a TTY)."""
        handler = DynamicStreamHandler()
        handler.setFormatter(CLIFormatter(use_colors=False))
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test emit",
            args=(),
            exc_info=None,
        )
        handler.emit(record)

        # Verify output was written to stderr (when stdout is not a TTY, logs go to stderr
        # to avoid corrupting binary data streams like Arrow IPC)
        captured = capsys.readouterr()
        assert captured.err, "Expected output to stderr but got nothing"
        assert "Test emit" in captured.err

    def test_emit_exception_handling(self):
        """Test emit handles exceptions (lines 142-143)."""
        handler = DynamicStreamHandler()
        handler.setFormatter(CLIFormatter(use_colors=False))
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test emit",
            args=(),
            exc_info=None,
        )

        # Mock handleError to verify it's called on exception
        handler.handleError = MagicMock()

        # Patch the parent class emit to raise an exception
        with patch("logging.StreamHandler.emit", side_effect=Exception("Write failed")):
            handler.emit(record)

        # handleError should have been called
        handler.handleError.assert_called_once()


class TestCLIFormatterPlainMessage:
    """Additional tests for CLIFormatter plain message path (line 103)."""

    def test_plain_info_message_without_markers(self):
        """Test formatting a plain INFO message with no markers (hits line 103)."""
        formatter = CLIFormatter(show_timestamps=False, use_colors=True)
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Plain message with no markers",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        # Plain INFO message without markers should return as-is (no color codes)
        assert result == "Plain message with no markers"
        # Should NOT have any ANSI color codes since no markers matched
        assert "\033[" not in result
