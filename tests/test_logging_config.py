"""Tests for the logging configuration module."""

import logging
from unittest.mock import MagicMock, patch

import pytest

from geoparquet_io.core.logging_config import (
    COLORS,
    CLIFormatter,
    DynamicStreamHandler,
    configure_verbose,
    debug,
    error,
    get_logger,
    info,
    progress,
    setup_cli_logging,
    success,
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

    def test_setup_creates_handler(self, capsys):
        """setup_cli_logging should configure the logger with a handler."""
        setup_cli_logging(verbose=False, show_timestamps=False)
        logger = get_logger()
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0].formatter, CLIFormatter)

        handler = DynamicStreamHandler()
        handler.setFormatter(CLIFormatter(use_colors=False))
        record = logging.LogRecord(
            "httpx", logging.WARNING, "", 0, "GET %s", ("https://u:pa'ss@h/x",), None
        )
        handler.handle(record)
        # Pytest's capture handler intentionally re-raises formatting errors.
        root = logging.getLogger()
        with patch.object(
            root, "handlers", [h for h in root.handlers if isinstance(h, DynamicStreamHandler)]
        ):
            logging.getLogger("third_party").warning("a %s %s", 1)
        assert record.msg == "GET %s"
        assert record.args == ("https://u:pa'ss@h/x",)
        try:
            raise ValueError("https://u:SECRET/other@h/x?token=HIDDEN")
        except ValueError:
            import sys

            record = logging.LogRecord(
                "httpcore", logging.ERROR, "", 0, "failed", (), sys.exc_info()
            )
            handler.handle(record)
        output = capsys.readouterr().err
        assert "pa'ss" not in output
        assert "SECRET" not in output
        assert "HIDDEN" not in output
        assert "ValueError" in output
        assert "--- Logging error ---" in output

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
        original_level = logger.level
        original_handlers = logger.handlers.copy()
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()
        logger.propagate = True  # Enable propagation for caplog to work
        yield
        # Clean up - restore original state
        logger.handlers.clear()
        logger.handlers.extend(original_handlers)
        logger.propagate = original_propagate
        logger.setLevel(original_level)

    def test_styled_helpers_preserve_level_message_and_marker(self, caplog):
        cases = (
            (success, logging.INFO, "Operation completed", "[SUCCESS]"),
            (warn, logging.WARNING, "Something to note", ""),
            (error, logging.ERROR, "Something went wrong", ""),
            (info, logging.INFO, "Informational message", "[INFO]"),
        )
        for emit, level, message, marker in cases:
            caplog.clear()
            with caplog.at_level(level, logger="geoparquet_io"):
                emit(message)
            assert message in caplog.text
            assert marker in caplog.text
            assert [record.levelno for record in caplog.records] == [level]

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


def test_every_log_level_sanitizes_url_and_query_credentials(caplog):
    messages = [
        "https://example.test/x?api_key=SORTSECRET",
        "https://example.test/x?token=SORTSECRET",
        "https://user:SORTSECRET@example.test/x",
        "https://SORTSECRET@example.test/x",
        "https://[invalid/x?token=SORTSECRET",
        "https://SORTSECRET:SORTSECRET@example.test/x",
        "https://example.test/x?X-Amz-Credential=SORTSECRET&X-Amz-Signature=SORTSECRET",
        "SELECT * FROM 'https://example.test/x?token=SORTSECRET' WHERE id=1",
    ]
    with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
        for emit in (debug, info, progress, success, warn, error):
            for message in messages:
                emit(message)
    assert len(caplog.records) == len(messages) * 6
    formatter = CLIFormatter(use_colors=False)
    assert all("SORTSECRET" not in formatter.format(record) for record in caplog.records)
    assert all("SORTSECRET" in record.getMessage() for record in caplog.records)

    for path in (
        ".../key=7/part-?.parquet",
        "/tmp/token=7/part-?.parquet",
        "s3://bucket/out/key=7/part-?.parquet",
        "https://host/out/api_key=7/part-?.parquet",
        "Sorting by key=value",
        "api_key=value token=value",
    ):
        caplog.clear()
        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            progress(path)
        assert formatter.format(caplog.records[-1]) == path
