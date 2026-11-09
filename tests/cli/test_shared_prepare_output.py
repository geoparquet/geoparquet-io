"""Unit tests for :func:`geoparquet_io.cli._shared.prepare_output`.

``prepare_output`` is the extracted form of a prologue that ran verbatim at the
top of nine write commands. These tests pin the two checks it performs and the
exception type each one surfaces. Its third step, the row-group parse, moved to
``pack_write_options`` (#761), which runs before the command body.
"""

from __future__ import annotations

import sys
from unittest import mock

import click
import pytest

from geoparquet_io.cli._shared import prepare_output
from geoparquet_io.core.exceptions import InvalidParameterError


class TestExtensionCheck:
    """``--any-extension`` is honoured, and the error type is unchanged."""

    def test_rejects_non_parquet_extension(self):
        with pytest.raises(InvalidParameterError, match="does not have .parquet extension"):
            prepare_output("out.csv", False)

    def test_any_extension_allows_anything(self):
        assert prepare_output("out.csv", True) is None

    def test_stream_marker_is_not_extension_checked(self):
        with mock.patch.object(sys.stdout, "isatty", return_value=False):
            assert prepare_output("-", False) is None


class TestMissingOutputGuard:
    """A missing output raises ``ClickException``, with the traceback suppressed."""

    def test_missing_output_on_a_terminal_is_a_click_exception(self):
        with mock.patch.object(sys.stdout, "isatty", return_value=True):
            with pytest.raises(click.ClickException) as excinfo:
                prepare_output(None, False)
        assert "Missing output" in str(excinfo.value)
        assert excinfo.value.__cause__ is None

    def test_missing_output_when_piped_is_allowed(self):
        with mock.patch.object(sys.stdout, "isatty", return_value=False):
            assert prepare_output(None, False) is None
