"""Unit tests for :func:`geoparquet_io.cli._shared.prepare_output`.

``prepare_output`` is the extracted form of a prologue that ran verbatim at the
top of nine write commands. These tests pin the three steps it performs, the
order it performs them in, and the exception type each one surfaces -- that
ordering is what makes the "missing output" hint reach a user who also typed a
non-``.parquet`` filename.
"""

from __future__ import annotations

import sys
from unittest import mock

import click
import pytest

from geoparquet_io.cli._shared import prepare_output
from geoparquet_io.core.exceptions import InvalidParameterError


class TestRowGroupResult:
    """The return value is exactly what ``parse_row_group_options`` produces."""

    def test_returns_none_when_neither_row_group_option_given(self):
        assert prepare_output("out.parquet", False, None, None) is None

    def test_returns_none_for_row_based_sizing(self):
        # Row-based sizing is passed through by the caller, not converted to MB.
        assert prepare_output("out.parquet", False, 50_000, None) is None

    def test_returns_megabytes_for_size_string(self):
        assert prepare_output("out.parquet", False, None, "256MB") == pytest.approx(256.0)

    def test_rejects_both_row_group_options(self):
        with pytest.raises(click.UsageError, match="mutually exclusive"):
            prepare_output("out.parquet", False, 50_000, "256MB")


class TestExtensionCheck:
    """``--any-extension`` is honoured, and the error type is unchanged."""

    def test_rejects_non_parquet_extension(self):
        with pytest.raises(InvalidParameterError, match="does not have .parquet extension"):
            prepare_output("out.csv", False, None, None)

    def test_any_extension_allows_anything(self):
        assert prepare_output("out.csv", True, None, None) is None

    def test_stream_marker_is_not_extension_checked(self):
        with mock.patch.object(sys.stdout, "isatty", return_value=False):
            assert prepare_output("-", False, None, None) is None


class TestMissingOutputGuard:
    """A missing output raises ``ClickException``, with the traceback suppressed."""

    def test_missing_output_on_a_terminal_is_a_click_exception(self):
        with mock.patch.object(sys.stdout, "isatty", return_value=True):
            with pytest.raises(click.ClickException) as excinfo:
                prepare_output(None, False, None, None)
        assert "Missing output" in str(excinfo.value)
        assert excinfo.value.__cause__ is None

    def test_missing_output_when_piped_is_allowed(self):
        with mock.patch.object(sys.stdout, "isatty", return_value=False):
            assert prepare_output(None, False, None, None) is None

    def test_output_guard_runs_before_the_extension_check(self):
        # Both would fail; the streaming hint is the more useful message, so it
        # must win. Reordering the helper's steps would surface the extension
        # error instead.
        with mock.patch.object(sys.stdout, "isatty", return_value=True):
            with pytest.raises(click.ClickException) as excinfo:
                prepare_output(None, False, 50_000, "256MB")
        assert "Missing output" in str(excinfo.value)
