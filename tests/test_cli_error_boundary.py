"""A DuckDB-rejected input gets an error line, not a traceback (#983).

The ``geo`` key on an input file is arbitrary JSON written by somebody else's
tool, and DuckDB's Parquet reader refuses several shapes of it outright --
``{"version": "1.1.0"}`` with no ``columns``, a non-string ``version``, a column
entry with no ``encoding``. That refusal is a property of the *input*, not a
gpio bug, and DuckDB's message ("Geoparquet metadata does not have a columns
object") says exactly what is wrong with the file.

``gpio inspect head`` and ``gpio convert geoparquet`` already answered such a
file with ``Error: <duckdb's message>`` and exit 1. Eleven other commands
answered it with a raw Python traceback, because the handling lived in per-site
``except`` blocks rather than at the boundary every command passes through.

The fix is the boundary: the root group converts a ``duckdb.Error`` that nobody
underneath owned into a ``ClickException``. One funnel, so a command added
tomorrow inherits it.
"""

from __future__ import annotations

import json
import logging
import struct

import click
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.decorators import ErrorBoundaryGroup
from geoparquet_io.cli.exception_handler import cli_error_for
from geoparquet_io.cli.main import cli

# One WKB point (1.0, 2.0), little-endian.
POINT_WKB = struct.pack("<BI2d", 1, 1, 1.0, 2.0)

#: The reproduction from #983: valid JSON, a string version, nothing gpio's own
#: guards object to, and nothing DuckDB will read.
DUCKDB_REJECTED_GEO = {"version": "1.1.0"}


@pytest.fixture
def rejected_file(tmp_path):
    """A real one-row Parquet file whose ``geo`` block DuckDB refuses."""
    table = pa.table({"id": pa.array([1]), "geometry": pa.array([POINT_WKB], pa.binary())})
    table = table.replace_schema_metadata({b"geo": json.dumps(DUCKDB_REJECTED_GEO).encode()})
    path = tmp_path / "rejected.parquet"
    pq.write_table(table, path)
    return str(path)


def _group_raising(exc: BaseException):
    """A throwaway root group whose one subcommand raises ``exc``."""

    @click.group(cls=ErrorBoundaryGroup)
    def root():
        pass

    @root.command()
    def boom():
        raise exc

    return root


# =============================================================================
# The translation itself
# =============================================================================


class TestCliErrorFor:
    def test_a_duckdb_error_becomes_a_click_exception(self):
        error = cli_error_for(duckdb.InvalidInputException("Invalid Input Error: no columns"))
        assert isinstance(error, click.ClickException)

    def test_duckdbs_own_message_is_kept_verbatim(self):
        message = "Invalid Input Error: Geoparquet metadata does not have a columns object"
        assert cli_error_for(duckdb.InvalidInputException(message)).format_message() == message

    @pytest.mark.parametrize(
        "exc",
        [
            duckdb.InvalidInputException("Invalid Input Error: no columns"),
            duckdb.IOException("IO Error: No files found that match the pattern"),
            duckdb.BinderException("Binder Error: Referenced column not found"),
            duckdb.ConversionException("Conversion Error: could not convert"),
            duckdb.Error("some error"),
        ],
    )
    def test_every_duckdb_error_is_covered_not_just_the_reproductions(self, exc):
        """The trigger is any input DuckDB refuses, not one particular block."""
        assert isinstance(cli_error_for(exc), click.ClickException)

    @pytest.mark.parametrize(
        "exc",
        [ValueError("bad value"), TypeError("bad type"), RuntimeError("boom")],
    )
    def test_a_non_duckdb_error_is_not_claimed(self, exc):
        """``None`` means "not mine" -- the caller re-raises with its traceback."""
        assert cli_error_for(exc) is None

    def test_the_spill_hint_still_wins_over_the_generic_translation(self):
        """An out-of-spill-space failure is a ``duckdb.Error`` too, and it keeps
        the ``TMPDIR`` line the generic translation would have dropped."""
        exc = duckdb.OutOfMemoryException(
            "Out of Memory Error: failed to offload data block of size 256.0 KiB.\n"
            "This limit was set by the 'max_temp_directory_size' setting."
        )
        message = cli_error_for(exc).format_message()
        assert "TMPDIR" in message
        assert "failed to offload data block" in message


# =============================================================================
# The boundary the translation is installed on
# =============================================================================


class TestTheRootGroupIsTheBoundary:
    def test_the_real_cli_group_uses_it(self):
        assert isinstance(cli, ErrorBoundaryGroup)

    def test_a_duckdb_error_reaches_the_user_as_an_error_line(self):
        result = CliRunner().invoke(
            _group_raising(
                duckdb.InvalidInputException(
                    "Invalid Input Error: Geoparquet metadata does not have a columns object"
                )
            ),
            ["boom"],
        )

        assert result.exit_code == 1
        assert not isinstance(result.exception, duckdb.Error)
        assert "Error: Invalid Input Error: Geoparquet metadata" in result.output
        assert "Traceback" not in result.output

    def test_the_original_exception_stays_in_the_cause_chain(self):
        original = duckdb.IOException("IO Error: nope")
        result = CliRunner().invoke(_group_raising(original), ["boom"], standalone_mode=False)
        assert result.exception.__cause__ is original

    def test_a_non_duckdb_error_still_propagates_untouched(self):
        result = CliRunner().invoke(_group_raising(RuntimeError("a real bug")), ["boom"])
        assert isinstance(result.exception, RuntimeError)

    def test_the_traceback_is_still_available_at_debug_level(self, caplog):
        """Nothing is lost: a gpio bug that surfaces as a DuckDB error is still
        debuggable, it just is not shouted at a user who cannot act on it."""
        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            CliRunner().invoke(
                _group_raising(duckdb.BinderException("Binder Error: oops")), ["boom"]
            )

        assert any(record.exc_info for record in caplog.records)


# =============================================================================
# The four commands the issue reproduces on, end to end
# =============================================================================

#: #983 reported these four. ``inspect head`` and ``convert geoparquet`` are the
#: two that already behaved, and are here as the parity the fix matches.
REPORTED = [
    ("check optimization", ["check", "optimization", "{in}"]),
    ("add bbox", ["add", "bbox", "{in}", "{out}"]),
    ("sort hilbert", ["sort", "hilbert", "{in}", "{out}"]),
    ("add quadkey", ["add", "quadkey", "{in}", "{out}"]),
]

ALREADY_CLEAN = [
    ("inspect head", ["inspect", "head", "{in}"]),
    ("convert geoparquet", ["convert", "geoparquet", "{in}", "{out}"]),
]


@pytest.mark.parametrize(
    ("name", "argv"), REPORTED + ALREADY_CLEAN, ids=[n for n, _ in REPORTED + ALREADY_CLEAN]
)
def test_a_rejected_file_gets_an_error_line_and_exit_one(name, argv, rejected_file, tmp_path):
    args = [
        a.format(**{"in": rejected_file, "out": str(tmp_path / f"{name}.parquet")}) for a in argv
    ]

    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 1, result.output
    assert not isinstance(result.exception, duckdb.Error), result.exception
    assert "Error: " in result.output
    assert "Traceback" not in result.output
