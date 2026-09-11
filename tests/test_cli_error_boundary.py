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

The fix is the boundary: the root group converts a DuckDB failure that nobody
underneath owned into a ``ClickException``. One funnel, so a command added
tomorrow inherits it.

The catch is deliberately *not* ``duckdb.Error``. gpio authors every SQL string
it runs, so a query that will not parse or bind is a bug in something we
generated, and answering it with the same ``Error:`` line used for a bad input
would tell a user their data is broken when the broken thing is ours. Both
directions are pinned below: the input-caused failures become an error line, and
the gpio-caused ones keep their traceback.
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
from geoparquet_io.cli.exception_handler import INPUT_FILE_DUCKDB_ERRORS, cli_error_for
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
            duckdb.HTTPException("HTTP Error: 404"),
            duckdb.ConversionException("Conversion Error: could not convert"),
        ],
    )
    def test_every_input_caused_failure_is_covered_not_just_the_reproductions(self, exc):
        """The trigger is any input DuckDB refuses, not one particular block.

        ``HTTPException`` is in the list because it hangs below ``IOException``:
        a remote URL that will not fetch is a property of the path the user
        named, and ``isinstance`` picks it up without a separate entry.
        """
        assert isinstance(cli_error_for(exc), click.ClickException)

    @pytest.mark.parametrize(
        "exc",
        [ValueError("bad value"), TypeError("bad type"), RuntimeError("boom")],
    )
    def test_a_non_duckdb_error_is_not_claimed(self, exc):
        """``None`` means "not mine" -- the caller re-raises with its traceback."""
        assert cli_error_for(exc) is None

    @pytest.mark.parametrize(
        ("sql", "expected"),
        [
            ("SELECT 1 +", duckdb.ParserException),
            ("SELECT nonexistent_col FROM range(1)", duckdb.BinderException),
            ("SELECT st_nosuchfunc(1)", duckdb.CatalogException),
            ("SELECT o'brien FROM range(1)", duckdb.ParserException),
        ],
    )
    def test_a_query_gpio_got_wrong_keeps_its_traceback(self, sql, expected):
        """The negative direction, and the reason the tuple is not ``duckdb.Error``.

        gpio authors every SQL string it runs, so a query that will not parse or
        bind is never news about the user's file -- it is a bug in something we
        generated, which is the defect class behind #700, #718 and #944. Raised
        by really executing the bad SQL rather than by constructing the
        exception, so the class is DuckDB's answer and not this test's guess.
        """
        with pytest.raises(expected) as raised:
            duckdb.connect().execute(sql)

        assert cli_error_for(raised.value) is None

    def test_the_unquoted_identifier_defect_is_not_reported_as_a_bad_file(self):
        """The concrete misattribution the narrowing prevents.

        A name with an apostrophe that reached the SQL unquoted is #718's exact
        shape. Answering it with ``Error:`` -- the line gpio uses to say "your
        file is bad" -- would send the user looking at their data for a bug that
        is ours.
        """
        with pytest.raises(duckdb.Error) as raised:
            duckdb.connect().execute("SELECT * FROM range(1) WHERE 'o'brien' = 1")

        assert cli_error_for(raised.value) is None

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

    def test_a_command_whose_sql_will_not_parse_still_propagates(self):
        """End to end, through the real boundary: a gpio-authored query that
        DuckDB rejects must not come out looking like a bad input file."""

        @click.group(cls=ErrorBoundaryGroup)
        def root():
            pass

        @root.command()
        def boom():
            duckdb.connect().execute("SELECT 1 +")

        result = CliRunner().invoke(root, ["boom"])

        assert isinstance(result.exception, duckdb.ParserException)
        assert not isinstance(result.exception, click.ClickException)

    def test_the_traceback_is_still_available_at_debug_level(self, caplog):
        """Nothing is lost: the hidden traceback is logged, not dropped."""
        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            CliRunner().invoke(
                _group_raising(duckdb.InvalidInputException("Invalid Input Error: oops")), ["boom"]
            )

        assert any(record.exc_info for record in caplog.records)


class TestTheLineTheTupleDraws:
    """Guards on ``INPUT_FILE_DUCKDB_ERRORS`` itself.

    The first version of this fix caught ``duckdb.Error``, which is every DuckDB
    failure there is -- its only direct subclass is ``DatabaseError`` and
    everything hangs below that. These pin the narrowing so it cannot be widened
    back without a failing test.
    """

    def test_it_is_not_the_base_class_in_disguise(self):
        assert duckdb.Error not in INPUT_FILE_DUCKDB_ERRORS
        assert duckdb.DatabaseError not in INPUT_FILE_DUCKDB_ERRORS

    @pytest.mark.parametrize(
        "gpio_authored",
        [duckdb.ParserException, duckdb.BinderException, duckdb.CatalogException],
    )
    def test_gpio_authored_sql_failures_are_excluded(self, gpio_authored):
        assert not issubclass(gpio_authored, INPUT_FILE_DUCKDB_ERRORS)

    def test_the_tuple_cannot_be_collapsed_to_a_shared_base_class(self):
        """Why the entries are enumerated instead of named by an ancestor.

        Measured against duckdb 1.5.5: ``InvalidInputException`` shares
        ``ProgrammingError`` with the three classes above, so any base class
        wide enough to include the first is wide enough to include the others.
        """
        assert issubclass(duckdb.InvalidInputException, duckdb.ProgrammingError)
        for gpio_authored in (duckdb.ParserException, duckdb.BinderException):
            assert issubclass(gpio_authored, duckdb.ProgrammingError)


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


@pytest.mark.parametrize(("name", "argv"), REPORTED, ids=[n for n, _ in REPORTED])
def test_each_reported_command_fails_with_an_error_the_narrowed_tuple_admits(
    name, argv, rejected_file, tmp_path
):
    """The narrowing keeps #983 fixed *because* of what DuckDB actually raises.

    The four reproductions are only covered if their underlying class is in
    ``INPUT_FILE_DUCKDB_ERRORS``. That is checked here against the exception the
    boundary wrapped, so a future narrowing that excluded one of them would fail
    loudly rather than quietly restoring a traceback.
    """
    args = [
        a.format(**{"in": rejected_file, "out": str(tmp_path / f"{name}-cause.parquet")})
        for a in argv
    ]

    # standalone_mode=False so the ClickException itself surfaces; under the
    # default, Click has already turned it into the SystemExit that carries
    # exit code 1, and the cause chain is one frame further down.
    result = CliRunner().invoke(cli, args, standalone_mode=False)
    cause = result.exception.__cause__

    assert isinstance(cause, duckdb.InvalidInputException), f"{name}: {cause!r}"
    assert isinstance(cause, INPUT_FILE_DUCKDB_ERRORS)
