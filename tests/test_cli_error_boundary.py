"""A DuckDB-rejected input gets an error line, not a traceback (#983).

The root group converts a DuckDB failure that no gpio frame owned into gpio's
error line. Which failures those are, and why the set is narrower than
``duckdb.Error``, is reasoned once on
:data:`geoparquet_io.core.duckdb_utils.INPUT_FILE_DUCKDB_ERRORS`; this file
pins the behaviour in both directions -- input-caused failures become an error
line, gpio-caused ones keep their traceback -- and pins the ``--verbose``
promise that makes hiding a traceback recoverable.
"""

from __future__ import annotations

import json
import logging
import pathlib
import struct

import click
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.decorators import ErrorBoundaryGroup, verbose_option
from geoparquet_io.cli.exception_handler import cli_error_for
from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import INPUT_FILE_DUCKDB_ERRORS

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


# ``configure_verbose`` is one-way and the logger is a module global, so a
# ``--verbose`` test hands DEBUG to whatever runs next. This file's own local
# fixture only covered the tests below, and the tests it could not save are the
# two that build their own ``ErrorBoundaryGroup`` -- ``cli`` resets the level on
# every invocation through ``setup_cli_logging``, a throwaway group does not.
# ``tests/conftest.py`` now puts every ``geoparquet_io`` logger back to a
# known-clean state around *every* test in the suite, which is what it takes:
# the poison arrived from ``test_add.py``'s module-scoped ``--verbose`` fixtures
# on the same xdist worker, before any function-scoped fixture could look.
# See ``tests/test_logging_state_isolation.py``.


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
            duckdb.HTTPException("HTTP Error: 403 Forbidden"),
        ],
    )
    def test_every_input_caused_failure_is_covered_not_just_the_reproductions(self, exc):
        """The trigger is any input DuckDB refuses, not one particular block.

        ``HTTPException`` is covered because it hangs below ``IOException``: a
        remote URL that will not fetch is a property of the path the user named,
        and ``isinstance`` picks it up without a separate entry.
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

    def test_the_tuple_cannot_be_collapsed_to_a_shared_base_class(self):
        """Why the entries are enumerated instead of named by an ancestor.

        A tripwire on duckdb's own class tree rather than on gpio's code: if
        duckdb ever reorganises so that an ancestor *does* split along this
        line, the enumeration can be replaced -- and until then, any base class
        wide enough to include ``InvalidInputException`` is wide enough to
        include the three classes that are its opposite.
        """
        assert issubclass(duckdb.InvalidInputException, duckdb.ProgrammingError)
        for gpio_authored in (duckdb.ParserException, duckdb.BinderException):
            assert issubclass(gpio_authored, duckdb.ProgrammingError)
            assert not issubclass(gpio_authored, INPUT_FILE_DUCKDB_ERRORS)

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


class TestAnUnpublishedExtensionIsNotABadInputFile:
    """A 404 from the extension registry hangs below ``IOException`` (#778).

    Without asking first, ``INSTALL geography FROM community`` failing because
    the registry has no build for this platform would be answered with the line
    gpio uses for "your input file is unreachable" -- and lose the guidance
    ``_UNPUBLISHED_EXTENSION_HINTS`` exists to give. Three ``INSTALL`` sites are
    unwrapped (``core/duckdb_utils._install_and_load_extension``,
    ``core/extract_bigquery``, ``core/partition/admin_hierarchical``), so their
    failures arrive here raw.
    """

    NOT_PUBLISHED = (
        'HTTP Error: Failed to download extension "geography" at URL '
        '"https://community-extensions.duckdb.org/v1.5.5/osx_arm64/geography.duckdb_extension.gz" '
        "(HTTP 404)"
    )

    def test_the_404_is_named_as_the_registry_not_the_input(self):
        message = cli_error_for(duckdb.HTTPException(self.NOT_PUBLISHED)).format_message()
        assert "not your input file" in message
        assert "not published for this one" in message

    def test_the_extension_specific_hint_survives(self):
        message = cli_error_for(duckdb.HTTPException(self.NOT_PUBLISHED)).format_message()
        assert "gpio add a5" in message

    def test_a_remote_input_file_that_404s_is_still_a_bad_input_file(self):
        """The guard that keeps the check from over-claiming.

        ``is_unpublished_extension_error`` matches on "http 404" anywhere in the
        message, and a remote *input* that is simply not there 404s too. Naming
        an extension is what separates the two, so a missing file keeps the
        plain error line and gets no advice about the extension registry.
        """
        missing = duckdb.HTTPException(
            "HTTP Error: HTTP GET error on 'https://host/missing.parquet' (HTTP 404)"
        )
        error = cli_error_for(missing)

        assert error is not None
        assert "community-extensions" not in error.format_message()
        assert error.format_message().endswith("(HTTP 404)")

    def test_an_offline_download_failure_is_not_blamed_on_the_registry(self):
        """#778's own distinction: only the 404 means "no build exists"."""
        offline = duckdb.IOException(
            'IO Error: Failed to download extension "geography" at URL '
            '"https://community-extensions.duckdb.org/..." '
            "(ERROR Could not establish connection)"
        )
        message = cli_error_for(offline).format_message()
        assert "not published" not in message


class TestTheMessageIsSanitized:
    """DuckDB's message carries text gpio did not author (#983 review).

    Pre-existing at every other site that prints one -- ``cli_error_for`` is
    where it can be fixed once, because it is the single funnel.
    """

    def test_a_presigned_signature_does_not_reach_the_error_line(self):
        exc = duckdb.HTTPException(
            "HTTP GET error on 'https://bucket.s3.amazonaws.com/data/x.parquet"
            "?X-Amz-Credential=AKIAEXAMPLE&X-Amz-Signature=deadbeefsecret' (HTTP 403)"
        )
        message = cli_error_for(exc).format_message()

        assert "deadbeefsecret" not in message
        assert "X-Amz-Credential" not in message
        assert "x.parquet" in message, "the filename is what makes the message useful"

    def test_a_url_without_a_query_string_is_left_readable(self):
        exc = duckdb.IOException("IO Error: No files found for 'https://host/a.parquet'")
        assert "https://host/a.parquet" in cli_error_for(exc).format_message()

    def test_an_input_cannot_author_the_error_line_with_ansi_escapes(self):
        """A value the *file* chose is quoted back verbatim by DuckDB. With the
        escapes intact it could rewind the line and print an ``Error:`` of its
        own choosing."""
        exc = duckdb.InvalidInputException(
            "Invalid Input Error: bad value '\x1b[2K\rError: everything is fine\x1b[0m'"
        )
        message = cli_error_for(exc).format_message()

        assert "\x1b" not in message
        assert "\r" not in message
        assert "[2K" not in message, "the escape has to go whole, not just its introducer"

    def test_a_tab_or_newline_is_not_stripped(self):
        exc = duckdb.InvalidInputException("Invalid Input Error: a\n\tb")
        assert cli_error_for(exc).format_message() == "Invalid Input Error: a\n\tb"


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


class TestVerboseRestoresTheHiddenTraceback:
    """Hiding a traceback is only acceptable if it is recoverable.

    It was not, on four of the eleven commands, until ``--verbose`` stopped
    depending on the failing path having reached a core function first. See
    :func:`geoparquet_io.cli.decorators.enable_verbose_logging`.
    """

    #: The four that were measured *not* to restore it before the flag's own
    #: callback raised the level (their paths fail before any
    #: ``configure_verbose`` call), plus one that did, so a regression in either
    #: direction shows up.
    COMMANDS = [
        ("add bbox", ["add", "bbox", "{in}", "{out}"]),
        ("sort hilbert", ["sort", "hilbert", "{in}", "{out}"]),
        ("extract geoparquet", ["extract", "geoparquet", "{in}", "{out}"]),
        ("add geometry-metrics", ["add", "geometry-metrics", "{in}", "{out}"]),
        ("check optimization", ["check", "optimization", "{in}"]),
    ]

    @pytest.mark.parametrize(("name", "argv"), COMMANDS, ids=[n for n, _ in COMMANDS])
    def test_verbose_prints_the_traceback_the_error_line_replaced(
        self, name, argv, rejected_file, tmp_path
    ):
        args = [
            a.format(**{"in": rejected_file, "out": str(tmp_path / f"{name}-v.parquet")})
            for a in argv
        ]

        quiet = CliRunner().invoke(cli, args)
        verbose = CliRunner().invoke(cli, [*args, "--verbose"])

        assert quiet.exit_code == 1 and "Traceback" not in quiet.output
        assert verbose.exit_code == 1, verbose.output
        assert "Traceback (most recent call last)" in verbose.output, verbose.output
        assert "InvalidInputException" in verbose.output

    def test_the_flag_raises_the_level_before_the_command_body_runs(self):
        """The mechanism, without a failure in the way: by the time any gpio
        code runs, DEBUG is already on. Nothing inside the command is asked to
        arrange it, which is exactly what the four commands above could not do.
        """
        logging.getLogger("geoparquet_io").setLevel(logging.INFO)
        seen = {}

        @click.group(cls=ErrorBoundaryGroup)
        def root():
            pass

        @root.command()
        @verbose_option
        def noisy(verbose):
            seen["level"] = logging.getLogger("geoparquet_io").level

        CliRunner().invoke(root, ["noisy", "--verbose"])

        assert seen["level"] == logging.DEBUG

    def test_verbose_is_a_per_command_flag(self, rejected_file):
        """Where it goes, pinned so the docs and the PR cannot drift from it:
        the root group has no ``--verbose``, so before the subcommand it is a
        usage error, not a quiet no-op."""
        result = CliRunner().invoke(cli, ["--verbose", "check", "optimization", rejected_file])

        assert result.exit_code == 2
        assert "No such option" in result.output and "--verbose" in result.output


class TestInnerHandlersStillRunFirst:
    """The boundary is outermost, so it never preempts a recovery (#988).

    ``convert`` identifies DuckDB's curved-geometry refusal by matching
    "Unsupported geometry type in WKB" on the exception, linearizes the source
    and writes again. That refusal is an ``InvalidInputException``, which is
    exactly what ``INPUT_FILE_DUCKDB_ERRORS`` claims -- so if the boundary ran
    anywhere but last, it would convert the error into a ``ClickException`` and
    the retry would never happen.
    """

    #: Measured, all of it, against ``tests/data/curved_geometry_test.gdb``:
    #: ``ST_AsWKB(geom)`` over ``ST_Read`` on that fixture raises
    #: ``duckdb.InvalidInputException`` with exactly this message. The substring
    #: ``_is_linearizable_curve_error`` matches on is the tail of it; the
    #: "Invalid Input Error:" prefix is DuckDB's, not this test's.
    CURVE_REFUSAL = "Invalid Input Error: Unsupported geometry type in WKB"

    def test_the_curve_refusal_is_a_class_the_boundary_would_otherwise_claim(self):
        """Without this being true, the test below would prove nothing."""
        exc = duckdb.InvalidInputException(self.CURVE_REFUSAL)
        assert isinstance(exc, INPUT_FILE_DUCKDB_ERRORS)
        assert cli_error_for(exc) is not None

    def test_the_real_curved_filegdb_still_converts_through_the_boundary(self, tmp_path):
        """End to end on #988's own fixture, through the real ``cli``.

        ``--skip-hilbert`` removes the bounds pass, so the curve refusal happens
        on the write and only the retry can rescue it. Exit 0 here means the
        boundary let the recovery run; a regression would show up as exit 1 with
        DuckDB's message on a file that used to convert.
        """
        gdb = pathlib.Path(__file__).parent / "data" / "curved_geometry_test.gdb"
        if not gdb.exists():  # pragma: no cover - fixture ships with the repo
            pytest.skip("curved FileGDB fixture not present")

        out = tmp_path / "curved.parquet"
        result = CliRunner().invoke(cli, ["convert", str(gdb), str(out), "--skip-hilbert"])

        assert result.exit_code == 0, result.output
        assert out.exists()

    def test_a_refusal_the_inner_handler_declines_still_becomes_an_error_line(self):
        """The other half of the contract: what the recovery re-raises (a
        Parquet input, or ``--no-linearize-curves``) is still input-caused, so
        it gets the error line rather than a traceback."""

        @click.group(cls=ErrorBoundaryGroup)
        def root():
            pass

        @root.command()
        def convert():
            try:
                raise duckdb.InvalidInputException(TestInnerHandlersStillRunFirst.CURVE_REFUSAL)
            except duckdb.Error:
                raise  # --no-linearize-curves: the fallback is off

        result = CliRunner().invoke(root, ["convert"])

        assert result.exit_code == 1
        assert "Unsupported geometry type in WKB" in result.output
        assert "Traceback" not in result.output


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
    """Both halves in one pass: what the user sees, and what it was underneath.

    The class matters because the fix only keeps #983 fixed if the exception
    these four really raise is in ``INPUT_FILE_DUCKDB_ERRORS`` -- so a future
    narrowing that excluded one of them fails loudly here rather than quietly
    restoring a traceback. ``standalone_mode=False`` is what leaves the
    ``ClickException`` itself in reach; under the default Click has already
    turned it into the ``SystemExit`` that carries exit code 1.
    """
    args = [
        a.format(**{"in": rejected_file, "out": str(tmp_path / f"{name}.parquet")}) for a in argv
    ]

    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 1, result.output
    assert not isinstance(result.exception, duckdb.Error), result.exception
    assert "Error: " in result.output
    assert "Traceback" not in result.output

    if (name, argv) in REPORTED:
        cause = CliRunner().invoke(cli, args, standalone_mode=False).exception.__cause__
        assert isinstance(cause, duckdb.InvalidInputException), f"{name}: {cause!r}"
        assert isinstance(cause, INPUT_FILE_DUCKDB_ERRORS)
