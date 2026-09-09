"""Tests asserting --write-memory is forwarded, validated, and never silently dropped.

Companion to the sort-hilbert fix in PR #627: the `output_format_options`
decorator attaches `--write-memory` to many commands, but several of them
captured `write_memory` in their signature and never forwarded it to the core
function / DuckDB write engine.

Layers tested here:
  * Introspection: EVERY command that exposes --write-memory forwards it. This
    is the test that cannot go stale — a new command (or a new decorator use)
    that declares the flag and drops it fails immediately.
  * CLI -> core function: the CLI passes ``write_memory`` through as the
    keyword argument ``memory_limit`` (not positionally, not dropped).
  * Core function (file-based path) -> DuckDB write engine: a full, unmocked
    invocation shows "DuckDB memory limit: <value>" in --verbose output.
  * Core function (streaming path) -> execute_transform/write_output.
  * Validation: bad values are rejected as Click parameter errors, and a value
    crafted to break out of ``SET memory_limit = '…'`` is refused.
  * 1.1-geoarrow: the auto-routed write strategy warns and ignores
    --write-memory instead of aborting with a raw traceback.
"""

from __future__ import annotations

import inspect
import os
from unittest import mock

import click
import duckdb
import pytest
from click.testing import CliRunner

# NOTE: import the *submodule object*, never a dotted mock.patch target.
# `geoparquet_io/__init__.py` does `from geoparquet_io.cli.main import cli`,
# which rebinds the attribute `geoparquet_io.cli` from the submodule to the
# Click Group. Python 3.10's mock._get_target getattr-walks that path and lands
# on `Group.main` (a bound method), so `mock.patch("geoparquet_io.cli.main.X")`
# raises AttributeError on 3.10 while passing on 3.11+ (which uses
# pkgutil.resolve_name). `import geoparquet_io.cli.main as cli_main` does NOT
# help: IMPORT_FROM getattrs the parent and returns the Group on every version.
# `from geoparquet_io.cli import main as cli_main` is the form that works
# everywhere. Do not "simplify" this back to a dotted patch target. The same
# holds for the group modules below: import the module object, patch on it.
from geoparquet_io.cli.commands import add as cli_add
from geoparquet_io.cli.commands import sort as cli_sort
from geoparquet_io.cli.main import cli
from tests.conftest import skip_if_geography_unavailable


def _find_non_geometry_column(parquet_file: str) -> str:
    conn = duckdb.connect()
    try:
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{parquet_file}"').fetchall()
    finally:
        conn.close()
    for col in columns:
        if col[0] != "geometry":
            return col[0]
    raise AssertionError("No non-geometry columns found in test fixture")


def _iter_commands(command, path=None):
    """Yield ``(dotted name, Command)`` for every leaf command in the CLI tree."""
    path = path or []
    if isinstance(command, click.Group):
        for name, sub in command.commands.items():
            yield from _iter_commands(sub, [*path, name])
    else:
        yield " ".join(path), command


def _commands_with_write_memory():
    return [
        (name, cmd)
        for name, cmd in _iter_commands(cli)
        if any(param.name == "write_memory" for param in cmd.params)
    ]


# ---------------------------------------------------------------------------
# Layer 0: no command may declare --write-memory without forwarding it
# ---------------------------------------------------------------------------


class TestEveryCommandForwardsWriteMemory:
    """The offender list must stay empty.

    A hand-written per-command list goes stale the moment someone adds a new
    command; walking the Click tree cannot.
    """

    def test_no_command_declares_write_memory_without_forwarding_it(self):
        commands = _commands_with_write_memory()
        assert commands, "Expected at least one command to expose --write-memory"

        offenders = []
        for name, cmd in commands:
            source = inspect.getsource(inspect.unwrap(cmd.callback))
            if "memory_limit=write_memory" not in source:
                offenders.append(name)

        assert not offenders, (
            "These commands accept --write-memory but never forward it as "
            f"memory_limit=write_memory: {offenders}"
        )


# ---------------------------------------------------------------------------
# Layer 1: CLI forwards --write-memory as memory_limit=<value> BY KEYWORD
# ---------------------------------------------------------------------------

# The module each case patches is the one that owns the command: `cli.main` for
# groups still defined there, `cli.commands.<group>` for groups already
# extracted into their own module (Deep Review 3.2).
CLI_FORWARDING_CASES = [
    pytest.param(
        cli_add,
        "add_h3_column_impl",
        lambda in_f, out_f, col: ["add", "h3", in_f, out_f],
        id="add-h3",
    ),
    pytest.param(
        cli_add,
        "add_a5_column_impl",
        lambda in_f, out_f, col: ["add", "a5", in_f, out_f],
        id="add-a5",
    ),
    pytest.param(
        cli_add,
        "add_s2_column_impl",
        lambda in_f, out_f, col: ["add", "s2", in_f, out_f],
        id="add-s2",
    ),
    pytest.param(
        cli_add,
        "add_kdtree_column_impl",
        lambda in_f, out_f, col: ["add", "kdtree", in_f, out_f],
        id="add-kdtree",
    ),
    pytest.param(
        cli_add,
        "add_quadkey_column_impl",
        lambda in_f, out_f, col: ["add", "quadkey", in_f, out_f],
        id="add-quadkey",
    ),
    pytest.param(
        cli_add,
        "add_bbox_column_impl",
        lambda in_f, out_f, col: ["add", "bbox", in_f, out_f],
        id="add-bbox",
    ),
    pytest.param(
        cli_sort,
        "sort_by_column_impl",
        lambda in_f, out_f, col: ["sort", "column", in_f, out_f, col],
        id="sort-column",
    ),
    pytest.param(
        cli_sort,
        "sort_by_quadkey_impl",
        lambda in_f, out_f, col: ["sort", "quadkey", in_f, out_f],
        id="sort-quadkey",
    ),
]


class TestCliForwardsWriteMemoryByKeyword:
    """The CLI must call the core function with memory_limit=<value> as a kwarg."""

    @pytest.mark.parametrize("impl_module,impl_name,build_args", CLI_FORWARDING_CASES)
    def test_write_memory_forwarded_as_keyword(self, impl_module, impl_name, build_args):
        runner = CliRunner()
        args = build_args("input.parquet", "output.parquet", "some_column")
        args = [*args, "--write-memory", "222MB"]

        with mock.patch.object(impl_module, impl_name) as mocked_impl:
            result = runner.invoke(cli, args)

        assert result.exit_code == 0, result.output
        mocked_impl.assert_called_once()
        _call_args, call_kwargs = mocked_impl.call_args
        assert "memory_limit" in call_kwargs, (
            f"{impl_name} was not called with memory_limit as a keyword argument "
            f"(kwargs were: {sorted(call_kwargs)})"
        )
        assert call_kwargs["memory_limit"] == "222MB"


# ---------------------------------------------------------------------------
# Layer 2: real (unmocked) file-based invocation reaches the DuckDB write
# engine, mirroring tests/test_sort.py::test_hilbert_sort_write_memory_reaches_engine
# ---------------------------------------------------------------------------


class TestFileBasedWriteMemoryReachesEngine:
    def test_add_h3_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "h3",
                places_test_file,
                temp_output_file,
                "--write-memory",
                "513MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 513MB" in result.output

    def test_add_a5_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "a5",
                places_test_file,
                temp_output_file,
                "--write-memory",
                "514MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 514MB" in result.output

    def test_add_s2_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        skip_if_geography_unavailable()
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "s2",
                places_test_file,
                temp_output_file,
                "--write-memory",
                "515MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 515MB" in result.output

    def test_add_kdtree_write_memory_reaches_engine(self, buildings_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "kdtree",
                buildings_test_file,
                temp_output_file,
                "--write-memory",
                "516MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 516MB" in result.output

    def test_add_quadkey_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "quadkey",
                places_test_file,
                temp_output_file,
                "--write-memory",
                "517MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 517MB" in result.output

    def test_sort_column_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        test_column = _find_non_geometry_column(places_test_file)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "sort",
                "column",
                places_test_file,
                temp_output_file,
                test_column,
                "--write-memory",
                "518MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 518MB" in result.output

    def test_sort_quadkey_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "sort",
                "quadkey",
                places_test_file,
                temp_output_file,
                "--write-memory",
                "519MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 519MB" in result.output

    def test_add_bbox_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "bbox",
                places_test_file,
                temp_output_file,
                # The fixture already has a bbox column without covering
                # metadata; without --force the command exits early and never
                # reaches the write engine.
                "--force",
                "--write-memory",
                "520MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 520MB" in result.output

    def test_add_geometry_metrics_write_memory_reaches_engine(
        self, buildings_test_file, temp_output_file
    ):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "geometry-metrics",
                buildings_test_file,
                temp_output_file,
                "--write-memory",
                "521MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 521MB" in result.output

    def test_convert_geoparquet_write_memory_reaches_engine(self, test_data_dir, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                "geoparquet",
                str(test_data_dir / "buildings_test.geojson"),
                temp_output_file,
                "--write-memory",
                "522MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 522MB" in result.output


# ---------------------------------------------------------------------------
# Layer 3: --write-memory is validated before it reaches SQL
# ---------------------------------------------------------------------------

INJECTION_PAYLOAD = "1GB'; COPY (SELECT 42 AS x) TO '{path}'; SET memory_limit='2GB"


class TestWriteMemoryValidation:
    @pytest.mark.parametrize(
        "bad_value",
        ["abc", "0", "", "512 megabytes", "-1GB", "1GB; DROP TABLE t"],
        ids=["letters", "bare-zero", "empty", "words", "negative", "trailing-sql"],
    )
    def test_invalid_write_memory_is_a_click_parameter_error(
        self, places_test_file, temp_output_file, bad_value
    ):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["add", "h3", places_test_file, temp_output_file, "--write-memory", bad_value],
        )
        assert result.exit_code == 2, result.output
        assert "--write-memory" in result.output
        assert not os.path.exists(temp_output_file)
        assert result.exception is None or isinstance(result.exception, SystemExit)

    @pytest.mark.parametrize(
        "good_value",
        ["512MB", "2GB", "4.5GB", "1024kb", "1GiB", "128 MB"],
    )
    def test_valid_write_memory_is_accepted(self, good_value):
        runner = CliRunner()
        with mock.patch.object(cli_add, "add_h3_column_impl"):
            result = runner.invoke(
                cli,
                ["add", "h3", "in.parquet", "out.parquet", "--write-memory", good_value],
            )
        assert result.exit_code == 0, result.output

    def test_sql_injection_payload_is_rejected(
        self, places_test_file, temp_output_file, temp_output_dir
    ):
        """A value crafted to close the SET string literal must never execute."""
        pwned = os.path.join(temp_output_dir, "pwned.csv")
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "h3",
                places_test_file,
                temp_output_file,
                "--write-memory",
                INJECTION_PAYLOAD.format(path=pwned),
            ],
        )
        assert result.exit_code != 0, result.output
        assert not os.path.exists(pwned), "Injected COPY statement executed!"
        assert not os.path.exists(temp_output_file)

    def test_core_validator_rejects_injection_payload(self):
        """Library callers (not just the CLI) are protected at the SET site."""
        from geoparquet_io.core.write_strategies.duckdb_kv import validate_memory_limit

        with pytest.raises(ValueError, match="Invalid memory_limit"):
            validate_memory_limit(INJECTION_PAYLOAD.format(path="/tmp/pwned.csv"))

    def test_core_validator_normalizes(self):
        from geoparquet_io.core.write_strategies.duckdb_kv import validate_memory_limit

        assert validate_memory_limit("512MB") == "512MB"
        assert validate_memory_limit("2gb") == "2GB"
        assert validate_memory_limit("4.5 GB") == "4.5GB"


# ---------------------------------------------------------------------------
# Layer 4: --write-memory + --geoparquet-version 1.1-geoarrow must not crash
# ---------------------------------------------------------------------------


class TestGeoarrowVersionIgnoresWriteMemory:
    """1.1-geoarrow auto-routes to the arrow-streaming strategy, which cannot
    honour a memory limit. Warn and continue — never abort with a traceback."""

    def test_add_h3_geoarrow_with_write_memory_succeeds(self, places_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "h3",
                places_test_file,
                temp_output_file,
                "--geoparquet-version",
                "1.1-geoarrow",
                "--write-memory",
                "512MB",
            ],
        )
        assert result.exit_code == 0, result.output
        assert os.path.exists(temp_output_file)
        assert "--write-memory" in result.output

    def test_sort_hilbert_geoarrow_with_write_memory_succeeds(
        self, places_test_file, temp_output_file
    ):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "sort",
                "hilbert",
                places_test_file,
                temp_output_file,
                "--geoparquet-version",
                "1.1-geoarrow",
                "--write-memory",
                "512MB",
            ],
        )
        assert result.exit_code == 0, result.output
        assert os.path.exists(temp_output_file)
        assert "--write-memory" in result.output

    def test_explicit_non_duckdb_kv_strategy_still_errors_cleanly(
        self, places_test_file, temp_output_file
    ):
        """A user-chosen strategy that cannot honour the limit is a real error,
        but it must surface as a clean message, not a bare ValueError."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "geoparquet",
                places_test_file,
                temp_output_file,
                "--write-strategy",
                "streaming",
                "--write-memory",
                "512MB",
            ],
        )
        assert result.exit_code != 0
        assert "--write-memory" in result.output
        assert not isinstance(result.exception, ValueError)


# ---------------------------------------------------------------------------
# Layer 5: streaming helpers forward memory_limit into execute_transform /
# write_output. Arguments are passed BY KEYWORD so a signature reorder fails
# loudly instead of silently rebinding every argument.
# ---------------------------------------------------------------------------


class TestPrivateHelpersForwardMemoryLimit:
    def test_add_h3_streaming_forwards_memory_limit(self):
        from geoparquet_io.core.add.h3 import _add_h3_streaming

        with mock.patch("geoparquet_io.core.add.h3.execute_transform") as mocked:
            _add_h3_streaming(
                input_path="in.parquet",
                output_path="out.parquet",
                h3_column_name="h3_cell",
                resolution=9,
                verbose=False,
                compression="ZSTD",
                compression_level=None,
                row_group_size_mb=None,
                row_group_rows=None,
                profile=None,
                geoparquet_version=None,
                memory_limit="601MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "601MB"

    def test_add_a5_streaming_forwards_memory_limit(self):
        from geoparquet_io.core.add.a5 import _add_a5_streaming

        with mock.patch("geoparquet_io.core.add.a5.execute_transform") as mocked:
            _add_a5_streaming(
                input_path="in.parquet",
                output_path="out.parquet",
                a5_column_name="a5_cell",
                resolution=15,
                verbose=False,
                compression="ZSTD",
                compression_level=None,
                row_group_size_mb=None,
                row_group_rows=None,
                profile=None,
                geoparquet_version=None,
                memory_limit="602MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "602MB"

    def test_add_s2_streaming_forwards_memory_limit(self):
        from geoparquet_io.core.add.s2 import _add_s2_streaming

        with mock.patch("geoparquet_io.core.add.s2.execute_transform") as mocked:
            _add_s2_streaming(
                input_path="in.parquet",
                output_path="out.parquet",
                s2_column_name="s2_cell",
                level=13,
                verbose=False,
                compression="ZSTD",
                compression_level=None,
                row_group_size_mb=None,
                row_group_rows=None,
                profile=None,
                geoparquet_version=None,
                memory_limit="603MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "603MB"

    def test_sort_by_column_streaming_forwards_memory_limit(self):
        from geoparquet_io.core.sort_by_column import _sort_by_column_streaming

        with mock.patch("geoparquet_io.core.sort_by_column.execute_transform") as mocked:
            _sort_by_column_streaming(
                input_path="in.parquet",
                output_path="out.parquet",
                column_list=["name"],
                descending=False,
                verbose=False,
                compression="ZSTD",
                compression_level=None,
                row_group_size_mb=None,
                row_group_rows=None,
                profile=None,
                geoparquet_version=None,
                memory_limit="604MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "604MB"

    def test_add_kdtree_streaming_forwards_memory_limit(self, buildings_test_file):
        from geoparquet_io.core.add.kdtree import _add_kdtree_streaming

        with mock.patch("geoparquet_io.core.add.kdtree.write_output") as mocked:
            _add_kdtree_streaming(
                input_path=buildings_test_file,
                output_path="unused_output.parquet",
                kdtree_column_name="kdtree_cell",
                iterations=1,
                verbose=False,
                compression="ZSTD",
                compression_level=None,
                row_group_size_mb=None,
                row_group_rows=None,
                sample_size=100,
                profile=None,
                geoparquet_version=None,
                memory_limit="605MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "605MB"

    def test_add_quadkey_streaming_forwards_memory_limit(self, places_test_file):
        from geoparquet_io.core.add.quadkey import _add_quadkey_streaming

        with mock.patch("geoparquet_io.core.add.quadkey.write_output") as mocked:
            _add_quadkey_streaming(
                input_path=places_test_file,
                output_path="unused_output.parquet",
                quadkey_column_name="quadkey",
                resolution=13,
                use_centroid=False,
                verbose=False,
                compression="ZSTD",
                compression_level=None,
                row_group_size_mb=None,
                row_group_rows=None,
                profile=None,
                geoparquet_version=None,
                memory_limit="606MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "606MB"

    def test_sort_by_quadkey_streaming_forwards_memory_limit(self, places_test_file):
        from geoparquet_io.core.sort_quadkey import _sort_by_quadkey_streaming

        with mock.patch("geoparquet_io.core.sort_quadkey.write_output") as mocked:
            _sort_by_quadkey_streaming(
                input_path=places_test_file,
                output_path="unused_output.parquet",
                quadkey_column_name="quadkey",
                resolution=13,
                use_centroid=False,
                remove_quadkey_column=False,
                verbose=False,
                compression="ZSTD",
                compression_level=None,
                row_group_size_mb=None,
                row_group_rows=None,
                profile=None,
                geoparquet_version=None,
                memory_limit="607MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "607MB"

    def test_add_bbox_streaming_forwards_memory_limit(self, places_test_file):
        from geoparquet_io.core.add.bbox import _add_bbox_streaming

        with mock.patch("geoparquet_io.core.add.bbox.write_output") as mocked:
            _add_bbox_streaming(
                input_path=places_test_file,
                output_path="out.parquet",
                bbox_column_name="bbox",
                verbose=False,
                compression="ZSTD",
                compression_level=None,
                row_group_size_mb=None,
                row_group_rows=None,
                profile=None,
                force=False,
                geoparquet_version=None,
                memory_limit="609MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "609MB"

    def test_execute_transform_forwards_memory_limit(self, places_test_file, temp_output_file):
        """execute_transform itself (used by h3/a5/s2/sort column streaming) must
        forward memory_limit into write_output -> write_parquet_with_metadata."""
        from geoparquet_io.core.stream_io import execute_transform

        def make_query(source: str, con) -> str:
            return f"SELECT * FROM {source}"

        with mock.patch("geoparquet_io.core.stream_io.write_output") as mocked:
            execute_transform(
                places_test_file,
                temp_output_file,
                make_query,
                verbose=False,
                memory_limit="608MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "608MB"


class TestStdoutStreamingIgnoresMemoryLimit:
    """Arrow IPC output to stdout has no DuckDB write engine to configure, so
    memory_limit is dropped — but the user must be told, not silently ignored."""

    def test_write_output_to_stdout_warns_that_memory_limit_is_ignored(
        self, places_test_file, capsys
    ):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.stream_io import write_output

        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            with mock.patch("geoparquet_io.core.stream_io._write_stream_output") as mocked:
                write_output(
                    con,
                    f"SELECT * FROM '{places_test_file}'",
                    "-",
                    memory_limit="512MB",
                )
        finally:
            con.close()
        mocked.assert_called_once()
        assert "memory" in capsys.readouterr().err.lower()


class TestWriteMemoryHelpTextIsPerCommand:
    """`extract bigquery` writes through PyArrow, so its --write-memory bounds the
    DuckDB scan of the BigQuery result, not a write. Its help text has to say so
    (gpio #760) — and overriding it there must not change every other command."""

    def test_bigquery_help_describes_the_scan_not_a_streaming_write(self):
        result = CliRunner().invoke(cli, ["extract", "bigquery", "--help"])
        assert result.exit_code == 0
        help_text = " ".join(result.output.split())
        assert "Memory limit for the DuckDB scan of the BigQuery result" in help_text
        assert "writes through PyArrow" in help_text
        assert "streaming writes" not in help_text

    def test_sibling_command_keeps_the_generic_streaming_write_wording(self):
        result = CliRunner().invoke(cli, ["sort", "hilbert", "--help"])
        assert result.exit_code == 0
        help_text = " ".join(result.output.split())
        assert "Memory limit for streaming writes" in help_text
        assert "BigQuery" not in help_text

    def test_decorator_works_bare_and_called(self):
        """Both spellings of the decorator attach the same option."""
        from geoparquet_io.cli.decorators import write_memory_option

        @click.command()
        @write_memory_option
        def bare(write_memory):  # pragma: no cover - help only
            pass

        @click.command()
        @write_memory_option(help="Custom wording.")
        def overridden(write_memory):  # pragma: no cover - help only
            pass

        runner = CliRunner()
        bare_help = " ".join(runner.invoke(bare, ["--help"]).output.split())
        overridden_help = " ".join(runner.invoke(overridden, ["--help"]).output.split())

        assert "Memory limit for streaming writes" in bare_help
        assert "Custom wording." in overridden_help
        assert "streaming writes" not in overridden_help
