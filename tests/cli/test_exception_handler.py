"""Tests for CLI exception handler."""

import click
import pytest

from geoparquet_io.cli.exception_handler import (
    handle_core_exception,
)
from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeometryError,
    GeoParquetError,
    InvalidParameterError,
    PartitionError,
    RemoteAccessError,
    ValidationError,
)


class TestExceptionHandler:
    """Test CLI exception handling."""

    def test_converts_file_not_found_to_click_exception(self):
        exc = FileNotFoundGeoParquetError("test.parquet")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.ClickException)
        assert "test.parquet" in click_exc.message

    def test_converts_invalid_parameter_to_bad_parameter(self):
        exc = InvalidParameterError("resolution", "must be 1-15")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.BadParameter)
        assert "resolution" in click_exc.format_message()  # the option is named once, by Click

    def test_converts_remote_access_error_to_click_exception(self):
        exc = RemoteAccessError("s3://bucket/file", "Access denied")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.ClickException)
        assert "Access denied" in click_exc.message

    def test_converts_geometry_error_to_click_exception(self):
        exc = GeometryError("No geometry column found")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.ClickException)
        assert "No geometry column" in click_exc.message

    def test_converts_partition_error_to_click_exception(self):
        exc = PartitionError("Failed to partition")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.ClickException)
        assert "Failed to partition" in click_exc.message

    def test_converts_validation_error_to_click_exception(self):
        exc = ValidationError("Invalid GeoParquet")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.ClickException)
        assert "Invalid GeoParquet" in click_exc.message

    def test_converts_base_geoparquet_error(self):
        exc = GeoParquetError("Generic error")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.ClickException)
        assert "Generic error" in click_exc.message

    def test_the_root_group_translates_a_gpio_error_to_one_line(self):
        from click.testing import CliRunner

        from geoparquet_io.cli.decorators import ErrorBoundaryGroup

        @click.group(cls=ErrorBoundaryGroup)
        def root():
            pass

        @root.command()
        def boom():
            raise FileNotFoundGeoParquetError("missing.parquet")

        result = CliRunner().invoke(root, ["boom"])
        assert result.exit_code == 1
        assert "Error: File not found: missing.parquet" in result.output
        assert "Traceback" not in result.output

    def test_invalid_parameter_is_a_bad_parameter_everywhere(self):
        """One table: the option-naming error is a BadParameter at every entry point."""
        from geoparquet_io.cli.exception_handler import click_error_for

        exc = InvalidParameterError("limit", "must be positive")
        assert isinstance(handle_core_exception(exc), click.BadParameter)
        assert isinstance(click_error_for(exc), click.BadParameter)

    def test_a_wrapped_duckdb_error_keeps_its_hint(self):
        """The hints are asked before the gpio-error short circuit."""
        import duckdb

        from geoparquet_io.cli.exception_handler import cli_error_for

        try:
            try:
                raise duckdb.OutOfMemoryException(
                    "Out of Memory Error: max_temp_directory_size reached"
                )
            except duckdb.Error as inner:
                raise GeoParquetError("conversion failed") from inner
        except GeoParquetError as outer:
            error = cli_error_for(outer)
        assert error is not None and "TMPDIR" in error.message


def test_a_wrapped_duckdb_error_keeps_its_hint_on_a_glob_aware_command():
    """The standalone command and the root group answer in the same order (spill hint first)."""
    import duckdb
    from click.testing import CliRunner

    from geoparquet_io.cli.decorators import GlobAwareCommand

    @click.command(cls=GlobAwareCommand)
    def boom():
        try:
            raise duckdb.OutOfMemoryException(
                "Out of Memory Error: max_temp_directory_size reached"
            )
        except duckdb.Error as inner:
            raise GeoParquetError("conversion failed") from inner

    result = CliRunner().invoke(boom, [])
    assert result.exit_code == 1
    assert "TMPDIR" in result.output


def test_the_where_hint_only_goes_where_the_input_could_not_be_read():
    """A disk shortage, an unpublished extension or a non-Parquet file are not a --where problem."""
    import duckdb

    from geoparquet_io.cli.exception_handler import click_error_for

    hint = 'This is most likely caused by --where "x"'
    spill = duckdb.OutOfMemoryException("Out of Memory Error: max_temp_directory_size reached")
    error = click_error_for(spill, hint=hint)
    assert "TMPDIR" in error.message and "--where" not in error.message

    not_parquet = duckdb.InvalidInputException("No magic bytes found at end of file 'x'")
    assert "--where" not in click_error_for(not_parquet, hint=hint).message

    binder = duckdb.BinderException('Binder Error: Referenced column "yr" not found')
    assert "--where" in click_error_for(binder, hint=hint).message


@pytest.mark.parametrize(
    "raised",
    [
        lambda: (_ for _ in ()).throw(GeoParquetError("boom")),
        lambda: __import__("duckdb").IOException(
            "IO Error: No files found that match the pattern 'x'"
        ),
    ],
    ids=["gpio-error", "duckdb-io"],
)
def test_a_glob_aware_command_and_a_plain_one_answer_alike(raised):
    """One boundary: same output and the same --verbose traceback for the same exception."""
    import logging

    from click.testing import CliRunner

    from geoparquet_io.cli.decorators import ErrorBoundaryGroup, GlobAwareCommand

    def _boom():
        exc = raised()
        raise exc

    def build(cls):
        @click.group(cls=ErrorBoundaryGroup)
        def root():
            pass

        @root.command("run", **({"cls": cls} if cls else {}))
        def run():
            _boom()

        return root

    class _Capture(logging.Handler):
        def __init__(self):
            super().__init__(logging.DEBUG)
            self.records = []

        def emit(self, record):
            self.records.append(record)

    results = {}
    for label, root, target in (
        ("plain-root", build(None), "run"),
        ("glob-root", build(GlobAwareCommand), "run"),
    ):
        handler = _Capture()
        logger = logging.getLogger("geoparquet_io")
        old = logger.level
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        try:
            result = CliRunner().invoke(root, [target])
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old)
        traced = [r for r in handler.records if r.exc_info]
        results[label] = (result.exit_code, result.output, len(traced))

    assert results["plain-root"] == results["glob-root"]
    assert results["glob-root"][2] == 1, "the --verbose traceback was not logged"

    # Standalone (no root group): the command answers the same way.
    standalone = CliRunner().invoke(build(GlobAwareCommand).commands["run"], [])
    assert standalone.exit_code == results["glob-root"][0]
    assert standalone.output == results["glob-root"][1]


def test_an_invalid_parameter_wrapped_for_the_cli_is_a_bad_parameter_on_a_bigquery_source():
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    result = CliRunner().invoke(
        cli, ["extract", "bigquery", "--sql", "SELECT 1", "p.d.t", "o.parquet"]
    )
    assert result.exit_code == 2


def test_a_gpio_error_from_a_real_subcommand_answers_like_a_plain_command(monkeypatch, tmp_path):
    """No translation in front of the boundary: no Usage preamble, one --verbose traceback."""
    import logging

    import pyarrow as pa
    import pyarrow.parquet as pq
    from click.testing import CliRunner

    from geoparquet_io.cli.decorators import ErrorBoundaryGroup
    from geoparquet_io.cli.main import cli

    src = tmp_path / "in.parquet"
    pq.write_table(pa.table({"id": [1]}), src)

    def boom(*_a, **_k):
        raise InvalidParameterError("tolerance", "must be positive")

    monkeypatch.setattr(
        __import__("sys").modules["geoparquet_io.cli.commands.process"], "simplify_file_impl", boom
    )

    @click.group(cls=ErrorBoundaryGroup)
    def plain_root():
        pass

    @plain_root.command("run")
    def run():
        raise InvalidParameterError("tolerance", "must be positive")

    class _Capture(logging.Handler):
        def __init__(self):
            super().__init__(logging.DEBUG)
            self.records = []

        def emit(self, record):
            self.records.append(record)

    def invoke(group, argv):
        handler = _Capture()
        logger = logging.getLogger()  # the root: the CLI re-installs the package logger's handlers
        old = logger.level
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        try:
            result = CliRunner().invoke(group, argv)
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old)
        return result, [r for r in handler.records if r.exc_info]

    real, real_traces = invoke(
        cli,
        [
            "process",
            "simplify",
            str(src),
            str(tmp_path / "o.parquet"),
            "--tolerance",
            "0.1",
            "--verbose",
        ],
    )
    plain, plain_traces = invoke(plain_root, ["run"])

    assert real.exit_code == plain.exit_code == 2

    def error_lines(result):
        return [ln for ln in result.output.splitlines() if ln.startswith("Error:")]

    assert error_lines(real) == error_lines(plain) != []
    assert "Usage:" not in real.output
    assert len(real_traces) == len(plain_traces) == 1


@pytest.mark.parametrize(
    ("argv", "target"),
    [
        (["inspect", "stats", "{src}"], "geoparquet_io.cli.commands.inspect._inspect_stats_core"),
        (
            ["process", "aggregate", "h3", "{src}", "{out}", "--resolution", "5"],
            "geoparquet_io.cli.commands.process.aggregate_by_h3_impl",
        ),
        (
            ["pmtiles", "create", "{src}", "{out}"],
            "geoparquet_io.core.pmtiles.create_pmtiles_from_geoparquet",
        ),
    ],
    ids=["inspect-stats", "process-aggregate-h3", "pmtiles-create"],
)
def test_every_answering_path_logs_exactly_one_traceback(monkeypatch, tmp_path, argv, target):
    """boundary, `click_error_for` and the inspect boundary all go through one answering step."""
    import importlib
    import logging
    import sys

    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    src = tmp_path / "in.parquet"
    pq.write_table(pa.table({"id": [1]}), src)
    module_name, _, attr = target.rpartition(".")
    importlib.import_module(module_name)

    def boom(*_a, **_k):
        raise duckdb.IOException("IO Error: Could not read the input")

    monkeypatch.setattr(sys.modules[module_name], attr, boom)

    records = []

    class _Capture(logging.Handler):
        def emit(self, record):
            if record.exc_info:
                records.append(record)

    root = logging.getLogger()
    handler = _Capture(logging.DEBUG)
    old = root.level
    root.setLevel(logging.DEBUG)
    root.addHandler(handler)
    try:
        result = CliRunner().invoke(
            cli,
            [a.format(src=src, out=tmp_path / "o.parquet") for a in argv] + ["--verbose"],
        )
    finally:
        root.removeHandler(handler)
        root.setLevel(old)

    assert result.exit_code == 1, result.output
    assert "Could not read the input" in result.output
    assert len(records) == 1, [r.getMessage() for r in records]


@pytest.mark.parametrize(
    ("argv", "target"),
    [
        (["inspect", "stats", "{src}"], "_inspect_stats_core"),
        (["inspect", "summary", "{src}"], "_inspect_summary_core"),
        (["inspect", "head", "{src}"], "_inspect_preview_core"),
        (["inspect", "tail", "{src}"], "_inspect_preview_core"),
        (["inspect", "meta", "{src}"], "_handle_meta_display"),
    ],
    ids=["stats", "summary", "head", "tail", "meta"],
)
def test_an_inspect_command_answers_a_gpio_error_without_a_usage_preamble(
    monkeypatch, tmp_path, argv, target
):
    import sys

    import pyarrow as pa
    import pyarrow.parquet as pq
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    src = tmp_path / "in.parquet"
    pq.write_table(pa.table({"id": [1]}), src)

    def boom(*_a, **_k):
        raise InvalidParameterError("count", "must be positive")

    monkeypatch.setattr(sys.modules["geoparquet_io.cli.commands.inspect"], target, boom)

    result = CliRunner().invoke(cli, [a.format(src=src) for a in argv])

    assert result.exit_code == 2, result.output
    assert "Usage:" not in result.output
    assert "must be positive" in result.output


def test_a_file_that_is_not_parquet_gets_one_answer_from_every_path(tmp_path):
    """inspect, check and the metadata readers word it the same, naming the file."""
    import pytest as _pytest
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli
    from geoparquet_io.core.duckdb_metadata import get_kv_metadata
    from geoparquet_io.core.exceptions import not_parquet_file_error
    from geoparquet_io.core.geo_metadata import raw_geo_of_file

    bad = tmp_path / "data.parquet"
    bad.write_text("id,name\n1,x\n")
    expected = not_parquet_file_error(str(bad)).message

    for argv in (
        ["inspect", "summary"],
        ["check", "all"],
        ["check", "bbox"],
        ["check", "row-group"],
    ):
        result = CliRunner().invoke(cli, [*argv, str(bad)])
        assert result.exit_code == 1, (argv, result.output)
        assert f"Error: {expected}" in result.output, (argv, result.output)
    for reader in (raw_geo_of_file, get_kv_metadata):
        with _pytest.raises(GeoParquetError) as caught:
            reader(str(bad))
        assert caught.value.message == expected


@pytest.mark.parametrize("content", [b"id,name\n1,x\n", b"PAR1"], ids=["text", "four-bytes"])
def test_every_reader_and_command_words_a_non_parquet_file_alike(tmp_path, content):
    """DuckDB's and PyArrow's phrasings, and every command that opens the file, agree."""
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli
    from geoparquet_io.core.duckdb_metadata import get_kv_metadata
    from geoparquet_io.core.exceptions import not_parquet_file_error
    from geoparquet_io.core.geo_metadata import raw_geo_of_file

    bad = tmp_path / "data.parquet"
    bad.write_bytes(content)
    out = tmp_path / "out.parquet"
    expected = not_parquet_file_error(str(bad)).message

    for argv in (
        ["inspect", "summary", str(bad)],
        ["check", "all", str(bad)],
        ["extract", "geoparquet", str(bad), str(out)],
        ["convert", "geoparquet", str(bad), str(out)],
    ):
        result = CliRunner().invoke(cli, argv)
        assert result.exit_code == 1, (argv, result.output)
        assert f"Error: {expected}" in result.output, (argv, result.output)
    for reader in (raw_geo_of_file, get_kv_metadata):
        with pytest.raises(GeoParquetError) as caught:
            reader(str(bad))
        assert caught.value.message == expected
