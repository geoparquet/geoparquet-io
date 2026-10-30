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
