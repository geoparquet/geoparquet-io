"""Tests for CLI exception handler."""

import click

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


def test_the_where_hint_does_not_displace_the_taxonomy():
    """A spill-space error under `process aggregate --where` keeps its disk-shortage line."""
    import duckdb

    from geoparquet_io.cli.exception_handler import click_error_for

    exc = duckdb.OutOfMemoryException("Out of Memory Error: max_temp_directory_size reached")
    error = click_error_for(exc, hint='This is most likely caused by --where "x"')
    assert "TMPDIR" in error.message
    assert "--where" in error.message


def test_an_invalid_parameter_wrapped_for_the_cli_is_a_bad_parameter_on_a_bigquery_source():
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    result = CliRunner().invoke(
        cli, ["extract", "bigquery", "--sql", "SELECT 1", "p.d.t", "o.parquet"]
    )
    assert result.exit_code == 2
