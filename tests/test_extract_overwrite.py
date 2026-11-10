"""Tests for --overwrite behavior in extract commands."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.extract import extract as extract_impl

# Sample test files
TEST_PARQUET = Path(__file__).parent / "data" / "fields_pgo_crs84_zstd.parquet"


class TestExtractGeoparquetOverwrite:
    """Test --overwrite behavior for extract geoparquet command."""

    def test_extract_geoparquet_default_fails_if_exists(self, tmp_path):
        """Test that extract geoparquet fails by default if output exists."""
        output_file = tmp_path / "output.parquet"

        # Create an existing file
        output_file.write_text("existing content")

        runner = CliRunner()
        result = runner.invoke(cli, ["extract", "geoparquet", str(TEST_PARQUET), str(output_file)])

        assert result.exit_code != 0
        assert "already exists" in result.output or "Use --overwrite" in result.output
        # Verify original file wasn't modified
        assert output_file.read_text() == "existing content"

    def test_extract_geoparquet_overwrite_true_replaces(self, tmp_path):
        """Test that --overwrite=true allows overwriting existing file."""
        output_file = tmp_path / "output.parquet"

        # Create an existing file
        output_file.write_text("existing content")

        runner = CliRunner()
        result = runner.invoke(
            cli, ["extract", "geoparquet", str(TEST_PARQUET), str(output_file), "--overwrite"]
        )

        assert result.exit_code == 0
        # Verify file was overwritten (no longer text content)
        assert output_file.exists()
        assert output_file.stat().st_size > 100  # Parquet file should be larger

    def test_extract_geoparquet_no_existing_file_works(self, tmp_path):
        """Test that extract works normally when output doesn't exist."""
        output_file = tmp_path / "output.parquet"

        runner = CliRunner()
        result = runner.invoke(cli, ["extract", "geoparquet", str(TEST_PARQUET), str(output_file)])

        if result.exit_code != 0:
            print(f"Command output: {result.output}")
            print(f"Exception: {result.exception}")
        assert result.exit_code == 0
        assert output_file.exists()
        assert output_file.stat().st_size > 100

    def test_extract_impl_overwrite_parameter(self, tmp_path):
        """Test that extract core function accepts overwrite parameter."""
        output_file = tmp_path / "output.parquet"
        output_file.write_text("existing")

        # Should fail with overwrite=False
        with pytest.raises(Exception, match="already exists|Use --overwrite"):
            extract_impl(str(TEST_PARQUET), str(output_file), overwrite=False, verbose=False)

        # Should succeed with overwrite=True
        extract_impl(str(TEST_PARQUET), str(output_file), overwrite=True, verbose=False)
        assert output_file.exists()
        assert output_file.stat().st_size > 100


class TestExtractArcGISOverwrite:
    """Test --overwrite behavior for extract arcgis command."""

    @pytest.mark.network
    def test_extract_arcgis_default_fails_if_exists(self, tmp_path):
        """Test that extract arcgis fails by default if output exists."""
        output_file = tmp_path / "output.parquet"
        output_file.write_text("existing content")

        runner = CliRunner()
        # Using a simple test URL - may need to adjust for actual testing
        result = runner.invoke(
            cli, ["extract", "arcgis", "https://services.arcgis.com/test", str(output_file)]
        )

        # Should fail because output exists
        assert result.exit_code != 0
        assert "already exists" in result.output or "Use --overwrite" in result.output

    @pytest.mark.network
    def test_extract_arcgis_overwrite_true_replaces(self, tmp_path):
        """Test that --overwrite=true allows overwriting for arcgis."""
        output_file = tmp_path / "output.parquet"
        output_file.write_text("existing content")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "arcgis",
                "https://services.arcgis.com/test",
                str(output_file),
                "--overwrite",
            ],
        )

        # This test validates that --overwrite flag is accepted by the CLI.
        # The command will fail due to invalid URL, but we verify that:
        # 1. The error is NOT about the file already existing (overwrite worked)
        # 2. The error is about network/URL issues instead
        assert "already exists" not in result.output
        # Network/URL errors are expected with fake URL - that's fine


class TestExtractBigQueryOverwrite:
    """Test --overwrite behavior for extract bigquery command."""

    @pytest.mark.network
    def test_extract_bigquery_default_fails_if_exists(self, tmp_path):
        """Test that extract bigquery fails by default if output exists."""
        output_file = tmp_path / "output.parquet"
        output_file.write_text("existing content")

        runner = CliRunner()
        result = runner.invoke(
            cli, ["extract", "bigquery", "project.dataset.table", str(output_file)]
        )

        # Should fail because output exists
        assert result.exit_code != 0
        assert "already exists" in result.output or "Use --overwrite" in result.output

    def test_extract_bigquery_overwrite_true_replaces(self, tmp_path):
        """Overwrite replaces existing bytes after a successful BigQuery read."""
        from unittest.mock import MagicMock, patch

        import pyarrow as pa
        import pyarrow.parquet as pq

        output_file = tmp_path / "output.parquet"
        output_file.write_text("existing content")
        expected = pa.table({"id": [42]})
        con = MagicMock()
        con.sql.return_value.arrow.return_value.read_all.return_value = expected
        with (
            patch("geoparquet_io.core.extract_bigquery.BigQueryConnection") as connection,
            patch(
                "geoparquet_io.core.extract_bigquery._schema_rows", return_value=[("id", "BIGINT")]
            ),
        ):
            connection.return_value.__enter__.return_value = con
            result = CliRunner().invoke(
                cli,
                ["extract", "bigquery", "project.dataset.table", str(output_file), "--overwrite"],
            )
        assert result.exit_code == 0, result.output
        assert pq.read_table(output_file).equals(expected)
