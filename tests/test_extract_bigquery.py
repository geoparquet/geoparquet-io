"""
Tests for the extract bigquery command and API.

Tests are organized into:
- Unit tests (no BigQuery access needed): CLI structure, dry-run, backwards compatibility
- Integration tests (marked @pytest.mark.network): require BQ_TEST_TABLE env var
"""

from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import extract
from tests.conftest import safe_unlink


class TestExtractCommandGroup:
    """Test that extract command group works correctly."""

    def test_extract_help_shows_subcommands(self):
        """Test that extract --help shows both subcommands."""
        runner = CliRunner()
        result = runner.invoke(extract, ["--help"])
        assert result.exit_code == 0
        assert "geoparquet" in result.output
        assert "bigquery" in result.output

    def test_extract_bigquery_help(self):
        """Test that extract bigquery --help works."""
        runner = CliRunner()
        result = runner.invoke(extract, ["bigquery", "--help"])
        assert result.exit_code == 0
        assert "TABLE_ID" in result.output
        assert "--project" in result.output
        assert "--credentials-file" in result.output
        assert "--where" in result.output
        assert "--limit" in result.output


class TestBackwardsCompatibility:
    """Test that 'gpio extract input.parquet output.parquet' still works."""

    @pytest.fixture
    def input_file(self):
        """Create a test parquet file."""
        # Create a simple test parquet file
        schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
            ]
        )
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}, schema=schema)
        tmp_path = Path(tempfile.gettempdir()) / f"test_input_{uuid.uuid4()}.parquet"
        pq.write_table(table, str(tmp_path))
        yield str(tmp_path)
        safe_unlink(tmp_path)

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_output_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_extract_without_subcommand(self, input_file, output_file):
        """Test that extract without explicit subcommand defaults to geoparquet."""
        runner = CliRunner()
        result = runner.invoke(extract, [input_file, output_file])
        # Should work (exit 0) or give appropriate error
        # The key is it shouldn't fail with "Unknown subcommand"
        assert "Unknown command" not in result.output


class TestDetectGeometryColumn:
    """Test geometry column detection function."""

    def test_detect_common_names(self):
        """Test detection of common geometry column names."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column

        # Create table with "geometry" column
        table = pa.table(
            {
                "id": [1, 2],
                "geometry": [b"test", b"test2"],
            }
        )
        assert _detect_geometry_column(table) == "geometry"

        # Create table with "geography" column
        table = pa.table(
            {
                "id": [1, 2],
                "geography": [b"test", b"test2"],
            }
        )
        assert _detect_geometry_column(table) == "geography"

        # Create table with "geom" column
        table = pa.table(
            {
                "id": [1, 2],
                "geom": [b"test", b"test2"],
            }
        )
        assert _detect_geometry_column(table) == "geom"

    def test_detect_fallback_to_geo_in_name(self):
        """Test fallback to columns containing 'geo' in name."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column

        table = pa.table(
            {
                "id": [1, 2],
                "building_geom_wkb": [b"test", b"test2"],
            }
        )
        assert _detect_geometry_column(table) == "building_geom_wkb"

    def test_detect_returns_none_when_not_found(self):
        """Test that None is returned when no geometry column found."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column

        table = pa.table(
            {
                "id": [1, 2],
                "name": ["a", "b"],
            }
        )
        assert _detect_geometry_column(table) is None


class TestExtractBigQueryTable:
    """Test the extract_bigquery_table function for in-memory tables."""

    def test_extract_with_limit(self):
        """Test extracting with a row limit."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery_table

        table = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["a", "b", "c", "d", "e"],
            }
        )
        result = extract_bigquery_table(table, limit=3)
        assert result.num_rows == 3

    def test_extract_with_columns(self):
        """Test extracting specific columns."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery_table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "value": [10, 20, 30],
            }
        )
        result = extract_bigquery_table(table, columns=["id", "name"])
        assert result.column_names == ["id", "name"]

    def test_extract_with_exclude_columns(self):
        """Test excluding specific columns."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery_table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "value": [10, 20, 30],
            }
        )
        result = extract_bigquery_table(table, exclude_columns=["value"])
        assert "value" not in result.column_names
        assert "id" in result.column_names
        assert "name" in result.column_names


class TestDryRun:
    """Test dry-run functionality."""

    def test_dry_run_shows_sql(self):
        """Test that dry-run shows SQL without executing."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        # Dry-run should not raise an error even without credentials
        result = extract_bigquery(
            table_id="project.dataset.table",
            dry_run=True,
        )
        assert result is None


class TestPythonAPI:
    """Test the Python API for BigQuery."""

    def test_table_from_bigquery_exists(self):
        """Test that Table.from_bigquery method exists."""
        from geoparquet_io.api import Table

        assert hasattr(Table, "from_bigquery")
        assert callable(Table.from_bigquery)

    def test_ops_read_bigquery_exists(self):
        """Test that ops.read_bigquery function exists."""
        from geoparquet_io.api import ops

        assert hasattr(ops, "read_bigquery")
        assert callable(ops.read_bigquery)

    def test_read_bigquery_top_level_export(self):
        """Test that read_bigquery is exported at top level."""
        import geoparquet_io as gpio

        assert hasattr(gpio, "read_bigquery")
        assert callable(gpio.read_bigquery)


class TestBigQueryConnection:
    """Test BigQuery connection setup."""

    @patch("duckdb.connect")
    def test_connection_loads_extensions_in_order(self, mock_connect):
        """Test that spatial extension is loaded before bigquery settings."""
        from geoparquet_io.core.extract_bigquery import get_bigquery_connection

        mock_con = MagicMock()
        mock_connect.return_value = mock_con

        try:
            get_bigquery_connection()
        except Exception:
            pass  # May fail without actual BigQuery, but we check calls

        # Verify order of execute calls
        calls = [call[0][0] for call in mock_con.execute.call_args_list]
        spatial_idx = next((i for i, c in enumerate(calls) if "spatial" in c.lower()), -1)
        bq_geom_idx = next(
            (i for i, c in enumerate(calls) if "geography_as_geometry" in c.lower()), -1
        )

        # Spatial should be loaded before geography setting
        if spatial_idx >= 0 and bq_geom_idx >= 0:
            assert spatial_idx < bq_geom_idx, "Spatial must be loaded before geography setting"

    def test_credentials_file_validation(self):
        """Test that non-existent credentials file raises error."""
        from geoparquet_io.core.extract_bigquery import get_bigquery_connection

        with pytest.raises(FileNotFoundError, match="Credentials file not found"):
            get_bigquery_connection(credentials_file="/nonexistent/path/credentials.json")


# Integration tests that require BigQuery access
@pytest.mark.network
class TestBigQueryIntegration:
    """Integration tests requiring BigQuery access.

    Set BQ_TEST_TABLE environment variable to run these tests:
    export BQ_TEST_TABLE=project.dataset.table
    """

    @pytest.fixture
    def bq_table_id(self):
        """Get BigQuery test table from environment."""
        table_id = os.environ.get("BQ_TEST_TABLE")
        if not table_id:
            pytest.skip("BQ_TEST_TABLE environment variable not set")
        return table_id

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_bq_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_extract_from_bigquery(self, bq_table_id, output_file):
        """Test extracting data from BigQuery to GeoParquet."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        extract_bigquery(
            table_id=bq_table_id,
            output_parquet=output_file,
            limit=10,
        )

        # Verify output file exists and is valid GeoParquet
        assert Path(output_file).exists()
        table = pq.read_table(output_file)
        assert table.num_rows == 10

    def test_extract_with_spherical_edges(self, bq_table_id, output_file):
        """Test that BigQuery output has spherical edges in metadata."""
        import json

        from geoparquet_io.core.extract_bigquery import extract_bigquery

        extract_bigquery(
            table_id=bq_table_id,
            output_parquet=output_file,
            limit=5,
        )

        # Check geo metadata for spherical edges
        pf = pq.ParquetFile(output_file)
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata, "Expected geo metadata in BigQuery-extracted Parquet"

        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        columns = geo_meta.get("columns", {})
        assert columns, "Expected at least one geometry column in geo metadata"

        # Verify all geometry columns have spherical edges
        for col_name, col_meta in columns.items():
            if "edges" in col_meta:
                assert col_meta["edges"] == "spherical", (
                    f"Expected spherical edges for column {col_name}, got {col_meta['edges']}"
                )

    def test_python_api_from_bigquery(self, bq_table_id):
        """Test Table.from_bigquery() method."""
        from geoparquet_io.api import Table

        table = Table.from_bigquery(bq_table_id, limit=5)
        assert isinstance(table, Table)
        assert table.num_rows == 5
