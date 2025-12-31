"""
Tests for GeoJSON streaming output.

Tests verify that the --to geojson option:
- Outputs valid GeoJSON Features
- Includes RFC 8142 record separators by default
- Supports --no-rs flag to disable separators
- Works with both file input and stdin (pipeline)
"""

import json
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.geojson_stream import (
    _build_feature_query,
    _get_property_columns,
    _quote_identifier,
    convert_to_geojson,
)

# Test data
TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES_PARQUET = TEST_DATA_DIR / "places_test.parquet"
BUILDINGS_PARQUET = TEST_DATA_DIR / "buildings_test.parquet"


class TestQuoteIdentifier:
    """Tests for SQL identifier quoting."""

    def test_simple_name(self):
        """Test quoting a simple identifier."""
        assert _quote_identifier("name") == '"name"'

    def test_name_with_spaces(self):
        """Test quoting identifier with spaces."""
        assert _quote_identifier("my column") == '"my column"'

    def test_name_with_quotes(self):
        """Test quoting identifier with embedded quotes."""
        assert _quote_identifier('foo"bar') == '"foo""bar"'


class TestBuildFeatureQuery:
    """Tests for SQL query construction."""

    def test_basic_query(self):
        """Test basic feature query generation."""
        query = _build_feature_query("test_table", "geometry", ["name", "population"])
        assert "ST_AsGeoJSON" in query
        assert "Feature" in query
        assert "name" in query
        assert "population" in query

    def test_empty_properties(self):
        """Test query with no properties."""
        query = _build_feature_query("test_table", "geometry", [])
        assert "ST_AsGeoJSON" in query
        assert "'{}'" in query  # Empty properties object

    def test_special_column_names(self):
        """Test query handles special column names."""
        query = _build_feature_query("test_table", "geometry", ["my column", "type"])
        assert '"my column"' in query
        assert '"type"' in query


class TestGetPropertyColumns:
    """Tests for property column selection."""

    @pytest.fixture
    def mock_duckdb_connection(self):
        """Create a mock DuckDB connection."""
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("""
            CREATE TABLE test_data AS
            SELECT
                1 as id,
                'test' as name,
                100 as population,
                ST_GeomFromText('POINT(0 0)') as geometry,
                STRUCT_PACK(xmin := 0, ymin := 0, xmax := 1, ymax := 1) as bbox
        """)
        yield con
        con.close()

    def test_excludes_geometry_column(self, mock_duckdb_connection):
        """Test that geometry column is excluded from properties."""
        cols = _get_property_columns(mock_duckdb_connection, "test_data", "geometry")
        assert "geometry" not in cols
        assert "id" in cols
        assert "name" in cols

    def test_excludes_bbox_column(self, mock_duckdb_connection):
        """Test that bbox column is excluded by default."""
        cols = _get_property_columns(mock_duckdb_connection, "test_data", "geometry")
        assert "bbox" not in cols


@pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
class TestConvertToGeoJSON:
    """Tests for convert_to_geojson function."""

    def test_basic_output(self, capsys):
        """Test basic GeoJSON output."""
        # Capture stdout
        count = convert_to_geojson(str(PLACES_PARQUET), rs=False)

        captured = capsys.readouterr()
        lines = [line for line in captured.out.strip().split("\n") if line]

        assert count > 0
        assert len(lines) == count

        # Verify first line is valid GeoJSON Feature
        feature = json.loads(lines[0])
        assert feature["type"] == "Feature"
        assert "geometry" in feature
        assert "properties" in feature

    def test_rs_separators_default(self, capsys):
        """Test RFC 8142 record separators are included by default."""
        convert_to_geojson(str(PLACES_PARQUET), rs=True)

        captured = capsys.readouterr()
        # Check for record separator character
        assert "\x1e" in captured.out

    def test_no_rs_separators(self, capsys):
        """Test disabling RS separators."""
        convert_to_geojson(str(PLACES_PARQUET), rs=False)

        captured = capsys.readouterr()
        # No record separator character
        assert "\x1e" not in captured.out

    def test_valid_geojson_features(self, capsys):
        """Test that all output lines are valid GeoJSON Features."""
        convert_to_geojson(str(PLACES_PARQUET), rs=False)

        captured = capsys.readouterr()
        for line in captured.out.strip().split("\n"):
            if line:
                feature = json.loads(line)
                assert feature["type"] == "Feature"
                assert "geometry" in feature
                assert feature["geometry"] is not None
                assert "type" in feature["geometry"]
                assert "coordinates" in feature["geometry"]
                assert "properties" in feature


@pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
class TestConvertCLI:
    """Tests for gpio convert --to geojson CLI."""

    def test_basic_geojson_output(self):
        """Test gpio convert --to geojson command."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", str(PLACES_PARQUET), "--to", "geojson"])

        assert result.exit_code == 0

        # Parse first feature (skip RS char if present)
        output = result.output
        if output.startswith("\x1e"):
            output = output[1:]
        first_line = output.split("\n")[0]
        feature = json.loads(first_line)
        assert feature["type"] == "Feature"

    def test_no_rs_flag(self):
        """Test --no-rs flag disables record separators."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", str(PLACES_PARQUET), "--to", "geojson", "--no-rs"])

        assert result.exit_code == 0
        assert "\x1e" not in result.output

    def test_rs_enabled_by_default(self):
        """Test RS separators are enabled by default."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", str(PLACES_PARQUET), "--to", "geojson"])

        assert result.exit_code == 0
        assert "\x1e" in result.output

    def test_missing_output_file_without_to_flag(self):
        """Test error when output file is missing without --to flag."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", str(PLACES_PARQUET)])

        assert result.exit_code != 0
        assert "OUTPUT_FILE is required" in result.output

    def test_verbose_flag_works(self):
        """Test that --verbose flag is accepted and command succeeds."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["convert", str(PLACES_PARQUET), "--to", "geojson", "--verbose", "--no-rs"]
        )

        # Command should succeed with verbose flag
        assert result.exit_code == 0

        # Output should contain GeoJSON features (may also have debug output mixed in)
        assert '"type":"Feature"' in result.output
        assert '"geometry"' in result.output


@pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
class TestPipelineIntegration:
    """Tests for pipeline integration."""

    @pytest.mark.slow
    def test_extract_to_geojson_pipeline(self):
        """Test: gpio extract | gpio convert - --to geojson."""
        # This test uses subprocess to test actual piping
        result = subprocess.run(
            f"gpio extract --limit 3 {PLACES_PARQUET} | gpio convert - --to geojson --no-rs",
            shell=True,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0

        lines = [line for line in result.stdout.strip().split("\n") if line]
        assert len(lines) == 3

        for line in lines:
            feature = json.loads(line)
            assert feature["type"] == "Feature"


@pytest.mark.skipif(not BUILDINGS_PARQUET.exists(), reason="Test data not available")
class TestPolygonGeometries:
    """Tests with polygon geometries (buildings)."""

    def test_polygon_output(self, capsys):
        """Test GeoJSON output for polygon geometries."""
        convert_to_geojson(str(BUILDINGS_PARQUET), rs=False)

        captured = capsys.readouterr()
        lines = [line for line in captured.out.strip().split("\n") if line]

        assert len(lines) > 0

        # Check first feature has polygon geometry
        feature = json.loads(lines[0])
        assert feature["type"] == "Feature"
        assert feature["geometry"]["type"] in ["Polygon", "MultiPolygon"]
