"""
Tests for GeoJSON conversion.

Tests verify that gpio convert geojson:
- Outputs valid GeoJSON Features in streaming mode
- Includes RFC 8142 record separators by default
- Supports --no-rs flag to disable separators
- Works with both file input and stdin (pipeline)
- Writes valid GeoJSON FeatureCollection to file
- Supports --precision, --write-bbox, --id-field options
"""

import json
import tempfile
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.geojson_stream import (
    WGS84_CRS,
    _build_feature_query,
    _get_property_columns,
    _needs_reprojection,
    _quote_identifier,
    convert_to_geojson,
    convert_to_geojson_stream,
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

    def test_with_write_bbox(self):
        """Test query includes bbox when requested."""
        query = _build_feature_query("test_table", "geometry", ["name"], write_bbox=True)
        assert "ST_XMin" in query
        assert "ST_YMin" in query
        assert "ST_XMax" in query
        assert "ST_YMax" in query
        assert "bbox" in query

    def test_with_id_field(self):
        """Test query includes id when requested."""
        query = _build_feature_query("test_table", "geometry", ["name"], id_field="osm_id")
        assert '"osm_id"' in query
        assert '"id":' in query


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
class TestConvertToGeoJSONStream:
    """Tests for streaming GeoJSON output."""

    def test_basic_output(self, capsys):
        """Test basic GeoJSON output."""
        count = convert_to_geojson_stream(str(PLACES_PARQUET), rs=False)

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
        convert_to_geojson_stream(str(PLACES_PARQUET), rs=True)

        captured = capsys.readouterr()
        # Check for record separator character
        assert "\x1e" in captured.out

    def test_no_rs_separators(self, capsys):
        """Test disabling RS separators."""
        convert_to_geojson_stream(str(PLACES_PARQUET), rs=False)

        captured = capsys.readouterr()
        # No record separator character
        assert "\x1e" not in captured.out

    def test_valid_geojson_features(self, capsys):
        """Test that all output lines are valid GeoJSON Features."""
        convert_to_geojson_stream(str(PLACES_PARQUET), rs=False)

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
class TestConvertGeoJSONCLI:
    """Tests for gpio convert geojson CLI."""

    def test_basic_geojson_output(self):
        """Test gpio convert geojson command."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", "geojson", str(PLACES_PARQUET)])

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
        result = runner.invoke(cli, ["convert", "geojson", str(PLACES_PARQUET), "--no-rs"])

        assert result.exit_code == 0
        assert "\x1e" not in result.output

    def test_rs_enabled_by_default(self):
        """Test RS separators are enabled by default."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", "geojson", str(PLACES_PARQUET)])

        assert result.exit_code == 0
        assert "\x1e" in result.output

    def test_precision_option(self):
        """Test --precision flag is accepted."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["convert", "geojson", str(PLACES_PARQUET), "--no-rs", "--precision", "5"]
        )

        assert result.exit_code == 0
        # Verify output is valid GeoJSON
        lines = [line for line in result.output.strip().split("\n") if line]
        if lines:
            feature = json.loads(lines[0])
            assert feature["type"] == "Feature"

    def test_verbose_flag_works(self):
        """Test that --verbose flag is accepted and command succeeds."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["convert", "geojson", str(PLACES_PARQUET), "--verbose", "--no-rs"]
        )

        # Command should succeed with verbose flag
        assert result.exit_code == 0

        # Output should contain GeoJSON features (may also have debug output mixed in)
        assert '"type":"Feature"' in result.output
        assert '"geometry"' in result.output

    def test_write_to_file(self):
        """Test writing to a GeoJSON file."""
        runner = CliRunner()
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.geojson"

        try:
            result = runner.invoke(
                cli, ["convert", "geojson", str(PLACES_PARQUET), str(output_path)]
            )

            assert result.exit_code == 0
            assert output_path.exists()

            # Verify the file contains valid GeoJSON
            with open(output_path, encoding="utf-8") as f:
                geojson = json.load(f)
            assert geojson["type"] == "FeatureCollection"
            assert "features" in geojson
            assert len(geojson["features"]) > 0
            assert geojson["features"][0]["type"] == "Feature"
        finally:
            if output_path.exists():
                output_path.unlink()


@pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
class TestPipelineIntegration:
    """Tests for pipeline integration."""

    def test_geometry_column_detection_from_arrow_table(self):
        """Test geometry column detection from Arrow table schema."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.streaming import find_geometry_column_from_table

        # Read test data
        table = pq.read_table(str(PLACES_PARQUET))

        # Find geometry column from the table
        geom_col = find_geometry_column_from_table(table)
        assert geom_col is not None

    def test_no_geometry_column_raises_error(self):
        """Test that missing geometry column raises ValueError with helpful message."""
        from unittest.mock import patch

        import pyarrow as pa

        from geoparquet_io.core.geojson_stream import _convert_from_stream

        # Create a table without geometry column
        table_no_geom = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        # Mock read_arrow_stream at the source module
        with patch("geoparquet_io.core.streaming.read_arrow_stream", return_value=table_no_geom):
            with pytest.raises(ValueError) as exc_info:
                _convert_from_stream()

            # Check error message contains helpful information
            error_msg = str(exc_info.value)
            assert "No geometry column found" in error_msg
            assert "id" in error_msg  # Available columns listed
            assert "name" in error_msg

    def test_arrow_stream_to_geojson_conversion(self):
        """Test full conversion from Arrow table to GeoJSON."""
        from unittest.mock import patch

        import pyarrow.parquet as pq

        from geoparquet_io.core.geojson_stream import _convert_from_stream

        # Read test data
        table = pq.read_table(str(PLACES_PARQUET))
        table = table.slice(0, 3)  # Small subset for testing

        # Create temp output file
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.geojson"

        try:
            # Mock read_arrow_stream at the source module
            with patch("geoparquet_io.core.streaming.read_arrow_stream", return_value=table):
                count = _convert_from_stream(output_path=str(output_path))

            assert count == 3
            assert output_path.exists()

            # Verify valid GeoJSON FeatureCollection
            with open(output_path, encoding="utf-8") as f:
                geojson = json.load(f)

            assert geojson["type"] == "FeatureCollection"
            assert len(geojson["features"]) == 3
            assert geojson["features"][0]["type"] == "Feature"
            assert "geometry" in geojson["features"][0]
        finally:
            if output_path.exists():
                output_path.unlink()


@pytest.mark.skipif(not BUILDINGS_PARQUET.exists(), reason="Test data not available")
class TestPolygonGeometries:
    """Tests with polygon geometries (buildings)."""

    def test_polygon_output(self, capsys):
        """Test GeoJSON output for polygon geometries."""
        convert_to_geojson_stream(str(BUILDINGS_PARQUET), rs=False)

        captured = capsys.readouterr()
        lines = [line for line in captured.out.strip().split("\n") if line]

        assert len(lines) > 0

        # Check first feature has polygon geometry
        feature = json.loads(lines[0])
        assert feature["type"] == "Feature"
        assert feature["geometry"]["type"] in ["Polygon", "MultiPolygon"]


@pytest.fixture
def output_file():
    """Create a temp output file path."""
    tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.geojson"
    yield str(tmp_path)
    if tmp_path.exists():
        tmp_path.unlink()


@pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
class TestConvertToGeoJSONFile:
    """Tests for file output mode."""

    def test_writes_valid_geojson_file(self, output_file):
        """Test that file output produces valid GeoJSON FeatureCollection."""
        count = convert_to_geojson(str(PLACES_PARQUET), output_path=output_file)

        assert count > 0
        assert Path(output_file).exists()

        with open(output_file, encoding="utf-8") as f:
            geojson = json.load(f)

        assert geojson["type"] == "FeatureCollection"
        assert "features" in geojson
        assert len(geojson["features"]) == count

    def test_routing_based_on_output(self, output_file, capsys):
        """Test that convert_to_geojson routes correctly based on output."""
        # Without output - streams to stdout
        count_stream = convert_to_geojson(str(PLACES_PARQUET), rs=False)
        captured = capsys.readouterr()
        assert len(captured.out.strip().split("\n")) == count_stream

        # With output - writes to file
        count_file = convert_to_geojson(str(PLACES_PARQUET), output_path=output_file)
        assert Path(output_file).exists()
        assert count_file > 0


class TestNeedsReprojection:
    """Tests for CRS detection and reprojection logic."""

    def test_wgs84_epsg4326_no_reprojection(self):
        """WGS84 (EPSG:4326) should not need reprojection."""
        assert _needs_reprojection("EPSG:4326") is False

    def test_wgs84_ogc_crs84_no_reprojection(self):
        """OGC:CRS84 is WGS84 and should not need reprojection."""
        assert _needs_reprojection("OGC:CRS84") is False

    def test_wgs84_crs84_no_reprojection(self):
        """CRS84 variant should not need reprojection."""
        assert _needs_reprojection("CRS84") is False

    def test_wgs84_case_insensitive(self):
        """WGS84 detection should be case insensitive."""
        assert _needs_reprojection("epsg:4326") is False
        assert _needs_reprojection("EPSG:4326") is False

    def test_other_crs_needs_reprojection(self):
        """Non-WGS84 CRS should need reprojection."""
        assert _needs_reprojection("EPSG:32610") is True
        assert _needs_reprojection("EPSG:3857") is True
        assert _needs_reprojection("EPSG:28992") is True

    def test_none_crs_no_reprojection(self):
        """None CRS should not need reprojection (assume WGS84)."""
        assert _needs_reprojection(None) is False


class TestBuildFeatureQueryWithReprojection:
    """Tests for query building with CRS reprojection."""

    def test_query_without_reprojection(self):
        """Test query without source_crs produces no ST_Transform."""
        query = _build_feature_query("test_table", "geometry", ["name"], source_crs=None)
        assert "ST_Transform" not in query
        assert "ST_AsGeoJSON" in query

    def test_query_with_reprojection(self):
        """Test query with source_crs includes ST_Transform."""
        query = _build_feature_query("test_table", "geometry", ["name"], source_crs="EPSG:32610")
        assert "ST_Transform" in query
        assert "EPSG:32610" in query
        assert WGS84_CRS in query

    def test_query_with_bbox_and_reprojection(self):
        """Test query with both bbox and reprojection."""
        query = _build_feature_query(
            "test_table",
            "geometry",
            ["name"],
            write_bbox=True,
            source_crs="EPSG:3857",
        )
        assert "ST_Transform" in query
        assert "ST_Envelope" in query or "ST_XMin" in query


class TestKeepCrsFlag:
    """Tests for --keep-crs flag behavior."""

    def test_keep_crs_flag_exists(self):
        """Test that --keep-crs flag is recognized by CLI."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", "geojson", "--help"])
        assert result.exit_code == 0
        assert "--keep-crs" in result.output

    def test_keep_crs_in_streaming_mode(self, capsys):
        """Test keep_crs parameter works in streaming mode."""
        # Should not raise with keep_crs=True
        count = convert_to_geojson_stream(
            str(PLACES_PARQUET),
            rs=False,
            keep_crs=True,
        )
        assert count > 0

    def test_keep_crs_in_file_mode(self, output_file):
        """Test keep_crs parameter works in file mode."""
        count = convert_to_geojson(
            str(PLACES_PARQUET),
            output_path=output_file,
            keep_crs=True,
        )
        assert count > 0
        assert Path(output_file).exists()


class TestWgs84Constant:
    """Tests for WGS84_CRS constant."""

    def test_wgs84_crs_value(self):
        """Test WGS84_CRS is correctly defined."""
        assert WGS84_CRS == "EPSG:4326"
