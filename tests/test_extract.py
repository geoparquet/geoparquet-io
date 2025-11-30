"""
Comprehensive tests for the extract command.

Tests column selection, spatial filtering (bbox, geometry),
SQL filtering, and various input formats.
"""

import json
import os
import tempfile
from pathlib import Path

import click
import duckdb
import pytest

from geoparquet_io.core.extract import (
    build_column_selection,
    build_extract_query,
    build_spatial_filter,
    convert_geojson_to_wkt,
    extract,
    get_schema_columns,
    parse_bbox,
    parse_geometry_input,
    validate_columns,
)

# Test data paths
TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES_PARQUET = TEST_DATA_DIR / "places_test.parquet"
BUILDINGS_PARQUET = TEST_DATA_DIR / "buildings_test.parquet"

# Remote test file (Danish fiboa dataset)
REMOTE_PARQUET_URL = "https://data.source.coop/fiboa/data/dk/dk-2024.parquet"


class TestParseBbox:
    """Tests for parse_bbox function."""

    def test_valid_bbox(self):
        """Test parsing valid bbox string."""
        result = parse_bbox("-122.5,37.5,-122.0,38.0")
        assert result == (-122.5, 37.5, -122.0, 38.0)

    def test_bbox_with_spaces(self):
        """Test bbox with spaces around values."""
        result = parse_bbox(" -122.5 , 37.5 , -122.0 , 38.0 ")
        assert result == (-122.5, 37.5, -122.0, 38.0)

    def test_bbox_integer_values(self):
        """Test bbox with integer values."""
        result = parse_bbox("0,0,10,10")
        assert result == (0.0, 0.0, 10.0, 10.0)

    def test_bbox_negative_values(self):
        """Test bbox with negative values."""
        result = parse_bbox("-180,-90,180,90")
        assert result == (-180.0, -90.0, 180.0, 90.0)

    def test_bbox_wrong_count(self):
        """Test bbox with wrong number of values."""
        with pytest.raises(click.ClickException) as exc_info:
            parse_bbox("1,2,3")
        assert "Expected 4 values" in str(exc_info.value)

    def test_bbox_too_many_values(self):
        """Test bbox with too many values."""
        with pytest.raises(click.ClickException) as exc_info:
            parse_bbox("1,2,3,4,5")
        assert "Expected 4 values" in str(exc_info.value)

    def test_bbox_non_numeric(self):
        """Test bbox with non-numeric values."""
        with pytest.raises(click.ClickException) as exc_info:
            parse_bbox("a,b,c,d")
        assert "Expected numeric values" in str(exc_info.value)


class TestConvertGeojsonToWkt:
    """Tests for convert_geojson_to_wkt function."""

    def test_point(self):
        """Test converting GeoJSON point to WKT."""
        geojson = {"type": "Point", "coordinates": [0, 0]}
        result = convert_geojson_to_wkt(geojson)
        assert "POINT" in result.upper()
        assert "0" in result

    def test_polygon(self):
        """Test converting GeoJSON polygon to WKT."""
        geojson = {
            "type": "Polygon",
            "coordinates": [[[-1, -1], [-1, 1], [1, 1], [1, -1], [-1, -1]]],
        }
        result = convert_geojson_to_wkt(geojson)
        assert "POLYGON" in result.upper()

    def test_multipolygon(self):
        """Test converting GeoJSON multipolygon to WKT."""
        geojson = {
            "type": "MultiPolygon",
            "coordinates": [
                [[[-1, -1], [-1, 1], [1, 1], [1, -1], [-1, -1]]],
                [[[2, 2], [2, 3], [3, 3], [3, 2], [2, 2]]],
            ],
        }
        result = convert_geojson_to_wkt(geojson)
        assert "MULTIPOLYGON" in result.upper()


class TestParseGeometryInput:
    """Tests for parse_geometry_input function."""

    def test_inline_wkt_polygon(self):
        """Test parsing inline WKT polygon."""
        wkt = "POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))"
        result = parse_geometry_input(wkt)
        assert "POLYGON" in result.upper()

    def test_inline_wkt_point(self):
        """Test parsing inline WKT point."""
        wkt = "POINT(0 0)"
        result = parse_geometry_input(wkt)
        assert "POINT" in result.upper()

    def test_inline_wkt_linestring(self):
        """Test parsing inline WKT linestring."""
        wkt = "LINESTRING(0 0, 1 1, 2 2)"
        result = parse_geometry_input(wkt)
        assert "LINESTRING" in result.upper()

    def test_inline_geojson_point(self):
        """Test parsing inline GeoJSON point."""
        geojson = '{"type": "Point", "coordinates": [0, 0]}'
        result = parse_geometry_input(geojson)
        assert "POINT" in result.upper()

    def test_inline_geojson_polygon(self):
        """Test parsing inline GeoJSON polygon."""
        geojson = (
            '{"type": "Polygon", "coordinates": [[[-1, -1], [-1, 1], [1, 1], [1, -1], [-1, -1]]]}'
        )
        result = parse_geometry_input(geojson)
        assert "POLYGON" in result.upper()

    def test_geojson_feature(self):
        """Test parsing GeoJSON Feature."""
        feature = '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {}}'
        result = parse_geometry_input(feature)
        assert "POINT" in result.upper()

    def test_geojson_feature_collection_single(self):
        """Test parsing FeatureCollection with single feature."""
        fc = '{"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {}}]}'
        result = parse_geometry_input(fc)
        assert "POINT" in result.upper()

    def test_geojson_feature_collection_multiple_error(self):
        """Test FeatureCollection with multiple features raises error."""
        fc = json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}},
                    {"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 1]}},
                ],
            }
        )
        with pytest.raises(click.ClickException) as exc_info:
            parse_geometry_input(fc)
        assert "Multiple geometries" in str(exc_info.value)

    def test_geojson_feature_collection_multiple_use_first(self):
        """Test FeatureCollection with multiple features using first."""
        fc = json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}},
                    {"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 1]}},
                ],
            }
        )
        result = parse_geometry_input(fc, use_first=True)
        assert "POINT" in result.upper()

    def test_file_reference_geojson(self):
        """Test loading geometry from file with @ prefix."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as f:
            json.dump({"type": "Point", "coordinates": [0, 0]}, f)
            f.flush()
            try:
                result = parse_geometry_input(f"@{f.name}")
                assert "POINT" in result.upper()
            finally:
                os.unlink(f.name)

    def test_file_reference_wkt(self):
        """Test loading geometry from WKT file with @ prefix."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".wkt", delete=False) as f:
            f.write("POINT(0 0)")
            f.flush()
            try:
                result = parse_geometry_input(f"@{f.name}")
                assert "POINT" in result.upper()
            finally:
                os.unlink(f.name)

    def test_auto_detect_file(self):
        """Test auto-detecting file by extension."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as f:
            json.dump({"type": "Point", "coordinates": [0, 0]}, f)
            f.flush()
            try:
                result = parse_geometry_input(f.name)
                assert "POINT" in result.upper()
            finally:
                os.unlink(f.name)

    def test_file_not_found(self):
        """Test error when file not found."""
        with pytest.raises(click.ClickException) as exc_info:
            parse_geometry_input("@nonexistent_file.geojson")
        assert "not found" in str(exc_info.value)

    def test_invalid_geojson(self):
        """Test error on invalid GeoJSON."""
        with pytest.raises(click.ClickException) as exc_info:
            parse_geometry_input('{"invalid": "json"}')
        # Error could be about parsing or type field
        error_msg = str(exc_info.value).lower()
        assert "geojson" in error_msg or "type" in error_msg

    def test_empty_feature_collection(self):
        """Test error on empty FeatureCollection."""
        fc = '{"type": "FeatureCollection", "features": []}'
        with pytest.raises(click.ClickException) as exc_info:
            parse_geometry_input(fc)
        assert "empty" in str(exc_info.value).lower()


class TestBuildColumnSelection:
    """Tests for build_column_selection function."""

    def test_no_filters(self):
        """Test with no include/exclude filters."""
        all_cols = ["id", "name", "geometry", "bbox"]
        result = build_column_selection(all_cols, None, None, "geometry", "bbox")
        assert result == ["id", "name", "geometry", "bbox"]

    def test_include_cols(self):
        """Test with include columns."""
        all_cols = ["id", "name", "address", "geometry", "bbox"]
        result = build_column_selection(all_cols, ["name"], None, "geometry", "bbox")
        # Should include name + geometry + bbox (auto-included)
        assert "name" in result
        assert "geometry" in result
        assert "bbox" in result
        assert "id" not in result
        assert "address" not in result

    def test_include_cols_preserves_order(self):
        """Test that include cols preserves original column order."""
        all_cols = ["id", "name", "geometry", "bbox"]
        result = build_column_selection(all_cols, ["name", "id"], None, "geometry", "bbox")
        # Order should match all_cols order
        assert result.index("id") < result.index("name")

    def test_exclude_cols(self):
        """Test with exclude columns."""
        all_cols = ["id", "name", "address", "geometry", "bbox"]
        result = build_column_selection(all_cols, None, ["address"], "geometry", "bbox")
        assert "id" in result
        assert "name" in result
        assert "geometry" in result
        assert "bbox" in result
        assert "address" not in result

    def test_exclude_geometry(self):
        """Test excluding geometry column."""
        all_cols = ["id", "name", "geometry", "bbox"]
        result = build_column_selection(all_cols, None, ["geometry"], "geometry", "bbox")
        assert "geometry" not in result
        assert "bbox" in result

    def test_include_with_explicit_geometry_exclude(self):
        """Test include cols with explicit geometry exclusion."""
        all_cols = ["id", "name", "geometry", "bbox"]
        result = build_column_selection(all_cols, ["name"], ["geometry"], "geometry", "bbox")
        assert "name" in result
        assert "bbox" in result
        assert "geometry" not in result

    def test_no_bbox_column(self):
        """Test when no bbox column exists."""
        all_cols = ["id", "name", "geometry"]
        result = build_column_selection(all_cols, ["name"], None, "geometry", None)
        assert "name" in result
        assert "geometry" in result
        assert len(result) == 2


class TestValidateColumns:
    """Tests for validate_columns function."""

    def test_valid_columns(self):
        """Test with valid columns."""
        # Should not raise
        validate_columns(["id", "name"], ["id", "name", "geometry"], "--include-cols")

    def test_missing_columns(self):
        """Test with missing columns."""
        with pytest.raises(click.ClickException) as exc_info:
            validate_columns(["id", "nonexistent"], ["id", "name", "geometry"], "--include-cols")
        assert "nonexistent" in str(exc_info.value)
        assert "--include-cols" in str(exc_info.value)

    def test_none_columns(self):
        """Test with None columns (should not raise)."""
        validate_columns(None, ["id", "name"], "--include-cols")


class TestBuildSpatialFilter:
    """Tests for build_spatial_filter function."""

    def test_bbox_with_bbox_column(self):
        """Test bbox filter with bbox column available."""
        bbox_info = {"has_bbox_column": True, "bbox_column_name": "bbox"}
        result = build_spatial_filter((-1, -1, 1, 1), None, bbox_info, "geometry")
        assert '"bbox".xmax' in result
        assert '"bbox".xmin' in result
        assert '"bbox".ymax' in result
        assert '"bbox".ymin' in result

    def test_bbox_without_bbox_column(self):
        """Test bbox filter without bbox column."""
        bbox_info = {"has_bbox_column": False}
        result = build_spatial_filter((-1, -1, 1, 1), None, bbox_info, "geometry")
        assert "ST_Intersects" in result
        assert "ST_MakeEnvelope" in result

    def test_geometry_filter(self):
        """Test geometry WKT filter."""
        bbox_info = {"has_bbox_column": False}
        result = build_spatial_filter(
            None, "POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))", bbox_info, "geometry"
        )
        assert "ST_Intersects" in result
        assert "ST_GeomFromText" in result

    def test_bbox_and_geometry_combined(self):
        """Test bbox and geometry filters combined."""
        bbox_info = {"has_bbox_column": True, "bbox_column_name": "bbox"}
        result = build_spatial_filter(
            (-1, -1, 1, 1),
            "POLYGON((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))",
            bbox_info,
            "geometry",
        )
        assert "bbox" in result
        assert "ST_GeomFromText" in result
        assert " AND " in result

    def test_no_spatial_filter(self):
        """Test with no spatial filters."""
        bbox_info = {"has_bbox_column": False}
        result = build_spatial_filter(None, None, bbox_info, "geometry")
        assert result is None


class TestBuildExtractQuery:
    """Tests for build_extract_query function."""

    def test_simple_query(self):
        """Test simple query with no filters."""
        result = build_extract_query("input.parquet", ["id", "name", "geometry"], None, None)
        assert 'SELECT "id", "name", "geometry"' in result
        assert "FROM read_parquet('input.parquet')" in result
        assert "WHERE" not in result

    def test_with_spatial_filter(self):
        """Test query with spatial filter."""
        spatial_filter = '"bbox".xmax >= -1'
        result = build_extract_query("input.parquet", ["id", "geometry"], spatial_filter, None)
        assert "WHERE" in result
        assert spatial_filter in result

    def test_with_where_clause(self):
        """Test query with WHERE clause."""
        result = build_extract_query("input.parquet", ["id", "geometry"], None, "id > 100")
        assert "WHERE" in result
        assert "id > 100" in result

    def test_with_both_filters(self):
        """Test query with both spatial and WHERE filters."""
        spatial_filter = '"bbox".xmax >= -1'
        result = build_extract_query(
            "input.parquet", ["id", "geometry"], spatial_filter, "id > 100"
        )
        assert "WHERE" in result
        assert spatial_filter in result
        assert "id > 100" in result
        assert " AND " in result


class TestGetSchemaColumns:
    """Tests for get_schema_columns function."""

    def test_local_file(self):
        """Test getting columns from local file."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        columns = get_schema_columns(str(PLACES_PARQUET))
        assert "geometry" in columns
        assert "name" in columns
        assert len(columns) > 0


class TestExtractIntegration:
    """Integration tests for the extract function."""

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            yield f.name
        # Cleanup after test
        if os.path.exists(f.name):
            os.unlink(f.name)

    def test_extract_all_columns(self, output_file):
        """Test extracting all columns."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        extract(str(PLACES_PARQUET), output_file)

        # Verify output
        assert os.path.exists(output_file)
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        result = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()
        assert result[0] == 766  # Original row count

    def test_extract_include_cols(self, output_file):
        """Test extracting with include columns."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        extract(str(PLACES_PARQUET), output_file, include_cols="name,address")

        # Verify output has only selected columns + geometry + bbox
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        result = con.execute(f"DESCRIBE SELECT * FROM '{output_file}'").fetchall()
        columns = [row[0] for row in result]
        assert "name" in columns
        assert "address" in columns
        assert "geometry" in columns
        assert "bbox" in columns
        assert "fsq_place_id" not in columns

    def test_extract_exclude_cols(self, output_file):
        """Test extracting with exclude columns."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        extract(str(PLACES_PARQUET), output_file, exclude_cols="placemaker_url,fsq_place_id")

        # Verify output doesn't have excluded columns
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        result = con.execute(f"DESCRIBE SELECT * FROM '{output_file}'").fetchall()
        columns = [row[0] for row in result]
        assert "name" in columns
        assert "geometry" in columns
        assert "placemaker_url" not in columns
        assert "fsq_place_id" not in columns

    def test_extract_mutually_exclusive_error(self, output_file):
        """Test that include and exclude are mutually exclusive."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        with pytest.raises(click.ClickException) as exc_info:
            extract(str(PLACES_PARQUET), output_file, include_cols="name", exclude_cols="address")
        assert "mutually exclusive" in str(exc_info.value)

    def test_extract_bbox_filter(self, output_file):
        """Test extracting with bbox filter."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        # Bbox that covers part of the data
        extract(str(PLACES_PARQUET), output_file, bbox="-0.5,10,0.5,11")

        # Verify fewer rows
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        result = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()
        assert result[0] < 766  # Should be filtered
        assert result[0] > 0  # But not empty

    def test_extract_geometry_filter_wkt(self, output_file):
        """Test extracting with WKT geometry filter."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        wkt = "POLYGON((-0.5 10, -0.5 11, 0.5 11, 0.5 10, -0.5 10))"
        extract(str(PLACES_PARQUET), output_file, geometry=wkt)

        # Verify filtered rows
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        result = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()
        assert result[0] < 766
        assert result[0] > 0

    def test_extract_geometry_filter_geojson(self, output_file):
        """Test extracting with GeoJSON geometry filter."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        geojson = (
            '{"type":"Polygon","coordinates":[[[-0.5,10],[-0.5,11],[0.5,11],[0.5,10],[-0.5,10]]]}'
        )
        extract(str(PLACES_PARQUET), output_file, geometry=geojson)

        # Verify filtered rows
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        result = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()
        assert result[0] < 766
        assert result[0] > 0

    def test_extract_where_clause(self, output_file):
        """Test extracting with WHERE clause."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        extract(str(PLACES_PARQUET), output_file, where="name LIKE '%Hotel%'")

        # Verify filtered rows
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        result = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()
        assert result[0] < 766
        assert result[0] > 0

    def test_extract_combined_filters(self, output_file):
        """Test extracting with combined filters."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        extract(
            str(PLACES_PARQUET),
            output_file,
            include_cols="name,address",
            bbox="-0.5,10,0.5,11",
            where="name LIKE '%Hotel%'",
        )

        # Verify output
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        # Check row count (should be very few with all filters)
        result = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()
        assert result[0] < 766

        # Check columns
        result = con.execute(f"DESCRIBE SELECT * FROM '{output_file}'").fetchall()
        columns = [row[0] for row in result]
        assert "name" in columns
        assert "address" in columns
        assert "geometry" in columns
        assert "fsq_place_id" not in columns

    def test_extract_dry_run(self, output_file, capsys):
        """Test dry run mode."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        # Remove the fixture-created file to test that dry run doesn't create it
        if os.path.exists(output_file):
            os.unlink(output_file)

        extract(str(PLACES_PARQUET), output_file, include_cols="name", dry_run=True)

        # File should not be created
        assert not os.path.exists(output_file)

        # Output should contain SQL
        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out
        assert "SELECT" in captured.out

    def test_extract_invalid_column(self, output_file):
        """Test error on invalid column name."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        with pytest.raises(click.ClickException) as exc_info:
            extract(str(PLACES_PARQUET), output_file, include_cols="nonexistent_column")
        assert "not found" in str(exc_info.value).lower()

    def test_extract_empty_result(self, output_file, capsys):
        """Test extraction that results in zero rows."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        # Use a bbox that doesn't intersect any data
        extract(str(PLACES_PARQUET), output_file, bbox="100,100,101,101")

        # File should be created but with 0 rows
        assert os.path.exists(output_file)
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        result = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()
        assert result[0] == 0

        # Warning should be displayed
        captured = capsys.readouterr()
        assert "Warning" in captured.out or "0 rows" in captured.out

    def test_extract_preserves_metadata(self, output_file):
        """Test that GeoParquet metadata is preserved."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        extract(str(PLACES_PARQUET), output_file)

        # Check GeoParquet metadata
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(output_file)
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata

        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        assert "primary_column" in geo_meta
        assert geo_meta["primary_column"] == "geometry"


class TestExtractCLI:
    """Tests for extract CLI command."""

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            yield f.name
        if os.path.exists(f.name):
            os.unlink(f.name)

    def test_cli_help(self):
        """Test CLI help output."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import extract as extract_cmd

        runner = CliRunner()
        result = runner.invoke(extract_cmd, ["--help"])
        assert result.exit_code == 0
        assert "Extract columns and rows" in result.output
        assert "--include-cols" in result.output
        assert "--exclude-cols" in result.output
        assert "--bbox" in result.output
        assert "--geometry" in result.output
        assert "--where" in result.output

    def test_cli_basic(self, output_file):
        """Test basic CLI invocation."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        from click.testing import CliRunner

        from geoparquet_io.cli.main import extract as extract_cmd

        runner = CliRunner()
        result = runner.invoke(extract_cmd, [str(PLACES_PARQUET), output_file])
        assert result.exit_code == 0
        assert os.path.exists(output_file)

    def test_cli_include_cols(self, output_file):
        """Test CLI with include-cols."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        from click.testing import CliRunner

        from geoparquet_io.cli.main import extract as extract_cmd

        runner = CliRunner()
        result = runner.invoke(
            extract_cmd, [str(PLACES_PARQUET), output_file, "--include-cols", "name,address"]
        )
        assert result.exit_code == 0

        # Verify columns
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        cols_result = con.execute(f"DESCRIBE SELECT * FROM '{output_file}'").fetchall()
        columns = [row[0] for row in cols_result]
        assert "name" in columns
        assert "geometry" in columns
        assert "fsq_place_id" not in columns

    def test_cli_bbox(self, output_file):
        """Test CLI with bbox filter."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        from click.testing import CliRunner

        from geoparquet_io.cli.main import extract as extract_cmd

        runner = CliRunner()
        result = runner.invoke(
            extract_cmd, [str(PLACES_PARQUET), output_file, "--bbox", "-0.5,10,0.5,11"]
        )
        assert result.exit_code == 0

        # Verify filtered
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        count = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[0]
        assert count < 766
        assert count > 0

    def test_cli_dry_run(self, output_file):
        """Test CLI dry run."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        # Remove the fixture-created file to test that dry run doesn't create it
        if os.path.exists(output_file):
            os.unlink(output_file)

        from click.testing import CliRunner

        from geoparquet_io.cli.main import extract as extract_cmd

        runner = CliRunner()
        result = runner.invoke(extract_cmd, [str(PLACES_PARQUET), output_file, "--dry-run"])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert not os.path.exists(output_file)


@pytest.mark.slow
class TestExtractRemote:
    """Tests for extract with remote files (marked slow - skip in CI)."""

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            yield f.name
        if os.path.exists(f.name):
            os.unlink(f.name)

    def test_remote_file_bbox(self, output_file):
        """Test extracting from remote file with bbox filter."""
        # Use Danish fiboa dataset with UTM coordinates
        extract(
            REMOTE_PARQUET_URL,
            output_file,
            bbox="500000,6200000,550000,6250000",
            include_cols="id,crop:name",
        )

        assert os.path.exists(output_file)
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        count = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[0]
        assert count > 0
        assert count < 617941  # Less than total rows
