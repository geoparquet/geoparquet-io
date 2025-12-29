"""
Comprehensive tests for the extract command.

Tests column selection, spatial filtering (bbox, geometry),
SQL filtering, and various input formats.
"""

import json
import os
import tempfile
import uuid
from pathlib import Path

import click
import duckdb
import pyarrow as pa
import pytest

from geoparquet_io.core.extract import (
    build_column_selection,
    build_extract_query,
    build_spatial_filter,
    convert_geojson_to_wkt,
    extract,
    extract_table,
    get_schema_columns,
    is_geographic_crs,
    looks_like_latlong_bbox,
    parse_bbox,
    parse_geometry_input,
    validate_columns,
    validate_where_clause,
)
from tests.conftest import safe_unlink

# Test data paths
TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES_PARQUET = TEST_DATA_DIR / "places_test.parquet"
BUILDINGS_PARQUET = TEST_DATA_DIR / "buildings_test.parquet"

# Remote test file (Danish fiboa dataset)
REMOTE_PARQUET_URL = "https://data.source.coop/fiboa/data/dk/dk-2024.parquet"


class TestLooksLikeLatLongBbox:
    """Tests for looks_like_latlong_bbox function."""

    def test_valid_latlong_bbox(self):
        """Test valid lat/long bbox returns True."""
        assert looks_like_latlong_bbox((-122.5, 37.5, -122.0, 38.0)) is True
        assert looks_like_latlong_bbox((-180.0, -90.0, 180.0, 90.0)) is True
        assert looks_like_latlong_bbox((0.0, 0.0, 0.0, 0.0)) is True

    def test_out_of_range_bbox(self):
        """Test out of range bbox returns False."""
        assert looks_like_latlong_bbox((500000, 500000, 600000, 600000)) is False
        assert looks_like_latlong_bbox((-200, 0, 200, 50)) is False


class TestIsGeographicCrs:
    """Tests for is_geographic_crs function."""

    def test_none_returns_none(self):
        """Test None CRS returns None."""
        assert is_geographic_crs(None) is None

    def test_string_geographic_crs(self):
        """Test string CRS codes for geographic."""
        assert is_geographic_crs("EPSG:4326") is True
        assert is_geographic_crs("OGC:CRS84") is True
        assert is_geographic_crs("crs84") is True

    def test_string_unknown_crs(self):
        """Test unknown string CRS returns None."""
        assert is_geographic_crs("EPSG:3857") is None

    def test_dict_geographic_crs(self):
        """Test dict CRS for geographic type."""
        geo_crs = {"type": "GeographicCRS", "name": "WGS 84"}
        assert is_geographic_crs(geo_crs) is True

    def test_dict_projected_crs(self):
        """Test dict CRS for projected type."""
        proj_crs = {"type": "ProjectedCRS", "name": "Web Mercator"}
        assert is_geographic_crs(proj_crs) is False

    def test_dict_with_epsg_code(self):
        """Test dict CRS with EPSG code."""
        crs_with_code = {"id": {"code": 4326, "authority": "EPSG"}}
        assert is_geographic_crs(crs_with_code) is True

    def test_dict_without_type(self):
        """Test dict CRS without type returns None."""
        unknown_crs = {"name": "Unknown"}
        assert is_geographic_crs(unknown_crs) is None


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

    def test_bbox_reversed_x_coordinates(self):
        """Test bbox with reversed x coordinates (xmax < xmin)."""
        with pytest.raises(click.ClickException) as exc_info:
            parse_bbox("10,0,5,10")  # xmin=10, xmax=5 is invalid
        assert "reversed" in str(exc_info.value).lower()
        assert "xmin" in str(exc_info.value).lower()

    def test_bbox_reversed_y_coordinates(self):
        """Test bbox with reversed y coordinates (ymax < ymin)."""
        with pytest.raises(click.ClickException) as exc_info:
            parse_bbox("0,10,10,5")  # ymin=10, ymax=5 is invalid
        assert "reversed" in str(exc_info.value).lower()
        assert "ymin" in str(exc_info.value).lower()

    def test_bbox_equal_coordinates_valid(self):
        """Test bbox with equal min/max (point) is valid."""
        result = parse_bbox("5,5,5,5")
        assert result == (5.0, 5.0, 5.0, 5.0)


class TestValidateWhereClause:
    """Tests for validate_where_clause function."""

    def test_valid_where_clause(self):
        """Test that valid WHERE clauses pass validation."""
        # Should not raise any exception
        validate_where_clause("name LIKE '%Hotel%'")
        validate_where_clause("id > 100 AND status = 'active'")
        validate_where_clause("category IN ('food', 'lodging')")
        validate_where_clause("created_at >= '2024-01-01'")

    def test_drop_keyword_blocked(self):
        """Test that DROP keyword is blocked."""
        with pytest.raises(click.ClickException) as exc_info:
            validate_where_clause("1=1; DROP TABLE users; --")
        assert "DROP" in str(exc_info.value)
        assert "dangerous" in str(exc_info.value).lower()

    def test_delete_keyword_blocked(self):
        """Test that DELETE keyword is blocked."""
        with pytest.raises(click.ClickException) as exc_info:
            validate_where_clause("DELETE FROM users WHERE 1=1")
        assert "DELETE" in str(exc_info.value)

    def test_insert_keyword_blocked(self):
        """Test that INSERT keyword is blocked."""
        with pytest.raises(click.ClickException) as exc_info:
            validate_where_clause("1=1; INSERT INTO users VALUES (1, 'hacker')")
        assert "INSERT" in str(exc_info.value)

    def test_update_keyword_blocked(self):
        """Test that UPDATE keyword is blocked."""
        with pytest.raises(click.ClickException) as exc_info:
            validate_where_clause("1=1; UPDATE users SET admin=true")
        assert "UPDATE" in str(exc_info.value)

    def test_create_keyword_blocked(self):
        """Test that CREATE keyword is blocked."""
        with pytest.raises(click.ClickException) as exc_info:
            validate_where_clause("1=1; CREATE TABLE evil ()")
        assert "CREATE" in str(exc_info.value)

    def test_column_name_with_keyword_allowed(self):
        """Test that column names containing keywords are allowed."""
        # These should NOT raise exceptions because the keywords are
        # part of column names, not standalone keywords
        validate_where_clause("updated_at > '2024-01-01'")
        validate_where_clause("created_by = 'admin'")
        validate_where_clause("deletion_flag = false")

    def test_multiple_dangerous_keywords(self):
        """Test error message includes all found dangerous keywords."""
        with pytest.raises(click.ClickException) as exc_info:
            validate_where_clause("DROP TABLE x; DELETE FROM y")
        error_msg = str(exc_info.value)
        assert "DROP" in error_msg
        assert "DELETE" in error_msg


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
        # Use unique filename to avoid Windows file locking issues
        tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.geojson"
        try:
            tmp_path.write_text(json.dumps({"type": "Point", "coordinates": [0, 0]}))
            result = parse_geometry_input(f"@{tmp_path}")
            assert "POINT" in result.upper()
        finally:
            safe_unlink(tmp_path)

    def test_file_reference_wkt(self):
        """Test loading geometry from WKT file with @ prefix."""
        # Use unique filename to avoid Windows file locking issues
        tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.wkt"
        try:
            tmp_path.write_text("POINT(0 0)")
            result = parse_geometry_input(f"@{tmp_path}")
            assert "POINT" in result.upper()
        finally:
            safe_unlink(tmp_path)

    def test_auto_detect_file(self):
        """Test auto-detecting file by extension."""
        # Use unique filename to avoid Windows file locking issues
        tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.geojson"
        try:
            tmp_path.write_text(json.dumps({"type": "Point", "coordinates": [0, 0]}))
            result = parse_geometry_input(str(tmp_path))
            assert "POINT" in result.upper()
        finally:
            safe_unlink(tmp_path)

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
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        input_path = str(PLACES_PARQUET)
        result = build_extract_query(input_path, ["id", "name", "geometry"], None, None)
        assert 'SELECT "id", "name", "geometry"' in result
        assert f"FROM read_parquet('{input_path}')" in result
        assert "WHERE" not in result

    def test_with_spatial_filter(self):
        """Test query with spatial filter."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        input_path = str(PLACES_PARQUET)
        spatial_filter = '"bbox".xmax >= -1'
        result = build_extract_query(input_path, ["id", "geometry"], spatial_filter, None)
        assert "WHERE" in result
        assert spatial_filter in result

    def test_with_where_clause(self):
        """Test query with WHERE clause."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        input_path = str(PLACES_PARQUET)
        result = build_extract_query(input_path, ["id", "geometry"], None, "id > 100")
        assert "WHERE" in result
        assert "id > 100" in result

    def test_with_both_filters(self):
        """Test query with both spatial and WHERE filters."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        input_path = str(PLACES_PARQUET)
        spatial_filter = '"bbox".xmax >= -1'
        result = build_extract_query(input_path, ["id", "geometry"], spatial_filter, "id > 100")
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
        """Create a temporary output file path.

        Uses a unique filename instead of NamedTemporaryFile to avoid
        Windows file locking issues with DuckDB.
        """
        # Generate unique path without creating the file
        tmp_path = Path(tempfile.gettempdir()) / f"test_output_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_extract_all_columns(self, output_file):
        """Test extracting all columns."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        extract(str(PLACES_PARQUET), output_file)

        # Verify output
        assert os.path.exists(output_file)
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            result = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()
            assert result[0] == 766  # Original row count
        finally:
            con.close()

    def test_extract_include_cols(self, output_file):
        """Test extracting with include columns."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        extract(str(PLACES_PARQUET), output_file, include_cols="name,address")

        # Verify output has only selected columns + geometry + bbox
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            result = con.execute(f"DESCRIBE SELECT * FROM '{output_file}'").fetchall()
            columns = [row[0] for row in result]
            assert "name" in columns
            assert "address" in columns
            assert "geometry" in columns
            assert "bbox" in columns
            assert "fsq_place_id" not in columns
        finally:
            con.close()

    def test_extract_exclude_cols(self, output_file):
        """Test extracting with exclude columns."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        extract(str(PLACES_PARQUET), output_file, exclude_cols="placemaker_url,fsq_place_id")

        # Verify output doesn't have excluded columns
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            result = con.execute(f"DESCRIBE SELECT * FROM '{output_file}'").fetchall()
            columns = [row[0] for row in result]
            assert "name" in columns
            assert "geometry" in columns
            assert "placemaker_url" not in columns
            assert "fsq_place_id" not in columns
        finally:
            con.close()

    def test_extract_include_and_exclude_geometry(self, output_file):
        """Test that include and exclude can be combined to exclude geometry/bbox."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        # Use include_cols for columns, exclude geometry
        extract(
            str(PLACES_PARQUET), output_file, include_cols="name,address", exclude_cols="geometry"
        )

        # Verify geometry was excluded but other columns present
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            result = con.execute(f"DESCRIBE SELECT * FROM '{output_file}'").fetchall()
            columns = [row[0] for row in result]
            assert "name" in columns
            assert "address" in columns
            assert "geometry" not in columns
        finally:
            con.close()

    def test_extract_overlap_non_special_error(self, output_file):
        """Test that non-geometry/bbox columns cannot be in both include and exclude."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        with pytest.raises(click.ClickException) as exc_info:
            extract(
                str(PLACES_PARQUET), output_file, include_cols="name,address", exclude_cols="name"
            )
        assert "cannot be in both" in str(exc_info.value).lower()

    def test_extract_bbox_filter(self, output_file):
        """Test extracting with bbox filter."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        # Bbox that covers part of the data
        extract(str(PLACES_PARQUET), output_file, bbox="-0.5,10,0.5,11")

        # Verify fewer rows
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            result = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()
            assert result[0] < 766  # Should be filtered
            assert result[0] > 0  # But not empty
        finally:
            con.close()

    def test_extract_geometry_filter_wkt(self, output_file):
        """Test extracting with WKT geometry filter."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        wkt = "POLYGON((-0.5 10, -0.5 11, 0.5 11, 0.5 10, -0.5 10))"
        extract(str(PLACES_PARQUET), output_file, geometry=wkt)

        # Verify filtered rows
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            result = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()
            assert result[0] < 766
            assert result[0] > 0
        finally:
            con.close()

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
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            result = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()
            assert result[0] < 766
            assert result[0] > 0
        finally:
            con.close()

    def test_extract_where_clause(self, output_file):
        """Test extracting with WHERE clause."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        extract(str(PLACES_PARQUET), output_file, where="name LIKE '%Hotel%'")

        # Verify filtered rows
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            result = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()
            assert result[0] < 766
            assert result[0] > 0
        finally:
            con.close()

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
        try:
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
        finally:
            con.close()

    def test_extract_dry_run(self, output_file, caplog):
        """Test dry run mode."""
        import logging

        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        # Remove the fixture-created file to test that dry run doesn't create it
        if os.path.exists(output_file):
            os.unlink(output_file)

        with caplog.at_level(logging.DEBUG):
            extract(str(PLACES_PARQUET), output_file, include_cols="name", dry_run=True)

        # File should not be created
        assert not os.path.exists(output_file)

        # Output should contain SQL - check log messages
        log_text = " ".join(record.message for record in caplog.records)
        assert "DRY RUN" in log_text
        assert "SELECT" in log_text

    def test_extract_invalid_column(self, output_file):
        """Test error on invalid column name."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        with pytest.raises(click.ClickException) as exc_info:
            extract(str(PLACES_PARQUET), output_file, include_cols="nonexistent_column")
        assert "not found" in str(exc_info.value).lower()

    def test_extract_empty_result(self, output_file, caplog):
        """Test extraction that results in zero rows."""
        import logging

        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        with caplog.at_level(logging.DEBUG):
            # Use a bbox that doesn't intersect any data
            extract(str(PLACES_PARQUET), output_file, bbox="100,100,101,101")

        # File should be created but with 0 rows
        assert os.path.exists(output_file)
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            result = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()
            assert result[0] == 0
        finally:
            con.close()

        # Row count should be in log (either warning or success message)
        log_text = " ".join(record.message for record in caplog.records)
        assert "0" in log_text or "rows" in log_text.lower()

    def test_extract_preserves_metadata(self, output_file):
        """Test that GeoParquet metadata is preserved."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        extract(str(PLACES_PARQUET), output_file)

        # Check GeoParquet metadata
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(output_file)
        try:
            metadata = pf.schema_arrow.metadata
            assert b"geo" in metadata

            geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
            assert "primary_column" in geo_meta
            assert geo_meta["primary_column"] == "geometry"
        finally:
            # Ensure file handle is released on Windows
            del pf


class TestExtractCLI:
    """Tests for extract CLI command."""

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path.

        Uses a unique filename instead of NamedTemporaryFile to avoid
        Windows file locking issues with DuckDB.
        """
        # Generate unique path without creating the file
        tmp_path = Path(tempfile.gettempdir()) / f"test_cli_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

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
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            cols_result = con.execute(f"DESCRIBE SELECT * FROM '{output_file}'").fetchall()
            columns = [row[0] for row in cols_result]
            assert "name" in columns
            assert "geometry" in columns
            assert "fsq_place_id" not in columns
        finally:
            con.close()

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
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            count = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[0]
            assert count < 766
            assert count > 0
        finally:
            con.close()

    def test_cli_dry_run(self, output_file, caplog):
        """Test CLI dry run."""
        import logging

        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        # Remove the fixture-created file to test that dry run doesn't create it
        if os.path.exists(output_file):
            os.unlink(output_file)

        from click.testing import CliRunner

        from geoparquet_io.cli.main import extract as extract_cmd

        runner = CliRunner()
        with caplog.at_level(logging.DEBUG):
            result = runner.invoke(extract_cmd, [str(PLACES_PARQUET), output_file, "--dry-run"])
        assert result.exit_code == 0
        # Check log messages instead of result.output (pytest captures logs separately)
        log_text = " ".join(record.message for record in caplog.records)
        assert "DRY RUN" in log_text
        assert not os.path.exists(output_file)


@pytest.mark.slow
class TestExtractRemote:
    """Tests for extract with remote files (marked slow - skip in CI)."""

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path.

        Uses a unique filename instead of NamedTemporaryFile to avoid
        Windows file locking issues with DuckDB.
        """
        # Generate unique path without creating the file
        tmp_path = Path(tempfile.gettempdir()) / f"test_remote_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

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
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            count = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[0]
            assert count > 0
            assert count < 617941  # Less than total rows
        finally:
            con.close()


class TestExtractTable:
    """Tests for extract_table Python API function."""

    def test_invalid_geometry_column_raises_error(self):
        """Test that specifying a non-existent geometry column raises ValueError."""
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "geometry": [b"", b"", b""]})

        with pytest.raises(ValueError) as exc_info:
            extract_table(table, geometry_column="nonexistent")

        assert "geometry_column 'nonexistent' not found" in str(exc_info.value)
        assert "id" in str(exc_info.value)
        assert "name" in str(exc_info.value)

    def test_default_geometry_column_not_found_raises_error(self):
        """Test that missing default 'geometry' column raises ValueError."""
        table = pa.table({"id": [1, 2, 3], "data": ["x", "y", "z"]})

        with pytest.raises(ValueError) as exc_info:
            extract_table(table)

        assert "geometry_column 'geometry' not found" in str(exc_info.value)
