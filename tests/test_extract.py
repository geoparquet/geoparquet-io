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

import duckdb
import pyarrow as pa
import pytest

from geoparquet_io.core.bbox_parse import parse_bbox
from geoparquet_io.core.duckdb_utils import fetch_one
from geoparquet_io.core.exceptions import GeoParquetError, InvalidParameterError
from geoparquet_io.core.extract import (
    build_column_selection,
    build_extract_query,
    build_spatial_filter,
    convert_geojson_to_wkt,
    extract,
    extract_table,
    get_schema_columns,
    looks_like_latlong_bbox,
    parse_geometry_input,
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


class TestBboxCrsMismatchOnlyForProjected:
    """The --bbox warning fires for a CRS pyproj reads as projected, and only then."""

    @pytest.mark.parametrize(
        ("crs", "warns"),
        [
            (None, False),
            ("EPSG:4326", False),
            ("EPSG:3857", True),
            ({"name": "Unknown"}, False),  # nothing can read it: unknown is not projected
            ("not a crs", False),
        ],
    )
    def test_warns_only_for_a_definitely_projected_crs(self, crs, warns, monkeypatch, caplog):
        import logging

        from geoparquet_io.core import extract as extract_module

        monkeypatch.setattr(extract_module, "crs_of", lambda *a, **k: crs)
        monkeypatch.setattr(extract_module, "_get_data_bounds", lambda *a: None)
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            extract_module._warn_if_crs_mismatch((0.0, 0.0, 1.0, 1.0), "f.parquet", "geometry")
        assert ("projected CRS" in caplog.text) is warns


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
        with pytest.raises(GeoParquetError) as exc_info:
            parse_bbox("1,2,3")
        assert "expected 4 values" in str(exc_info.value).lower()

    def test_bbox_too_many_values(self):
        """Test bbox with too many values."""
        with pytest.raises(GeoParquetError) as exc_info:
            parse_bbox("1,2,3,4,5")
        assert "expected 4 values" in str(exc_info.value).lower()

    def test_bbox_non_numeric(self):
        """Test bbox with non-numeric values."""
        with pytest.raises(GeoParquetError) as exc_info:
            parse_bbox("a,b,c,d")
        assert "numeric" in str(exc_info.value).lower()

    def test_bbox_reversed_x_coordinates(self):
        """Reversed x beyond longitudes (xmax < xmin, not an antimeridian wrap)."""
        with pytest.raises(GeoParquetError) as exc_info:
            parse_bbox("500000,0,400000,10")
        assert "reversed" in str(exc_info.value).lower()
        assert "xmin" in str(exc_info.value).lower()

    def test_bbox_reversed_y_coordinates(self):
        """Test bbox with reversed y coordinates (ymax < ymin)."""
        with pytest.raises(GeoParquetError) as exc_info:
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
        with pytest.raises(GeoParquetError) as exc_info:
            validate_where_clause("1=1; DROP TABLE users; --")
        assert "DROP" in str(exc_info.value)
        assert "dangerous" in str(exc_info.value).lower()

    def test_delete_keyword_blocked(self):
        """Test that DELETE keyword is blocked."""
        with pytest.raises(GeoParquetError) as exc_info:
            validate_where_clause("DELETE FROM users WHERE 1=1")
        assert "DELETE" in str(exc_info.value)

    def test_insert_keyword_blocked(self):
        """Test that INSERT keyword is blocked."""
        with pytest.raises(GeoParquetError) as exc_info:
            validate_where_clause("1=1; INSERT INTO users VALUES (1, 'hacker')")
        assert "INSERT" in str(exc_info.value)

    def test_update_keyword_blocked(self):
        """Test that UPDATE keyword is blocked."""
        with pytest.raises(GeoParquetError) as exc_info:
            validate_where_clause("1=1; UPDATE users SET admin=true")
        assert "UPDATE" in str(exc_info.value)

    def test_create_keyword_blocked(self):
        """Test that CREATE keyword is blocked."""
        with pytest.raises(GeoParquetError) as exc_info:
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
        with pytest.raises(GeoParquetError) as exc_info:
            validate_where_clause("DROP TABLE x; DELETE FROM y")
        error_msg = str(exc_info.value)
        assert "DROP" in error_msg
        assert "DELETE" in error_msg

    def test_unquoted_semicolon_blocked(self):
        """A statement separator outside string literals is rejected even when no
        blocklisted keyword appears (e.g. COPY-based injection, gpio #612)."""
        with pytest.raises(GeoParquetError) as exc_info:
            validate_where_clause("1=1); COPY (SELECT 42 AS x) TO '/tmp/pwned.csv'; SELECT (1=1")
        assert ";" in str(exc_info.value)

    def test_bare_semicolon_blocked(self):
        with pytest.raises(GeoParquetError):
            validate_where_clause("1=1;")

    def test_semicolon_inside_single_quoted_literal_allowed(self):
        validate_where_clause("name = 'a;b'")
        validate_where_clause("note = 'it''s; fine'")

    def test_semicolon_inside_double_quoted_identifier_allowed(self):
        validate_where_clause('"weird;col" = 5')


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
        with pytest.raises(GeoParquetError) as exc_info:
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
        with pytest.raises(GeoParquetError) as exc_info:
            parse_geometry_input("@nonexistent_file.geojson")
        assert "not found" in str(exc_info.value)

    def test_invalid_geojson(self):
        """Test error on invalid GeoJSON."""
        with pytest.raises(GeoParquetError) as exc_info:
            parse_geometry_input('{"invalid": "json"}')
        # Error could be about parsing or type field
        error_msg = str(exc_info.value).lower()
        assert "geojson" in error_msg or "type" in error_msg

    def test_empty_feature_collection(self):
        """Test error on empty FeatureCollection."""
        fc = '{"type": "FeatureCollection", "features": []}'
        with pytest.raises(GeoParquetError) as exc_info:
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


class TestColumnSelectionResolvesNames:
    """``build_column_selection`` checks the names it is given against the schema."""

    COLS = ["id", "name", "geometry"]

    def test_valid_columns(self):
        assert build_column_selection(self.COLS, ["id", "name"], None, "geometry", None) == [
            "id",
            "name",
            "geometry",
        ]

    def test_missing_columns(self):
        with pytest.raises(GeoParquetError) as exc_info:
            build_column_selection(
                self.COLS,
                ["id", "nonexistent"],
                None,
                "geometry",
                None,
                include_option="--include-cols",
            )
        assert "nonexistent" in str(exc_info.value)
        assert "--include-cols" in str(exc_info.value)

    def test_none_columns(self):
        assert build_column_selection(self.COLS, None, None, "geometry", None) == self.COLS


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
            result = fetch_one(con.execute(f"SELECT COUNT(*) FROM '{output_file}'"))
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

        with pytest.raises(GeoParquetError) as exc_info:
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
            result = fetch_one(con.execute(f"SELECT COUNT(*) FROM '{output_file}'"))
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
            result = fetch_one(con.execute(f"SELECT COUNT(*) FROM '{output_file}'"))
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
            result = fetch_one(con.execute(f"SELECT COUNT(*) FROM '{output_file}'"))
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
            result = fetch_one(con.execute(f"SELECT COUNT(*) FROM '{output_file}'"))
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
            result = fetch_one(con.execute(f"SELECT COUNT(*) FROM '{output_file}'"))
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

        with pytest.raises(GeoParquetError) as exc_info:
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
            result = fetch_one(con.execute(f"SELECT COUNT(*) FROM '{output_file}'"))
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
        """Test CLI help output for extract command group."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import extract as extract_cmd

        runner = CliRunner()

        # Test group help shows subcommands
        result = runner.invoke(extract_cmd, ["--help"])
        assert result.exit_code == 0
        assert "geoparquet" in result.output
        assert "bigquery" in result.output

        # Test geoparquet subcommand help shows options
        result = runner.invoke(extract_cmd, ["geoparquet", "--help"])
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
            count = fetch_one(con.execute(f"SELECT COUNT(*) FROM '{output_file}'"))[0]
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
@pytest.mark.network
class TestExtractRemote:
    """Tests for extract with remote files (requires network access)."""

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
            count = fetch_one(con.execute(f"SELECT COUNT(*) FROM '{output_file}'"))[0]
            assert count > 0
            assert count < 617941  # Less than total rows
        finally:
            con.close()


class TestExtractTable:
    """Tests for extract_table Python API function."""

    def test_invalid_geometry_column_raises_error(self):
        """Specifying a non-existent geometry column is an invalid parameter."""
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "geometry": [b"", b"", b""]})

        with pytest.raises(InvalidParameterError) as exc_info:
            extract_table(table, geometry_column="nonexistent")

        assert "geometry column 'nonexistent' not found" in str(exc_info.value)
        assert "id" in str(exc_info.value)
        assert "name" in str(exc_info.value)

    def test_table_without_geometry_extracts_without_geometry(self):
        """A table with no geometry column is a legitimate input, not an error.

        extract() on an attribute table (one produced by an earlier
        ``exclude_columns=['geometry']``, say) used to default geom_col to
        'geometry' and fail hard on any further call, even a limit-only one.
        """
        table = pa.table({"id": [1, 2, 3], "data": ["x", "y", "z"]})

        result = extract_table(table, limit=2)

        assert result.num_rows == 2
        assert result.column_names == ["id", "data"]

    def test_table_without_geometry_still_selects_columns(self):
        """Column projection works on a table that has no geometry column."""
        table = pa.table({"id": [1, 2, 3], "data": ["x", "y", "z"], "extra": [1, 2, 3]})

        result = extract_table(table, columns=["id", "data"])

        assert result.column_names == ["id", "data"]

    def test_bbox_without_geometry_column_raises(self):
        """A spatial filter needs geometry: say so by name instead of silently ignoring it."""
        table = pa.table({"id": [1, 2, 3], "data": ["x", "y", "z"]})

        with pytest.raises(InvalidParameterError) as exc_info:
            extract_table(table, bbox=(0.0, 0.0, 1.0, 1.0))

        assert "bbox" in str(exc_info.value)

    def test_explicit_geometry_column_missing_still_raises(self):
        """An explicitly named geometry column that is absent is still an error."""
        table = pa.table({"id": [1, 2, 3], "data": ["x", "y", "z"]})

        with pytest.raises(GeoParquetError) as exc_info:
            extract_table(table, geometry_column="geom")

        assert "geom" in str(exc_info.value)

    def test_column_in_both_include_and_exclude_raises(self):
        """columns= and exclude_columns= naming the same column is a contradiction (#731).

        The CLI rejects it via ``_validate_column_overlap``; the API used to
        silently ignore the exclusion and keep the column.
        """
        table = pa.table({"id": [1, 2], "name": ["a", "b"], "geometry": [b"", b""]})

        with pytest.raises(InvalidParameterError) as exc_info:
            extract_table(table, columns=["id", "name"], exclude_columns=["name"])

        message = str(exc_info.value)
        assert "name" in message
        assert "columns/exclude_columns" in message

    def test_geometry_may_be_in_both_include_and_exclude(self):
        """geometry and bbox are the exception the CLI already carves out.

        Only *ordinary* columns are a contradiction; naming geometry in both
        lists is how a caller says "these attributes, and decide geometry
        separately", so it must not raise.
        """
        table = pa.table(
            {
                "id": [1, 2],
                "name": ["a", "b"],
                "geometry": [_wkb_point(0.0, 0.0), _wkb_point(1.0, 1.0)],
            }
        )

        result = extract_table(table, columns=["id", "geometry"], exclude_columns=["geometry"])

        assert "id" in result.column_names

    def test_unknown_columns_raise_naming_the_missing_ones(self):
        """extract_table validates columns= against the schema, like --include-cols.

        Unknown names used to pass silently and surface much later as a
        KeyError in the writer that named a column the file *does* have (#731).
        """
        table = pa.table({"id": [1, 2], "name": ["a", "b"], "geometry": [b"", b""]})

        with pytest.raises(GeoParquetError) as exc_info:
            extract_table(table, columns=["id", "name", "population", "bogus"])

        message = str(exc_info.value)
        assert "population" in message
        assert "bogus" in message
        # The available-columns list helps the caller correct the call.
        assert "name" in message

    def test_unknown_exclude_columns_raise(self):
        """exclude_columns= is validated the same way as --exclude-cols (#731)."""
        table = pa.table({"id": [1, 2], "name": ["a", "b"], "geometry": [b"", b""]})

        with pytest.raises(GeoParquetError) as exc_info:
            extract_table(table, exclude_columns=["nonexistent"])

        assert "nonexistent" in str(exc_info.value)

    def test_valid_columns_still_pass(self):
        """Validation must not reject names that do exist (#731)."""
        import struct

        def _wkb_point(x, y):
            return b"\x01\x01\x00\x00\x00" + struct.pack("<dd", x, y)

        table = pa.table(
            {
                "id": [1, 2],
                "name": ["a", "b"],
                "geometry": [_wkb_point(0.0, 0.0), _wkb_point(1.0, 1.0)],
            }
        )

        result = extract_table(table, columns=["id", "name"])

        assert "id" in result.column_names
        assert "name" in result.column_names


def _wkb_point(x: float, y: float) -> bytes:
    """Minimal little-endian WKB POINT, so DuckDB can read the column as geometry."""
    import struct

    return b"\x01\x01\x00\x00\x00" + struct.pack("<dd", x, y)


def _table_with_geo(columns: dict, geo: dict) -> pa.Table:
    """Build an Arrow table carrying ``geo`` file-level metadata."""
    table = pa.table(columns)
    return table.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")})


def _geo_of(table: pa.Table) -> dict | None:
    """Decode the ``geo`` metadata of a result table, or None when absent."""
    metadata = table.schema.metadata or {}
    if b"geo" not in metadata:
        return None
    return json.loads(metadata[b"geo"].decode("utf-8"))


class TestExtractTableGeoMetadata:
    """The carried geo metadata must describe the columns the result actually has."""

    def test_dropping_primary_geometry_drops_geo_metadata(self):
        """An attribute table is plain Parquet: it must not advertise geo metadata."""
        table = _table_with_geo(
            {"id": [1, 2], "geometry": [_wkb_point(0.0, 0.0), _wkb_point(1.0, 1.0)]},
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
            },
        )

        result = extract_table(table, exclude_columns=["geometry"])

        assert result.column_names == ["id"]
        assert _geo_of(result) is None

    def test_dropping_bbox_column_drops_its_covering(self):
        """A covering pointing at a dropped column leaves geo metadata readers reject."""
        bbox_type = pa.struct(
            [("xmin", pa.float64()), ("ymin", pa.float64()), ("xmax", pa.float64())]
        )
        table = _table_with_geo(
            {
                "id": pa.array([1, 2]),
                "geometry": pa.array([_wkb_point(0.0, 0.0), _wkb_point(1.0, 1.0)]),
                "bbox": pa.array(
                    [
                        {"xmin": 0.0, "ymin": 0.0, "xmax": 0.0},
                        {"xmin": 1.0, "ymin": 1.0, "xmax": 1.0},
                    ],
                    type=bbox_type,
                ),
            },
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "covering": {
                            "bbox": {
                                "xmin": ["bbox", "xmin"],
                                "ymin": ["bbox", "ymin"],
                                "xmax": ["bbox", "xmax"],
                            }
                        },
                    }
                },
            },
        )

        result = extract_table(table, exclude_columns=["bbox"])

        assert "bbox" not in result.column_names
        geo = _geo_of(result)
        assert geo is not None
        assert "covering" not in geo["columns"]["geometry"]

    def test_dropping_primary_repoints_at_a_surviving_geometry_column(self):
        """A secondary geometry column that survives becomes the primary."""
        table = _table_with_geo(
            {
                "id": [1, 2],
                "geom_a": [_wkb_point(0.0, 0.0), _wkb_point(1.0, 1.0)],
                "geom_b": [_wkb_point(2.0, 2.0), _wkb_point(3.0, 3.0)],
            },
            {
                "version": "1.1.0",
                "primary_column": "geom_a",
                "columns": {
                    "geom_a": {"encoding": "WKB"},
                    "geom_b": {"encoding": "WKB"},
                },
            },
        )

        result = extract_table(table, exclude_columns=["geom_a"])

        assert "geom_a" not in result.column_names
        geo = _geo_of(result)
        assert geo is not None
        assert geo["primary_column"] == "geom_b"
        assert set(geo["columns"]) == {"geom_b"}

    def test_unfiltered_extract_keeps_geo_metadata_intact(self):
        """Nothing is dropped, so nothing about the carried metadata changes."""
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
        }
        table = _table_with_geo(
            {"id": [1, 2], "geometry": [_wkb_point(0.0, 0.0), _wkb_point(1.0, 1.0)]}, geo
        )

        result = extract_table(table)

        assert _geo_of(result) == geo


# A self-intersecting "bowtie" is invalid, and ST_MakeValid splits it into two
# triangles: repairing it turns a Polygon into a MultiPolygon (issue #812).
_BOWTIE_WKT = "POLYGON ((0 0, 1 1, 1 0, 0 1, 0 0))"
_SQUARE_WKT = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"

# Deliberately over-wide: the declared bbox of these fixtures does not describe
# their row, so an output reporting it carried the input's stats over, and an
# output reporting the tight [0, 0, 1, 1] recomputed them from the rows written.
_WORLD_BBOX = [-180.0, -90.0, 180.0, 90.0]


def _wkb_of(wkt: str) -> bytes:
    """WKB for a WKT literal, via DuckDB's spatial extension."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    try:
        return fetch_one(con.execute("SELECT ST_AsWKB(ST_GeomFromText(?))", [wkt]))[0]
    finally:
        con.close()


def _polygon_fixture_table(wkt: str, orientation: str | None = None) -> pa.Table:
    """One-row GeoParquet 1.1 table declaring Polygon and a world-wide bbox."""
    column: dict = {
        "encoding": "WKB",
        "geometry_types": ["Polygon"],
        "bbox": list(_WORLD_BBOX),
    }
    if orientation is not None:
        column["orientation"] = orientation
    return _table_with_geo(
        {"id": [1], "geometry": [_wkb_of(wkt)]},
        {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": column},
        },
    )


def _write_polygon_fixture(path: Path, wkt: str, orientation: str | None = None) -> Path:
    """Write the fixture table of :func:`_polygon_fixture_table` to ``path``."""
    import pyarrow.parquet as pq

    pq.write_table(_polygon_fixture_table(wkt, orientation), str(path))
    return path


def _geo_column_of_file(path: Path) -> dict:
    """The primary geometry column's geo metadata entry of a written file."""
    import pyarrow.parquet as pq

    geo = json.loads(pq.read_schema(str(path)).metadata[b"geo"])
    return geo["columns"][geo["primary_column"]]


class TestRepairInvalidatesCarriedStats:
    """A geometry repair rewrites rows, so carried stats must be recomputed (#812).

    ``--repair-geometry`` is on by default and ``ST_MakeValid`` can change a
    geometry's type, so an unfiltered extract that carried the input's
    ``geometry_types``/``bbox`` verbatim shipped an output declaring ``Polygon``
    while holding a ``MultiPolygon`` — which ``gpio check spec`` then failed.
    """

    @pytest.fixture
    def bowtie_file(self, tmp_path):
        return _write_polygon_fixture(tmp_path / "bowtie.parquet", _BOWTIE_WKT)

    @pytest.fixture
    def square_file(self, tmp_path):
        return _write_polygon_fixture(tmp_path / "square.parquet", _SQUARE_WKT)

    def test_repair_recomputes_geometry_types_and_bbox(self, bowtie_file, tmp_path):
        from geoparquet_io.core.validate import validate_geoparquet

        assert _geo_column_of_file(bowtie_file)["geometry_types"] == ["Polygon"]

        out = tmp_path / "out.parquet"
        extract(str(bowtie_file), str(out))

        col = _geo_column_of_file(out)
        assert col["geometry_types"] == ["MultiPolygon"]
        assert col["bbox"] == [0.0, 0.0, 1.0, 1.0]
        result = validate_geoparquet(str(out))
        assert result.is_valid, [c.message for c in result.checks if not c.passed]

    def test_no_repair_keeps_the_carried_stats(self, bowtie_file, tmp_path):
        """Repair off leaves the rows alone, so the carried stats still describe them."""
        out = tmp_path / "out.parquet"
        extract(str(bowtie_file), str(out), repair_geometry=False)

        col = _geo_column_of_file(out)
        assert col["geometry_types"] == ["Polygon"]
        assert col["bbox"] == _WORLD_BBOX

    def test_nothing_to_repair_keeps_the_carried_stats(self, square_file, tmp_path):
        """Repair on but nothing invalid: no rewrite happened, so no rescan either."""
        out = tmp_path / "out.parquet"
        extract(str(square_file), str(out))

        col = _geo_column_of_file(out)
        assert col["geometry_types"] == ["Polygon"]
        assert col["bbox"] == _WORLD_BBOX

    def test_streaming_output_recomputes_stats(self, bowtie_file, monkeypatch):
        """The stdout path strips the stale stats and backfills them from the rows."""
        import io
        from unittest import mock

        import pyarrow.ipc as ipc

        buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.isatty.return_value = False
        mock_stdout.buffer = buffer
        monkeypatch.setattr("sys.stdout", mock_stdout)

        extract(str(bowtie_file), "-")

        buffer.seek(0)
        col = _geo_of(ipc.open_stream(buffer).read_all())["columns"]["geometry"]
        assert col["geometry_types"] == ["MultiPolygon"]
        assert col["bbox"] == [0.0, 0.0, 1.0, 1.0]

    def test_extract_table_repair_recomputes_stats(self):
        """The in-memory (Python API) path carries the same metadata and must fix it too."""
        result = extract_table(_polygon_fixture_table(_BOWTIE_WKT))

        col = _geo_of(result)["columns"]["geometry"]
        assert col["geometry_types"] == ["MultiPolygon"]
        assert col["bbox"] == [0.0, 0.0, 1.0, 1.0]

    def test_extract_table_no_repair_keeps_the_carried_stats(self):
        result = extract_table(_polygon_fixture_table(_BOWTIE_WKT), repair_geometry=False)

        col = _geo_of(result)["columns"]["geometry"]
        assert col["geometry_types"] == ["Polygon"]
        assert col["bbox"] == _WORLD_BBOX

    def test_extract_table_nothing_to_repair_keeps_the_carried_stats(self):
        result = extract_table(_polygon_fixture_table(_SQUARE_WKT))

        col = _geo_of(result)["columns"]["geometry"]
        assert col["geometry_types"] == ["Polygon"]
        assert col["bbox"] == _WORLD_BBOX


class TestRepairDropsCarriedOrientation:
    """A geometry repair can rewind rings, so a carried ``orientation`` must go.

    ``ST_MakeValid`` gives the repaired bowtie clockwise exterior rings, so a
    carried ``orientation: "counterclockwise"`` declaration would be factually
    false in the output. Orientation is optional per spec and gpio does not
    re-orient, so absence is the honest value — dropped only when the repair
    actually rewrote a row, on the same condition that recomputes bbox and
    geometry_types.
    """

    def test_repair_drops_the_carried_orientation(self, tmp_path):
        src = _write_polygon_fixture(
            tmp_path / "bowtie.parquet", _BOWTIE_WKT, orientation="counterclockwise"
        )

        out = tmp_path / "out.parquet"
        extract(str(src), str(out))

        assert "orientation" not in _geo_column_of_file(out)

    def test_no_repair_keeps_the_carried_orientation(self, tmp_path):
        """Repair off leaves the rows alone, so the declaration still holds."""
        src = _write_polygon_fixture(
            tmp_path / "bowtie.parquet", _BOWTIE_WKT, orientation="counterclockwise"
        )

        out = tmp_path / "out.parquet"
        extract(str(src), str(out), repair_geometry=False)

        assert _geo_column_of_file(out)["orientation"] == "counterclockwise"

    def test_nothing_to_repair_keeps_the_carried_orientation(self, tmp_path):
        """Repair on but nothing invalid: no ring was rewound."""
        src = _write_polygon_fixture(
            tmp_path / "square.parquet", _SQUARE_WKT, orientation="counterclockwise"
        )

        out = tmp_path / "out.parquet"
        extract(str(src), str(out))

        assert _geo_column_of_file(out)["orientation"] == "counterclockwise"

    def test_streaming_repair_drops_the_carried_orientation(self, tmp_path, monkeypatch):
        import io
        from unittest import mock

        import pyarrow.ipc as ipc

        src = _write_polygon_fixture(
            tmp_path / "bowtie.parquet", _BOWTIE_WKT, orientation="counterclockwise"
        )

        buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.isatty.return_value = False
        mock_stdout.buffer = buffer
        monkeypatch.setattr("sys.stdout", mock_stdout)

        extract(str(src), "-")

        buffer.seek(0)
        col = _geo_of(ipc.open_stream(buffer).read_all())["columns"]["geometry"]
        assert "orientation" not in col

    def test_extract_table_repair_drops_the_carried_orientation(self):
        result = extract_table(_polygon_fixture_table(_BOWTIE_WKT, orientation="counterclockwise"))

        assert "orientation" not in _geo_of(result)["columns"]["geometry"]

    def test_extract_table_no_repair_keeps_the_carried_orientation(self):
        result = extract_table(
            _polygon_fixture_table(_BOWTIE_WKT, orientation="counterclockwise"),
            repair_geometry=False,
        )

        col = _geo_of(result)["columns"]["geometry"]
        assert col["orientation"] == "counterclockwise"
