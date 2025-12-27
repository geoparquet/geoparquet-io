"""Tests for core/duckdb_metadata.py module."""

from geoparquet_io.core.duckdb_metadata import (
    detect_geometry_columns,
    get_bbox_from_row_group_stats,
    get_column_names,
    get_compression_info,
    get_geo_metadata,
    get_kv_metadata,
    get_per_row_group_bbox_stats,
    get_row_count,
    get_row_group_stats_summary,
    get_schema_info,
    has_bbox_column,
    is_geometry_column,
    parse_geometry_logical_type,
    resolve_crs_reference,
)


class TestGetKvMetadata:
    """Tests for get_kv_metadata function."""

    def test_returns_dict(self, places_test_file):
        """Test that get_kv_metadata returns a dict."""
        result = get_kv_metadata(places_test_file)
        assert isinstance(result, dict)

    def test_contains_geo_key(self, places_test_file):
        """Test that GeoParquet file contains geo key."""
        result = get_kv_metadata(places_test_file)
        assert b"geo" in result


class TestGetGeoMetadata:
    """Tests for get_geo_metadata function."""

    def test_returns_dict_for_geoparquet(self, places_test_file):
        """Test that get_geo_metadata returns parsed dict for GeoParquet."""
        result = get_geo_metadata(places_test_file)
        assert isinstance(result, dict)
        assert "version" in result or "columns" in result

    def test_returns_dict_for_buildings_file(self, buildings_test_file):
        """Test get_geo_metadata with buildings test file."""
        result = get_geo_metadata(buildings_test_file)
        # Buildings file has geo metadata
        assert isinstance(result, dict)
        assert "version" in result or "columns" in result


class TestGetRowGroupStatsSummary:
    """Tests for get_row_group_stats_summary function."""

    def test_returns_expected_keys(self, places_test_file):
        """Test that get_row_group_stats_summary returns expected structure."""
        result = get_row_group_stats_summary(places_test_file)
        assert isinstance(result, dict)
        assert "num_groups" in result
        assert "total_rows" in result
        assert "avg_rows_per_group" in result

    def test_positive_values(self, places_test_file):
        """Test that stats have positive values."""
        result = get_row_group_stats_summary(places_test_file)
        assert result["num_groups"] > 0
        assert result["total_rows"] > 0


class TestGetSchemaInfo:
    """Tests for get_schema_info function."""

    def test_returns_list(self, places_test_file):
        """Test that get_schema_info returns a list."""
        result = get_schema_info(places_test_file)
        assert isinstance(result, list)
        assert len(result) > 0

    def test_column_info(self, places_test_file):
        """Test that each column has expected info."""
        result = get_schema_info(places_test_file)
        for col in result:
            assert "name" in col
            assert "type" in col


class TestGetColumnNames:
    """Tests for get_column_names function."""

    def test_returns_list(self, places_test_file):
        """Test that get_column_names returns a list of strings."""
        result = get_column_names(places_test_file)
        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(name, str) for name in result)


class TestGetRowCount:
    """Tests for get_row_count function."""

    def test_returns_positive_int(self, places_test_file):
        """Test that get_row_count returns a positive integer."""
        result = get_row_count(places_test_file)
        assert isinstance(result, int)
        assert result > 0


class TestGetCompressionInfo:
    """Tests for get_compression_info function."""

    def test_returns_dict(self, places_test_file):
        """Test that get_compression_info returns a dict."""
        result = get_compression_info(places_test_file)
        assert isinstance(result, dict)
        assert len(result) > 0


class TestIsGeometryColumn:
    """Tests for is_geometry_column function."""

    def test_geometry_types(self):
        """Test that geometry types are detected (DuckDB format)."""
        assert is_geometry_column("GeometryType(Point, XY)") is True
        assert is_geometry_column("GeographyType(Polygon, XY)") is True

    def test_non_geometry_types(self):
        """Test that non-geometry types return False."""
        assert is_geometry_column("VARCHAR") is False
        assert is_geometry_column("INTEGER") is False
        assert is_geometry_column("BLOB") is False
        assert is_geometry_column("") is False
        assert is_geometry_column(None) is False


class TestDetectGeometryColumns:
    """Tests for detect_geometry_columns function."""

    def test_returns_dict(self, places_test_file):
        """Test that detect_geometry_columns returns a dict."""
        result = detect_geometry_columns(places_test_file)
        assert isinstance(result, dict)


class TestHasBboxColumn:
    """Tests for has_bbox_column function."""

    def test_places_has_bbox(self, places_test_file):
        """Test that places file has bbox column."""
        has_bbox, bbox_name = has_bbox_column(places_test_file)
        assert has_bbox is True
        assert bbox_name is not None
        assert isinstance(bbox_name, str)

    def test_buildings_no_bbox(self, buildings_test_file):
        """Test that buildings file doesn't have bbox column."""
        has_bbox, bbox_name = has_bbox_column(buildings_test_file)
        assert has_bbox is False
        assert bbox_name is None


class TestGetPerRowGroupBboxStats:
    """Tests for get_per_row_group_bbox_stats function."""

    def test_with_bbox_column(self, places_test_file):
        """Test getting bbox stats for file with bbox."""
        has_bbox, bbox_name = has_bbox_column(places_test_file)
        if has_bbox and bbox_name:
            result = get_per_row_group_bbox_stats(places_test_file, bbox_name)
            assert isinstance(result, list)


class TestGetBboxFromRowGroupStats:
    """Tests for get_bbox_from_row_group_stats function."""

    def test_with_bbox_column(self, places_test_file):
        """Test getting overall bbox for file with bbox."""
        has_bbox, bbox_name = has_bbox_column(places_test_file)
        if has_bbox and bbox_name:
            result = get_bbox_from_row_group_stats(places_test_file, bbox_name)
            if result is not None:
                assert len(result) == 4
                # xmin <= xmax and ymin <= ymax
                assert result[0] <= result[2]
                assert result[1] <= result[3]


class TestParseGeometryLogicalType:
    """Tests for parse_geometry_logical_type function."""

    def test_geometry_type_with_inline_crs(self):
        """Test parsing GeometryType with inline PROJJSON CRS."""
        logical_type = (
            'GeometryType(crs={"type": "ProjectedCRS", "id": {"authority": "EPSG", "code": 5070}})'
        )
        result = parse_geometry_logical_type(logical_type)

        assert result is not None
        assert result["geo_type"] == "Geometry"
        assert isinstance(result.get("crs"), dict)
        assert result["crs"]["id"]["code"] == 5070

    def test_geometry_type_with_projjson_reference(self):
        """Test parsing GeometryType with projjson: reference."""
        logical_type = "GeometryType(crs=projjson:projjson_epsg_5070)"
        result = parse_geometry_logical_type(logical_type)

        assert result is not None
        assert result["geo_type"] == "Geometry"
        # Should store the reference string for later resolution
        assert result.get("crs") == "projjson:projjson_epsg_5070"

    def test_geometry_type_with_srid(self):
        """Test parsing GeometryType with srid: format."""
        logical_type = "GeometryType(crs=srid:5070)"
        result = parse_geometry_logical_type(logical_type)

        assert result is not None
        assert result["geo_type"] == "Geometry"
        # Should store the srid string for later resolution
        assert result.get("crs") == "srid:5070"

    def test_geometry_type_with_null_crs(self):
        """Test parsing GeometryType with null CRS."""
        logical_type = "GeometryType(crs=<null>)"
        result = parse_geometry_logical_type(logical_type)

        assert result is not None
        assert result["geo_type"] == "Geometry"
        # Should not have crs key when null
        assert "crs" not in result

    def test_geography_type_with_algorithm(self):
        """Test parsing GeographyType with algorithm."""
        logical_type = "GeographyType(algorithm=spherical)"
        result = parse_geometry_logical_type(logical_type)

        assert result is not None
        assert result["geo_type"] == "Geography"
        assert result.get("algorithm") == "spherical"


class TestResolveCrsReference:
    """Tests for resolve_crs_reference function."""

    def test_resolve_none_returns_none(self):
        """Test that None CRS returns None."""
        result = resolve_crs_reference("any_file.parquet", None)
        assert result is None

    def test_resolve_inline_projjson_unchanged(self):
        """Test that inline PROJJSON dict is returned unchanged."""
        inline_crs = {"type": "ProjectedCRS", "id": {"authority": "EPSG", "code": 5070}}
        result = resolve_crs_reference("any_file.parquet", inline_crs)
        assert result == inline_crs

    def test_resolve_srid_format(self):
        """Test resolving srid:XXXX format to PROJJSON."""
        result = resolve_crs_reference("any_file.parquet", "srid:5070")

        # Should return a full PROJJSON dict
        assert isinstance(result, dict)
        # Should have CRS structure
        assert "type" in result or "$schema" in result
        # Should be EPSG:5070
        assert result.get("id", {}).get("code") == 5070

    def test_resolve_projjson_reference(self, crs_projjson_file):
        """Test resolving projjson:key_name format from file metadata."""
        result = resolve_crs_reference(crs_projjson_file, "projjson:projjson_epsg_5070")

        # Should return a full PROJJSON dict
        assert isinstance(result, dict)
        # Should have CRS structure
        assert "type" in result or "$schema" in result
        # Should be EPSG:5070
        assert result.get("id", {}).get("code") == 5070

    def test_resolve_unknown_format_passthrough(self):
        """Test that unknown format strings are returned as-is."""
        result = resolve_crs_reference("any_file.parquet", "unknown:format")
        assert result == "unknown:format"


class TestExtractCrsFromParquet:
    """Tests for extract_crs_from_parquet with CRS reference formats."""

    def test_extract_crs_from_projjson_reference(self, crs_projjson_file):
        """Test extracting CRS from file with projjson: reference."""
        from geoparquet_io.core.common import extract_crs_from_parquet

        crs = extract_crs_from_parquet(crs_projjson_file)

        # Should return resolved PROJJSON, not the reference string
        assert isinstance(crs, dict)
        assert "type" in crs or "$schema" in crs
        # Should be EPSG:5070
        assert crs.get("id", {}).get("code") == 5070

    def test_extract_crs_from_srid_format(self, crs_srid_file):
        """Test extracting CRS from file with srid: format."""
        from geoparquet_io.core.common import extract_crs_from_parquet

        crs = extract_crs_from_parquet(crs_srid_file)

        # Should return resolved PROJJSON, not the srid string
        assert isinstance(crs, dict)
        assert "type" in crs or "$schema" in crs
        # Should be EPSG:5070
        assert crs.get("id", {}).get("code") == 5070
