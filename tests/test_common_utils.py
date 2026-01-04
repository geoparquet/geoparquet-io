"""Tests for core/common.py utility functions."""

import pytest

from geoparquet_io.core.common import (
    _get_geometry_type_name,
    check_bbox_structure,
    detect_geoparquet_file_type,
    find_primary_geometry_column,
    format_size,
    get_bbox_advice,
    get_crs_display_name,
    get_duckdb_connection,
    get_parquet_metadata,
    has_glob_pattern,
    is_azure_url,
    is_gcs_url,
    is_geographic_crs,
    is_remote_url,
    is_s3_url,
    needs_httpfs,
    parse_size_string,
    should_skip_bbox,
    validate_compression_settings,
    validate_parquet_extension,
)


class TestIsRemoteUrl:
    """Tests for is_remote_url function."""

    def test_s3_url(self):
        """Test S3 URLs are detected as remote."""
        assert is_remote_url("s3://bucket/file.parquet") is True
        assert is_remote_url("s3://my-bucket/path/to/file.parquet") is True

    def test_gcs_url(self):
        """Test GCS URLs are detected as remote."""
        assert is_remote_url("gs://bucket/file.parquet") is True

    def test_azure_url(self):
        """Test Azure URLs are detected as remote."""
        assert is_remote_url("az://container/file.parquet") is True
        assert is_remote_url("abfs://container/file.parquet") is True

    def test_http_urls(self):
        """Test HTTP/HTTPS URLs are detected as remote."""
        assert is_remote_url("https://example.com/data.parquet") is True
        assert is_remote_url("http://example.com/data.parquet") is True

    def test_local_paths_not_remote(self):
        """Test local paths are not detected as remote."""
        assert is_remote_url("local.parquet") is False
        assert is_remote_url("/path/to/file.parquet") is False
        assert is_remote_url("./relative/path.parquet") is False
        assert is_remote_url("C:\\Windows\\path.parquet") is False


class TestIsS3Url:
    """Tests for is_s3_url function."""

    def test_s3_url(self):
        """Test S3 URL detection."""
        assert is_s3_url("s3://bucket/file.parquet") is True
        assert is_s3_url("s3a://bucket/file.parquet") is True

    def test_non_s3_urls(self):
        """Test non-S3 URLs return False."""
        assert is_s3_url("gs://bucket/file.parquet") is False
        assert is_s3_url("/local/path.parquet") is False
        assert is_s3_url("https://example.com/file.parquet") is False


class TestIsAzureUrl:
    """Tests for is_azure_url function."""

    def test_azure_url(self):
        """Test Azure URL detection."""
        assert is_azure_url("az://container/file.parquet") is True
        assert is_azure_url("abfs://container/file.parquet") is True
        assert is_azure_url("abfss://container@account.dfs.core.windows.net/file") is True

    def test_non_azure_urls(self):
        """Test non-Azure URLs return False."""
        assert is_azure_url("s3://bucket/file.parquet") is False
        assert is_azure_url("gs://bucket/file.parquet") is False


class TestIsGcsUrl:
    """Tests for is_gcs_url function."""

    def test_gcs_url(self):
        """Test GCS URL detection."""
        assert is_gcs_url("gs://bucket/file.parquet") is True
        assert is_gcs_url("gcs://bucket/file.parquet") is True

    def test_non_gcs_urls(self):
        """Test non-GCS URLs return False."""
        assert is_gcs_url("s3://bucket/file.parquet") is False
        assert is_gcs_url("az://container/file.parquet") is False


class TestNeedsHttpfs:
    """Tests for needs_httpfs function."""

    def test_s3_urls_need_httpfs(self):
        """Test that S3 URLs need httpfs."""
        assert needs_httpfs("s3://bucket/file.parquet") is True
        assert needs_httpfs("s3a://bucket/file.parquet") is True

    def test_http_urls_dont_need_httpfs(self):
        """Test that HTTP URLs don't need httpfs (DuckDB handles them directly)."""
        assert needs_httpfs("https://example.com/data.parquet") is False
        assert needs_httpfs("http://example.com/data.parquet") is False

    def test_local_paths_dont_need_httpfs(self):
        """Test that local paths don't need httpfs."""
        assert needs_httpfs("/local/path.parquet") is False
        assert needs_httpfs("./relative/path.parquet") is False


class TestHasGlobPattern:
    """Tests for has_glob_pattern function."""

    def test_asterisk_pattern(self):
        """Test asterisk glob pattern detection."""
        assert has_glob_pattern("*.parquet") is True
        assert has_glob_pattern("/path/**/*.parquet") is True

    def test_question_mark_pattern(self):
        """Test question mark glob pattern detection."""
        assert has_glob_pattern("file?.parquet") is True

    def test_bracket_pattern(self):
        """Test bracket glob pattern detection."""
        assert has_glob_pattern("file[0-9].parquet") is True

    def test_no_pattern(self):
        """Test paths without glob patterns."""
        assert has_glob_pattern("/path/to/file.parquet") is False
        assert has_glob_pattern("simple_name.parquet") is False


class TestShouldSkipBbox:
    """Tests for should_skip_bbox function."""

    def test_skip_for_v2(self):
        """Test bbox should be skipped for GeoParquet 2.0."""
        assert should_skip_bbox("2.0") is True

    def test_skip_for_parquet_geo_only(self):
        """Test bbox should be skipped for parquet-geo-only."""
        assert should_skip_bbox("parquet-geo-only") is True

    def test_no_skip_for_v1(self):
        """Test bbox should not be skipped for GeoParquet 1.x."""
        assert should_skip_bbox("1.0") is False
        assert should_skip_bbox("1.1") is False
        assert should_skip_bbox(None) is False


class TestFormatSize:
    """Tests for format_size function."""

    def test_bytes(self):
        """Test formatting bytes."""
        result = format_size(500)
        assert "B" in result
        assert "500" in result

        result = format_size(0)
        assert "B" in result

    def test_kilobytes(self):
        """Test formatting kilobytes."""
        result = format_size(1024)
        assert "KB" in result or "kB" in result

    def test_megabytes(self):
        """Test formatting megabytes."""
        result = format_size(1024 * 1024)
        assert "MB" in result

    def test_gigabytes(self):
        """Test formatting gigabytes."""
        result = format_size(1024 * 1024 * 1024)
        assert "GB" in result


class TestParseSizeString:
    """Tests for parse_size_string function."""

    def test_parse_plain_number_assumes_mb(self):
        """Test that plain numbers are treated as MB."""
        # Plain numbers are assumed to be MB
        assert parse_size_string("100") == 100 * 1024 * 1024
        assert parse_size_string("1") == 1 * 1024 * 1024

    def test_parse_bytes(self):
        """Test parsing byte sizes."""
        assert parse_size_string("100B") == 100
        assert parse_size_string("100 B") == 100

    def test_parse_kilobytes(self):
        """Test parsing kilobyte sizes."""
        assert parse_size_string("1KB") == 1024
        assert parse_size_string("2KB") == 2048

    def test_parse_megabytes(self):
        """Test parsing megabyte sizes."""
        assert parse_size_string("1MB") == 1024 * 1024
        assert parse_size_string("100MB") == 100 * 1024 * 1024

    def test_parse_gigabytes(self):
        """Test parsing gigabyte sizes."""
        assert parse_size_string("1GB") == 1024 * 1024 * 1024
        assert parse_size_string("2GB") == 2 * 1024 * 1024 * 1024

    def test_empty_returns_none(self):
        """Test that empty string returns None."""
        assert parse_size_string("") is None
        assert parse_size_string(None) is None

    def test_invalid_size_raises_error(self):
        """Test that invalid sizes raise ValueError."""
        with pytest.raises(ValueError):
            parse_size_string("invalid")


class TestValidateCompressionSettings:
    """Tests for validate_compression_settings function."""

    def test_valid_zstd(self):
        """Test valid ZSTD compression with default level."""
        compression, level, desc = validate_compression_settings("ZSTD", None, False)
        assert compression == "ZSTD"
        assert level == 15  # Default ZSTD level
        assert desc == "ZSTD:15"

    def test_valid_zstd_with_level(self):
        """Test valid ZSTD with custom compression level."""
        compression, level, desc = validate_compression_settings("ZSTD", 10, False)
        assert compression == "ZSTD"
        assert level == 10
        assert desc == "ZSTD:10"

    def test_valid_snappy(self):
        """Test valid SNAPPY compression (no level support)."""
        compression, level, desc = validate_compression_settings("SNAPPY", None, False)
        assert compression == "SNAPPY"
        assert level is None
        assert desc == "SNAPPY"

    def test_valid_gzip(self):
        """Test valid GZIP compression with default level."""
        compression, level, desc = validate_compression_settings("GZIP", None, False)
        assert compression == "GZIP"
        assert level == 6  # Default GZIP level
        assert desc == "GZIP:6"

    def test_valid_uncompressed(self):
        """Test valid UNCOMPRESSED setting."""
        compression, level, desc = validate_compression_settings("UNCOMPRESSED", None, False)
        assert compression == "UNCOMPRESSED"
        assert level is None
        assert desc == "UNCOMPRESSED"

    def test_case_insensitive(self):
        """Test that compression names are case insensitive."""
        compression, level, desc = validate_compression_settings("zstd", None, False)
        assert compression == "ZSTD"


class TestDetectGeoparquetFileType:
    """Tests for detect_geoparquet_file_type function."""

    def test_detects_geoparquet(self, places_test_file):
        """Test detection of GeoParquet file."""
        result = detect_geoparquet_file_type(places_test_file, verbose=False)
        assert isinstance(result, dict)
        assert "file_type" in result
        assert "has_geo_metadata" in result
        assert "has_native_geo_types" in result
        # file_type can be geoparquet_v1, geoparquet_v2, parquet_geo_only, etc.
        assert result["file_type"] in ["geoparquet_v1", "geoparquet_v2", "parquet_geo_only"]

    def test_with_verbose(self, places_test_file):
        """Test detection with verbose flag."""
        result = detect_geoparquet_file_type(places_test_file, verbose=True)
        assert isinstance(result, dict)

    def test_buildings_file(self, buildings_test_file):
        """Test detection with buildings test file."""
        result = detect_geoparquet_file_type(buildings_test_file, verbose=False)
        assert isinstance(result, dict)
        assert "bbox_recommended" in result


class TestFindPrimaryGeometryColumn:
    """Tests for find_primary_geometry_column function."""

    def test_finds_geometry_column(self, places_test_file):
        """Test finding geometry column in places file."""
        result = find_primary_geometry_column(places_test_file, verbose=False)
        assert isinstance(result, str)
        assert result == "geometry"

    def test_buildings_geometry_column(self, buildings_test_file):
        """Test finding geometry column in buildings file."""
        result = find_primary_geometry_column(buildings_test_file, verbose=False)
        assert isinstance(result, str)

    def test_with_verbose(self, places_test_file):
        """Test with verbose flag."""
        result = find_primary_geometry_column(places_test_file, verbose=True)
        assert isinstance(result, str)


class TestGetParquetMetadata:
    """Tests for get_parquet_metadata function."""

    def test_returns_tuple(self, places_test_file):
        """Test that get_parquet_metadata returns a tuple."""
        result = get_parquet_metadata(places_test_file, verbose=False)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_geo_metadata_dict(self, places_test_file):
        """Test that geo metadata is a dict."""
        geo_metadata, primary_col = get_parquet_metadata(places_test_file, verbose=False)
        assert geo_metadata is None or isinstance(geo_metadata, dict)

    def test_with_verbose(self, places_test_file):
        """Test with verbose flag."""
        result = get_parquet_metadata(places_test_file, verbose=True)
        assert isinstance(result, tuple)


class TestCheckBboxStructure:
    """Tests for check_bbox_structure function."""

    def test_places_file_has_bbox(self, places_test_file):
        """Test that places file has bbox structure."""
        result = check_bbox_structure(places_test_file, verbose=False)
        assert isinstance(result, dict)
        assert "has_bbox_column" in result

    def test_buildings_file_no_bbox(self, buildings_test_file):
        """Test buildings file without bbox."""
        result = check_bbox_structure(buildings_test_file, verbose=False)
        assert isinstance(result, dict)
        assert result["has_bbox_column"] is False

    def test_with_verbose(self, places_test_file):
        """Test with verbose flag."""
        result = check_bbox_structure(places_test_file, verbose=True)
        assert isinstance(result, dict)


class TestGetDuckdbConnection:
    """Tests for get_duckdb_connection function."""

    def test_basic_connection(self):
        """Test creating a basic DuckDB connection."""
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        assert con is not None
        con.close()

    def test_with_spatial(self):
        """Test creating connection with spatial extension."""
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        assert con is not None
        con.close()

    def test_with_httpfs(self):
        """Test creating connection with httpfs extension."""
        con = get_duckdb_connection(load_spatial=False, load_httpfs=True)
        assert con is not None
        con.close()

    def test_with_both_extensions(self):
        """Test creating connection with both extensions."""
        con = get_duckdb_connection(load_spatial=True, load_httpfs=True)
        assert con is not None
        # Execute a simple query to verify connection works
        result = con.execute("SELECT 1").fetchone()
        assert result[0] == 1
        con.close()


class TestGetCrsDisplayName:
    """Tests for get_crs_display_name function."""

    def test_none_returns_default(self):
        """Test that None CRS returns default (OGC:CRS84)."""
        assert get_crs_display_name(None) == "None (OGC:CRS84)"

    def test_string_crs(self):
        """Test string CRS is returned as-is."""
        assert get_crs_display_name("EPSG:4326") == "EPSG:4326"
        assert get_crs_display_name("srid:4326") == "srid:4326"

    def test_projjson_with_name_and_code(self):
        """Test PROJJSON dict with name and code."""
        crs = {"name": "WGS 84", "id": {"authority": "EPSG", "code": 4326}}
        assert get_crs_display_name(crs) == "WGS 84 (EPSG:4326)"

    def test_projjson_with_code_only(self):
        """Test PROJJSON dict with code but no name."""
        crs = {"id": {"authority": "EPSG", "code": 4326}}
        assert get_crs_display_name(crs) == "EPSG:4326"

    def test_projjson_with_name_only(self):
        """Test PROJJSON dict with name but no code."""
        crs = {"name": "WGS 84"}
        assert get_crs_display_name(crs) == "WGS 84"

    def test_projjson_empty(self):
        """Test PROJJSON dict with no name or id."""
        crs = {"type": "GeographicCRS"}
        assert get_crs_display_name(crs) == "PROJJSON object"


class TestIsGeographicCrs:
    """Tests for is_geographic_crs function."""

    def test_none_is_geographic(self):
        """Test that None CRS is treated as geographic (default is OGC:CRS84)."""
        assert is_geographic_crs(None) is True

    def test_epsg_4326_is_geographic(self):
        """Test that EPSG:4326 is detected as geographic."""
        crs = {"id": {"authority": "EPSG", "code": 4326}}
        assert is_geographic_crs(crs) is True

    def test_geographic_crs_type(self):
        """Test PROJJSON with GeographicCRS type."""
        crs = {"type": "GeographicCRS", "name": "WGS 84"}
        assert is_geographic_crs(crs) is True

    def test_projected_crs_type(self):
        """Test PROJJSON with ProjectedCRS type."""
        crs = {"type": "ProjectedCRS", "name": "UTM Zone 10N"}
        assert is_geographic_crs(crs) is False

    def test_string_epsg_4326(self):
        """Test string EPSG:4326 is geographic."""
        assert is_geographic_crs("EPSG:4326") is True
        assert is_geographic_crs("epsg:4326") is True

    def test_string_crs84(self):
        """Test string CRS84 is geographic."""
        assert is_geographic_crs("OGC:CRS84") is True
        assert is_geographic_crs("CRS84") is True

    def test_string_utm_is_projected(self):
        """Test string UTM is detected as projected."""
        assert is_geographic_crs("UTM Zone 10N") is False
        assert is_geographic_crs("EPSG:32610") is False  # UTM 10N

    def test_name_with_wgs84(self):
        """Test CRS with WGS84 in name is geographic."""
        crs = {"name": "WGS 84"}
        assert is_geographic_crs(crs) is True

    def test_name_with_utm_is_projected(self):
        """Test CRS with UTM in name is projected."""
        crs = {"name": "WGS 84 / UTM zone 10N"}
        assert is_geographic_crs(crs) is False


class TestGetGeometryTypeName:
    """Tests for _get_geometry_type_name function."""

    def test_2d_types(self):
        """Test 2D geometry types (codes 0-7)."""
        assert _get_geometry_type_name(0) == "Unknown"
        assert _get_geometry_type_name(1) == "Point"
        assert _get_geometry_type_name(2) == "LineString"
        assert _get_geometry_type_name(3) == "Polygon"
        assert _get_geometry_type_name(4) == "MultiPoint"
        assert _get_geometry_type_name(5) == "MultiLineString"
        assert _get_geometry_type_name(6) == "MultiPolygon"
        assert _get_geometry_type_name(7) == "GeometryCollection"

    def test_z_types(self):
        """Test Z geometry types (codes 1001-1007)."""
        assert _get_geometry_type_name(1001) == "Point Z"
        assert _get_geometry_type_name(1002) == "LineString Z"
        assert _get_geometry_type_name(1003) == "Polygon Z"
        assert _get_geometry_type_name(1004) == "MultiPoint Z"
        assert _get_geometry_type_name(1005) == "MultiLineString Z"
        assert _get_geometry_type_name(1006) == "MultiPolygon Z"
        assert _get_geometry_type_name(1007) == "GeometryCollection Z"

    def test_m_types(self):
        """Test M geometry types (codes 2001-2007)."""
        assert _get_geometry_type_name(2001) == "Point M"
        assert _get_geometry_type_name(2002) == "LineString M"
        assert _get_geometry_type_name(2003) == "Polygon M"
        assert _get_geometry_type_name(2004) == "MultiPoint M"
        assert _get_geometry_type_name(2005) == "MultiLineString M"
        assert _get_geometry_type_name(2006) == "MultiPolygon M"
        assert _get_geometry_type_name(2007) == "GeometryCollection M"

    def test_zm_types(self):
        """Test ZM geometry types (codes 3001-3007)."""
        assert _get_geometry_type_name(3001) == "Point ZM"
        assert _get_geometry_type_name(3002) == "LineString ZM"
        assert _get_geometry_type_name(3003) == "Polygon ZM"
        assert _get_geometry_type_name(3004) == "MultiPoint ZM"
        assert _get_geometry_type_name(3005) == "MultiLineString ZM"
        assert _get_geometry_type_name(3006) == "MultiPolygon ZM"
        assert _get_geometry_type_name(3007) == "GeometryCollection ZM"

    def test_unknown_base_type(self):
        """Test unknown base type returns Unknown."""
        assert _get_geometry_type_name(8) == "Unknown"
        assert _get_geometry_type_name(99) == "Unknown"
        assert _get_geometry_type_name(1008) == "Unknown"
        assert _get_geometry_type_name(2099) == "Unknown"


class TestGetBboxAdvice:
    """Tests for get_bbox_advice function."""

    def test_returns_expected_keys(self, places_test_file):
        """Test that get_bbox_advice returns dict with expected keys."""
        result = get_bbox_advice(places_test_file, "spatial_filtering", verbose=False)
        assert isinstance(result, dict)
        assert "needs_warning" in result
        assert "skip_bbox_prefilter" in result
        assert "has_native_geometry" in result
        assert "has_bbox_column" in result
        assert "message" in result
        assert "suggestions" in result

    def test_spatial_filtering_v1_with_bbox(self, places_test_file):
        """Test spatial_filtering for 1.x file with bbox - no warning, no skip."""
        result = get_bbox_advice(places_test_file, "spatial_filtering", verbose=False)
        # places_test_file is GeoParquet 1.x with bbox
        assert result["needs_warning"] is False
        assert result["skip_bbox_prefilter"] is False  # Not native geo

    def test_spatial_filtering_v1_without_bbox(self, buildings_test_file):
        """Test spatial_filtering for 1.x file without bbox - warning issued."""
        result = get_bbox_advice(buildings_test_file, "spatial_filtering", verbose=False)
        # buildings_test_file is 1.x without bbox
        assert result["needs_warning"] is True
        assert result["skip_bbox_prefilter"] is False
        assert "No bbox column found" in result["message"]
        assert len(result["suggestions"]) > 0

    def test_spatial_filtering_v2_skip_bbox(self, fields_v2_file):
        """Test spatial_filtering for 2.0 file - skip bbox prefilter."""
        result = get_bbox_advice(fields_v2_file, "spatial_filtering", verbose=False)
        # fields_v2_file is GeoParquet 2.0 with native geometry
        assert result["has_native_geometry"] is True
        assert result["skip_bbox_prefilter"] is True  # Native geo + spatial_filtering
        assert result["needs_warning"] is False

    def test_bounds_calculation_never_skips_bbox(self, fields_v2_file):
        """Test bounds_calculation for 2.0 file - does NOT skip bbox."""
        result = get_bbox_advice(fields_v2_file, "bounds_calculation", verbose=False)
        # Even with native geometry, bounds_calculation should NOT skip bbox
        # because pre-computed bbox values are faster for bounds calculation
        assert result["has_native_geometry"] is True
        assert result["skip_bbox_prefilter"] is False  # Key test: bounds_calculation never skips

    def test_bounds_calculation_without_bbox(self, buildings_test_file):
        """Test bounds_calculation without bbox - warning issued."""
        result = get_bbox_advice(buildings_test_file, "bounds_calculation", verbose=False)
        assert result["needs_warning"] is True
        assert "computing from geometry" in result["message"].lower()

    def test_check_v1_without_bbox(self, buildings_test_file):
        """Test check operation for 1.x file without bbox - warning issued."""
        result = get_bbox_advice(buildings_test_file, "check", verbose=False)
        assert result["needs_warning"] is True
        assert result["skip_bbox_prefilter"] is False

    def test_check_v2_no_warning(self, fields_v2_file):
        """Test check operation for 2.0 file - no warning needed."""
        result = get_bbox_advice(fields_v2_file, "check", verbose=False)
        assert result["has_native_geometry"] is True
        assert result["needs_warning"] is False
        # check operation is not spatial_filtering, so skip_bbox_prefilter should be False
        assert result["skip_bbox_prefilter"] is False

    def test_verbose_mode(self, places_test_file):
        """Test that verbose mode does not cause errors."""
        result = get_bbox_advice(places_test_file, "spatial_filtering", verbose=True)
        assert isinstance(result, dict)


class TestValidateParquetExtension:
    """Tests for validate_parquet_extension function."""

    def test_valid_parquet_extension(self):
        """Test that .parquet extension passes validation."""
        # Should not raise
        validate_parquet_extension("output.parquet")
        validate_parquet_extension("/path/to/output.parquet")
        validate_parquet_extension("./relative/output.parquet")

    def test_valid_parquet_extension_uppercase(self):
        """Test that uppercase .PARQUET extension passes validation."""
        validate_parquet_extension("output.PARQUET")
        validate_parquet_extension("output.Parquet")

    def test_invalid_extension_raises_error(self):
        """Test that non-.parquet extension raises ClickException."""
        import click

        with pytest.raises(click.ClickException) as exc_info:
            validate_parquet_extension("output.geojson")
        assert ".parquet extension" in str(exc_info.value)
        assert "--any-extension" in str(exc_info.value)

    def test_no_extension_raises_error(self):
        """Test that file without extension raises ClickException."""
        import click

        with pytest.raises(click.ClickException):
            validate_parquet_extension("output_file")

    def test_wrong_extension_variations(self):
        """Test various wrong extensions raise errors."""
        import click

        for ext in [".json", ".csv", ".txt", ".gpkg", ".shp"]:
            with pytest.raises(click.ClickException):
                validate_parquet_extension(f"output{ext}")

    def test_any_extension_flag_allows_non_parquet(self):
        """Test that any_extension=True bypasses validation."""
        # Should not raise with any_extension=True
        validate_parquet_extension("output.geojson", any_extension=True)
        validate_parquet_extension("output.csv", any_extension=True)
        validate_parquet_extension("no_extension", any_extension=True)

    def test_none_output_skips_validation(self):
        """Test that None output (streaming) skips validation."""
        # Should not raise
        validate_parquet_extension(None)
        validate_parquet_extension(None, any_extension=False)

    def test_streaming_output_skips_validation(self):
        """Test that '-' output (stdout) skips validation."""
        # Should not raise
        validate_parquet_extension("-")
        validate_parquet_extension("-", any_extension=False)

    def test_remote_s3_url_validation(self):
        """Test that S3 URLs are validated for extension."""
        import click

        # Valid
        validate_parquet_extension("s3://bucket/path/file.parquet")

        # Invalid
        with pytest.raises(click.ClickException):
            validate_parquet_extension("s3://bucket/path/file.geojson")

    def test_remote_url_with_any_extension(self):
        """Test that remote URLs with any_extension bypass validation."""
        validate_parquet_extension("s3://bucket/path/file.geojson", any_extension=True)
        validate_parquet_extension("gs://bucket/path/file.json", any_extension=True)
