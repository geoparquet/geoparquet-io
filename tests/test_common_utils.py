"""Tests for core/common.py utility functions."""

import pytest

from geoparquet_io.core.common import (
    check_bbox_structure,
    detect_geoparquet_file_type,
    find_primary_geometry_column,
    format_size,
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
