"""Tests for remote file support (HTTPS, S3, Azure, GCS)."""

import pytest
from click import BadParameter

from geoparquet_io.core.common import is_remote_url, needs_httpfs, safe_file_url


class TestRemoteURLDetection:
    """Test URL detection helpers."""

    def test_is_remote_url_https(self):
        """Test HTTPS URL detection."""
        assert is_remote_url("https://example.com/file.parquet")
        assert is_remote_url("http://example.com/file.parquet")

    def test_is_remote_url_s3(self):
        """Test S3 URL detection."""
        assert is_remote_url("s3://bucket/file.parquet")
        assert is_remote_url("s3a://bucket/file.parquet")

    def test_is_remote_url_azure(self):
        """Test Azure URL detection."""
        assert is_remote_url("az://container/file.parquet")
        assert is_remote_url("azure://container/file.parquet")
        assert is_remote_url("abfs://container/file.parquet")
        assert is_remote_url("abfss://container/file.parquet")

    def test_is_remote_url_gcs(self):
        """Test GCS URL detection."""
        assert is_remote_url("gs://bucket/file.parquet")
        assert is_remote_url("gcs://bucket/file.parquet")

    def test_is_remote_url_local(self):
        """Test local path detection."""
        assert not is_remote_url("/local/path/file.parquet")
        assert not is_remote_url("./relative/path/file.parquet")
        assert not is_remote_url("file.parquet")

    def test_needs_httpfs_s3(self):
        """Test httpfs requirement for S3."""
        assert needs_httpfs("s3://bucket/file.parquet")
        assert needs_httpfs("s3a://bucket/file.parquet")

    def test_needs_httpfs_azure(self):
        """Test httpfs requirement for Azure."""
        assert needs_httpfs("az://container/file.parquet")
        assert needs_httpfs("azure://container/file.parquet")
        assert needs_httpfs("abfs://container/file.parquet")
        assert needs_httpfs("abfss://container/file.parquet")

    def test_needs_httpfs_gcs(self):
        """Test httpfs requirement for GCS."""
        assert needs_httpfs("gs://bucket/file.parquet")
        assert needs_httpfs("gcs://bucket/file.parquet")

    def test_needs_httpfs_https(self):
        """Test that HTTPS doesn't require httpfs."""
        assert not needs_httpfs("https://example.com/file.parquet")
        assert not needs_httpfs("http://example.com/file.parquet")

    def test_needs_httpfs_local(self):
        """Test that local paths don't require httpfs."""
        assert not needs_httpfs("/local/path/file.parquet")


class TestSafeFileURL:
    """Test safe_file_url function."""

    def test_safe_file_url_https(self):
        """Test HTTPS URL handling."""
        url = "https://example.com/path/file.parquet"
        assert safe_file_url(url) == url

    def test_safe_file_url_https_with_spaces(self):
        """Test HTTPS URL with spaces gets encoded."""
        url = "https://example.com/path with spaces/file.parquet"
        result = safe_file_url(url)
        assert "path%20with%20spaces" in result

    def test_safe_file_url_s3(self):
        """Test S3 URL handling."""
        url = "s3://bucket/path/file.parquet"
        assert safe_file_url(url) == url

    def test_safe_file_url_local_nonexistent(self):
        """Test local file that doesn't exist."""
        with pytest.raises(BadParameter, match="Local file not found"):
            safe_file_url("/nonexistent/file.parquet")


@pytest.mark.network
class TestRemoteFileReading:
    """Test reading from actual remote files."""

    HTTPS_URL = "https://data.source.coop/nlebovits/gaul-l2-admin/by_country/USA.parquet"

    def test_metadata_https(self):
        """Test reading metadata from HTTPS URL."""
        from geoparquet_io.core.common import get_parquet_metadata

        metadata, schema = get_parquet_metadata(self.HTTPS_URL)
        assert metadata is not None
        assert schema is not None
        assert len(schema) > 0

    def test_geometry_column_https(self):
        """Test finding geometry column from HTTPS URL."""
        from geoparquet_io.core.common import find_primary_geometry_column

        geom_col = find_primary_geometry_column(self.HTTPS_URL)
        assert geom_col == "geometry"

    def test_bbox_structure_https(self):
        """Test checking bbox structure from HTTPS URL."""
        from geoparquet_io.core.common import check_bbox_structure

        bbox_info = check_bbox_structure(self.HTTPS_URL)
        assert bbox_info["has_bbox_column"] is True
        assert bbox_info["bbox_column_name"] == "geometry_bbox"

    def test_duckdb_query_https(self):
        """Test DuckDB query on HTTPS URL."""
        import duckdb

        con = duckdb.connect()
        result = con.execute(f"SELECT COUNT(*) FROM '{self.HTTPS_URL}'").fetchone()
        assert result[0] == 3145  # Known row count for USA.parquet

    def test_duckdb_spatial_query_https(self):
        """Test DuckDB spatial query on HTTPS URL."""
        from geoparquet_io.core.common import get_duckdb_connection

        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        query = f"""
        SELECT
            ST_GeometryType(geometry) as type,
            COUNT(*) as count
        FROM '{self.HTTPS_URL}'
        GROUP BY type
        """
        results = con.execute(query).fetchall()
        assert len(results) > 0
        # Should have POLYGON and MULTIPOLYGON
        geom_types = {row[0] for row in results}
        assert "POLYGON" in geom_types or "MULTIPOLYGON" in geom_types


import os


@pytest.mark.network
@pytest.mark.skipif(
    not (
        os.getenv("AWS_ACCESS_KEY_ID") or os.path.exists(os.path.expanduser("~/.aws/credentials"))
    ),
    reason="AWS credentials not configured",
)
class TestS3FileReading:
    """Test reading from S3 (requires AWS credentials)."""

    S3_URL = "s3://arg-fulbright-data/censo-argentino-2022/radios-2022.parquet"

    def test_duckdb_query_s3(self):
        """Test DuckDB query on S3 URL."""
        from geoparquet_io.core.common import get_duckdb_connection

        con = get_duckdb_connection(load_spatial=True, load_httpfs=True)
        result = con.execute(f"SELECT COUNT(*) FROM '{self.S3_URL}'").fetchone()
        assert result[0] > 0  # Should have rows

    def test_metadata_s3(self):
        """Test reading metadata from S3 URL."""
        from geoparquet_io.core.common import get_parquet_metadata

        metadata, schema = get_parquet_metadata(self.S3_URL)
        assert metadata is not None
        assert schema is not None
        assert len(schema) > 0


class TestGetDuckDBConnection:
    """Test DuckDB connection helper."""

    def test_get_connection_defaults(self):
        """Test connection with defaults."""
        from geoparquet_io.core.common import get_duckdb_connection

        con = get_duckdb_connection()
        # Should have spatial extension loaded
        result = con.execute("SELECT ST_Point(0, 0)").fetchone()
        assert result is not None
        con.close()

    def test_get_connection_with_httpfs(self):
        """Test connection with httpfs."""
        from geoparquet_io.core.common import get_duckdb_connection

        con = get_duckdb_connection(load_httpfs=True)
        # Should have httpfs loaded (can't easily test without actual S3 access)
        con.close()

    def test_get_connection_no_spatial(self):
        """Test connection without spatial."""
        from geoparquet_io.core.common import get_duckdb_connection

        con = get_duckdb_connection(load_spatial=False)
        # Spatial functions should not work
        with pytest.raises(Exception):
            con.execute("SELECT ST_Point(0, 0)").fetchone()
        con.close()
