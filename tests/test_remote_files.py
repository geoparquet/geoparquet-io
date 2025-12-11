"""Tests for remote file support (HTTPS, S3, Azure, GCS)."""

import os

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

        # No special configuration needed - DuckDB handles public S3 buckets automatically
        con = get_duckdb_connection(load_spatial=True, load_httpfs=True)
        # Set S3 region for this specific bucket (was relocated from us-west-2 to us-east-2)
        con.execute("SET s3_region = 'us-east-2';")
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
        import duckdb

        from geoparquet_io.core.common import get_duckdb_connection

        con = get_duckdb_connection(load_spatial=False)
        # Spatial functions should not work
        with pytest.raises(duckdb.CatalogException):
            con.execute("SELECT ST_Point(0, 0)").fetchone()
        con.close()


class TestRemoteErrorHints:
    """Test error hint generation for remote file failures."""

    def test_get_remote_error_hint_403_s3(self):
        """Test hint for S3 403 Forbidden error."""
        from geoparquet_io.core.common import get_remote_error_hint

        hint = get_remote_error_hint("403 Forbidden", "s3://bucket/file.parquet")
        assert "AWS_ACCESS_KEY_ID" in hint
        assert "AWS_SECRET_ACCESS_KEY" in hint

    def test_get_remote_error_hint_403_azure(self):
        """Test hint for Azure 403 error."""
        from geoparquet_io.core.common import get_remote_error_hint

        hint = get_remote_error_hint("Access Denied", "az://container/file.parquet")
        assert "AZURE_STORAGE_ACCOUNT_NAME" in hint
        assert "AZURE_STORAGE_ACCOUNT_KEY" in hint

    def test_get_remote_error_hint_403_gcs(self):
        """Test hint for GCS 403 error."""
        from geoparquet_io.core.common import get_remote_error_hint

        hint = get_remote_error_hint("Forbidden", "gs://bucket/file.parquet")
        assert "GOOGLE_APPLICATION_CREDENTIALS" in hint

    def test_get_remote_error_hint_404(self):
        """Test hint for 404 Not Found error."""
        from geoparquet_io.core.common import get_remote_error_hint

        hint = get_remote_error_hint("404 Not Found", "https://example.com/file.parquet")
        assert "not found" in hint.lower()
        assert "verify" in hint.lower() or "check" in hint.lower()

    def test_get_remote_error_hint_timeout(self):
        """Test hint for timeout error."""
        from geoparquet_io.core.common import get_remote_error_hint

        hint = get_remote_error_hint("Connection timed out", "https://example.com/file.parquet")
        assert "timed out" in hint.lower()
        assert "network" in hint.lower()

    def test_get_remote_error_hint_connection(self):
        """Test hint for connection error."""
        from geoparquet_io.core.common import get_remote_error_hint

        hint = get_remote_error_hint("Unable to connect", "https://example.com/file.parquet")
        assert "connect" in hint.lower()
        assert "network" in hint.lower()


class TestConvertRemoteParquet:
    """Test convert functionality with remote parquet files."""

    def test_is_parquet_file(self):
        """Test parquet file detection."""
        from geoparquet_io.core.convert import _is_parquet_file

        assert _is_parquet_file("file.parquet")
        assert _is_parquet_file("https://example.com/file.parquet")
        assert _is_parquet_file("s3://bucket/file.parquet")
        assert _is_parquet_file("https://example.com/file.parquet?query=param")
        assert not _is_parquet_file("file.geojson")
        assert not _is_parquet_file("file.csv")

    @pytest.mark.network
    def test_convert_remote_parquet(self, tmp_path):
        """Test converting remote parquet file."""
        import warnings

        from geoparquet_io.core.convert import convert_to_geoparquet

        output = tmp_path / "output.parquet"
        url = "https://github.com/opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet"

        try:
            convert_to_geoparquet(
                input_file=url, output_file=str(output), skip_hilbert=True, verbose=False
            )
        except Exception as e:
            error_msg = str(e).lower()
            # Skip on transient network errors (SSL, connection, timeout)
            if any(
                term in error_msg
                for term in ["ssl", "connection", "timeout", "network", "certificate"]
            ):
                warnings.warn(f"Skipping due to transient network error: {e}", stacklevel=2)
                pytest.skip(f"Transient network error: {e}")
            raise  # Re-raise non-network errors

        assert output.exists()
        assert output.stat().st_size > 0


class TestSTACRemoteBlocking:
    """Test STAC generation blocks remote files appropriately."""

    def test_stac_item_blocks_remote_https(self):
        """Test STAC item generation blocks HTTPS URLs."""
        from click import ClickException

        from geoparquet_io.core.stac import generate_stac_item

        with pytest.raises(ClickException, match="STAC generation requires local"):
            generate_stac_item(
                parquet_file="https://example.com/file.parquet",
                bucket_prefix="s3://bucket/",
                verbose=False,
            )

    def test_stac_item_blocks_remote_s3(self):
        """Test STAC item generation blocks S3 URLs."""
        from click import ClickException

        from geoparquet_io.core.stac import generate_stac_item

        with pytest.raises(ClickException, match="STAC generation requires local"):
            generate_stac_item(
                parquet_file="s3://bucket/file.parquet", bucket_prefix="s3://bucket/", verbose=False
            )

    def test_stac_collection_blocks_remote(self):
        """Test STAC collection generation blocks remote directories."""
        from click import ClickException

        from geoparquet_io.core.stac import generate_stac_collection

        with pytest.raises(ClickException, match="requires a local directory"):
            generate_stac_collection(
                partition_dir="s3://bucket/partitions/",
                bucket_prefix="s3://bucket/",
                verbose=False,
            )
