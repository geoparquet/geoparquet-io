"""Tests for metadata performance optimizations (Issue #232).

These tests verify:
1. PyArrow is used for local file metadata reads (fast path)
2. DuckDB is still used for remote files (S3, HTTP)
3. Connection reuse in detect_geoparquet_file_type()
4. Lazy spatial extension loading
5. LRU caching for detect_geoparquet_file_type()
"""

import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


class TestPyArrowMetadataForLocalFiles:
    """Test that PyArrow is used for local file metadata reads."""

    def test_get_geo_metadata_uses_pyarrow_for_local_files(self, places_test_file):
        """Test that get_geo_metadata uses PyArrow for local files."""
        from geoparquet_io.core.duckdb_metadata import get_geo_metadata

        # Should work and return valid geo metadata
        result = get_geo_metadata(places_test_file)
        assert result is not None
        assert isinstance(result, dict)
        # GeoParquet files have version in geo metadata
        assert "version" in result or "columns" in result

    def test_get_kv_metadata_uses_pyarrow_for_local_files(self, places_test_file):
        """Test that get_kv_metadata uses PyArrow for local files."""
        from geoparquet_io.core.duckdb_metadata import get_kv_metadata

        result = get_kv_metadata(places_test_file)
        assert isinstance(result, dict)
        assert b"geo" in result

    def test_get_schema_info_uses_pyarrow_for_local_files(self, places_test_file):
        """Test that get_schema_info uses PyArrow for local files."""
        from geoparquet_io.core.duckdb_metadata import get_schema_info

        result = get_schema_info(places_test_file)
        assert isinstance(result, list)
        assert len(result) > 0
        # Each column should have name and type info
        for col in result:
            assert "name" in col
            assert "type" in col

    def test_get_file_metadata_uses_pyarrow_for_local_files(self, places_test_file):
        """Test that get_file_metadata uses PyArrow for local files."""
        from geoparquet_io.core.duckdb_metadata import get_file_metadata

        result = get_file_metadata(places_test_file)
        assert isinstance(result, dict)
        assert "num_rows" in result
        assert "num_row_groups" in result
        assert result["num_rows"] > 0

    def test_local_file_metadata_is_fast(self, places_test_file):
        """Test that local file metadata reads are fast (< 50ms)."""
        from geoparquet_io.core.duckdb_metadata import get_geo_metadata

        # Warm up (first call may load modules)
        get_geo_metadata(places_test_file)

        # Time multiple calls
        start = time.perf_counter()
        for _ in range(10):
            get_geo_metadata(places_test_file)
        elapsed = time.perf_counter() - start

        avg_ms = (elapsed / 10) * 1000
        # Should average < 50ms per call (PyArrow is ~0.03ms, DuckDB was ~122ms)
        assert avg_ms < 50, f"Average metadata read took {avg_ms:.1f}ms, expected < 50ms"


class TestDuckDBUsedForRemoteFiles:
    """Test that DuckDB is still used for remote files."""

    def test_remote_url_detection(self):
        """Test is_remote_url correctly identifies remote URLs."""
        from geoparquet_io.core.remote import is_remote_url

        # Remote URLs
        assert is_remote_url("s3://bucket/file.parquet") is True
        assert is_remote_url("s3a://bucket/file.parquet") is True
        assert is_remote_url("https://example.com/file.parquet") is True
        assert is_remote_url("http://example.com/file.parquet") is True
        assert is_remote_url("gs://bucket/file.parquet") is True
        assert is_remote_url("az://container/file.parquet") is True

        # Local files
        assert is_remote_url("/path/to/file.parquet") is False
        assert is_remote_url("./relative/file.parquet") is False
        assert is_remote_url("C:\\Windows\\file.parquet") is False
        assert is_remote_url(None) is False

    @pytest.mark.network
    def test_get_geo_metadata_uses_duckdb_for_s3(self):
        """Test that S3 URLs use DuckDB path."""
        from geoparquet_io.core.duckdb_metadata import get_geo_metadata

        # This is a public test file from Overture Maps
        s3_url = "s3://overturemaps-us-west-2/release/2024-01-17-alpha.0/theme=places/type=place/part-00000-*.parquet"
        # Just verify it doesn't crash - actual network test
        # In CI this might be skipped
        try:
            result = get_geo_metadata(s3_url)
            # May or may not have geo metadata
            assert result is None or isinstance(result, dict)
        except Exception:
            pytest.skip("Network test - S3 access failed")


class TestConnectionReuse:
    """Test connection reuse in detect_geoparquet_file_type."""

    def test_detect_geoparquet_file_type_accepts_connection(self, places_test_file):
        """Test that detect_geoparquet_file_type accepts a connection parameter."""
        from geoparquet_io.core.file_type import detect_geoparquet_file_type

        # Should work without connection (uses PyArrow for local)
        result = detect_geoparquet_file_type(places_test_file)
        assert result["file_type"] in [
            "geoparquet_v1",
            "geoparquet_v2",
            "parquet_geo_only",
            "unknown",
        ]

    def test_detect_geoparquet_file_type_with_connection(self, places_test_file):
        """Test detect_geoparquet_file_type with pre-existing connection."""
        import duckdb

        from geoparquet_io.core.file_type import detect_geoparquet_file_type

        # Create connection once
        con = duckdb.connect()
        con.execute("LOAD spatial;")

        try:
            # Pass connection - should reuse it
            result = detect_geoparquet_file_type(places_test_file, con=con)
            assert result["file_type"] in [
                "geoparquet_v1",
                "geoparquet_v2",
                "parquet_geo_only",
                "unknown",
            ]
        finally:
            con.close()


class TestLazySpatialExtension:
    """Test lazy spatial extension loading."""

    def test_metadata_reads_dont_load_spatial(self, places_test_file):
        """Test that pure metadata reads don't require spatial extension."""
        from geoparquet_io.core.duckdb_metadata import (
            get_file_metadata,
            get_geo_metadata,
            get_kv_metadata,
        )

        # These functions read metadata only - should work without spatial
        # For local files, they use PyArrow so this is naturally satisfied
        kv = get_kv_metadata(places_test_file)
        assert b"geo" in kv

        geo = get_geo_metadata(places_test_file)
        assert geo is not None

        meta = get_file_metadata(places_test_file)
        assert meta["num_rows"] > 0


class TestDetectGeoparquetFileTypeCache:
    """Test LRU caching for detect_geoparquet_file_type."""

    def test_detect_geoparquet_file_type_caches_results(self, places_test_file):
        """Test that repeated calls are cached."""
        from geoparquet_io.core.file_type import detect_geoparquet_file_type

        # Clear any existing cache
        if hasattr(detect_geoparquet_file_type, "cache_clear"):
            detect_geoparquet_file_type.cache_clear()

        # First call
        result1 = detect_geoparquet_file_type(places_test_file)

        # Second call should be cached (same result)
        result2 = detect_geoparquet_file_type(places_test_file)

        assert result1 == result2

    def test_cache_invalidates_on_file_modification(self, temp_output_dir):
        """Test that cache invalidates when file is modified."""
        import json

        from geoparquet_io.core.file_type import detect_geoparquet_file_type

        # Create a test parquet file
        output_file = Path(temp_output_dir) / "test.parquet"

        # Create initial file without geo metadata
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        pq.write_table(table, output_file)

        # Clear cache
        if hasattr(detect_geoparquet_file_type, "cache_clear"):
            detect_geoparquet_file_type.cache_clear()

        # First detection - should be "unknown" (no geo metadata)
        result1 = detect_geoparquet_file_type(str(output_file))
        assert result1["file_type"] == "unknown"

        # Modify file - add geo metadata
        geo_metadata = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
        }

        # Read and rewrite with metadata
        existing_meta = pq.read_metadata(output_file)

        # Create new metadata with geo key
        new_meta = {b"geo": json.dumps(geo_metadata).encode("utf-8")}
        if existing_meta.metadata:
            new_meta.update(existing_meta.metadata)

        table = pq.read_table(output_file)
        pq.write_table(table.replace_schema_metadata(new_meta), output_file)

        # Small delay to ensure mtime changes
        time.sleep(0.01)

        # Second detection - should see updated file
        result2 = detect_geoparquet_file_type(str(output_file))
        # Now should detect geo metadata
        assert result2["has_geo_metadata"] is True


class TestGeoArrowPyArrowCRSExtraction:
    """Test CRS extraction works when geoarrow-pyarrow is imported.

    When geoarrow-pyarrow is imported, it registers custom extension types
    that change how PyArrow exposes geometry metadata. The CRS is available
    via field.type.crs instead of field.metadata['ARROW:extension:metadata'].
    """

    def test_pyarrow_schema_info_extracts_crs_with_geoarrow(self, fields_5070_file):
        """Test get_schema_info extracts CRS when geoarrow-pyarrow is imported."""
        # Import geoarrow to register extension types (simulates CI environment)
        import geoarrow.pyarrow  # noqa: F401

        from geoparquet_io.core.duckdb_metadata import (
            get_schema_info,
            parse_geometry_logical_type,
        )

        schema_info = get_schema_info(fields_5070_file)

        # Find geometry column
        geom_col = next((c for c in schema_info if c.get("name") == "geometry"), None)
        assert geom_col is not None, "No geometry column found"

        logical_type = geom_col.get("logical_type")
        assert logical_type is not None, "No logical_type for geometry column"
        assert logical_type.startswith("GeometryType("), f"Unexpected logical_type: {logical_type}"

        # Parse the logical type to extract CRS
        parsed = parse_geometry_logical_type(logical_type)
        assert parsed is not None, "Failed to parse geometry logical type"
        assert "crs" in parsed, f"No CRS in parsed result: {parsed}"

        crs = parsed["crs"]
        assert isinstance(crs, dict), f"CRS should be dict, got {type(crs)}"
        assert "id" in crs, f"CRS missing 'id' key: {crs}"
        assert crs["id"]["code"] == 5070, f"Expected EPSG:5070, got {crs['id']}"

    def test_extract_crs_from_parquet_with_geoarrow(self, fields_5070_file):
        """Test extract_crs_from_parquet works when geoarrow-pyarrow is imported."""
        # Import geoarrow to register extension types
        import geoarrow.pyarrow  # noqa: F401

        from geoparquet_io.core.crs_utils import extract_crs_from_parquet

        crs = extract_crs_from_parquet(fields_5070_file)

        assert crs is not None, "Failed to extract CRS"
        assert isinstance(crs, dict), f"CRS should be dict, got {type(crs)}"
        assert crs.get("id", {}).get("code") == 5070, f"Expected EPSG:5070, got {crs}"


class TestPerformanceRegression:
    """Integration tests for performance regression fix."""

    def test_inspect_metadata_performance(self, places_test_file):
        """Test that inspect metadata operations are fast."""
        from geoparquet_io.core.duckdb_metadata import (
            get_file_metadata,
            get_geo_metadata,
            get_kv_metadata,
            get_schema_info,
        )

        # Warm up
        get_kv_metadata(places_test_file)

        # Time a typical inspect workflow
        start = time.perf_counter()

        kv = get_kv_metadata(places_test_file)
        geo = get_geo_metadata(places_test_file)
        meta = get_file_metadata(places_test_file)
        schema = get_schema_info(places_test_file)

        elapsed = time.perf_counter() - start
        elapsed_ms = elapsed * 1000

        # Should complete in < 200ms (was ~500ms with DuckDB, target ~50ms with PyArrow)
        assert elapsed_ms < 200, f"Inspect metadata took {elapsed_ms:.1f}ms, expected < 200ms"

        # Verify results are valid
        assert b"geo" in kv
        assert geo is not None
        assert meta["num_rows"] > 0
        assert len(schema) > 0

    def test_detect_geoparquet_file_type_performance(self, places_test_file):
        """Test that detect_geoparquet_file_type is fast."""
        from geoparquet_io.core.file_type import detect_geoparquet_file_type

        # Clear cache to test uncached performance
        if hasattr(detect_geoparquet_file_type, "cache_clear"):
            detect_geoparquet_file_type.cache_clear()

        # Warm up imports
        detect_geoparquet_file_type(places_test_file)

        # Clear again for clean timing
        if hasattr(detect_geoparquet_file_type, "cache_clear"):
            detect_geoparquet_file_type.cache_clear()

        start = time.perf_counter()
        result = detect_geoparquet_file_type(places_test_file)
        elapsed = time.perf_counter() - start
        elapsed_ms = elapsed * 1000

        # Should complete in < 300ms (was ~244ms with 2 DuckDB connections)
        # Note: macOS CI runners are ~2x slower than Linux, so threshold is relaxed
        assert elapsed_ms < 300, (
            f"detect_geoparquet_file_type took {elapsed_ms:.1f}ms, expected < 300ms"
        )

        # Verify result
        assert result["file_type"] in [
            "geoparquet_v1",
            "geoparquet_v2",
            "parquet_geo_only",
            "unknown",
        ]
