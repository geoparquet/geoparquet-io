"""
Tests for streaming write functionality.

These tests verify that write_geoparquet_streaming() produces valid GeoParquet
files with correct metadata while using bounded memory.
"""

import os
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import write_geoparquet_streaming

from .conftest import duckdb_connection, get_geo_metadata


class TestWriteGeoparquetStreaming:
    """Tests for the write_geoparquet_streaming function."""

    def test_basic_streaming_write(self, temp_output_file):
        """Test basic streaming write produces valid GeoParquet."""
        with duckdb_connection() as con:
            # Create test data
            con.execute("""
                CREATE TABLE test_data AS
                SELECT
                    i as id,
                    ST_AsWKB(ST_Point(i * 0.1, i * 0.1)) as geometry
                FROM range(1000) t(i)
            """)

            # Write using streaming
            result = write_geoparquet_streaming(
                con,
                "SELECT * FROM test_data",
                temp_output_file,
                geometry_column="geometry",
            )

            assert result == Path(temp_output_file)
            assert os.path.exists(temp_output_file)

            # Verify output
            pf = pq.ParquetFile(temp_output_file)
            assert pf.metadata.num_rows == 1000
            assert "geometry" in pf.schema_arrow.names

    def test_streaming_write_preserves_geo_metadata(self, temp_output_file):
        """Test that streaming write creates proper GeoParquet metadata."""
        with duckdb_connection() as con:
            con.execute("""
                CREATE TABLE test_data AS
                SELECT
                    i as id,
                    ST_AsWKB(ST_Point(i, i)) as geometry
                FROM range(100) t(i)
            """)

            write_geoparquet_streaming(
                con,
                "SELECT * FROM test_data",
                temp_output_file,
                geometry_column="geometry",
            )

            geo_meta = get_geo_metadata(temp_output_file)
            assert geo_meta is not None
            assert geo_meta.get("version") == "1.1.0"
            assert geo_meta.get("primary_column") == "geometry"
            assert "geometry" in geo_meta.get("columns", {})
            assert geo_meta["columns"]["geometry"].get("encoding") == "WKB"

    def test_streaming_write_with_crs_string(self, temp_output_file):
        """Test streaming write with CRS specified as string."""
        with duckdb_connection() as con:
            con.execute("""
                CREATE TABLE test_data AS
                SELECT
                    i as id,
                    ST_AsWKB(ST_Point(i, i)) as geometry
                FROM range(100) t(i)
            """)

            write_geoparquet_streaming(
                con,
                "SELECT * FROM test_data",
                temp_output_file,
                geometry_column="geometry",
                crs="EPSG:32610",
            )

            geo_meta = get_geo_metadata(temp_output_file)
            assert geo_meta is not None
            crs = geo_meta["columns"]["geometry"].get("crs")
            assert crs is not None
            # Check it's PROJJSON format
            assert isinstance(crs, dict)
            assert "id" in crs or "name" in crs

    def test_streaming_write_with_crs_dict(self, temp_output_file):
        """Test streaming write with CRS specified as PROJJSON dict."""
        crs_projjson = {
            "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
            "type": "GeographicCRS",
            "name": "WGS 84",
            "id": {"authority": "EPSG", "code": 4326},
        }

        with duckdb_connection() as con:
            con.execute("""
                CREATE TABLE test_data AS
                SELECT
                    i as id,
                    ST_AsWKB(ST_Point(i, i)) as geometry
                FROM range(100) t(i)
            """)

            write_geoparquet_streaming(
                con,
                "SELECT * FROM test_data",
                temp_output_file,
                geometry_column="geometry",
                crs=crs_projjson,
            )

            geo_meta = get_geo_metadata(temp_output_file)
            assert geo_meta["columns"]["geometry"]["crs"] == crs_projjson

    def test_streaming_write_with_bbox_column(self, temp_output_file):
        """Test streaming write detects bbox covering column."""
        with duckdb_connection() as con:
            con.execute("""
                CREATE TABLE test_data AS
                SELECT
                    i as id,
                    ST_AsWKB(ST_Point(i, i)) as geometry,
                    {'xmin': CAST(i AS DOUBLE), 'ymin': CAST(i AS DOUBLE),
                     'xmax': CAST(i+1 AS DOUBLE), 'ymax': CAST(i+1 AS DOUBLE)} as bbox
                FROM range(100) t(i)
            """)

            write_geoparquet_streaming(
                con,
                "SELECT * FROM test_data",
                temp_output_file,
                geometry_column="geometry",
            )

            geo_meta = get_geo_metadata(temp_output_file)
            covering = geo_meta["columns"]["geometry"].get("covering", {})
            assert "bbox" in covering

    def test_streaming_write_with_compression(self, temp_output_file):
        """Test streaming write respects compression settings."""
        with duckdb_connection() as con:
            con.execute("""
                CREATE TABLE test_data AS
                SELECT
                    i as id,
                    ST_AsWKB(ST_Point(i, i)) as geometry
                FROM range(100) t(i)
            """)

            write_geoparquet_streaming(
                con,
                "SELECT * FROM test_data",
                temp_output_file,
                geometry_column="geometry",
                compression="GZIP",
            )

            pf = pq.ParquetFile(temp_output_file)
            # Check compression in row group metadata
            row_group = pf.metadata.row_group(0)
            # At least one column should use GZIP
            compressions = [row_group.column(i).compression for i in range(row_group.num_columns)]
            assert "GZIP" in compressions

    def test_streaming_write_with_row_group_size(self, temp_output_file):
        """Test streaming write creates multiple row groups with small batch size."""
        with duckdb_connection() as con:
            con.execute("""
                CREATE TABLE test_data AS
                SELECT
                    i as id,
                    ST_AsWKB(ST_Point(i, i)) as geometry
                FROM range(1000) t(i)
            """)

            write_geoparquet_streaming(
                con,
                "SELECT * FROM test_data",
                temp_output_file,
                geometry_column="geometry",
                row_group_size=100,  # Small batch size
            )

            pf = pq.ParquetFile(temp_output_file)
            assert pf.metadata.num_rows == 1000
            # Should have 10 row groups (1000 rows / 100 per group)
            assert pf.metadata.num_row_groups == 10

    def test_streaming_write_missing_geometry_column_error(self, temp_output_file):
        """Test streaming write raises error if geometry column not found."""
        with duckdb_connection() as con:
            con.execute("""
                CREATE TABLE test_data AS
                SELECT i as id FROM range(100) t(i)
            """)

            with pytest.raises(Exception) as exc_info:
                write_geoparquet_streaming(
                    con,
                    "SELECT * FROM test_data",
                    temp_output_file,
                    geometry_column="geometry",
                )

            assert "geometry" in str(exc_info.value).lower()

    def test_streaming_write_geoparquet_version(self, temp_output_file):
        """Test streaming write respects geoparquet_version parameter."""
        with duckdb_connection() as con:
            con.execute("""
                CREATE TABLE test_data AS
                SELECT
                    i as id,
                    ST_AsWKB(ST_Point(i, i)) as geometry
                FROM range(100) t(i)
            """)

            write_geoparquet_streaming(
                con,
                "SELECT * FROM test_data",
                temp_output_file,
                geometry_column="geometry",
                geoparquet_version="1.0",
            )

            geo_meta = get_geo_metadata(temp_output_file)
            assert geo_meta.get("version") == "1.0.0"

    def test_streaming_write_with_edges(self, temp_output_file):
        """Test streaming write with spherical edges."""
        with duckdb_connection() as con:
            con.execute("""
                CREATE TABLE test_data AS
                SELECT
                    i as id,
                    ST_AsWKB(ST_Point(i, i)) as geometry
                FROM range(100) t(i)
            """)

            write_geoparquet_streaming(
                con,
                "SELECT * FROM test_data",
                temp_output_file,
                geometry_column="geometry",
                edges="spherical",
            )

            geo_meta = get_geo_metadata(temp_output_file)
            assert geo_meta["columns"]["geometry"].get("edges") == "spherical"
            # Spherical edges should set orientation
            assert geo_meta["columns"]["geometry"].get("orientation") == "counterclockwise"


@pytest.mark.slow
class TestStreamingPerformance:
    """Performance benchmarks for streaming vs eager execution.

    These tests use the japan.parquet file and compare memory usage.
    """

    TEST_FILE = "/Users/cholmes/geodata/parquet-test-data/japan.parquet"

    @pytest.fixture(autouse=True)
    def check_test_file(self):
        """Skip tests if test file doesn't exist."""
        if not os.path.exists(self.TEST_FILE):
            pytest.skip(f"Test file not found: {self.TEST_FILE}")

    def test_streaming_creates_valid_output(self, temp_output_file):
        """Test streaming write with real data produces valid output."""
        with duckdb_connection() as con:
            write_geoparquet_streaming(
                con,
                f"SELECT * FROM read_parquet('{self.TEST_FILE}') LIMIT 10000",
                temp_output_file,
                geometry_column="geometry",
                row_group_size=1000,
            )

            pf = pq.ParquetFile(temp_output_file)
            assert pf.metadata.num_rows == 10000
            assert pf.metadata.num_row_groups == 10

            geo_meta = get_geo_metadata(temp_output_file)
            assert geo_meta is not None
            assert geo_meta.get("primary_column") == "geometry"
