"""Tests for streaming DuckDB write with footer rewrite."""

import json
import tempfile
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import pytest


class TestFooterRewrite:
    """Tests for footer metadata rewrite functionality."""

    @pytest.fixture
    def sample_parquet(self):
        """Create a sample parquet file without geo metadata."""
        import duckdb

        tmp_path = Path(tempfile.gettempdir()) / f"test_footer_{uuid.uuid4()}.parquet"

        # Create simple parquet via DuckDB (no geo metadata)
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute(f"""
            COPY (
                SELECT
                    1 as id,
                    ST_AsWKB(ST_Point(-122.4, 37.8)) as geometry
            ) TO '{tmp_path}' (FORMAT PARQUET)
        """)
        con.close()

        yield str(tmp_path)

        if tmp_path.exists():
            tmp_path.unlink()

    def test_rewrite_footer_adds_geo_metadata(self, sample_parquet):
        """Test that rewrite_footer_with_geo_metadata adds geo key."""
        from geoparquet_io.core.common import rewrite_footer_with_geo_metadata

        geo_meta = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "bbox": [-122.4, 37.8, -122.4, 37.8],
                }
            },
        }

        rewrite_footer_with_geo_metadata(sample_parquet, geo_meta)

        # Verify metadata was added
        pf = pq.ParquetFile(sample_parquet)
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata

        parsed = json.loads(metadata[b"geo"].decode("utf-8"))
        assert parsed["version"] == "1.1.0"
        assert parsed["primary_column"] == "geometry"
        assert "geometry" in parsed["columns"]

    def test_rewrite_footer_raises_for_remote_url(self):
        """Test that rewrite_footer_with_geo_metadata raises for remote URLs."""
        from geoparquet_io.core.common import rewrite_footer_with_geo_metadata

        geo_meta = {"version": "1.1.0", "primary_column": "geometry", "columns": {}}

        with pytest.raises(ValueError, match="only works on local files"):
            rewrite_footer_with_geo_metadata("s3://bucket/file.parquet", geo_meta)


class TestSQLMetadataComputation:
    """Tests for computing metadata via SQL queries."""

    def test_compute_bbox_via_sql(self):
        """Test bbox computation using DuckDB SQL."""
        import duckdb

        from geoparquet_io.core.common import compute_bbox_via_sql

        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial")

            query = """
                SELECT ST_Point(-122.4, 37.8) as geometry
                UNION ALL
                SELECT ST_Point(-122.0, 38.0) as geometry
            """

            bbox = compute_bbox_via_sql(con, query, "geometry")

            assert bbox is not None
            assert len(bbox) == 4
            assert bbox[0] == pytest.approx(-122.4, rel=1e-6)  # xmin
            assert bbox[1] == pytest.approx(37.8, rel=1e-6)  # ymin
            assert bbox[2] == pytest.approx(-122.0, rel=1e-6)  # xmax
            assert bbox[3] == pytest.approx(38.0, rel=1e-6)  # ymax
        finally:
            con.close()

    def test_compute_geometry_types_via_sql(self):
        """Test geometry type computation using DuckDB SQL."""
        import duckdb

        from geoparquet_io.core.common import compute_geometry_types_via_sql

        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial")

            query = """
                SELECT ST_Point(-122.4, 37.8) as geometry
                UNION ALL
                SELECT ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') as geometry
            """

            types = compute_geometry_types_via_sql(con, query, "geometry")

            assert "Point" in types
            assert "Polygon" in types
            assert len(types) == 2
        finally:
            con.close()
