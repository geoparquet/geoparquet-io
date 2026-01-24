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


class TestMetadataPreparation:
    """Tests for preparing geo metadata from input."""

    def test_prepare_geo_metadata_preserves_from_input(self):
        """Test that metadata is preserved from input when flags are True."""
        from geoparquet_io.core.common import prepare_geo_metadata_for_streaming

        original_metadata = {
            b"geo": json.dumps(
                {
                    "version": "1.1.0",
                    "primary_column": "geometry",
                    "columns": {
                        "geometry": {
                            "encoding": "WKB",
                            "geometry_types": ["Point", "Polygon"],
                            "bbox": [-180, -90, 180, 90],
                        }
                    },
                }
            ).encode("utf-8")
        }

        result = prepare_geo_metadata_for_streaming(
            original_metadata=original_metadata,
            geometry_column="geometry",
            geoparquet_version="1.1",
            preserve_bbox=True,
            preserve_geometry_types=True,
            input_crs=None,
        )

        assert result["version"] == "1.1.0"
        assert result["columns"]["geometry"]["bbox"] == [-180, -90, 180, 90]
        assert result["columns"]["geometry"]["geometry_types"] == ["Point", "Polygon"]

    def test_prepare_geo_metadata_clears_bbox_when_not_preserved(self):
        """Test that bbox is cleared when preserve_bbox=False."""
        from geoparquet_io.core.common import prepare_geo_metadata_for_streaming

        original_metadata = {
            b"geo": json.dumps(
                {
                    "version": "1.1.0",
                    "primary_column": "geometry",
                    "columns": {
                        "geometry": {
                            "encoding": "WKB",
                            "geometry_types": ["Point"],
                            "bbox": [-180, -90, 180, 90],
                        }
                    },
                }
            ).encode("utf-8")
        }

        result = prepare_geo_metadata_for_streaming(
            original_metadata=original_metadata,
            geometry_column="geometry",
            geoparquet_version="1.1",
            preserve_bbox=False,
            preserve_geometry_types=True,
            input_crs=None,
        )

        # bbox should not be present (needs recomputation)
        assert "bbox" not in result["columns"]["geometry"]
        # geometry_types should still be preserved
        assert result["columns"]["geometry"]["geometry_types"] == ["Point"]

    def test_prepare_geo_metadata_adds_crs(self):
        """Test that CRS is added when provided."""
        from geoparquet_io.core.common import prepare_geo_metadata_for_streaming

        input_crs = {
            "type": "GeographicCRS",
            "name": "NAD83",
            "id": {"authority": "EPSG", "code": 4269},
        }

        result = prepare_geo_metadata_for_streaming(
            original_metadata=None,
            geometry_column="geometry",
            geoparquet_version="1.1",
            preserve_bbox=True,
            preserve_geometry_types=True,
            input_crs=input_crs,
        )

        assert result["columns"]["geometry"]["crs"] == input_crs

    def test_prepare_geo_metadata_clears_geometry_types_when_not_preserved(self):
        """Test that geometry_types is cleared when preserve_geometry_types=False."""
        from geoparquet_io.core.common import prepare_geo_metadata_for_streaming

        original_metadata = {
            b"geo": json.dumps(
                {
                    "version": "1.1.0",
                    "primary_column": "geometry",
                    "columns": {
                        "geometry": {
                            "encoding": "WKB",
                            "geometry_types": ["Point", "Polygon"],
                            "bbox": [-180, -90, 180, 90],
                        }
                    },
                }
            ).encode("utf-8")
        }

        result = prepare_geo_metadata_for_streaming(
            original_metadata=original_metadata,
            geometry_column="geometry",
            geoparquet_version="1.1",
            preserve_bbox=True,
            preserve_geometry_types=False,
            input_crs=None,
        )

        # geometry_types should not be present (needs recomputation)
        assert "geometry_types" not in result["columns"]["geometry"]
        # bbox should still be preserved
        assert result["columns"]["geometry"]["bbox"] == [-180, -90, 180, 90]

    def test_prepare_geo_metadata_skips_default_crs(self):
        """Test that default CRS (EPSG:4326) is not added."""
        from geoparquet_io.core.common import prepare_geo_metadata_for_streaming

        default_crs = {
            "type": "GeographicCRS",
            "name": "WGS 84",
            "id": {"authority": "EPSG", "code": 4326},
        }

        result = prepare_geo_metadata_for_streaming(
            original_metadata=None,
            geometry_column="geometry",
            geoparquet_version="1.1",
            preserve_bbox=True,
            preserve_geometry_types=True,
            input_crs=default_crs,
        )

        assert "crs" not in result["columns"]["geometry"]


class TestStreamingWrite:
    """Tests for write_geoparquet_via_duckdb function."""

    @pytest.fixture
    def output_file(self):
        """Create temp output path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_streaming_out_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        if tmp_path.exists():
            tmp_path.unlink()

    @pytest.fixture
    def sample_input(self):
        """Create sample input parquet with geo metadata."""
        import duckdb
        import pyarrow.parquet as pq

        tmp_path = Path(tempfile.gettempdir()) / f"test_streaming_in_{uuid.uuid4()}.parquet"

        # Create via DuckDB
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial")
            table = con.execute("""
                SELECT
                    1 as id,
                    ST_AsWKB(ST_Point(-122.4, 37.8)) as geometry
                UNION ALL
                SELECT
                    2 as id,
                    ST_AsWKB(ST_Point(-122.0, 38.0)) as geometry
            """).fetch_arrow_table()
        finally:
            con.close()

        # Add geo metadata
        geo_meta = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "bbox": [-122.4, 37.8, -122.0, 38.0],
                }
            },
        }
        new_meta = {b"geo": json.dumps(geo_meta).encode("utf-8")}
        table = table.replace_schema_metadata(new_meta)
        pq.write_table(table, str(tmp_path))

        yield str(tmp_path)

        if tmp_path.exists():
            tmp_path.unlink()

    def test_streaming_write_basic(self, sample_input, output_file):
        """Test basic streaming write preserves data and metadata."""
        import duckdb

        from geoparquet_io.core.common import (
            get_parquet_metadata,
            write_geoparquet_via_duckdb,
        )

        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial")

            original_metadata, _ = get_parquet_metadata(sample_input)
            query = f"SELECT * FROM read_parquet('{sample_input}')"

            write_geoparquet_via_duckdb(
                con=con,
                query=query,
                output_path=output_file,
                geometry_column="geometry",
                original_metadata=original_metadata,
                geoparquet_version="1.1",
                preserve_bbox=True,
                preserve_geometry_types=True,
            )
        finally:
            con.close()

        # Verify output
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows == 2

        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata

        geo = json.loads(metadata[b"geo"].decode("utf-8"))
        assert geo["version"] == "1.1.0"
        assert geo["columns"]["geometry"]["bbox"] == [-122.4, 37.8, -122.0, 38.0]

    def test_streaming_write_recalculates_bbox(self, sample_input, output_file):
        """Test that bbox is recalculated when preserve_bbox=False."""
        import duckdb

        from geoparquet_io.core.common import (
            get_parquet_metadata,
            write_geoparquet_via_duckdb,
        )

        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial")

            original_metadata, _ = get_parquet_metadata(sample_input)
            # Filter to just one point
            query = f"SELECT * FROM read_parquet('{sample_input}') WHERE id = 1"

            write_geoparquet_via_duckdb(
                con=con,
                query=query,
                output_path=output_file,
                geometry_column="geometry",
                original_metadata=original_metadata,
                geoparquet_version="1.1",
                preserve_bbox=False,  # Force recalculation
                preserve_geometry_types=True,
            )
        finally:
            con.close()

        # Verify bbox was recalculated (should be smaller, just one point)
        pf = pq.ParquetFile(output_file)
        geo = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))
        bbox = geo["columns"]["geometry"]["bbox"]

        # Should be point bbox, not original [-122.4, 37.8, -122.0, 38.0]
        assert bbox[0] == pytest.approx(-122.4, rel=1e-6)
        assert bbox[2] == pytest.approx(-122.4, rel=1e-6)  # xmax == xmin for single point
