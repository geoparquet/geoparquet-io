"""Tests for multi-geometry column support.

GeoParquet files can have multiple geometry columns (e.g., 'geometry' for point
locations and 'boundary' for polygon boundaries). This module tests that gpio
correctly preserves all geometry columns during conversion.

Key behaviors tested:
- Detection of all geometry columns from GeoParquet metadata
- Preservation of secondary geometry columns in output
- Metadata generation for all geometry columns
- Bbox/Hilbert computed from primary column only (documented behavior)
"""

import json
from pathlib import Path

import pyarrow.parquet as pq

from tests.fixtures.multi_geometry import (
    create_geoparquet_with_custom_column_name,
    create_multi_geometry_geoparquet,
    create_multi_geometry_geoparquet_different_crs,
    create_multi_geometry_with_custom_primary_name,
)


class TestMultiGeometryDetection:
    """Tests for detecting multiple geometry columns from input files."""

    def test_detect_all_geometry_columns_from_geoparquet(self, tmp_path):
        """Should detect all geometry columns from GeoParquet metadata."""
        from geoparquet_io.core.convert import detect_all_geometry_columns

        input_file = tmp_path / "multi_geom.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        columns = detect_all_geometry_columns(str(input_file))

        assert columns["primary"] == "geometry"
        assert "boundary" in columns["secondary"]
        assert len(columns["secondary"]) == 1

    def test_detect_preserves_column_metadata(self, tmp_path):
        """Should preserve per-column metadata (encoding, crs, geometry_types)."""
        from geoparquet_io.core.convert import detect_all_geometry_columns

        input_file = tmp_path / "multi_geom.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        columns = detect_all_geometry_columns(str(input_file))

        # Primary column metadata
        assert columns["metadata"]["geometry"]["encoding"] == "WKB"
        assert "Point" in columns["metadata"]["geometry"]["geometry_types"]

        # Secondary column metadata
        assert columns["metadata"]["boundary"]["encoding"] == "WKB"
        assert "Polygon" in columns["metadata"]["boundary"]["geometry_types"]

    def test_detect_single_geometry_file_returns_empty_secondary(self, tmp_path):
        """Single-geometry files should return empty secondary list."""
        import duckdb

        from geoparquet_io.core.convert import detect_all_geometry_columns

        input_file = tmp_path / "single_geom.parquet"
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"""
            COPY (
                SELECT 1 as id, ST_Point(0, 0) as geometry
            ) TO '{input_file}' (FORMAT PARQUET)
        """)
        con.close()

        columns = detect_all_geometry_columns(str(input_file))

        assert columns["primary"] is not None
        assert columns["secondary"] == []


class TestMultiGeometryConversion:
    """Tests for converting files with multiple geometry columns."""

    def test_convert_preserves_all_geometry_columns(self, tmp_path):
        """Converting should preserve all geometry columns in output."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        # Read output and verify both columns exist
        table = pq.read_table(str(output_file))
        column_names = [f.name for f in table.schema]

        assert "geometry" in column_names
        assert "boundary" in column_names

    def test_convert_preserves_geometry_metadata_for_all_columns(self, tmp_path):
        """Output metadata should include all geometry columns."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        # Read geo metadata
        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        # Both columns should be in the columns dict
        assert "geometry" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]

        # Both should have encoding
        assert geo_meta["columns"]["geometry"]["encoding"] == "WKB"
        assert geo_meta["columns"]["boundary"]["encoding"] == "WKB"

    def test_convert_preserves_primary_column_designation(self, tmp_path):
        """Output should preserve the primary_column from input."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert geo_meta["primary_column"] == "geometry"

    def test_convert_preserves_crs_for_all_columns(self, tmp_path):
        """CRS should be preserved for all geometry columns.

        Note: EPSG:4326 is the default CRS and may be omitted per GeoParquet spec.
        When CRS is missing, it implies EPSG:4326.
        """
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        # Both columns should have CRS preserved or be omitted (implying 4326)
        geom_crs = geo_meta["columns"]["geometry"].get("crs")
        boundary_crs = geo_meta["columns"]["boundary"].get("crs")

        # Per GeoParquet spec: missing CRS implies EPSG:4326
        # Either CRS is present with 4326, or it's omitted (None)
        if geom_crs is not None:
            assert geom_crs.get("id", {}).get("code") == 4326
        # Missing CRS is valid for 4326

        if boundary_crs is not None:
            assert boundary_crs.get("id", {}).get("code") == 4326
        # Missing CRS is valid for 4326

    def test_convert_preserves_different_crs_per_column(self, tmp_path):
        """Each geometry column's CRS should be preserved independently.

        Note: EPSG:4326 (primary) may be omitted per GeoParquet spec.
        EPSG:3857 (secondary) should be explicitly preserved.
        """
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet_different_crs(str(input_file))

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        # Primary (geometry) is 4326 - may be omitted
        geom_crs = geo_meta["columns"]["geometry"].get("crs")
        if geom_crs is not None:
            assert geom_crs.get("id", {}).get("code") == 4326

        # Secondary (boundary) is 3857 - must be explicitly preserved
        boundary_crs = geo_meta["columns"]["boundary"].get("crs", {})
        assert boundary_crs.get("id", {}).get("code") == 3857

    def test_convert_preserves_geometry_types(self, tmp_path):
        """geometry_types should be preserved for all columns."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        # Check geometry_types preserved
        assert "Point" in geo_meta["columns"]["geometry"].get("geometry_types", [])
        assert "Polygon" in geo_meta["columns"]["boundary"].get("geometry_types", [])


class TestMultiGeometryBboxAndHilbert:
    """Tests for bbox and Hilbert ordering with multiple geometry columns.

    DOCUMENTED BEHAVIOR: Bbox column and Hilbert ordering are computed based
    on the PRIMARY geometry column only. Secondary geometry columns are
    preserved but do not influence spatial indexing.
    """

    def test_bbox_added_for_primary_column_only(self, tmp_path):
        """Bbox covering metadata should reference primary column only."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        # Convert with bbox (GeoParquet 1.1)
        convert_to_geoparquet(
            str(input_file), str(output_file), skip_hilbert=True, geoparquet_version="1.1"
        )

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        secondary_meta = geo_meta["columns"]["boundary"]

        # Primary may have covering metadata for bbox
        # Secondary should NOT have covering (bbox is for primary geometry)
        assert "covering" not in secondary_meta or secondary_meta.get("covering") is None

    def test_hilbert_ordering_succeeds_with_multiple_columns(self, tmp_path):
        """Hilbert ordering should work (using primary column)."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        # Convert with Hilbert ordering
        convert_to_geoparquet(
            str(input_file), str(output_file), skip_hilbert=False, geoparquet_version="1.1"
        )

        # Verify file was created and both columns present
        assert Path(output_file).exists()

        table = pq.read_table(str(output_file))
        column_names = [f.name for f in table.schema]
        assert "geometry" in column_names
        assert "boundary" in column_names


class TestMultiGeometryRoundTrip:
    """Integration tests for round-trip conversion with multiple geometry columns."""

    def test_geoparquet_to_geoparquet_roundtrip(self, tmp_path):
        """GeoParquet -> GeoParquet should preserve all geometry columns."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        roundtrip_file = tmp_path / "roundtrip.parquet"

        create_multi_geometry_geoparquet(str(input_file))

        # First conversion
        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        # Second conversion (round-trip)
        convert_to_geoparquet(str(output_file), str(roundtrip_file), skip_hilbert=True)

        # Verify both geometry columns preserved through both conversions
        meta = pq.read_metadata(str(roundtrip_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert "geometry" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]
        assert geo_meta["primary_column"] == "geometry"

    def test_row_count_preserved(self, tmp_path):
        """Row count should be preserved through conversion."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"

        create_multi_geometry_geoparquet(str(input_file))

        input_table = pq.read_table(str(input_file))
        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)
        output_table = pq.read_table(str(output_file))

        assert len(input_table) == len(output_table)

    def test_data_integrity_preserved(self, tmp_path):
        """Actual geometry data should be preserved through conversion."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"

        create_multi_geometry_geoparquet(str(input_file))

        input_table = pq.read_table(str(input_file))
        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)
        output_table = pq.read_table(str(output_file))

        # Compare boundary column data (secondary geometry)
        input_boundary = input_table.column("boundary").to_pylist()
        output_boundary = output_table.column("boundary").to_pylist()

        assert input_boundary == output_boundary


class TestMultiGeometryVersions:
    """Tests for multi-geometry support across GeoParquet versions."""

    def test_geoparquet_1_0_preserves_multiple_columns(self, tmp_path):
        """GeoParquet 1.0 output should preserve all geometry columns."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(
            str(input_file), str(output_file), skip_hilbert=True, geoparquet_version="1.0"
        )

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert "geometry" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]
        assert geo_meta["version"] == "1.0.0"

    def test_geoparquet_1_1_preserves_multiple_columns(self, tmp_path):
        """GeoParquet 1.1 output should preserve all geometry columns."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(
            str(input_file), str(output_file), skip_hilbert=True, geoparquet_version="1.1"
        )

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert "geometry" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]
        assert geo_meta["version"] == "1.1.0"

    def test_geoparquet_2_0_preserves_multiple_columns(self, tmp_path):
        """GeoParquet 2.0 output should preserve all geometry columns."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(
            str(input_file), str(output_file), skip_hilbert=True, geoparquet_version="2.0"
        )

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert "geometry" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]
        assert geo_meta["version"] == "2.0.0"


class TestColumnNamePreservation:
    """Tests for preserving original geometry column names (issue #328).

    GeoParquet files can have non-standard geometry column names like
    "the_geom" or "my_geometry". These should be preserved during conversion,
    not renamed to "geometry".
    """

    def test_preserves_custom_primary_column_name(self, tmp_path):
        """Custom geometry column name should be preserved in output."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_geoparquet_with_custom_column_name(str(input_file), "the_geom")

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        # Verify column name is preserved
        table = pq.read_table(str(output_file))
        column_names = [f.name for f in table.schema]

        assert "the_geom" in column_names
        assert "geometry" not in column_names  # Should NOT be renamed

    def test_preserves_custom_name_in_metadata(self, tmp_path):
        """Custom column name should appear in output geo metadata."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_geoparquet_with_custom_column_name(str(input_file), "my_geometry")

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert geo_meta["primary_column"] == "my_geometry"
        assert "my_geometry" in geo_meta["columns"]
        assert "geometry" not in geo_meta["columns"]

    def test_multi_geometry_with_custom_primary_name(self, tmp_path):
        """Multi-geometry with custom primary name should preserve both columns."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_with_custom_primary_name(str(input_file), "the_geom")

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        # Verify both columns preserved with original names
        table = pq.read_table(str(output_file))
        column_names = [f.name for f in table.schema]

        assert "the_geom" in column_names
        assert "boundary" in column_names
        assert "geometry" not in column_names

        # Verify metadata
        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert geo_meta["primary_column"] == "the_geom"
        assert "the_geom" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]

    def test_hilbert_ordering_with_custom_column_name(self, tmp_path):
        """Hilbert ordering should work with custom geometry column names."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_geoparquet_with_custom_column_name(str(input_file), "the_geom")

        # Convert with Hilbert ordering (should not fail)
        convert_to_geoparquet(
            str(input_file), str(output_file), skip_hilbert=False, geoparquet_version="1.1"
        )

        # Verify file exists and column preserved
        assert Path(output_file).exists()
        table = pq.read_table(str(output_file))
        assert "the_geom" in [f.name for f in table.schema]


class TestWriteStrategiesWithMultiGeometry:
    """Tests for multi-geometry support across different write strategies.

    Ensures all write strategies (duckdb-kv, disk-rewrite, in-memory,
    streaming) correctly handle multiple geometry columns.

    These tests use write_parquet_with_metadata directly since convert_to_geoparquet
    doesn't expose write_strategy parameter.
    """

    def _setup_test_query(self, tmp_path, input_filename="input.parquet"):
        """Create test input and return connection + query for multi-geometry file."""

        from geoparquet_io.core.convert import detect_all_geometry_columns
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        input_file = tmp_path / input_filename
        create_multi_geometry_geoparquet(str(input_file))

        # Detect geometry columns
        geom_info = detect_all_geometry_columns(str(input_file))

        # Get connection and build query
        con = get_duckdb_connection(load_spatial=True)

        # Simple SELECT * preserves all columns
        query = f"SELECT * FROM read_parquet('{input_file}')"

        return con, query, geom_info, input_file

    def test_disk_rewrite_strategy_preserves_multi_geometry(self, tmp_path):
        """disk-rewrite strategy should preserve secondary geometry columns."""
        from geoparquet_io.core.common import write_parquet_with_metadata

        con, query, geom_info, _ = self._setup_test_query(tmp_path)
        output_file = tmp_path / "output.parquet"

        try:
            write_parquet_with_metadata(
                con,
                query,
                str(output_file),
                geoparquet_version="1.1",
                write_strategy="disk-rewrite",
                geometry_info=geom_info,
            )
        finally:
            con.close()

        # Verify both columns in metadata
        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert "geometry" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]
        assert geo_meta["columns"]["boundary"]["encoding"] == "WKB"

    def test_arrow_memory_strategy_preserves_multi_geometry(self, tmp_path):
        """in-memory strategy should preserve secondary geometry columns."""
        from geoparquet_io.core.common import write_parquet_with_metadata

        con, query, geom_info, _ = self._setup_test_query(tmp_path)
        output_file = tmp_path / "output.parquet"

        try:
            write_parquet_with_metadata(
                con,
                query,
                str(output_file),
                geoparquet_version="1.1",
                write_strategy="in-memory",
                geometry_info=geom_info,
            )
        finally:
            con.close()

        # Verify both columns in metadata
        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert "geometry" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]
        assert geo_meta["columns"]["boundary"]["encoding"] == "WKB"

    def test_arrow_streaming_strategy_preserves_multi_geometry(self, tmp_path):
        """streaming strategy should preserve secondary geometry columns."""
        from geoparquet_io.core.common import write_parquet_with_metadata

        con, query, geom_info, _ = self._setup_test_query(tmp_path)
        output_file = tmp_path / "output.parquet"

        try:
            write_parquet_with_metadata(
                con,
                query,
                str(output_file),
                geoparquet_version="1.1",
                write_strategy="streaming",
                geometry_info=geom_info,
            )
        finally:
            con.close()

        # Verify both columns in metadata
        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert "geometry" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]
        assert geo_meta["columns"]["boundary"]["encoding"] == "WKB"

    def test_all_strategies_preserve_crs_for_secondary_columns(self, tmp_path):
        """All write strategies should preserve CRS for secondary columns."""
        from geoparquet_io.core.common import write_parquet_with_metadata
        from geoparquet_io.core.convert import detect_all_geometry_columns
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        strategies = ["duckdb-kv", "disk-rewrite", "in-memory", "streaming"]

        for strategy in strategies:
            input_file = tmp_path / f"input_{strategy}.parquet"
            output_file = tmp_path / f"output_{strategy}.parquet"
            create_multi_geometry_geoparquet_different_crs(str(input_file))

            # Detect geometry columns
            geom_info = detect_all_geometry_columns(str(input_file))

            con = get_duckdb_connection(load_spatial=True)
            query = f"SELECT * FROM read_parquet('{input_file}')"

            try:
                write_parquet_with_metadata(
                    con,
                    query,
                    str(output_file),
                    geoparquet_version="1.1",
                    write_strategy=strategy,
                    geometry_info=geom_info,
                )
            finally:
                con.close()

            meta = pq.read_metadata(str(output_file))
            geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

            # Secondary column (boundary) should have EPSG:3857 preserved
            boundary_crs = geo_meta["columns"]["boundary"].get("crs", {})
            assert boundary_crs.get("id", {}).get("code") == 3857, (
                f"Strategy {strategy} did not preserve CRS for secondary column"
            )


class TestMultiGeometryReproject:
    """Reproject must not strip a secondary column's derived stats (#890).

    ``gpio convert reproject`` transforms the PRIMARY geometry column only —
    the query is ``SELECT * EXCLUDE (geometry), ST_Transform(geometry, ...)``,
    so a secondary column's bytes reach the output byte-for-byte. Its carried
    ``geometry_types``/``bbox`` therefore still describe the rows and must
    survive; blanket-invalidating them left the output without the
    ``geometry_types`` GeoParquet 1.1 requires, and DuckDB refuses to open such
    a file at all: "Geoparquet column 'boundary' does not have geometry types".
    """

    def _reproject(self, tmp_path, name, **kwargs):
        from geoparquet_io.core.reproject import reproject

        input_file = tmp_path / f"in_{name}.parquet"
        output_file = tmp_path / f"out_{name}.parquet"
        create_multi_geometry_geoparquet(str(input_file))
        reproject(str(input_file), str(output_file), target_crs="EPSG:3857", **kwargs)
        meta = pq.read_metadata(str(output_file))
        return output_file, json.loads(meta.metadata[b"geo"].decode("utf-8"))

    def test_secondary_column_keeps_geometry_types(self, tmp_path):
        _, geo_meta = self._reproject(tmp_path, "types")
        boundary = geo_meta["columns"]["boundary"]
        assert boundary["geometry_types"] == ["Polygon"], boundary

    def test_secondary_column_keeps_its_bbox(self, tmp_path):
        """The untransformed column's coordinates never moved, so its bbox holds."""
        _, geo_meta = self._reproject(tmp_path, "bbox")
        assert geo_meta["columns"]["boundary"]["bbox"] == [-0.5, -0.5, 2.5, 2.5]

    def test_primary_column_stats_are_still_recomputed(self, tmp_path):
        """The transformed column's carried degree-space stats must NOT survive."""
        _, geo_meta = self._reproject(tmp_path, "primary")
        geometry = geo_meta["columns"]["geometry"]
        assert geometry["geometry_types"] == ["Point"]
        # Reprojected to Web Mercator: meters, not the carried [0, 0, 2, 2].
        assert geometry["bbox"][2] > 1000, geometry["bbox"]

    def test_streaming_path_also_keeps_secondary_stats(self, tmp_path):
        """The pipe path strips the same way and needed the same scoping."""
        from geoparquet_io.core.reproject import _reproject_streaming

        input_file = tmp_path / "in_stream.parquet"
        output_file = tmp_path / "out_stream.parquet"
        create_multi_geometry_geoparquet(str(input_file))
        _reproject_streaming(
            str(input_file),
            str(output_file),
            "EPSG:3857",
            None,
            "ZSTD",
            None,
            False,
            None,
            None,
        )
        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))
        assert geo_meta["columns"]["boundary"]["geometry_types"] == ["Polygon"]

    def test_duckdb_can_read_the_output(self, tmp_path):
        """The end-user symptom of #890: the output was unreadable outright."""
        import duckdb

        output_file, _ = self._reproject(tmp_path, "duckdb")
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            rows = con.execute(f"SELECT * FROM read_parquet('{output_file.as_posix()}')").fetchall()
        finally:
            con.close()
        assert len(rows) == 3


class TestMultiGeometryDerivedStatsInvalidation:
    """A row-changing write must leave a secondary column readable (#934).

    ``geometry_types`` is REQUIRED by GeoParquet 1.1, and only the PRIMARY
    geometry column's is recomputed on a file-write path. Deleting the key to
    mark it stale therefore left a secondary column with no ``geometry_types``
    at all, which DuckDB refuses to open — "Geoparquet column 'boundary' does
    not have geometry types" — and which ``gpio check spec`` fails. gpio wrote a
    file gpio rejects.

    Two answers, chosen per site by whether the carried stats OVER- or
    UNDER-cover the output:

    * A row filter over a single file keeps a subset of the rows, so the
      secondary column's carried stats still cover it — the strip is scoped to
      the primary and the real ``["Polygon"]`` survives.
    * A multi-file merge or a partition split carries the FIRST file's (or the
      whole input's) stats, which under-cover or misdescribe the output. Those
      cannot be kept, so the key is emptied to ``[]`` — the spec's "not known"
      — rather than deleted.
    """

    @staticmethod
    def _geo(path):
        return json.loads(pq.read_metadata(str(path)).metadata[b"geo"].decode("utf-8"))

    @staticmethod
    def _assert_duckdb_reads(path):
        import duckdb

        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            return con.execute(f"SELECT * FROM read_parquet('{Path(path).as_posix()}')").fetchall()
        finally:
            con.close()

    @staticmethod
    def _assert_spec_valid(path):
        from geoparquet_io.core.validate import validate_geoparquet

        failed = sorted(
            c.name for c in validate_geoparquet(str(path)).checks if c.status.value == "failed"
        )
        assert failed == [], f"gpio check spec failed on gpio's own output: {failed}"

    def _glob_input(self, tmp_path):
        """Two identical files behind a glob — the multi-file merge shape."""
        folder = tmp_path / "parts"
        folder.mkdir()
        for name in ("a.parquet", "b.parquet"):
            create_multi_geometry_geoparquet(str(folder / name))
        return str(folder / "*.parquet")

    # --- extract: row filter over a single file (scoped) --------------------

    def test_extract_where_keeps_the_secondary_readable(self, tmp_path):
        from geoparquet_io.core.extract import extract

        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        create_multi_geometry_geoparquet(str(input_file))
        extract(str(input_file), str(output_file), where="id = 1")

        boundary = self._geo(output_file)["columns"]["boundary"]
        assert boundary["geometry_types"] == ["Polygon"]
        assert len(self._assert_duckdb_reads(output_file)) == 1
        self._assert_spec_valid(output_file)

    def test_extract_bbox_keeps_the_secondary_readable(self, tmp_path):
        from geoparquet_io.core.extract import extract

        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        create_multi_geometry_geoparquet(str(input_file))
        extract(str(input_file), str(output_file), bbox="0,0,4,4")

        boundary = self._geo(output_file)["columns"]["boundary"]
        assert boundary["geometry_types"] == ["Polygon"]
        self._assert_duckdb_reads(output_file)
        self._assert_spec_valid(output_file)

    def test_extract_still_retightens_the_primary(self, tmp_path):
        """Scoping must not stop the primary's own stats being recomputed."""
        from geoparquet_io.core.extract import extract

        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        create_multi_geometry_geoparquet(str(input_file))
        extract(str(input_file), str(output_file), where="id = 1")

        # Input bbox was [0, 0, 2, 2]; only the (0, 0) point survives.
        assert self._geo(output_file)["columns"]["geometry"]["bbox"] == [0.0, 0.0, 0.0, 0.0]

    def test_extract_streaming_to_a_file_keeps_the_secondary_readable(self, tmp_path):
        """stdin-shaped input with a file output takes the other extract path."""
        from geoparquet_io.core.extract import _extract_streaming

        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        create_multi_geometry_geoparquet(str(input_file))
        _extract_streaming(
            str(input_file),
            str(output_file),
            None,
            None,
            None,
            None,
            "id = 1",
            None,
            False,
            "ZSTD",
            None,
            None,
            None,
            None,
            None,
        )

        boundary = self._geo(output_file)["columns"]["boundary"]
        assert boundary["geometry_types"] == ["Polygon"]
        self._assert_duckdb_reads(output_file)
        self._assert_spec_valid(output_file)

    # --- extract: multi-file merge (emptied) --------------------------------

    def test_extract_over_a_glob_empties_rather_than_deletes(self, tmp_path):
        """The first file's stats under-cover the merge, so they cannot be kept."""
        from geoparquet_io.core.extract import extract

        output_file = tmp_path / "out.parquet"
        extract(self._glob_input(tmp_path), str(output_file))

        assert self._geo(output_file)["columns"]["boundary"]["geometry_types"] == []
        assert len(self._assert_duckdb_reads(output_file)) == 6
        self._assert_spec_valid(output_file)

    # --- sort: multi-file merge (emptied) -----------------------------------

    def test_sort_by_column_over_a_glob(self, tmp_path):
        from geoparquet_io.core.sort_by_column import sort_by_column

        output_file = tmp_path / "out.parquet"
        sort_by_column(self._glob_input(tmp_path), str(output_file), columns="id")

        assert self._geo(output_file)["columns"]["boundary"]["geometry_types"] == []
        assert len(self._assert_duckdb_reads(output_file)) == 6
        self._assert_spec_valid(output_file)

    def test_sort_by_quadkey_over_a_glob(self, tmp_path):
        from geoparquet_io.core.sort_quadkey import sort_by_quadkey

        output_file = tmp_path / "out.parquet"
        sort_by_quadkey(self._glob_input(tmp_path), str(output_file))

        assert self._geo(output_file)["columns"]["boundary"]["geometry_types"] == []
        assert len(self._assert_duckdb_reads(output_file)) == 6
        self._assert_spec_valid(output_file)

    def test_sort_over_a_single_file_still_carries_real_stats(self, tmp_path):
        """No invalidation at all for a single file — nothing about it is stale."""
        from geoparquet_io.core.sort_by_column import sort_by_column

        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        create_multi_geometry_geoparquet(str(input_file))
        sort_by_column(str(input_file), str(output_file), columns="id")

        assert self._geo(output_file)["columns"]["boundary"]["geometry_types"] == ["Polygon"]

    # --- partition (emptied) -------------------------------------------------

    def test_partition_writes_readable_partitions(self, tmp_path):
        """Each partition holds a subset, and the carried stats describe the whole."""
        from geoparquet_io.core.partition.common import partition_by_column

        input_file = tmp_path / "in.parquet"
        output_folder = tmp_path / "parts_out"
        create_multi_geometry_geoparquet(str(input_file))
        partition_by_column(
            str(input_file), str(output_folder), "name", force=True, skip_analysis=True
        )

        written = sorted(output_folder.rglob("*.parquet"))
        assert written, "partitioning produced no files"
        for part in written:
            assert self._geo(part)["columns"]["boundary"]["geometry_types"] == []
            self._assert_duckdb_reads(part)
            self._assert_spec_valid(part)
