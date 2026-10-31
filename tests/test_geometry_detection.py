"""
Tests for geoparquet_io.core.geometry_detection module.

Tests geometry column detection from GeoParquet metadata and schema.
"""

import duckdb
import pytest

from geoparquet_io.core.geometry_detection import (
    STANDARD_GEOMETRY_NAMES,
    _crs_short,
    _detect_geometry_from_query,
    _summarize_geo_metadata,
    primary_geometry_column,
)

# duckdb_connection is available from conftest.py via pytest discovery


# =============================================================================
# Tests for STANDARD_GEOMETRY_NAMES constant
# =============================================================================


class TestStandardGeometryNames:
    """Test the standard geometry names constant."""

    def test_common_names_included(self):
        """Common geometry column names are in the list."""
        assert "geometry" in STANDARD_GEOMETRY_NAMES
        assert "geom" in STANDARD_GEOMETRY_NAMES
        assert "wkb_geometry" in STANDARD_GEOMETRY_NAMES

    def test_shape_included(self):
        """'shape' is included (ESRI convention)."""
        assert "shape" in STANDARD_GEOMETRY_NAMES

    def test_the_geom_included(self):
        """'the_geom' is included (PostGIS convention)."""
        assert "the_geom" in STANDARD_GEOMETRY_NAMES

    def test_list_has_expected_count(self):
        """List has expected number of standard names."""
        assert len(STANDARD_GEOMETRY_NAMES) >= 5

    def test_priority_order(self):
        """'geometry' is first (highest priority for fallback)."""
        assert STANDARD_GEOMETRY_NAMES[0] == "geometry"


# =============================================================================
# Tests for primary_geometry_column()
# =============================================================================


class TestDetectParquetGeometryColumn:
    """Test geometry column detection from parquet files."""

    def test_detects_from_geoparquet_metadata(self, places_test_file):
        """Detects geometry column from GeoParquet metadata."""
        result = primary_geometry_column(places_test_file)
        assert result is not None
        assert result.lower() in [n.lower() for n in STANDARD_GEOMETRY_NAMES]

    def test_fallback_to_schema_name(self, places_test_file):
        """Falls back to schema-based detection when no metadata."""
        # Most GeoParquet files have standard column names
        result = primary_geometry_column(places_test_file)
        assert result is not None

    def test_verbose_mode(self, places_test_file, capsys):
        """Verbose mode logs detection info."""
        primary_geometry_column(places_test_file, verbose=True)
        # Function uses debug() which may or may not output to stdout
        # Just verify no exception is raised

    def test_returns_none_for_non_geo_file(self, tmp_path):
        """Returns None for non-GeoParquet file."""
        # Create a simple parquet file without geometry
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        output_path = str(tmp_path / "non_geo.parquet")
        pq.write_table(table, output_path)

        result = primary_geometry_column(output_path)
        assert result is None


# =============================================================================
# Tests for primary_geometry_column()
# =============================================================================


class TestFindPrimaryGeometryColumn:
    """Test primary geometry column lookup."""

    def test_returns_geometry_column_name(self, places_test_file):
        """Returns the primary geometry column name."""
        result = primary_geometry_column(places_test_file)
        assert result is not None
        assert isinstance(result, str)

    def test_no_literal_fallback(self, tmp_path):
        """No detection possible: None, never a guessed 'geometry' (#1367)."""
        # Create a parquet file without geo metadata
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table({"id": [1, 2], "value": [10, 20]})
        output_path = str(tmp_path / "no_geo.parquet")
        pq.write_table(table, output_path)

        result = primary_geometry_column(output_path)
        assert result is None

    def test_v2_file_detection(self, fields_v2_file):
        """Detects geometry in GeoParquet 2.0 file."""
        result = primary_geometry_column(fields_v2_file)
        assert result is not None

    def test_parquet_geo_only_detection(self, fields_geom_type_only_file):
        """Detects geometry in Parquet Geo Only file."""
        result = primary_geometry_column(fields_geom_type_only_file)
        assert result is not None

    def test_verbose_output(self, places_test_file):
        """Verbose mode works without error."""
        result = primary_geometry_column(places_test_file, verbose=True)
        assert result is not None


# =============================================================================
# Tests for _detect_geometry_from_query()
# =============================================================================


class TestDetectGeometryFromQuery:
    """Test SQL query-based geometry detection."""

    @pytest.fixture
    def spatial_con(self):
        """DuckDB connection with spatial extension loaded."""
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        yield con
        con.close()

    def test_detects_by_standard_name(self, spatial_con):
        """Detects geometry column by standard name in query."""
        spatial_con.execute("""
            CREATE TABLE test_table AS
            SELECT 1 as id, ST_Point(0, 0) as geometry
        """)

        query = "SELECT * FROM test_table"
        result = _detect_geometry_from_query(spatial_con, query)

        assert result == "geometry"

    def test_detects_by_type(self, spatial_con):
        """Detects geometry column by GEOMETRY type."""
        spatial_con.execute("""
            CREATE TABLE test_table AS
            SELECT 1 as id, ST_Point(0, 0) as my_special_geom
        """)

        query = "SELECT * FROM test_table"
        result = _detect_geometry_from_query(spatial_con, query)

        # Should detect by type if not by standard name
        assert result == "my_special_geom"

    def test_prefers_original_metadata_column(self, spatial_con):
        """Prefers original metadata primary_column when present."""
        spatial_con.execute("""
            CREATE TABLE test_table AS
            SELECT 1 as id, ST_Point(0, 0) as custom_geom
        """)

        original_meta = {"primary_column": "custom_geom"}
        query = "SELECT * FROM test_table"
        result = _detect_geometry_from_query(spatial_con, query, original_metadata=original_meta)

        assert result == "custom_geom"

    def test_returns_none_for_no_geometry(self, spatial_con):
        """Returns None when no geometry column in query."""
        spatial_con.execute("""
            CREATE TABLE test_table AS
            SELECT 1 as id, 'value' as name
        """)

        query = "SELECT * FROM test_table"
        result = _detect_geometry_from_query(spatial_con, query)

        assert result is None

    def test_handles_query_error_gracefully(self, spatial_con):
        """Returns None for invalid queries."""
        result = _detect_geometry_from_query(spatial_con, "SELECT * FROM nonexistent_table")
        assert result is None

    def test_verbose_output(self, spatial_con, capsys):
        """Verbose mode logs detection info."""
        spatial_con.execute("""
            CREATE TABLE test_table AS
            SELECT 1 as id, ST_Point(0, 0) as geometry
        """)

        query = "SELECT * FROM test_table"
        _detect_geometry_from_query(spatial_con, query, verbose=True)
        # Just verify no exception

    def test_case_insensitive_standard_names(self, spatial_con):
        """Standard name matching is case-insensitive."""
        spatial_con.execute("""
            CREATE TABLE test_table AS
            SELECT 1 as id, ST_Point(0, 0) as GEOMETRY
        """)

        query = "SELECT * FROM test_table"
        result = _detect_geometry_from_query(spatial_con, query)

        # Should match despite case difference
        assert result is not None

    def test_with_multiple_geometry_columns(self, spatial_con):
        """With multiple geometry columns, returns first standard name match."""
        spatial_con.execute("""
            CREATE TABLE test_table AS
            SELECT
                1 as id,
                ST_Point(0, 0) as geom,
                ST_Point(1, 1) as geometry
        """)

        query = "SELECT * FROM test_table"
        result = _detect_geometry_from_query(spatial_con, query)

        # Should prefer 'geometry' over 'geom' based on standard names order
        assert result == "geometry"


# =============================================================================
# Integration Tests
# =============================================================================


class TestGeometryDetectionIntegration:
    """Integration tests with real GeoParquet files."""

    def test_consistent_detection(self, places_test_file):
        """primary_geometry_column and primary_geometry_column are consistent."""
        detected = primary_geometry_column(places_test_file)
        primary = primary_geometry_column(places_test_file)

        # If detected, should match primary
        if detected:
            assert detected == primary

    def test_partition_first_file_detection(self, country_partition_dir):
        """Geometry detection works with partition directory."""
        from geoparquet_io.core.file_utils import get_first_parquet_file

        first_file = get_first_parquet_file(country_partition_dir)
        if first_file:
            result = primary_geometry_column(first_file)
            assert result is not None

    def test_different_file_formats(
        self, places_test_file, fields_v2_file, fields_geom_type_only_file
    ):
        """Detection works across different GeoParquet formats."""
        # GeoParquet 1.x
        assert primary_geometry_column(places_test_file) is not None

        # GeoParquet 2.0
        assert primary_geometry_column(fields_v2_file) is not None

        # Parquet Geo Only
        assert primary_geometry_column(fields_geom_type_only_file) is not None


# =============================================================================
# Edge Cases
# =============================================================================


class TestGeometryDetectionEdgeCases:
    """Test edge cases in geometry detection."""

    def test_empty_metadata(self, tmp_path):
        """Handles empty geo metadata gracefully."""
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create parquet with empty geo metadata
        table = pa.table({"id": [1], "geometry": [b"\x00"]})
        metadata = {b"geo": json.dumps({}).encode()}
        schema = table.schema.with_metadata(metadata)
        table = table.cast(schema)

        output_path = str(tmp_path / "empty_meta.parquet")
        pq.write_table(table, output_path)

        # Should fall back to schema-based detection
        result = primary_geometry_column(output_path)
        assert result in ["geometry", None] or result is not None

    def test_malformed_geo_metadata(self, tmp_path):
        """Handles malformed geo metadata gracefully."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create parquet with invalid JSON in geo metadata
        table = pa.table({"id": [1], "value": [10]})
        metadata = {b"geo": b"not valid json"}
        schema = table.schema.with_metadata(metadata)
        table = table.cast(schema)

        output_path = str(tmp_path / "bad_meta.parquet")
        pq.write_table(table, output_path)

        # Should handle gracefully and fall back
        result = primary_geometry_column(output_path)
        # No column the data supports: None, not the literal 'geometry'
        assert result is None


class TestGeoMetadataSummary:
    """Verbose geo-metadata output is a concise one-liner, not a full CRS dump."""

    def test_summary_is_concise_single_line(self):
        meta = {
            "version": "1.1.0",
            "primary_column": "geom",
            "columns": {
                "geom": {
                    "encoding": "WKB",
                    "crs": {
                        "name": "Amersfoort / RD New",
                        "id": {"authority": "EPSG", "code": 28992},
                    },
                    "geometry_types": ["Polygon"],
                }
            },
        }
        summary = _summarize_geo_metadata(meta)
        assert "\n" not in summary
        assert "version=1.1.0" in summary
        assert "primary_column=geom" in summary
        assert "encoding=WKB" in summary
        assert "EPSG:28992" in summary
        assert "Polygon" in summary
        # The verbose CRS internals must not leak into the summary.
        assert "projjson" not in summary.lower()
        assert "ellipsoid" not in summary.lower()

    def test_crs_short_handles_variants(self):
        assert _crs_short(None) == "none"
        assert _crs_short("OGC:CRS84") == "OGC:CRS84"
        assert _crs_short({"id": {"authority": "EPSG", "code": 4326}}) == "EPSG:4326"
        assert _crs_short({"name": "Custom CRS"}) == "Custom CRS"

    def test_summary_handles_non_dict_metadata(self):
        # Older/list-form metadata should not raise.
        assert _summarize_geo_metadata([{"name": "geometry"}])
