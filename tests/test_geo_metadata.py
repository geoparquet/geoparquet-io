"""
Tests for geoparquet_io.core.geo_metadata module.

Tests GeoParquet metadata handling including parsing, creation,
and bbox covering metadata.
"""

import json

import pytest

from geoparquet_io.core.geo_metadata import (
    DEFAULT_GEOPARQUET_VERSION,
    GEOPARQUET_VERSIONS,
    GeoBlockOptions,
    QuerySource,
    SchemaSource,
    bounds_of,
    compose_geo_block,
    geometry_types_of,
    read_raw_geo,
    sanitized_carried_geo,
)


def _schema(*names):
    """A schema with a WKB column per name (a bbox struct for ``bbox``, a string otherwise)."""
    import pyarrow as pa

    bbox = pa.struct([(n, pa.float64()) for n in ("xmin", "ymin", "xmax", "ymax")])
    types = {"bbox": bbox, "h3": pa.string(), "s2": pa.string()}
    return pa.schema([(n, types.get(n, pa.binary())) for n in names])


# =============================================================================
# Tests for Constants
# =============================================================================


class TestGeoParquetVersions:
    """Test GeoParquet version configuration constants."""

    def test_contains_expected_versions(self):
        """All expected versions are defined."""
        assert "1.0" in GEOPARQUET_VERSIONS
        assert "1.1" in GEOPARQUET_VERSIONS
        assert "2.0" in GEOPARQUET_VERSIONS
        assert "parquet-geo-only" in GEOPARQUET_VERSIONS

    def test_version_structure(self):
        """Each version has required config keys."""
        for _version, config in GEOPARQUET_VERSIONS.items():
            assert "duckdb_param" in config
            assert "metadata_version" in config or config["metadata_version"] is None
            assert "rewrite_metadata" in config

    def test_v1_rewrite_metadata_true(self):
        """GeoParquet 1.x versions rewrite metadata."""
        assert GEOPARQUET_VERSIONS["1.0"]["rewrite_metadata"] is True
        assert GEOPARQUET_VERSIONS["1.1"]["rewrite_metadata"] is True

    def test_v2_rewrite_metadata_false(self):
        """GeoParquet 2.0 does not rewrite metadata."""
        assert GEOPARQUET_VERSIONS["2.0"]["rewrite_metadata"] is False

    def test_default_version(self):
        """Default version is 1.1."""
        assert DEFAULT_GEOPARQUET_VERSION == "1.1"


# =============================================================================
# Tests for read_raw_geo() (the raw diagnostic decoder)
# =============================================================================


class TestReadRawGeo:
    """Test parsing geo metadata from Parquet file metadata."""

    def test_parses_valid_metadata(self):
        """Parses valid geo metadata dict."""
        geo_dict = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB"}},
        }
        metadata = {b"geo": json.dumps(geo_dict).encode("utf-8")}

        result = read_raw_geo(metadata).value

        assert result is not None
        assert result["version"] == "1.1.0"
        assert result["primary_column"] == "geometry"

    def test_returns_none_for_none_metadata(self):
        """Returns None for None input."""
        assert read_raw_geo(None).value is None

    def test_returns_none_for_missing_geo_key(self):
        """Returns None when 'geo' key is missing."""
        metadata = {b"other": b"data"}
        assert read_raw_geo(metadata).value is None

    def test_returns_none_for_invalid_json(self):
        """Returns None for invalid JSON in geo key."""
        metadata = {b"geo": b"not valid json"}
        assert read_raw_geo(metadata).value is None

    def test_invalid_json_carries_the_error(self):
        """An undecodable value says why instead of raising."""
        result = read_raw_geo({b"geo": b"not valid json"})
        assert result.present and result.value is None
        assert "not valid JSON" in result.error

    def test_handles_empty_geo_dict(self):
        """Handles empty geo dict."""
        metadata = {b"geo": b"{}"}
        result = read_raw_geo(metadata).value

        assert result == {}


# =============================================================================
# Tests for sanitized_carried_geo() (the carried decoder compose_geo_block reads with)
# =============================================================================


class TestSanitizedCarriedGeo:
    """Test parsing existing geo metadata from original metadata."""

    def test_parses_valid_metadata(self):
        """Parses valid metadata dict."""
        geo_dict = {"version": "1.0.0", "primary_column": "geom"}
        metadata = {b"geo": json.dumps(geo_dict).encode("utf-8")}

        result = sanitized_carried_geo(metadata)

        assert result is not None
        assert result["version"] == "1.0.0"

    def test_returns_none_for_none(self):
        """Reads as absent for None input."""
        assert sanitized_carried_geo(None) == {}

    def test_returns_none_for_missing_key(self):
        """Returns None for missing geo key."""
        assert sanitized_carried_geo({b"other": b"data"}) == {}

    def test_returns_none_for_invalid_json(self):
        """Returns None for invalid JSON."""
        assert sanitized_carried_geo({b"geo": b"invalid"}) == {}


# =============================================================================
# Tests for SQL-based Metadata Computation
# =============================================================================


class TestComputeBboxViaSql:
    """Test SQL-based bounding box computation."""

    @pytest.fixture
    def spatial_con(self):
        """DuckDB connection with spatial extension."""
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        yield con
        con.close()

    def test_computes_bbox_from_points(self, spatial_con):
        """Computes bbox from point geometries."""
        spatial_con.execute("""
            CREATE TABLE test AS
            SELECT ST_Point(x, y) as geometry
            FROM (VALUES (0, 0), (10, 20), (-5, 15)) AS t(x, y)
        """)

        query = "SELECT * FROM test"
        bbox = bounds_of(QuerySource(spatial_con, query), "geometry")

        assert bbox is not None
        assert len(bbox) == 4
        assert bbox[0] == -5  # xmin
        assert bbox[1] == 0  # ymin
        assert bbox[2] == 10  # xmax
        assert bbox[3] == 20  # ymax

    def test_returns_none_for_empty_result(self, spatial_con):
        """Returns None for empty query result."""
        spatial_con.execute("""
            CREATE TABLE test AS
            SELECT ST_Point(0, 0) as geometry
            WHERE 1 = 0
        """)

        query = "SELECT * FROM test"
        bbox = bounds_of(QuerySource(spatial_con, query), "geometry")

        assert bbox is None

    def test_returns_none_for_missing_column(self, spatial_con):
        """Returns None when geometry column not in query."""
        spatial_con.execute("CREATE TABLE test AS SELECT 1 as id")

        query = "SELECT * FROM test"
        bbox = bounds_of(QuerySource(spatial_con, query), "geometry")

        assert bbox is None

    def test_an_invalid_query_raises(self, spatial_con):
        """A query that cannot bind raises, as the write it measures for would."""
        import duckdb

        with pytest.raises(duckdb.Error):
            bounds_of(QuerySource(spatial_con, "SELECT * FROM nonexistent"), "geometry")


class TestComputeGeometryTypesViaSql:
    """Test SQL-based geometry type detection."""

    @pytest.fixture
    def spatial_con(self):
        """DuckDB connection with spatial extension."""
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        yield con
        con.close()

    def test_detects_point_type(self, spatial_con):
        """Detects Point geometry type."""
        spatial_con.execute("""
            CREATE TABLE test AS
            SELECT ST_Point(0, 0) as geometry
        """)

        query = "SELECT * FROM test"
        types = geometry_types_of(QuerySource(spatial_con, query), "geometry")

        assert "Point" in types

    def test_detects_multiple_types(self, spatial_con):
        """Detects multiple geometry types."""
        spatial_con.execute("""
            CREATE TABLE test AS
            SELECT ST_Point(0, 0) as geometry
            UNION ALL
            SELECT ST_GeomFromText('LINESTRING(0 0, 1 1)') as geometry
        """)

        query = "SELECT * FROM test"
        types = geometry_types_of(QuerySource(spatial_con, query), "geometry")

        assert len(types) >= 2


# =============================================================================
# Integration Tests
# =============================================================================


class TestGeoMetadataIntegration:
    """Integration tests with real GeoParquet files."""

    def test_parse_places_metadata(self, places_test_file):
        """Parses metadata from real GeoParquet file."""
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(places_test_file)
        metadata = pf.schema_arrow.metadata

        if metadata:
            result = read_raw_geo(metadata).value
            if result:
                assert "version" in result
                assert "primary_column" in result

    def test_create_metadata_for_output(self, places_test_file):
        """Creates new metadata for output file."""
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(places_test_file)
        original_metadata = pf.schema_arrow.metadata

        result = compose_geo_block(
            SchemaSource(pf.schema_arrow, original_metadata),
            GeoBlockOptions("geometry", version="1.1", covering=None),
        )

        assert result["version"] == "1.1.0"
        assert result["columns"]["geometry"]["encoding"] == "WKB"

    def test_metadata_roundtrip(self, tmp_path):
        """Metadata survives JSON serialization roundtrip."""
        geo_meta = compose_geo_block(
            SchemaSource(_schema("geometry", "bbox")),
            GeoBlockOptions("geometry", version="1.1", edges="spherical"),
        )
        assert geo_meta["columns"]["geometry"]["covering"]

        # Serialize and deserialize
        json_str = json.dumps(geo_meta)
        restored = json.loads(json_str)

        assert restored == geo_meta


# =============================================================================
# Edge Cases
# =============================================================================


class TestGeoMetadataEdgeCases:
    """Test edge cases in geo metadata handling."""

    def test_special_characters_in_column_name(self):
        """Handles special characters in geometry column name."""
        result = compose_geo_block(
            SchemaSource(_schema("my geometry")),  # Space in name
            GeoBlockOptions("my geometry", version="1.1"),
        )

        assert "my geometry" in result["columns"]

    def test_unicode_in_metadata(self):
        """Handles Unicode in metadata."""
        custom = {"description": "Données géographiques"}

        result = compose_geo_block(
            SchemaSource(_schema("geometry")),
            GeoBlockOptions("geometry", version="1.1", custom_metadata=custom),
        )

        assert result["description"] == "Données géographiques"

    def test_deeply_nested_covering(self):
        """Nested index entries survive beside a bbox member; never without one (#954)."""
        custom = {
            "covering": {
                "h3": {"column": "h3", "resolution": 10},
                "s2": {"column": "s2", "level": 15},
            }
        }

        result = compose_geo_block(
            SchemaSource(_schema("geometry", "bbox", "h3", "s2")),
            GeoBlockOptions("geometry", version="1.1", custom_metadata=custom),
        )

        covering = result["columns"]["geometry"]["covering"]
        assert covering["h3"]["resolution"] == 10
        assert covering["s2"]["level"] == 15
        assert "bbox" in covering

        # Without a bbox column there is no spec-conformant member to write,
        # so the whole covering is omitted (geopandas indexes covering["bbox"]
        # unguarded).
        bboxless = compose_geo_block(
            SchemaSource(_schema("geometry", "h3", "s2")),
            GeoBlockOptions("geometry", version="1.1", custom_metadata=custom),
        )
        assert "covering" not in bboxless["columns"]["geometry"]
