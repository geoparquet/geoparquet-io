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
    _add_bbox_covering,
    _add_custom_covering,
    _initialize_geo_metadata,
    _parse_existing_geo_metadata,
    compute_bbox_via_sql,
    compute_geometry_types_via_sql,
    create_geo_metadata,
    parse_geo_metadata,
)

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
# Tests for parse_geo_metadata()
# =============================================================================


class TestParseGeoMetadata:
    """Test parsing geo metadata from Parquet file metadata."""

    def test_parses_valid_metadata(self):
        """Parses valid geo metadata dict."""
        geo_dict = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB"}},
        }
        metadata = {b"geo": json.dumps(geo_dict).encode("utf-8")}

        result = parse_geo_metadata(metadata)

        assert result is not None
        assert result["version"] == "1.1.0"
        assert result["primary_column"] == "geometry"

    def test_returns_none_for_none_metadata(self):
        """Returns None for None input."""
        assert parse_geo_metadata(None) is None

    def test_returns_none_for_missing_geo_key(self):
        """Returns None when 'geo' key is missing."""
        metadata = {b"other": b"data"}
        assert parse_geo_metadata(metadata) is None

    def test_returns_none_for_invalid_json(self):
        """Returns None for invalid JSON in geo key."""
        metadata = {b"geo": b"not valid json"}
        assert parse_geo_metadata(metadata) is None

    def test_verbose_mode(self, capsys):
        """Verbose mode logs parsing info."""
        geo_dict = {"version": "1.1.0", "primary_column": "geometry", "columns": {}}
        metadata = {b"geo": json.dumps(geo_dict).encode("utf-8")}

        result = parse_geo_metadata(metadata, verbose=True)

        assert result is not None

    def test_handles_empty_geo_dict(self):
        """Handles empty geo dict."""
        metadata = {b"geo": b"{}"}
        result = parse_geo_metadata(metadata)

        assert result == {}


# =============================================================================
# Tests for _parse_existing_geo_metadata()
# =============================================================================


class TestParseExistingGeoMetadata:
    """Test parsing existing geo metadata from original metadata."""

    def test_parses_valid_metadata(self):
        """Parses valid metadata dict."""
        geo_dict = {"version": "1.0.0", "primary_column": "geom"}
        metadata = {b"geo": json.dumps(geo_dict).encode("utf-8")}

        result = _parse_existing_geo_metadata(metadata)

        assert result is not None
        assert result["version"] == "1.0.0"

    def test_returns_none_for_none(self):
        """Returns None for None input."""
        assert _parse_existing_geo_metadata(None) is None

    def test_returns_none_for_missing_key(self):
        """Returns None for missing geo key."""
        assert _parse_existing_geo_metadata({b"other": b"data"}) is None

    def test_returns_none_for_invalid_json(self):
        """Returns None for invalid JSON."""
        assert _parse_existing_geo_metadata({b"geo": b"invalid"}) is None


# =============================================================================
# Tests for _initialize_geo_metadata()
# =============================================================================


class TestInitializeGeoMetadata:
    """Test geo metadata initialization."""

    def test_creates_minimal_structure(self):
        """Creates minimal valid structure from None."""
        result = _initialize_geo_metadata(None, "geometry")

        assert result["version"] == "1.1.0"  # default version
        assert result["primary_column"] == "geometry"
        assert result["columns"]["geometry"] == {}

    def test_custom_version(self):
        """Respects custom version parameter."""
        result = _initialize_geo_metadata(None, "geometry", version="2.0.0")

        assert result["version"] == "2.0.0"

    def test_upgrades_existing_metadata(self):
        """Updates version in existing metadata."""
        existing = {"version": "1.0.0", "primary_column": "geom", "columns": {"geom": {}}}

        result = _initialize_geo_metadata(existing, "geom", version="1.1.0")

        assert result["version"] == "1.1.0"
        assert result["primary_column"] == "geom"

    def test_adds_missing_columns(self):
        """Adds missing columns dict."""
        existing = {"version": "1.0.0", "primary_column": "geometry"}

        result = _initialize_geo_metadata(existing, "geometry")

        assert "columns" in result
        assert "geometry" in result["columns"]

    def test_adds_missing_geometry_column(self):
        """Adds entry for geometry column if missing."""
        existing = {"version": "1.0.0", "columns": {}}

        result = _initialize_geo_metadata(existing, "new_geom")

        assert "new_geom" in result["columns"]


# =============================================================================
# Tests for _add_bbox_covering()
# =============================================================================


class TestAddBboxCovering:
    """Test adding bbox covering metadata."""

    def test_adds_bbox_covering(self):
        """Adds bbox covering to geo metadata."""
        geo_meta = {"columns": {"geometry": {}}}
        bbox_info = {"has_bbox_column": True, "bbox_column_name": "bbox"}

        _add_bbox_covering(geo_meta, "geometry", bbox_info, verbose=False)

        covering = geo_meta["columns"]["geometry"]["covering"]
        assert covering["bbox"]["xmin"] == ["bbox", "xmin"]
        assert covering["bbox"]["ymin"] == ["bbox", "ymin"]
        assert covering["bbox"]["xmax"] == ["bbox", "xmax"]
        assert covering["bbox"]["ymax"] == ["bbox", "ymax"]

    def test_no_op_without_bbox(self):
        """Does nothing when bbox_info is None."""
        geo_meta = {"columns": {"geometry": {}}}

        _add_bbox_covering(geo_meta, "geometry", None, verbose=False)

        assert "covering" not in geo_meta["columns"]["geometry"]

    def test_no_op_when_has_bbox_false(self):
        """Does nothing when has_bbox_column is False."""
        geo_meta = {"columns": {"geometry": {}}}
        bbox_info = {"has_bbox_column": False}

        _add_bbox_covering(geo_meta, "geometry", bbox_info, verbose=False)

        assert "covering" not in geo_meta["columns"]["geometry"]

    def test_custom_bbox_column_name(self):
        """Uses custom bbox column name."""
        geo_meta = {"columns": {"geometry": {}}}
        bbox_info = {"has_bbox_column": True, "bbox_column_name": "geometry_bbox"}

        _add_bbox_covering(geo_meta, "geometry", bbox_info, verbose=False)

        covering = geo_meta["columns"]["geometry"]["covering"]
        assert covering["bbox"]["xmin"] == ["geometry_bbox", "xmin"]


# =============================================================================
# Tests for _add_custom_covering()
# =============================================================================


class TestAddCustomCovering:
    """Test adding custom covering metadata (H3, S2, etc.)."""

    def test_adds_custom_covering(self):
        """Adds custom covering to geo metadata."""
        geo_meta = {"columns": {"geometry": {}}}
        custom_metadata = {"covering": {"h3": {"column": "h3_index"}}}

        _add_custom_covering(geo_meta, "geometry", custom_metadata, verbose=False)

        assert geo_meta["columns"]["geometry"]["covering"]["h3"] == {"column": "h3_index"}

    def test_no_op_without_covering(self):
        """Does nothing when custom_metadata has no covering."""
        geo_meta = {"columns": {"geometry": {}}}
        custom_metadata = {"other_key": "value"}

        _add_custom_covering(geo_meta, "geometry", custom_metadata, verbose=False)

        assert "covering" not in geo_meta["columns"]["geometry"]

    def test_no_op_for_none(self):
        """Does nothing for None custom_metadata."""
        geo_meta = {"columns": {"geometry": {}}}

        _add_custom_covering(geo_meta, "geometry", None, verbose=False)

        assert "covering" not in geo_meta["columns"]["geometry"]

    def test_merges_with_existing_covering(self):
        """Merges with existing covering metadata."""
        geo_meta = {"columns": {"geometry": {"covering": {"bbox": {}}}}}
        custom_metadata = {"covering": {"h3": {"column": "h3"}}}

        _add_custom_covering(geo_meta, "geometry", custom_metadata, verbose=False)

        assert "bbox" in geo_meta["columns"]["geometry"]["covering"]
        assert "h3" in geo_meta["columns"]["geometry"]["covering"]


# =============================================================================
# Tests for create_geo_metadata()
# =============================================================================


class TestCreateGeoMetadata:
    """Test creating complete geo metadata."""

    def test_creates_basic_metadata(self):
        """Creates basic geo metadata structure."""
        result = create_geo_metadata(
            original_metadata=None,
            geom_col="geometry",
            bbox_info=None,
            verbose=False,
        )

        assert result["version"] == "1.1.0"
        assert result["primary_column"] == "geometry"
        assert result["columns"]["geometry"]["encoding"] == "WKB"

    def test_custom_version(self):
        """Respects version parameter."""
        result = create_geo_metadata(
            original_metadata=None,
            geom_col="geometry",
            bbox_info=None,
            version="2.0.0",
        )

        assert result["version"] == "2.0.0"

    def test_includes_bbox_covering(self):
        """Includes bbox covering when provided."""
        bbox_info = {"has_bbox_column": True, "bbox_column_name": "bbox"}

        result = create_geo_metadata(
            original_metadata=None,
            geom_col="geometry",
            bbox_info=bbox_info,
            verbose=False,
        )

        assert "covering" in result["columns"]["geometry"]
        assert "bbox" in result["columns"]["geometry"]["covering"]

    def test_includes_spherical_edges(self):
        """Adds edges and orientation for spherical data."""
        result = create_geo_metadata(
            original_metadata=None,
            geom_col="geometry",
            bbox_info=None,
            edges="spherical",
        )

        assert result["columns"]["geometry"]["edges"] == "spherical"
        assert result["columns"]["geometry"]["orientation"] == "counterclockwise"

    def test_preserves_existing_metadata(self):
        """Preserves data from original metadata."""
        original = {
            b"geo": json.dumps(
                {
                    "version": "1.0.0",
                    "primary_column": "geometry",
                    "columns": {"geometry": {"crs": {"id": {"authority": "EPSG", "code": 4326}}}},
                }
            ).encode()
        }

        result = create_geo_metadata(
            original_metadata=original,
            geom_col="geometry",
            bbox_info=None,
            version="1.1.0",
        )

        # Version should be updated
        assert result["version"] == "1.1.0"
        # CRS should be preserved
        assert result["columns"]["geometry"]["crs"] is not None

    def test_adds_top_level_custom_metadata(self):
        """Adds top-level custom metadata keys."""
        custom = {"custom_key": "custom_value", "covering": {}}

        result = create_geo_metadata(
            original_metadata=None,
            geom_col="geometry",
            bbox_info=None,
            custom_metadata=custom,
        )

        assert result["custom_key"] == "custom_value"

    def test_default_encoding_wkb(self):
        """Default encoding is WKB."""
        result = create_geo_metadata(
            original_metadata=None,
            geom_col="geometry",
            bbox_info=None,
        )

        assert result["columns"]["geometry"]["encoding"] == "WKB"


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
        bbox = compute_bbox_via_sql(spatial_con, query, "geometry")

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
        bbox = compute_bbox_via_sql(spatial_con, query, "geometry")

        assert bbox is None

    def test_returns_none_for_missing_column(self, spatial_con):
        """Returns None when geometry column not in query."""
        spatial_con.execute("CREATE TABLE test AS SELECT 1 as id")

        query = "SELECT * FROM test"
        bbox = compute_bbox_via_sql(spatial_con, query, "geometry")

        assert bbox is None

    def test_handles_invalid_query(self, spatial_con):
        """Returns None for invalid query."""
        bbox = compute_bbox_via_sql(spatial_con, "SELECT * FROM nonexistent", "geometry")

        assert bbox is None


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
        types = compute_geometry_types_via_sql(spatial_con, query, "geometry")

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
        types = compute_geometry_types_via_sql(spatial_con, query, "geometry")

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
            result = parse_geo_metadata(metadata)
            if result:
                assert "version" in result
                assert "primary_column" in result

    def test_create_metadata_for_output(self, places_test_file):
        """Creates new metadata for output file."""
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(places_test_file)
        original_metadata = pf.schema_arrow.metadata

        result = create_geo_metadata(
            original_metadata=original_metadata,
            geom_col="geometry",
            bbox_info=None,
            version="1.1.0",
        )

        assert result["version"] == "1.1.0"
        assert result["columns"]["geometry"]["encoding"] == "WKB"

    def test_metadata_roundtrip(self, tmp_path):
        """Metadata survives JSON serialization roundtrip."""
        geo_meta = create_geo_metadata(
            original_metadata=None,
            geom_col="geometry",
            bbox_info={"has_bbox_column": True, "bbox_column_name": "bbox"},
            edges="spherical",
        )

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
        result = create_geo_metadata(
            original_metadata=None,
            geom_col="my geometry",  # Space in name
            bbox_info=None,
        )

        assert "my geometry" in result["columns"]

    def test_unicode_in_metadata(self):
        """Handles Unicode in metadata."""
        custom = {"description": "Données géographiques"}

        result = create_geo_metadata(
            original_metadata=None,
            geom_col="geometry",
            bbox_info=None,
            custom_metadata=custom,
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

        result = create_geo_metadata(
            original_metadata=None,
            geom_col="geometry",
            bbox_info={"has_bbox_column": True, "bbox_column_name": "bbox"},
            custom_metadata=custom,
        )

        covering = result["columns"]["geometry"]["covering"]
        assert covering["h3"]["resolution"] == 10
        assert covering["s2"]["level"] == 15
        assert "bbox" in covering

        # Without a bbox column there is no spec-conformant member to write,
        # so the whole covering is omitted (geopandas indexes covering["bbox"]
        # unguarded).
        bboxless = create_geo_metadata(
            original_metadata=None,
            geom_col="geometry",
            bbox_info=None,
            custom_metadata=custom,
        )
        assert "covering" not in bboxless["columns"]["geometry"]
