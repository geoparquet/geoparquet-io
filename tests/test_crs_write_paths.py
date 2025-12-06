"""
Tests to verify CRS appears in the correct locations for each GeoParquet version.

This test suite specifically validates:
- v1.0/v1.1: CRS ONLY in GeoParquet 'geo' metadata
- v2.0: CRS in BOTH Parquet schema AND GeoParquet 'geo' metadata
- parquet-geo-only: CRS ONLY in Parquet schema (no 'geo' metadata)

These tests establish a baseline before consolidating the dual-write path for v2.0.
"""

import json
import os

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.metadata_utils import parse_geometry_type_from_schema
from tests.conftest import has_geoparquet_metadata, has_native_geo_types

# Helper functions


def get_parquet_schema_crs(parquet_file, geometry_column="geometry"):
    """
    Extract CRS from Parquet native geometry type in schema.

    Returns:
        dict: PROJJSON CRS dict or None
    """
    pf = pq.ParquetFile(parquet_file)
    parquet_schema_str = str(pf.metadata.schema)

    geom_details = parse_geometry_type_from_schema(geometry_column, parquet_schema_str)
    if geom_details and "crs" in geom_details:
        return geom_details["crs"]
    return None


def get_metadata_crs(parquet_file, geometry_column="geometry"):
    """
    Extract CRS from GeoParquet 'geo' metadata.

    Returns:
        dict: PROJJSON CRS dict or None
    """
    pf = pq.ParquetFile(parquet_file)
    metadata = pf.schema_arrow.metadata

    if not metadata or b"geo" not in metadata:
        return None

    geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
    columns = geo_meta.get("columns", {})

    if geometry_column in columns:
        return columns[geometry_column].get("crs")

    return None


def extract_epsg_code(crs_dict):
    """Extract EPSG code from PROJJSON CRS dict."""
    if not crs_dict:
        return None

    # Handle PROJJSON format
    if "id" in crs_dict:
        id_info = crs_dict["id"]
        if isinstance(id_info, dict):
            authority = id_info.get("authority")
            code = id_info.get("code")
            if authority == "EPSG":
                return code

    return None


def crs_dicts_are_equivalent(crs1, crs2):
    """
    Compare two CRS dicts for equivalence.

    For these tests, we compare EPSG codes. More sophisticated comparison
    could check full PROJJSON equality.
    """
    if crs1 is None and crs2 is None:
        return True

    if crs1 is None or crs2 is None:
        return False

    epsg1 = extract_epsg_code(crs1)
    epsg2 = extract_epsg_code(crs2)

    return epsg1 == epsg2 and epsg1 is not None


# Test classes


class TestV1CRSLocation:
    """Test that v1.0 and v1.1 only write CRS to metadata, not Parquet schema."""

    @pytest.mark.parametrize("version", ["1.0", "1.1"])
    def test_v1_crs_only_in_metadata(self, fields_5070_file, temp_output_file, version):
        """
        Test v1.x has CRS ONLY in GeoParquet metadata.

        Expected:
        - GeoParquet 'geo' metadata: Has CRS
        - Parquet schema: NO CRS (uses WKB binary, not native types)
        """
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version=version,
        )

        # Should have GeoParquet metadata
        assert has_geoparquet_metadata(temp_output_file)

        # Should NOT have native geo types (v1.x uses WKB)
        assert not has_native_geo_types(temp_output_file)

        # CRS should be in metadata
        metadata_crs = get_metadata_crs(temp_output_file)
        assert metadata_crs is not None, f"v{version} missing CRS in GeoParquet metadata"
        assert extract_epsg_code(metadata_crs) == 5070

        # CRS should NOT be in Parquet schema (v1 uses WKB, not native types)
        schema_crs = get_parquet_schema_crs(temp_output_file)
        assert schema_crs is None, f"v{version} should not have CRS in Parquet schema"

    @pytest.mark.parametrize("version", ["1.0", "1.1"])
    def test_v1_default_crs_in_metadata(
        self, fields_geom_type_only_file, temp_output_file, version
    ):
        """
        Test v1.x with default CRS.

        Expected: May or may not write default CRS to metadata (implementation-dependent).
        """
        convert_to_geoparquet(
            fields_geom_type_only_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version=version,
        )

        # Should have GeoParquet metadata
        assert has_geoparquet_metadata(temp_output_file)

        # If CRS is present in metadata, it should be default (EPSG:4326)
        metadata_crs = get_metadata_crs(temp_output_file)
        if metadata_crs:
            epsg = extract_epsg_code(metadata_crs)
            assert epsg == 4326 or epsg is None


class TestV2CRSLocation:
    """Test that v2.0 writes CRS to BOTH Parquet schema AND metadata."""

    def test_v2_crs_in_both_locations(self, fields_5070_file, temp_output_file):
        """
        Test v2.0 has CRS in BOTH Parquet schema AND GeoParquet metadata.

        This is the key behavior we're testing - v2.0 currently does TWO writes:
        1. apply_crs_to_parquet() - writes CRS to Parquet schema
        2. add_crs_to_geoparquet_metadata() - writes CRS to 'geo' metadata

        Expected:
        - GeoParquet 'geo' metadata: Has CRS
        - Parquet schema: Has CRS
        - Both CRS should be identical
        """
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
        )

        # Should have GeoParquet metadata
        assert has_geoparquet_metadata(temp_output_file)

        # Should have native geo types
        assert has_native_geo_types(temp_output_file)

        # CRS should be in metadata
        metadata_crs = get_metadata_crs(temp_output_file)
        assert metadata_crs is not None, "v2.0 missing CRS in GeoParquet metadata"
        assert extract_epsg_code(metadata_crs) == 5070

        # CRS should be in Parquet schema
        schema_crs = get_parquet_schema_crs(temp_output_file)
        assert schema_crs is not None, "v2.0 missing CRS in Parquet schema"
        assert extract_epsg_code(schema_crs) == 5070

        # Both CRS should be equivalent
        assert crs_dicts_are_equivalent(metadata_crs, schema_crs), (
            "v2.0 CRS mismatch between schema and metadata"
        )

    def test_v2_default_crs_not_written(self, fields_geom_type_only_file, temp_output_file):
        """
        Test v2.0 with default CRS does not write explicit CRS.

        This is an optimization - default CRS (EPSG:4326) is implied.

        Expected:
        - GeoParquet 'geo' metadata: No CRS or default CRS
        - Parquet schema: No CRS or default CRS
        """
        from geoparquet_io.core.common import is_default_crs

        convert_to_geoparquet(
            fields_geom_type_only_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
        )

        # If CRS is present, it should be default
        metadata_crs = get_metadata_crs(temp_output_file)
        if metadata_crs:
            assert is_default_crs(metadata_crs)

    def test_v2_multiple_crs_values(self, buildings_gpkg_6933, temp_output_file):
        """
        Test v2.0 with different CRS (EPSG:6933).

        Verifies the dual-location CRS writing works for various CRS values.
        """
        if not os.path.exists(buildings_gpkg_6933):
            pytest.skip("buildings_test_6933.gpkg not available")

        convert_to_geoparquet(
            buildings_gpkg_6933,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
        )

        # Both locations should have EPSG:6933
        metadata_crs = get_metadata_crs(temp_output_file)
        schema_crs = get_parquet_schema_crs(temp_output_file)

        assert metadata_crs is not None
        assert schema_crs is not None
        assert extract_epsg_code(metadata_crs) == 6933
        assert extract_epsg_code(schema_crs) == 6933
        assert crs_dicts_are_equivalent(metadata_crs, schema_crs)


class TestParquetGeoOnlyCRSLocation:
    """Test that parquet-geo-only writes CRS ONLY to Parquet schema, not metadata."""

    def test_parquet_geo_only_crs_only_in_schema(self, fields_5070_file, temp_output_file):
        """
        Test parquet-geo-only has CRS ONLY in Parquet schema.

        Expected:
        - GeoParquet 'geo' metadata: NONE (parquet-geo-only has no metadata)
        - Parquet schema: Has CRS
        """
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="parquet-geo-only",
        )

        # Should NOT have GeoParquet metadata
        assert not has_geoparquet_metadata(temp_output_file)

        # Should have native geo types
        assert has_native_geo_types(temp_output_file)

        # CRS should be in Parquet schema
        schema_crs = get_parquet_schema_crs(temp_output_file)
        assert schema_crs is not None, "parquet-geo-only missing CRS in Parquet schema"
        assert extract_epsg_code(schema_crs) == 5070

        # CRS should NOT be in metadata (no metadata exists)
        metadata_crs = get_metadata_crs(temp_output_file)
        assert metadata_crs is None, "parquet-geo-only should not have GeoParquet metadata"

    def test_parquet_geo_only_default_crs(self, fields_geom_type_only_file, temp_output_file):
        """
        Test parquet-geo-only with default CRS.

        Expected: No explicit CRS written (default is implied).
        """
        convert_to_geoparquet(
            fields_geom_type_only_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="parquet-geo-only",
        )

        # Should not have metadata
        assert not has_geoparquet_metadata(temp_output_file)


class TestCRSConsistencyAcrossConversions:
    """Test that CRS remains consistent when converting between versions."""

    def test_parquet_geo_only_to_v2_crs_consistency(self, fields_5070_file, temp_output_file):
        """
        Test converting parquet-geo-only to v2.0 maintains CRS.

        parquet-geo-only (schema only) → v2.0 (schema + metadata)
        """
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
        )

        metadata_crs = get_metadata_crs(temp_output_file)
        schema_crs = get_parquet_schema_crs(temp_output_file)

        # Both should have EPSG:5070
        assert extract_epsg_code(metadata_crs) == 5070
        assert extract_epsg_code(schema_crs) == 5070
        assert crs_dicts_are_equivalent(metadata_crs, schema_crs)

    def test_parquet_geo_only_to_v1_crs_transfer(self, fields_5070_file, temp_output_file):
        """
        Test converting parquet-geo-only to v1.1 transfers CRS to metadata.

        parquet-geo-only (schema only) → v1.1 (metadata only)
        """
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="1.1",
        )

        # v1.1 should have CRS in metadata
        metadata_crs = get_metadata_crs(temp_output_file)
        assert extract_epsg_code(metadata_crs) == 5070

        # v1.1 should NOT have native geo types
        assert not has_native_geo_types(temp_output_file)

    def test_v2_to_parquet_geo_only_keeps_schema_crs(self, temp_output_dir):
        """
        Test converting v2.0 to parquet-geo-only keeps schema CRS, drops metadata.

        v2.0 (schema + metadata) → parquet-geo-only (schema only)
        """
        from geoparquet_io.core.convert import convert_to_geoparquet

        # First create a v2.0 file with CRS in both locations
        v2_file = os.path.join(temp_output_dir, "v2.parquet")
        final_file = os.path.join(temp_output_dir, "parquet_geo_only.parquet")

        # Create v2.0 file from test data with EPSG:5070
        test_data_dir = os.path.join(os.path.dirname(__file__), "data")
        fields_5070 = os.path.join(test_data_dir, "fields_pgo_5070_snappy.parquet")

        convert_to_geoparquet(fields_5070, v2_file, skip_hilbert=True, geoparquet_version="2.0")

        # Convert v2.0 to parquet-geo-only
        convert_to_geoparquet(
            v2_file, final_file, skip_hilbert=True, geoparquet_version="parquet-geo-only"
        )

        # Should have CRS in schema
        schema_crs = get_parquet_schema_crs(final_file)
        assert schema_crs is not None
        assert extract_epsg_code(schema_crs) == 5070

        # Should NOT have metadata
        assert not has_geoparquet_metadata(final_file)


class TestCRSPROJJSONFormat:
    """Test that CRS is stored in PROJJSON format in both locations."""

    def test_v2_crs_is_projjson(self, fields_5070_file, temp_output_file):
        """
        Test that v2.0 stores CRS in PROJJSON format in both locations.

        PROJJSON should have keys like: $schema, type, name, id, etc.
        """
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
        )

        metadata_crs = get_metadata_crs(temp_output_file)
        schema_crs = get_parquet_schema_crs(temp_output_file)

        # Both should be dicts (PROJJSON format)
        assert isinstance(metadata_crs, dict), "metadata CRS should be PROJJSON dict"
        assert isinstance(schema_crs, dict), "schema CRS should be PROJJSON dict"

        # PROJJSON should have 'id' key with authority/code
        assert "id" in metadata_crs, "metadata CRS missing 'id' key"
        assert "id" in schema_crs, "schema CRS missing 'id' key"

        # Verify PROJJSON structure
        assert isinstance(metadata_crs["id"], dict)
        assert "authority" in metadata_crs["id"]
        assert "code" in metadata_crs["id"]

    def test_parquet_geo_only_crs_is_projjson(self, fields_5070_file, temp_output_file):
        """Test that parquet-geo-only stores CRS in PROJJSON format in schema."""
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="parquet-geo-only",
        )

        schema_crs = get_parquet_schema_crs(temp_output_file)

        # Should be dict (PROJJSON format)
        assert isinstance(schema_crs, dict), "schema CRS should be PROJJSON dict"
        assert "id" in schema_crs, "schema CRS missing 'id' key"
