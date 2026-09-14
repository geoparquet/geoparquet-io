"""Unit tests for validator bugs surfaced by the geoparquet-testing corpus.

Covers gpio issues:
- #581: V2-2/V2-3 must treat explicit-CRS84 metadata + absent native CRS as valid
- #582: edges vocabulary includes the 2.0 ellipsoidal values
- #584: empty geometries excluded from stats/bbox containment
- #585: validator reports FAILED instead of crashing on corrupt metadata
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.validate import (
    CheckStatus,
    _check_edges_valid,
    _check_native_geo_stats_contains_data,
    _check_v2_crs_consistency,
    _check_v2_crs_in_parquet_type,
    validate_geoparquet,
)

CRS84_PROJJSON = {
    "type": "GeographicCRS",
    "name": "WGS 84 (CRS84)",
    "id": {"authority": "OGC", "code": "CRS84"},
}
EPSG_4326_PROJJSON = {
    "type": "GeographicCRS",
    "name": "WGS 84",
    "id": {"authority": "EPSG", "code": 4326},
}
EPSG_3857_PROJJSON = {
    "type": "ProjectedCRS",
    "name": "WGS 84 / Pseudo-Mercator",
    "id": {"authority": "EPSG", "code": 3857},
}

SCHEMA_NO_CRS = [{"name": "geometry", "logical_type": "GeometryType()", "type": "binary"}]


def _geo_meta(crs):
    return {"columns": {"geometry": {"encoding": "WKB", "crs": crs}}}


class TestV2CrsDefaultEquivalence:
    """#581: explicit CRS84-equivalent metadata + absent native CRS is spec-valid."""

    @pytest.mark.parametrize("crs", [CRS84_PROJJSON, EPSG_4326_PROJJSON])
    def test_crs_in_parquet_type_passes_for_default_equivalent(self, crs):
        check = _check_v2_crs_in_parquet_type(_geo_meta(crs), SCHEMA_NO_CRS, "geometry")
        assert check.status == CheckStatus.PASSED, check.message

    def test_crs_in_parquet_type_fails_for_non_default(self):
        check = _check_v2_crs_in_parquet_type(
            _geo_meta(EPSG_3857_PROJJSON), SCHEMA_NO_CRS, "geometry"
        )
        assert check.status == CheckStatus.FAILED

    @pytest.mark.parametrize("crs", [CRS84_PROJJSON, EPSG_4326_PROJJSON])
    def test_consistency_passes_for_default_equivalent(self, crs):
        check = _check_v2_crs_consistency(_geo_meta(crs), SCHEMA_NO_CRS, "geometry")
        assert check.status == CheckStatus.PASSED, check.message

    def test_consistency_fails_for_genuine_mismatch(self):
        """Metadata 3857 vs absent native CRS (=CRS84) is a real inconsistency."""
        check = _check_v2_crs_consistency(_geo_meta(EPSG_3857_PROJJSON), SCHEMA_NO_CRS, "geometry")
        assert check.status == CheckStatus.FAILED

    def test_consistency_passes_when_both_declare_same_crs(self):
        schema = [
            {
                "name": "geometry",
                "logical_type": f"GeometryType(crs={json.dumps(EPSG_3857_PROJJSON)})",
                "type": "binary",
            }
        ]
        check = _check_v2_crs_consistency(_geo_meta(EPSG_3857_PROJJSON), schema, "geometry")
        assert check.status == CheckStatus.PASSED, check.message


class TestEdgesVocabulary:
    """#582: GeoParquet 2.0 allows the four ellipsoidal-geodesic edges values."""

    @pytest.mark.parametrize("edges", ["vincenty", "thomas", "andoyer", "karney"])
    def test_ellipsoidal_edges_valid_for_v2(self, edges):
        check = _check_edges_valid({"edges": edges}, "geometry", geo_version="2.0.0")
        assert check.status == CheckStatus.PASSED, check.message

    @pytest.mark.parametrize("edges", ["vincenty", "karney"])
    def test_ellipsoidal_edges_invalid_for_v1_1(self, edges):
        check = _check_edges_valid({"edges": edges}, "geometry", geo_version="1.1.0")
        assert check.status == CheckStatus.FAILED

    @pytest.mark.parametrize("version", ["1.1.0", "2.0.0"])
    def test_planar_and_spherical_valid_everywhere(self, version):
        for edges in ("planar", "spherical"):
            check = _check_edges_valid({"edges": edges}, "geometry", geo_version=version)
            assert check.status == CheckStatus.PASSED

    @pytest.mark.parametrize("version", ["1.1.0", "2.0.0"])
    def test_bogus_edges_always_invalid(self, version):
        check = _check_edges_valid({"edges": "geodesic"}, "geometry", geo_version=version)
        assert check.status == CheckStatus.FAILED


@pytest.fixture
def native_file_with_empty_point(tmp_path):
    """Native-typed file whose geo statistics (correctly) exclude a POINT EMPTY."""
    out = tmp_path / "empty.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (ST_GeomFromText('POINT (30 10)')),
            (ST_GeomFromText('POINT (40 40)')),
            (ST_GeomFromText('POINT EMPTY'))
          ) t(geometry)
        ) TO '{out.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
    """)
    con.close()
    return out


class TestEmptyGeometryContainment:
    """#584: empties have no extent and must not count as outside the stats bbox."""

    def test_native_stats_containment_ignores_empty(self, native_file_with_empty_point):
        con = get_duckdb_connection(load_spatial=True)
        try:
            check = _check_native_geo_stats_contains_data(
                str(native_file_with_empty_point), "geometry", con, sample_size=100
            )
        finally:
            con.close()
        # Two non-empty points must be checked and pass; a loose
        # PASSED-or-SKIPPED assertion could mask a containment regression.
        assert check.status == CheckStatus.PASSED, check.message


class TestSchemaInfoRegistrationIndependent:
    """Schema info must not change when geoarrow extension types get registered.

    Importing geoarrow.pyarrow anywhere in the process registers extension
    types globally, after which pyarrow reads native-GEOMETRY columns as
    extension fields. get_schema_info must keep reporting the storage type so
    checks like geometry_byte_array don't fail depending on test order.
    """

    def test_native_geometry_type_stable_after_geoarrow_import(self, native_file_with_empty_point):
        import geoarrow.pyarrow  # noqa: F401  (import registers extension types)

        from geoparquet_io.core.duckdb_metadata import get_schema_info

        (geom,) = [
            c for c in get_schema_info(str(native_file_with_empty_point)) if c["name"] == "geometry"
        ]
        assert "binary" in geom["type"].lower() or "BYTE_ARRAY" in geom["type"], geom["type"]
        assert (geom["logical_type"] or "").startswith("GeometryType"), geom["logical_type"]

        result = validate_geoparquet(str(native_file_with_empty_point), validate_data=False)
        byte_array_checks = [c for c in result.checks if c.name.startswith("geometry_byte_array")]
        assert all(c.status == CheckStatus.PASSED for c in byte_array_checks), [
            (c.name, c.message) for c in byte_array_checks
        ]


class TestValidatorNeverCrashes:
    """#585: corrupt metadata must produce FAILED checks, not exceptions."""

    def _write_with_geo_bytes(self, tmp_path, geo_bytes):
        table = pa.table({"a": [1], "geometry": [b"\x01\x01" + b"\x00" * 20]})
        table = table.replace_schema_metadata({b"geo": geo_bytes})
        out = tmp_path / "corrupt.parquet"
        pq.write_table(table, out)
        return out

    def test_invalid_utf8_geo_metadata_reports_failed(self, tmp_path):
        out = self._write_with_geo_bytes(tmp_path, b'\xff\xfe{"version"')
        result = validate_geoparquet(str(out), validate_data=False)
        assert not result.is_valid
        assert any(c.status == CheckStatus.FAILED for c in result.checks)

    def test_missing_version_auto_mode_reports_failed(self, tmp_path):
        geo = {
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": []}},
        }
        out = self._write_with_geo_bytes(tmp_path, json.dumps(geo).encode())
        result = validate_geoparquet(str(out), validate_data=False)
        assert not result.is_valid
        failed = [c.name for c in result.checks if c.status == CheckStatus.FAILED]
        assert any("version" in n for n in failed), failed
