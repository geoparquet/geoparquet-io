"""Unit tests for detection gaps surfaced by bad_data corpus files (gpio #586)."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.validate import (
    CheckStatus,
    _check_crs_valid,
    _check_epoch_valid,
    _check_version_features,
    _check_version_known,
    validate_geoparquet,
)

ITRF2014 = {"type": "GeographicCRS", "name": "ITRF2014", "id": {"authority": "EPSG", "code": 9989}}
EPSG_4326 = {"type": "GeographicCRS", "name": "WGS 84", "id": {"authority": "EPSG", "code": 4326}}


class TestVersionKnown:
    def test_unknown_version_fails(self):
        check = _check_version_known({"version": "99.0.0"})
        assert check.status == CheckStatus.FAILED

    @pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "2.0.0"])
    def test_known_versions_pass(self, version):
        check = _check_version_known({"version": version})
        assert check.status == CheckStatus.PASSED


class TestVersionFeatures:
    def _write(self, tmp_path, version_option):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        path = tmp_path / "f.parquet"
        con = get_duckdb_connection(load_spatial=True)
        con.execute(
            f"COPY (SELECT ST_GeomFromText('POINT (1 2)') AS geometry) "
            f"TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION '{version_option}')"
        )
        con.close()
        return path

    def test_v1_metadata_with_native_types_fails(self, tmp_path):
        path = self._write(tmp_path, "V2")
        check = _check_version_features(str(path), {"version": "1.0.0"})
        assert check.status == CheckStatus.FAILED

    def test_v2_metadata_with_native_types_passes(self, tmp_path):
        path = self._write(tmp_path, "V2")
        check = _check_version_features(str(path), {"version": "2.0.0"})
        assert check.status == CheckStatus.PASSED


class TestCrsRequiresType:
    def test_projjson_missing_type_fails(self):
        crs = {"id": {"authority": "XYZ", "code": 999}, "name": "Not a real CRS"}
        check = _check_crs_valid({"crs": crs}, "geometry")
        assert check.status == CheckStatus.FAILED

    def test_projjson_with_type_passes(self):
        check = _check_crs_valid({"crs": EPSG_4326}, "geometry")
        assert check.status == CheckStatus.PASSED


class TestEpochRequiresDynamicDatum:
    def test_epoch_on_datum_ensemble_fails(self):
        """EPSG:4326 is a datum ensemble — no single frame an epoch can refer to."""
        check = _check_epoch_valid({"epoch": 2020.0, "crs": EPSG_4326}, "geometry")
        assert check.status == CheckStatus.FAILED

    def test_epoch_on_dynamic_datum_passes(self):
        check = _check_epoch_valid({"epoch": 2020.0, "crs": ITRF2014}, "geometry")
        assert check.status == CheckStatus.PASSED, check.message

    def test_epoch_on_static_frame_warns(self):
        """GDA2020 is a specific static frame: tolerated with a warning
        (matches the corpus, which ships valid GDA2020+epoch samples)."""
        gda2020 = {
            "type": "GeographicCRS",
            "name": "GDA2020",
            "id": {"authority": "EPSG", "code": 7843},
        }
        check = _check_epoch_valid({"epoch": 2020.0, "crs": gda2020}, "geometry")
        assert check.status == CheckStatus.WARNING, check.message

    def test_epoch_on_default_crs84_fails(self):
        """No crs key = OGC:CRS84 default = WGS 84 ensemble."""
        check = _check_epoch_valid({"epoch": 2020.0}, "geometry")
        assert check.status == CheckStatus.FAILED

    def test_no_epoch_passes(self):
        check = _check_epoch_valid({"crs": EPSG_4326}, "geometry")
        assert check.status == CheckStatus.PASSED


class TestInvalidJsonGeoDetectedInAutoMode:
    def test_invalid_json_geo_fails_auto(self, tmp_path):
        table = pa.table({"a": [1], "geometry": [b"\x01\x01" + b"\x00" * 20]})
        table = table.replace_schema_metadata({b"geo": b'{"version": "2.0.0",}'})
        out = tmp_path / "bad.parquet"
        pq.write_table(table, out)
        result = validate_geoparquet(str(out), validate_data=False)
        assert not result.is_valid
        failed = [c.name for c in result.checks if c.status == CheckStatus.FAILED]
        assert any("geo_metadata" in n for n in failed), failed
