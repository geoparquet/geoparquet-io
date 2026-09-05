"""bbox checks for 6-element (XYZ) and 8-element (XYZM) bboxes and antimeridian extents (#603)."""

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import get_duckdb_connection
from geoparquet_io.core.validate import (
    CheckStatus,
    _check_bbox_contains_data,
    _check_bbox_valid,
    validate_geoparquet,
)


def _write_v2(path, wkt_rows):
    con = get_duckdb_connection(load_spatial=True)
    values = ", ".join(f"({i}, ST_GeomFromText('{wkt}'))" for i, wkt in enumerate(wkt_rows, 1))
    con.execute(
        f"COPY (SELECT * FROM (VALUES {values}) t(id, geometry)) "
        f"TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
    )
    con.close()
    return path


def _declared_bbox(path):
    geo = json.loads(pq.read_metadata(path).metadata[b"geo"])
    return geo["columns"]["geometry"]["bbox"]


@pytest.fixture
def xyz_file(tmp_path):
    return _write_v2(
        tmp_path / "xyz.parquet",
        [
            "POLYGON Z ((0 0 10, 4 0 12, 4 4 15, 0 4 11, 0 0 10))",
            "POLYGON Z ((10 10 5, 12 10 5, 12 12 5, 10 12 5, 10 10 5))",
        ],
    )


@pytest.fixture
def xyzm_file(tmp_path):
    return _write_v2(
        tmp_path / "xyzm.parquet",
        ["POINT ZM (1 2 3 4)", "POINT ZM (10 20 30 40)"],
    )


@pytest.fixture
def antimeridian_file(tmp_path):
    return _write_v2(tmp_path / "am.parquet", ["POINT (175 0)", "POINT (-175 5)"])


@pytest.fixture
def con():
    con = get_duckdb_connection(load_spatial=True)
    yield con
    con.close()


class TestBboxValid:
    @pytest.mark.parametrize("n", [4, 6, 8])
    def test_accepts_spec_lengths(self, n):
        check = _check_bbox_valid({"bbox": [float(i) for i in range(n)]}, "geometry")
        assert check.status == CheckStatus.PASSED, check.message

    @pytest.mark.parametrize("n", [1, 2, 3, 5, 7, 9])
    def test_rejects_other_lengths(self, n):
        check = _check_bbox_valid({"bbox": [float(i) for i in range(n)]}, "geometry")
        assert check.status == CheckStatus.FAILED
        assert "4, 6 or 8" in check.message


class TestBboxContainsData:
    def test_duckdb_xyz_bbox_is_not_a_false_failure(self, xyz_file, con):
        bbox = _declared_bbox(xyz_file)
        assert len(bbox) == 6
        check = _check_bbox_contains_data(str(xyz_file), "geometry", bbox, con, 0)
        assert check.status == CheckStatus.PASSED, check.message

    def test_xyz_bbox_that_excludes_data_fails(self, xyz_file, con):
        check = _check_bbox_contains_data(str(xyz_file), "geometry", [0, 0, 0, 5, 5, 20], con, 0)
        assert check.status == CheckStatus.FAILED
        assert "1 of 2" in check.message

    def test_xyzm_bbox(self, xyzm_file, con):
        inside = [0, 0, 0, 0, 20, 30, 40, 50]
        outside = [0, 0, 0, 0, 5, 30, 40, 50]
        assert (
            _check_bbox_contains_data(str(xyzm_file), "geometry", inside, con, 0).status
            == CheckStatus.PASSED
        )
        check = _check_bbox_contains_data(str(xyzm_file), "geometry", outside, con, 0)
        assert check.status == CheckStatus.FAILED
        assert "1 of 2" in check.message

    def test_antimeridian_bbox(self, antimeridian_file, con):
        # xmin > xmax means the extent wraps across the antimeridian (RFC 7946 5.2)
        check = _check_bbox_contains_data(
            str(antimeridian_file), "geometry", [170, -10, -170, 10], con, 0
        )
        assert check.status == CheckStatus.PASSED, check.message

    def test_antimeridian_bbox_excludes_geometry_in_the_gap(self, tmp_path, con):
        path = _write_v2(tmp_path / "gap.parquet", ["POINT (175 0)", "POINT (0 0)"])
        check = _check_bbox_contains_data(str(path), "geometry", [170, -10, -170, 10], con, 0)
        assert check.status == CheckStatus.FAILED
        assert "1 of 2" in check.message

    def test_antimeridian_bbox_checks_every_vertex(self, tmp_path, con):
        # X extremes sit in the two lobes, but the middle vertex is in the gap
        path = _write_v2(tmp_path / "gap.parquet", ["MULTIPOINT (175 0, -175 0, 0 0)"])
        check = _check_bbox_contains_data(str(path), "geometry", [170, -10, -170, 10], con, 0)
        assert check.status == CheckStatus.FAILED
        assert "1 of 1" in check.message

    def test_invalid_length_skips_data_check(self, xyz_file, con):
        check = _check_bbox_contains_data(str(xyz_file), "geometry", [0, 0, 0, 5, 5], con, 0)
        assert check.status == CheckStatus.SKIPPED
        assert "expected 4, 6 or 8" in check.message

    def test_geoarrow_encoding(self, con):
        path = "tests/data/data-polygon-encoding_native.parquet"
        wide = _check_bbox_contains_data(
            path, "geometry", [-180, -90, -1, 180, 90, 1], con, 0, "polygon"
        )
        assert wide.status == CheckStatus.PASSED, wide.message
        wrap = _check_bbox_contains_data(path, "geometry", [170, -90, -170, 90], con, 0, "polygon")
        assert wrap.status == CheckStatus.SKIPPED

    def test_full_validation_of_xyz_file_passes_bbox_checks(self, xyz_file):
        result = validate_geoparquet(str(xyz_file))
        bbox_checks = {c.name: c for c in result.checks if c.name.startswith("bbox_")}
        assert bbox_checks, [c.name for c in result.checks]
        assert all(c.status == CheckStatus.PASSED for c in bbox_checks.values()), [
            (c.name, c.message) for c in bbox_checks.values()
        ]
