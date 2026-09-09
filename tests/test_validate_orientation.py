"""orientation_matches_data: exterior rings CCW and holes CW when orientation is declared (#586)."""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import get_duckdb_connection
from geoparquet_io.core.duckdb_utils import sql_path
from geoparquet_io.core.validate import (
    CheckStatus,
    _check_orientation_matches_data,
    validate_geoparquet,
)

CORPUS = "tests/data/geoparquet-testing"
CCW = "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"
CW = "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))"
CCW_WITH_CW_HOLE = "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))"
CCW_WITH_CCW_HOLE = "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))"


def _write_v2(path, wkts):
    con = get_duckdb_connection(load_spatial=True)
    values = ", ".join("(NULL)" if w is None else f"(ST_GeomFromText('{w}'))" for w in wkts)
    con.execute(
        f"COPY (SELECT * FROM (VALUES {values}) t(geometry)) "
        f"TO {sql_path(path.as_posix())} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
    )
    con.close()
    return str(path)


def _write_wkb_with_orientation(path, wkts, orientation="counterclockwise"):
    """A plain-WKB GeoParquet file that *declares* an orientation.

    ``_write_v2`` leaves the field unset, which makes the orientation check
    short-circuit to SKIPPED -- useless for an end-to-end assertion.
    """
    con = get_duckdb_connection(load_spatial=True)
    values = ", ".join(f"(ST_AsWKB(ST_GeomFromText('{w}')))" for w in wkts)
    table = con.execute(f"SELECT * FROM (VALUES {values}) t(geometry)").arrow().read_all()
    con.close()
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Polygon"],
                "orientation": orientation,
            }
        },
    }
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    pq.write_table(table, path)
    return str(path)


@pytest.fixture
def con():
    con = get_duckdb_connection(load_spatial=True)
    yield con
    con.close()


def _check(path, con, orientation="counterclockwise", sample_size=0, **kw):
    return _check_orientation_matches_data(path, "geometry", orientation, con, sample_size, **kw)


@pytest.mark.corpus
@pytest.mark.skipif(not Path(CORPUS, "data").exists(), reason="run: git submodule update --init")
class TestCorpusFiles:
    def test_declared_ccw_with_cw_rings_fails(self, con):
        check = _check(f"{CORPUS}/bad_data/orientation-ccw-declared-rings-cw.parquet", con)
        assert check.status == CheckStatus.FAILED
        assert "1 of 1" in check.message

    def test_declared_ccw_with_ccw_rings_passes(self, con):
        check = _check(f"{CORPUS}/data/orientation/polygon-ccw.parquet", con)
        assert check.status == CheckStatus.PASSED, check.message

    def test_no_orientation_declared_is_skipped(self, con):
        check = _check(f"{CORPUS}/data/orientation/polygon-cw.parquet", con, orientation=None)
        assert check.status == CheckStatus.SKIPPED

    def test_reported_through_validate(self):
        result = validate_geoparquet(f"{CORPUS}/bad_data/orientation-ccw-declared-rings-cw.parquet")
        (check,) = [c for c in result.checks if c.name == "orientation_matches_data_geometry"]
        assert check.status == CheckStatus.FAILED


class TestRings:
    def test_holes_must_be_clockwise(self, tmp_path, con):
        good = _write_v2(tmp_path / "good.parquet", [CCW_WITH_CW_HOLE])
        bad = _write_v2(tmp_path / "bad.parquet", [CCW_WITH_CCW_HOLE])
        assert _check(good, con).status == CheckStatus.PASSED
        assert _check(bad, con).status == CheckStatus.FAILED

    def test_multipolygon_parts_are_checked(self, tmp_path, con):
        wkt = (
            "MULTIPOLYGON (((10 10, 12 10, 12 12, 10 12, 10 10)), "
            "((20 20, 20 22, 22 22, 22 20, 20 20)))"
        )
        check = _check(_write_v2(tmp_path / "f.parquet", [wkt]), con)
        assert check.status == CheckStatus.FAILED
        assert "1 of 2 polygons" in check.message

    def test_polygons_inside_collections_and_z_are_checked(self, tmp_path, con):
        wkt = "GEOMETRYCOLLECTION Z (POINT Z (1 1 1), POLYGON Z ((0 0 1, 0 4 1, 4 4 1, 4 0 1, 0 0 1)))"
        check = _check(_write_v2(tmp_path / "f.parquet", [wkt]), con)
        assert check.status == CheckStatus.FAILED

    def test_null_empty_and_non_polygons_are_ignored(self, tmp_path, con):
        path = _write_v2(tmp_path / "f.parquet", [CCW, None, "POLYGON EMPTY", "POINT (1 1)"])
        check = _check(path, con)
        assert check.status == CheckStatus.PASSED
        assert "1 polygon" in check.message

    def test_no_polygons_is_skipped(self, tmp_path, con):
        check = _check(
            _write_v2(tmp_path / "f.parquet", ["POINT (1 1)", "LINESTRING (0 0, 1 1)"]), con
        )
        assert check.status == CheckStatus.SKIPPED

    def test_sample_size_limits_rows(self, tmp_path, con):
        path = _write_v2(tmp_path / "f.parquet", [CCW, CW])
        assert _check(path, con, sample_size=1).status == CheckStatus.PASSED
        assert _check(path, con, sample_size=0).status == CheckStatus.FAILED

    def test_geoarrow_encoding_is_skipped(self, con):
        check = _check("tests/data/data-polygon-encoding_native.parquet", con, encoding="polygon")
        assert check.status == CheckStatus.SKIPPED


class TestEdges:
    def test_zero_area_rings_are_not_judged(self, tmp_path, con):
        path = _write_v2(tmp_path / "f.parquet", ["POLYGON ((0 0, 1 1, 2 2, 0 0))", CCW])
        check = _check(path, con)
        assert check.status == CheckStatus.PASSED
        assert "all 1 polygon follows" in check.message

    def test_passing_message_uses_plural_verb_for_many_polygons(self, tmp_path, con):
        check = _check(_write_v2(tmp_path / "f.parquet", [CCW, CCW]), con)
        assert check.status == CheckStatus.PASSED
        assert "all 2 polygons follow " in check.message

    def test_spherical_polygon_across_the_antimeridian_is_not_judged_planar(self, tmp_path, con):
        # counterclockwise on the sphere, clockwise when read as planar lon/lat
        wkt = "POLYGON ((170 -10, -170 -10, -170 10, 170 10, 170 -10))"
        path = _write_v2(tmp_path / "f.parquet", [wkt])
        assert _check(path, con, edges="planar").status == CheckStatus.FAILED
        assert _check(path, con, edges="spherical").status == CheckStatus.SKIPPED
        both = _write_v2(tmp_path / "g.parquet", [wkt, CCW])
        check = _check(both, con, edges="spherical")
        assert check.status == CheckStatus.PASSED
        assert "all 1 polygon follows" in check.message

    def test_unknown_orientation_value_is_skipped(self, tmp_path, con):
        check = _check(_write_v2(tmp_path / "f.parquet", [CCW]), con, orientation="clockwise")
        assert check.status == CheckStatus.SKIPPED

    def test_unreadable_file_fails(self, tmp_path, con):
        check = _check(str(tmp_path / "missing.parquet"), con)
        assert check.status == CheckStatus.FAILED
        assert "failed to validate orientation" in check.message


class TestPathEscapedExactlyOnce:
    """#937: the check escaped its own path twice.

    ``_describe_geom_type(con, raw_url, ...)`` escapes its argument itself (it
    builds ``read_parquet({sql_path(raw_url)})``), so passing the already
    escaped ``safe_file_url()`` result turned ``o'brien`` into ``o''''brien``
    inside the literal. The read then failed, the broad ``except`` caught it,
    and a perfectly valid file was reported as *failing* the orientation
    check -- the #718 failure mode, in a check whose verdict users act on.
    """

    def test_apostrophe_in_path_does_not_fail_a_valid_file(self, tmp_path, con):
        directory = tmp_path / "o'brien"
        directory.mkdir()
        path = _write_v2(directory / "f.parquet", [CCW])

        check = _check(path, con)

        assert check.status == CheckStatus.PASSED, check.message

    def test_apostrophe_in_path_still_detects_a_real_violation(self, tmp_path, con):
        """The path fix must not make the check vacuously pass."""
        directory = tmp_path / "o'brien"
        directory.mkdir()
        path = _write_v2(directory / "bad.parquet", [CW])

        check = _check(path, con)

        assert check.status == CheckStatus.FAILED
        assert "1 of 1" in check.message

    def test_reported_through_validate_geoparquet(self, tmp_path):
        """The whole `check spec` run, not just the helper.

        Needs a file that actually *declares* an orientation, or the check
        short-circuits to SKIPPED before it ever builds the query.
        """
        directory = tmp_path / "o'brien"
        directory.mkdir()
        path = _write_wkb_with_orientation(directory / "f.parquet", [CCW])

        result = validate_geoparquet(path)
        (check,) = [c for c in result.checks if c.name == "orientation_matches_data_geometry"]

        assert check.status == CheckStatus.PASSED, check.message
