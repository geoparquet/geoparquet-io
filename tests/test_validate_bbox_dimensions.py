"""bbox checks for 6-element (XYZ) and 8-element (XYZM) bboxes and antimeridian extents.

Covers the validate checks (#603) and the same two bug classes where they survived
outside them: the partition summary's bbox merge and the native geospatial
statistics aggregate (#886).
"""

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import get_duckdb_connection
from geoparquet_io.core.crs_utils import merge_longitude_ranges
from geoparquet_io.core.duckdb_metadata import aggregate_native_geo_stats
from geoparquet_io.core.inspect_utils import extract_partition_summary
from geoparquet_io.core.validate import (
    CheckStatus,
    _check_bbox_contains_data,
    _check_bbox_valid,
    _check_native_geo_stats_contains_data,
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


def _rewrite_geo(path, out_path, *, version=None, **column_updates):
    """Copy a GeoParquet file with its geo metadata version/column fields edited."""
    table = pq.read_table(path)
    meta = dict(table.schema.metadata or {})
    geo = json.loads(meta[b"geo"])
    if version is not None:
        geo["version"] = version
    geo["columns"]["geometry"].update(column_updates)
    meta[b"geo"] = json.dumps(geo).encode()
    pq.write_table(table.replace_schema_metadata(meta), out_path)
    return out_path


UTM_33N = {
    "type": "ProjectedCRS",
    "name": "WGS 84 / UTM zone 33N",
    "id": {"authority": "EPSG", "code": 32633},
}

CRS84 = {
    "type": "GeographicCRS",
    "name": "WGS 84 longitude-latitude",
    "id": {"authority": "OGC", "code": "CRS84"},
}

#: A wrapping extent: legal for a geographic CRS, corrupt for a projected one.
WRAPPING_BBOX = [170.0, -10.0, -170.0, 10.0]


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
    @pytest.mark.parametrize("n", [4, 6])
    def test_accepts_spec_lengths(self, n):
        check = _check_bbox_valid({"bbox": [float(i) for i in range(n)]}, "geometry")
        assert check.status == CheckStatus.PASSED, check.message

    def test_accepts_xyzm_bbox_on_2_0(self):
        check = _check_bbox_valid({"bbox": [float(i) for i in range(8)]}, "geometry", "2.0.0")
        assert check.status == CheckStatus.PASSED, check.message

    @pytest.mark.parametrize("version", ["1.0.0", "1.1.0"])
    def test_rejects_xyzm_bbox_below_2_0(self, version):
        # The XYZM bbox is spec text only on the 2.0 line; the released 1.1.0
        # schema allows only 4 or 6 elements.
        check = _check_bbox_valid({"bbox": [float(i) for i in range(8)]}, "geometry", version)
        assert check.status == CheckStatus.FAILED
        assert "2.0" in check.message

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
        # The reinterpretation must never be invisible in the check output.
        assert "antimeridian" in check.message

    def test_wrap_bbox_on_projected_crs_fails(self, tmp_path, con):
        # For a projected CRS xmin > xmax is not a wrap -- it is broken metadata,
        # and the geometry must not be blessed by the wrap reading.
        path = _write_v2(tmp_path / "proj.parquet", ["POINT (0 0)"])
        check = _check_bbox_contains_data(
            str(path), "geometry", [10, -10, 5, 10], con, 0, "WKB", crs=UTM_33N
        )
        assert check.status == CheckStatus.FAILED
        assert "projected" in check.message.lower()

    def test_wrap_fail_message_names_the_interpretation(self, tmp_path, con):
        path = _write_v2(tmp_path / "gap2.parquet", ["POINT (0 0)"])
        check = _check_bbox_contains_data(str(path), "geometry", [170, -10, -170, 10], con, 0)
        assert check.status == CheckStatus.FAILED
        assert "antimeridian" in check.message

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

    def test_non_numeric_bbox_element_skips_data_check(self, xyz_file, con):
        # A non-numeric metadata element must never reach the SQL f-string.
        check = _check_bbox_contains_data(str(xyz_file), "geometry", [0, 0, "east", 5], con, 0)
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

    def test_full_validation_rejects_xyzm_bbox_on_1_1(self, xyz_file, tmp_path):
        path = _rewrite_geo(
            xyz_file,
            tmp_path / "xyzm_11.parquet",
            version="1.1.0",
            bbox=[0.0, 0.0, 10.0, 0.0, 4.0, 4.0, 15.0, 0.0],
        )
        result = validate_geoparquet(str(path))
        check = next(c for c in result.checks if c.name == "bbox_valid_geometry")
        assert check.status == CheckStatus.FAILED, check.message
        assert "2.0" in check.message

    def test_full_validation_accepts_xyzm_bbox_on_2_0(self, xyz_file, tmp_path):
        path = _rewrite_geo(
            xyz_file,
            tmp_path / "xyzm_20.parquet",
            bbox=[0.0, 0.0, 10.0, 0.0, 4.0, 4.0, 15.0, 0.0],
        )
        result = validate_geoparquet(str(path))
        check = next(c for c in result.checks if c.name == "bbox_valid_geometry")
        assert check.status == CheckStatus.PASSED, check.message

    def test_full_validation_fails_wrap_bbox_on_projected_crs(self, xyz_file, tmp_path):
        path = _rewrite_geo(
            xyz_file,
            tmp_path / "proj_wrap.parquet",
            bbox=[170.0, -10.0, -170.0, 10.0],
            crs=UTM_33N,
        )
        result = validate_geoparquet(str(path))
        check = next(c for c in result.checks if c.name == "bbox_contains_data_geometry")
        assert check.status == CheckStatus.FAILED, check.message
        assert "projected" in check.message.lower()

    def test_full_validation_of_xyz_file_passes_bbox_checks(self, xyz_file):
        result = validate_geoparquet(str(xyz_file))
        bbox_checks = {c.name: c for c in result.checks if c.name.startswith("bbox_")}
        assert bbox_checks, [c.name for c in result.checks]
        assert all(c.status == CheckStatus.PASSED for c in bbox_checks.values()), [
            (c.name, c.message) for c in bbox_checks.values()
        ]


def _chunk(xmin, ymin, xmax, ymax, **extra):
    return {
        "row_group_id": 0,
        "xmin": xmin,
        "ymin": ymin,
        "xmax": xmax,
        "ymax": ymax,
        "zmin": None,
        "zmax": None,
        "geometry_types": [],
        **extra,
    }


class TestMergeLongitudeRanges:
    """The union is taken on the circle, so a wrapping range is not inverted (#886)."""

    def test_plain_ranges_are_min_max(self):
        assert merge_longitude_ranges([(0.0, 10.0), (-5.0, 4.0)]) == (-5.0, 10.0)

    def test_wrapping_range_survives(self):
        assert merge_longitude_ranges([(170.0, -170.0)]) == (170.0, -170.0)

    def test_union_with_a_wrapping_range_keeps_the_short_way_round(self):
        assert merge_longitude_ranges([(170.0, -170.0), (-179.0, -175.0)]) == (170.0, -170.0)

    def test_union_widens_the_wrapping_range(self):
        # 170 east to -90 spans 100 degrees; -100 west to -170 would span 290.
        assert merge_longitude_ranges([(170.0, -170.0), (-100.0, -90.0)]) == (170.0, -90.0)

    def test_plain_ranges_that_meet_at_the_antimeridian_stay_min_max(self):
        # Deliberate: with nothing declaring a wrap, the producer's own reading
        # of its two extents is kept rather than guessed at.
        assert merge_longitude_ranges([(170.0, 180.0), (-180.0, -170.0)]) == (-180.0, 180.0)

    def test_no_ranges_is_a_programming_error(self):
        with pytest.raises(ValueError):
            merge_longitude_ranges([])

    def test_full_circle_collapses_to_the_whole_world(self):
        xmin, xmax = merge_longitude_ranges([(170.0, -170.0), (-175.0, 175.0)])
        assert (xmin, xmax) == (-180.0, 180.0)


class TestNativeGeoStatsWrap:
    """Parquet's own geospatial statistics may wrap the antimeridian (#886)."""

    def test_aggregate_keeps_a_wrapping_extent(self):
        stats = aggregate_native_geo_stats(
            [_chunk(175.0, 0.0, -175.0, 5.0), _chunk(178.0, 1.0, 179.0, 2.0)]
        )
        assert stats["bbox"] == [175.0, 0.0, -175.0, 5.0]

    def test_aggregate_without_wrap_is_unchanged(self):
        stats = aggregate_native_geo_stats(
            [_chunk(0.0, 0.0, 10.0, 5.0), _chunk(-5.0, 1.0, 4.0, 20.0)]
        )
        assert stats["bbox"] == [-5.0, 0.0, 10.0, 20.0]

    def test_check_accepts_data_inside_a_wrapping_stat(self, antimeridian_file, con, monkeypatch):
        monkeypatch.setattr(
            "geoparquet_io.core.duckdb_metadata.get_aggregated_native_geo_stats",
            lambda *a, **k: {"bbox": [170.0, -10.0, -170.0, 10.0]},
        )
        check = _check_native_geo_stats_contains_data(str(antimeridian_file), "geometry", con, 0)
        assert check.status == CheckStatus.PASSED, check.message
        assert "antimeridian" in check.message

    def test_check_still_fails_geometry_in_the_wrap_gap(self, tmp_path, con, monkeypatch):
        path = _write_v2(tmp_path / "gap_stats.parquet", ["POINT (175 0)", "POINT (0 0)"])
        monkeypatch.setattr(
            "geoparquet_io.core.duckdb_metadata.get_aggregated_native_geo_stats",
            lambda *a, **k: {"bbox": [170.0, -10.0, -170.0, 10.0]},
        )
        check = _check_native_geo_stats_contains_data(str(path), "geometry", con, 0)
        assert check.status == CheckStatus.FAILED
        assert "1 of 2" in check.message


class TestPartitionSummaryBbox:
    """extract_partition_summary read indices 2/3 as xmax/ymax (#886)."""

    def test_six_element_bboxes_merge_on_x_and_y(self, tmp_path, xyz_file):
        other = _write_v2(
            tmp_path / "xyz2.parquet",
            ["POLYGON Z ((10 10 5, 12 10 5, 12 12 5, 10 12 5, 10 10 5))"],
        )
        assert len(_declared_bbox(xyz_file)) == 6
        summary = extract_partition_summary([str(xyz_file), str(other)])
        assert summary["combined_bbox"] == [0.0, 0.0, 12.0, 12.0]

    def test_wrapping_bboxes_merge_on_the_circle(self, tmp_path, xyz_file):
        wrapped = _rewrite_geo(
            xyz_file, tmp_path / "wrap.parquet", bbox=[170.0, -10.0, -170.0, 10.0]
        )
        near = _rewrite_geo(xyz_file, tmp_path / "near.parquet", bbox=[-179.0, -5.0, -175.0, 5.0])
        summary = extract_partition_summary([str(wrapped), str(near)])
        assert summary["combined_bbox"] == [170.0, -10.0, -170.0, 10.0]

    def test_short_bbox_is_ignored(self, tmp_path, xyz_file):
        broken = _rewrite_geo(xyz_file, tmp_path / "broken.parquet", bbox=[0.0, 0.0, 1.0])
        summary = extract_partition_summary([str(broken)])
        assert summary["combined_bbox"] is None


def _stats(monkeypatch, bbox):
    """Force the aggregated native geospatial statistics to ``bbox``."""
    monkeypatch.setattr(
        "geoparquet_io.core.duckdb_metadata.get_aggregated_native_geo_stats",
        lambda *a, **k: {"bbox": bbox},
    )


class TestNativeGeoStatsWrapNeedsGeographicCrs:
    """xmin > xmax is a wrap only for a geographic CRS (GeoParquet 1.1.0, #876)."""

    def test_projected_crs_rejects_wrapping_statistics(self, antimeridian_file, con, monkeypatch):
        _stats(monkeypatch, WRAPPING_BBOX)
        check = _check_native_geo_stats_contains_data(
            str(antimeridian_file), "geometry", con, 0, crs=UTM_33N
        )
        assert check.status == CheckStatus.FAILED, check.message
        assert "projected" in check.message.lower()
        assert "geographic" in check.message.lower()

    def test_geographic_crs_still_reads_the_wrap(self, antimeridian_file, con, monkeypatch):
        _stats(monkeypatch, WRAPPING_BBOX)
        check = _check_native_geo_stats_contains_data(
            str(antimeridian_file), "geometry", con, 0, crs=CRS84
        )
        assert check.status == CheckStatus.PASSED, check.message
        assert "antimeridian" in check.message

    def test_absent_crs_is_the_geographic_crs84_default(self, antimeridian_file, con, monkeypatch):
        _stats(monkeypatch, WRAPPING_BBOX)
        check = _check_native_geo_stats_contains_data(str(antimeridian_file), "geometry", con, 0)
        assert check.status == CheckStatus.PASSED, check.message
        assert "antimeridian" in check.message

    def test_the_two_wrap_checks_agree_on_a_projected_crs(
        self, antimeridian_file, con, monkeypatch
    ):
        # The bug: these two functions, on the same numbers and the same CRS,
        # returned PASSED and FAILED respectively.
        _stats(monkeypatch, WRAPPING_BBOX)
        stats_check = _check_native_geo_stats_contains_data(
            str(antimeridian_file), "geometry", con, 0, crs=UTM_33N
        )
        bbox_check = _check_bbox_contains_data(
            str(antimeridian_file), "geometry", WRAPPING_BBOX, con, 0, "WKB", UTM_33N
        )
        assert stats_check.status == bbox_check.status == CheckStatus.FAILED, (
            stats_check.message,
            bbox_check.message,
        )

    def _stats_check(self, path):
        result = validate_geoparquet(str(path))
        return next(c for c in result.checks if c.name == "native_geo_stats_contains_data_geometry")

    def test_projected_crs_reaches_the_check_end_to_end(
        self, tmp_path, antimeridian_file, monkeypatch
    ):
        # The column's crs has to travel from the metadata to the check; without
        # the wiring the file validates clean on statistics it contradicts. Same
        # data and same statistics as the test below -- only the CRS differs.
        path = _rewrite_geo(antimeridian_file, tmp_path / "proj.parquet", crs=UTM_33N)
        _stats(monkeypatch, WRAPPING_BBOX)
        check = self._stats_check(path)
        assert check.status == CheckStatus.FAILED, check.message
        assert "projected" in check.message.lower()

    def test_default_crs_file_reads_the_wrap_end_to_end(
        self, tmp_path, antimeridian_file, monkeypatch
    ):
        path = _rewrite_geo(antimeridian_file, tmp_path / "default_crs.parquet")
        _stats(monkeypatch, WRAPPING_BBOX)
        check = self._stats_check(path)
        assert check.status == CheckStatus.PASSED, check.message
        assert "antimeridian" in check.message


class TestPartitionSummaryWrapNeedsGeographicCrs:
    """merge_longitude_ranges splits at +/-180, which is meaningless in metres (#886)."""

    def _projected_pair(self, tmp_path, xyz_file):
        a = _rewrite_geo(
            xyz_file, tmp_path / "m_a.parquet", bbox=[1000.0, 0.0, 2000.0, 100.0], crs=UTM_33N
        )
        b = _rewrite_geo(
            xyz_file, tmp_path / "m_b.parquet", bbox=[5000.0, 0.0, 3000.0, 100.0], crs=UTM_33N
        )
        return [str(a), str(b)]

    def test_projected_crs_takes_plain_min_max(self, tmp_path, xyz_file):
        summary = extract_partition_summary(self._projected_pair(tmp_path, xyz_file))
        assert summary["combined_bbox"] == [1000.0, 0.0, 3000.0, 100.0]

    def test_projected_crs_never_yields_the_wrap_merge(self, tmp_path, xyz_file):
        summary = extract_partition_summary(self._projected_pair(tmp_path, xyz_file))
        xmin, xmax = summary["combined_bbox"][0], summary["combined_bbox"][2]
        assert (xmin, xmax) != (5000.0, 3000.0)
        assert xmin <= xmax

    def test_a_single_projected_crs_file_is_not_reinterpreted(self, tmp_path, xyz_file):
        one = _rewrite_geo(
            xyz_file,
            tmp_path / "m_one.parquet",
            bbox=[-500000.0, 0.0, 500000.0, 100.0],
            crs=UTM_33N,
        )
        other = _rewrite_geo(
            xyz_file,
            tmp_path / "m_two.parquet",
            bbox=[900000.0, 0.0, 800000.0, 100.0],
            crs=UTM_33N,
        )
        summary = extract_partition_summary([str(one), str(other)])
        assert summary["combined_bbox"] == [-500000.0, 0.0, 800000.0, 100.0]

    def test_a_projected_file_disables_the_wrap_for_the_whole_partition(self, tmp_path, xyz_file):
        # One projected member is enough to make the wrap reading unsafe.
        geographic = _rewrite_geo(xyz_file, tmp_path / "geo.parquet", bbox=WRAPPING_BBOX)
        projected = _rewrite_geo(
            xyz_file, tmp_path / "proj_member.parquet", bbox=[0.0, 0.0, 10.0, 5.0], crs=UTM_33N
        )
        summary = extract_partition_summary([str(geographic), str(projected)])
        assert summary["combined_bbox"] == [0.0, -10.0, 10.0, 10.0]
