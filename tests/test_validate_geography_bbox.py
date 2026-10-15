"""``check spec`` validates a GEOGRAPHY bounding box (#1272).

Two new checks: the row-group statistics of a GEOGRAPHY column must stay in
the canonical ranges the Parquet spec gives them (X in [-180, 180], Y in
[-90, 90], ``xmin > xmax`` legal as the antimeridian wrap), and a box that is
only the vertex extent -- it does not contain the geodesic edge bulge -- is
worth a warning, because a reader pruning row groups by it can skip rows that
really match.

The second check is judged per row group, against an oracle independent of the
writer: each sampled geometry's edges densified with ``pyproj.Geod``. A box
that is wrong in one row group must not hide behind a file-wide union that
happens to cover it (#1286 review).
"""

from __future__ import annotations

from pathlib import Path

from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.parquet_footer import patch_footer
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet

BULGE_LINE = "LINESTRING (-30 60, 30 60)"  # great-circle peak ~63.43 N


def _geography_file(tmp_path, wkt: str, geography_bbox=None, name="geo.parquet") -> Path:
    """A 2.0 file whose column was retyped GEOGRAPHY with the given stats box."""
    path = tmp_path / name
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"COPY (SELECT ST_GeomFromText('{wkt}') AS geometry) "
            f"TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
        )
    finally:
        con.close()
    patch_footer(
        str(path),
        geography={"geometry": "spherical"},
        geography_bbox=geography_bbox,
    )
    return path


def _check(path, name: str):
    result = validate_geoparquet(str(path))
    found = [c for c in result.checks if c.name == name]
    assert found, f"no check named {name} ran: {[c.name for c in result.checks]}"
    return found[0]


class TestCanonicalRanges:
    def test_an_out_of_range_bbox_fails(self, tmp_path):
        path = _geography_file(
            tmp_path, "POINT (10 20)", geography_bbox={"geometry": [(190.0, -5.0, 200.0, 95.0)]}
        )
        check = _check(path, "geography_stats_bbox_geometry")
        assert check.status == CheckStatus.FAILED
        assert "190" in (check.details or "") and "95" in (check.details or "")

    def test_an_antimeridian_wrap_passes(self, tmp_path):
        path = _geography_file(
            tmp_path,
            "LINESTRING (170 5, -170 9)",
            geography_bbox={"geometry": [(170.0, 5.0, -170.0, 9.0)]},
        )
        check = _check(path, "geography_stats_bbox_geometry")
        assert check.status == CheckStatus.PASSED

    def test_an_inverted_y_fails(self, tmp_path):
        path = _geography_file(
            tmp_path, "POINT (10 20)", geography_bbox={"geometry": [(9.0, 21.0, 11.0, 19.0)]}
        )
        check = _check(path, "geography_stats_bbox_geometry")
        assert check.status == CheckStatus.FAILED
        assert "wrap" in (check.details or "").lower() or "ymin" in (check.details or "")

    def test_a_geography_without_boxes_is_skipped(self, tmp_path):
        path = _geography_file(tmp_path, "POINT (10 20)")  # no boxes: #1254's drop
        check = _check(path, "geography_stats_bbox_geometry")
        assert check.status == CheckStatus.SKIPPED

    def test_a_planar_column_is_skipped(self, tmp_path):
        path = tmp_path / "planar.parquet"
        con = get_duckdb_connection(load_spatial=True)
        try:
            con.execute(
                "COPY (SELECT ST_Point(1, 2) AS geometry) "
                f"TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
            )
        finally:
            con.close()
        check = _check(path, "geography_stats_bbox_geometry")
        assert check.status == CheckStatus.SKIPPED


GEOGRAPHY_SCHEMA = [{"name": "geometry", "logical_type": "GeographyType(crs=OGC:CRS84)"}]


class TestGates:
    def test_a_remote_file_is_skipped(self):
        from geoparquet_io.core.validate import _check_geography_stats_bbox

        check = _check_geography_stats_bbox(
            "s3://bucket/file.parquet", "geometry", GEOGRAPHY_SCHEMA
        )
        assert check.status == CheckStatus.SKIPPED
        assert "remote" in check.message

    def test_an_unreadable_file_is_skipped(self, tmp_path):
        from geoparquet_io.core.validate import _check_geography_stats_bbox

        missing = tmp_path / "missing.parquet"
        check = _check_geography_stats_bbox(str(missing), "geometry", GEOGRAPHY_SCHEMA)
        assert check.status == CheckStatus.SKIPPED

    def test_a_broken_sample_read_is_skipped(self, tmp_path):
        """The containment comparison degrades to SKIPPED, never a crash."""
        from geoparquet_io.core.validate import _check_geography_stats_cover_edges

        path = _geography_file(
            tmp_path, "POINT (10 20)", geography_bbox={"geometry": [(9.0, 19.0, 11.0, 21.0)]}
        )
        check = _check_geography_stats_cover_edges(
            str(path), "geometry", GEOGRAPHY_SCHEMA, con=None, sample_size=10
        )
        assert check.status == CheckStatus.SKIPPED
        assert "could not compare" in check.message

    def test_the_edge_algorithm_defaults_to_spherical(self):
        from geoparquet_io.core.validate import _geography_edge_algorithm

        assert _geography_edge_algorithm([], "geometry") == "spherical"
        assert (
            _geography_edge_algorithm(
                [
                    {
                        "name": "geometry",
                        "logical_type": "GeographyType(crs=OGC:CRS84, algorithm=karney)",
                    }
                ],
                "geometry",
            )
            == "karney"
        )


def test_a_sample_with_nothing_boundable_is_skipped(tmp_path):
    path = _geography_file(
        tmp_path, "POINT EMPTY", geography_bbox={"geometry": [(9.0, 19.0, 11.0, 21.0)]}
    )
    check = _check(path, "geography_stats_cover_edges_geometry")
    assert check.status == CheckStatus.SKIPPED


def _two_row_groups(tmp_path, name: str, boxes) -> Path:
    """Rows 0..2047 bulge to 63.43 N; the rest are short and near-equatorial."""
    path = tmp_path / name
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            "COPY (SELECT i AS id, CASE WHEN i < 2048 "
            "THEN ST_GeomFromText('LINESTRING (-30 60, 30 60)') "
            "ELSE ST_GeomFromText('LINESTRING (-10 -10, 10 -5)') END AS geometry "
            f"FROM range(4096) t(i)) TO {sql_path(str(path))} "
            "(FORMAT PARQUET, ROW_GROUP_SIZE 2048, GEOPARQUET_VERSION 'V2')"
        )
    finally:
        con.close()
    patch_footer(str(path), geography={"geometry": "spherical"}, geography_bbox={"geometry": boxes})
    return path


class TestPerRowGroup:
    def test_a_vertex_only_group_is_not_masked_by_the_union(self, tmp_path):
        """Row group 1's box reaches 70 N, so the union covers row group 0's bulge."""
        path = _two_row_groups(
            tmp_path, "masked.parquet", [(-30.0, 60.0, 30.0, 60.0), (-10.0, -11.0, 10.0, 70.0)]
        )
        check = _check(path, "geography_stats_cover_edges_geometry")
        assert check.status == CheckStatus.WARNING
        assert "row group 0" in (check.details or "")
        assert "row group 1" not in (check.details or "")

    def test_a_complemented_longitude_range_warns(self, tmp_path):
        """(30, ..., -30) wraps the far side of the globe: the data is outside it."""
        path = _two_row_groups(
            tmp_path, "wrongwrap.parquet", [(30.0, 60.0, -30.0, 63.5), (-10.0, -11.0, 10.0, 0.0)]
        )
        check = _check(path, "geography_stats_cover_edges_geometry")
        assert check.status == CheckStatus.WARNING
        assert "row group 0" in (check.details or "")

    def test_correct_boxes_pass(self, tmp_path):
        path = _two_row_groups(
            tmp_path, "ok.parquet", [(-30.0, 60.0, 30.0, 63.44), (-10.0, -11.0, 10.0, 0.0)]
        )
        assert _check(path, "geography_stats_cover_edges_geometry").status == CheckStatus.PASSED

    def test_the_oracle_does_not_ask_the_writer(self, tmp_path, monkeypatch):
        """Sabotaging the writer's own bound must not change the verdict."""
        import geoparquet_io.core.geodesic_bbox as writer

        path = _two_row_groups(
            tmp_path, "masked2.parquet", [(-30.0, 60.0, 30.0, 60.0), (-10.0, -11.0, 10.0, 70.0)]
        )

        def refuse(*args, **kwargs):
            raise AssertionError("the validator consulted the writer's bound")

        monkeypatch.setattr(writer, "geodesic_bbox_of_wkb", refuse)
        monkeypatch.setattr(writer, "row_group_geography_bboxes", refuse)
        assert _check(path, "geography_stats_cover_edges_geometry").status == CheckStatus.WARNING


class TestNonFiniteStatistics:
    def test_a_nan_box_fails_the_range_check_and_never_reaches_sql(self, tmp_path):
        nan = float("nan")
        path = _geography_file(
            tmp_path,
            "LINESTRING (0 0, 20 20)",
            geography_bbox={"geometry": [(nan, 0.0, 0.0, 20.0)]},
        )
        result = validate_geoparquet(str(path))
        by_name = {c.name: c for c in result.checks}
        stats = by_name["geography_stats_bbox_geometry"]
        assert stats.status == CheckStatus.FAILED
        assert "finite" in (stats.details or "")
        for check in result.checks:
            text = f"{check.message} {check.details or ''}"
            assert "Binder Error" not in text, check


class TestVertexOnlyWarning:
    def test_a_vertex_only_box_warns(self, tmp_path):
        """The vertex box of the bulged line misses ~3.4 degrees of edge."""
        path = _geography_file(
            tmp_path, BULGE_LINE, geography_bbox={"geometry": [(-30.0, 60.0, 30.0, 60.0)]}
        )
        check = _check(path, "geography_stats_cover_edges_geometry")
        assert check.status == CheckStatus.WARNING
        assert "vertex" in check.message.lower()

    def test_a_geodesic_box_passes(self, tmp_path):
        """A box gpio's own write computes contains the bulge and is clean."""
        from geoparquet_io.core.write_funnels import _apply_nonplanar_edges

        path = tmp_path / "gpio.parquet"
        con = get_duckdb_connection(load_spatial=True)
        try:
            con.execute(
                f"COPY (SELECT ST_GeomFromText('{BULGE_LINE}') AS geometry) "
                f"TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
            )
        finally:
            con.close()
        _apply_nonplanar_edges({"geometry": "spherical"}, str(path))

        check = _check(path, "geography_stats_cover_edges_geometry")
        assert check.status == CheckStatus.PASSED

    def test_a_wrapping_geodesic_box_passes(self, tmp_path):
        from geoparquet_io.core.write_funnels import _apply_nonplanar_edges

        path = tmp_path / "wrap.parquet"
        con = get_duckdb_connection(load_spatial=True)
        try:
            con.execute(
                "COPY (SELECT ST_GeomFromText('LINESTRING (170 10, -170 15)') AS geometry) "
                f"TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
            )
        finally:
            con.close()
        _apply_nonplanar_edges({"geometry": "spherical"}, str(path))

        check = _check(path, "geography_stats_cover_edges_geometry")
        assert check.status == CheckStatus.PASSED

    def test_a_boxless_geography_is_skipped(self, tmp_path):
        path = _geography_file(tmp_path, BULGE_LINE)
        check = _check(path, "geography_stats_cover_edges_geometry")
        assert check.status == CheckStatus.SKIPPED


def test_gpio_outputs_pass_both_checks(tmp_path):
    """End to end: a gpio 2.0 GEOGRAPHY output is clean under its own checks."""
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq
    import shapely

    from geoparquet_io.core.convert import convert_to_geoparquet

    src = tmp_path / "in.parquet"
    table = pa.table(
        {
            "id": [0],
            "geometry": pa.array([shapely.to_wkb(shapely.from_wkt(BULGE_LINE))], pa.binary()),
        }
    )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["LineString"], "edges": "spherical"}
        },
    }
    pq.write_table(table.replace_schema_metadata({"geo": json.dumps(geo)}), str(src))
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(src), str(out), skip_hilbert=True, geoparquet_version="2.0")

    result = validate_geoparquet(str(out))
    by_name = {c.name: c for c in result.checks}
    assert by_name["geography_stats_bbox_geometry"].status == CheckStatus.PASSED
    assert by_name["geography_stats_cover_edges_geometry"].status == CheckStatus.PASSED
    failed = [f"{c.name}: {c.message}" for c in result.checks if c.status == CheckStatus.FAILED]
    assert failed == []


def test_a_karney_output_passes_against_the_ellipsoidal_oracle(tmp_path):
    from geoparquet_io.core.write_funnels import _apply_nonplanar_edges

    path = tmp_path / "karney.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            "COPY (SELECT ST_GeomFromText('LINESTRING (-60 30, 60 35)') AS geometry) "
            f"TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
        )
    finally:
        con.close()
    _apply_nonplanar_edges({"geometry": "karney"}, str(path))

    assert _check(path, "geography_stats_cover_edges_geometry").status == CheckStatus.PASSED


def test_the_oracle_uses_the_column_crs_ellipsoid():
    from geoparquet_io.core.validate import _geography_geod

    schema = [
        {
            "name": "geometry",
            "logical_type": 'GeographyType(crs={"type": "GeographicCRS"}, algorithm=karney)',
        }
    ]
    assert _geography_geod(schema, "geometry", "karney").f > 0
    assert _geography_geod(schema, "geometry", "spherical").f == 0
