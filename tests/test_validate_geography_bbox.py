"""``check spec`` validates a GEOGRAPHY bounding box (#1272).

Two new checks: the row-group statistics of a GEOGRAPHY column must stay in
the canonical ranges the Parquet spec gives them (X in [-180, 180], Y in
[-90, 90], ``xmin > xmax`` legal as the antimeridian wrap), and a box that is
only the vertex extent -- it does not contain the geodesic edge bulge -- is
worth a warning, because a reader pruning row groups by it can skip rows that
really match.
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


def test_a_sample_with_nothing_boundable_is_skipped(tmp_path, monkeypatch):
    from geoparquet_io.core import validate as validate_module

    path = _geography_file(
        tmp_path, "POINT (10 20)", geography_bbox={"geometry": [(9.0, 19.0, 11.0, 21.0)]}
    )
    monkeypatch.setattr(validate_module, "_sampled_geodesic_bbox", lambda *a, **k: None)

    check = _check(path, "geography_stats_cover_edges_geometry")
    assert check.status == CheckStatus.SKIPPED
    assert "no geometries to compare" in check.message


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
