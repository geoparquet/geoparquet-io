"""One geometry-stats and bounds owner, one degenerate-geometry policy (#1367 concept 5).

``geo_metadata.geometry_stats_of`` / ``bounds_of`` / ``geometry_types_of`` take
one source union -- an Arrow table, a DuckDB query, or a Parquet file path --
and every source answers the same question the same way:

* NULL and EMPTY geometries contribute no bbox, but every non-NULL geometry
  contributes its declared type, including EMPTY shapes.
* An empty or all-degenerate input yields ``bbox=None``: never
  ``[0, 0, 0, 0]``, never an inverted ``[inf, inf, -inf, -inf]``, never a raise.
  Only empty or all-NULL input yields ``types=[]``.
* A located geometry with an infinite coordinate is not measurable: every
  source raises one ``GeometryError`` naming it (#1165), so no write path
  can put ``Infinity`` into a ``geo`` bbox (#236).
* ``antimeridian=True`` reports a dataset astride the antimeridian in the
  RFC 7946 wrap form (``xmin > xmax``), on every source.
* ``geometry_types`` is sorted, whichever source computed it.

The table below runs every case through the Arrow path, the DuckDB path and
the file path and asserts they agree with each other and with the expected
answer.
"""

from __future__ import annotations

import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.duckdb_utils import get_duckdb_connection, register_arrow
from geoparquet_io.core.exceptions import GeometryError
from geoparquet_io.core.geo_metadata import (
    GeometryStats,
    QuerySource,
    bounds_of,
    geometry_stats_of,
    geometry_types_of,
)

INF = float("inf")
NAN = float("nan")


def _pt(x: float, y: float) -> bytes:
    return struct.pack("<BI2d", 1, 1, x, y)


def _line(*coords: float) -> bytes:
    return struct.pack("<BII", 1, 2, len(coords) // 2) + struct.pack(f"<{len(coords)}d", *coords)


def _poly(xmin: float, ymin: float, xmax: float, ymax: float) -> bytes:
    ring = [(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax), (xmin, ymin)]
    return (
        struct.pack("<BII", 1, 3, 1)
        + struct.pack("<I", len(ring))
        + b"".join(struct.pack("<2d", x, y) for x, y in ring)
    )


def _multipoly(*polys: bytes) -> bytes:
    return struct.pack("<BII", 1, 6, len(polys)) + b"".join(polys)


EMPTY_POINT = _pt(NAN, NAN)
EMPTY_POLYGON = struct.pack("<BII", 1, 3, 0)
EMPTY_COLLECTION = struct.pack("<BII", 1, 7, 0)
ASTRIDE = _multipoly(_poly(179.5, 0.0, 180.0, 1.0), _poly(-180.0, 0.0, -179.5, 1.0))

#: case -> (geometries, expected bbox, expected types, expected antimeridian bbox)
CASES = {
    "empty": ([], None, [], None),
    "all-null": ([None, None], None, [], None),
    "all-empty": (
        [EMPTY_POLYGON, EMPTY_POINT, EMPTY_COLLECTION],
        None,
        ["GeometryCollection", "Point", "Polygon"],
        None,
    ),
    "mixed": (
        [_pt(1.0, 2.0), None, EMPTY_POLYGON, EMPTY_POINT, _pt(3.0, 4.0), _line(2.0, 2.0, 5.0, 1.0)],
        [1.0, 1.0, 5.0, 4.0],
        ["LineString", "Point", "Polygon"],
        [1.0, 1.0, 5.0, 4.0],
    ),
    "antimeridian": (
        [ASTRIDE, None, EMPTY_POLYGON],
        [-180.0, 0.0, 180.0, 1.0],
        ["MultiPolygon", "Polygon"],
        [179.5, 0.0, -179.5, 1.0],
    ),
}


def _table(geoms: list[bytes | None]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(range(len(geoms)), pa.int64()),
            "geometry": pa.array(geoms, pa.binary()),
        }
    )


def _file(tmp_path, geoms) -> str:
    path = tmp_path / "data.parquet"
    pq.write_table(_table(geoms), str(path))
    return str(path)


def _all_sources(tmp_path, geoms):
    """The case through each source form: (label, stats-callable)."""
    table = _table(geoms)
    path = _file(tmp_path, geoms)

    def via_query(fn, **kwargs):
        con = get_duckdb_connection(load_spatial=True)
        try:
            register_arrow(con, "t", table)
            return fn(QuerySource(con, "SELECT * FROM t"), "geometry", **kwargs)
        finally:
            con.close()

    def via_native_query(fn, **kwargs):
        # The query hands the owner a GEOMETRY column rather than WKB bytes.
        con = get_duckdb_connection(load_spatial=True)
        try:
            register_arrow(con, "t", table)
            query = "SELECT ST_GeomFromWKB(geometry) AS geometry FROM t"
            return fn(QuerySource(con, query), "geometry", **kwargs)
        finally:
            con.close()

    return [
        ("arrow", lambda fn, **kw: fn(table, "geometry", **kw)),
        ("query", via_query),
        ("native-query", via_native_query),
        ("file", lambda fn, **kw: fn(path, "geometry", **kw)),
    ]


@pytest.mark.parametrize("case", sorted(CASES))
def test_every_source_gives_the_same_stats(case, tmp_path):
    geoms, bbox, types, _ = CASES[case]
    for label, run in _all_sources(tmp_path, geoms):
        stats = run(geometry_stats_of)
        assert isinstance(stats, GeometryStats), label
        assert (stats.bbox, stats.geometry_types) == (bbox, types), f"{case} via {label}"
        assert run(bounds_of) == bbox, f"{case} via {label}"
        assert run(geometry_types_of) == types, f"{case} via {label}"


@pytest.mark.parametrize("case", sorted(CASES))
def test_every_source_gives_the_same_antimeridian_bounds(case, tmp_path):
    geoms, _, _, wrapped = CASES[case]
    for label, run in _all_sources(tmp_path, geoms):
        assert run(bounds_of, antimeridian=True) == wrapped, f"{case} via {label}"


@pytest.mark.parametrize(
    "geometry",
    [_pt(INF, 0.0), _line(0.0, 0.0, INF, 1.0), _pt(1.0, -INF)],
    ids=["point-inf-x", "line-inf-x", "point-neg-inf-y"],
)
def test_an_infinite_coordinate_is_one_clear_error_on_every_source(geometry, tmp_path):
    """#1165: the stats pass named neither the value nor the column; #236: never Infinity."""
    for label, run in _all_sources(tmp_path, [_pt(1.0, 1.0), geometry, None]):
        for fn in (geometry_stats_of, bounds_of):
            with pytest.raises(GeometryError, match=r"'geometry'.*non-finite") as caught:
                run(fn)
            assert "inf" in str(caught.value).lower(), label
        # The types alone need no envelope, so they are still answered.
        assert run(geometry_types_of) in (["Point"], ["LineString", "Point"]), label


def test_bbox_column_fast_path_agrees_and_skips_null_boxes(tmp_path):
    """``bbox_column=`` reads a covering's struct instead of measuring the geometry."""
    table = pa.table(
        {
            "geometry": pa.array([_pt(1.0, 2.0), None, _pt(3.0, 4.0)], pa.binary()),
            "bbox": pa.array(
                [
                    {"xmin": 1.0, "ymin": 2.0, "xmax": 1.0, "ymax": 2.0},
                    None,
                    {"xmin": 3.0, "ymin": 4.0, "xmax": 3.0, "ymax": 4.0},
                ]
            ),
        }
    )
    path = tmp_path / "b.parquet"
    pq.write_table(table, str(path))
    assert bounds_of(str(path), "geometry", bbox_column="bbox") == [1.0, 2.0, 3.0, 4.0]
    assert bounds_of(table, "geometry", bbox_column="bbox") == [1.0, 2.0, 3.0, 4.0]


def test_a_missing_geometry_column_measures_nothing(tmp_path):
    table = _table([_pt(1.0, 2.0)])
    assert geometry_stats_of(table, "nope") == GeometryStats(None, [])
    con = get_duckdb_connection(load_spatial=True)
    try:
        register_arrow(con, "t", table)
        assert geometry_stats_of(QuerySource(con, "SELECT * FROM t"), "nope") == GeometryStats(
            None, []
        )
    finally:
        con.close()


def test_types_carry_the_dimension_suffix_on_every_source(tmp_path):
    point_z = struct.pack("<BI3d", 1, 1001, 1.0, 2.0, 3.0)
    for label, run in _all_sources(tmp_path, [point_z, _pt(0.0, 0.0)]):
        assert run(geometry_types_of) == ["Point", "Point Z"], label
