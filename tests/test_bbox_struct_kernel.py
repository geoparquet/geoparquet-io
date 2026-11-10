"""One bbox-struct kernel, one degenerate-geometry rule (#1367 concept 4).

``bbox_struct_array`` (Arrow) and ``bbox_struct_sql`` (DuckDB) are the only
per-row bbox builders, and they say the stats owner's rule: a NULL, EMPTY or
NaN geometry is a NULL struct; a located geometry with an infinite coordinate
raises the degenerate-geometry error (#1165) instead of writing a box no
reader can use.
"""

from __future__ import annotations

import re
import struct

import pyarrow as pa
import pytest

from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.exceptions import GeometryError
from geoparquet_io.core.geo_metadata import bbox_struct_array, bbox_struct_sql, bbox_struct_type


def _point_wkb(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, x, y)


EMPTY_POINT = _point_wkb(float("nan"), float("nan"))
EMPTY_POLYGON = struct.pack("<BII", 1, 3, 0)


@pytest.fixture
def con():
    c = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    yield c
    c.close()


def _arrow(values):
    table = pa.table({"g": pa.array(values, pa.binary())})
    return bbox_struct_array(table, "g", bbox_struct_type()).to_pylist()


def _sql(con, values):
    table = pa.table({"g": pa.array(values, pa.binary())})
    con.register("t", table)
    expr = bbox_struct_sql("ST_GeomFromWKB(g)", "g")
    return [r[0] for r in con.sql(f"SELECT {expr} FROM t").fetchall()]


CASES = [_point_wkb(1, 2), None, EMPTY_POINT, EMPTY_POLYGON]
EXPECTED = [{"xmin": 1.0, "ymin": 2.0, "xmax": 1.0, "ymax": 2.0}, None, None, None]


def test_arrow_kernel_rule():
    assert _arrow(CASES) == EXPECTED


def test_sql_expression_says_the_same_rule(con):
    assert _sql(con, CASES) == EXPECTED


def test_unreadable_bytes_are_a_null_box():
    assert _arrow([_point_wkb(1, 2), b"\x01\x02"])[1] is None


def test_arrow_kernel_raises_on_an_infinite_coordinate():
    with pytest.raises(GeometryError, match="non-finite"):
        _arrow([_point_wkb(1, 2), _point_wkb(float("inf"), 0)])


def test_sql_expression_raises_on_an_infinite_coordinate(con):
    with pytest.raises(Exception, match="non-finite"):
        _sql(con, [_point_wkb(1, 2), _point_wkb(float("inf"), 0)])


def _query_stats_of(con, values):
    from geoparquet_io.core.geo_metadata import QuerySource, geometry_stats_of

    con.register("s", pa.table({"g": pa.array(values, pa.binary())}))
    return geometry_stats_of(QuerySource(con, "SELECT ST_GeomFromWKB(g) AS g FROM s"), "g")


def test_sql_stats_say_the_struct_rule_for_a_nan_envelope(con):
    """A NaN envelope is no location on the stats path too (audit 2, finding D).

    DuckDB orders NaN last and calls ``NaN <= NaN`` true, so without the
    struct rule's NaN guard a NaN box counted as located and then failed the
    finite check.
    """
    from geoparquet_io.core.geo_metadata import QuerySource, geometry_stats_of

    con.execute(
        "CREATE TABLE b AS SELECT * FROM (VALUES "
        "({'xmin': 1.0, 'ymin': 2.0, 'xmax': 1.0, 'ymax': 2.0}), "
        "({'xmin': 'NaN'::DOUBLE, 'ymin': 'NaN'::DOUBLE, "
        "'xmax': 'NaN'::DOUBLE, 'ymax': 'NaN'::DOUBLE})) t(bbox)"
    )
    stats = geometry_stats_of(QuerySource(con, "SELECT bbox FROM b"), "g", bbox_column="bbox")
    assert stats.bbox == [1.0, 2.0, 1.0, 2.0]


def test_the_sql_paths_raise_the_owners_one_message(con):
    """The struct SQL's error is ``_non_finite_message``'s text, not a copy of it."""
    import geoparquet_io.core.geo_metadata as gm

    message = gm._non_finite_message("g")
    with pytest.raises(Exception, match=re.escape(message)):
        _sql(con, [_point_wkb(float("inf"), 0)])
    with pytest.raises(GeometryError, match="non-finite"):
        _query_stats_of(con, [_point_wkb(float("inf"), 0)])


def test_a_geoarrow_envelope_is_the_owners_read(con):
    """``bbox_struct_sql`` takes a ``GeometryRead``: a GeoArrow EMPTY (NaN) is NULL."""
    from geoparquet_io.core.geometry_detection import GeometryRead

    con.execute(
        "CREATE TABLE p AS SELECT * FROM (VALUES "
        "({'x': 1.0, 'y': 2.0}), ({'x': 'NaN'::DOUBLE, 'y': 'NaN'::DOUBLE})) t(g)"
    )
    read = GeometryRead.for_geoarrow("g", "point")
    expr = bbox_struct_sql("g", "g", read=read)
    rows = [r[0] for r in con.sql(f"SELECT {expr} FROM p").fetchall()]
    assert rows == [{"xmin": 1.0, "ymin": 2.0, "xmax": 1.0, "ymax": 2.0}, None]
