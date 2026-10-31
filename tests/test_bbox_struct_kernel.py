"""One bbox-struct kernel, one degenerate-geometry rule (#1367 concept 4).

``bbox_struct_array`` (Arrow) and ``bbox_struct_sql`` (DuckDB) are the only
per-row bbox builders, and they say the stats owner's rule: a NULL, EMPTY or
NaN geometry is a NULL struct; a located geometry with an infinite coordinate
raises the degenerate-geometry error (#1165) instead of writing a box no
reader can use.
"""

from __future__ import annotations

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
