"""One query-schema probe, one native-geo-type probe (#1367 concepts 10 and 17).

``duckdb_utils.query_schema`` (Arrow) and its DuckDB-type view
``duckdb_utils.describe_query`` are the only ways gpio asks what a query or
relation outputs; ``duckdb_utils.parse_duckdb_geo_type`` (and its two predicates) the only
way it reads DuckDB's own GEOMETRY type name; ``duckdb_metadata.native_geo_type`` / ``is_native_geo_column``
the only way it asks whether a column is natively GEOMETRY or GEOGRAPHY.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from geoparquet_io.core.duckdb_metadata import is_native_geo_column, native_geo_type
from geoparquet_io.core.duckdb_utils import (
    _get_query_column_type,
    _get_query_columns,
    _output_types,
    describe_query,
    duckdb_capabilities,
    duckdb_type_contains_geometry,
    duckdb_type_is_geometry,
    parse_duckdb_geo_type,
    query_schema,
)

REPO = Path(__file__).resolve().parents[1]
PACKAGE = REPO / "geoparquet_io"


@pytest.fixture
def con():
    connection = duckdb.connect()
    yield connection
    connection.close()


# --- the probe ---------------------------------------------------------------------


def test_plain_query(con):
    schema = query_schema(con, "SELECT 1::INTEGER AS a, 'x' AS b")
    assert schema.names == ["a", "b"]
    assert schema.field("a").type == pa.int32()
    assert describe_query(con, "SELECT 1::INTEGER AS a, 'x' AS b") == [
        ("a", "INTEGER"),
        ("b", "VARCHAR"),
    ]


def test_relation_is_not_scanned(con):
    """LIMIT 0 / DESCRIBE: a query that would fail on its first row still has a schema."""
    sql = "SELECT error('boom')::INTEGER AS a FROM range(3)"
    assert query_schema(con, sql).names == ["a"]
    assert describe_query(con, sql) == [("a", "INTEGER")]


def test_parameters(con):
    sql = "SELECT ?::BIGINT AS n, ?::VARCHAR AS s"
    assert query_schema(con, sql, [1, "x"]).names == ["n", "s"]
    assert _output_types(con, sql, [1, "x"]) == ["BIGINT", "VARCHAR"]


def test_trailing_comment_and_semicolon(con):
    sql = "SELECT 1 AS a; -- trailing"
    assert query_schema(con, "SELECT 1 AS a -- trailing").names == ["a"]
    assert _get_query_columns(con, sql.split(";")[0] + ";") == ["a"]


def test_variant_column(con):
    """#1362/#1352: a VARIANT column has a schema on 1.5 (a sentinel) and on 2.0 (real)."""
    sql = "SELECT 1 AS id, {'a': 1}::VARIANT AS v"
    schema = query_schema(con, sql)
    assert schema.names == ["id", "v"]
    if duckdb_capabilities().is_v2:
        # 2.0 exports VARIANT for real, as Arrow's parquet.variant extension.
        assert schema.field("v").metadata[b"ARROW:extension:name"] == b"arrow.parquet.variant"
    else:
        # 1.5 cannot export it; the probe keeps the column as a binary sentinel.
        assert schema.field("v").type == pa.binary()


def test_failing_query_raises(con):
    with pytest.raises(duckdb.Error):
        query_schema(con, "SELECT * FROM no_such_table")
    with pytest.raises(duckdb.Error):
        describe_query(con, "SELECT * FROM no_such_table")


def test_non_variant_arrow_failure_reraises():
    class Boom:
        def arrow(self):
            raise OSError("some unrelated arrow failure")

    class FakeCon:
        def sql(self, sql, params=None):
            return Boom()

    with pytest.raises(OSError, match="unrelated"):
        query_schema(FakeCon(), "SELECT 1")


def test_column_type_view_tolerates_a_miss(con):
    assert _get_query_column_type(con, "SELECT 1 AS a", "a") == "INTEGER"
    assert _get_query_column_type(con, "SELECT 1 AS a", "b") is None
    assert _get_query_column_type(con, "SELECT * FROM no_such_table", "a") is None


# --- native GEOMETRY/GEOGRAPHY probe -----------------------------------------------


@pytest.mark.parametrize(
    ("logical", "expected"),
    [
        ("GeometryType(crs=...)", "GEOMETRY"),
        ("GeographyType(crs=..., algorithm=spherical)", "GEOGRAPHY"),
        ("Geometry(crs=)", "GEOMETRY"),  # PyArrow's rendering
        ("Geography(crs=, algorithm=spherical)", "GEOGRAPHY"),
        ("String", None),
        ("", None),
        (None, None),
    ],
)
def test_native_geo_type(logical, expected):
    assert native_geo_type(logical) == expected


def test_is_native_geo_column():
    rows = [
        {"name": "geom", "logical_type": "GeometryType(crs=...)"},
        {"name": "geog", "logical_type": "GeographyType(crs=...)"},
        {"name": "name", "logical_type": None},
    ]
    assert is_native_geo_column(rows, "geom") == "GEOMETRY"
    assert is_native_geo_column(rows, "geog") == "GEOGRAPHY"
    assert is_native_geo_column(rows, "name") is None
    assert is_native_geo_column(rows, "missing") is None
    assert is_native_geo_column(None, "geom") is None


# --- DuckDB's own GEOMETRY type name ---------------------------------------------


@pytest.mark.parametrize(
    ("type_name", "parsed"),
    [
        ("GEOMETRY", ("GEOMETRY", None)),
        ("geometry", ("GEOMETRY", None)),
        ("GEOMETRY('EPSG:4326')", ("GEOMETRY", "EPSG:4326")),
        ("GEOMETRY('{\"name\": \"it''s\"}')", ("GEOMETRY", '{"name": "it\'s"}')),
        ("GEOGRAPHY('OGC:CRS84')", ("GEOGRAPHY", "OGC:CRS84")),
        ("GEOMETRY[]", None),
        ("STRUCT(g GEOMETRY)", None),
        ("BLOB", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_duckdb_geo_type(type_name, parsed):
    assert parse_duckdb_geo_type(type_name) == parsed
    assert duckdb_type_is_geometry(type_name) is (parsed is not None and parsed[0] == "GEOMETRY")


def test_duckdb_type_contains_geometry_reaches_nested_types():
    assert duckdb_type_contains_geometry("STRUCT(g GEOMETRY)")
    assert duckdb_type_contains_geometry("GEOMETRY[]")
    assert duckdb_type_contains_geometry("GEOMETRY('EPSG:4326')")
    assert not duckdb_type_contains_geometry("BLOB")
    assert not duckdb_type_contains_geometry(None)


def test_describe_renders_what_the_parser_reads(con):
    """The spellings above are the ones DuckDB's DESCRIBE actually produces."""
    con.execute("LOAD spatial")
    rows = describe_query(
        con,
        "SELECT ST_Point(0, 0) AS g, ST_SetCRS(ST_Point(0, 0), 'EPSG:4326') AS c, "
        "[ST_Point(0, 0)] AS l",
    )
    types = dict(rows)
    assert duckdb_type_is_geometry(types["g"])
    assert parse_duckdb_geo_type(types["c"]) == ("GEOMETRY", "EPSG:4326")
    assert not duckdb_type_is_geometry(types["l"])
    assert duckdb_type_contains_geometry(types["l"])
