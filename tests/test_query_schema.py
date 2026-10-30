"""One query-schema probe, one native-geo-type probe (#1367 concepts 10 and 17).

``duckdb_utils.query_schema`` (Arrow) and its DuckDB-type view
``duckdb_utils.describe_query`` are the only ways gpio asks what a query or
relation outputs; ``duckdb_metadata.native_geo_type`` / ``is_native_geo_column``
the only way it asks whether a column is natively GEOMETRY or GEOGRAPHY.
"""

from __future__ import annotations

import ast
import re
from collections import Counter
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


# --- guards ---------------------------------------------------------------------------

_PROBE_SQL = re.compile(r"\bLIMIT\s+0\b|\bDESCRIBE\b")

#: ``LIMIT 0`` / ``DESCRIBE`` literals left outside ``duckdb_utils.py``, per file.
#: The count may only go down: a site routed to ``query_schema``/``describe_query``
#: lowers its file's number here in the same commit. Reasons:
#: [T3] functions T3 deletes or owns (aggregate/common.py:92, auto_resolution.py:371,
#: convert.py:225/397/537, grid_common.py:258) -- T3c removes them;
#: [other] outside T1c's files or line ranges (hand-off in the track ledger), or a
#: SQL statement that is not a schema probe (DESCRIBE inside a larger query).
PROBE_BASELINE = {
    "geoparquet_io/api/table.py": 1,  # other
    "geoparquet_io/core/add/a5.py": 2,  # other
    "geoparquet_io/core/add/country_codes.py": 5,  # other
    "geoparquet_io/core/add/geometry_metrics.py": 1,  # other
    "geoparquet_io/core/add/h3.py": 2,  # other
    "geoparquet_io/core/add/kdtree.py": 1,  # other
    "geoparquet_io/core/add/quadkey.py": 2,  # other
    "geoparquet_io/core/add/s2.py": 2,  # other
    "geoparquet_io/core/benchmark.py": 1,  # other
    "geoparquet_io/core/carto.py": 1,  # other
    "geoparquet_io/core/convert.py": 4,  # T3 (225/397/537) + other (1182)
    "geoparquet_io/core/crs_utils.py": 1,  # other
    "geoparquet_io/core/extract.py": 2,  # other (699, 979)
    "geoparquet_io/core/geojson_stream.py": 3,  # other
    "geoparquet_io/core/geometry_detection.py": 5,  # other
    "geoparquet_io/core/geometry_repair.py": 1,  # other
    "geoparquet_io/core/hilbert_order.py": 1,  # other
    "geoparquet_io/core/inspect_utils.py": 1,  # other (685, a DESCRIBE subquery)
    "geoparquet_io/core/partition/admin_hierarchical.py": 1,  # other
    "geoparquet_io/core/partition/auto_resolution.py": 1,  # T3
    "geoparquet_io/core/partition/common.py": 1,  # other
    "geoparquet_io/core/pmtiles_chunks.py": 1,  # other
    "geoparquet_io/core/process/aggregate/by_admin.py": 1,  # other
    "geoparquet_io/core/process/aggregate/common.py": 3,  # T3 (92) + other
    "geoparquet_io/core/process/aggregate/grid_common.py": 1,  # T3 (258)
    "geoparquet_io/core/process/overview/detect.py": 1,  # other
    "geoparquet_io/core/sort_by_column.py": 2,  # other
    "geoparquet_io/core/str_order.py": 2,  # other
    "geoparquet_io/core/stream_io.py": 1,  # other (99)
    "geoparquet_io/core/wfs.py": 1,  # other
    "geoparquet_io/core/write_strategies/base.py": 1,  # other
    "geoparquet_io/core/write_strategies/duckdb_kv.py": 1,  # other
}

_NATIVE_PROBE = re.compile(r"(?<!ST_)Geo(graphy|metry)Type")

#: Native-type string probes left outside ``duckdb_metadata.py`` (outside T1c's
#: line ranges; hand-off in the track ledger). May only go down.
NATIVE_BASELINE = {
    "geoparquet_io/core/validate.py": 4,  # 2198, 2287, 3479, 3483
    "geoparquet_io/core/write_funnels.py": 1,  # 347
}


def _string_literals(tree: ast.AST):
    docstrings = {
        id(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) not in docstrings:
                yield node


def _counts(root: Path, owner: str, matches) -> Counter:
    counts: Counter = Counter()
    for path in sorted(root.rglob("*.py")):
        if path.name == owner:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        rel = path.relative_to(root.parent).as_posix()
        counts[rel] += sum(1 for node in _string_literals(tree) if matches(node.value))
        if any(
            isinstance(n, ast.FunctionDef) and n.name == "_is_geography_column"
            for n in ast.walk(tree)
        ):
            counts[f"{rel}::_is_geography_column"] += 1
    return +counts


def _probe_counts(root: Path = PACKAGE) -> Counter:
    return Counter(
        {
            k: v
            for k, v in _counts(root, "duckdb_utils.py", _PROBE_SQL.search).items()
            if "::" not in k
        }
    )


def _native_counts(root: Path = PACKAGE) -> Counter:
    return _counts(root, "duckdb_metadata.py", _NATIVE_PROBE.search)


def test_schema_probes_only_go_down():
    assert dict(_probe_counts()) == PROBE_BASELINE


def test_native_type_probes_only_go_down():
    assert dict(_native_counts()) == NATIVE_BASELINE


def test_guards_catch_new_copies(tmp_path):
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "rogue.py").write_text(
        '"""DESCRIBE in a docstring is fine."""\n'
        "def _is_geography_column(rows, col):\n"
        '    return "GeographyType" in rows[col]\n\n'
        "def cols(con, src):\n"
        '    return con.execute(f"SELECT * FROM {src} LIMIT 0").description\n'
    )
    assert dict(_probe_counts(pkg)) == {"pkg/rogue.py": 1}
    assert dict(_native_counts(pkg)) == {
        "pkg/rogue.py": 1,
        "pkg/rogue.py::_is_geography_column": 1,
    }
