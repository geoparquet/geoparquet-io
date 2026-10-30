"""One query-schema probe, one native-geo-type probe (#1367 concepts 10 and 17).

``duckdb_utils.query_schema`` (Arrow) and its DuckDB-type view
``duckdb_utils.describe_query`` are the only ways gpio asks what a query or
relation outputs; ``duckdb_utils.parse_duckdb_geo_type`` (and its two predicates) the only
way it reads DuckDB's own GEOMETRY type name; ``duckdb_metadata.native_geo_type`` / ``is_native_geo_column``
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


# --- guards ---------------------------------------------------------------------------

#: A schema probe: ``LIMIT 0``, ``DESCRIBE``, or a ``TYPEOF(col)`` that reads a row for its type.
_PROBE_SQL = re.compile(r"\bLIMIT\s+0\b|\bDESCRIBE\b|\b(?i:typeof)\s*\(")

#: The probe owner's own functions: the only code allowed to spell a schema probe.
#: ``_query_schema_without_variant`` is ``query_schema``'s VARIANT fallback.
PROBE_OWNER = "geoparquet_io/core/duckdb_utils.py"
PROBE_OWNER_FUNCTIONS = frozenset(
    {"describe_query", "query_schema", "_query_schema_without_variant"}
)

#: ``LIMIT 0`` / ``DESCRIBE`` literals left outside the owner functions, per file.
#: Only sites inside functions a later track deletes; each names the track and may
#: only go down (a routed site lowers its file's number in the same commit).
PROBE_BASELINE = {
    # Not a DuckDB probe: a ``LIMIT 0`` sent to the CARTO remote SQL API.
    "geoparquet_io/core/carto.py": 1,  # 421
    "geoparquet_io/core/convert.py": 3,  # T3: 226, 398, 539 (functions T3 deletes)
    "geoparquet_io/core/partition/auto_resolution.py": 1,  # T3: 371
    "geoparquet_io/core/process/aggregate/common.py": 1,  # T3: 98
    "geoparquet_io/core/process/aggregate/grid_common.py": 1,  # T3: 258
}

#: A Parquet logical type read as a string, as either reader renders it: DuckDB's
#: ``GeometryType(...)``/``GeographyType(...)`` anywhere in a literal, or PyArrow's
#: ``Geometry(``/``Geography(`` at its start; plus a bare family name used as a
#: prefix or substring test (``lt.startswith("Geometry")``, ``"Geography" in lt``).
#: Comparing an owner-decided *label* (``== "Geography"``) is not a probe.
_NATIVE_PROBE = re.compile(r"(?<!ST_)Geo(?:graphy|metry)Type|^Geo(?:graphy|metry)\\?\(")
_FAMILIES = frozenset({"Geometry", "Geography"})

#: Native-type string probes left outside ``duckdb_metadata.py``. May only go down.
NATIVE_BASELINE: dict[str, int] = {}

#: Tests of a DuckDB type name (DESCRIBE / TYPEOF output) for GEOMETRY outside
#: ``duckdb_utils.py`` / ``duckdb_metadata.py``: ``"GEOMETRY" in t``, ``t.startswith("GEOMETRY")``,
#: ``t.upper() == "GEOMETRY"`` (owner: ``duckdb_utils.parse_duckdb_geo_type``). Comparing an owner's answer
#: (``parse_duckdb_geo_type(t)[0] == "GEOMETRY"``, ``native_geo_type(lt) == ...``)
#: is not one. May only go down.
DUCKDB_GEO_TYPE_BASELINE = {
    "geoparquet_io/core/convert.py": 1,  # T3: 228 (inside the 226 probe's function)
    "geoparquet_io/core/partition/auto_resolution.py": 1,  # T3: 373
    "geoparquet_io/core/process/aggregate/common.py": 1,  # T3: 100
}

_GEO_TYPE_OWNERS = frozenset({"native_geo_type", "parse_duckdb_geo_type", "is_native_geo_column"})


def _docstring_ids(tree: ast.AST) -> set[int]:
    return {
        id(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
    }


def _string_literals(tree: ast.AST, skip: set[int] = frozenset()):
    docstrings = _docstring_ids(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) not in docstrings and id(node) not in skip:
                yield node


def _owner_function_nodes(tree: ast.AST, names: frozenset[str]) -> set[int]:
    return {
        id(sub)
        for func in ast.walk(tree)
        if isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)) and func.name in names
        for sub in ast.walk(func)
    }


def _modules(root: Path):
    for path in sorted(root.rglob("*.py")):
        yield path.relative_to(root.parent).as_posix(), ast.parse(path.read_text(encoding="utf-8"))


def _probe_counts(root: Path = PACKAGE) -> Counter:
    counts: Counter = Counter()
    owner = PROBE_OWNER.split("/", 1)[1]
    for rel, tree in _modules(root):
        skip = (
            _owner_function_nodes(tree, PROBE_OWNER_FUNCTIONS)
            if rel.split("/", 1)[1] == owner
            else set()
        )
        counts[rel] += sum(1 for n in _string_literals(tree, skip) if _PROBE_SQL.search(n.value))
    return +counts


def _consts(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.Tuple):
        return [v for e in node.elts for v in _consts(e)]
    return []


_MEMBERSHIP = (ast.In, ast.NotIn)  # codespell:ignore


def _prefix_or_substring_tests(tree: ast.AST, wanted) -> int:
    """``x.startswith(c)`` / ``x.endswith(c)`` / ``c in x`` with a constant ``c`` in ``wanted``."""
    found = 0
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("startswith", "endswith")
        ):
            found += any(wanted(c) for a in node.args for c in _consts(a))
        elif isinstance(node, ast.Compare) and any(isinstance(op, _MEMBERSHIP) for op in node.ops):
            found += any(wanted(c) for c in _consts(node.left))
    return found


def _native_counts(root: Path = PACKAGE) -> Counter:
    counts: Counter = Counter()
    for rel, tree in _modules(root):
        if rel.endswith("/duckdb_metadata.py"):
            continue
        counts[rel] += sum(1 for n in _string_literals(tree) if _NATIVE_PROBE.search(n.value))
        counts[rel] += _prefix_or_substring_tests(tree, _FAMILIES.__contains__)
        if any(
            isinstance(n, ast.FunctionDef) and n.name == "_is_geography_column"
            for n in ast.walk(tree)
        ):
            counts[f"{rel}::_is_geography_column"] += 1
    return +counts


def _owner_names(tree: ast.AST) -> set[str]:
    """The geo-type owners' names in this module, ``import ... as`` aliases included."""
    return _GEO_TYPE_OWNERS | {
        alias.asname
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
        if alias.name in _GEO_TYPE_OWNERS and alias.asname
    }


def _owner_answers(tree: ast.AST, owners: set[str]) -> set[str]:
    """Names bound straight from a geo-type owner call (``parsed = parse_duckdb_geo_type(t)``)."""
    return {
        target.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign) and _is_owner_call(node.value, owners, set())
        for target in node.targets
        if isinstance(target, ast.Name)
    }


def _is_owner_call(node: ast.AST, owners: set[str], answers: set[str]) -> bool:
    if isinstance(node, ast.Subscript):
        node = node.value
    if isinstance(node, ast.Name):
        return node.id in answers
    if isinstance(node, ast.Call):
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        return name in owners
    return False


def _duckdb_geo_type_checks(tree: ast.AST) -> int:
    owners = _owner_names(tree)
    answers = _owner_answers(tree, owners)
    found = _prefix_or_substring_tests(tree, "GEOMETRY".__eq__)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare) or not any(
            isinstance(op, (ast.Eq, ast.NotEq)) for op in node.ops
        ):
            continue
        operands = [node.left, *node.comparators]
        if any(c == "GEOMETRY" for x in operands for c in _consts(x)) and not any(
            _is_owner_call(x, owners, answers) for x in operands
        ):
            found += 1
    return found


#: The DuckDB type-name owner's own functions, the only code that may spell the test.
GEO_TYPE_OWNER_FUNCTIONS = frozenset(
    {"parse_duckdb_geo_type", "duckdb_type_is_geometry", "duckdb_type_contains_geometry"}
)


def _without_functions(tree: ast.AST, names: frozenset[str]) -> ast.AST:
    """*tree* with the named top-level functions dropped."""
    kept = [
        n
        for n in getattr(tree, "body", [])
        if not (isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names)
    ]
    return ast.Module(body=kept, type_ignores=[])


def _duckdb_geo_type_counts(root: Path = PACKAGE) -> Counter:
    owner = PROBE_OWNER.split("/", 1)[1]
    return +Counter(
        {
            rel: _duckdb_geo_type_checks(
                _without_functions(tree, GEO_TYPE_OWNER_FUNCTIONS)
                if rel.split("/", 1)[1] == owner
                else tree
            )
            for rel, tree in _modules(root)
            if not rel.endswith("/duckdb_metadata.py")
        }
    )


def test_schema_probes_only_go_down():
    assert dict(_probe_counts()) == PROBE_BASELINE


def test_native_type_probes_only_go_down():
    assert dict(_native_counts()) == NATIVE_BASELINE


def test_duckdb_geometry_type_checks_only_go_down():
    assert dict(_duckdb_geo_type_counts()) == DUCKDB_GEO_TYPE_BASELINE


def test_guards_catch_new_copies(tmp_path):
    pkg = tmp_path / "pkg"
    (pkg / "core").mkdir(parents=True)
    (pkg / "rogue.py").write_text(
        '"""DESCRIBE in a docstring is fine."""\n'
        "def _is_geography_column(rows, col):\n"
        '    return "GeographyType" in rows[col]\n\n'
        "def pa_native(t):\n"
        '    return str(t).startswith(("Geometry", "Geography"))\n\n'
        "def pa_regex(t):\n"
        '    return re.match("Geography\\\\(.*algorithm=(\\\\w+)", t)\n\n'
        "def cols(con, src):\n"
        '    return con.execute(f"SELECT * FROM {src} LIMIT 0").description\n\n'
        "def col_type(con, src, c):\n"
        '    return con.execute(f"SELECT typeof({c}) FROM {src} LIMIT 1").fetchone()\n\n'
        "def is_geom(t):\n"
        '    return t.upper() == "GEOMETRY" or t.startswith("GEOMETRY") or "GEOMETRY" in t\n'
    )
    # Near-misses: comparing an owner's answer or a decided label is not a probe.
    (pkg / "clean.py").write_text(
        "from x.duckdb_metadata import parse_duckdb_geo_type as parse_type\n\n"
        "def f(t, lt, info):\n"
        "    parsed = parse_type(t)\n"
        '    a = parsed[0] == "GEOMETRY"\n'
        '    b = native_geo_type(lt) != "GEOMETRY"\n'
        '    c = info["logical_type"] == "Geography"\n'
        '    return a, b, c, "geometry" in info\n'
    )
    # The owner's probe functions may spell the probe; any other function there may not.
    (pkg / "core" / "duckdb_utils.py").write_text(
        "def describe_query(con, sql):\n"
        '    return con.execute(f"DESCRIBE ({sql})").fetchall()\n\n'
        "def query_schema(con, sql):\n"
        '    return con.sql(f"SELECT * FROM ({sql}) LIMIT 0")\n\n'
        "def _wrap(con, q):\n"
        '    return con.execute(f"DESCRIBE ({q})").fetchall()\n\n'
        "def duckdb_type_contains_geometry(t):\n"
        '    return "GEOMETRY" in t\n\n'
        "def _wkb(t):\n"
        '    return t.startswith("GEOMETRY")\n'
    )
    assert dict(_probe_counts(pkg)) == {"pkg/rogue.py": 2, "pkg/core/duckdb_utils.py": 1}
    assert dict(_native_counts(pkg)) == {
        "pkg/rogue.py": 3,
        "pkg/rogue.py::_is_geography_column": 1,
    }
    assert dict(_duckdb_geo_type_counts(pkg)) == {"pkg/rogue.py": 3, "pkg/core/duckdb_utils.py": 1}
