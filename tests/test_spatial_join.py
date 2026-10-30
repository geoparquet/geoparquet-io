"""One spatial-join predicate and one dedup decision (#1367 concept 16).

``core/duckdb_utils.py`` owns the admin spatial join: the ON clause
(:func:`build_spatial_join_condition`) and the query around it
(:func:`build_spatial_join_query`), whose ``one_match_per_input`` parameter
decides whether an input feature that intersects two target polygons comes out
once or once per match. The three admin commands build their join only through
it, so:

* #543 -- a feature in the overlap of two admin polygons is assigned / counted
  exactly once, to the polygon with the smallest admin code (the documented
  tiebreak), in ``gpio add admin-divisions``, ``gpio partition admin`` and
  ``gpio process aggregate admin``;
* #942 / #935 -- every identifier in the join is quoted by the owner, so a bbox
  column whose name needs quoting works on both the input and the admin side;
* a guard pins ``ST_Intersects(`` SQL literals outside the owner.

The admin boundaries are a network resource, so the tests substitute a local
Overture-shaped file for it at ``AdminDatasetFactory.create``; everything else
runs through the CLI.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import admin_datasets
from geoparquet_io.core import duckdb_utils as du
from geoparquet_io.core.duckdb_utils import quote_identifier, sql_path

# AA and BB overlap on x in [1, 2]: the point at x=1.5 sits in both.
AA = "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"
BB = "POLYGON((1 0, 3 0, 3 2, 1 2, 1 0))"
POINTS = [(1, 0.5, 1.0), (2, 1.5, 1.0), (3, 2.5, 1.0), (4, 10.0, 10.0)]
EXPECTED_COUNTRY = {1: "AA", 2: "AA", 3: "BB", 4: None}


def _write_admin(path: Path, bbox_name: str = "bbox") -> str:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    rows = ", ".join(
        f"('country', '{code}', NULL, ST_GeomFromText('{wkt}'))"
        for code, wkt in [("BB", BB), ("AA", AA)]
    )
    con.execute(
        f"""
        COPY (
            SELECT subtype, country, region, geometry,
                {{'xmin': ST_XMin(geometry), 'xmax': ST_XMax(geometry),
                  'ymin': ST_YMin(geometry), 'ymax': ST_YMax(geometry)}} AS {quote_identifier(bbox_name)}
            FROM (VALUES {rows}) AS t(subtype, country, region, geometry)
        ) TO {sql_path(str(path))} (FORMAT PARQUET)
        """
    )
    con.close()
    return str(path)


def _write_points(path: Path, bbox_name: str | None = None) -> str:
    """A GeoParquet of the four POINTS, optionally with a bbox covering column."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    values = ", ".join(f"({i}, ST_Point({x}, {y}))" for i, x, y in POINTS)
    bbox_select = ""
    if bbox_name:
        bbox_select = (
            ", struct_pack(xmin := ST_XMin(geometry), xmax := ST_XMax(geometry), "
            "ymin := ST_YMin(geometry), ymax := ST_YMax(geometry)) AS bbox_tmp"
        )
    con.execute(
        f"COPY (SELECT id, geometry{bbox_select} FROM (VALUES {values}) AS t(id, geometry)) "
        f"TO {sql_path(str(path))} (FORMAT PARQUET)"
    )
    con.close()
    table = pq.read_table(path)
    column = {"encoding": "WKB", "geometry_types": ["Point"]}
    if bbox_name:
        table = table.rename_columns(
            [bbox_name if n == "bbox_tmp" else n for n in table.column_names]
        )
        column["covering"] = {"bbox": {k: [bbox_name, k] for k in ("xmin", "ymin", "xmax", "ymax")}}
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": column}}
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    pq.write_table(table, path)
    return str(path)


@pytest.fixture
def local_admin(tmp_path, monkeypatch):
    """Point every admin command at a local Overture-shaped file.

    Returns a function taking the admin bbox column name, so a test can give
    the admin side a name that needs quoting.
    """

    def install(bbox_name: str = "bbox") -> str:
        admin_file = _write_admin(tmp_path / "admin.parquet", bbox_name)

        class LocalOverture(admin_datasets.OvertureAdminDataset):
            def get_bbox_column(self):
                return bbox_name

        monkeypatch.setattr(
            admin_datasets.AdminDatasetFactory,
            "create",
            classmethod(
                lambda cls, dataset_name, source_path=None, verbose=False: LocalOverture(
                    source_path=admin_file, verbose=verbose
                )
            ),
        )
        return admin_file

    return install


def _invoke(args):
    result = CliRunner().invoke(cli, args, catch_exceptions=False)
    assert result.exit_code == 0, result.output
    return result


def _add_admin(tmp_path, input_file):
    out = tmp_path / "out.parquet"
    _invoke(
        [
            "add",
            "admin-divisions",
            input_file,
            str(out),
            "--dataset",
            "overture",
            "--levels",
            "country",
        ]
    )
    rows = pq.read_table(out).to_pylist()
    return [(r["id"], r["overture_country"]) for r in rows]


def _partition_admin(tmp_path, input_file):
    out = tmp_path / "parts"
    _invoke(
        [
            "partition",
            "admin",
            input_file,
            str(out),
            "--dataset",
            "overture",
            "--levels",
            "country",
            "--hive",
        ]
    )
    assignments = []
    for part in sorted(out.rglob("*.parquet")):
        country = part.parent.name.split("=", 1)[1]
        assignments += [(i, country) for i in pq.read_table(part, columns=["id"])["id"].to_pylist()]
    return sorted(assignments)


def _aggregate_admin(tmp_path, input_file):
    out = tmp_path / "agg.parquet"
    _invoke(
        [
            "process",
            "aggregate",
            "admin",
            input_file,
            str(out),
            "--level",
            "country",
            "--out-geometry",
            "none",
        ]
    )
    return {r["admin_code"]: r["count"] for r in pq.read_table(out).to_pylist()}


# --- #543: one assignment per feature ------------------------------------------


class TestOneMatchPerInput:
    def test_add_admin_divisions_emits_each_feature_once(self, tmp_path, local_admin):
        local_admin()
        rows = _add_admin(tmp_path, _write_points(tmp_path / "in.parquet"))
        # Input order is kept and the overlap point (id 2) goes to the smaller code.
        assert rows == sorted(EXPECTED_COUNTRY.items())

    def test_partition_admin_writes_each_feature_to_one_partition(self, tmp_path, local_admin):
        local_admin()
        assignments = _partition_admin(tmp_path, _write_points(tmp_path / "in.parquet"))
        ids = [i for i, _country in assignments]
        assert sorted(ids) == sorted(set(ids)), assignments
        assert dict(assignments)[2] == "AA"

    def test_aggregate_admin_counts_each_feature_once(self, tmp_path, local_admin):
        local_admin()
        counts = _aggregate_admin(tmp_path, _write_points(tmp_path / "in.parquet"))
        assert counts == {"AA": 2, "BB": 1, "unassigned": 1}
        assert sum(counts.values()) == len(POINTS)


# --- #942 / #935: identifiers that need quoting ----------------------------------

ODD_NAME = 'b "box"'


class TestQuotedIdentifiers:
    def test_add_admin_divisions(self, tmp_path, local_admin):
        local_admin(ODD_NAME)
        rows = _add_admin(tmp_path, _write_points(tmp_path / "in.parquet", ODD_NAME))
        assert rows == sorted(EXPECTED_COUNTRY.items())

    def test_partition_admin(self, tmp_path, local_admin):
        local_admin(ODD_NAME)
        assignments = _partition_admin(tmp_path, _write_points(tmp_path / "in.parquet", ODD_NAME))
        assert dict(assignments) == {i: c for i, c in EXPECTED_COUNTRY.items() if c}

    def test_aggregate_admin(self, tmp_path, local_admin):
        local_admin(ODD_NAME)
        counts = _aggregate_admin(tmp_path, _write_points(tmp_path / "in.parquet"))
        assert counts == {"AA": 2, "BB": 1, "unassigned": 1}


# --- the owner's SQL -------------------------------------------------------------


class TestOwnerQuery:
    def test_one_match_per_input_is_a_spillable_dedup(self):
        sql = du.build_spatial_join_query(
            input_relation="read_parquet('x')",
            target_relation="t",
            target_select='b."code" AS "c"',
            condition=du.build_spatial_join_condition("geometry", "geometry"),
            one_match_per_input=True,
            match_order=['b."code"'],
        )
        assert "row_number() OVER ()" in sql  # streaming: no PARTITION BY
        assert "DISTINCT ON (__gpio_input_row)" in sql
        assert "PARTITION BY" not in sql and "QUALIFY" not in sql

    def test_all_matches_is_the_plain_join(self):
        sql = du.build_spatial_join_query(
            input_relation="t_in",
            target_relation="t",
            target_select='b."code" AS "c"',
            condition="TRUE",
            one_match_per_input=False,
        )
        assert "DISTINCT" not in sql and "row_number" not in sql
        assert "LEFT JOIN t b" in sql and "ON TRUE" in sql

    def test_one_match_requires_a_match_order(self):
        with pytest.raises(ValueError, match="match_order"):
            du.build_spatial_join_query(
                input_relation="t_in",
                target_relation="t",
                target_select="b.x",
                condition="TRUE",
                one_match_per_input=True,
            )

    def test_point_input_gets_a_point_in_bbox_prefilter(self):
        cond = du.build_spatial_join_condition(
            "__cen", "geom", target_bbox_col='b"x', input_alias="s", input_is_point=True
        )
        assert 'b."b""x".xmin <= ST_X(s."__cen")' in cond
        assert cond.index("xmin") < cond.index("ST_Intersects")

    def test_point_input_without_target_bbox_is_bare(self):
        cond = du.build_spatial_join_condition(
            "__cen", "geom", input_alias="s", input_is_point=True
        )
        assert cond == 'ST_Intersects(b."geom", s."__cen")'


# --- guard: the predicate is spelled only in the owner ---------------------------

PACKAGE = Path(__file__).resolve().parent.parent / "geoparquet_io"
OWNER = "core/duckdb_utils.py"

# These sites filter rows against a constant geometry (a --bbox/--geometry
# extract filter, or a remote service's SQL dialect); none joins two tables.
FILTER_BASELINE = {
    "core/carto.py": 2,
    "core/extract.py": 2,
    "core/extract_bigquery.py": 4,
    "core/wfs.py": 1,
}


def _docstring_ids(tree):
    ids = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                ids.add(id(body[0].value))
    return ids


def st_intersects_literals(source: str) -> int:
    """Count string constants (f-string parts included) that spell ``ST_Intersects(``."""
    tree = ast.parse(source)
    skip = _docstring_ids(tree)
    return sum(
        1
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in skip
        and "ST_Intersects(" in node.value
    )


def test_st_intersects_is_spelled_only_in_the_owner():
    found = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        rel = path.relative_to(PACKAGE).as_posix()
        if rel == OWNER:
            continue
        count = st_intersects_literals(path.read_text())
        if count:
            found[rel] = count
    assert found == FILTER_BASELINE


def test_guard_sees_a_hand_rolled_join():
    hand_rolled = 'def f(g):\n    """ST_Intersects( in a docstring is fine."""\n'
    hand_rolled += '    return f"ON ST_Intersects(b.{g}, a.{g})"\n'
    assert st_intersects_literals(hand_rolled) == 1


def _literals_containing(source: str, needle: str) -> int:
    tree = ast.parse(source)
    skip = _docstring_ids(tree)
    return sum(
        1
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in skip
        and needle in node.value
    )


def test_admin_subquery_is_built_only_in_admin_datasets():
    """The admin side's boundary-column projection (``<expr> as _col_<i>``) and
    its reprojected bbox are spelled once, in ``admin_datasets.build_admin_subquery``
    (#942)."""
    found = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        rel = path.relative_to(PACKAGE).as_posix()
        if rel == "core/admin_datasets.py":
            continue
        text = path.read_text()
        count = _literals_containing(text, " as _col_") + _literals_containing(
            text, "struct_pack(xmin := ST_XMin("
        )
        if count:
            found[rel] = count
    assert found == {}


# --- one admin join: the query and the connection it runs on ----------------------


def _calls_named(source: str, name: str) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", getattr(node.func, "attr", None)) == name
    ]


def test_admin_join_query_is_built_only_in_admin_datasets():
    """``add admin-divisions`` and ``partition admin`` join the full geometry
    through ``admin_datasets.build_admin_join_query``; only the two joins with a
    different shape call the generic owner directly: aggregate admin joins a
    POINT under its own alias, add country-codes keeps every match."""
    found = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        rel = path.relative_to(PACKAGE).as_posix()
        if rel in (OWNER, "core/admin_datasets.py"):
            continue
        count = len(_calls_named(path.read_text(), "build_spatial_join_query"))
        if count:
            found[rel] = count
    assert found == {"core/add/country_codes.py": 1, "core/process/aggregate/by_admin.py": 1}


def test_admin_join_query_keeps_one_match_per_feature():
    sql = admin_datasets.build_admin_join_query(
        input_relation="read_parquet('in.parquet')",
        admin_relation="(SELECT * FROM admin)",
        admin_select_clause='b."country" AS country',
        input_geom_col="geometry",
        admin_geom_col="geometry",
        input_bbox_col="bbox",
        admin_bbox_col="bbox",
        boundary_columns=["country"],
    )
    assert "DISTINCT ON" in sql
    assert '"country" AS __gpio_match_0' in sql
    assert 'a."bbox".xmin <= b."bbox".xmax' in sql


def test_admin_join_connections_are_opened_only_in_admin_datasets():
    """The memory-disciplined admin-join connection (spill onto the admin cache,
    ``threads=1``, no insertion-order buffering) is opened in one place."""
    found = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        rel = path.relative_to(PACKAGE).as_posix()
        if rel in (OWNER, "core/admin_datasets.py"):
            continue
        text = path.read_text()
        count = sum(
            1
            for call in _calls_named(text, "get_duckdb_connection")
            if any(kw.arg == "threads" for kw in call.keywords)
        ) + _literals_containing(text, "preserve_insertion_order = false")
        if count and ("/admin" in rel or "admin_" in rel):
            found[rel] = count
    assert found == {}


def _setting(con, name):
    return du.fetch_one(con.execute(f"SELECT current_setting('{name}')"))[0]


def test_admin_join_connection_spills_single_threaded(tmp_path, monkeypatch):
    monkeypatch.setattr(admin_datasets, "get_cache_dir", lambda: tmp_path / "cache")
    con = admin_datasets.open_admin_join_connection(spill_to_cache=True)
    try:
        assert _setting(con, "threads") == 1
        assert _setting(con, "preserve_insertion_order") is False
        assert _setting(con, "geometry_always_xy") is True
        assert str(tmp_path / "cache") in _setting(con, "temp_directory")
    finally:
        con.close()


def test_admin_join_connection_default_keeps_duckdb_threads(tmp_path, monkeypatch):
    monkeypatch.setattr(admin_datasets, "get_cache_dir", lambda: tmp_path / "cache")
    con = admin_datasets.open_admin_join_connection(spill_to_cache=False)
    try:
        assert _setting(con, "preserve_insertion_order") is True
        assert _setting(con, "geometry_always_xy") is True
        assert str(tmp_path / "cache") not in _setting(con, "temp_directory")
        assert not (tmp_path / "cache").exists()
    finally:
        con.close()
