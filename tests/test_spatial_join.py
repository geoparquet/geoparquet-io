"""One spatial-join predicate and one dedup decision (#1367 concept 16).

``core/duckdb_utils.py`` owns the admin spatial join: the ON clause
(:func:`build_spatial_join_condition`) and the query around it
(:func:`build_spatial_join_query`), which keeps one match per input row: an
input feature that intersects two target polygons comes out once. The three
admin commands build their join only through it, so:

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
            match_order=['b."code"'],
        )
        assert "row_number() OVER ()" in sql  # streaming: no PARTITION BY
        assert "DISTINCT ON (__gpio_input_row)" in sql
        assert "PARTITION BY" not in sql and "QUALIFY" not in sql

    def test_there_is_no_all_matches_flag(self):
        """One match per input is the only join (#1367 T1d ruling): a
        one-value flag has no purpose."""
        with pytest.raises(TypeError, match="one_match_per_input"):
            du.build_spatial_join_query(
                input_relation="t_in",
                target_relation="t",
                target_select='b."code" AS "c"',
                condition="TRUE",
                match_order=['b."code"'],
                one_match_per_input=False,
            )

    def test_one_match_requires_a_match_order(self):
        with pytest.raises(ValueError, match="match_order"):
            du.build_spatial_join_query(
                input_relation="t_in",
                target_relation="t",
                target_select="b.x",
                condition="TRUE",
                match_order=(),
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
    "core/carto.py": 1,
    "core/extract.py": 2,
    "core/extract_bigquery.py": 2,
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
    """``add admin-divisions``, ``partition admin`` and ``process aggregate
    admin`` (a POINT join, under its own alias) all join through
    ``admin_datasets.build_admin_join_query``; nothing else calls the generic
    owner."""
    found = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        rel = path.relative_to(PACKAGE).as_posix()
        if rel in (OWNER, "core/admin_datasets.py"):
            continue
        count = len(_calls_named(path.read_text(), "build_spatial_join_query"))
        if count:
            found[rel] = count
    assert found == {}


def test_admin_join_query_keeps_one_match_per_feature():
    sql = admin_datasets.build_admin_join_query(
        input_relation="read_parquet('in.parquet')",
        admin_table_ref="admin",
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


# --- the admin side of the join lives in admin_datasets (#1367 T1d audit 1-7, 12) ---

ADMIN_CALLERS = (
    "core/add/admin_divisions.py",
    "core/partition/admin_hierarchical.py",
    "core/process/aggregate/by_admin.py",
)

#: What each admin caller used to spell for itself, and the owner that spells it now.
ADMIN_SIDE_LITERALS = {
    "MIN(ST_XMin(": "run_admin_join",
    "MIN({": "run_admin_join",
    ".xmin <= ": "run_admin_join",
    "_col_": "admin_column_ref",
    "read_parquet(": "AdminDataset.table_reference",
    "geometry_always_xy": "get_duckdb_connection",
}
ADMIN_SIDE_CALLS = {
    "transform_geom_sql": "admin_side_reprojected / build_admin_join_query",
    "build_admin_subquery": "build_admin_join_query",
    "build_spatial_join_condition": "build_admin_join_query",
    "open_bounded_connection": "open_admin_join_connection",
    "get_duckdb_connection": "open_admin_join_connection",
}


def admin_side_spellings(source: str) -> list[str]:
    found = [lit for lit in ADMIN_SIDE_LITERALS if _literals_containing(source, lit)]
    found += [name for name in ADMIN_SIDE_CALLS if _calls_named(source, name)]
    return found


def test_admin_callers_leave_the_admin_side_to_admin_datasets():
    """The extent pre-filter, the boundary-column naming, the admin relation,
    the reprojection decision and the connection are each decided once, in
    admin_datasets; the three admin commands only call it."""
    found = {}
    for rel in ADMIN_CALLERS:
        spelled = admin_side_spellings((PACKAGE / rel).read_text())
        if spelled:
            found[rel] = spelled
    assert found == {}


def test_admin_side_guard_sees_a_hand_rolled_extent_filter():
    source = 'def f(b, x):\n    return f"({b}.xmin <= {x})"\n'
    assert admin_side_spellings(source) == [".xmin <= "]


def _projected_points(tmp_path):
    """The four POINTS reprojected to EPSG:3857, with no bbox column."""
    src = _write_points(tmp_path / "pts_crs84.parquet")
    out = tmp_path / "pts_3857.parquet"
    _invoke(["convert", "reproject", src, str(out), "--dst-crs", "EPSG:3857"])
    assert "bbox" not in pq.read_schema(out).names
    return str(out)


def test_one_reprojection_rule_for_partition_and_add(tmp_path):
    """A non-CRS84 input with no bbox, against an admin dataset with one: the
    admin side is reprojected (it has a bbox to rebuild), never the input
    (#525). partition used to reproject the input here; both commands now
    build the join in run_admin_join."""
    steps = []
    con = du.get_duckdb_connection(load_spatial=True)
    try:
        admin_datasets.run_admin_join(
            con,
            admin_datasets.OvertureAdminDataset(source_path="admin.parquet"),
            ["country"],
            input_path=_projected_points(tmp_path),
            input_geom_col="geometry",
            input_bbox_col=None,
            admin_bbox_col="bbox",
            select_for=lambda levels, columns: 'b."country" AS country',
            final_step=steps.append,
            source_crs="EPSG:3857",
            dry_run=True,
            on_step=steps.append,
        )
    finally:
        con.close()
    (step,) = steps
    assert "ST_Transform(\"geometry\", 'OGC:CRS84', 'EPSG:3857')" in step.query
    assert 'ST_Transform(a."geometry"' not in step.query


def test_partition_admin_assigns_a_projected_input_without_bbox(tmp_path, local_admin):
    """The same assignments as the CRS84 input: the admin side is reprojected
    into EPSG:3857 (it has a bbox), and every feature lands where it did."""
    local_admin()
    crs84 = _partition_admin(tmp_path / "a", _write_points(tmp_path / "pts.parquet"))
    projected = _partition_admin(tmp_path / "b", _projected_points(tmp_path))
    assert projected == crs84
    assert crs84 == [(1, "AA"), (2, "AA"), (3, "BB")]


def test_aggregate_admin_opens_the_admin_join_connection(tmp_path, local_admin, monkeypatch):
    """aggregate's connection comes from the owner, in its materialize shape."""
    from geoparquet_io.core.process.aggregate import by_admin

    local_admin()
    calls = []
    real = admin_datasets.open_admin_join_connection

    def spy(**kwargs):
        calls.append(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(by_admin, "open_admin_join_connection", spy, raising=False)
    src = _write_points(tmp_path / "pts.parquet")
    _aggregate_admin(tmp_path, src)
    assert calls == [
        {"spill_to_cache": False, "materialize": True, "memory_limit": None, "verbose": False}
    ]


def test_admin_join_connection_materialize_carries_the_memory_limit():
    con = admin_datasets.open_admin_join_connection(
        spill_to_cache=False, materialize=True, memory_limit="512MB"
    )
    try:
        assert _setting(con, "memory_limit") in ("488.2 MiB", "512.0 MB", "488.2MiB")
        assert _setting(con, "preserve_insertion_order") is False
        assert _setting(con, "geometry_always_xy") is True
    finally:
        con.close()
    with pytest.raises(ValueError, match="spill_to_cache"):
        admin_datasets.open_admin_join_connection(spill_to_cache=True, materialize=True)


# --- one chain, one predicate, one resolver (#1367 T1d+T1e audit 2) -----------


@pytest.mark.parametrize(
    ("source_crs", "admin_bbox_col", "expected"),
    [
        (None, None, False),
        (None, "bbox", False),
        ("EPSG:3857", None, True),
        ("EPSG:3857", "bbox", False),
    ],
)
def test_admin_input_reprojected_is_the_other_side_of_the_rule(
    source_crs, admin_bbox_col, expected
):
    """A non-CRS84 input moves when the admin side cannot (no admin bbox, #525)."""
    assert admin_datasets.admin_input_reprojected(source_crs, admin_bbox_col) is expected
    if source_crs:
        assert expected is not admin_datasets.admin_side_reprojected(source_crs, admin_bbox_col)


#: The admin commands that chain one join per level (add, partition).
CHAIN_CALLERS = ("core/add/admin_divisions.py", "core/partition/admin_hierarchical.py")
CHAIN_CALLS = {
    "admin_side_reprojected": "admin_input_reprojected",
    "admin_input_extent": "run_admin_join",
    "build_admin_where_clause": "run_admin_join",
    "build_admin_join_query": "run_admin_join",
    "get_source_for_level": "run_admin_join",
    "get_or_cache_dataset": "resolve_admin_dataset",
    "_setup_admin_dataset": "resolve_admin_dataset",
}


def chain_spellings(source: str) -> list[str]:
    found = [name for name in CHAIN_CALLS if _calls_named(source, name)]
    return found + (["_step_"] if _literals_containing(source, "_step_") else [])


def test_add_and_partition_leave_the_join_chain_to_admin_datasets():
    """Reading each level's source, its WHERE (extent read once), the join, the
    chained temp tables and the table-vs-path rule are run_admin_join's."""
    found = {}
    for rel in CHAIN_CALLERS:
        spelled = chain_spellings((PACKAGE / rel).read_text())
        if spelled:
            found[rel] = spelled
    assert found == {}


def test_chain_guard_sees_a_second_chain():
    source = 'def f(con, i, q):\n    con.execute(f"CREATE TEMP TABLE _x_step_{i} AS {q}")\n'
    assert chain_spellings(source) == ["_step_"]


def test_no_admin_module_imports_the_partition_dataset_setup():
    """aggregate admin and overview rollup took partition's private setup."""
    for rel in (*ADMIN_CALLERS, "core/process/overview/rollup.py"):
        assert "_setup_admin_dataset" not in (PACKAGE / rel).read_text(), rel


def _two_level_admin(tmp_path, monkeypatch, *, per_level=True):
    """An Overture-shaped local file with country and region rows."""
    admin_file = _write_admin(tmp_path / "admin.parquet")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(
        f"COPY (SELECT * FROM {sql_path(admin_file)} UNION ALL "
        f"SELECT 'region', country, country || '-1', geometry, bbox FROM {sql_path(admin_file)}) "
        f"TO {sql_path(str(tmp_path / 'admin2.parquet'))} (FORMAT PARQUET)"
    )
    con.close()
    path = str(tmp_path / "admin2.parquet")

    class Local(admin_datasets.OvertureAdminDataset):
        def supports_per_level_sources(self):
            return per_level

    monkeypatch.setattr(
        admin_datasets.AdminDatasetFactory,
        "create",
        classmethod(
            lambda cls, dataset_name, source_path=None, verbose=False: Local(
                source_path=path, verbose=verbose
            )
        ),
    )


def _add_two_levels(tmp_path, input_file, *extra):
    return CliRunner().invoke(
        cli,
        [
            "add",
            "admin-divisions",
            input_file,
            str(tmp_path / "out2.parquet"),
            "--dataset",
            "overture",
            "--levels",
            "country,region",
            *extra,
        ],
    )


def test_per_level_dry_run_prints_every_level(tmp_path, monkeypatch):
    """The second level's WHERE read its extent off the step-1 temp table, which
    a dry run never creates: the dry run died on a Catalog Error."""
    _two_level_admin(tmp_path, monkeypatch)
    result = _add_two_levels(tmp_path, _write_points(tmp_path / "in.parquet"), "--dry-run")
    assert result.exit_code == 0, result.output
    assert "-- Step 2: Add" in result.output
    assert "FROM _gpio_admin_step_0" in result.output


@pytest.mark.parametrize("per_level", [True, False], ids=["per-level", "single-source"])
def test_the_input_extent_is_read_once(tmp_path, monkeypatch, per_level):
    _two_level_admin(tmp_path, monkeypatch, per_level=per_level)
    real = admin_datasets._admin_input_extent
    relations = []

    def spy(con, input_relation, *args, **kwargs):
        relations.append(input_relation)
        return real(con, input_relation, *args, **kwargs)

    monkeypatch.setattr(admin_datasets, "_admin_input_extent", spy)
    src = _write_points(tmp_path / "in.parquet")
    result = _add_two_levels(tmp_path, src)
    assert result.exit_code == 0, result.output
    assert relations == [sql_path(src)]
    rows = pq.read_table(tmp_path / "out2.parquet").to_pylist()
    assert [(r["id"], r["overture_country"], r["overture_region"]) for r in rows] == [
        (1, "AA", "1"),  # the region code loses its country prefix
        (2, "AA", "1"),
        (3, "BB", "1"),
        (4, None, None),
    ]


def test_no_extent_is_read_without_an_admin_bbox(tmp_path, monkeypatch):
    """The extent only feeds the admin bbox filter, so without one it is not read."""
    calls = []
    monkeypatch.setattr(
        admin_datasets, "_admin_input_extent", lambda *a, **k: calls.append(a) or (0, 1, 0, 1)
    )
    con = du.get_duckdb_connection(load_spatial=True)
    try:
        dataset = admin_datasets.OvertureAdminDataset(source_path="admin.parquet")
        admin_datasets.run_admin_join(
            con,
            dataset,
            ["country"],
            input_path="in.parquet",
            input_geom_col="geometry",
            input_bbox_col=None,
            admin_bbox_col=None,
            select_for=lambda levels, columns: 'b."country" AS c',
            final_step=lambda step: None,
            dry_run=True,
        )
    finally:
        con.close()
    assert calls == []


def test_one_admin_dataset_resolver_caches_only_for_add(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        admin_datasets,
        "get_or_cache_dataset",
        lambda dataset, no_cache=False, verbose=False: calls.append(no_cache) or "cached.parquet",
    )
    _, columns, source = admin_datasets.resolve_admin_dataset(
        "gaul", ["continent"], cache=True, no_cache=True
    )
    assert (source, calls) == ("cached.parquet", [True])
    _, columns_again, source = admin_datasets.resolve_admin_dataset(
        "gaul", ["continent"], cache=False
    )
    assert (source, calls, columns_again) == (None, [True], columns)
    dataset, _, source = admin_datasets.resolve_admin_dataset("overture", ["country"], cache=True)
    assert (source, calls) == (None, [True])  # per-level: each level reads its own source


def test_admin_join_connection_refuses_a_memory_limit_it_would_not_apply():
    """Only the materialize shape carries the limit itself; the others run
    under scoped_write_memory_limit, so a limit passed to them was dropped."""
    for spill in (True, False):
        with pytest.raises(ValueError, match="memory_limit"):
            admin_datasets.open_admin_join_connection(spill_to_cache=spill, memory_limit="1GB")


def test_a_single_source_dataset_refuses_a_per_level_source():
    dataset = admin_datasets.AdminDatasetFactory.create("gaul")
    assert not dataset.supports_per_level_sources()
    with pytest.raises(NotImplementedError, match="no per-level sources"):
        dataset.get_source_for_level("country")


# --- one strategy report, one source rule, one materialize (#1367 T1d+T1e audit 3)


def _spy_admin_join_queries(monkeypatch) -> list[str]:
    queries: list[str] = []
    real = admin_datasets.build_admin_join_query

    def spy(**kwargs):
        query = real(**kwargs)
        queries.append(query)
        return query

    monkeypatch.setattr(admin_datasets, "build_admin_join_query", spy)
    return queries


@pytest.mark.parametrize("input_rewritten", [False, True], ids=["as-stored", "rewritten"])
def test_partition_reports_the_join_strategy_its_sql_runs(
    tmp_path, local_admin, monkeypatch, input_rewritten
):
    """partition admin's verbose strategy line reads the owner's rule
    (``spatial_join_strategy`` over ``admin_input_reprojected``), not the two
    bbox columns: when the rule rewrites the input geometry the bbox
    pre-filter leaves the SQL, and the line has to say so."""
    from geoparquet_io.core.partition import admin_hierarchical

    local_admin()
    if input_rewritten:
        for module in (admin_datasets, admin_hierarchical):
            monkeypatch.setattr(
                module, "admin_input_reprojected", lambda crs, bbox: True, raising=False
            )
    queries = _spy_admin_join_queries(monkeypatch)
    src = _write_points(tmp_path / "pts.parquet", bbox_name="bbox")
    result = _invoke(
        [
            "partition",
            "admin",
            src,
            str(tmp_path / "parts"),
            "--dataset",
            "overture",
            "--levels",
            "country",
            "--hive",
            "--verbose",
        ]
    )
    (query,) = queries
    prefilter = 'a."bbox".xmin <= b."bbox".xmax' in query
    assert prefilter is not input_rewritten
    said_bbox = "Using bbox columns for optimized spatial join" in result.output
    said_full = "Using full geometry intersection" in result.output
    assert (said_bbox, said_full) == (prefilter, not prefilter), result.output


def test_one_rule_for_the_source_a_join_step_reads(monkeypatch):
    """A per-level dataset reads each level's own source; any other reads its
    own (``None``: :meth:`AdminDataset.table_reference` picks it)."""
    gaul = admin_datasets.AdminDatasetFactory.create("gaul")
    assert gaul.source_for_level("continent", no_cache=True) is None
    monkeypatch.setattr(
        admin_datasets.OvertureAdminDataset,
        "get_source_for_level",
        lambda self, level, no_cache=False: f"{level}:{no_cache}",
    )
    overture = admin_datasets.OvertureAdminDataset(source_path="admin.parquet")
    assert overture.source_for_level("country", no_cache=True) == "country:True"
    assert overture.source_for_level("region") == "region:False"


def _functions_calling(path: Path, name: str) -> list[str]:
    """``module:function`` for every function in ``path`` that calls ``name``."""
    found = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and _calls_named(
            ast.unparse(node), name
        ):
            found.append(f"{path.relative_to(PACKAGE)}:{node.name}")
    return found


def test_only_source_for_level_picks_a_per_level_source():
    """``get_source_for_level if supports_per_level_sources() else <own>`` was
    decided in run_admin_join and again in aggregate's ``_get_admin_ref``.
    overview rollup reads Overture's country cache by name: it asks a
    per-level dataset for one level and decides nothing."""
    callers = []
    for path in sorted(PACKAGE.rglob("*.py")):
        callers += _functions_calling(path, "get_source_for_level")
    assert callers == [
        "core/admin_datasets.py:source_for_level",
        "core/process/overview/rollup.py:get_admin_country_context",
    ]


def test_run_admin_join_fills_the_final_table(tmp_path):
    con = du.get_duckdb_connection(load_spatial=True)
    try:
        admin_datasets.run_admin_join(
            con,
            admin_datasets.OvertureAdminDataset(
                source_path=_write_admin(tmp_path / "admin.parquet")
            ),
            ["country"],
            input_path=_write_points(tmp_path / "pts.parquet"),
            input_geom_col="geometry",
            input_bbox_col=None,
            admin_bbox_col="bbox",
            select_for=lambda levels, columns: (
                f"{admin_datasets.admin_column_ref(0, columns[0])} AS c"
            ),
            final_table="_enriched",
        )
        rows = con.execute("SELECT id, c FROM _enriched ORDER BY id").fetchall()
    finally:
        con.close()
    assert rows == [(1, "AA"), (2, "AA"), (3, "BB"), (4, None)]


@pytest.mark.parametrize("sinks", [{}, {"final_step": print, "final_table": "_t"}])
def test_run_admin_join_takes_exactly_one_sink(sinks):
    with pytest.raises(ValueError, match="final_step or final_table"):
        admin_datasets.run_admin_join(
            None,
            admin_datasets.OvertureAdminDataset(source_path="admin.parquet"),
            ["country"],
            input_path="in.parquet",
            input_geom_col="geometry",
            input_bbox_col=None,
            admin_bbox_col=None,
            select_for=lambda levels, columns: "",
            **sinks,
        )


def test_partition_leaves_materializing_the_join_to_run_admin_join():
    """The last step's ``CREATE TEMP TABLE`` under ``scoped_write_memory_limit``
    is the driver's own statement; partition passes the table's name."""
    source = (PACKAGE / "core/partition/admin_hierarchical.py").read_text()
    assert _literals_containing(source, "TEMP TABLE") == 0
    assert _calls_named(source, "scoped_write_memory_limit") == []
