"""One row-group policy: every write path lands on the facade's count (#1367 T2c).

``parquet_writer.resolve_row_group_rows`` is the only place a row-group count is
decided: an explicit row count (snapped to a whole writer vector), else an MB
target measured from the data (``resolve_row_group_rows_for_table`` /
``resolve_row_group_rows_for_query``), else ``DEFAULT_ROW_GROUP_ROWS``. The
table below runs one fixture through every command that writes a file, with
each option and two table sizes, and asserts the group count each output has
is the count the owner computes from the input -- so a path that re-derives a
number, or leaves it to its writer, shows up as the one cell that disagrees.

The commands that take no row-group option (``process aggregate``,
``process overview``, ``add bbox-metadata``'s fallback rewrite) are held to the
default. Only the network (the admin boundary download) is replaced, by a local
boundary file; ``add bbox-metadata`` is made to take its fallback rewrite the
way its own tests do, by refusing the footer patch.

The guard at the bottom keeps the policy in one place: no ``49152`` literal, no
``row_group_size=`` / ``ROW_GROUP_SIZE`` with a literal count, no row-group
constant and no ``rows or <fallback>`` second default outside the owner.
"""

from __future__ import annotations

import ast
import json
import math
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.parquet_writer import (
    DEFAULT_ROW_GROUP_ROWS,
    WRITER_VECTOR_ROWS,
    resolve_row_group_rows,
    resolve_row_group_rows_for_rewrite,
    resolve_row_group_rows_for_table,
)
from tests._defrag_fixtures import geo_block, grid_points_table

REPO = Path(__file__).resolve().parents[1]
PACKAGE = REPO / "geoparquet_io"
OWNER = PACKAGE / "core" / "parquet_writer.py"

SIZES = (25_000, 60_000)
#: option name -> CLI flags. 4.3 MB over ~380-byte rows is ~11,300 rows: the
#: middle of the 10,240-12,288 vector step, so a DuckDB sample and a pyarrow
#: table that weigh a few bytes apart still land on one count.
OPTIONS = {
    "default": (),
    "rows": ("--row-group-size", "10000"),
    "mb": ("--row-group-size-mb", "4.3"),
}
#: Paths that take --row-group-size / --row-group-size-mb, and those that take neither.
SIZED_PATHS = (
    "convert",
    "sort_hilbert",
    "extract",
    "partition_admin",
    *(f"table_write_{s}" for s in ("duckdb-kv", "in-memory", "streaming", "disk-rewrite")),
)
DEFAULT_ONLY_PATHS = ("aggregate_h3_none", "aggregate_h3_polygon", "overview", "add_bbox_metadata")


def _run(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, f"gpio {' '.join(map(str, args))}: {result.output[-1500:]}"


def _group_sizes(path: Path) -> list[int]:
    md = pq.ParquetFile(str(path)).metadata
    return [md.row_group(i).num_rows for i in range(md.num_row_groups)]


@pytest.fixture(scope="module", params=SIZES, ids=lambda n: f"{n}rows")
def source(request, tmp_path_factory) -> Path:
    """``n`` distinct points (one per H3 r15 cell) with a fixed-width pad column,
    written in 4,096-row groups so no policy matches the input's own layout.

    The input's groups are whole writer vectors on purpose. DuckDB's ``COPY``
    closes a row group on the first chunk that reaches ``ROW_GROUP_SIZE``, and a
    5,000-row input group reads back as 2,048 + 2,048 + 904, so a 12,288-row
    request over it lands at 14,096: the writer's chunking, not a policy.
    """
    n = request.param
    table = grid_points_table(n, with_bbox=False)
    table = table.append_column("pad", pa.array([f"{i:08d}" * 40 for i in range(n)]))
    path = tmp_path_factory.mktemp(f"rg_{n}") / "source.parquet"
    pq.write_table(
        table.replace_schema_metadata({b"geo": json.dumps(geo_block()).encode()}),
        str(path),
        row_group_size=4_096,
    )
    return path


def _expected(source: Path, option: str) -> int:
    """The group count the owner computes for ``source`` under ``option``."""
    table = pq.read_table(str(source))
    if option == "mb":
        rows = resolve_row_group_rows_for_table(table, None, float(OPTIONS["mb"][1]))
    elif option == "rows":
        rows = resolve_row_group_rows(int(OPTIONS["rows"][1]), None)
    else:
        rows = resolve_row_group_rows(None, None)
    return math.ceil(table.num_rows / rows)


def _use_local_admin(tmp_path: Path, monkeypatch) -> None:
    """One Overture-shaped country polygon over the fixture, instead of the download."""
    from geoparquet_io.core.admin_datasets import OvertureAdminDataset
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

    admin_file = tmp_path / "admin.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"""
            COPY (
              SELECT 'country' AS subtype, 'AA' AS country, NULL::VARCHAR AS region, g AS geometry,
                {{'xmin': ST_XMin(g), 'xmax': ST_XMax(g),
                  'ymin': ST_YMin(g), 'ymax': ST_YMax(g)}} AS bbox
              FROM (SELECT ST_GeomFromText('POLYGON((-11 -1, 11 -1, 11 21, -11 21, -11 -1))') AS g)
            ) TO {sql_path(str(admin_file))} (FORMAT PARQUET)
        """)
    finally:
        con.close()

    def create(*_args, verbose=False, **_kwargs):
        return OvertureAdminDataset(source_path=str(admin_file), verbose=verbose)

    monkeypatch.setattr(
        "geoparquet_io.core.partition.admin_hierarchical.AdminDatasetFactory.create",
        staticmethod(create),
    )


def _write(path_name: str, src: Path, out_dir: Path, flags: tuple[str, ...]) -> Path:
    out = out_dir / "out.parquet"
    if path_name.startswith("table_write_"):
        kwargs = {}
        if flags and flags[0] == "--row-group-size":
            kwargs["row_group_rows"] = int(flags[1])
        elif flags:
            kwargs["row_group_size_mb"] = float(flags[1])
        strategy = path_name.removeprefix("table_write_")
        gpio.read(str(src)).write(str(out), write_strategy=strategy, **kwargs)
    elif path_name == "convert":
        _run("convert", "geoparquet", src, out, *flags)
    elif path_name == "sort_hilbert":
        _run("sort", "hilbert", src, out, *flags)
    elif path_name == "extract":
        _run("extract", src, out, *flags)
    elif path_name == "partition_admin":
        part = out_dir / "partitions"
        _run(
            "partition", "admin", src, part, "--dataset", "overture", "--levels", "country", *flags
        )
        (out,) = sorted(part.rglob("*.parquet"))
    return out


def _assert_owners_count(path_name: str, option: str, source: Path, tmp_path: Path) -> None:
    out = _write(path_name, source, tmp_path, OPTIONS[option])
    sizes = _group_sizes(out)
    assert sum(sizes) == pq.read_metadata(str(source)).num_rows
    assert len(sizes) == _expected(source, option), f"{path_name} {option}: {sizes}"


@pytest.mark.parametrize("option", sorted(OPTIONS))
@pytest.mark.parametrize("path_name", [p for p in SIZED_PATHS if p != "partition_admin"])
def test_every_sized_path_lands_on_the_owners_count(path_name, option, source, tmp_path):
    _assert_owners_count(path_name, option, source, tmp_path)


@pytest.mark.parametrize("option", sorted(OPTIONS))
def test_partition_admin_lands_on_the_owners_count(option, source, tmp_path, monkeypatch):
    _use_local_admin(tmp_path, monkeypatch)
    _assert_owners_count("partition_admin", option, source, tmp_path)


@pytest.mark.parametrize("path_name", DEFAULT_ONLY_PATHS)
def test_every_unsized_path_lands_on_the_default(path_name, source, tmp_path, monkeypatch):
    n = pq.read_metadata(str(source)).num_rows
    if path_name.startswith("aggregate_h3_"):
        geometry = path_name.removeprefix("aggregate_h3_")
        out = tmp_path / "cells.parquet"
        _run(
            "process",
            "aggregate",
            "h3",
            source,
            out,
            "--resolution",
            "15",
            "--out-geometry",
            geometry,
        )
    elif path_name == "overview":
        cells = tmp_path / "cells.parquet"
        _run(
            "process",
            "aggregate",
            "h3",
            source,
            cells,
            "--resolution",
            "15",
            "--out-geometry",
            "none",
        )
        levels = tmp_path / "levels"
        levels.mkdir()
        _run("process", "overview", cells, "--levels", "14", "--output-dir", levels)
        (out,) = sorted(levels.glob("*.parquet"))
    else:
        from geoparquet_io.core.add import bbox_metadata
        from geoparquet_io.core.parquet_footer import FooterPatchUnsupported

        def refuse(*args, **kwargs):
            raise FooterPatchUnsupported("planted: take the fallback rewrite")

        monkeypatch.setattr(bbox_metadata, "patch_footer_kv", refuse)
        # The same rows and layout with an undeclared `bbox` struct to declare.
        # Not made by `gpio add bbox`, whose own 49,152-row groups would hide
        # the input-average rule this rewrite used to apply.
        out = tmp_path / "out.parquet"
        pq.write_table(
            grid_points_table(n).replace_schema_metadata(
                {b"geo": json.dumps(geo_block()).encode()}
            ),
            str(out),
            row_group_size=pq.read_metadata(str(source)).row_group(0).num_rows,
        )
        _run("add", "bbox-metadata", out)
    sizes = _group_sizes(out)
    assert sum(sizes) == n, f"{path_name}: the fixture lost its one-row-per-cell shape"
    assert len(sizes) == math.ceil(n / DEFAULT_ROW_GROUP_ROWS), f"{path_name}: {sizes}"


def test_the_fixture_makes_the_options_distinguishable(source):
    """Non-vacuity: on the big size, the three options give three different counts."""
    n = pq.read_metadata(str(source)).num_rows
    if n < max(SIZES):
        pytest.skip("checked on the larger size")
    counts = {option: _expected(source, option) for option in OPTIONS}
    assert len(set(counts.values())) == len(counts), counts
    assert pq.read_metadata(str(source)).num_row_groups not in counts.values()


# ---------------------------------------------------------------------------
# The rewrite rule (#1098)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("num_rows", "groups", "expected"),
    [
        (60_000, 1, DEFAULT_ROW_GROUP_ROWS),  # nothing to keep
        (200_000, 2, DEFAULT_ROW_GROUP_ROWS),  # the default already keeps 2
        (60_000, 12, 4_096),  # 5,000 per group -> the largest whole vector below
        (60_000, 2, DEFAULT_ROW_GROUP_ROWS),  # ceil(60,000 / 49,152) == 2
        (10_000, 50, WRITER_VECTOR_ROWS),  # 200 per group: floored at one vector
        (0, 0, DEFAULT_ROW_GROUP_ROWS),
    ],
)
def test_a_rewrite_keeps_at_least_the_input_row_groups(num_rows, groups, expected):
    rows = resolve_row_group_rows_for_rewrite(num_rows, groups)
    assert rows == expected
    if rows > WRITER_VECTOR_ROWS and groups > 1:
        assert math.ceil(num_rows / rows) >= groups


# ---------------------------------------------------------------------------
# The guard: no second row-group policy outside the owner
# ---------------------------------------------------------------------------

#: Sites that may name a literal row count, each with its reason.
JUSTIFIED = {
    ("geoparquet_io/core/benchmark.py", "ROW_GROUP_SIZE 100000"): (
        "the benchmark's fixed DuckDB baseline layout; it measures DuckDB, not a gpio output"
    ),
    ("geoparquet_io/core/benchmark.py", "ROW_GROUP_SIZE=100000"): (
        "the same baseline, named in the benchmark's report"
    ),
}

_ROW_GROUP_NAME = re.compile(r"(?i)row_?group|batch_size")
_COPY_LITERAL = re.compile(r"(?i)ROW_GROUP_SIZE[\s=]+\d+")


def _default_literal(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Constant)
        and type(node.value) is int
        and node.value in (DEFAULT_ROW_GROUP_ROWS, 50_000, 100_000, 122_880)
    )


def _fallback_count(node: ast.AST) -> bool:
    """An int literal or a CONSTANT name: what ``rows or <this>`` falls back to."""
    if isinstance(node, ast.Constant):
        return type(node.value) is int
    name = node.id if isinstance(node, ast.Name) else getattr(node, "attr", "")
    return bool(name) and name.isupper()


def _names_row_groups(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return bool(_ROW_GROUP_NAME.search(node.id))
    if isinstance(node, ast.Attribute):
        return bool(_ROW_GROUP_NAME.search(node.attr))
    if isinstance(node, ast.Call):
        return _names_row_groups(node.func)
    return False


def row_group_policy_hits(path: Path, rel: str) -> list[str]:
    """Every place ``path`` decides a row-group count for itself."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and type(node.value) is int:
            if node.value == DEFAULT_ROW_GROUP_ROWS:
                hits.append(f"{rel}:{node.lineno}: the default typed as a literal")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            for match in _COPY_LITERAL.findall(node.value):
                if (rel, match) not in JUSTIFIED:
                    hits.append(f"{rel}:{node.lineno}: COPY {match!r}")
        if isinstance(node, ast.keyword) and node.arg == "row_group_size":
            if isinstance(node.value, ast.Constant):
                hits.append(f"{rel}:{node.lineno}: row_group_size={node.value.value!r}")
        if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
            first, *rest = node.values
            if _names_row_groups(first) and any(_fallback_count(r) for r in rest):
                hits.append(f"{rel}:{node.lineno}: a second default after `or`")
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(_names_row_groups(t) for t in targets) and _default_literal(node.value):
                hits.append(f"{rel}:{node.lineno}: a row-group constant")
    return hits


def _package_hits(root: Path) -> list[str]:
    hits = []
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root.parent).as_posix()
        if path.resolve() == OWNER.resolve() or rel.endswith("core/parquet_writer.py"):
            continue
        hits.extend(row_group_policy_hits(path, rel))
    return hits


def test_no_row_group_policy_outside_the_owner():
    assert _package_hits(PACKAGE) == []


def test_every_justified_site_still_exists():
    for rel, text in JUSTIFIED:
        assert text in (REPO / rel).read_text(encoding="utf-8"), f"stale: {rel} {text!r}"


@pytest.mark.parametrize(
    "source",
    [
        "x = 49152\n",
        "pq.write_table(t, p, row_group_size=100_000)\n",
        "writer.write_table(t, row_group_size=10000)\n",
        "con.execute('COPY t TO f (ROW_GROUP_SIZE 122880)')\n",
        "batch_size = row_group_rows or DEFAULT_BATCH_SIZE\n",
        "rows = self.row_group_rows or 50000\n",
        "rows = resolve_row_group_rows(None, None) or DEFAULT_ROW_GROUP_ROWS\n",
        "DEFAULT_BATCH_SIZE = 100_000\n",
        "ROW_GROUP_ROWS: int = 50_000\n",
    ],
)
def test_the_guard_catches_a_second_policy(tmp_path, source):
    pkg = tmp_path / "geoparquet_io" / "core"
    pkg.mkdir(parents=True)
    (pkg / "planted.py").write_text(source)
    assert _package_hits(tmp_path / "geoparquet_io"), source


@pytest.mark.parametrize(
    "source",
    [
        "rows = resolve_row_group_rows(row_group_rows, row_group_size_mb)\n",
        "writer.write_table(t, row_group_size=rows_per_group)\n",
        "options.append(f'ROW_GROUP_SIZE {row_group_rows}')\n",
        "_LINEARIZE_BATCH_ROWS = 20_000\n",
        "count = len(groups) or 1\n",
        "chunks = get_native_geo_stats_by_row_group(path, col) or []\n",
        "if row_group_rows or row_group_size_mb:\n    pass\n",
    ],
)
def test_the_guard_leaves_owner_answers_alone(tmp_path, source):
    pkg = tmp_path / "geoparquet_io" / "core"
    pkg.mkdir(parents=True)
    (pkg / "planted.py").write_text(source)
    assert _package_hits(tmp_path / "geoparquet_io") == [], source


def test_the_owner_itself_is_exempt(tmp_path):
    pkg = tmp_path / "geoparquet_io" / "core"
    pkg.mkdir(parents=True)
    (pkg / "parquet_writer.py").write_text("DEFAULT_ROW_GROUP_ROWS = 49152\n")
    assert _package_hits(tmp_path / "geoparquet_io") == []
