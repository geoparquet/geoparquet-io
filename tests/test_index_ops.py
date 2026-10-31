"""Ownership guards and shared spatial-index driver contracts."""

import ast
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.api import ops
from geoparquet_io.cli.main import cli
from geoparquet_io.core.geometry_detection import primary_geometry_column
from geoparquet_io.core.index_ops import (
    calculate_index_resolution,
    clamp_resolution,
    ensure_index_column,
    get_index_spec,
)
from tests.test_spatial_index_golden import A5_BUILD_CELLS, GOLDEN, INDEX_SPECS, golden_fixture

#: The names only an index streaming copy calls. ``computed_column_query`` is not
#: one of them: it is the general ``duckdb_utils`` composer every computed-column
#: write uses, so keying on it flagged any ``make_query`` that adds a column.
INDEX_ONLY_NAMES = frozenset(
    {"index_expression", "load_index"}
    | {f"_make_add_{name}_query" for name in ("h3", "a5", "s2", "quadkey")}
)


def _index_workflow_copies(path: Path, source: str) -> list[str]:
    copies = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.FunctionDef):
            continue
        ensure_copy = node.name.startswith("_ensure_") and node.name.endswith("_column")
        called = {
            child.id if isinstance(child, ast.Name) else child.attr
            for child in ast.walk(node)
            if isinstance(child, (ast.Name, ast.Attribute))
        }
        streaming_copy = node.name == "make_query" and bool(called & INDEX_ONLY_NAMES)
        if ensure_copy or streaming_copy:
            copies.append(f"{path}:{node.name}")
    return copies


def test_index_workflows_have_one_owner():
    root = Path(__file__).parents[1] / "geoparquet_io"
    copies = []
    for path in root.rglob("*.py"):
        if path.name == "index_ops.py":
            continue
        copies += _index_workflow_copies(path.relative_to(root), path.read_text(encoding="utf-8"))
    assert copies == [], f"{len(copies)} index workflow copies: {copies}"


@pytest.mark.parametrize(
    "body",
    [
        "    return computed_column_query(src, 'h3', index_expression(spec, 'g', 9))\n",
        "    load_index(con, spec)\n    return q\n",
        "    return index_ops.index_expression(spec, 'g', 9)\n",
        "    return _make_add_h3_query(src)\n",
    ],
)
def test_index_workflow_guard_catches_a_streaming_copy(body):
    source = f"def make_query(src, con, crs, md):\n{body}"
    assert _index_workflow_copies(Path("plant.py"), source) == ["plant.py:make_query"]


@pytest.mark.parametrize(
    "source",
    [
        # A general computed-column streaming add is not an index copy (S1).
        "def make_query(src, con, crs, md):\n"
        "    return computed_column_query(src, 'area', 'ST_Area(geometry)')\n",
        # The index names outside a make_query are a caller, not a copy.
        "def run(spec):\n    return index_expression(spec, 'g', 9)\n",
    ],
)
def test_index_workflow_guard_leaves_near_misses_alone(source):
    assert _index_workflow_copies(Path("plant.py"), source) == []


INDICES = ("h3", "a5", "s2", "quadkey")
# Observed on the unchanged base with DuckDB 1.5.6's installed A5 extension.
# #1361: A5 IDs vary with extension builds; never remap them in the driver.


@pytest.mark.parametrize("index", INDICES)
def test_spec_fields(index):
    spec = get_index_spec(index)
    assert spec.name == index
    assert spec.column_name == INDEX_SPECS[index]["column"]
    assert spec.min_resolution == 0
    assert spec.max_resolution == {"h3": 15, "a5": 30, "s2": 30, "quadkey": 23}[index]
    assert spec.base_cells == {"h3": 122, "a5": 6, "s2": 6, "quadkey": 1}[index]
    assert spec.subdivision == (7 if index == "h3" else 4)
    assert spec.extension == {"h3": "h3", "a5": "a5", "s2": "geography", "quadkey": None}[index]
    assert callable(spec.expression) and callable(spec.size)


@pytest.mark.parametrize("index", INDICES)
def test_public_api_keeps_base_cells(index):
    resolution = INDEX_SPECS[index]["resolution"]
    kwargs = {"level" if index == "s2" else "resolution": resolution}
    result = getattr(ops, f"add_{index}")(golden_fixture(), **kwargs)
    actual = result[get_index_spec(index).column_name].to_pylist()
    expected = GOLDEN[index][resolution]
    if index == "a5":
        assert actual in (expected, A5_BUILD_CELLS), "Unrecognized A5 extension build (#1361)"
    else:
        assert actual == expected
    assert result["name"].to_pylist() == golden_fixture()["name"].to_pylist()


@pytest.mark.parametrize("index", INDICES)
@pytest.mark.parametrize("existing", ("same", "case", "absent"))
def test_ensure_index_column_idempotence(index, existing, tmp_path):
    spec = get_index_spec(index)
    table = golden_fixture()
    resolution = INDEX_SPECS[index]["resolution"]
    kwargs = {"level" if index == "s2" else "resolution": resolution}
    indexed = getattr(ops, f"add_{index}")(table, **kwargs)
    expected = indexed[spec.column_name].to_pylist()
    column = spec.column_name.upper() if existing == "case" else spec.column_name
    if existing != "absent":
        table = indexed.rename_columns(
            [column if c == spec.column_name else c for c in indexed.column_names]
        )
    path = tmp_path / "input.parquet"
    pq.write_table(table, path)
    working, resolved, temporary = ensure_index_column(
        spec, str(path), spec.column_name, resolution
    )
    try:
        assert resolved == column
        assert pq.read_table(working)[resolved].to_pylist() == expected
        if existing == "absent":
            assert temporary == working and working != str(path)
        else:
            assert (working, temporary) == (str(path), None)
        again = ensure_index_column(spec, working, resolved, resolution)
        assert again == (working, resolved, None)
    finally:
        if temporary:
            Path(temporary).unlink()


@pytest.mark.parametrize("index", INDICES)
def test_resolution_bounds(index):
    spec = get_index_spec(index)
    assert clamp_resolution(-100, spec.min_resolution, spec.max_resolution) == spec.min_resolution
    assert clamp_resolution(100, spec.min_resolution, spec.max_resolution) == spec.max_resolution
    assert (
        calculate_index_resolution(spec, 10**100, 1, max_partitions=10**100) == spec.max_resolution
    )
    assert calculate_index_resolution(spec, 1, 100000) == spec.min_resolution
    assert calculate_index_resolution(spec, 0, 1, min_resolution=3) == 3


def test_standard_geometry_name_preserves_schema_spelling():
    assert primary_geometry_column(["ID", "GeOmEtRy"]) == "GeOmEtRy"


@pytest.mark.parametrize("index", INDICES)
def test_partition_reuses_case_different_index(index, tmp_path):
    spec = get_index_spec(index)
    column = spec.column_name.upper()
    cells = ["0", "1"] if index == "quadkey" else ["left", "right"]
    table = golden_fixture().slice(0, 2).append_column(column, pa.array(cells))
    path = tmp_path / "input.parquet"
    pq.write_table(table, path)
    output = tmp_path / "parts"
    flags = ["--level" if index == "s2" else "--resolution", "1"]
    if index == "quadkey":
        flags += ["--partition-resolution", "1"]
    result = CliRunner().invoke(
        cli, ["partition", index, str(path), str(output), *flags, "--force"]
    )
    assert result.exit_code == 0, result.output
    assert {p.stem for p in output.glob("*.parquet")} == set(cells)
    assert sum(pq.read_metadata(p).num_rows for p in output.glob("*.parquet")) == 2


@pytest.mark.parametrize("group_name", ["add", "partition"])
@pytest.mark.parametrize("index_name", INDICES)
def test_cli_resolution_bounds_belong_to_index(group_name, index_name):
    """Walk the Click tree and pin each option to its own registry entry."""
    import inspect

    import click

    group = cli.commands[group_name]
    command = group.commands[index_name]
    spec = get_index_spec(index_name)
    ranges = [param for param in command.params if isinstance(param.type, click.IntRange)]
    resolution_ranges = [
        param for param in ranges if param.name in {"resolution", "level", "partition_resolution"}
    ]
    assert resolution_ranges
    for param in resolution_ranges:
        assert (param.type.min, param.type.max) == (spec.min_resolution, spec.max_resolution)
    # A5 and S2 currently share bounds: also catch a lookup of the wrong spec.
    source = inspect.getsource(command.callback)
    lookups = [
        node.args[0].value
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "get_index_spec"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    ]
    assert lookups and set(lookups) == {index_name}


@pytest.mark.parametrize("index_name", INDICES)
def test_sub_partition_conflict_uses_api_argument_names(tmp_path, index_name):
    from geoparquet_io.core.exceptions import InvalidParameterError

    spec = get_index_spec(index_name)
    with pytest.raises(InvalidParameterError) as caught:
        getattr(ops, f"sub_partition_by_{index_name}")(
            tmp_path, min_size="1B", auto=True, **{spec.unit: 6}
        )
    assert "auto=True" in caught.value.reason
    assert f"{spec.unit}=<int>" in caught.value.reason
    assert "--" not in caught.value.reason
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("streaming", [False, True])
def test_sort_reuses_case_insensitive_quadkey_column(tmp_path, streaming):
    import subprocess
    import sys

    table = golden_fixture()
    table = table.append_column("QUADKEY", pa.array(["3", "1", "2"] + ["0"] * (table.num_rows - 3)))
    source = tmp_path / "source.parquet"
    output = tmp_path / "sorted.parquet"
    pq.write_table(table, source)
    args = ["sort", "quadkey", str(source), str(output)]
    if streaming:
        import pyarrow.ipc as ipc

        with pa.BufferOutputStream() as sink:
            with ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            payload = sink.getvalue().to_pybytes()
        args[2] = "-"
        result = subprocess.run(
            [sys.executable, "-c", "from geoparquet_io.cli.main import cli; cli()", *args],
            input=payload,
            capture_output=True,
        )
        assert result.returncode == 0, result.stderr.decode()
    else:
        result = CliRunner().invoke(cli, args)
        assert result.exit_code == 0, result.output
    sorted_table = pq.read_table(output)
    assert sorted_table["QUADKEY"].to_pylist() == sorted(table["QUADKEY"].to_pylist())
    assert sorted_table.column_names == table.column_names


@pytest.mark.parametrize("remove_column", [False, True])
def test_sort_table_reuses_case_insensitive_quadkey_column(remove_column):
    table = golden_fixture().append_column(
        "QUADKEY", pa.array(["3", "1", "2"] + ["0"] * (golden_fixture().num_rows - 3))
    )
    result = ops.sort_quadkey(table, remove_column=remove_column)
    expected = sorted(table["QUADKEY"].to_pylist())
    if remove_column:
        assert "QUADKEY" not in result.column_names
        assert "quadkey" not in result.column_names
    else:
        assert result["QUADKEY"].to_pylist() == expected
        assert result.column_names == table.column_names


def test_index_drivers_use_shared_funnels():
    tree = ast.parse(
        (Path(__file__).parents[1] / "geoparquet_io/core/index_ops.py").read_text(encoding="utf-8")
    )
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    for driver, funnel in [
        ("add_index_column", "add_computed_column"),
        ("add_index_streaming", "execute_transform"),
    ]:
        calls = {
            node.func.id
            for node in ast.walk(functions[driver])
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        assert funnel in calls
        assert not calls.intersection({"open_input", "write_output", "write_parquet_with_metadata"})


def test_add_computed_column_has_no_query_override():
    """The funnel builds its own query; a caller hands it an expression (#1367)."""
    import inspect

    from geoparquet_io.core.common import add_computed_column

    assert "query_builder" not in inspect.signature(add_computed_column).parameters
