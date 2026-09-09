"""Root-level column listing must exclude struct children (issue #931).

``get_schema_info()`` returns a flat, depth-first listing of the Parquet schema
in which a struct's children follow their parent. Neither the DuckDB nor the
pyarrow shape renders dotted paths, so filtering that listing with
``"." not in name`` never excluded anything and every covering-bbox file
(``geometry_bbox`` plus ``xmin``/``ymin``/``xmax``/``ymax``) was over-counted.
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_metadata import get_column_names, get_schema_info
from geoparquet_io.core.parquet_schema import (
    root_schema_columns,
    root_schema_index,
    schema_direct_children,
)

COVERING_FILE = str(Path(__file__).parent / "data" / "austria_bbox_covering.parquet")


@pytest.fixture
def nested_struct_parquet(tmp_path):
    """A file with a struct nested inside a struct, plus a plain column."""
    table = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "outer": pa.array(
                [{"inner": {"deep": 1}, "flat": 2}, {"inner": {"deep": 3}, "flat": 4}],
                pa.struct(
                    [
                        ("inner", pa.struct([("deep", pa.int64())])),
                        ("flat", pa.int64()),
                    ]
                ),
            ),
        }
    )
    path = tmp_path / "nested_struct.parquet"
    pq.write_table(table, path)
    return str(path)


def test_get_column_names_excludes_covering_struct_children():
    """xmin/ymin/xmax/ymax are children of geometry_bbox, not root columns."""
    names = get_column_names(COVERING_FILE)

    assert "geometry_bbox" in names
    for child in ("xmin", "ymin", "xmax", "ymax"):
        assert child not in names, f"{child} is a struct child, not a root column"
    assert len(names) == 10


def test_inspect_head_and_meta_agree_on_column_count():
    """``inspect head`` and ``inspect meta`` must report the same column count."""
    runner = CliRunner()

    head = runner.invoke(cli, ["inspect", "head", COVERING_FILE])
    assert head.exit_code == 0, head.output
    meta = runner.invoke(cli, ["inspect", "meta", COVERING_FILE])
    assert meta.exit_code == 0, meta.output

    assert "Columns (10)" in head.output
    assert "Columns: 10" in meta.output


def test_duckdb_schema_wrapper_counts_root_columns_only():
    from geoparquet_io.core.duckdb_utils import _DuckDBSchemaWrapper

    wrapper = _DuckDBSchemaWrapper(get_schema_info(COVERING_FILE))

    assert len(wrapper) == 10
    assert [wrapper.field(i).name for i in range(len(wrapper))] == get_column_names(COVERING_FILE)


def test_nested_struct_children_are_not_root_columns(nested_struct_parquet):
    """A struct inside a struct contributes no root-level columns."""
    assert get_column_names(nested_struct_parquet) == ["id", "outer"]


def test_duckdb_schema_shape_agrees_with_pyarrow_fast_path(nested_struct_parquet):
    """Passing a connection takes the DuckDB path, which renders the full tree.

    DuckDB's ``parquet_schema()`` lists grandchildren (``outer.inner.deep``) that
    the pyarrow fast path omits, so this is the shape that exercises the
    recursive subtree walk rather than a single level of children.
    """
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection(load_spatial=False)
    try:
        schema_info = get_schema_info(nested_struct_parquet, con)
        assert "deep" in [c.get("name") for c in schema_info]
        assert get_column_names(nested_struct_parquet, con) == ["id", "outer"]
        assert root_schema_index(schema_info, "deep") is None
    finally:
        con.close()


def test_root_schema_columns_matches_root_index_lookup():
    """The listing helper and the single-name lookup agree on what is a root."""
    schema_info = get_schema_info(COVERING_FILE)
    roots = root_schema_columns(schema_info)

    assert [c["name"] for c in roots] == get_column_names(COVERING_FILE)
    assert root_schema_index(schema_info, "xmin") is None
    assert root_schema_index(schema_info, "geometry_bbox") is not None


def test_helpers_survive_a_truncated_listing():
    """A struct claiming more children than the listing holds must not overrun."""
    truncated = [
        {"name": "outer", "type": None, "num_children": 3},
        {"name": "only_child", "type": "INT64", "num_children": 0},
    ]

    assert [c["name"] for c in root_schema_columns(truncated)] == ["outer"]
    assert [c["name"] for c in schema_direct_children(truncated, 0)] == ["only_child"]


def test_empty_listing_has_no_root_columns():
    assert root_schema_columns([]) == []
    assert root_schema_index([], "geometry") is None


def test_root_schema_columns_skips_empty_names():
    """The DuckDB root group element carries no name and is never a column."""
    schema_info = [
        {"file_name": "f.parquet", "name": "root", "type": None, "num_children": 2},
        {"name": "", "type": "INT64", "num_children": 0},
        {"name": "id", "type": "INT64", "num_children": 0},
    ]

    assert [c["name"] for c in root_schema_columns(schema_info)] == ["id"]
