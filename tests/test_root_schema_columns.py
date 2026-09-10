"""Root-level column listing must exclude struct children (issue #931).

``get_schema_info()`` returns a flat, depth-first listing of the Parquet schema
in which a struct's children follow their parent. Neither the DuckDB nor the
pyarrow shape renders dotted paths, so filtering that listing with
``"." not in name`` never excluded anything and every covering-bbox file
(``geometry_bbox`` plus ``xmin``/``ymin``/``xmax``/``ymax``) was over-counted.

Skipping subtrees only works while every entry's ``num_children`` agrees with
the number of rows the listing actually goes on to emit for it. The pyarrow
fast path broke that for ``list``/``map``/``fixed_size_list``: it wrote the
type's field count but emitted child rows only for iterable (struct) types, so
each root-level list column swallowed the *next* root column and columns
vanished from ``get_column_names()``.
"""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_metadata import get_column_names, get_schema_info
from geoparquet_io.core.parquet_schema import (
    root_schema_columns,
    root_schema_index,
    schema_direct_children,
)

COVERING_FILE = str(Path(__file__).parent / "data" / "austria_bbox_covering.parquet")
OVERTURE_FILE = str(Path(__file__).parent / "data" / "country_partition" / "El_Salvador.parquet")


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


# ---------------------------------------------------------------------------
# non-struct nesting: list, large_list, fixed_size_list and map (issue #931)
# ---------------------------------------------------------------------------


@pytest.fixture
def nested_types_parquet(tmp_path):
    """Every root-level nesting kind, each followed by a plain column.

    The follower is what makes the file a regression test: a parent that claims
    children the listing never emits consumes its next *sibling* instead.
    """
    table = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "tags": pa.array([["a"], ["b", "c"]], pa.list_(pa.string())),
            "after_list": pa.array([1, 2], pa.int64()),
            "big_tags": pa.array([["a"], ["b"]], pa.large_list(pa.string())),
            "after_large_list": pa.array([1, 2], pa.int64()),
            "coords": pa.array([[1.0, 2.0], [3.0, 4.0]], pa.list_(pa.float64(), 2)),
            "after_fixed_size_list": pa.array([1, 2], pa.int64()),
            "attrs": pa.array([[("k", "v")], [("k", "w")]], pa.map_(pa.string(), pa.string())),
            "after_map": pa.array([1, 2], pa.int64()),
            "bbox": pa.array(
                [{"xmin": 0.0, "ymin": 0.0}, {"xmin": 1.0, "ymin": 1.0}],
                pa.struct([("xmin", pa.float64()), ("ymin", pa.float64())]),
            ),
            "after_struct": pa.array([1, 2], pa.int64()),
        }
    )
    path = tmp_path / "nested_types.parquet"
    pq.write_table(table, path)
    return str(path)


NESTED_TYPES_COLUMNS = [
    "id",
    "tags",
    "after_list",
    "big_tags",
    "after_large_list",
    "coords",
    "after_fixed_size_list",
    "attrs",
    "after_map",
    "bbox",
    "after_struct",
]


def test_list_and_map_columns_do_not_swallow_the_next_column(nested_types_parquet):
    """pyarrow fast path: no root column may be hidden by its predecessor."""
    assert get_column_names(nested_types_parquet) == NESTED_TYPES_COLUMNS


def test_list_and_map_columns_are_root_columns_on_the_duckdb_path(nested_types_parquet):
    """The DuckDB shape renders list/map internals and must agree all the same."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection(load_spatial=False)
    try:
        assert get_column_names(nested_types_parquet, con) == NESTED_TYPES_COLUMNS
        schema_info = get_schema_info(nested_types_parquet, con)
        # "element"/"key_value" are list and map internals, never root columns.
        for internal in ("element", "key_value", "key", "value"):
            assert root_schema_index(schema_info, internal) is None
    finally:
        con.close()


def test_both_schema_shapes_list_the_same_root_columns(nested_types_parquet):
    """The two listing shapes are only useful if they agree on the roots."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection(load_spatial=False)
    try:
        duckdb_roots = [
            c["name"] for c in root_schema_columns(get_schema_info(nested_types_parquet, con))
        ]
    finally:
        con.close()
    pyarrow_roots = [c["name"] for c in root_schema_columns(get_schema_info(nested_types_parquet))]

    assert pyarrow_roots == duckdb_roots == NESTED_TYPES_COLUMNS


def test_pyarrow_num_children_counts_only_the_rows_it_emits(nested_types_parquet):
    """The invariant the subtree walk depends on, asserted directly.

    ``num_children`` is a count of *emitted rows*, not of the type's fields:
    the fast path renders no rows for list/map internals, so those entries
    declare none.
    """
    schema_info = get_schema_info(nested_types_parquet)
    by_name = {c["name"]: c for c in schema_info}

    for nested in ("tags", "big_tags", "coords", "attrs"):
        assert by_name[nested]["num_children"] == 0, nested
    assert by_name["bbox"]["num_children"] == 2
    assert [
        c["name"] for c in schema_direct_children(schema_info, schema_info.index(by_name["bbox"]))
    ] == [
        "xmin",
        "ymin",
    ]


def test_wrapper_and_column_names_agree_on_a_file_with_list_columns(nested_types_parquet):
    from geoparquet_io.core.duckdb_utils import _DuckDBSchemaWrapper

    wrapper = _DuckDBSchemaWrapper(get_schema_info(nested_types_parquet))

    assert [wrapper.field(i).name for i in range(len(wrapper))] == NESTED_TYPES_COLUMNS


# ---------------------------------------------------------------------------
# the shipped Overture fixture, which is what users actually hit
# ---------------------------------------------------------------------------


def test_overture_fixture_head_and_meta_agree(tmp_path):
    """``inspect head`` and ``inspect meta`` must not disagree in either direction.

    El_Salvador.parquet has 19 root columns, six of which follow a list column.
    """
    expected = [f.name for f in pq.ParquetFile(OVERTURE_FILE).schema_arrow]
    assert len(expected) == 19

    assert get_column_names(OVERTURE_FILE) == expected

    runner = CliRunner()
    head = runner.invoke(cli, ["inspect", "head", OVERTURE_FILE])
    assert head.exit_code == 0, head.output
    meta = runner.invoke(cli, ["inspect", "meta", OVERTURE_FILE])
    assert meta.exit_code == 0, meta.output

    assert "Columns (19)" in head.output
    assert "Columns: 19" in meta.output
    for hidden in ("names", "socials", "emails", "phones", "brand", "operating_status"):
        assert hidden in get_column_names(OVERTURE_FILE)


def test_partition_string_accepts_a_column_that_follows_a_list(tmp_path):
    """A membership check built on the walk must not reject a real column."""
    out = tmp_path / "parts"
    result = CliRunner().invoke(
        cli,
        ["partition", "string", OVERTURE_FILE, str(out), "--column", "operating_status"],
    )

    assert result.exit_code == 0, result.output
    assert "not found in the Parquet file" not in result.output


def test_add_quadkey_still_refuses_an_existing_column_after_a_list(tmp_path):
    """The collision guard reads the same listing; a hidden column bypassed it."""
    table = pa.table(
        {
            "tags": pa.array([["a"], ["b"]], pa.list_(pa.string())),
            "quadkey": pa.array(["0", "1"], pa.string()),
            "geometry": pa.array(
                [shapely.to_wkb(shapely.Point(1.0, 2.0)), shapely.to_wkb(shapely.Point(3.0, 4.0))],
                pa.binary(),
            ),
        }
    )
    geo = {
        "version": "1.0.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
    }
    src = tmp_path / "has_quadkey.parquet"
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), src)

    assert "quadkey" in get_column_names(str(src))

    result = CliRunner().invoke(cli, ["add", "quadkey", str(src), str(tmp_path / "out.parquet")])

    assert result.exit_code != 0
    assert "already exists" in result.output


# ---------------------------------------------------------------------------
# a covering bbox that follows a list column is still at the schema root
# ---------------------------------------------------------------------------


def test_check_spec_finds_a_covering_bbox_that_follows_a_list_column(tmp_path):
    """Pre-existing since #930: the walk lost the bbox struct behind a list.

    A spec-compliant 1.1 file failed ``check spec`` three times over with
    'bbox column "bbox" is not at the schema root'.
    """
    points = [shapely.Point(1.0, 2.0), shapely.Point(3.0, 4.0)]
    table = pa.table(
        {
            "tags": pa.array([["a"], ["b"]], pa.list_(pa.string())),
            "bbox": pa.array(
                [{"xmin": p.x, "ymin": p.y, "xmax": p.x, "ymax": p.y} for p in points],
                pa.struct(
                    [
                        ("xmin", pa.float64()),
                        ("ymin", pa.float64()),
                        ("xmax", pa.float64()),
                        ("ymax", pa.float64()),
                    ]
                ),
            ),
            "geometry": pa.array([shapely.to_wkb(p) for p in points], pa.binary()),
        }
    )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "bbox": [1.0, 2.0, 3.0, 4.0],
                "covering": {
                    "bbox": {
                        "xmin": ["bbox", "xmin"],
                        "ymin": ["bbox", "ymin"],
                        "xmax": ["bbox", "xmax"],
                        "ymax": ["bbox", "ymax"],
                    }
                },
            }
        },
    }
    src = tmp_path / "covering_after_list.parquet"
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), src)

    result = CliRunner().invoke(cli, ["check", "spec", str(src)])

    assert "is not at the schema root" not in result.output
    assert "0 failed" in result.output


# ---------------------------------------------------------------------------
# the walk's own defences
# ---------------------------------------------------------------------------


def test_a_leaf_entry_cannot_claim_children_pyarrow_shape():
    """A primitive type has no subtree, whatever ``num_children`` says."""
    schema_info = [
        {"name": "id", "type": "int64", "num_children": 2},
        {"name": "after", "type": "int64", "num_children": 0},
        {"name": "last", "type": "string", "num_children": 0},
    ]

    assert [c["name"] for c in root_schema_columns(schema_info)] == ["id", "after", "last"]
    assert schema_direct_children(schema_info, 0) == []


def test_a_leaf_entry_cannot_claim_children_duckdb_shape():
    """DuckDB renders leaves with a physical type and groups with ``None``."""
    schema_info = [
        {"file_name": "f.parquet", "name": "schema", "type": None, "num_children": 2},
        {"name": "id", "type": "BYTE_ARRAY", "num_children": 3},
        {"name": "after", "type": "INT64", "num_children": None},
    ]

    assert [c["name"] for c in root_schema_columns(schema_info)] == ["id", "after"]
