"""Every Table method that hands an Arrow table to DuckDB accepts spherical edges (#1253).

DuckDB 1.5 refuses to import a ``geoarrow.wkb`` column whose extension
metadata says ``"edges"`` is anything but ``"planar"`` ("Can't import non-planar
edges", ``src/common/arrow/arrow_type_extension.cpp``). A table read from a
native GEOGRAPHY file carries exactly that, so every ``con.register`` of it
crashed: ``add_bbox``, the ``add_*`` keys, the sorts, ``extract``, ``reproject``,
the aggregations and ``stats``. ``bounds`` swallowed the same error and
returned ``None``.

``duckdb_utils.register_arrow`` is the one registration step: it hands DuckDB
a planar view of the geometry columns and remembers their input fields, so an
operation that passes the geometry through can give the result its edges back,
and one that builds new geometry (an aggregation's cells, a reprojection) does
not stamp spherical edges on shapes it computed in planar math.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1253
"""

from __future__ import annotations

import json

import geoarrow.pyarrow as ga
import geoarrow.types as gt
import pyarrow as pa
import pytest

import geoparquet_io as gpio
from geoparquet_io.core.duckdb_utils import (
    geoarrow_field_edges,
    get_duckdb_connection,
    planar_view,
    register_arrow,
)
from tests.native_geo_probes import conus_wkb, write_native_geo_only

EXT_NAME = b"ARROW:extension:name"
EXT_META = b"ARROW:extension:metadata"


@pytest.fixture(scope="module")
def rows():
    return conus_wkb("cell")


@pytest.fixture(scope="module")
def native_geography(tmp_path_factory, rows):
    return write_native_geo_only(
        tmp_path_factory.mktemp("native") / "in.parquet",
        rows,
        {"geometry": (2, ga.wkb().with_edge_type(gt.EdgeType.SPHERICAL))},
    )


@pytest.fixture(scope="module")
def table(native_geography):
    return gpio.read(str(native_geography))


def _edges(result, column="geometry"):
    """The edges the result's geometry field declares, in either carrier."""
    if isinstance(result, gpio.Table):
        result = result.table
    field = result.schema.field(column)
    name = getattr(getattr(field.type, "edge_type", None), "name", None)
    if name:
        return name.lower()
    raw = (field.metadata or {}).get(EXT_META)
    return (json.loads(raw) if raw else {}).get("edges", "planar")


# --------------------------------------------------------------------------
# The helper itself
# --------------------------------------------------------------------------


def _metadata_only_spherical(rows):
    """The carrier DuckDB and pyarrow hand back unresolved: storage + field metadata."""
    field = pa.field(
        "geometry",
        pa.binary(),
        metadata={EXT_NAME: b"geoarrow.wkb", EXT_META: b'{"edges": "spherical"}'},
    )
    column = pa.array([bytes(r[2]) for r in rows], pa.binary())
    return pa.Table.from_arrays(
        [pa.array([r[0] for r in rows]), column],
        schema=pa.schema([pa.field("id", pa.int64()), field]),
    )


@pytest.mark.parametrize("carrier", ["resolved", "metadata-only"])
def test_register_arrow_accepts_spherical_edges(carrier, table, rows):
    source = table.table if carrier == "resolved" else _metadata_only_spherical(rows)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        registered = register_arrow(con, "t", source)
        count = con.execute("SELECT count(ST_Area(geometry)) FROM t").fetchone()[0]
        assert count == source.num_rows
        assert registered.edges == {"geometry": "spherical"}
        result = con.execute("SELECT * FROM t").arrow().read_all()
    finally:
        con.close()
    assert _edges(source) == "spherical", "the caller's table must not change"
    restored = registered.restore_edges(result)
    assert _edges(restored) == "spherical"
    assert restored.column("geometry").to_pylist() == source.column("geometry").to_pylist()


def test_register_arrow_leaves_a_planar_table_alone(rows):
    planar = pa.table(
        {
            "geometry": pa.ExtensionArray.from_storage(
                ga.wkb(), pa.array([bytes(r[2]) for r in rows], pa.binary())
            )
        }
    )
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        registered = register_arrow(con, "t", planar)
        result = con.execute("SELECT * FROM t").arrow().read_all()
    finally:
        con.close()
    assert registered.edges == {}
    assert registered.restore_edges(result) is result


def test_planar_view_keeps_a_resolved_type_and_its_crs(rows):
    sph = ga.wkb().with_crs("OGC:CRS84").with_edge_type(gt.EdgeType.SPHERICAL)
    source = pa.table(
        {"geometry": pa.ExtensionArray.from_storage(sph, pa.array([bytes(r[2]) for r in rows]))}
    )
    view, registered = planar_view(source)
    field = view.schema.field("geometry")
    assert field.type.edge_type == gt.EdgeType.PLANAR
    assert field.type.crs is not None
    assert (
        view.column("geometry").chunk(0).storage.equals(source.column("geometry").chunk(0).storage)
    )
    assert registered.edges == {"geometry": "spherical"}


def test_planar_view_limits_to_the_named_columns(table):
    view, registered = planar_view(table.table, columns=["id"])
    assert registered.fields == {}
    assert view is table.table


def _meta_field(raw):
    return pa.field("g", pa.binary(), metadata={EXT_NAME: b"geoarrow.wkb", EXT_META: raw})


@pytest.mark.parametrize(
    ("field", "expected"),
    [
        (pa.field("g", ga.wkb().with_edge_type(gt.EdgeType.SPHERICAL)), "spherical"),
        (_meta_field(b'{"edges": "Spherical"}'), "spherical"),
        (_meta_field(b'{"edges": "karney"}'), "karney"),
        (_meta_field(b"{nope"), None),
        (_meta_field(b"[1]"), None),
        (pa.field("g", pa.binary()), None),
    ],
)
def test_geoarrow_field_edges_reads_both_carriers(field, expected):
    assert geoarrow_field_edges(field) == expected


def test_geoarrow_field_edges_planar_is_not_non_planar():
    assert geoarrow_field_edges(pa.field("g", ga.wkb())) in (None, "planar")


def test_one_copy_of_the_edge_helpers():
    """#1280's write-side names are the duckdb_utils helpers, not a second copy."""
    from geoparquet_io.core import arrow_geo_metadata, duckdb_utils

    assert arrow_geo_metadata._field_edges is geoarrow_field_edges
    assert not hasattr(duckdb_utils, "_nonplanar_edges")


def _with_geo_block(table, edges):
    """``table`` with a carried ``geo`` block that states ``edges`` for its geometry."""
    geo = {"version": "2.0.0", "primary_column": "geometry", "columns": {}}
    geo["columns"]["geometry"] = {"encoding": "WKB", "geometry_types": [], "edges": edges}
    return table.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")})


@pytest.mark.parametrize(
    ("type_edges", "block_edges", "declared"),
    [
        # The block wins for what the table declares, but DuckDB reads the type.
        ("spherical", "planar", {}),
        # A typo counts for nothing on the restore side, and DuckDB still refuses it.
        ("sphericall", None, {}),
        ("spherical", None, {"geometry": "spherical"}),
    ],
)
def test_register_follows_the_type_not_the_geo_block(rows, type_edges, block_edges, declared):
    """``geoarrow_field_edges`` is the type's answer, on purpose.

    DuckDB never reads a ``geo`` block: it refuses a column whose GeoArrow
    type is non-planar whatever the block says. So the register path relabels
    on the type, and the geo-block-wins rule stays with
    ``write_funnels.table_nonplanar_edges``, which decides what a write restores.
    """
    from geoparquet_io.core.write_funnels import table_nonplanar_edges

    base = _metadata_only_spherical(rows)
    field = base.schema.field("geometry").with_metadata(
        {EXT_NAME: b"geoarrow.wkb", EXT_META: json.dumps({"edges": type_edges}).encode()}
    )
    arrow = base.set_column(1, field, base.column("geometry"))
    if block_edges is not None:
        arrow = _with_geo_block(arrow, block_edges)

    assert geoarrow_field_edges(arrow.schema.field("geometry")) == type_edges
    assert table_nonplanar_edges(arrow, "geometry", None) == declared
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        with pytest.raises(Exception, match="non-planar"):
            con.register("raw", arrow)
            con.execute("SELECT count(geometry) FROM raw").fetchone()
        registered = register_arrow(con, "t", arrow)
        count = con.execute("SELECT count(ST_Area(geometry)) FROM t").fetchone()[0]
    finally:
        con.close()
    assert count == arrow.num_rows
    assert registered.edges == {"geometry": type_edges}


# --------------------------------------------------------------------------
# Every Table method that registers the table
# --------------------------------------------------------------------------

#: Operations that pass the geometry through: the result keeps the edges.
PASS_THROUGH = {
    "add_bbox": lambda t: t.add_bbox(),
    "add_quadkey": lambda t: t.add_quadkey(),
    "add_h3": lambda t: t.add_h3(),
    "add_a5": lambda t: t.add_a5(),
    "add_s2": lambda t: t.add_s2(),
    "add_kdtree": lambda t: t.add_kdtree(iterations=2),
    "sort_hilbert": lambda t: t.sort_hilbert(),
    "sort_str": lambda t: t.sort_str(),
    "sort_column": lambda t: t.sort_column("id"),
    "sort_quadkey": lambda t: t.sort_quadkey(),
    "extract_bbox": lambda t: t.extract(bbox=(-180, -90, 180, 90)),
    "extract_columns": lambda t: t.extract(columns=["id", "geometry"]),
    "extract_where": lambda t: t.extract(where="id >= 0"),
}


@pytest.mark.parametrize("op", sorted(PASS_THROUGH))
def test_pass_through_ops_keep_spherical_edges(op, table):
    result = PASS_THROUGH[op](table)
    assert result.num_rows > 0
    assert _edges(result) == "spherical"


#: Operations that compute new geometry in planar math: run, but no edges.
NEW_GEOMETRY = {
    "aggregate_h3": lambda t: t.aggregate_h3(3),
    "aggregate_a5": lambda t: t.aggregate_a5(3),
    "reproject_3857": lambda t: t.reproject("EPSG:3857"),
}


@pytest.mark.parametrize("op", sorted(NEW_GEOMETRY))
def test_new_geometry_ops_run_and_do_not_restore_edges(op, table):
    result = NEW_GEOMETRY[op](table)
    assert result.num_rows > 0
    assert _edges(result) == "planar"


def test_add_bbox_metadata_reaches_its_own_check(table):
    """No longer the DuckDB import error: the geo-key check is what refuses now."""
    from geoparquet_io.core.exceptions import GeoParquetError

    with pytest.raises(GeoParquetError, match="no GeoParquet metadata"):
        table.add_bbox().add_bbox_metadata()


def test_stats_reads_a_spherical_table(table):
    assert table.stats()["geometry"]["nulls"] == 0


def test_bounds_do_not_depend_on_the_edges(table, rows, tmp_path):
    """Same answer as the planar twin (both None today: ``bounds`` wraps a
    GEOMETRY column in ``ST_GeomFromWKB``, a separate bug, so this pins only
    that the edges no longer change the outcome)."""
    planar = gpio.read(
        str(write_native_geo_only(tmp_path / "planar.parquet", rows, {"geometry": (2, ga.wkb())}))
    )
    assert table.bounds == planar.bounds


def test_add_bbox_then_write_keeps_the_geography(table, tmp_path):
    from tests.native_geo_probes import native_geo_edges

    out = tmp_path / "out.parquet"
    table.add_bbox().write(str(out))
    assert native_geo_edges(out) == ("Geography", "EdgeType.SPHERICAL")


def _leaf_logical_type(path, column="geometry") -> str:
    """The Parquet logical type of ``column``, found by leaf path.

    ``ParquetSchema.column(i)`` counts leaves, so a struct before the geometry
    (a bbox) shifts it. Reading by the Arrow field index lands on a bbox child.
    """
    import pyarrow.parquet as pq

    schema = pq.ParquetFile(path).schema
    leaf = next(schema.column(i) for i in range(len(schema)) if schema.column(i).path == column)
    return str(leaf.logical_type)


@pytest.mark.parametrize("strategy", ["duckdb-kv", "disk-rewrite", "in-memory", "streaming"])
@pytest.mark.parametrize("with_bbox", [False, True], ids=["no-bbox", "bbox"])
@pytest.mark.parametrize("edges", ["planar", "spherical"])
def test_every_strategy_writes_the_native_type_and_keeps_column_order(
    rows, tmp_path, strategy, with_bbox, edges
):
    """Every write strategy gives the same file: the native logical type with
    the right edges, and the columns in the table's order.

    disk-rewrite used to move the geometry after every other column, so a bbox
    added after the geometry came out ahead of it.
    """
    import pyarrow.parquet as pq

    geo_type = ga.wkb() if edges == "planar" else ga.wkb().with_edge_type(gt.EdgeType.SPHERICAL)
    source = write_native_geo_only(tmp_path / "in.parquet", rows, {"geometry": (2, geo_type)})
    table = gpio.read(str(source))
    if with_bbox:
        table = table.add_bbox()
    out = tmp_path / "out.parquet"
    table.write(str(out), write_strategy=strategy)

    logical = _leaf_logical_type(out)
    if edges == "planar":
        assert logical.startswith("Geometry(")
    else:
        assert logical.startswith("Geography(") and "algorithm=spherical" in logical
    pf = pq.ParquetFile(out)
    assert pf.schema_arrow.names == table.table.column_names
    geo = json.loads(pf.schema_arrow.metadata[b"geo"])
    assert geo["version"] == "2.0.0"
    assert geo["columns"]["geometry"].get("edges") == (None if edges == "planar" else edges)


def test_wkb_conversion_keeps_the_geometry_in_place():
    """The root cause of the disk-rewrite reorder: the wrapper put the geometry last."""
    from geoparquet_io.core.duckdb_utils import _wrap_query_with_wkb_conversion

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        query = "SELECT 1 AS id, ST_Point(0, 0) AS geometry, 2 AS after"
        wrapped = _wrap_query_with_wkb_conversion(query, "geometry", con)
        names = [d[0] for d in con.execute(wrapped).description]
    finally:
        con.close()
    assert names == ["id", "geometry", "after"]


# --------------------------------------------------------------------------
# A planar-only op on spherical data says so, once
# --------------------------------------------------------------------------

PLANAR_MATH = {
    "add_bbox": lambda t: t.add_bbox(),
    "add_quadkey": lambda t: t.add_quadkey(),
    "add_h3": lambda t: t.add_h3(),
    "add_a5": lambda t: t.add_a5(),
    "add_s2": lambda t: t.add_s2(),
    "add_kdtree": lambda t: t.add_kdtree(iterations=2),
    "simplify": lambda t: t.simplify(0.01),
    "reproject": lambda t: t.reproject("EPSG:3857"),
    "aggregate_h3": lambda t: t.aggregate_h3(3),
    "aggregate_a5": lambda t: t.aggregate_a5(3),
}
PLANAR_MARKER = "planar math"


def _planar_warnings(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if PLANAR_MARKER in r.getMessage()]


@pytest.mark.parametrize("op", sorted(PLANAR_MATH))
def test_planar_only_op_on_spherical_data_warns_once(op, table, caplog):
    import logging

    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        PLANAR_MATH[op](table)
    messages = _planar_warnings(caplog)
    assert len(messages) == 1, messages
    assert "spherical" in messages[-1]
    assert "#1286" in messages[-1] and "#1289" in messages[-1]


@pytest.mark.parametrize("op", ["sort_hilbert", "extract_where", "sort_column"])
def test_pass_through_op_does_not_warn(op, table, caplog):
    import logging

    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        PASS_THROUGH[op](table)
    assert _planar_warnings(caplog) == []


def test_planar_data_does_not_warn(rows, tmp_path, caplog):
    import logging

    planar = gpio.read(
        str(write_native_geo_only(tmp_path / "planar.parquet", rows, {"geometry": (2, ga.wkb())}))
    )
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        planar.add_bbox()
        planar.add_h3()
    assert _planar_warnings(caplog) == []


def test_the_warning_does_not_change_the_result(table):
    """Only a log line: the bbox values are the planar ones they were before."""
    bbox = table.add_bbox().table.column("bbox").to_pylist()[0]
    assert bbox == pytest.approx(
        {"xmin": -122.0, "ymin": 26.0, "xmax": -121.6, "ymax": 26.3}, abs=0.05
    )


# --------------------------------------------------------------------------
# The CLI file paths say so too, once per run
# --------------------------------------------------------------------------

PLANAR_MATH_CLI = {
    "add_bbox": ["add", "bbox"],
    "add_quadkey": ["add", "quadkey"],
    "add_h3": ["add", "h3"],
    "add_s2": ["add", "s2"],
    "add_a5": ["add", "a5"],
    "add_kdtree": ["add", "kdtree", "--partitions", "4"],
    "simplify": ["process", "simplify", "--tolerance", "0.01"],
    "reproject": ["convert", "reproject", "--dst-crs", "EPSG:3857"],
    "aggregate": ["process", "aggregate", "h3", "--resolution", "3"],
}


def _run_cli(args, src, out):
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    split = next((i for i, a in enumerate(args) if a.startswith("--")), len(args))
    command, options = args[:split], args[split:]
    return CliRunner().invoke(cli, [*command, str(src), str(out), *options])


@pytest.mark.parametrize("op", sorted(PLANAR_MATH_CLI))
def test_planar_only_cli_on_spherical_file_warns_once(op, native_geography, tmp_path):
    result = _run_cli(PLANAR_MATH_CLI[op], native_geography, tmp_path / "out.parquet")
    assert result.exit_code == 0, result.output
    messages = [line for line in result.output.splitlines() if PLANAR_MARKER in line]
    assert len(messages) == 1, result.output
    assert f"{op} uses planar math" in messages[0]
    assert "spherical" in messages[0]


@pytest.mark.parametrize("op", ["add_bbox", "simplify", "aggregate"])
def test_planar_only_cli_on_a_geo_block_spherical_file_warns(op, rows, tmp_path):
    """A 1.1 WKB file whose geo block declares spherical edges counts too.

    Plain WKB lets simplify take its streaming branch, which never runs
    simplify_table, so the file check is the only one there.
    """
    import pyarrow.parquet as pq

    src = tmp_path / "wkb_spherical.parquet"
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": [], "edges": "spherical"}},
    }
    tbl = pa.table(
        {"id": [r[0] for r in rows], "geometry": pa.array([r[2] for r in rows], pa.binary())}
    )
    pq.write_table(tbl.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), src)
    result = _run_cli(PLANAR_MATH_CLI[op], src, tmp_path / "out.parquet")
    assert result.exit_code == 0, result.output
    assert sum(PLANAR_MARKER in line for line in result.output.splitlines()) == 1


def test_planar_only_cli_on_planar_file_does_not_warn(rows, tmp_path):
    planar = write_native_geo_only(tmp_path / "planar.parquet", rows, {"geometry": (2, ga.wkb())})
    for op in ("add_bbox", "simplify"):
        result = _run_cli(PLANAR_MATH_CLI[op], planar, tmp_path / f"{op}.parquet")
        assert result.exit_code == 0, result.output
        assert PLANAR_MARKER not in result.output


def test_simplify_file_in_memory_warns_once(native_geography, tmp_path, caplog):
    """The in-memory file path runs simplify_table, so the file check must not add a second."""
    import logging

    from geoparquet_io.core.process.simplify import simplify_file

    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        simplify_file(str(native_geography), str(tmp_path / "out.parquet"), tolerance=0.01)
    assert len(_planar_warnings(caplog)) == 1
