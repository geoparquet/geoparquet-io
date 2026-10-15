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
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, register_arrow
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
