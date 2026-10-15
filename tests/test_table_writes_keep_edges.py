"""Non-planar edges survive the table write paths (#1269, #1253).

The DuckDB-query funnel restores a GEOGRAPHY's edges after every write (#999).
The Arrow-table funnel (``write_geoparquet_table``) and the Python API's
``Table.write`` did not. A native GEOGRAPHY table lost its edges, a 2.0 output
of a declared ``edges: spherical`` block came out a planar GEOMETRY (which
``check spec`` warns about), and ``Table.write`` of a native GEOGRAPHY table
raised DuckDB's "Can't import non-planar edges".

Refs: https://github.com/geoparquet/geoparquet-io/issues/1269
Refs: https://github.com/geoparquet/geoparquet-io/issues/1253
"""

from __future__ import annotations

import json
from pathlib import Path

import geoarrow.pyarrow as ga
import geoarrow.types as gt
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.arrow_geo_metadata import (
    nonplanar_edges_from_schema,
    with_planar_edge_types,
)
from geoparquet_io.core.write_funnels import table_nonplanar_edges, write_geoparquet_table
from tests.native_geo_probes import (
    conus_wkb,
    geo_block,
    native_geo_edges,
    spec_problems,
    write_native_geo_only,
)

SPHERICAL = ("Geography", "EdgeType.SPHERICAL")
EXT_NAME = b"ARROW:extension:name"
EXT_META = b"ARROW:extension:metadata"


def _edges(path, column="geometry"):
    return ((geo_block(path) or {}).get("columns") or {}).get(column, {}).get("edges")


def _assert_spherical(out: Path, version: str | None, *, failures: bool = True) -> None:
    if version == "1.1":
        assert native_geo_edges(out) == (None, None)
    else:
        assert native_geo_edges(out) == SPHERICAL
    if geo_block(out) is not None:
        assert _edges(out) == "spherical"
    assert spec_problems(out, "warning") == []
    if failures:
        assert spec_problems(out, "failed") == []


@pytest.fixture(scope="module")
def rows():
    return conus_wkb("cell", "ST_Centroid(cell)")


@pytest.fixture(scope="module")
def native(tmp_path_factory, rows) -> Path:
    return write_native_geo_only(
        tmp_path_factory.mktemp("native") / "in.parquet",
        rows,
        {"geometry": (2, ga.wkb().with_edge_type(gt.EdgeType.SPHERICAL))},
    )


def _plain_table(rows) -> pa.Table:
    return pa.table(
        {
            "id": [r[0] for r in rows],
            "grp": [r[1] for r in rows],
            "geometry": pa.array([bytes(r[2]) for r in rows], pa.binary()),
        }
    )


@pytest.fixture(scope="module")
def declared(tmp_path_factory, rows) -> Path:
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["Polygon"], "edges": "spherical"}
        },
    }
    path = tmp_path_factory.mktemp("declared") / "in.parquet"
    pq.write_table(_plain_table(rows).replace_schema_metadata({"geo": json.dumps(geo)}), path)
    return path


def _metadata_carrier_table(rows) -> pa.Table:
    """The GeoArrow carrier shape pyarrow gives without ``geoarrow.pyarrow``."""
    table = _plain_table(rows)
    field = pa.field(
        "geometry",
        pa.binary(),
        metadata={EXT_NAME: b"geoarrow.wkb", EXT_META: b'{"edges": "spherical"}'},
    )
    return table.set_column(2, field, table.column("geometry"))


class TestSchemaEdges:
    def test_extension_type_and_metadata_carrier(self, rows):
        native_type = ga.wkb().with_edge_type(gt.EdgeType.SPHERICAL)
        schema = pa.schema(
            [
                ("id", pa.int64()),
                pa.field("a", native_type),
                pa.field(
                    "b",
                    pa.binary(),
                    metadata={EXT_NAME: b"geoarrow.wkb", EXT_META: b'{"edges": "Vincenty"}'},
                ),
                pa.field("planar", ga.wkb()),
                pa.field(
                    "bad", pa.binary(), metadata={EXT_NAME: b"geoarrow.wkb", EXT_META: b"{nope"}
                ),
                pa.field(
                    "list", pa.binary(), metadata={EXT_NAME: b"geoarrow.wkb", EXT_META: b"[1]"}
                ),
                pa.field(
                    "num",
                    pa.binary(),
                    metadata={EXT_NAME: b"geoarrow.wkb", EXT_META: b'{"edges": 1}'},
                ),
                pa.field("bare", pa.binary(), metadata={EXT_NAME: b"geoarrow.wkb"}),
            ]
        )
        assert nonplanar_edges_from_schema(schema) == {"a": "spherical", "b": "vincenty"}

    def test_caller_edges_win_and_absent_columns_drop(self, rows):
        geo = {"columns": {"geometry": {"edges": "spherical"}, "gone": {"edges": "spherical"}}}
        table = _plain_table(rows).replace_schema_metadata({"geo": json.dumps(geo)})
        assert table_nonplanar_edges(table, "geometry", "karney") == {"geometry": "karney"}
        assert table_nonplanar_edges(table, "geometry", "planar") == {"geometry": "spherical"}

    def test_planar_edge_types_keep_bytes_and_crs(self, rows):
        crs_type = ga.wkb().with_crs("OGC:CRS84").with_edge_type(gt.EdgeType.SPHERICAL)
        table = pa.table(
            {"geometry": pa.ExtensionArray.from_storage(crs_type, pa.array([bytes(rows[0][2])]))}
        )
        out = with_planar_edge_types(table, ["geometry"])
        assert out.schema.field("geometry").type.edge_type == gt.EdgeType.PLANAR
        assert out.schema.field("geometry").type.crs is not None
        assert (
            out.column("geometry")
            .chunk(0)
            .storage.equals(table.column("geometry").chunk(0).storage)
        )

    def test_planar_edge_types_metadata_carrier(self, rows):
        out = with_planar_edge_types(_metadata_carrier_table(rows), ["geometry"])
        field = out.schema.field("geometry")
        assert json.loads(field.metadata[EXT_META]) == {}
        assert field.metadata[EXT_NAME] == b"geoarrow.wkb"

    @pytest.mark.parametrize("raw", [b"{nope", b"[1]"])
    def test_planar_edge_types_leave_unreadable_metadata(self, rows, raw):
        table = _plain_table(rows)
        field = pa.field(
            "geometry", pa.binary(), metadata={EXT_NAME: b"geoarrow.wkb", EXT_META: raw}
        )
        table = table.set_column(2, field, table.column("geometry"))
        assert (
            with_planar_edge_types(table, ["geometry"]).schema.field("geometry").metadata[EXT_META]
            == raw
        )


class TestArrowFunnel:
    @pytest.mark.parametrize("version", [None, "1.1", "2.0"])
    def test_native_geography(self, native, tmp_path, version):
        out = tmp_path / "out.parquet"
        write_geoparquet_table(
            pq.read_table(native), str(out), geometry_column="geometry", geoparquet_version=version
        )
        _assert_spherical(out, version)

    def test_declared_block_at_2_0_is_geography(self, declared, tmp_path):
        out = tmp_path / "out.parquet"
        write_geoparquet_table(
            pq.read_table(declared), str(out), geometry_column="geometry", geoparquet_version="2.0"
        )
        _assert_spherical(out, "2.0")

    def test_edges_argument_at_2_0_is_geography(self, rows, tmp_path):
        out = tmp_path / "out.parquet"
        write_geoparquet_table(
            _plain_table(rows),
            str(out),
            geometry_column="geometry",
            geoparquet_version="2.0",
            edges="spherical",
        )
        # `edges=` also declares counterclockwise orientation (the BigQuery
        # contract), which these CONUS cells do not follow. Only the edges
        # carriers are under test here.
        _assert_spherical(out, "2.0", failures=False)

    def test_metadata_carrier_at_1_1(self, rows, tmp_path):
        out = tmp_path / "out.parquet"
        write_geoparquet_table(
            _metadata_carrier_table(rows),
            str(out),
            geometry_column="geometry",
            geoparquet_version="1.1",
        )
        _assert_spherical(out, "1.1")

    def test_planar_table_is_untouched(self, rows, tmp_path):
        out = tmp_path / "out.parquet"
        write_geoparquet_table(
            _plain_table(rows), str(out), geometry_column="geometry", geoparquet_version="2.0"
        )
        assert native_geo_edges(out) == ("Geometry", "EdgeType.PLANAR")
        assert _edges(out) is None


class TestSimplify:
    @pytest.mark.parametrize("version", ["1.1", "2.0"])
    def test_native_geography(self, native, tmp_path, version):
        out = tmp_path / "out.parquet"
        args = ["process", "simplify", str(native), str(out), "--tolerance", "0.01"]
        result = CliRunner().invoke(cli, [*args, "--geoparquet-version", version])
        assert result.exit_code == 0, result.output
        _assert_spherical(out, version)

    def test_declared_block_at_2_0(self, declared, tmp_path):
        out = tmp_path / "out.parquet"
        args = ["process", "simplify", str(declared), str(out), "--tolerance", "0.01"]
        result = CliRunner().invoke(cli, [*args, "--geoparquet-version", "2.0"])
        assert result.exit_code == 0, result.output
        _assert_spherical(out, "2.0")


class TestTableWrite:
    @pytest.mark.parametrize("version", [None, "1.1", "2.0"])
    def test_native_geography(self, native, tmp_path, version):
        out = tmp_path / "out.parquet"
        gpio.read(str(native)).write(str(out), geoparquet_version=version)
        _assert_spherical(out, version)

    @pytest.mark.parametrize("version", ["1.1", "2.0"])
    def test_declared_block(self, declared, tmp_path, version):
        out = tmp_path / "out.parquet"
        gpio.read(str(declared)).write(str(out), geoparquet_version=version)
        _assert_spherical(out, version)

    def test_metadata_carrier_table(self, rows, tmp_path):
        out = tmp_path / "out.parquet"
        gpio.Table(_metadata_carrier_table(rows), geometry_column="geometry").write(
            str(out), geoparquet_version="1.1"
        )
        _assert_spherical(out, "1.1")
