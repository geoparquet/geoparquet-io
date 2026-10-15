"""Planar repair must not run on columns that declare non-planar edges (#1287).

The default geometry repair is planar (``ST_MakeValid`` on the plane). A
geometry that is valid under spherical edges but degenerate or
self-intersecting on the plane -- canonically a pole-enclosing ring -- was
silently collapsed into line fragments before every write, while the output
still carried ``edges: spherical`` metadata describing the wrong shapes.

When the geometry column's declared edges are non-planar, the repair (and its
invalid count, which is just as planar a statement) is skipped with a warning.
The declaration can arrive three ways: the input's ``geo`` block, a GeoArrow
spherical edge type on an Arrow schema, or a native GEOGRAPHY logical type.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1287
"""

import json
import logging

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.geometry_repair import (
    repair_arrow_table_geometry,
    repair_query_geometry,
)

#: A cap around the north pole: valid on the sphere, collinear (zero area,
#: hence invalid) on the plane. ST_MakeValid collapses it to line fragments.
POLE_RING_WKT = "POLYGON ((0 80, 90 80, 180 80, -90 80, 0 80))"
#: The winding variant: non-degenerate but self-intersecting on the plane.
WINDING_RING_WKT = "POLYGON ((0 80, 90 82, 180 80, -90 82, 0 80))"
#: Genuinely invalid on the plane AND on the sphere: the planar control case.
BOWTIE_WKT = "POLYGON ((0 0, 1 1, 1 0, 0 1, 0 0))"

SKIP_MARKER = "Skipping planar geometry repair"


def _wkb_of(*wkts: str) -> list[bytes]:
    con = get_duckdb_connection(load_spatial=True)
    try:
        return [
            bytes(con.execute(f"SELECT ST_AsWKB(ST_GeomFromText('{w}'))::BLOB").fetchone()[0])
            for w in wkts
        ]
    finally:
        con.close()


def _geo_block(edges, extra: dict | None = None, column: str = "geometry") -> bytes:
    col: dict = {"encoding": "WKB", "geometry_types": ["Polygon"]}
    if edges:
        col["edges"] = edges
    col.update(extra or {})
    return json.dumps(
        {"version": "1.1.0", "primary_column": column, "columns": {column: col}}
    ).encode()


def _write_input(
    path,
    *wkts: str,
    edges: str | None = "spherical",
    extra: dict | None = None,
    column: str = "geometry",
):
    """A GeoParquet 1.1 file whose geo block declares the given edges value."""
    table = pa.table(
        {
            "id": pa.array(range(len(wkts)), pa.int64()),
            column: pa.array(_wkb_of(*wkts), pa.binary()),
        }
    )
    table = table.replace_schema_metadata({b"geo": _geo_block(edges, extra, column=column)})
    pq.write_table(table, str(path))
    return path


def _storage_wkb(path, column: str = "geometry") -> list[bytes]:
    """The raw WKB bytes of a written file's geometry column."""
    col = pq.read_table(str(path)).column(column).combine_chunks()
    storage = getattr(col, "storage", None)
    chunks = storage if storage is not None else col
    return [v.as_py() for v in chunks]


def _geometry_types(path, column: str = "geometry") -> set[str]:
    """Distinct geometry type names, parsed from the file's raw WKB bytes."""
    con = get_duckdb_connection(load_spatial=True)
    try:
        return {
            con.execute("SELECT ST_GeometryType(ST_GeomFromWKB(?::BLOB))", [wkb]).fetchone()[0]
            for wkb in _storage_wkb(path, column)
        }
    finally:
        con.close()


def _run_cli(caplog, *args) -> str:
    with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
        result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output
    return caplog.text + result.output


# ---------------------------------------------------------------------------
# Unit: the two gated entry points
# ---------------------------------------------------------------------------


def _wkb_query_table(con, *wkts: str):
    rows = "\n UNION ALL ".join(
        f"SELECT {i} AS id, ST_AsWKB(ST_GeomFromText('{w}')) AS geometry"
        for i, w in enumerate(wkts)
    )
    return con.execute(rows).arrow().read_all()


def test_repair_query_geometry_skips_nonplanar_edges(caplog):
    con = get_duckdb_connection(load_spatial=True)
    con.register("t", _wkb_query_table(con, POLE_RING_WKT))
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        out = repair_query_geometry(
            con, "SELECT * FROM t", "geometry", repair=True, nonplanar_edges="spherical"
        )
    assert out == "SELECT * FROM t"
    assert SKIP_MARKER in caplog.text
    assert "spherical" in caplog.text
    assert "Repaired" not in caplog.text


def test_repair_query_geometry_planar_edges_still_repairs(caplog):
    con = get_duckdb_connection(load_spatial=True)
    con.register("t", _wkb_query_table(con, BOWTIE_WKT))
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        out = repair_query_geometry(con, "SELECT * FROM t", "geometry", repair=True)
    assert out != "SELECT * FROM t"
    assert "Repaired 1 invalid geometry" in caplog.text
    assert SKIP_MARKER not in caplog.text


def test_repair_query_geometry_planar_value_is_not_a_skip():
    con = get_duckdb_connection(load_spatial=True)
    con.register("t", _wkb_query_table(con, BOWTIE_WKT))
    out = repair_query_geometry(
        con, "SELECT * FROM t", "geometry", repair=True, nonplanar_edges="planar"
    )
    assert out != "SELECT * FROM t"


def test_repair_arrow_table_skips_on_explicit_edges(caplog):
    con = get_duckdb_connection(load_spatial=True)
    table = _wkb_query_table(con, POLE_RING_WKT)
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        result, n = repair_arrow_table_geometry(
            table, "geometry", repair=True, nonplanar_edges="spherical"
        )
    assert result is table
    assert n == 0
    assert SKIP_MARKER in caplog.text
    assert "Repaired" not in caplog.text


def test_repair_arrow_table_skips_on_geo_block_metadata(caplog):
    con = get_duckdb_connection(load_spatial=True)
    table = _wkb_query_table(con, POLE_RING_WKT).replace_schema_metadata(
        {b"geo": _geo_block("spherical")}
    )
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        result, n = repair_arrow_table_geometry(table, "geometry", repair=True)
    assert result is table
    assert n == 0
    assert SKIP_MARKER in caplog.text


def test_repair_arrow_table_skips_on_geoarrow_spherical_type(caplog):
    import geoarrow.pyarrow as ga
    import geoarrow.types as gt

    spherical_wkb = ga.wkb().with_edge_type(gt.EdgeType.SPHERICAL)
    table = pa.table(
        {
            "id": pa.array([0], pa.int64()),
            "geometry": pa.ExtensionArray.from_storage(
                spherical_wkb, pa.array(_wkb_of(POLE_RING_WKT), pa.binary())
            ),
        }
    )
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        result, n = repair_arrow_table_geometry(table, "geometry", repair=True)
    assert result is table
    assert n == 0
    assert SKIP_MARKER in caplog.text


def test_repair_arrow_table_opt_out_on_nonplanar_is_quiet(caplog):
    """--no-repair-geometry on spherical data: no planar count, no skip warning."""
    con = get_duckdb_connection(load_spatial=True)
    table = _wkb_query_table(con, POLE_RING_WKT)
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        result, n = repair_arrow_table_geometry(
            table, "geometry", repair=False, nonplanar_edges="spherical"
        )
    assert result is table
    assert n == 0
    assert "unrepaired" not in caplog.text
    assert SKIP_MARKER not in caplog.text


def test_repair_arrow_table_planar_control_still_repairs(caplog):
    con = get_duckdb_connection(load_spatial=True)
    table = _wkb_query_table(con, BOWTIE_WKT)
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        result, n = repair_arrow_table_geometry(table, "geometry", repair=True)
    assert n == 1
    assert "Repaired 1 invalid geometry" in caplog.text
    assert result is not table


def test_nonplanar_edges_of_arrow_table_without_the_column_is_none():
    from geoparquet_io.core.geometry_repair import nonplanar_edges_of_arrow_table

    table = pa.table({"id": pa.array([1], pa.int64())}).replace_schema_metadata(
        {b"geo": _geo_block("spherical")}
    )
    assert nonplanar_edges_of_arrow_table(table, "missing") is None


def test_nonplanar_edges_detection_never_breaks_a_pipeline():
    """Edge detection is best-effort: a broken table reads as planar, not a crash."""
    from geoparquet_io.core.geometry_repair import nonplanar_edges_of_arrow_table

    class _Boom:
        @property
        def schema(self):
            raise RuntimeError("no schema")

    assert nonplanar_edges_of_arrow_table(_Boom(), "geometry") is None


def test_api_convert_of_a_planar_geojson_still_repairs(tmp_path, caplog):
    """Control for the Arrow read path: a non-Parquet input is never gated."""
    import geoparquet_io as gpio

    src = tmp_path / "bowtie.geojson"
    src.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"id": 1},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]]],
                        },
                    }
                ],
            }
        )
    )
    out = tmp_path / "out.parquet"
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        gpio.convert(str(src)).write(str(out))
    assert "Repaired 1 invalid geometry" in caplog.text
    assert SKIP_MARKER not in caplog.text


def test_declared_edges_fall_back_to_the_kv_metadata_dict():
    """A stream input has no re-readable path; the KV dict is the witness then."""
    from geoparquet_io.core.write_funnels import declared_nonplanar_edges

    metadata = {b"geo": _geo_block("spherical")}
    assert declared_nonplanar_edges(None, metadata) == {"geometry": "spherical"}
    assert declared_nonplanar_edges(None, None) == {}


def test_declared_edges_prefer_the_file(tmp_path):
    from geoparquet_io.core.write_funnels import declared_nonplanar_edges

    src = _write_input(tmp_path / "in.parquet", POLE_RING_WKT, edges="vincenty")
    metadata = {b"geo": _geo_block("spherical")}
    assert declared_nonplanar_edges(str(src), metadata) == {"geometry": "vincenty"}


# ---------------------------------------------------------------------------
# End to end: the issue's repro through convert, extract, and the Python API
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ring", [POLE_RING_WKT, WINDING_RING_WKT])
def test_convert_preserves_spherical_rings(ring, tmp_path, caplog):
    src = _write_input(tmp_path / "in.parquet", ring)
    out = tmp_path / "out.parquet"
    text = _run_cli(caplog, "convert", "geoparquet", src, out)
    assert SKIP_MARKER in text
    assert "Repaired" not in text
    assert _storage_wkb(out) == _wkb_of(ring)
    assert _geometry_types(out) == {"POLYGON"}


@pytest.mark.parametrize("ring", [POLE_RING_WKT, WINDING_RING_WKT])
def test_extract_preserves_spherical_rings(ring, tmp_path, caplog):
    src = _write_input(tmp_path / "in.parquet", ring)
    out = tmp_path / "out.parquet"
    text = _run_cli(caplog, "extract", "geoparquet", src, out)
    assert SKIP_MARKER in text
    assert "Repaired" not in text
    assert _storage_wkb(out) == _wkb_of(ring)
    assert _geometry_types(out) == {"POLYGON"}


def test_api_convert_preserves_spherical_pole_ring(tmp_path, caplog):
    import geoparquet_io as gpio

    src = _write_input(tmp_path / "in.parquet", POLE_RING_WKT)
    out = tmp_path / "out.parquet"
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        gpio.convert(str(src)).write(str(out))
    assert SKIP_MARKER in caplog.text
    assert "Repaired" not in caplog.text
    assert _storage_wkb(out) == _wkb_of(POLE_RING_WKT)


@pytest.mark.parametrize("column", ["geometry", "geom"])
def test_api_convert_table_describes_spherical_edges(column, tmp_path, caplog):
    """The skipped repair is not enough: the Table must still say spherical.

    ``gpio.convert()`` reads into a bare-WKB table and describes it with a fresh
    geo block; without ``edges`` there every later step reads it as planar. The
    ``geom`` variant catches a lookup under the renamed column only.
    """
    import geoparquet_io as gpio

    src = _write_input(tmp_path / "in.parquet", POLE_RING_WKT, column=column)
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        table = gpio.convert(str(src)).to_arrow()
    assert SKIP_MARKER in caplog.text
    geo = json.loads(table.schema.metadata[b"geo"])
    assert geo["columns"][geo["primary_column"]].get("edges") == "spherical"


def test_api_convert_of_a_planar_parquet_describes_no_edges(tmp_path):
    import geoparquet_io as gpio

    src = _write_input(tmp_path / "in.parquet", BOWTIE_WKT, edges=None)
    geo = json.loads(gpio.convert(str(src)).to_arrow().schema.metadata[b"geo"])
    assert "edges" not in geo["columns"][geo["primary_column"]]


@pytest.mark.xfail(
    strict=False,
    reason=(
        "Table.write rebuilds the geo block without the table's edges (#1253); "
        "PR #1280 restores them. Remove this marker when it lands."
    ),
)
@pytest.mark.parametrize("column", ["geometry", "geom"])
@pytest.mark.parametrize("version", [None, "2.0"])
def test_api_convert_output_declares_spherical_edges(column, version, tmp_path):
    import geoparquet_io as gpio
    from tests.native_geo_probes import geo_block, spec_problems

    src = _write_input(tmp_path / "in.parquet", POLE_RING_WKT, column=column)
    out = tmp_path / "out.parquet"
    gpio.convert(str(src)).write(str(out), geoparquet_version=version)
    geo = geo_block(out)
    assert geo["columns"][geo["primary_column"]].get("edges") == "spherical"
    assert spec_problems(out) == []


def test_api_extract_table_skips_repair_for_spherical_table(tmp_path, caplog):
    from geoparquet_io.core.extract import extract_table

    con = get_duckdb_connection(load_spatial=True)
    table = _wkb_query_table(con, POLE_RING_WKT).replace_schema_metadata(
        {b"geo": _geo_block("spherical")}
    )
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        result = extract_table(table)
    assert SKIP_MARKER in caplog.text
    assert "Repaired" not in caplog.text
    assert result.column("geometry").to_pylist() == _wkb_of(POLE_RING_WKT)


def test_native_geography_input_skips_repair(tmp_path, caplog):
    """A native GEOGRAPHY logical type (no geo block) gets the same protection."""
    import geoarrow.pyarrow as ga
    import geoarrow.types as gt

    from tests.native_geo_probes import write_native_geo_only

    rows = [(0, "g0", _wkb_of(POLE_RING_WKT)[0])]
    src = write_native_geo_only(
        tmp_path / "in.parquet",
        rows,
        {"geometry": (2, ga.wkb().with_edge_type(gt.EdgeType.SPHERICAL))},
    )
    out = tmp_path / "out.parquet"
    text = _run_cli(caplog, "convert", "geoparquet", src, out)
    assert SKIP_MARKER in text
    assert "Repaired" not in text
    assert _geometry_types(out) == {"POLYGON"}

    caplog.clear()
    out2 = tmp_path / "out2.parquet"
    text = _run_cli(caplog, "extract", "geoparquet", src, out2)
    assert SKIP_MARKER in text
    assert "Repaired" not in text
    assert _geometry_types(out2) == {"POLYGON"}


def test_planar_bowtie_is_still_repaired_through_convert(tmp_path, caplog):
    """Control: planar inputs are completely unchanged by the gate."""
    src = _write_input(tmp_path / "in.parquet", BOWTIE_WKT, edges=None)
    out = tmp_path / "out.parquet"
    text = _run_cli(caplog, "convert", "geoparquet", src, out)
    assert "Repaired 1 invalid geometry" in text
    assert SKIP_MARKER not in text


def test_planar_bowtie_is_still_repaired_through_extract(tmp_path, caplog):
    src = _write_input(tmp_path / "in.parquet", BOWTIE_WKT, edges=None)
    out = tmp_path / "out.parquet"
    text = _run_cli(caplog, "extract", "geoparquet", src, out)
    assert "Repaired 1 invalid geometry" in text
    assert SKIP_MARKER not in text


def test_extract_skip_keeps_orientation_declaration(tmp_path, caplog):
    """The orientation drop is tied to an actual repair; a skip keeps it (#1287)."""
    src = _write_input(
        tmp_path / "in.parquet",
        POLE_RING_WKT,
        extra={"orientation": "counterclockwise"},
    )
    out = tmp_path / "out.parquet"
    _run_cli(caplog, "extract", "geoparquet", src, out)
    geo = json.loads(pq.read_table(str(out)).schema.metadata[b"geo"])
    assert geo["columns"]["geometry"].get("orientation") == "counterclockwise"
    assert geo["columns"]["geometry"].get("edges") == "spherical"


# ---------------------------------------------------------------------------
# The gate trusts only a known non-planar algorithm
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bogus", ["Planar", "bogus", 7, {"a": 1}, "\x1b[31mspherical\x1b[0m"])
def test_unknown_edges_value_keeps_planar_repair(bogus, caplog):
    """Only a GeoParquet edge algorithm skips the repair; anything else is planar."""
    con = get_duckdb_connection(load_spatial=True)
    table = _wkb_query_table(con, BOWTIE_WKT)
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        result, n = repair_arrow_table_geometry(
            table, "geometry", repair=True, nonplanar_edges=bogus
        )
    assert n == 1
    assert result is not table
    assert SKIP_MARKER not in caplog.text


def test_unknown_edges_value_in_geo_block_keeps_planar_repair(tmp_path, caplog):
    src = _write_input(tmp_path / "in.parquet", BOWTIE_WKT, edges="bogus")
    out = tmp_path / "out.parquet"
    text = _run_cli(caplog, "extract", "geoparquet", src, out)
    assert "Repaired 1 invalid geometry" in text
    assert SKIP_MARKER not in text


@pytest.mark.parametrize("algorithm", ["vincenty", "karney"])
def test_ellipsoidal_edges_value_skips(algorithm, caplog):
    con = get_duckdb_connection(load_spatial=True)
    table = _wkb_query_table(con, POLE_RING_WKT)
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        _, n = repair_arrow_table_geometry(
            table, "geometry", repair=True, nonplanar_edges=algorithm
        )
    assert n == 0
    assert f"'{algorithm}' edges" in caplog.text


def test_skip_warning_is_one_concise_line_naming_no_cli_flag(caplog):
    con = get_duckdb_connection(load_spatial=True)
    table = _wkb_query_table(con, POLE_RING_WKT)
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        repair_arrow_table_geometry(table, "geometry", repair=True, nonplanar_edges="spherical")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "--" not in message.replace("(#1287)", "")
    assert "column 'geometry'" in message
    assert "'spherical' edges" in message


@pytest.mark.parametrize("edge_type", ["KARNEY", "VINCENTY", "THOMAS", "ANDOYER"])
def test_geoarrow_ellipsoidal_edge_type_is_detected(edge_type):
    import geoarrow.pyarrow as ga
    import geoarrow.types as gt

    from geoparquet_io.core.geometry_repair import nonplanar_edges_of_arrow_table

    wkb_type = ga.wkb().with_edge_type(getattr(gt.EdgeType, edge_type))
    table = pa.table(
        {"geometry": pa.ExtensionArray.from_storage(wkb_type, pa.array(_wkb_of(POLE_RING_WKT)))}
    )
    assert nonplanar_edges_of_arrow_table(table, "geometry") == edge_type.lower()


def test_geoarrow_planar_edge_type_is_planar():
    import geoarrow.pyarrow as ga

    from geoparquet_io.core.geometry_repair import nonplanar_edges_of_arrow_table

    table = pa.table(
        {"geometry": pa.ExtensionArray.from_storage(ga.wkb(), pa.array(_wkb_of(BOWTIE_WKT)))}
    )
    assert nonplanar_edges_of_arrow_table(table, "geometry") is None


# ---------------------------------------------------------------------------
# A failed edges read is said out loud, not silently planar
# ---------------------------------------------------------------------------


def test_unreadable_edges_metadata_warns(tmp_path, monkeypatch, caplog):
    from geoparquet_io.core import write_funnels

    def boom(_path):
        raise RuntimeError("footer unreadable")

    monkeypatch.setattr(write_funnels, "get_schema_info", boom)
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        assert write_funnels.collect_nonplanar_edges(str(tmp_path / "x.parquet")) == {}
    assert "edges" in caplog.text
    assert "footer unreadable" in caplog.text


# ---------------------------------------------------------------------------
# Each call site hands its declaration to the gate
# ---------------------------------------------------------------------------


def test_streaming_extract_to_stdout_preserves_spherical_ring(tmp_path, monkeypatch, caplog):
    """``gpio extract geoparquet in.parquet -``: the streaming path is gated too."""
    import io
    from unittest import mock

    import pyarrow.ipc as ipc

    from geoparquet_io.core.extract import extract

    src = _write_input(tmp_path / "in.parquet", POLE_RING_WKT)
    buffer = io.BytesIO()
    mock_stdout = mock.MagicMock()
    mock_stdout.isatty.return_value = False
    mock_stdout.buffer = buffer
    monkeypatch.setattr("sys.stdout", mock_stdout)

    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        extract(str(src), "-")

    buffer.seek(0)
    column = ipc.open_stream(buffer).read_all().column("geometry").combine_chunks()
    storage = getattr(column, "storage", column)
    assert storage.to_pylist() == _wkb_of(POLE_RING_WKT)
    assert SKIP_MARKER in caplog.text
    assert "Repaired" not in caplog.text


def test_bigquery_native_geography_skips_planar_repair(caplog):
    """A native GEOGRAPHY export declares spherical edges; the repair must not run."""
    from unittest.mock import MagicMock, patch

    from geoparquet_io.core.extract_bigquery import extract_bigquery

    fetched = pa.table(
        {
            "id": pa.array([0], pa.int64()),
            "geom": pa.array(_wkb_of(POLE_RING_WKT), pa.binary()),
        }
    )
    with (
        patch("geoparquet_io.core.extract_bigquery.BigQueryConnection") as mock_conn,
        patch(
            "geoparquet_io.core.extract_bigquery._schema_rows",
            return_value=[("id", "BIGINT"), ("geom", "GEOMETRY")],
        ),
        patch(
            "geoparquet_io.core.extract_bigquery._read_with_view_fallback",
            return_value=fetched,
        ),
        caplog.at_level(logging.WARNING, logger="geoparquet_io"),
    ):
        mock_conn.return_value.__enter__.return_value = MagicMock()
        result = extract_bigquery(table_id="project-name.dataset.table", output_parquet=None)

    assert result.column("geom").to_pylist() == _wkb_of(POLE_RING_WKT)
    assert SKIP_MARKER in caplog.text
    assert "Repaired" not in caplog.text
