"""Every transform path reprojects a projected CRS that has no authority code (#1258).

The input is a small square near Tokyo in a custom Transverse Mercator whose
PROJJSON has no ``id``. The authority-code-only resolver read it as lon/lat, so
``convert geojson`` wrote metres as degrees and ``pmtiles create`` tiled them
off the map, while ``convert reproject`` (a different resolver) got it right.
Each test holds one path to the lon/lat answer, for the CRS in a ``geo`` block
and for the CRS only in the native GEOMETRY logical type.

The streamed paths are covered too. ``convert geojson -`` read the stream's CRS
with an EPSG-only parser (#1261), and ``Table.reproject`` / ``aggregate_h3``
ignored the GeoArrow field of a native-only table (#1262).
"""

from __future__ import annotations

import io
import json
import shutil
import struct
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely
import shapely.geometry
import shapely.ops
from click.testing import CliRunner
from pyproj import CRS, Transformer

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.add.geometry_metrics import AREA_COLUMN
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

TM_PROJ4 = (
    "+proj=tmerc +lat_0=36 +lon_0=139.833333 +k=0.9999 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"
)
TOKYO_LONLAT = (139.8996, 35.7025)
#: A 200 m square centred on TM (6000, -33000), which is TOKYO_LONLAT.
TM_SQUARE = shapely.box(5900.0, -33100.0, 6100.0, -32900.0)
VARIANTS = ["geo-block", "native-only"]


def _tm_body() -> dict:
    body = CRS.from_proj4(TM_PROJ4).to_json_dict()
    body.pop("id", None)
    return body


def _write_geo_block(path: Path, geom, crs) -> None:
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon"]}},
    }
    if crs is not None:
        geo["columns"]["geometry"]["crs"] = crs
    table = pa.table({"id": [1], "geometry": pa.array([shapely.to_wkb(geom)], pa.binary())})
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), str(path))


def _to_native(src: Path, dst: Path) -> None:
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"COPY (SELECT * FROM {sql_path(str(src))}) TO {sql_path(str(dst))} "
            "(FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')"
        )
    finally:
        con.close()
    with pq.ParquetFile(str(dst)) as reader:
        assert b"geo" not in (reader.metadata.metadata or {}), "must be native-geo-only"


@pytest.fixture(scope="module")
def fixtures(tmp_path_factory) -> dict[str, Path]:
    root = tmp_path_factory.mktemp("uncoded_crs")
    geo_block = root / "tm_geo_block.parquet"
    _write_geo_block(geo_block, TM_SQUARE, _tm_body())
    native = root / "tm_native.parquet"
    _to_native(geo_block, native)
    to_lonlat = Transformer.from_crs(CRS.from_proj4(TM_PROJ4), "OGC:CRS84", always_xy=True)
    control = root / "control.parquet"
    _write_geo_block(control, shapely.ops.transform(to_lonlat.transform, TM_SQUARE), None)
    return {"geo-block": geo_block, "native-only": native, "control": control}


def _assert_tokyo(x: float, y: float) -> None:
    assert (x, y) == pytest.approx(TOKYO_LONLAT, abs=0.01), (x, y)


def _geojson_centre(text: str) -> tuple[float, float]:
    feature = json.loads(text)
    feature = feature["features"][0] if "features" in feature else feature
    c = shapely.geometry.shape(feature["geometry"]).centroid
    return c.x, c.y


def _invoke(*args: str):
    result = CliRunner().invoke(cli, list(args))
    assert result.exit_code == 0, result.output
    return result


# --------------------------------------------------------------------------- #
# convert geojson (pmtiles create pipes through it)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("variant", VARIANTS)
def test_convert_geojson_file(variant, fixtures, tmp_path):
    out = tmp_path / "out.geojson"
    _invoke("convert", "geojson", str(fixtures[variant]), str(out))
    _assert_tokyo(*_geojson_centre(out.read_text()))


@pytest.mark.parametrize("variant", VARIANTS)
def test_convert_geojson_stdin(variant, fixtures, tmp_path):
    """The ``--where``/``--bbox`` pmtiles path: an Arrow stream on stdin (#1261)."""
    from geoparquet_io.core.geojson_stream import _convert_from_stream

    table = gpio.read(str(fixtures[variant])).to_arrow()
    out = tmp_path / "out.geojson"
    with patch("geoparquet_io.core.streaming.read_arrow_stream", return_value=table):
        _convert_from_stream(output_path=str(out))
    _assert_tokyo(*_geojson_centre(out.read_text()))


def _explicit_null_crs_table() -> pa.Table:
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {}}}
    geo["columns"]["geometry"]["crs"] = None
    table = pa.table({"geometry": pa.array([shapely.to_wkb(TM_SQUARE)], pa.binary())})
    return table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})


def test_convert_geojson_stdin_warns_once_on_an_explicit_null_crs(tmp_path, caplog):
    """An explicit ``crs: null`` on stdin is unknown, not lon/lat, so the user is told."""
    from geoparquet_io.core.geojson_stream import _convert_from_stream

    table = _explicit_null_crs_table()
    with (
        patch("geoparquet_io.core.streaming.read_arrow_stream", return_value=table),
        caplog.at_level("WARNING"),
    ):
        _convert_from_stream(output_path=str(tmp_path / "a.geojson"))
        _convert_from_stream(output_path=str(tmp_path / "b.geojson"))
    warned = [r for r in caplog.records if r.getMessage().startswith("Input has an explicit null")]
    assert len(warned) == 1


def test_convert_geojson_keep_crs_does_not_resolve(fixtures, tmp_path):
    """``--keep-crs`` writes the input coordinates, so an unresolvable CRS is no error."""
    src = tmp_path / "garbage.parquet"
    _write_geo_block(src, TM_SQUARE, {"type": "ProjectedCRS", "name": "not a real crs"})
    out = tmp_path / "out.geojson"
    _invoke("convert", "geojson", str(src), str(out), "--keep-crs")
    x, _ = _geojson_centre(out.read_text())
    assert x == pytest.approx(6000.0)


def test_convert_geojson_unresolvable_fails_loudly(tmp_path):
    src = tmp_path / "garbage.parquet"
    _write_geo_block(src, TM_SQUARE, {"type": "ProjectedCRS", "name": "not a real crs"})
    result = CliRunner().invoke(cli, ["convert", "geojson", str(src), str(tmp_path / "o.json")])
    assert result.exit_code != 0
    assert "--src-crs" in result.output


@pytest.mark.parametrize("variant", VARIANTS)
def test_convert_reproject(variant, fixtures, tmp_path):
    out = tmp_path / "out.parquet"
    _invoke("convert", "reproject", str(fixtures[variant]), str(out), "--verbose")
    c = shapely.from_wkb(pq.read_table(str(out)).column("geometry")[0].as_py()).centroid
    _assert_tokyo(c.x, c.y)


def test_convert_reproject_summary_names_an_uncoded_source(fixtures, tmp_path):
    """The summary line shows a short label, not a page of PROJJSON."""
    out = tmp_path / "out.parquet"
    result = _invoke("convert", "reproject", str(fixtures["geo-block"]), str(out))
    source_line = next(ln for ln in result.output.splitlines() if "Source CRS" in ln)
    assert "(no authority code)" in source_line
    assert "{" not in source_line


def test_convert_reproject_summary_names_an_uncoded_crs84_body_ogc_crs84(tmp_path):
    """A lon/lat body with no id is reported as OGC:CRS84, as before the resolver."""
    body = CRS.from_user_input("OGC:CRS84").to_json_dict()
    body.pop("id", None)
    src = tmp_path / "crs84.parquet"
    _write_geo_block(src, shapely.box(0, 0, 1, 1), body)
    result = _invoke("convert", "reproject", str(src), str(tmp_path / "out.parquet"))
    source_line = next(ln for ln in result.output.splitlines() if "Source CRS" in ln)
    assert source_line.strip() == "Source CRS: OGC:CRS84"


@pytest.mark.parametrize("grid", [("h3", "--resolution", "7"), ("a5", "--resolution", "10")])
def test_process_aggregate_unresolvable_is_a_clean_error(grid, tmp_path):
    """An unusable CRS prints one ``Error:`` line, like every other command, not a traceback."""
    src = tmp_path / "garbage.parquet"
    _write_geo_block(src, TM_SQUARE, {"type": "ProjectedCRS", "name": "not a real crs"})
    args = ["process", "aggregate", grid[0], str(src), str(tmp_path / "o.parquet"), *grid[1:]]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit), result.exception
    assert "Error: Cannot use the input CRS" in result.output
    assert "--src-crs" in result.output


def test_convert_reproject_src_crs_works_around_an_unresolvable_crs(tmp_path):
    """``--src-crs`` is what the error message points at, so it must not read the file's CRS."""
    src = tmp_path / "garbage.parquet"
    _write_geo_block(src, TM_SQUARE, {"type": "ProjectedCRS", "name": "not a real crs"})
    out = tmp_path / "out.parquet"
    _invoke("convert", "reproject", str(src), str(out), "--src-crs", TM_PROJ4, "--verbose")
    c = shapely.from_wkb(pq.read_table(str(out)).column("geometry")[0].as_py()).centroid
    _assert_tokyo(c.x, c.y)


def test_stream_transform_reads_the_stream_crs(fixtures):
    """A streamed input's CRS reaches a CRS-aware query builder through the resolver."""
    from geoparquet_io.core.stream_io import _build_transform_query

    metadata = pq.read_schema(str(fixtures["geo-block"])).metadata
    seen = []
    query = _build_transform_query(
        lambda source, con, crs: seen.append(crs) or "SELECT 1", "src", None, "-", metadata, True
    )
    assert query == "SELECT 1"
    assert seen[0].startswith("{")


def test_a_builder_without_a_signature_gets_no_crs():
    from geoparquet_io.core.stream_io import _wants_source_crs

    assert _wants_source_crs(max) is False


def test_table_explicit_null_crs_is_not_read_from_the_field():
    from geoparquet_io.core.crs_utils import extract_crs_from_table

    assert extract_crs_from_table(_explicit_null_crs_table()) is None


# --------------------------------------------------------------------------- #
# add grid keys, partition, metrics
# --------------------------------------------------------------------------- #

_GRID_CASES = [
    ("h3", "h3_cell", ["--resolution", "7"]),
    ("a5", "a5_cell", ["--resolution", "10"]),
    ("s2", "s2_cell", ["--level", "12"]),
    ("quadkey", "quadkey", ["--resolution", "12"]),
]


def _grid_key(fixtures, tmp_path, src_name, cmd, col, args):
    out = tmp_path / f"{src_name}_{cmd}.parquet"
    _invoke("add", cmd, str(fixtures[src_name]), str(out), *args)
    return pq.read_table(str(out)).column(col).to_pylist()


@pytest.mark.parametrize("variant", VARIANTS)
@pytest.mark.parametrize(("cmd", "col", "args"), _GRID_CASES, ids=[c[0] for c in _GRID_CASES])
def test_add_grid_key(cmd, col, args, variant, fixtures, tmp_path):
    got = _grid_key(fixtures, tmp_path, variant, cmd, col, args)
    expected = _grid_key(fixtures, tmp_path, "control", cmd, col, args)
    assert got == expected


@pytest.mark.parametrize("variant", VARIANTS)
def test_partition_h3_preview(variant, fixtures, tmp_path):
    expected = _grid_key(fixtures, tmp_path, "control", "h3", "h3_cell", ["--resolution", "7"])
    result = _invoke("partition", "h3", str(fixtures[variant]), "--resolution", "7", "--preview")
    assert expected[0] in result.output


@pytest.mark.parametrize("variant", VARIANTS)
def test_geometry_metrics_to_stdout(variant, fixtures, tmp_path):
    result = _invoke("add", "geometry-metrics", str(fixtures[variant]), "-")
    area = pa.ipc.open_stream(io.BytesIO(result.stdout_bytes)).read_all().column(AREA_COLUMN)
    # A 200 m square on a scale-0.9999 projection: about 40,000 m².
    assert area[0].as_py() == pytest.approx(40_000.0, rel=0.01)


# --------------------------------------------------------------------------- #
# Python API
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("variant", VARIANTS)
def test_table_grid_keys(variant, fixtures):
    table = gpio.read(str(fixtures[variant]))
    control = gpio.read(str(fixtures["control"]))
    for method, kwargs, col in (
        ("add_h3", {"resolution": 7}, "h3_cell"),
        ("add_s2", {"level": 12}, "s2_cell"),
        ("add_quadkey", {"resolution": 12}, "quadkey"),
    ):
        got = getattr(table, method)(**kwargs).to_arrow().column(col).to_pylist()
        want = getattr(control, method)(**kwargs).to_arrow().column(col).to_pylist()
        assert got == want, method


@pytest.mark.parametrize("variant", VARIANTS)
def test_table_to_geojson(variant, fixtures, tmp_path):
    out = tmp_path / "out.geojson"
    gpio.read(str(fixtures[variant])).to_geojson(str(out))
    _assert_tokyo(*_geojson_centre(out.read_text()))


@pytest.mark.parametrize("variant", VARIANTS)
def test_table_reproject(variant, fixtures):
    """#1262: a native-only table's CRS is on the GeoArrow field, not in a geo block."""
    result = gpio.read(str(fixtures[variant])).reproject(target_crs="EPSG:4326")
    geom = shapely.from_wkb(result.to_arrow().column("geometry")[0].as_py())
    _assert_tokyo(geom.centroid.x, geom.centroid.y)


@pytest.mark.parametrize("variant", VARIANTS)
def test_table_aggregate_h3(variant, fixtures):
    got = gpio.read(str(fixtures[variant])).aggregate_h3(resolution=7)
    want = gpio.read(str(fixtures["control"])).aggregate_h3(resolution=7)
    assert got.to_arrow().column("h3_cell").to_pylist() == (
        want.to_arrow().column("h3_cell").to_pylist()
    )


# --------------------------------------------------------------------------- #
# pmtiles create, end to end through tippecanoe
# --------------------------------------------------------------------------- #


def _pmtiles_bounds(path: Path) -> tuple[float, float, float, float]:
    """``(min_lon, min_lat, max_lon, max_lat)`` from a PMTiles v3 header."""
    header = path.read_bytes()[:127]
    assert header[:7] == b"PMTiles", header[:7]
    return tuple(v / 1e7 for v in struct.unpack_from("<iiii", header, 102))


@pytest.mark.slow
@pytest.mark.skipif(sys.platform == "win32", reason="tippecanoe is not on Windows")
@pytest.mark.skipif(shutil.which("tippecanoe") is None, reason="tippecanoe not installed")
@pytest.mark.skipif(shutil.which("gpio") is None, reason="gpio not on PATH")
@pytest.mark.parametrize("extra", [[], ["--where", "id >= 0"]], ids=["plain", "where"])
def test_pmtiles_create_lands_in_tokyo(extra, fixtures, tmp_path):
    out = tmp_path / "out.pmtiles"
    cmd = ["gpio", "pmtiles", "create", str(fixtures["geo-block"]), str(out), *extra]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    min_lon, min_lat, max_lon, max_lat = _pmtiles_bounds(out)
    _assert_tokyo((min_lon + max_lon) / 2, (min_lat + max_lat) / 2)
