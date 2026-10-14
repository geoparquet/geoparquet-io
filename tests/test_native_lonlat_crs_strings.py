"""A lon/lat CRS spelled as a plain string still needs no transform (#1264).

DuckDB's ``ST_SetCRS`` writes whatever string it is given into the native
GEOMETRY logical type, so a file with no ``geo`` block can carry ``WGS84`` or
``+proj=longlat +datum=WGS84 +no_defs``. Both are lon/lat. Every transform
path accepted them before the one resolver, and must keep doing so instead of
refusing them as an unknown CRS.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import shapely
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.crs_utils import extract_crs_from_parquet
from geoparquet_io.core.duckdb_utils import _escape_sql_string, get_duckdb_connection, sql_path

LON, LAT = 139.9, 35.7
#: Two points, because tippecanoe cannot guess a max zoom from one location.
_POINTS = (
    f"(VALUES (1, 'POINT ({LON} {LAT})'), (2, 'POINT ({LON + 0.05} {LAT + 0.05})')) t(id, wkt)"
)
#: A compound CRS counts as lon/lat when its horizontal part is.
LONLAT_STRINGS = {
    "wgs84": "WGS84",
    "longlat": "+proj=longlat +datum=WGS84 +no_defs",
    "compound_code": "EPSG:4326+5773",
    "compound_name": "WGS 84 + EGM96 height",
}


@pytest.fixture(scope="module")
def native_files(tmp_path_factory) -> dict[str, Path]:
    root = tmp_path_factory.mktemp("native_lonlat")
    con = get_duckdb_connection(load_spatial=True)
    files = {}
    try:
        for name, crs in LONLAT_STRINGS.items():
            path = root / f"{name}.parquet"
            con.execute(
                "COPY (SELECT id, ST_SetCRS(ST_GeomFromText(wkt), "
                f"'{_escape_sql_string(crs)}') AS geometry FROM {_POINTS} ORDER BY id) "
                f"TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')"
            )
            files[name] = path
        control = root / "control.parquet"
        con.execute(
            f"COPY (SELECT id, ST_GeomFromText(wkt) AS geometry FROM {_POINTS} ORDER BY id) "
            f"TO {sql_path(str(control))} (FORMAT PARQUET)"
        )
        files["control"] = control
    finally:
        con.close()
    return files


def _invoke(*args: str):
    result = CliRunner().invoke(cli, list(args))
    assert result.exit_code == 0, result.output
    return result


def _assert_point(geom) -> None:
    assert (geom.x, geom.y) == pytest.approx((LON, LAT), abs=1e-6)


@pytest.mark.parametrize("name", LONLAT_STRINGS)
def test_the_fixture_carries_the_plain_string(name, native_files):
    with pq.ParquetFile(str(native_files[name])) as reader:
        assert b"geo" not in (reader.metadata.metadata or {})
    assert extract_crs_from_parquet(str(native_files[name])) == LONLAT_STRINGS[name]


def _h3_cells(native_files, tmp_path, name, *command) -> list:
    """The h3 cells a command wrote, in an order that does not depend on row order."""
    out = tmp_path / f"{name}.parquet"
    _invoke(*command, str(native_files[name]), str(out), "--resolution", "7")
    table = pq.read_table(str(out))
    cells = table.column("h3_cell").to_pylist()
    # Row order is not part of either command's contract (aggregate groups).
    if "id" in table.column_names:
        return sorted(zip(table.column("id").to_pylist(), cells, strict=True))
    return sorted(cells)


@pytest.mark.parametrize("command", [("add", "h3"), ("process", "aggregate", "h3")])
@pytest.mark.parametrize("name", LONLAT_STRINGS)
def test_h3_cells_match_the_crs_less_control(name, command, native_files, tmp_path):
    got = _h3_cells(native_files, tmp_path, name, *command)
    assert got == _h3_cells(native_files, tmp_path, "control", *command)


@pytest.mark.parametrize("name", LONLAT_STRINGS)
def test_convert_geojson(name, native_files, tmp_path):
    out = tmp_path / "out.geojson"
    _invoke("convert", "geojson", str(native_files[name]), str(out))
    feature = json.loads(out.read_text())["features"][0]
    _assert_point(shapely.geometry.shape(feature["geometry"]))


@pytest.mark.parametrize("name", LONLAT_STRINGS)
def test_convert_reproject_without_src_crs(name, native_files, tmp_path):
    out = tmp_path / "out.parquet"
    _invoke("convert", "reproject", str(native_files[name]), str(out))
    _assert_point(shapely.from_wkb(pq.read_table(str(out)).column("geometry")[0].as_py()))


@pytest.mark.skipif(sys.platform == "win32", reason="tippecanoe is not on Windows")
@pytest.mark.skipif(shutil.which("tippecanoe") is None, reason="tippecanoe not installed")
@pytest.mark.skipif(shutil.which("gpio") is None, reason="gpio not on PATH")
@pytest.mark.parametrize("name", LONLAT_STRINGS)
def test_pmtiles_create(name, native_files, tmp_path):
    out = tmp_path / "out.pmtiles"
    cmd = ["gpio", "pmtiles", "create", str(native_files[name]), str(out)]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    assert out.read_bytes()[:7] == b"PMTiles"
