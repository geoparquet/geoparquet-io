"""The CLI and the Table API read a free-form native CRS string the same way (#1264).

DuckDB ``ST_SetCRS`` writes any string into the native GEOMETRY type. The Table
API reads such a string through the GeoArrow field, where pyproj turns it into
PROJJSON. The CLI reads the same string from the Parquet schema. Both must land
on the same coordinates, so a file that works in one works in the other.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
import shapely
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import _escape_sql_string, get_duckdb_connection, sql_path

TOKYO = "(VALUES (1, 'POINT (139.9 35.7)'), (2, 'POINT (139.95 35.75)')) t(id, wkt)"
UTM31 = "(VALUES (1, 'POINT (500000 4982950.4)'), (2, 'POINT (500200 4983150.4)')) t(id, wkt)"

#: name -> (CRS string, points in that CRS)
FREE_FORM = {
    "ellps_wgs84": ("+proj=longlat +ellps=WGS84", TOKYO),
    "compound_code": ("EPSG:4326+5773", TOKYO),
    "compound_name": ("WGS 84 + EGM96 height", TOKYO),
    "nad83": ("NAD83", TOKYO),
    "proj_nad83": ("+proj=longlat +datum=NAD83", TOKYO),
    "grs80": ("+proj=longlat +ellps=GRS80", TOKYO),
    "tokyo": ("Tokyo", TOKYO),
    "utm31": ("+proj=utm +zone=31 +datum=WGS84", UTM31),
}

COMMANDS = {
    "add-h3": ("add", "h3", "--resolution", "7"),
    "add-quadkey": ("add", "quadkey"),
    "add-s2": ("add", "s2"),
    "add-a5": ("add", "a5"),
    "aggregate-h3": ("process", "aggregate", "h3", "--resolution", "7"),
    "convert-reproject": ("convert", "reproject"),
}


@pytest.fixture(scope="module")
def files(tmp_path_factory) -> dict[str, Path]:
    root = tmp_path_factory.mktemp("free_form_crs")
    con = get_duckdb_connection(load_spatial=True)
    out = {}
    try:
        for name, (crs, points) in FREE_FORM.items():
            path = root / f"{name}.parquet"
            con.execute(
                "COPY (SELECT id, ST_SetCRS(ST_GeomFromText(wkt), "
                f"'{_escape_sql_string(crs)}') AS geometry FROM {points} ORDER BY id) "
                f"TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')"
            )
            out[name] = path
    finally:
        con.close()
    return out


def _run(*args: str) -> None:
    result = CliRunner().invoke(cli, list(args))
    assert result.exit_code == 0, result.output


def _cli_output(command, src: Path, tmp_path: Path, suffix: str = ".parquet") -> Path:
    group, *rest = command
    out = tmp_path / f"out{suffix}"
    head = [group, rest[0]] if group != "process" else [group, rest[0], rest[1]]
    tail = rest[1:] if group != "process" else rest[2:]
    _run(*head, str(src), str(out), *tail)
    return out


@pytest.mark.parametrize("command", COMMANDS.values(), ids=COMMANDS.keys())
@pytest.mark.parametrize("name", FREE_FORM)
def test_every_transform_path_accepts_the_string(name, command, files, tmp_path):
    assert pq.read_table(str(_cli_output(command, files[name], tmp_path))).num_rows > 0


@pytest.mark.parametrize("name", FREE_FORM)
def test_convert_geojson_accepts_the_string(name, files, tmp_path):
    _run("convert", "geojson", str(files[name]), str(tmp_path / "out.geojson"))


def _by_id(table, column: str) -> list:
    return sorted(
        zip(table.column("id").to_pylist(), table.column(column).to_pylist(), strict=True)
    )


@pytest.mark.parametrize("name", FREE_FORM)
def test_add_h3_cli_matches_the_table_api(name, files, tmp_path):
    cli_out = pq.read_table(str(_cli_output(COMMANDS["add-h3"], files[name], tmp_path)))
    api_out = gpio.read(str(files[name])).add_h3(resolution=7).to_arrow()
    assert _by_id(cli_out, "h3_cell") == _by_id(api_out, "h3_cell")


def _coords(table) -> list:
    points = [shapely.from_wkb(g) for g in table.column("geometry").to_pylist()]
    ids = table.column("id").to_pylist()
    return [(x, y) for _, x, y in sorted((i, p.x, p.y) for i, p in zip(ids, points, strict=True))]


@pytest.mark.parametrize("name", FREE_FORM)
def test_convert_reproject_cli_matches_the_table_api(name, files, tmp_path):
    cli_out = pq.read_table(str(_cli_output(COMMANDS["convert-reproject"], files[name], tmp_path)))
    api_out = gpio.read(str(files[name])).reproject(target_crs="EPSG:4326").to_arrow()
    for got, want in zip(_coords(cli_out), _coords(api_out), strict=True):
        assert got == pytest.approx(want, abs=1e-6)
