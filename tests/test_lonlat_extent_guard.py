"""#1263: coordinates that are not lon/lat fail loudly instead of shipping an empty map.

Two defences behind the CRS fix of #1258:

* ``convert geojson`` that does not reproject checks the input extent and
  refuses one outside lon/lat range, suggesting ``--src-crs`` on ``convert
  reproject``; ``--keep-crs`` skips the check because that output is not
  RFC 7946 by request.
* ``pmtiles create`` reads the header of the archive it just wrote and fails,
  removing the archive, when it addresses no tiles although the input had rows.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pytest
from click.testing import CliRunner
from pmtiles.tile import Compression, TileType, deserialize_header, serialize_header, zxy_to_tileid
from pmtiles.writer import Writer

from geoparquet_io.cli.main import cli
from tests._defrag_fixtures import geo_block, point_wkb, write_with_geo


def _run(*args):
    return CliRunner().invoke(cli, [str(a) for a in args], catch_exceptions=True)


def _projected_points(tmp_path, *, crs):
    """Points a UTM-sized distance from the origin; ``crs`` is the block's claim."""
    table = pa.table(
        {
            "id": pa.array([0, 1], pa.int64()),
            "geometry": pa.array(
                [point_wkb(500000.0, 4000000.0), point_wkb(500100.0, 4000100.0)], pa.binary()
            ),
        }
    )
    block = geo_block(crs=crs)
    return write_with_geo(tmp_path / "projected.parquet", table, block)


def _lonlat_points(tmp_path):
    table = pa.table(
        {
            "id": pa.array([0, 1], pa.int64()),
            "geometry": pa.array([point_wkb(-122.4, 37.8), point_wkb(-122.3, 37.9)], pa.binary()),
        }
    )
    return write_with_geo(tmp_path / "lonlat.parquet", table, geo_block())


# ---------------------------------------------------------------------------
# convert geojson
# ---------------------------------------------------------------------------


def test_geojson_refuses_an_extent_outside_lon_lat_when_the_crs_is_unknown(tmp_path):
    src = _projected_points(tmp_path, crs=None)  # "crs": null -- unknown, so no reprojection
    out = tmp_path / "out.geojson"
    result = _run("convert", "geojson", src, out)
    assert result.exit_code != 0, result.output
    assert "--src-crs" in result.output
    assert "convert reproject" in result.output
    assert not out.exists()


def test_keep_crs_skips_the_extent_check(tmp_path):
    src = _projected_points(tmp_path, crs=None)
    out = tmp_path / "out.geojson"
    result = _run("convert", "geojson", src, out, "--keep-crs")
    assert result.exit_code == 0, result.output
    coords = json.loads(out.read_text())["features"][0]["geometry"]["coordinates"]
    assert coords[0] == 500000.0


def test_lon_lat_data_converts_as_before(tmp_path):
    src = _lonlat_points(tmp_path)
    out = tmp_path / "out.geojson"
    result = _run("convert", "geojson", src, out)
    assert result.exit_code == 0, result.output
    assert len(json.loads(out.read_text())["features"]) == 2


def test_a_declared_projected_crs_is_still_reprojected_not_refused(tmp_path):
    from pyproj import CRS

    src = _projected_points(tmp_path, crs=CRS.from_epsg(32610).to_json_dict())
    out = tmp_path / "out.geojson"
    result = _run("convert", "geojson", src, out)
    assert result.exit_code == 0, result.output
    lon, lat = json.loads(out.read_text())["features"][0]["geometry"]["coordinates"]
    assert -180 <= lon <= 180 and -90 <= lat <= 90


def test_the_streaming_geojson_path_refuses_it_too(tmp_path):
    src = _projected_points(tmp_path, crs=None)
    result = _run("convert", "geojson", src)  # GeoJSONSeq to stdout
    assert result.exit_code != 0, result.output
    assert "--src-crs" in result.output


def _arrow_stream_bytes(path) -> bytes:
    import pyarrow.ipc as ipc
    import pyarrow.parquet as pq

    table = pq.read_table(str(path))
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def test_the_stdin_stream_path_refuses_it_too(tmp_path):
    src = _projected_points(tmp_path, crs=None)
    result = CliRunner().invoke(
        cli,
        ["convert", "geojson", "-", str(tmp_path / "o.geojson")],
        input=_arrow_stream_bytes(src),
    )
    assert result.exit_code != 0, result.output
    assert "--src-crs" in result.output
    assert not (tmp_path / "o.geojson").exists()


def test_the_stdin_stream_path_keep_crs_and_lonlat_still_work(tmp_path):
    out = tmp_path / "o.geojson"
    src = _projected_points(tmp_path, crs=None)
    result = CliRunner().invoke(
        cli, ["convert", "geojson", "-", str(out), "--keep-crs"], input=_arrow_stream_bytes(src)
    )
    assert result.exit_code == 0, result.output
    out2 = tmp_path / "o2.geojson"
    ll = _lonlat_points(tmp_path)
    result = CliRunner().invoke(
        cli, ["convert", "geojson", "-", str(out2)], input=_arrow_stream_bytes(ll)
    )
    assert result.exit_code == 0, result.output


def test_table_to_geojson_refuses_it_too(tmp_path):
    import geoparquet_io as gpio
    from geoparquet_io.core.exceptions import GeoParquetError

    table = gpio.read(str(_projected_points(tmp_path, crs=None)))
    with pytest.raises(GeoParquetError, match="--src-crs"):
        table.to_geojson(str(tmp_path / "t.geojson"))
    assert not (tmp_path / "t.geojson").exists()


# ---------------------------------------------------------------------------
# pmtiles create
# ---------------------------------------------------------------------------


def _write_archive(path, tiles: int) -> None:
    """A real PMTiles archive; ``tiles=0`` has the header of an empty one."""
    with open(path, "wb") as handle:
        writer = Writer(handle)
        writer.write_tile(zxy_to_tileid(0, 0, 0), b"tile")
        writer.finalize(
            {
                "tile_type": TileType.MVT,
                "tile_compression": Compression.GZIP,
                "min_lon_e7": -1800000000,
                "min_lat_e7": -850000000,
                "max_lon_e7": 1800000000,
                "max_lat_e7": -850000000,
                "center_zoom": 0,
                "center_lon_e7": 0,
                "center_lat_e7": 0,
            },
            {},
        )
    if tiles == 0:
        raw = path.read_bytes()
        header = deserialize_header(raw[:127])
        header["addressed_tiles_count"] = 0
        path.write_bytes(serialize_header(header) + raw[127:])


@pytest.fixture
def fake_tippecanoe(monkeypatch):
    """Stand in for the tippecanoe subprocess pipeline: it writes the archive asked for."""
    from geoparquet_io.core import pmtiles as pm

    produced = {"tiles": 0}

    def pipeline(*args, **kwargs):
        from pathlib import Path

        tippecanoe_cmd = args[1]
        _write_archive(Path(tippecanoe_cmd[tippecanoe_cmd.index("-o") + 1]), produced["tiles"])

    monkeypatch.setattr(pm, "_check_tippecanoe", lambda: True)
    monkeypatch.setattr(pm, "_run_pipeline", pipeline)
    return produced


def test_an_archive_with_no_tiles_from_a_non_empty_input_is_an_error(tmp_path, fake_tippecanoe):
    src = _lonlat_points(tmp_path)
    out = tmp_path / "out.pmtiles"
    fake_tippecanoe["tiles"] = 0
    result = _run("pmtiles", "create", src, out)
    assert result.exit_code != 0, result.output
    assert "no tiles" in result.output.lower()
    assert "--src-crs" in result.output
    assert not out.exists()


def test_an_archive_with_tiles_is_kept(tmp_path, fake_tippecanoe):
    src = _lonlat_points(tmp_path)
    out = tmp_path / "out.pmtiles"
    fake_tippecanoe["tiles"] = 1
    result = _run("pmtiles", "create", src, out)
    assert result.exit_code == 0, result.output
    assert out.exists()


def test_an_empty_input_still_makes_an_empty_archive(tmp_path, fake_tippecanoe):
    table = pa.table({"id": pa.array([], pa.int64()), "geometry": pa.array([], pa.binary())})
    src = write_with_geo(tmp_path / "empty.parquet", table, geo_block())
    out = tmp_path / "out.pmtiles"
    fake_tippecanoe["tiles"] = 0
    result = _run("pmtiles", "create", src, out)
    assert result.exit_code == 0, result.output
    assert out.exists()
