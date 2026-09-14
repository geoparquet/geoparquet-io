"""CLI and Python API must resolve "auto" GeoParquet version identically (todo 043).

Before the fix, ``gpio.read(pgo_file).write(out)`` emitted geo metadata claiming
``version: 1.1.0, encoding: WKB`` while the column kept the native GEOMETRY
logical type — an internally inconsistent hybrid — and diverged from the CLI,
which upgrades native-geo-only inputs to 2.0.
"""

import pytest

import geoparquet_io as gpio
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from tests.conftest import get_geo_metadata, get_geoparquet_version, has_native_geo_types


def _write_native(path, version_option):
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('POINT (30 10)')),
            (2, ST_GeomFromText('POINT (40 40)'))
          ) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION '{version_option}')
    """)
    con.close()


@pytest.fixture
def pgo_input(tmp_path):
    path = tmp_path / "pgo.parquet"
    _write_native(path, "NONE")
    assert get_geoparquet_version(str(path)) is None
    assert has_native_geo_types(str(path))
    return path


@pytest.fixture
def v2_input(tmp_path):
    path = tmp_path / "v2.parquet"
    _write_native(path, "V2")
    assert get_geoparquet_version(str(path)) == "2.0.0"
    return path


def _assert_self_consistent(path):
    """Native logical types require 2.x metadata (or none); 1.x means WKB blobs."""
    version = get_geoparquet_version(str(path))
    native = has_native_geo_types(str(path))
    if version and version.startswith("1."):
        assert not native, f"1.x geo metadata ({version}) but native GEOMETRY logical type"
    if native and version:
        assert version.startswith("2."), f"native logical type but metadata claims {version}"


def test_pgo_input_api_matches_cli(pgo_input, tmp_path):
    cli_out = tmp_path / "cli.parquet"
    convert_to_geoparquet(str(pgo_input), str(cli_out), skip_hilbert=True, geoparquet_version=None)

    api_out = tmp_path / "api.parquet"
    gpio.read(pgo_input).write(api_out)

    assert get_geoparquet_version(str(cli_out)) == "2.0.0"
    assert get_geoparquet_version(str(api_out)) == "2.0.0"
    assert has_native_geo_types(str(api_out))
    _assert_self_consistent(cli_out)
    _assert_self_consistent(api_out)


def test_v2_input_api_matches_cli(v2_input, tmp_path):
    cli_out = tmp_path / "cli.parquet"
    convert_to_geoparquet(str(v2_input), str(cli_out), skip_hilbert=True, geoparquet_version=None)

    api_out = tmp_path / "api.parquet"
    gpio.read(v2_input).write(api_out)

    assert get_geoparquet_version(str(cli_out)) == "2.0.0"
    assert get_geoparquet_version(str(api_out)) == "2.0.0"
    _assert_self_consistent(cli_out)
    _assert_self_consistent(api_out)


def test_api_explicit_v1_1_is_true_wkb(pgo_input, tmp_path):
    """Explicit 1.1 must produce a real 1.1 WKB file, not a hybrid."""
    out = tmp_path / "api11.parquet"
    gpio.read(pgo_input).write(out, geoparquet_version="1.1")
    assert get_geoparquet_version(str(out)) == "1.1.0"
    assert not has_native_geo_types(str(out))
    geo = get_geo_metadata(str(out))
    assert geo["columns"]["geometry"]["encoding"] == "WKB"
