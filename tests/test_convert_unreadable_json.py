"""A JSON file that is not GeoJSON is a clean error, never a crash (#1102).

On DuckDB 1.5.5, ``ST_Read_Meta`` over a JSON file GDAL could not open as a
vector dataset (a bare array of records, a ``{"data": [...]}`` wrapper) killed
the process with SIGSEGV (duckdb/duckdb-spatial#860), so ``gpio convert`` died
with no message at all. DuckDB 1.5.6 fixes it and the dependency floor now
requires it, which leaves gpio's own error path to report the file instead.

The CLI cases run in a subprocess on purpose: a segfault inside ``CliRunner``
would take the whole pytest worker down rather than fail one test.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

import geoparquet_io as gpio
from geoparquet_io.core.exceptions import GeoParquetError

_PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"

_INPUTS = {
    "records": [{"id": 1, "lon": 1.0, "lat": 2.0}, {"id": 2, "lon": 3.0, "lat": 4.0}],
    "wrapped": {"data": [{"id": 1, "lon": 1.0, "lat": 2.0}]},
    "empty_feature_collection_typo": {"type": "FeatureColection", "features": []},
}


@pytest.fixture(params=sorted(_INPUTS))
def unreadable_json(request, tmp_path):
    path = tmp_path / f"{request.param}.json"
    path.write_text(json.dumps(_INPUTS[request.param]))
    return path


def _convert_cli(src: Path, dst: Path) -> subprocess.CompletedProcess:
    args = ["convert", "geoparquet", str(src), str(dst), "--allow-no-geometry", "--skip-hilbert"]
    return subprocess.run(
        [sys.executable, "-m", "geoparquet_io.cli.main", *args],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_cli_reports_the_file_instead_of_crashing(unreadable_json, tmp_path):
    result = _convert_cli(unreadable_json, tmp_path / "out.parquet")

    # A signal shows up as a negative return code (139 under a shell).
    assert result.returncode == 1, (result.returncode, result.stderr)
    assert "Error:" in result.stderr + result.stdout
    assert not (tmp_path / "out.parquet").exists()


def test_python_api_raises_a_gpio_error(unreadable_json):
    with pytest.raises(GeoParquetError):
        gpio.convert(str(unreadable_json))


def _declared_duckdb_floor() -> tuple[int, ...]:
    match = re.search(r'"duckdb>=([0-9.]+),', _PYPROJECT.read_text())
    assert match, "pyproject.toml no longer declares a duckdb floor"
    return tuple(int(part) for part in match.group(1).split("."))


def test_dependency_floor_is_past_the_st_read_meta_segfault():
    assert _declared_duckdb_floor() >= (1, 5, 6)


def test_running_duckdb_is_past_the_st_read_meta_segfault():
    version = tuple(int(part) for part in duckdb.__version__.split(".")[:3])
    assert version >= (1, 5, 6), f"DuckDB {duckdb.__version__} segfaults in ST_Read_Meta (#1102)"
