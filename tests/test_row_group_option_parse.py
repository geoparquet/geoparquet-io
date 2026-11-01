"""``--row-group-size-mb`` is parsed once, before any core call (HANDOFFS row 78).

``process simplify``, ``polygonize`` and ``contour`` handed the option's raw
string to the core, which compared it with a number and raised ``TypeError``.
Every command with ``@row_group_options`` parses through
``decorators.parse_row_group_options``; these run the real CLI.
"""

from __future__ import annotations

from importlib.util import find_spec
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli

BUILDINGS = Path(__file__).parent / "data" / "buildings_test.parquet"

_raster = pytest.mark.skipif(
    find_spec("rasterio") is None, reason="requires the raster extra (rasterio)"
)
_contour = pytest.mark.skipif(
    find_spec("contourrs") is None, reason="requires the raster extra (contourrs)"
)


def _dem(tmp_path: Path) -> Path:
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    dem = np.fromfunction(lambda y, x: 10.0 * (x // 3), (12, 12)).astype(np.float32)
    tif = tmp_path / "dem.tif"
    with rasterio.open(
        str(tif),
        "w",
        driver="GTiff",
        height=12,
        width=12,
        count=1,
        dtype="float32",
        crs="EPSG:32633",
        transform=from_origin(0, 120, 10, 10),
    ) as dst:
        dst.write(dem, 1)
    return tif


def _args(command: str, tmp_path: Path, out: Path) -> list[str]:
    if command == "simplify":
        return ["process", "simplify", str(BUILDINGS), str(out), "--tolerance", "0.00001"]
    if command == "polygonize":
        return ["process", "polygonize", str(_dem(tmp_path)), str(out)]
    return ["process", "contour", str(_dem(tmp_path)), str(out), "--interval", "10"]


COMMANDS = [
    pytest.param("simplify"),
    pytest.param("polygonize", marks=[_raster, _contour]),
    pytest.param("contour", marks=[_raster, _contour]),
]


@pytest.mark.parametrize("command", COMMANDS)
def test_a_byte_target_is_parsed_before_the_core_sees_it(command, tmp_path):
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli, [*_args(command, tmp_path, out), "--row-group-size-mb", "0.05"]
    )
    assert result.exit_code == 0, f"{result.output}\n{result.exception!r}"
    assert pq.read_metadata(out).num_rows > 0


@pytest.mark.parametrize("command", ["simplify"])
def test_both_row_group_options_are_refused_together(command, tmp_path):
    out = tmp_path / "out.parquet"
    args = [*_args(command, tmp_path, out), "--row-group-size", "10", "--row-group-size-mb", "1"]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 2, result.output
    assert "--row-group-size" in result.output and "--row-group-size-mb" in result.output
