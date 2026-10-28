"""``add geometry-metrics`` measures projected input instead of writing NaN (#1193).

``ST_Area_Spheroid`` / ``ST_Perimeter_Spheroid`` read their input as lon/lat
degrees. A projected input (EPSG:3857, UTM) handed them metres and the command
reported success over a column of NaN. The primary geometry's CRS is now read
with the same resolver every other command uses, and a non-default CRS is
transformed to OGC:CRS84 before measuring.

The reference shape is a 1 x 1 degree square at the equator: 12,308,778,361 m²
and 443,770.9 m of perimeter on the WGS84 spheroid.
"""

from __future__ import annotations

import io
import json
import math

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.add.geometry_metrics import AREA_COLUMN, PERIMETER_COLUMN
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

AREA_M2 = 12_308_778_361.0
PERIMETER_M = 443_770.9
SQUARE = "ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')"


def _write_square(path, crs: str, geoparquet_version: str) -> None:
    geom = f"ST_SetCRS({SQUARE}, 'OGC:CRS84')"
    if crs != "OGC:CRS84":
        geom = f"ST_SetCRS(ST_Transform({geom}, 'OGC:CRS84', '{crs}'), '{crs}')"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"COPY (SELECT 1 AS id, {geom} AS geometry) TO {sql_path(str(path))} "
            f"(FORMAT PARQUET, GEOPARQUET_VERSION '{geoparquet_version}')"
        )
    finally:
        con.close()


def _metrics(path) -> tuple[float, float]:
    table = pq.read_table(str(path), columns=[AREA_COLUMN, PERIMETER_COLUMN])
    return table.column(AREA_COLUMN)[0].as_py(), table.column(PERIMETER_COLUMN)[0].as_py()


def _assert_reference(area: float, perimeter: float) -> None:
    assert math.isfinite(area) and math.isfinite(perimeter), (area, perimeter)
    assert area == pytest.approx(AREA_M2, rel=1e-3)
    assert perimeter == pytest.approx(PERIMETER_M, rel=1e-3)


_CRS_CASES = ["OGC:CRS84", "EPSG:3857", "EPSG:32631"]


@pytest.mark.parametrize("version", ["V1", "NONE"], ids=["geo-block", "native-only"])
@pytest.mark.parametrize("crs", _CRS_CASES)
def test_file_metrics_match_wgs84_reference(crs, version, tmp_path):
    src = tmp_path / "in.parquet"
    out = tmp_path / "out.parquet"
    _write_square(src, crs, version)
    result = CliRunner().invoke(cli, ["add", "geometry-metrics", str(src), str(out)])
    assert result.exit_code == 0, result.output
    _assert_reference(*_metrics(out))


@pytest.mark.parametrize("crs", _CRS_CASES)
def test_streaming_metrics_match_wgs84_reference(crs, tmp_path):
    src = tmp_path / "in.parquet"
    _write_square(src, crs, "V1")
    result = CliRunner().invoke(cli, ["add", "geometry-metrics", str(src), "-"])
    assert result.exit_code == 0, result.output
    table = pa.ipc.open_stream(io.BytesIO(result.stdout_bytes)).read_all()
    _assert_reference(
        table.column(AREA_COLUMN)[0].as_py(), table.column(PERIMETER_COLUMN)[0].as_py()
    )


def test_dry_run_shows_the_transform(tmp_path):
    src = tmp_path / "in.parquet"
    _write_square(src, "EPSG:3857", "V1")
    result = CliRunner().invoke(
        cli, ["add", "geometry-metrics", str(src), "x.parquet", "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    assert "ST_Transform" in result.output


def test_api_metrics_on_projected_input(tmp_path):
    import geoparquet_io as gpio

    src = tmp_path / "in.parquet"
    _write_square(src, "EPSG:3857", "V1")
    table = gpio.read(str(src)).add_geometry_metrics().to_arrow()
    _assert_reference(
        table.column(AREA_COLUMN)[0].as_py(), table.column(PERIMETER_COLUMN)[0].as_py()
    )


def test_unknown_crs_warns_and_measures_as_lonlat(tmp_path, caplog):
    """An explicit ``crs: null`` is unknown, not projected: warn, measure as lon/lat."""
    src = tmp_path / "in.parquet"
    _write_square(src, "OGC:CRS84", "V1")
    table = pq.read_table(str(src))
    geo = json.loads(table.schema.metadata[b"geo"])
    geo["columns"]["geometry"]["crs"] = None
    meta = {**table.schema.metadata, b"geo": json.dumps(geo).encode()}
    pq.write_table(table.replace_schema_metadata(meta), str(src))

    from geoparquet_io.core.crs_utils import reset_null_crs_warnings

    reset_null_crs_warnings()
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(cli, ["add", "geometry-metrics", str(src), str(out)])
    assert result.exit_code == 0, result.output
    assert "null CRS" in result.output + caplog.text
    _assert_reference(*_metrics(out))
