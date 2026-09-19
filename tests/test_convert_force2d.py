"""Z/M dropping on ``gpio convert geoparquet`` (``--force-2d``).

Every layer of the Estonian Topographic Database (2026-09-19) is 3D (POINT Z /
LINESTRING Z / POLYGON Z). Conversion and tiling succeeded, but downstream
rendering of the tiles produced empty images, and there was no way to ask for
2D output.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.convert import (
    convert_to_geoparquet,
    force_2d_expr,
    read_spatial_to_arrow,
)

DATA = Path(__file__).parent / "data"


@pytest.fixture
def geojson_3d(tmp_path: Path) -> Path:
    features = [
        {
            "type": "Feature",
            "properties": {"id": i},
            "geometry": {"type": "Point", "coordinates": [10.0 + i, 50.0 + i, 100.0 + i]},
        }
        for i in range(3)
    ]
    path = tmp_path / "points3d.geojson"
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
    return path


def _geom_col(parquet: Path) -> str:
    geo = json.loads(pq.read_metadata(parquet).metadata[b"geo"])
    return geo["primary_column"]


def _has_z(parquet: Path) -> bool:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    col = _geom_col(parquet)
    (column_type,) = con.execute(
        f"SELECT column_type FROM (DESCRIBE SELECT \"{col}\" FROM read_parquet('{parquet}'))"
    ).fetchone()
    geom = f'"{col}"' if column_type.upper().startswith("GEOMETRY") else f'ST_GeomFromWKB("{col}")'
    (has_z,) = con.execute(
        f"SELECT bool_or(ST_HasZ({geom})) FROM read_parquet('{parquet}')"
    ).fetchone()
    return bool(has_z)


class TestForce2D:
    def test_force_2d_expr_wraps_the_source(self):
        assert force_2d_expr("ST_Read('a.shp')", "geom") == (
            '(SELECT * REPLACE (ST_Force2D("geom") AS "geom") FROM ST_Read(\'a.shp\'))'
        )

    def test_3d_source_keeps_z_by_default(self, geojson_3d, tmp_path):
        out = tmp_path / "z.parquet"
        convert_to_geoparquet(str(geojson_3d), str(out))
        assert _has_z(out)

    def test_force_2d_drops_z(self, geojson_3d, tmp_path):
        out = tmp_path / "flat.parquet"
        convert_to_geoparquet(str(geojson_3d), str(out), force_2d=True)
        assert not _has_z(out)
        assert pq.read_metadata(out).num_rows == 3

    def test_force_2d_on_read_api(self, geojson_3d):
        table, _crs, _geom = read_spatial_to_arrow(str(geojson_3d), force_2d=True)
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.register("t", table)
        (has_z,) = con.execute(
            "SELECT bool_or(ST_HasZ(ST_GeomFromWKB(geometry))) FROM t"
        ).fetchone()
        assert not has_z

    @pytest.mark.parametrize("version", ["1.1", "2.0"])
    def test_force_2d_on_parquet_input(self, geojson_3d, tmp_path, version):
        first = tmp_path / "z.parquet"
        convert_to_geoparquet(str(geojson_3d), str(first), geoparquet_version=version)
        assert _has_z(first)
        second = tmp_path / "flat.parquet"
        convert_to_geoparquet(str(first), str(second), force_2d=True, geoparquet_version=version)
        assert not _has_z(second)


class TestCli:
    def test_cli_force_2d_option(self, geojson_3d, tmp_path):
        out = tmp_path / "cli.parquet"
        result = CliRunner().invoke(
            cli, ["convert", "geoparquet", str(geojson_3d), str(out), "--force-2d"]
        )
        assert result.exit_code == 0, result.output
        assert not _has_z(out)
