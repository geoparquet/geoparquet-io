"""Compound CRS handling when ``--force-2d`` drops Z (``gpio convert geoparquet``).

The Estonian Topographic Database shapefiles declare "EST97 + EVRF2007 height"
(EPSG:3301 + EPSG:5621). After ``--force-2d`` the output still carried the
CompoundCRS, which has no authority id of its own, so ``crs_string_for_transform``
returned None and ``gpio pmtiles create`` handed metre coordinates to Tippecanoe
as lon/lat: every tile collapsed at the pole, silently.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.convert import convert_to_geoparquet, read_spatial_to_arrow


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


# --- compound CRS: --force-2d keeps only the horizontal component ---------------

COMPOUND_CRS = {
    "$schema": "https://proj.org/schemas/v0.5/projjson.schema.json",
    "type": "CompoundCRS",
    "name": "Estonian Coordinate System of 1997 + EVRF2007 height",
    "components": [
        {
            "type": "ProjectedCRS",
            "name": "Estonian Coordinate System of 1997",
            "id": {"authority": "EPSG", "code": 3301},
        },
        {
            "type": "VerticalCRS",
            "name": "EVRF2007 height",
            "id": {"authority": "EPSG", "code": 5621},
        },
    ],
}


def test_horizontal_crs_reduces_compound_to_projected_component():
    from geoparquet_io.core.crs_utils import horizontal_crs

    reduced = horizontal_crs(COMPOUND_CRS)
    assert reduced["type"] == "ProjectedCRS"
    assert reduced["id"] == {"authority": "EPSG", "code": 3301}
    assert reduced["$schema"] == COMPOUND_CRS["$schema"]


@pytest.mark.parametrize(
    "crs",
    [
        None,
        "EPSG:3301",
        {"type": "ProjectedCRS", "id": {"authority": "EPSG", "code": 3301}},
        {"type": "CompoundCRS", "components": "not-a-list"},
        {
            "type": "CompoundCRS",
            "components": [
                {"type": "ProjectedCRS", "id": {"authority": "EPSG", "code": 3301}},
                {"type": "GeographicCRS", "id": {"authority": "EPSG", "code": 4326}},
            ],
        },
    ],
)
def test_horizontal_crs_passes_through_anything_not_reducible(crs):
    from geoparquet_io.core.crs_utils import horizontal_crs

    assert horizontal_crs(crs) is crs


def test_compound_crs_is_identified_by_its_horizontal_component():
    from geoparquet_io.core.crs_utils import _extract_crs_identifier, crs_string_for_transform

    assert _extract_crs_identifier(COMPOUND_CRS) == ("EPSG", 3301)
    assert crs_string_for_transform(COMPOUND_CRS) == "EPSG:3301"


@pytest.fixture
def compound_crs_gpkg(tmp_path: Path) -> Path:
    """3D points in EPSG:3301 + EVRF2007 height (EPSG:5621), as GDAL writes them."""
    path = tmp_path / "compound.gpkg"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(
        f"""
        COPY (
            SELECT i AS id, ST_GeomFromText(
                'POINT Z (' || (500000 + i * 100) || ' ' || (6500000 + i * 100) || ' 10)'
            ) AS geom
            FROM range(3) t(i)
        ) TO '{path}' WITH (FORMAT GDAL, DRIVER 'GPKG', SRS 'EPSG:3301+5621')
        """
    )
    return path


def _crs_of(parquet: Path) -> dict:
    geo = json.loads(pq.read_metadata(parquet).metadata[b"geo"])
    return geo["columns"][geo["primary_column"]]["crs"]


def test_convert_without_force_2d_keeps_compound_crs(compound_crs_gpkg: Path, tmp_path: Path):
    out = tmp_path / "kept.parquet"
    convert_to_geoparquet(str(compound_crs_gpkg), str(out), verbose=False)
    assert _has_z(out)
    assert _crs_of(out)["type"] == "CompoundCRS"


def test_convert_force_2d_drops_vertical_crs_component(compound_crs_gpkg: Path, tmp_path: Path):
    out = tmp_path / "flat.parquet"
    convert_to_geoparquet(str(compound_crs_gpkg), str(out), verbose=False, force_2d=True)
    assert not _has_z(out)
    crs = _crs_of(out)
    assert crs["type"] == "ProjectedCRS"
    assert crs["id"] == {"authority": "EPSG", "code": 3301}


def test_cli_force_2d_drops_vertical_crs_component(compound_crs_gpkg: Path, tmp_path: Path):
    out = tmp_path / "flat_cli.parquet"
    result = CliRunner().invoke(
        cli, ["convert", "geoparquet", str(compound_crs_gpkg), str(out), "--force-2d"]
    )
    assert result.exit_code == 0, result.output
    assert not _has_z(out)
    assert _crs_of(out)["type"] == "ProjectedCRS"


def test_read_spatial_to_arrow_force_2d_returns_horizontal_crs(compound_crs_gpkg: Path):
    _table, crs, _col = read_spatial_to_arrow(str(compound_crs_gpkg), force_2d=True)
    assert crs["type"] == "ProjectedCRS"
    assert crs["id"] == {"authority": "EPSG", "code": 3301}
