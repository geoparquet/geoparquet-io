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

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core import convert as convert_mod
from geoparquet_io.core.convert import (
    _bounds_with_curve_fallback,
    _read_spatial_to_arrow,
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


def _has_z(parquet: Path, col: str | None = None) -> bool:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    col = col or _geom_col(parquet)
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
        assert not _table_has_z(table)

    def test_force_2d_on_public_api(self, geojson_3d):
        table = gpio.convert(str(geojson_3d), force_2d=True)
        assert not _table_has_z(table.to_arrow())

    @pytest.mark.parametrize("version", ["1.1", "2.0"])
    def test_force_2d_on_read_api_with_parquet_input(self, geojson_3d, tmp_path, version):
        """DuckDB hands a GeoParquet column back as GEOMETRY, not BLOB; both must bind."""
        first = tmp_path / "z.parquet"
        convert_to_geoparquet(str(geojson_3d), str(first), geoparquet_version=version)
        table, _crs, _geom = read_spatial_to_arrow(str(first), force_2d=True)
        assert table.num_rows == 3
        assert not _table_has_z(table)

    def test_force_2d_flattens_secondary_geometry_columns(self, tmp_path):
        source = tmp_path / "two_geoms.parquet"
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(
            f"""
            COPY (
                SELECT i AS id,
                       ST_GeomFromText('POINT Z (' || i || ' ' || i || ' 9)') AS geometry,
                       ST_GeomFromText('POINT Z (' || i || ' ' || i || ' 9)') AS centroid
                FROM range(3) t(i)
            ) TO '{source}' (FORMAT PARQUET)
            """
        )
        geo = json.loads(pq.read_metadata(source).metadata[b"geo"])
        assert set(geo["columns"]) == {"geometry", "centroid"}, "fixture must carry two columns"
        out = tmp_path / "flat.parquet"
        convert_to_geoparquet(str(source), str(out), force_2d=True)
        assert not _has_z(out, "geometry")
        assert not _has_z(out, "centroid")
        geo = json.loads(pq.read_metadata(out).metadata[b"geo"])
        assert geo["columns"]["geometry"]["geometry_types"] == ["Point"]
        assert geo["columns"]["centroid"]["geometry_types"] == ["Point"]
        assert len(geo["columns"]["centroid"].get("bbox", [])) in (0, 4)

    def test_flatten_geometry_metadata_corrects_declared_dimensions(self):
        from geoparquet_io.core.convert import _flatten_geometry_metadata

        meta = _flatten_geometry_metadata(
            {
                "encoding": "WKB",
                "geometry_types": ["Point Z", "LineString ZM", "Polygon M", "Point"],
                "bbox": [0, 1, 2, 3, 4, 5],
                "crs": {"id": {"authority": "EPSG", "code": 3301}},
            }
        )
        assert meta["geometry_types"] == ["Point", "LineString", "Polygon"]
        assert meta["bbox"] == [0, 1, 3, 4]
        assert meta["crs"] == {"id": {"authority": "EPSG", "code": 3301}}


class TestForce2DOnOtherInputs:
    def test_force_2d_on_a_curved_source_goes_through_the_linearized_read(self):
        table, _crs, geom = read_spatial_to_arrow(
            str(DATA / "curved_geometry_test.gpkg"), force_2d=True
        )
        assert geom == "geometry"
        assert table.num_rows > 0
        assert not _table_has_z(table)

    def test_force_2d_refuses_a_geoarrow_encoded_parquet(self, geojson_3d, tmp_path):
        """ST_Force2D needs WKB (or native GEOMETRY); a GeoArrow struct column is neither."""
        from geoparquet_io.core.exceptions import GeoParquetError

        geoarrow = tmp_path / "geoarrow.parquet"
        convert_to_geoparquet(str(geojson_3d), str(geoarrow), geoparquet_version="1.1-geoarrow")
        # The encoding is only known once the geo block is read, mid-conversion,
        # so this one arrives wrapped by convert_to_geoparquet's handler.
        with pytest.raises(GeoParquetError, match="must be WKB"):
            convert_to_geoparquet(str(geoarrow), str(tmp_path / "flat.parquet"), force_2d=True)


def _table_has_z(table) -> bool:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.register("t", table)
    (has_z,) = con.execute("SELECT bool_or(ST_HasZ(ST_GeomFromWKB(geometry))) FROM t").fetchone()
    return bool(has_z)


class TestCsv:
    @pytest.fixture
    def csv_3d(self, tmp_path: Path) -> Path:
        path = tmp_path / "points.csv"
        path.write_text("id,geom\n1,POINT Z (1 2 3)\n2,POINT Z (4 5 6)\n")
        return path

    def test_csv_wkt_keeps_z_by_default(self, csv_3d, tmp_path):
        out = tmp_path / "z.parquet"
        convert_to_geoparquet(str(csv_3d), str(out))
        assert _has_z(out)

    @pytest.mark.parametrize("skip_invalid", [False, True])
    def test_force_2d_drops_z_from_csv_wkt(self, csv_3d, tmp_path, skip_invalid):
        out = tmp_path / "flat.parquet"
        convert_to_geoparquet(str(csv_3d), str(out), force_2d=True, skip_invalid=skip_invalid)
        assert not _has_z(out)
        geo = json.loads(pq.read_metadata(out).metadata[b"geo"])
        assert geo["columns"]["geometry"]["geometry_types"] == ["Point"]

    @pytest.mark.parametrize("skip_invalid", [False, True])
    def test_force_2d_on_csv_read_api(self, csv_3d, skip_invalid):
        table, _crs, _geom = read_spatial_to_arrow(
            str(csv_3d), force_2d=True, skip_invalid=skip_invalid
        )
        assert table.num_rows == 2
        assert not _table_has_z(table)


class TestFallbacksKeepForce2D:
    CURVE_ERROR = "Unsupported geometry type in WKB"

    def test_arrow_read_fallback_forwards_force_2d(self, monkeypatch):
        class Con:
            def execute(self, query):
                raise duckdb.InvalidInputException(TestFallbacksKeepForce2D.CURVE_ERROR)

        calls = []

        def fake_linearized(con, input_file, layer, geom_column, max_angle_deg=None, **kwargs):
            calls.append(kwargs)
            return "table"

        monkeypatch.setattr(convert_mod, "_detect_geometry_column", lambda *a, **k: "geom")
        monkeypatch.setattr(convert_mod, "_choose_read_strategy", lambda *a, **k: "normal")
        monkeypatch.setattr(convert_mod, "_read_spatial_linearized", fake_linearized)
        assert _read_spatial_to_arrow(Con(), "a.gdb", False, force_2d=True) == "table"
        assert calls == [{"open_options": None, "force_2d": True}]

    def test_bounds_fallback_rewraps_the_linearized_view(self, monkeypatch):
        seen = []

        def fake_bounds(con, input_file, geom_column, verbose, **kwargs):
            seen.append(kwargs["table_expr"])
            if len(seen) == 1:
                raise duckdb.InvalidInputException(self.CURVE_ERROR)
            return (0, 0, 1, 1)

        monkeypatch.setattr(convert_mod, "_calculate_bounds", fake_bounds)
        monkeypatch.setattr(
            convert_mod, "_register_linearized_view", lambda *a, **k: "_gpio_linearized"
        )
        _bounds, table_expr = _bounds_with_curve_fallback(
            None,
            "a.gdb",
            "geom",
            False,
            is_parquet=False,
            encoding="WKB",
            table_expr=force_2d_expr("ST_Read('a.gdb')", "geom"),
            layer=None,
            linearize_curves=True,
            max_angle_deg=None,
            already_linearized=False,
            force_2d=True,
        )
        assert table_expr == force_2d_expr("_gpio_linearized", "geom")
        assert seen[1] == table_expr

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
