"""Tests for gpio process contour (core/process/raster/contour.py).

Level resolution and output finalization are dependency-free and run on
every CI leg; the tests that call contourrs/rasterio skip where the
``raster`` extra is not installed (contourrs needs Python >= 3.12).
"""

import json
from importlib.util import find_spec

import pyarrow as pa
import pytest

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.raster.contour import contour_array, resolve_levels

requires_contourrs = pytest.mark.skipif(
    find_spec("contourrs") is None, reason="requires the raster extra (contourrs)"
)
requires_rasterio = pytest.mark.skipif(
    find_spec("rasterio") is None, reason="requires the raster extra (rasterio)"
)


class TestResolveLevels:
    def test_explicit_levels_pass_through(self):
        assert resolve_levels([0.0, 100.0, 250.0], None, 0.0, 10.0, 200.0) == [
            0.0,
            100.0,
            250.0,
        ]

    def test_explicit_levels_must_increase(self):
        with pytest.raises(InvalidParameterError, match="increas"):
            resolve_levels([0.0, 100.0, 100.0], None, 0.0, 0.0, 1.0)

    def test_explicit_levels_need_at_least_two(self):
        with pytest.raises(InvalidParameterError, match="two"):
            resolve_levels([5.0], None, 0.0, 0.0, 1.0)

    def test_interval_brackets_the_band_range(self):
        levels = resolve_levels(None, 100.0, 0.0, 12.0, 340.0)
        assert levels == [0.0, 100.0, 200.0, 300.0, 400.0]

    def test_interval_with_base_offset(self):
        levels = resolve_levels(None, 100.0, 50.0, 12.0, 340.0)
        assert levels == [-50.0, 50.0, 150.0, 250.0, 350.0]

    def test_interval_must_be_positive(self):
        with pytest.raises(InvalidParameterError, match="interval"):
            resolve_levels(None, -5.0, 0.0, 0.0, 1.0)

    def test_levels_and_interval_are_exclusive(self):
        with pytest.raises(InvalidParameterError, match="both"):
            resolve_levels([0.0, 1.0], 1.0, 0.0, 0.0, 1.0)

    def test_one_of_them_is_required(self):
        with pytest.raises(InvalidParameterError, match="levels.*interval|interval.*levels"):
            resolve_levels(None, None, 0.0, 0.0, 1.0)

    def test_flat_raster_still_gets_a_band(self):
        levels = resolve_levels(None, 10.0, 0.0, 25.0, 25.0)
        assert len(levels) >= 2
        assert levels[0] <= 25.0 <= levels[-1]


@requires_contourrs
class TestContourArray:
    def _dem(self):
        import numpy as np

        # Linear x gradient: value = 10 * column index, 20x20.
        return np.fromfunction(lambda y, x: 10.0 * x, (20, 20)).astype(np.float32)

    def test_bands_partition_the_footprint(self):
        shapely = pytest.importorskip("shapely")
        table = contour_array(self._dem(), [0.0, 50.0, 100.0, 200.0])
        geoms = [shapely.from_wkb(v) for v in table.column("geometry").to_pylist()]
        assert len(geoms) == 3
        total = sum(g.area for g in geoms)
        # Marching squares runs on the 19x19 cell lattice between grid nodes.
        assert total == pytest.approx(19 * 19, rel=1e-6)
        for i in range(len(geoms)):
            for j in range(i + 1, len(geoms)):
                assert geoms[i].intersection(geoms[j]).area == pytest.approx(0.0, abs=1e-9)

    def test_min_max_columns_pair_adjacent_levels(self):
        table = contour_array(self._dem(), [0.0, 50.0, 100.0, 200.0])
        rows = sorted(
            zip(table.column("min").to_pylist(), table.column("max").to_pylist())
        )
        assert rows == [(0.0, 50.0), (50.0, 100.0), (100.0, 200.0)]

    def test_custom_column_names(self):
        table = contour_array(
            self._dem(), [0.0, 100.0, 200.0], min_column="lo", max_column="hi"
        )
        assert "lo" in table.column_names and "hi" in table.column_names

    def test_output_metadata_is_gpio_shaped(self):
        crs = {"id": {"authority": "EPSG", "code": 32633}}
        table = contour_array(self._dem(), [0.0, 100.0, 200.0], crs=crs)
        geo = json.loads(table.schema.metadata[b"geo"])
        assert geo["primary_column"] == "geometry"
        assert geo["columns"]["geometry"]["crs"] == crs
        assert geo["columns"]["geometry"]["encoding"] == "WKB"
        # contourrs' own extension-field metadata must not leak through
        field = table.schema.field("geometry")
        assert not field.metadata or b"ARROW:extension:name" not in field.metadata


class TestCliContour:
    """CLI surface: option validation is dependency-free."""

    def _invoke(self, args):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        return CliRunner().invoke(cli, ["process", "contour", *args])

    def test_levels_and_interval_are_exclusive(self, tmp_path):
        result = self._invoke(
            ["in.tif", str(tmp_path / "o.parquet"), "--levels", "0,100", "--interval", "50"]
        )
        assert result.exit_code != 0
        assert "--levels" in result.output and "--interval" in result.output

    def test_one_of_levels_or_interval_required(self, tmp_path):
        result = self._invoke(["in.tif", str(tmp_path / "o.parquet")])
        assert result.exit_code != 0
        assert "--levels" in result.output and "--interval" in result.output

    def test_bad_levels_value_is_a_clean_error(self, tmp_path):
        result = self._invoke(
            ["in.tif", str(tmp_path / "o.parquet"), "--levels", "0,abc"]
        )
        assert result.exit_code != 0
        assert "abc" in result.output
        assert "Traceback" not in result.output

    def test_missing_raster_extra_gives_install_hint(self, tmp_path, monkeypatch):
        import sys as _sys

        monkeypatch.setitem(_sys.modules, "rasterio", None)
        monkeypatch.setitem(_sys.modules, "contourrs", None)
        result = self._invoke(
            ["in.tif", str(tmp_path / "o.parquet"), "--interval", "100"]
        )
        assert result.exit_code != 0
        assert "geoparquet-io[raster]" in result.output
        assert "Traceback" not in result.output

    @requires_contourrs
    @requires_rasterio
    def test_roundtrip(self, tmp_path):
        import numpy as np
        import pyarrow.parquet as pq
        import rasterio
        from rasterio.transform import from_origin

        dem = np.fromfunction(lambda y, x: 10.0 * x, (20, 20)).astype(np.float32)
        tif = tmp_path / "dem.tif"
        with rasterio.open(
            str(tif),
            "w",
            driver="GTiff",
            height=20,
            width=20,
            count=1,
            dtype="float32",
            crs="EPSG:32633",
            transform=from_origin(100000, 200000, 10, 10),
        ) as dst:
            dst.write(dem, 1)
        out = tmp_path / "c.parquet"
        result = self._invoke([str(tif), str(out), "--interval", "50"])
        assert result.exit_code == 0, result.output
        table = pq.read_table(str(out))
        assert table.num_rows >= 2
        assert {"min", "max"}.issubset(table.column_names)


@requires_contourrs
@requires_rasterio
class TestContourFile:
    def test_roundtrip_with_transform_and_crs(self, tmp_path):
        import numpy as np
        import pyarrow.parquet as pq
        import rasterio
        from rasterio.transform import from_origin

        from geoparquet_io.core.process.raster.contour import contour_file

        shapely = pytest.importorskip("shapely")
        dem = np.fromfunction(lambda y, x: 10.0 * x, (20, 20)).astype(np.float32)
        tif = tmp_path / "dem.tif"
        with rasterio.open(
            str(tif),
            "w",
            driver="GTiff",
            height=20,
            width=20,
            count=1,
            dtype="float32",
            crs="EPSG:32633",
            transform=from_origin(100000, 200000, 10, 10),
        ) as dst:
            dst.write(dem, 1)

        out = tmp_path / "contours.parquet"
        contour_file(str(tif), str(out), interval=50.0)

        table = pq.read_table(str(out))
        assert table.num_rows >= 2
        geo = json.loads(table.schema.metadata[b"geo"])
        crs = geo["columns"]["geometry"]["crs"]
        assert crs["id"]["code"] in (32633, "32633")
        geoms = [shapely.from_wkb(v) for v in table.column("geometry").to_pylist()]
        union_bounds = shapely.unary_union(geoms).bounds
        # Grid nodes 0..19 map through the transform: bands span the node
        # lattice, not the outer pixel edges.
        assert union_bounds == pytest.approx((100000, 199810, 100190, 200000))


@requires_contourrs
@requires_rasterio
class TestPythonApi:
    def _tif(self, tmp_path):
        import numpy as np
        import rasterio
        from rasterio.transform import from_origin

        dem = np.fromfunction(lambda y, x: 10.0 * x, (10, 10)).astype(np.float32)
        tif = tmp_path / "dem.tif"
        with rasterio.open(
            str(tif), "w", driver="GTiff", height=10, width=10, count=1,
            dtype="float32", crs="EPSG:32633", transform=from_origin(0, 100, 10, 10),
        ) as dst:
            dst.write(dem, 1)
        return tif

    def test_ops_contour(self, tmp_path):
        from geoparquet_io.api import ops

        table = ops.contour(str(self._tif(tmp_path)), interval=30.0)
        assert isinstance(table, pa.Table)
        assert {"min", "max"}.issubset(table.column_names)

    def test_table_contour_constructor(self, tmp_path):
        from geoparquet_io.api.table import Table

        table = Table.contour(str(self._tif(tmp_path)), levels=[0.0, 50.0, 100.0])
        assert isinstance(table, Table)
        assert table.to_arrow().num_rows >= 1
