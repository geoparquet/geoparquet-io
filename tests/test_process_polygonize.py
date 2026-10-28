"""Tests for gpio process polygonize (core/process/raster/polygonize.py).

The output-finalization tests are dependency-free (a hand-built table shaped
like contourrs output) and run on every CI leg; the rest skip where the
``raster`` extra is not installed. The cross-validation tests against
rasterio's GDAL reference implementation double as an upstream regression
signal for contourrs (ADR-0007).
"""

import json
from importlib.util import find_spec

import pyarrow as pa
import pytest

from geoparquet_io.core.process.raster.output import finalize_raster_table

requires_contourrs = pytest.mark.skipif(
    find_spec("contourrs") is None, reason="requires the raster extra (contourrs)"
)
requires_rasterio = pytest.mark.skipif(
    find_spec("rasterio") is None, reason="requires the raster extra (rasterio)"
)

# A tiny valid WKB polygon (unit square), so dep-free tests need no shapely.
_WKB_SQUARE = bytes.fromhex(
    "010300000001000000050000000000000000000000000000000000000000000000000000"
    "00000000000000f03f000000000000f03f000000000000f03f000000000000f03f000000"
    "000000000000000000000000000000000000000000"
)


def _contourrs_shaped_table():
    """A table shaped like contourrs *_arrow output: extension field metadata
    on the geometry column and its own ``geo`` schema metadata block."""
    geom_field = pa.field(
        "geometry",
        pa.binary(),
        nullable=False,
        metadata={
            b"ARROW:extension:name": b"geoarrow.wkb",
            b"ARROW:extension:metadata": b"{}",
        },
    )
    schema = pa.schema(
        [geom_field, pa.field("value", pa.float64(), nullable=False)],
        metadata={
            b"geo": json.dumps(
                {
                    "version": "1.1.0",
                    "primary_column": "geometry",
                    "columns": {"geometry": {"encoding": "WKB", "geometry_types": []}},
                }
            ).encode()
        },
    )
    return pa.table({"geometry": [_WKB_SQUARE], "value": [7.0]}, schema=schema)


class TestFinalizeRasterTable:
    """Dependency-free: gpio owns the output metadata, not contourrs."""

    def test_strips_foreign_metadata_and_attaches_crs(self):
        crs = {"id": {"authority": "EPSG", "code": 32633}}
        table = finalize_raster_table(_contourrs_shaped_table(), crs=crs)
        field = table.schema.field("geometry")
        assert not field.metadata or b"ARROW:extension:name" not in field.metadata
        geo = json.loads(table.schema.metadata[b"geo"])
        assert geo["primary_column"] == "geometry"
        col = geo["columns"]["geometry"]
        assert col["encoding"] == "WKB"
        assert col["crs"] == crs
        # stats are the write funnel's to compute, not ours to carry
        assert "geometry_types" not in col
        assert "bbox" not in col

    def test_no_crs_written_when_unknown(self):
        table = finalize_raster_table(_contourrs_shaped_table(), crs=None)
        geo = json.loads(table.schema.metadata[b"geo"])
        assert "crs" not in geo["columns"]["geometry"]

    def test_renames_value_column(self):
        table = finalize_raster_table(
            _contourrs_shaped_table(), crs=None, rename={"value": "class_id"}
        )
        assert "class_id" in table.column_names
        assert "value" not in table.column_names


@requires_contourrs
class TestPolygonizeArray:
    def _classes(self):
        import numpy as np

        arr = np.zeros((20, 20), dtype=np.uint8)
        arr[2:8, 2:12] = 1  # 6x10 block
        arr[12:18, 5:10] = 3  # 6x5 block
        return arr

    def test_class_areas_match_pixel_counts(self):
        shapely = pytest.importorskip("shapely")
        from geoparquet_io.core.process.raster.polygonize import polygonize_array

        table = polygonize_array(self._classes())
        areas = {}
        for value, wkb in zip(
            table.column("value").to_pylist(), table.column("geometry").to_pylist(), strict=True
        ):
            areas[value] = areas.get(value, 0) + shapely.from_wkb(wkb).area
        assert areas[1.0] == pytest.approx(60.0)
        assert areas[3.0] == pytest.approx(30.0)
        assert areas[0.0] == pytest.approx(400.0 - 90.0)

    def test_values_filter_keeps_only_requested_classes(self):
        from geoparquet_io.core.process.raster.polygonize import polygonize_array

        table = polygonize_array(self._classes(), values=[1.0, 3.0])
        assert set(table.column("value").to_pylist()) == {1.0, 3.0}

    def test_exterior_rings_are_ccw(self):
        """GeoParquet says outer rings should be counterclockwise; contourrs
        delivers that today — this pins it as an upstream regression signal."""
        shapely = pytest.importorskip("shapely")
        from geoparquet_io.core.process.raster.polygonize import polygonize_array

        table = polygonize_array(self._classes())
        for wkb in table.column("geometry").to_pylist():
            geom = shapely.from_wkb(wkb)
            polys = [geom] if geom.geom_type == "Polygon" else list(geom.geoms)
            for poly in polys:
                assert shapely.is_ccw(shapely.get_exterior_ring(poly))

    def test_value_column_rename(self):
        from geoparquet_io.core.process.raster.polygonize import polygonize_array

        table = polygonize_array(self._classes(), value_column="class_id")
        assert "class_id" in table.column_names


@requires_contourrs
@requires_rasterio
class TestPolygonizeCrossValidation:
    """contourrs vs the GDAL reference implementation (upstream signal)."""

    def test_matches_rasterio_shapes(self):
        import numpy as np
        import rasterio.features
        import shapely
        import shapely.geometry

        from geoparquet_io.core.process.raster.polygonize import polygonize_array

        rng = np.random.default_rng(42)
        arr = rng.integers(0, 4, size=(30, 30)).astype(np.uint8)

        ours = {}
        table = polygonize_array(arr)
        for value, wkb in zip(
            table.column("value").to_pylist(), table.column("geometry").to_pylist(), strict=True
        ):
            ours[value] = ours.get(value, 0) + shapely.from_wkb(wkb).area

        reference = {}
        for geom, value in rasterio.features.shapes(arr):
            reference[value] = reference.get(value, 0) + shapely.geometry.shape(geom).area

        assert set(ours) == set(reference)
        for value in reference:
            assert ours[value] == pytest.approx(reference[value]), value

    def test_rasterize_roundtrip_is_exact(self):
        import numpy as np
        import rasterio.features
        import shapely
        import shapely.geometry

        from geoparquet_io.core.process.raster.polygonize import polygonize_array

        arr = np.zeros((20, 20), dtype=np.uint8)
        arr[2:8, 2:12] = 1
        arr[12:18, 5:10] = 3

        table = polygonize_array(arr)
        pairs = [
            (shapely.from_wkb(wkb), int(value))
            for value, wkb in zip(
                table.column("value").to_pylist(),
                table.column("geometry").to_pylist(),
                strict=True,
            )
        ]
        back = rasterio.features.rasterize(pairs, out_shape=(20, 20), dtype="uint8")
        assert np.array_equal(back, arr)


class TestCliPolygonize:
    """CLI surface: option validation is dependency-free."""

    def _invoke(self, args):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        return CliRunner().invoke(cli, ["process", "polygonize", *args])

    def test_bad_values_option_is_a_clean_error(self, tmp_path):
        result = self._invoke(["in.tif", str(tmp_path / "o.parquet"), "--values", "1,x"])
        assert result.exit_code != 0
        assert "x" in result.output
        assert "Traceback" not in result.output

    def test_missing_raster_extra_gives_install_hint(self, tmp_path, monkeypatch):
        import sys as _sys

        monkeypatch.setitem(_sys.modules, "rasterio", None)
        monkeypatch.setitem(_sys.modules, "contourrs", None)
        result = self._invoke(["in.tif", str(tmp_path / "o.parquet")])
        assert result.exit_code != 0
        assert "geoparquet-io[raster]" in result.output
        assert "Traceback" not in result.output

    @requires_contourrs
    @requires_rasterio
    def test_roundtrip_with_values_filter(self, tmp_path):
        import numpy as np
        import pyarrow.parquet as pq
        import rasterio
        from rasterio.transform import from_origin

        arr = np.zeros((20, 20), dtype=np.uint8)
        arr[2:8, 2:12] = 1
        arr[12:18, 5:10] = 3
        tif = tmp_path / "classes.tif"
        with rasterio.open(
            str(tif),
            "w",
            driver="GTiff",
            height=20,
            width=20,
            count=1,
            dtype="uint8",
            crs="EPSG:32633",
            transform=from_origin(100000, 200000, 10, 10),
        ) as dst:
            dst.write(arr, 1)
        out = tmp_path / "p.parquet"
        result = self._invoke([str(tif), str(out), "--values", "1,3", "--value-column", "class_id"])
        assert result.exit_code == 0, result.output
        table = pq.read_table(str(out))
        assert set(table.column("class_id").to_pylist()) == {1.0, 3.0}


@requires_contourrs
@requires_rasterio
class TestPolygonizeFile:
    def test_roundtrip_with_nodata_and_crs(self, tmp_path):
        import numpy as np
        import pyarrow.parquet as pq
        import rasterio
        from rasterio.transform import from_origin

        from geoparquet_io.core.process.raster.polygonize import polygonize_file

        shapely = pytest.importorskip("shapely")
        arr = np.zeros((20, 20), dtype=np.uint8)
        arr[2:8, 2:12] = 1
        arr[0:2, 0:2] = 255  # nodata corner
        tif = tmp_path / "classes.tif"
        with rasterio.open(
            str(tif),
            "w",
            driver="GTiff",
            height=20,
            width=20,
            count=1,
            dtype="uint8",
            crs="EPSG:32633",
            transform=from_origin(100000, 200000, 10, 10),
            nodata=255,
        ) as dst:
            dst.write(arr, 1)

        out = tmp_path / "classes.parquet"
        polygonize_file(str(tif), str(out))

        table = pq.read_table(str(out))
        geo = json.loads(table.schema.metadata[b"geo"])
        assert geo["columns"]["geometry"]["crs"]["id"]["code"] in (32633, "32633")
        values = set(table.column("value").to_pylist())
        assert 255.0 not in values  # nodata polygonized away
        total = sum(shapely.from_wkb(w).area for w in table.column("geometry").to_pylist())
        # 400 pixels minus the 4 nodata pixels, at 10m resolution
        assert total == pytest.approx((400 - 4) * 100.0)
        # world coordinates, not pixel coordinates
        class1 = [
            shapely.from_wkb(w)
            for v, w in zip(
                table.column("value").to_pylist(),
                table.column("geometry").to_pylist(),
                strict=True,
            )
            if v == 1.0
        ]
        bounds = shapely.unary_union(class1).bounds
        assert bounds == pytest.approx((100020, 199920, 100120, 199980))


@requires_contourrs
@requires_rasterio
class TestPythonApi:
    def _tif(self, tmp_path):
        import numpy as np
        import rasterio
        from rasterio.transform import from_origin

        arr = np.zeros((10, 10), dtype=np.uint8)
        arr[2:6, 2:6] = 1
        tif = tmp_path / "c.tif"
        with rasterio.open(
            str(tif),
            "w",
            driver="GTiff",
            height=10,
            width=10,
            count=1,
            dtype="uint8",
            crs="EPSG:32633",
            transform=from_origin(0, 100, 10, 10),
        ) as dst:
            dst.write(arr, 1)
        return tif

    def test_ops_polygonize(self, tmp_path):
        from geoparquet_io.api import ops

        table = ops.polygonize(str(self._tif(tmp_path)), values=[1.0])
        assert isinstance(table, pa.Table)
        assert set(table.column("value").to_pylist()) == {1.0}

    def test_table_polygonize_constructor(self, tmp_path):
        from geoparquet_io.api.table import Table

        table = Table.polygonize(str(self._tif(tmp_path)))
        assert isinstance(table, Table)
        assert table.to_arrow().num_rows >= 2


class TestColumnNameValidation:
    def test_value_column_may_not_shadow_geometry(self):
        import numpy as np

        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.process.raster.polygonize import polygonize_array

        with pytest.raises(InvalidParameterError, match="geometry"):
            polygonize_array(np.zeros((4, 4), dtype=np.uint8), value_column="geometry")


@requires_contourrs
@requires_rasterio
class TestNodataOverride:
    def test_override_releases_the_tags_pixels(self, tmp_path):
        """--nodata N replaces the file's tag: the tag's pixels are data again."""
        import numpy as np
        import rasterio
        from rasterio.transform import from_origin

        from geoparquet_io.core.process.raster.polygonize import polygonize_file

        arr = np.zeros((10, 10), dtype=np.uint8)
        arr[2:6, 2:6] = 7  # real data that HAPPENS to equal the (wrong) tag
        arr[8:10, 8:10] = 1  # what the user says nodata actually is
        tif = tmp_path / "c.tif"
        with rasterio.open(
            str(tif),
            "w",
            driver="GTiff",
            height=10,
            width=10,
            count=1,
            dtype="uint8",
            nodata=7,
            transform=from_origin(0, 100, 10, 10),
        ) as dst:
            dst.write(arr, 1)

        out = tmp_path / "out.parquet"
        polygonize_file(str(tif), str(out), nodata=1)
        import pyarrow.parquet as pq

        values = set(pq.read_table(str(out)).column("value").to_pylist())
        assert 7.0 in values  # the tag's pixels are kept under the override
        assert 1.0 not in values  # the override is excluded

    def test_missing_raster_is_a_clean_error(self, tmp_path):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        result = CliRunner().invoke(
            cli,
            ["process", "polygonize", str(tmp_path / "nope.tif"), str(tmp_path / "o.parquet")],
        )
        assert result.exit_code != 0
        assert "Traceback" not in result.output
        assert "nope.tif" in result.output
