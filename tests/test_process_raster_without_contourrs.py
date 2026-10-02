"""Raster polygonize/contour tests that do not need contourrs installed.

contourrs ships no wheels below Python 3.12, so on the 3.11 leg — the one leg
that enforces the coverage floor and the changed-lines gate — every
contourrs-gated test skips and the code around the library reads as dead.
What gpio actually owns there is everything *around* the call: column renames
and collisions, the ``values`` filter, band pairing, level resolution, the
output's ``geo`` block, the file write, and the CLI's error translation.

:func:`~geoparquet_io.core.optional_deps.require_contourrs` imports the module
on every call, so a ``sys.modules`` entry installed by ``monkeypatch`` decides
what the raster code sees — deterministically, whether or not the real library
is installed. The stand-in returns a table shaped exactly like contourrs'
``*_arrow`` output (a ``geoarrow.wkb``-annotated binary ``geometry`` column, a
float64 ``value`` column, and its own ``geo`` schema metadata) and records its
keyword arguments, which is what lets the pass-through assertions bite.

The numeric agreement with GDAL stays in tests/test_process_polygonize.py and
tests/test_process_contour.py, which do need the real library.
"""

import json
import sys
import types
from importlib.util import find_spec

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.raster.contour import (
    _band_max_for,
    _valid_range,
    contour_array,
    contour_file,
    contour_raster,
)
from geoparquet_io.core.process.raster.polygonize import (
    polygonize_array,
    polygonize_file,
    polygonize_raster,
)

requires_rasterio = pytest.mark.skipif(
    find_spec("rasterio") is None, reason="requires the raster extra (rasterio)"
)


def _contourrs_shaped_table(values):
    """A table shaped like contourrs ``*_arrow`` output, one row per value."""
    geoms = [shapely.box(i, 0, i + 1, 1) for i in range(len(values))]
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
                    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon"]}},
                }
            ).encode()
        },
    )
    return pa.table(
        {
            "geometry": [shapely.to_wkb(g) for g in geoms],
            "value": [float(v) for v in values],
        },
        schema=schema,
    )


@pytest.fixture
def fake_contourrs(monkeypatch):
    """A stand-in contourrs, installed only for the duration of one test.

    ``monkeypatch.setitem`` restores the real module (where there is one)
    afterwards, so this never leaks into the tests that cross-validate
    against GDAL.
    """
    module = types.ModuleType("contourrs")
    module.calls = []
    module.shapes_values = [0.0, 1.0, 3.0]
    #: None means "one band per level, lowest break first", as contourrs does.
    module.contour_values = None
    module.raises = None

    def shapes_arrow(array, **kwargs):
        module.calls.append(("shapes_arrow", array, kwargs))
        if module.raises is not None:
            raise module.raises
        return _contourrs_shaped_table(module.shapes_values)

    def contours_arrow(array, **kwargs):
        module.calls.append(("contours_arrow", array, kwargs))
        if module.raises is not None:
            raise module.raises
        values = module.contour_values
        if values is None:
            values = list(kwargs["thresholds"])[:-1]
        return _contourrs_shaped_table(values)

    module.shapes_arrow = shapes_arrow
    module.contours_arrow = contours_arrow
    monkeypatch.setitem(sys.modules, "contourrs", module)
    return module


def _write_tif(path, array, *, nodata=None, crs="EPSG:32633"):
    import rasterio
    from rasterio.transform import from_origin

    height, width = array.shape
    kwargs = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": 1,
        "dtype": array.dtype.name,
        "transform": from_origin(1000.0, 2000.0, 10.0, 10.0),
        "crs": crs,
    }
    if nodata is not None:
        kwargs["nodata"] = nodata
    with rasterio.open(str(path), "w", **kwargs) as dst:
        dst.write(array, 1)
    return str(path)


def _classes():
    arr = np.zeros((6, 6), dtype="uint8")
    arr[1:3, 1:4] = 1
    arr[4:6, 0:2] = 3
    return arr


class TestPolygonizeArrayAroundTheLibrary:
    def test_value_column_rename(self, fake_contourrs):
        table = polygonize_array(_classes(), value_column="class_id")
        assert table.column_names == ["geometry", "class_id"]

    def test_values_filter_keeps_only_the_listed_classes(self, fake_contourrs):
        table = polygonize_array(_classes(), values=[1.0, 3.0])
        assert table.column("value").to_pylist() == [1.0, 3.0]

    def test_values_filter_applies_after_the_rename(self, fake_contourrs):
        """The filter reads the renamed column, not contourrs' 'value'."""
        table = polygonize_array(_classes(), values=[3.0], value_column="class_id")
        assert table.column("class_id").to_pylist() == [3.0]

    def test_value_column_may_not_shadow_geometry(self, fake_contourrs):
        with pytest.raises(InvalidParameterError, match="geometry"):
            polygonize_array(_classes(), value_column="geometry")
        assert fake_contourrs.calls == []  # rejected before the library is touched

    def test_connectivity_and_nodata_reach_the_library(self, fake_contourrs):
        mask = np.ones((6, 6), dtype=bool)
        polygonize_array(_classes(), connectivity=8, nodata=255.0, mask=mask)
        name, _array, kwargs = fake_contourrs.calls[0]
        assert name == "shapes_arrow"
        assert kwargs["connectivity"] == 8
        assert kwargs["nodata"] == 255.0
        assert kwargs["mask"] is mask

    def test_output_metadata_is_gpio_shaped(self, fake_contourrs):
        crs = {"id": {"authority": "EPSG", "code": 32633}}
        table = polygonize_array(_classes(), crs=crs, verbose=True)
        geo = json.loads(table.schema.metadata[b"geo"])
        assert geo["primary_column"] == "geometry"
        assert geo["columns"]["geometry"]["crs"] == crs
        # contourrs' own stats and extension annotation must not survive
        assert "geometry_types" not in geo["columns"]["geometry"]
        field = table.schema.field("geometry")
        assert not field.metadata or b"ARROW:extension:name" not in field.metadata


@requires_rasterio
class TestPolygonizeRasterAndFile:
    def test_raster_forwards_the_bands_georeferencing(self, fake_contourrs, tmp_path):
        path = _write_tif(tmp_path / "c.tif", _classes(), nodata=3)
        table = polygonize_raster(path, values=[0.0, 1.0])
        _name, array, kwargs = fake_contourrs.calls[0]
        assert np.array_equal(array, _classes())
        assert kwargs["transform"] == (10.0, 0.0, 1000.0, 0.0, -10.0, 2000.0)
        assert kwargs["nodata"] == 3
        assert kwargs["mask"] is not None  # the tag masks the 3-valued block
        geo = json.loads(table.schema.metadata[b"geo"])
        assert geo["columns"]["geometry"]["crs"]["id"]["code"] in (32633, "32633")

    def test_file_writes_real_geoparquet(self, fake_contourrs, tmp_path):
        path = _write_tif(tmp_path / "c.tif", _classes())
        out = tmp_path / "c.parquet"
        polygonize_file(path, str(out), value_column="class_id", verbose=True)
        table = pq.read_table(str(out))
        assert table.column("class_id").to_pylist() == [0.0, 1.0, 3.0]
        geo = json.loads(table.schema.metadata[b"geo"])
        column = geo["columns"]["geometry"]
        # the write funnel, not contourrs, computed these
        assert column["geometry_types"] == ["Polygon"]
        assert column["bbox"] == [0.0, 0.0, 3.0, 1.0]

    def test_band_out_of_range_still_reports_cleanly(self, fake_contourrs, tmp_path):
        path = _write_tif(tmp_path / "c.tif", _classes())
        with pytest.raises(InvalidParameterError, match="band"):
            polygonize_raster(path, band=4)


class TestBandMaxFor:
    """Pairing a lower break with its upper break, including the fallback."""

    def test_adjacent_level_is_the_upper_break(self):
        assert _band_max_for([0.0, 50.0, 100.0], 50.0) == 100.0
        assert _band_max_for([0.0, 50.0, 100.0], 0.0) == 50.0

    def test_unasked_lower_bound_falls_back_to_the_nearest_band(self):
        """contourrs returning a break we did not ask for must not crash."""
        assert _band_max_for([0.0, 50.0, 100.0], 10.0) == 50.0
        assert _band_max_for([0.0, 50.0, 100.0], 37.0) == 100.0

    def test_lower_bound_at_or_past_the_top_falls_back_too(self):
        assert _band_max_for([0.0, 50.0, 100.0], 100.0) == 100.0
        assert _band_max_for([0.0, 50.0, 100.0], 150.0) == 100.0


class TestContourArrayAroundTheLibrary:
    def _dem(self):
        return np.fromfunction(lambda y, x: 10.0 * x, (6, 6)).astype("float32")

    def test_bands_pair_adjacent_levels(self, fake_contourrs):
        table = contour_array(self._dem(), [0.0, 50.0, 100.0, 200.0])
        rows = list(
            zip(table.column("min").to_pylist(), table.column("max").to_pylist(), strict=True)
        )
        assert rows == [(0.0, 50.0), (50.0, 100.0), (100.0, 200.0)]

    def test_levels_reach_the_library_as_thresholds(self, fake_contourrs):
        contour_array(self._dem(), [0.0, 50.0], nodata=-9999.0)
        name, _array, kwargs = fake_contourrs.calls[0]
        assert name == "contours_arrow"
        assert kwargs["thresholds"] == [0.0, 50.0]
        assert kwargs["nodata"] == -9999.0

    def test_custom_min_max_column_names(self, fake_contourrs):
        table = contour_array(self._dem(), [0.0, 50.0, 100.0], min_column="lo", max_column="hi")
        assert table.column_names == ["geometry", "lo", "hi"]
        assert table.column("lo").to_pylist() == [0.0, 50.0]

    def test_min_and_max_columns_must_differ(self, fake_contourrs):
        with pytest.raises(InvalidParameterError, match="both"):
            contour_array(self._dem(), [0.0, 50.0], min_column="x", max_column="x")
        assert fake_contourrs.calls == []

    @pytest.mark.parametrize("field", ["min_column", "max_column"])
    def test_neither_column_may_shadow_geometry(self, fake_contourrs, field):
        with pytest.raises(InvalidParameterError, match="geometry"):
            contour_array(self._dem(), [0.0, 50.0], **{field: "geometry"})

    def test_levels_are_validated_before_the_library_runs(self, fake_contourrs):
        with pytest.raises(InvalidParameterError, match="increasing"):
            contour_array(self._dem(), [50.0, 0.0])
        assert fake_contourrs.calls == []

    def test_unasked_lower_bound_is_paired_anyway(self, fake_contourrs):
        """End-to-end of the nearest-wins fallback."""
        fake_contourrs.contour_values = [0.0, 37.0]
        table = contour_array(self._dem(), [0.0, 50.0, 100.0])
        assert table.column("max").to_pylist() == [50.0, 100.0]

    def test_output_metadata_is_gpio_shaped(self, fake_contourrs):
        crs = {"id": {"authority": "EPSG", "code": 32633}}
        table = contour_array(self._dem(), [0.0, 50.0, 100.0], crs=crs, verbose=True)
        geo = json.loads(table.schema.metadata[b"geo"])
        assert geo["columns"]["geometry"]["crs"] == crs
        assert "geometry_types" not in geo["columns"]["geometry"]
        field = table.schema.field("geometry")
        assert not field.metadata or b"ARROW:extension:name" not in field.metadata


class TestValidRange:
    """The [min, max] that interval mode resolves its level ladder against."""

    def test_plain_array(self):
        assert _valid_range(np.array([[1.0, 5.0], [3.0, 2.0]]), None, None) == (1.0, 5.0)

    def test_mask_excludes_invalid_pixels(self):
        array = np.array([[1.0, 5.0], [3.0, 2.0]])
        mask = np.array([[False, False], [True, True]])
        assert _valid_range(array, mask, None) == (2.0, 3.0)

    def test_nodata_value_is_excluded(self):
        array = np.array([[-9999.0, 5.0], [3.0, -9999.0]])
        assert _valid_range(array, None, -9999.0) == (3.0, 5.0)

    def test_non_finite_pixels_are_excluded(self):
        array = np.array([[np.nan, 5.0], [3.0, np.inf]])
        assert _valid_range(array, None, None) == (3.0, 5.0)

    def test_no_valid_pixels_is_a_named_error(self):
        array = np.array([[-9999.0, -9999.0]])
        with pytest.raises(InvalidParameterError, match="no valid pixels"):
            _valid_range(array, None, -9999.0)


@requires_rasterio
class TestContourRasterAndFile:
    def _dem(self):
        return np.fromfunction(lambda y, x: 10.0 * x, (6, 6)).astype("float32")

    def test_interval_mode_resolves_levels_from_the_band(self, fake_contourrs, tmp_path):
        path = _write_tif(tmp_path / "dem.tif", self._dem())
        contour_raster(path, interval=20.0)
        _name, _array, kwargs = fake_contourrs.calls[0]
        # the band spans 0..50, so the ladder brackets it
        assert kwargs["thresholds"] == [0.0, 20.0, 40.0, 60.0]

    def test_interval_mode_honors_the_nodata_tag(self, fake_contourrs, tmp_path):
        dem = self._dem()
        dem[:, 0] = -9999.0
        path = _write_tif(tmp_path / "dem.tif", dem, nodata=-9999.0)
        contour_raster(path, interval=20.0)
        _name, _array, kwargs = fake_contourrs.calls[0]
        # -9999 is masked away, so the ladder starts from the real minimum (10)
        assert kwargs["thresholds"][0] == 0.0
        assert kwargs["thresholds"][-1] == 60.0

    def test_explicit_levels_skip_range_detection(self, fake_contourrs, tmp_path):
        path = _write_tif(tmp_path / "dem.tif", self._dem())
        table = contour_raster(path, levels=[0.0, 25.0, 50.0], min_column="lo", max_column="hi")
        assert table.column("lo").to_pylist() == [0.0, 25.0]
        assert table.column("hi").to_pylist() == [25.0, 50.0]

    def test_levels_and_interval_are_exclusive_at_the_core(self, fake_contourrs, tmp_path):
        path = _write_tif(tmp_path / "dem.tif", self._dem())
        with pytest.raises(InvalidParameterError, match="both"):
            contour_raster(path, levels=[0.0, 1.0], interval=10.0)

    def test_file_writes_real_geoparquet(self, fake_contourrs, tmp_path):
        path = _write_tif(tmp_path / "dem.tif", self._dem())
        out = tmp_path / "c.parquet"
        contour_file(path, str(out), interval=25.0, verbose=True)
        table = pq.read_table(str(out))
        assert {"min", "max"}.issubset(table.column_names)
        geo = json.loads(table.schema.metadata[b"geo"])
        assert geo["columns"]["geometry"]["geometry_types"] == ["Polygon"]
        assert geo["columns"]["geometry"]["crs"]["id"]["code"] in (32633, "32633")

    def test_flat_band_still_contours(self, fake_contourrs, tmp_path):
        path = _write_tif(tmp_path / "flat.tif", np.full((4, 4), 7.0, dtype="float32"))
        out = tmp_path / "flat.parquet"
        contour_file(path, str(out), interval=10.0)
        assert pq.read_table(str(out)).num_rows >= 1


@requires_rasterio
class TestPythonApiWithoutContourrs:
    def test_ops_polygonize(self, fake_contourrs, tmp_path):
        from geoparquet_io.api import ops

        path = _write_tif(tmp_path / "c.tif", _classes())
        table = ops.polygonize(path, values=[1.0], value_column="class_id")
        assert isinstance(table, pa.Table)
        assert table.column("class_id").to_pylist() == [1.0]

    def test_ops_contour(self, fake_contourrs, tmp_path):
        from geoparquet_io.api import ops

        path = _write_tif(tmp_path / "dem.tif", np.zeros((4, 4), dtype="float32"))
        table = ops.contour(path, levels=[0.0, 10.0, 20.0], min_column="lo", max_column="hi")
        assert isinstance(table, pa.Table)
        assert table.column("lo").to_pylist() == [0.0, 10.0]

    def test_table_polygonize_constructor(self, fake_contourrs, tmp_path):
        from geoparquet_io.api.table import Table

        path = _write_tif(tmp_path / "c.tif", _classes())
        table = Table.polygonize(path, band=1, nodata=255.0)
        assert isinstance(table, Table)
        assert table.to_arrow().num_rows == 3

    def test_table_contour_constructor(self, fake_contourrs, tmp_path):
        from geoparquet_io.api.table import Table

        path = _write_tif(tmp_path / "dem.tif", np.zeros((4, 4), dtype="float32"))
        table = Table.contour(path, interval=5.0, base=1.0)
        assert isinstance(table, Table)
        assert {"min", "max"}.issubset(table.to_arrow().column_names)


@requires_rasterio
class TestCliWithoutContourrs:
    """The CLI runs in-process, so the stand-in module reaches it too."""

    def _invoke(self, args):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        return CliRunner().invoke(cli, ["process", *args])

    def test_polygonize_writes_the_output(self, fake_contourrs, tmp_path):
        path = _write_tif(tmp_path / "c.tif", _classes())
        out = tmp_path / "c.parquet"
        result = self._invoke(
            ["polygonize", path, str(out), "--values", "1,3", "--value-column", "class_id"]
        )
        assert result.exit_code == 0, result.output
        assert pq.read_table(str(out)).column("class_id").to_pylist() == [1.0, 3.0]

    def test_polygonize_passes_connectivity_and_no_mask(self, fake_contourrs, tmp_path):
        path = _write_tif(tmp_path / "c.tif", _classes(), nodata=3)
        out = tmp_path / "c.parquet"
        result = self._invoke(["polygonize", path, str(out), "--no-mask", "--nodata", "9"])
        assert result.exit_code == 0, result.output
        _name, _array, kwargs = fake_contourrs.calls[0]
        assert kwargs["mask"] is None
        assert kwargs["nodata"] == 9.0

    def test_contour_writes_the_output(self, fake_contourrs, tmp_path):
        path = _write_tif(tmp_path / "dem.tif", np.zeros((4, 4), dtype="float32"))
        out = tmp_path / "c.parquet"
        result = self._invoke(
            ["contour", path, str(out), "--levels", "0,10,20", "--min-column", "lo"]
        )
        assert result.exit_code == 0, result.output
        assert pq.read_table(str(out)).column("lo").to_pylist() == [0.0, 10.0]

    def test_polygonize_value_error_becomes_a_clean_message(self, fake_contourrs, tmp_path):
        """A ValueError out of the library is a user-facing message, not a traceback."""
        fake_contourrs.raises = ValueError("array must be 2D")
        path = _write_tif(tmp_path / "c.tif", _classes())
        result = self._invoke(["polygonize", path, str(tmp_path / "o.parquet")])
        assert result.exit_code != 0
        assert "array must be 2D" in result.output
        assert "Traceback" not in result.output

    def test_contour_value_error_becomes_a_clean_message(self, fake_contourrs, tmp_path):
        fake_contourrs.raises = ValueError("thresholds must be finite")
        path = _write_tif(tmp_path / "dem.tif", np.zeros((4, 4), dtype="float32"))
        result = self._invoke(["contour", path, str(tmp_path / "o.parquet"), "--interval", "5"])
        assert result.exit_code != 0
        assert "thresholds must be finite" in result.output
        assert "Traceback" not in result.output
