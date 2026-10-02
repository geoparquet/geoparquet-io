"""Tests for the raster band reader (core/process/raster/reader.py).

These gate on rasterio alone, never on contourrs. contourrs publishes no
wheels below Python 3.12, so a reader test skipped on contourrs would leave
``read_band`` — mask policy, nodata override, CRS extraction — unmeasured on
the 3.11 coverage leg, which is the leg that enforces the gates.
"""

from importlib.util import find_spec

import numpy as np
import pytest

from geoparquet_io.core.exceptions import FileNotFoundGeoParquetError, InvalidParameterError
from geoparquet_io.core.process.raster.reader import read_band

requires_rasterio = pytest.mark.skipif(
    find_spec("rasterio") is None, reason="requires the raster extra (rasterio)"
)

pytestmark = requires_rasterio


def _write_tif(
    path,
    array,
    *,
    nodata=None,
    crs=None,
    mask=None,
    count=1,
):
    """A tiny GeoTIFF at a known transform; ``mask`` writes a dataset mask."""
    import rasterio
    from rasterio.transform import from_origin

    height, width = array.shape
    kwargs = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": count,
        "dtype": array.dtype.name,
        "transform": from_origin(1000.0, 2000.0, 10.0, 10.0),
    }
    if nodata is not None:
        kwargs["nodata"] = nodata
    if crs is not None:
        kwargs["crs"] = crs
    with rasterio.open(str(path), "w", **kwargs) as dst:
        for band in range(1, count + 1):
            dst.write(array, band)
        if mask is not None:
            dst.write_mask(mask)
    return str(path)


def _ramp():
    return np.arange(16, dtype="uint8").reshape(4, 4)


class TestReadBandGeoreferencing:
    def test_transform_is_a_six_tuple(self, tmp_path):
        band = read_band(_write_tif(tmp_path / "r.tif", _ramp()))
        assert band.transform == (10.0, 0.0, 1000.0, 0.0, -10.0, 2000.0)

    def test_crs_comes_back_as_projjson(self, tmp_path):
        band = read_band(_write_tif(tmp_path / "r.tif", _ramp(), crs="EPSG:32633"))
        assert band.crs["id"]["code"] in (32633, "32633")
        assert "coordinate_system" in band.crs

    def test_no_crs_is_none_not_a_guess(self, tmp_path):
        band = read_band(_write_tif(tmp_path / "r.tif", _ramp()))
        assert band.crs is None

    def test_array_is_the_requested_band(self, tmp_path):
        path = _write_tif(tmp_path / "r.tif", _ramp(), count=2)
        band = read_band(path, band=2)
        assert np.array_equal(band.array, _ramp())


class TestReadBandErrors:
    def test_band_above_the_count_names_the_count(self, tmp_path):
        path = _write_tif(tmp_path / "r.tif", _ramp())
        with pytest.raises(InvalidParameterError, match="1 band"):
            read_band(path, band=2)

    def test_band_below_one_is_rejected(self, tmp_path):
        path = _write_tif(tmp_path / "r.tif", _ramp())
        with pytest.raises(InvalidParameterError, match="band"):
            read_band(path, band=0)

    def test_missing_file_names_the_path(self, tmp_path):
        missing = tmp_path / "nope.tif"
        with pytest.raises(FileNotFoundGeoParquetError, match="nope.tif"):
            read_band(str(missing))


class TestReadBandMaskPolicy:
    """An explicit nodata REPLACES the tag; a mask with another source stays."""

    def test_nodata_tag_builds_a_mask(self, tmp_path):
        path = _write_tif(tmp_path / "r.tif", _ramp(), nodata=3)
        band = read_band(path)
        assert band.nodata == 3
        assert band.mask is not None
        assert not band.mask[0, 3]  # the tag's pixel
        assert band.mask[1, 0]

    def test_fully_valid_raster_has_no_mask(self, tmp_path):
        path = _write_tif(tmp_path / "r.tif", _ramp(), nodata=99)
        band = read_band(path)
        assert band.mask is None  # nothing is masked, so nothing to carry

    def test_override_replaces_the_tag_and_drops_its_mask(self, tmp_path):
        path = _write_tif(tmp_path / "r.tif", _ramp(), nodata=3)
        band = read_band(path, nodata=7.0)
        assert band.nodata == 7.0
        # The tag's mask would have excluded pixel 3 behind the override's back.
        assert band.mask is None

    def test_override_keeps_a_mask_with_another_source(self, tmp_path):
        """A dataset mask is not the nodata tag: the override must not drop it."""
        dataset_mask = np.full((4, 4), 255, dtype="uint8")
        dataset_mask[0, 0] = 0
        path = _write_tif(tmp_path / "r.tif", _ramp(), nodata=3, mask=dataset_mask)
        band = read_band(path, nodata=7.0)
        assert band.mask is not None
        assert not band.mask[0, 0]
        assert band.mask[0, 3]  # the replaced tag's pixel is data again

    def test_use_mask_false_ignores_the_mask_entirely(self, tmp_path):
        path = _write_tif(tmp_path / "r.tif", _ramp(), nodata=3)
        band = read_band(path, use_mask=False)
        assert band.mask is None
        assert band.nodata == 3  # still reported, just not pre-applied
