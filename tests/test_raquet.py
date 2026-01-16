"""Tests for raquet (raster parquet) functionality."""

import json
import os
import tempfile
import uuid
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli


@pytest.fixture
def runner():
    """Provide a Click CLI runner."""
    return CliRunner()


@pytest.fixture
def temp_geotiff():
    """Create a temporary GeoTIFF for testing."""
    import rasterio
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds

    temp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.tif"

    # Create simple test raster - 256x256 with random values
    data = np.random.randint(0, 255, (256, 256), dtype=np.uint8)
    transform = from_bounds(-122.5, 37.5, -122.0, 38.0, 256, 256)

    with rasterio.open(
        temp_path,
        "w",
        driver="GTiff",
        height=256,
        width=256,
        count=1,
        dtype=np.uint8,
        crs=CRS.from_epsg(4326),
        transform=transform,
    ) as dst:
        dst.write(data, 1)

    yield str(temp_path)

    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def temp_multiband_geotiff():
    """Create a temporary multi-band GeoTIFF for testing."""
    import rasterio
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds

    temp_path = Path(tempfile.gettempdir()) / f"test_rgb_{uuid.uuid4()}.tif"

    # Create RGB raster - 256x256 with 3 bands
    red = np.random.randint(0, 255, (256, 256), dtype=np.uint8)
    green = np.random.randint(0, 255, (256, 256), dtype=np.uint8)
    blue = np.random.randint(0, 255, (256, 256), dtype=np.uint8)

    transform = from_bounds(-122.5, 37.5, -122.0, 38.0, 256, 256)

    with rasterio.open(
        temp_path,
        "w",
        driver="GTiff",
        height=256,
        width=256,
        count=3,
        dtype=np.uint8,
        crs=CRS.from_epsg(4326),
        transform=transform,
    ) as dst:
        dst.write(red, 1)
        dst.write(green, 2)
        dst.write(blue, 3)

    yield str(temp_path)

    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def temp_output_parquet():
    """Provide a temporary output path for parquet files."""
    temp_path = Path(tempfile.gettempdir()) / f"test_output_{uuid.uuid4()}.parquet"
    yield str(temp_path)
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def temp_output_geotiff():
    """Provide a temporary output path for GeoTIFF files."""
    temp_path = Path(tempfile.gettempdir()) / f"test_output_{uuid.uuid4()}.tif"
    yield str(temp_path)
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def temp_raquet(temp_geotiff, temp_output_parquet):
    """Create a temporary raquet file from the test GeoTIFF."""
    from geoparquet_io.core.raquet import geotiff_to_raquet

    geotiff_to_raquet(temp_geotiff, temp_output_parquet, block_size=256)

    return temp_output_parquet


@pytest.fixture
def example_raquet():
    """Return path to example raquet file if it exists."""
    example_path = "/tmp/raquet/examples/example_data.parquet"
    if os.path.exists(example_path):
        return example_path
    pytest.skip("Example raquet file not available")


class TestIsRaquetFile:
    """Tests for raquet file detection."""

    def test_valid_raquet_detected(self, temp_raquet):
        """Test that a valid raquet file is detected."""
        from geoparquet_io.core.raquet import is_raquet_file

        assert is_raquet_file(temp_raquet) is True

    def test_non_raquet_parquet_not_detected(self):
        """Test that a non-raquet parquet file is not detected as raquet."""
        from geoparquet_io.core.raquet import is_raquet_file

        test_file = "tests/data/places_test.parquet"
        if os.path.exists(test_file):
            assert is_raquet_file(test_file) is False

    def test_nonexistent_file_returns_false(self):
        """Test that a nonexistent file returns False."""
        from geoparquet_io.core.raquet import is_raquet_file

        assert is_raquet_file("/nonexistent/path.parquet") is False


class TestRaquetMetadata:
    """Tests for raquet metadata reading."""

    def test_read_metadata(self, temp_raquet):
        """Test reading metadata from a raquet file."""
        from geoparquet_io.core.raquet import read_raquet_metadata

        metadata = read_raquet_metadata(temp_raquet)

        assert metadata is not None
        assert metadata.version == "0.1.0"
        assert metadata.block_width == 256
        assert metadata.block_height == 256
        assert len(metadata.bands) >= 1

    def test_metadata_has_bounds(self, temp_raquet):
        """Test that metadata includes bounds."""
        from geoparquet_io.core.raquet import read_raquet_metadata

        metadata = read_raquet_metadata(temp_raquet)

        assert metadata is not None
        assert metadata.bounds is not None
        assert len(metadata.bounds) == 4

    def test_metadata_has_compression(self, temp_raquet):
        """Test that metadata includes compression info."""
        from geoparquet_io.core.raquet import read_raquet_metadata

        metadata = read_raquet_metadata(temp_raquet)

        assert metadata is not None
        assert metadata.compression == "gzip"

    def test_non_raquet_returns_none(self):
        """Test that reading metadata from non-raquet file returns None."""
        from geoparquet_io.core.raquet import read_raquet_metadata

        test_file = "tests/data/places_test.parquet"
        if os.path.exists(test_file):
            assert read_raquet_metadata(test_file) is None


class TestGetBandColumns:
    """Tests for getting band column names."""

    def test_get_bands(self, temp_raquet):
        """Test getting band names from a raquet file."""
        from geoparquet_io.core.raquet import get_band_columns

        bands = get_band_columns(temp_raquet)

        assert len(bands) >= 1
        assert "band_1" in bands


class TestGeoTiffToRaquet:
    """Tests for GeoTIFF to raquet conversion."""

    def test_convert_basic(self, temp_geotiff, temp_output_parquet):
        """Test basic GeoTIFF to raquet conversion."""
        from geoparquet_io.core.raquet import geotiff_to_raquet

        result = geotiff_to_raquet(temp_geotiff, temp_output_parquet)

        assert os.path.exists(temp_output_parquet)
        assert result["num_bands"] >= 1
        assert result["num_blocks"] >= 1

    def test_convert_to_table(self, temp_geotiff):
        """Test converting GeoTIFF to PyArrow table."""
        from geoparquet_io.core.raquet import geotiff_to_raquet_table

        table = geotiff_to_raquet_table(temp_geotiff)

        assert isinstance(table, pa.Table)
        assert "block" in table.column_names
        assert "metadata" in table.column_names
        assert "band_1" in table.column_names

    def test_convert_multiband(self, temp_multiband_geotiff, temp_output_parquet):
        """Test converting multi-band GeoTIFF."""
        from geoparquet_io.core.raquet import geotiff_to_raquet, read_raquet_metadata

        result = geotiff_to_raquet(temp_multiband_geotiff, temp_output_parquet)

        assert result["num_bands"] == 3

        metadata = read_raquet_metadata(temp_output_parquet)
        assert len(metadata.bands) == 3

    def test_block_size_512(self, temp_geotiff, temp_output_parquet):
        """Test conversion with block size 512."""
        from geoparquet_io.core.raquet import geotiff_to_raquet, read_raquet_metadata

        geotiff_to_raquet(temp_geotiff, temp_output_parquet, block_size=512)
        metadata = read_raquet_metadata(temp_output_parquet)

        assert metadata.block_width == 512
        assert metadata.block_height == 512

    def test_no_compression(self, temp_geotiff, temp_output_parquet):
        """Test conversion without block compression."""
        from geoparquet_io.core.raquet import geotiff_to_raquet, read_raquet_metadata

        geotiff_to_raquet(temp_geotiff, temp_output_parquet, compression=None)
        metadata = read_raquet_metadata(temp_output_parquet)

        assert metadata.compression is None


class TestRaquetToGeoTiff:
    """Tests for raquet to GeoTIFF export."""

    def test_export_basic(self, temp_raquet, temp_output_geotiff):
        """Test basic raquet to GeoTIFF export."""
        from geoparquet_io.core.raquet import raquet_to_geotiff

        result = raquet_to_geotiff(temp_raquet, temp_output_geotiff)

        assert os.path.exists(temp_output_geotiff)
        assert result["width"] > 0
        assert result["height"] > 0

    def test_export_roundtrip(self, temp_geotiff, temp_output_parquet, temp_output_geotiff):
        """Test roundtrip conversion GeoTIFF -> raquet -> GeoTIFF."""
        import rasterio

        from geoparquet_io.core.raquet import geotiff_to_raquet, raquet_to_geotiff

        # Convert to raquet
        geotiff_to_raquet(temp_geotiff, temp_output_parquet)

        # Convert back to GeoTIFF
        raquet_to_geotiff(temp_output_parquet, temp_output_geotiff)

        # Verify the output is a valid GeoTIFF
        with rasterio.open(temp_output_geotiff) as src:
            assert src.count >= 1
            assert src.width > 0
            assert src.height > 0
            assert src.crs is not None


class TestRasterCLI:
    """Tests for raster CLI commands."""

    def test_raster_group_help(self, runner):
        """Test that raster group shows help."""
        result = runner.invoke(cli, ["raster", "--help"])

        assert result.exit_code == 0
        assert "raquet" in result.output.lower() or "raster" in result.output.lower()

    def test_raster_inspect(self, runner, temp_raquet):
        """Test raster inspect command."""
        result = runner.invoke(cli, ["raster", "inspect", temp_raquet])

        assert result.exit_code == 0

    def test_raster_inspect_json(self, runner, temp_raquet):
        """Test raster inspect with JSON output."""
        result = runner.invoke(cli, ["raster", "inspect", temp_raquet, "--json"])

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "version" in data
        assert "bands" in data

    def test_raster_convert(self, runner, temp_geotiff, temp_output_parquet):
        """Test raster convert command."""
        result = runner.invoke(cli, ["raster", "convert", temp_geotiff, temp_output_parquet])

        assert result.exit_code == 0
        assert os.path.exists(temp_output_parquet)

    def test_raster_convert_block_size(self, runner, temp_geotiff, temp_output_parquet):
        """Test raster convert with custom block size."""
        result = runner.invoke(
            cli, ["raster", "convert", temp_geotiff, temp_output_parquet, "--block-size", "512"]
        )

        assert result.exit_code == 0

    def test_raster_export(self, runner, temp_raquet, temp_output_geotiff):
        """Test raster export command."""
        result = runner.invoke(cli, ["raster", "export", temp_raquet, temp_output_geotiff])

        assert result.exit_code == 0
        assert os.path.exists(temp_output_geotiff)

    def test_raster_inspect_non_raquet_file(self, runner):
        """Test that inspect fails gracefully on non-raquet files."""
        test_file = "tests/data/places_test.parquet"
        if os.path.exists(test_file):
            result = runner.invoke(cli, ["raster", "inspect", test_file])
            assert result.exit_code != 0
            assert "not a valid raquet file" in result.output.lower()


class TestRasterPythonAPI:
    """Tests for raster Python API."""

    def test_is_raquet(self, temp_raquet):
        """Test is_raquet function."""
        from geoparquet_io.api import raster

        assert raster.is_raquet(temp_raquet) is True

    def test_read_metadata(self, temp_raquet):
        """Test read_metadata function."""
        from geoparquet_io.api import raster

        meta = raster.read_metadata(temp_raquet)

        assert meta is not None
        assert meta.version == "0.1.0"

    def test_get_bands(self, temp_raquet):
        """Test get_bands function."""
        from geoparquet_io.api import raster

        bands = raster.get_bands(temp_raquet)

        assert len(bands) >= 1
        assert "band_1" in bands

    def test_geotiff_to_table(self, temp_geotiff):
        """Test geotiff_to_table function."""
        from geoparquet_io.api import raster

        table = raster.geotiff_to_table(temp_geotiff)

        assert isinstance(table, pa.Table)
        assert "block" in table.column_names

    def test_convert_geotiff(self, temp_geotiff, temp_output_parquet):
        """Test convert_geotiff function."""
        from geoparquet_io.api import raster

        result = raster.convert_geotiff(temp_geotiff, temp_output_parquet)

        assert os.path.exists(temp_output_parquet)
        assert result["num_bands"] >= 1

    def test_export_geotiff(self, temp_raquet, temp_output_geotiff):
        """Test export_geotiff function."""
        from geoparquet_io.api import raster

        result = raster.export_geotiff(temp_raquet, temp_output_geotiff)

        assert os.path.exists(temp_output_geotiff)
        assert result["width"] > 0


class TestExampleRaquet:
    """Tests using the example raquet file from the raquet repo."""

    @pytest.mark.skipif(
        not os.path.exists("/tmp/raquet/examples/example_data.parquet"),
        reason="Example raquet file not available",
    )
    def test_inspect_example(self):
        """Test inspecting the example raquet file."""
        from geoparquet_io.core.raquet import is_raquet_file, read_raquet_metadata

        example_path = "/tmp/raquet/examples/example_data.parquet"

        assert is_raquet_file(example_path) is True

        metadata = read_raquet_metadata(example_path)
        assert metadata is not None
        assert metadata.version is not None
