"""Tests for add_quadkey_column module."""

import tempfile
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.core.add_quadkey_column import (
    _is_geographic_crs,
    _lat_lon_to_quadkey,
)
from geoparquet_io.core.common import get_crs_display_name
from tests.conftest import safe_unlink


class TestLatLonToQuadkey:
    """Tests for _lat_lon_to_quadkey function."""

    def test_known_location(self):
        """Test quadkey generation for a known location."""
        # San Francisco area at zoom level 10
        quadkey = _lat_lon_to_quadkey(37.7749, -122.4194, 10)
        assert isinstance(quadkey, str)
        assert len(quadkey) == 10

    def test_equator_prime_meridian(self):
        """Test quadkey at equator/prime meridian."""
        quadkey = _lat_lon_to_quadkey(0.0, 0.0, 5)
        assert isinstance(quadkey, str)
        assert len(quadkey) == 5

    def test_different_resolutions(self):
        """Test that higher resolution produces longer quadkeys."""
        lat, lon = 40.7128, -74.0060  # New York
        qk_low = _lat_lon_to_quadkey(lat, lon, 5)
        qk_high = _lat_lon_to_quadkey(lat, lon, 15)
        assert len(qk_low) == 5
        assert len(qk_high) == 15
        # Higher resolution should start with the lower resolution key
        assert qk_high.startswith(qk_low)


class TestIsGeographicCrs:
    """Tests for _is_geographic_crs function."""

    def test_none_crs(self):
        """Test with None CRS."""
        assert _is_geographic_crs(None) is None

    def test_epsg_4326_string(self):
        """Test WGS84 EPSG:4326 string."""
        assert _is_geographic_crs("EPSG:4326") is True
        assert _is_geographic_crs("epsg:4326") is True

    def test_crs84_string(self):
        """Test CRS84 string."""
        assert _is_geographic_crs("OGC:CRS84") is True
        assert _is_geographic_crs("CRS:84") is True

    def test_unknown_string(self):
        """Test unknown CRS string."""
        assert _is_geographic_crs("EPSG:3857") is None  # Web Mercator

    def test_geographic_crs_dict(self):
        """Test GeographicCRS type in dict."""
        crs_dict = {"type": "GeographicCRS", "name": "WGS 84"}
        assert _is_geographic_crs(crs_dict) is True

    def test_projected_crs_dict(self):
        """Test ProjectedCRS type in dict."""
        crs_dict = {"type": "ProjectedCRS", "name": "Web Mercator"}
        assert _is_geographic_crs(crs_dict) is False

    def test_crs_dict_with_epsg_code(self):
        """Test CRS dict with EPSG code."""
        crs_dict = {"id": {"authority": "EPSG", "code": 4326}}
        assert _is_geographic_crs(crs_dict) is True


class TestValidateCrsForQuadkey:
    """Tests for CRS validation."""

    def test_none_crs_returns_true(self):
        """Test with None CRS (assumes WGS84)."""
        assert _is_geographic_crs(None) is None

    def test_epsg_4269_is_geographic(self):
        """Test EPSG:4269 (NAD83) is recognized as geographic."""
        assert _is_geographic_crs("EPSG:4269") is True

    def test_epsg_4267_is_geographic(self):
        """Test EPSG:4267 (NAD27) is recognized as geographic."""
        assert _is_geographic_crs("EPSG:4267") is True

    def test_dict_with_epsg_4269(self):
        """Test dict with NAD83 code."""
        crs_dict = {"id": {"authority": "EPSG", "code": 4269}}
        assert _is_geographic_crs(crs_dict) is True


class TestGetCrsDisplayName:
    """Tests for get_crs_display_name function (shared from common.py)."""

    def test_none_crs(self):
        """Test with None CRS."""
        assert get_crs_display_name(None) == "None (OGC:CRS84)"

    def test_string_crs(self):
        """Test with string CRS."""
        assert get_crs_display_name("EPSG:4326") == "EPSG:4326"

    def test_dict_with_name_and_code(self):
        """Test dict with name and code."""
        crs_dict = {"name": "WGS 84", "id": {"authority": "EPSG", "code": 4326}}
        result = get_crs_display_name(crs_dict)
        assert "WGS 84" in result
        assert "4326" in result

    def test_dict_with_only_code(self):
        """Test dict with only code."""
        crs_dict = {"id": {"authority": "EPSG", "code": 4326}}
        assert get_crs_display_name(crs_dict) == "EPSG:4326"

    def test_empty_dict(self):
        """Test with empty dict."""
        assert get_crs_display_name({}) == "PROJJSON object"


class TestAddQuadkeyCommand:
    """Tests for the add quadkey CLI command."""

    @pytest.fixture
    def sample_file(self):
        """Return path to the sample file."""
        return str(Path(__file__).parent / "data" / "sample.parquet")

    @pytest.fixture
    def output_file(self):
        """Create a temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_quadkey_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_add_quadkey_help(self):
        """Test that add quadkey command has help."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["add", "quadkey", "--help"])
        assert result.exit_code == 0
        assert "quadkey" in result.output.lower()

    def test_add_quadkey_invalid_resolution_via_cli(self, sample_file, output_file):
        """Test with invalid resolution via CLI."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(
            cli, ["add", "quadkey", sample_file, output_file, "--resolution", "25"]
        )
        # Should fail - resolution out of range
        assert result.exit_code != 0
