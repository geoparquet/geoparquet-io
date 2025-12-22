"""Tests for core/split_by_country.py module."""

import pytest
from click import UsageError

from geoparquet_io.core.split_by_country import (
    _is_wgs84,
    check_country_code_column,
    check_crs,
)


class TestIsWgs84:
    """Tests for _is_wgs84 function."""

    def test_none_returns_true(self):
        """Test that None CRS returns True (default is WGS84)."""
        assert _is_wgs84(None) is True

    def test_epsg_4326_string(self):
        """Test EPSG:4326 string is WGS84."""
        assert _is_wgs84("EPSG:4326") is True
        assert _is_wgs84("4326") is True

    def test_urn_crs84_string(self):
        """Test URN CRS:84 format is WGS84."""
        assert _is_wgs84("urn:ogc:def:crs:OGC:1.3:CRS84") is True

    def test_wgs84_name(self):
        """Test WGS84 by name."""
        assert _is_wgs84("WGS84") is True
        assert _is_wgs84("WGS 84") is True

    def test_dict_geographic_crs(self):
        """Test dict with GeographicCRS type containing WGS84."""
        wgs84_dict = {"type": "GeographicCRS", "name": "WGS 84"}
        assert _is_wgs84(wgs84_dict) is True

    def test_non_wgs84(self):
        """Test non-WGS84 CRS returns False."""
        assert _is_wgs84("EPSG:3857") is False

    def test_empty_string(self):
        """Test empty string returns True."""
        assert _is_wgs84("") is True

    def test_dict_non_wgs84_geographic_crs(self):
        """Test dict with GeographicCRS but not WGS84 returns False (covers line 96)."""
        # Dict with GeographicCRS type but not WGS84
        non_wgs84_dict = {"type": "GeographicCRS", "name": "NAD83"}
        assert _is_wgs84(non_wgs84_dict) is False

    def test_dict_projected_crs(self):
        """Test dict with ProjectedCRS returns False."""
        projected_dict = {"type": "ProjectedCRS", "name": "Web Mercator"}
        assert _is_wgs84(projected_dict) is False

    def test_dict_unknown_type(self):
        """Test dict with unknown type returns False."""
        unknown_dict = {"type": "Unknown", "name": "Something"}
        assert _is_wgs84(unknown_dict) is False

    def test_non_string_non_dict_returns_false(self):
        """Test that non-string, non-dict types return False (covers line 96)."""
        # Pass a list (neither string nor dict)
        assert _is_wgs84([4326]) is False
        # Pass an int
        assert _is_wgs84(4326) is False
        # Pass a tuple
        assert _is_wgs84(("EPSG", 4326)) is False


class TestCheckCrs:
    """Tests for check_crs function."""

    def test_with_wgs84_file(self, places_test_file):
        """Test check_crs with WGS84 file."""
        # Should not raise
        check_crs(places_test_file, verbose=False)

    def test_with_verbose(self, places_test_file):
        """Test check_crs with verbose flag."""
        # Should not raise
        check_crs(places_test_file, verbose=True)


class TestCheckCountryCodeColumn:
    """Tests for check_country_code_column function."""

    def test_raises_for_missing_column(self, places_test_file):
        """Test that missing column raises UsageError."""
        with pytest.raises(UsageError) as exc_info:
            check_country_code_column(places_test_file, "nonexistent_column")
        assert "not found" in str(exc_info.value)
