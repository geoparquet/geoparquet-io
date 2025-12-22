"""Tests for core/check_spatial_order.py module."""

from geoparquet_io.core.check_spatial_order import check_spatial_order


class TestCheckSpatialOrder:
    """Tests for check_spatial_order function."""

    def test_returns_results(self, places_test_file):
        """Test check_spatial_order with return_results=True."""
        result = check_spatial_order(
            places_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=True,
        )
        assert isinstance(result, dict)
        assert "passed" in result

    def test_with_verbose(self, places_test_file):
        """Test check_spatial_order with verbose flag."""
        result = check_spatial_order(
            places_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=True,
            return_results=True,
        )
        assert isinstance(result, dict)

    def test_with_small_sample(self, places_test_file):
        """Test check_spatial_order with small sample size."""
        result = check_spatial_order(
            places_test_file,
            random_sample_size=10,
            limit_rows=100,
            verbose=False,
            return_results=True,
        )
        assert isinstance(result, dict)

    def test_buildings_file(self, buildings_test_file):
        """Test check_spatial_order on buildings file."""
        result = check_spatial_order(
            buildings_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=True,
        )
        assert isinstance(result, dict)
        assert "passed" in result

    def test_without_return_results(self, places_test_file):
        """Test check_spatial_order with return_results=False (covers line 144)."""
        result = check_spatial_order(
            places_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=False,
        )
        # When return_results=False, returns the ratio directly
        assert result is None or isinstance(result, float)

    def test_poorly_ordered_file(self, unsorted_test_file):
        """Test check_spatial_order with poorly ordered file (covers lines 122-123, 131-132)."""
        result = check_spatial_order(
            unsorted_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=True,  # Use verbose to cover line 122-123 output
            return_results=True,
        )
        assert isinstance(result, dict)
        assert result["passed"] is False
        assert result["ratio"] >= 0.5
        assert len(result["issues"]) > 0
        assert len(result["recommendations"]) > 0
        assert "Poor spatial ordering" in result["issues"][0]
