"""Tests for check_spatial_order helper functions."""

from geoparquet_io.core.check_spatial_order import _build_results_dict


class TestBuildResultsDict:
    """The sampling fallback reports statistics and withholds its verdict.

    The spec judges a file without row-group statistics by its rows (its row
    check), which gpio does not run (portolan-spec#188).
    """

    def test_the_distances_are_kept_as_statistics(self):
        result = _build_results_dict(ratio=0.3, consecutive_avg=10.0, random_avg=33.33)

        assert result["ratio"] == 0.3
        assert result["consecutive_avg"] == 10.0
        assert result["random_avg"] == 33.33
        assert result["method"] == "sampling"
        assert any("not judged" in w for w in result["warnings"])

    def test_no_ratio_gives_a_verdict(self):
        """Low, at the old 0.5 threshold, high or missing: never a pass or a failure."""
        for ratio in (0.3, 0.499, 0.5, 0.8, None):
            result = _build_results_dict(ratio=ratio, consecutive_avg=1.0, random_avg=2.0)

            assert result["judged"] is False, f"ratio={ratio}"
            assert result["passed"] is True, "no failure found"
            assert result["issues"] == []
            assert result["recommendations"] == []
            # A withheld verdict is not known-ordered: an explicit fix sorts.
            assert result["fix_available"] is True
