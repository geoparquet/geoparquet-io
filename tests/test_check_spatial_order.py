"""Tests for core/check_spatial_order.py module."""

import pytest

from geoparquet_io.core.check_spatial_order import (
    _bboxes_overlap,
    check_spatial_order,
    check_spatial_order_bbox_stats,
)


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
        """Test check_spatial_order with poorly ordered file."""
        result = check_spatial_order(
            unsorted_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=True,
            return_results=True,
        )
        assert isinstance(result, dict)
        assert "method" in result
        # Different thresholds for different methods
        if result["method"] == "sampling":
            # Sampling method: passed=False when ratio >= 0.5
            if result["ratio"] >= 0.5:
                assert result["passed"] is False
        elif result["method"] == "bbox_stats":
            # Bbox-stats method: passed=False when ratio >= 0.3
            if result["ratio"] >= 0.3:
                assert result["passed"] is False
        # Check that failure includes proper feedback
        if not result["passed"]:
            assert len(result["issues"]) > 0
            assert len(result["recommendations"]) > 0
            assert "spatial ordering" in str(result["issues"]).lower()


class TestBboxOverlap:
    """Tests for _bboxes_overlap helper function."""

    def test_overlapping_bboxes(self):
        """Test bboxes that overlap."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        bbox2 = {"xmin": 5.0, "ymin": 5.0, "xmax": 15.0, "ymax": 15.0}
        assert _bboxes_overlap(bbox1, bbox2) is True

    def test_adjacent_bboxes_touching_edge(self):
        """Test bboxes that touch at an edge (not overlapping)."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        bbox2 = {"xmin": 10.0, "ymin": 0.0, "xmax": 20.0, "ymax": 10.0}
        assert _bboxes_overlap(bbox1, bbox2) is False

    def test_disjoint_bboxes(self):
        """Test bboxes that are completely separate."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        bbox2 = {"xmin": 20.0, "ymin": 20.0, "xmax": 30.0, "ymax": 30.0}
        assert _bboxes_overlap(bbox1, bbox2) is False

    def test_one_bbox_contains_other(self):
        """Test when one bbox completely contains another."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 20.0, "ymax": 20.0}
        bbox2 = {"xmin": 5.0, "ymin": 5.0, "xmax": 15.0, "ymax": 15.0}
        assert _bboxes_overlap(bbox1, bbox2) is True

    def test_identical_bboxes(self):
        """Test identical bboxes."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        bbox2 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        assert _bboxes_overlap(bbox1, bbox2) is True

    def test_overlap_only_in_x_dimension(self):
        """Test bboxes that overlap in X but not Y (no overlap)."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 5.0}
        bbox2 = {"xmin": 5.0, "ymin": 10.0, "xmax": 15.0, "ymax": 15.0}
        assert _bboxes_overlap(bbox1, bbox2) is False

    def test_overlap_only_in_y_dimension(self):
        """Test bboxes that overlap in Y but not X (no overlap)."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 5.0, "ymax": 10.0}
        bbox2 = {"xmin": 10.0, "ymin": 5.0, "xmax": 15.0, "ymax": 15.0}
        assert _bboxes_overlap(bbox1, bbox2) is False

    def test_touching_at_corner(self):
        """Test bboxes that touch only at a corner point (not overlapping)."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        bbox2 = {"xmin": 10.0, "ymin": 10.0, "xmax": 20.0, "ymax": 20.0}
        assert _bboxes_overlap(bbox1, bbox2) is False

    def test_negative_coordinates(self):
        """Test with negative coordinate values."""
        bbox1 = {"xmin": -10.0, "ymin": -10.0, "xmax": 0.0, "ymax": 0.0}
        bbox2 = {"xmin": -5.0, "ymin": -5.0, "xmax": 5.0, "ymax": 5.0}
        assert _bboxes_overlap(bbox1, bbox2) is True

    def test_bbox_ordering_doesnt_matter(self):
        """Test that order of bbox arguments doesn't affect result."""
        bbox1 = {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0}
        bbox2 = {"xmin": 5.0, "ymin": 5.0, "xmax": 15.0, "ymax": 15.0}
        assert _bboxes_overlap(bbox1, bbox2) == _bboxes_overlap(bbox2, bbox1)


class TestCheckSpatialOrderBboxStats:
    """Tests for check_spatial_order_bbox_stats function."""

    def test_returns_dict_structure(self, places_test_file):
        """Test that bbox-stats method returns proper dict structure."""
        result = check_spatial_order_bbox_stats(
            places_test_file, verbose=False, return_results=True, quiet=False
        )
        assert isinstance(result, dict)
        assert "passed" in result
        assert "ratio" in result
        assert "method" in result
        assert result["method"] == "bbox_stats"
        assert "overlap_count" in result
        assert "total_pairs" in result
        assert "issues" in result
        assert "recommendations" in result
        assert "fix_available" in result

    def test_with_spatially_ordered_file(self, places_test_file):
        """Test with file expected to have good spatial ordering."""
        result = check_spatial_order_bbox_stats(
            places_test_file, verbose=False, return_results=True, quiet=False
        )
        # Just verify structure - actual ordering depends on test data
        assert isinstance(result["passed"], bool)
        assert isinstance(result["ratio"], float)
        assert 0.0 <= result["ratio"] <= 1.0
        assert result["overlap_count"] >= 0
        assert result["total_pairs"] >= 0

    def test_without_bbox_column_raises_error(self, buildings_test_file):
        """Test that files without bbox column raise ValueError."""
        with pytest.raises(ValueError, match="does not have a bbox column"):
            check_spatial_order_bbox_stats(
                buildings_test_file, verbose=False, return_results=True, quiet=False
            )

    def test_verbose_mode(self, places_test_file):
        """Test bbox-stats method with verbose=True."""
        result = check_spatial_order_bbox_stats(
            places_test_file, verbose=True, return_results=True, quiet=False
        )
        assert isinstance(result, dict)
        assert result["method"] == "bbox_stats"

    def test_quiet_mode(self, places_test_file):
        """Test bbox-stats method with quiet=True."""
        result = check_spatial_order_bbox_stats(
            places_test_file, verbose=False, return_results=True, quiet=True
        )
        assert isinstance(result, dict)
        assert result["method"] == "bbox_stats"

    def test_passing_threshold(self, places_test_file):
        """Test that ratio < 0.3 means passed=True."""
        result = check_spatial_order_bbox_stats(
            places_test_file, verbose=False, return_results=True, quiet=False
        )
        if result["ratio"] < 0.3:
            assert result["passed"] is True
            assert len(result["issues"]) == 0
            assert result["fix_available"] is False
        else:
            assert result["passed"] is False
            assert len(result["issues"]) > 0
            assert result["fix_available"] is True

    def test_issues_and_recommendations_when_failed(self, unsorted_test_file):
        """Test that failing check includes proper issues and recommendations."""
        result = check_spatial_order_bbox_stats(
            unsorted_test_file, verbose=False, return_results=True, quiet=False
        )
        if not result["passed"]:
            assert len(result["issues"]) > 0
            assert len(result["recommendations"]) > 0
            assert "spatial ordering" in str(result["issues"]).lower()
            assert "Hilbert" in str(result["recommendations"])

    def test_without_return_results(self, places_test_file):
        """Test bbox-stats method with return_results=False."""
        result = check_spatial_order_bbox_stats(
            places_test_file, verbose=False, return_results=False, quiet=False
        )
        # Should return ratio directly when return_results=False
        assert result is None or isinstance(result, float)


class TestAutoDetectionAndFallback:
    """Tests for automatic method detection and fallback behavior."""

    def test_auto_detection_uses_bbox_stats_when_available(self, places_test_file):
        """Test that check_spatial_order auto-detects and uses bbox-stats."""
        result = check_spatial_order(
            places_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=True,
        )
        # Should use bbox_stats method automatically
        assert result["method"] == "bbox_stats"

    def test_fallback_to_sampling_when_no_bbox(self, buildings_test_file):
        """Test that check_spatial_order falls back to sampling when no bbox."""
        result = check_spatial_order(
            buildings_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=True,
        )
        # Should fall back to sampling method
        assert result["method"] == "sampling"
        assert "consecutive_avg" in result
        assert "random_avg" in result

    def test_fallback_uses_sampling_method(self, buildings_test_file):
        """Test that fallback correctly uses sampling method."""
        result = check_spatial_order(
            buildings_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=True,
            quiet=True,  # Suppress warnings for test
        )
        # Should use sampling method when no bbox column
        assert result["method"] == "sampling"

    def test_bbox_stats_method_indicated_in_result(self, places_test_file):
        """Test that method field correctly indicates bbox_stats."""
        result = check_spatial_order(
            places_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=True,
        )
        assert "method" in result
        assert result["method"] in ["bbox_stats", "sampling"]

    def test_return_structure_consistent_across_methods(
        self, places_test_file, buildings_test_file
    ):
        """Test that both methods return compatible structures."""
        bbox_result = check_spatial_order(
            places_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=True,
        )
        sampling_result = check_spatial_order(
            buildings_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=True,
        )
        # Both should have these common fields
        for field in ["passed", "ratio", "issues", "recommendations", "fix_available", "method"]:
            assert field in bbox_result
            assert field in sampling_result


class TestNativeGeoBboxDetection:
    """Tests for native geo_bbox stats detection (fixes #410)."""

    def test_check_spatial_order_with_native_geo_bbox(self, tmp_path):
        """Test that check_spatial_order detects native geo_bbox stats from GeoParquet 2.0."""
        import duckdb
        import pytest

        # Create a GeoParquet 2.0 file (without separate bbox column, but with native geo_bbox)
        output_file = str(tmp_path / "test_v2.parquet")
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Create test data with geometry
        conn.execute(f"""
            COPY (
                SELECT
                    ST_Point(i * 0.01, i * 0.01) as geometry,
                    i as id
                FROM range(100) t(i)
            ) TO '{output_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD, GEOPARQUET_VERSION 'V2')
        """)

        # Verify no bbox column exists
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "bbox" not in column_names, "Test file should not have bbox column"

        # Check for native geo_bbox stats - skip if DuckDB version doesn't support them
        geo_bbox_result = conn.execute(f"""
            SELECT geo_bbox
            FROM parquet_metadata('{output_file}')
            WHERE path_in_schema = 'geometry'
              AND geo_bbox IS NOT NULL
            LIMIT 1
        """).fetchone()

        if not geo_bbox_result:
            pytest.skip(
                "DuckDB version does not populate geo_bbox stats for GeoParquet V2 - "
                "cannot test native geo_bbox detection"
            )

        from geoparquet_io.core.check_spatial_order import check_spatial_order

        result = check_spatial_order(
            output_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=True,
        )

        # Should use native_geo_bbox method, not sampling
        assert result["method"] in ("native_geo_bbox", "bbox_stats"), (
            f"Expected native_geo_bbox or bbox_stats method, got {result['method']}"
        )

    def test_helper_function_returns_correct_structure(self):
        """Test that _check_spatial_order_from_row_group_bboxes returns correct structure."""
        from geoparquet_io.core.check_spatial_order import (
            _check_spatial_order_from_row_group_bboxes,
        )

        # Mock row group bboxes
        row_group_bboxes = [
            {"row_group_id": 0, "xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0},
            {"row_group_id": 1, "xmin": 0.5, "ymin": 0.5, "xmax": 1.5, "ymax": 1.5},
            {"row_group_id": 2, "xmin": 1.0, "ymin": 1.0, "xmax": 2.0, "ymax": 2.0},
        ]

        result = _check_spatial_order_from_row_group_bboxes(
            row_group_bboxes,
            parquet_file="test.parquet",
            verbose=False,
            return_results=True,
            quiet=True,
        )

        # Verify structure
        assert "passed" in result
        assert "ratio" in result
        assert "method" in result
        assert result["method"] == "native_geo_bbox"
        assert "overlap_count" in result
        assert "total_pairs" in result


class TestSpatialLocalitySecondaryCheck:
    """Tests for secondary locality metrics (area ratio + skip rate).

    When consecutive overlap ratio is high but data has good spatial locality
    (tight row group bboxes, high skip rate), the check should still pass.
    This is the common pattern with Hilbert-sorted global data.
    """

    def test_hilbert_like_overlapping_but_tight_bboxes_pass(self):
        """Hilbert-sorted data: consecutive RGs overlap but bboxes are tight."""
        from geoparquet_io.core.check_spatial_order import (
            _check_spatial_order_from_row_group_bboxes,
        )

        # Simulate Hilbert-sorted global data: consecutive RGs overlap
        # but each covers only a small fraction of the total extent.
        # Modeled after real Hilbert-sorted data with 9 row groups.
        row_group_bboxes = [
            {"row_group_id": 0, "xmin": 0.0, "ymin": 0.0, "xmax": 20.0, "ymax": 15.0},
            {"row_group_id": 1, "xmin": 5.0, "ymin": 10.0, "xmax": 20.0, "ymax": 15.0},
            {"row_group_id": 2, "xmin": 15.0, "ymin": 5.0, "xmax": 35.0, "ymax": 12.0},
            {"row_group_id": 3, "xmin": 25.0, "ymin": 7.0, "xmax": 35.0, "ymax": 12.0},
            {"row_group_id": 4, "xmin": 30.0, "ymin": 10.0, "xmax": 45.0, "ymax": 13.0},
            {"row_group_id": 5, "xmin": 40.0, "ymin": 9.0, "xmax": 45.0, "ymax": 13.0},
            {"row_group_id": 6, "xmin": 38.0, "ymin": 7.0, "xmax": 45.0, "ymax": 10.0},
            {"row_group_id": 7, "xmin": 25.0, "ymin": 0.0, "xmax": 45.0, "ymax": 10.0},
            {"row_group_id": 8, "xmin": 25.0, "ymin": -5.0, "xmax": 44.0, "ymax": 1.0},
        ]
        # All consecutive pairs overlap → overlap ratio = 1.0
        # But each RG covers a small fraction of total extent (45x20=900)

        result = _check_spatial_order_from_row_group_bboxes(
            row_group_bboxes,
            parquet_file="test_hilbert.parquet",
            verbose=False,
            return_results=True,
            quiet=True,
        )

        assert result["ratio"] >= 0.3, "Overlap ratio should be high"
        assert result["passed"] is True, (
            "Should pass because bbox area ratio is low and skip rate is high"
        )
        assert "estimated_skip_rate" in result
        assert "avg_bbox_area_ratio" in result

    def test_genuinely_poor_spatial_order_still_fails(self):
        """Truly unordered data: large overlapping bboxes covering most of extent."""
        from geoparquet_io.core.check_spatial_order import (
            _check_spatial_order_from_row_group_bboxes,
        )

        # Each RG bbox covers nearly the entire extent → poor locality
        row_group_bboxes = [
            {"row_group_id": 0, "xmin": 0.0, "ymin": 0.0, "xmax": 95.0, "ymax": 95.0},
            {"row_group_id": 1, "xmin": 5.0, "ymin": 5.0, "xmax": 100.0, "ymax": 100.0},
            {"row_group_id": 2, "xmin": 2.0, "ymin": 2.0, "xmax": 98.0, "ymax": 98.0},
            {"row_group_id": 3, "xmin": 1.0, "ymin": 1.0, "xmax": 99.0, "ymax": 99.0},
            {"row_group_id": 4, "xmin": 3.0, "ymin": 3.0, "xmax": 97.0, "ymax": 97.0},
        ]

        result = _check_spatial_order_from_row_group_bboxes(
            row_group_bboxes,
            parquet_file="test_unsorted.parquet",
            verbose=False,
            return_results=True,
            quiet=True,
        )

        assert result["ratio"] >= 0.3, "Overlap ratio should be high"
        assert result["passed"] is False, (
            "Should fail because bboxes cover nearly the entire extent"
        )

    def test_two_row_groups_are_measured_not_skipped(self):
        """Two row groups get the locality check like any other count (#755).

        This used to assert the opposite: the check was gated on >= 3 row groups,
        so a two-group file whose boxes overlap at all was reported "Poor spatial
        ordering (overlap ratio: 1.00)" with no metrics computed. These two boxes
        prune exactly as well as two row groups allow -- estimated and ideal skip
        rates are both 0.475 -- so failing it was measuring the row-group count,
        not the ordering.
        """
        from geoparquet_io.core.check_spatial_order import (
            _check_spatial_order_from_row_group_bboxes,
        )

        row_group_bboxes = [
            {"row_group_id": 0, "xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0},
            {"row_group_id": 1, "xmin": 5.0, "ymin": 5.0, "xmax": 15.0, "ymax": 15.0},
        ]

        result = _check_spatial_order_from_row_group_bboxes(
            row_group_bboxes,
            parquet_file="test_two_rg.parquet",
            verbose=False,
            return_results=True,
            quiet=True,
        )

        assert result["ratio"] == 1.0, "Both groups overlap - kept as a statistic"
        assert result["passed"] is True
        assert result["skip_rate_efficiency"] == pytest.approx(1.0)
        assert result["estimated_skip_rate"] is not None
        assert result["avg_bbox_area_ratio"] is not None

    def test_hilbert_few_large_row_groups_pass(self):
        """Hilbert-sorted data with few large row groups must still pass.

        With only ~5 row groups each Hilbert segment legitimately covers a
        larger share of the extent (roughly 1/N plus bbox slop), so the
        area-ratio threshold must scale with the group count. Regression test
        for the removed 80k-120k rows/group heuristic.
        """
        from geoparquet_io.core.check_spatial_order import (
            _check_spatial_order_from_row_group_bboxes,
        )

        # 5 overlapping quadrant-traversal segments over a 100x100 extent,
        # avg bbox area ratio ~0.29 (above the fixed 0.25 cutoff) but high
        # skip rate (~0.64)
        row_group_bboxes = [
            {"row_group_id": 0, "xmin": 0.0, "ymin": 0.0, "xmax": 55.0, "ymax": 55.0},
            {"row_group_id": 1, "xmin": 0.0, "ymin": 45.0, "xmax": 55.0, "ymax": 100.0},
            {"row_group_id": 2, "xmin": 45.0, "ymin": 45.0, "xmax": 100.0, "ymax": 100.0},
            {"row_group_id": 3, "xmin": 45.0, "ymin": 0.0, "xmax": 100.0, "ymax": 55.0},
            {"row_group_id": 4, "xmin": 40.0, "ymin": 0.0, "xmax": 90.0, "ymax": 50.0},
        ]

        result = _check_spatial_order_from_row_group_bboxes(
            row_group_bboxes,
            parquet_file="test_hilbert_few_groups.parquet",
            verbose=False,
            return_results=True,
            quiet=True,
        )

        assert result["ratio"] == 1.0, "All consecutive pairs overlap"
        assert result["passed"] is True, (
            "Should pass via the locality check with the count-scaled area threshold"
        )
        assert result["fix_available"] is False

    def test_print_path_reflects_secondary_pass(self, caplog):
        """Standalone print output must match the structured passed verdict."""
        import logging

        from geoparquet_io.core.check_spatial_order import (
            _check_spatial_order_from_row_group_bboxes,
        )

        row_group_bboxes = [
            {"row_group_id": 0, "xmin": 0.0, "ymin": 0.0, "xmax": 20.0, "ymax": 15.0},
            {"row_group_id": 1, "xmin": 5.0, "ymin": 10.0, "xmax": 20.0, "ymax": 15.0},
            {"row_group_id": 2, "xmin": 15.0, "ymin": 5.0, "xmax": 35.0, "ymax": 12.0},
            {"row_group_id": 3, "xmin": 25.0, "ymin": 7.0, "xmax": 35.0, "ymax": 12.0},
            {"row_group_id": 4, "xmin": 30.0, "ymin": 10.0, "xmax": 45.0, "ymax": 13.0},
            {"row_group_id": 5, "xmin": 40.0, "ymin": 9.0, "xmax": 45.0, "ymax": 13.0},
            {"row_group_id": 6, "xmin": 38.0, "ymin": 7.0, "xmax": 45.0, "ymax": 10.0},
            {"row_group_id": 7, "xmin": 25.0, "ymin": 0.0, "xmax": 45.0, "ymax": 10.0},
            {"row_group_id": 8, "xmin": 25.0, "ymin": -5.0, "xmax": 44.0, "ymax": 1.0},
        ]

        # Sanity: this fixture passes via the secondary locality check
        result = _check_spatial_order_from_row_group_bboxes(
            row_group_bboxes, "hilbert.parquet", return_results=True, quiet=True
        )
        assert result["passed"] is True and result["ratio"] >= 0.3

        with caplog.at_level(logging.INFO):
            _check_spatial_order_from_row_group_bboxes(
                row_group_bboxes, "hilbert.parquet", return_results=False, quiet=False
            )

        messages = " ".join(r.message for r in caplog.records)
        assert "may benefit from spatial ordering" not in messages
        assert "well spatially ordered" in messages

    def test_return_ratio_when_return_results_false(self):
        """_check_spatial_order_from_row_group_bboxes returns float when return_results=False."""
        from geoparquet_io.core.check_spatial_order import (
            _check_spatial_order_from_row_group_bboxes,
        )

        row_group_bboxes = [
            {"row_group_id": 0, "xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0},
            {"row_group_id": 1, "xmin": 20.0, "ymin": 20.0, "xmax": 30.0, "ymax": 30.0},
            {"row_group_id": 2, "xmin": 40.0, "ymin": 40.0, "xmax": 50.0, "ymax": 50.0},
        ]

        result = _check_spatial_order_from_row_group_bboxes(
            row_group_bboxes,
            parquet_file="test_ratio.parquet",
            verbose=False,
            return_results=False,
            quiet=True,
        )

        assert isinstance(result, float)
        assert result == 0.0

    def test_secondary_metrics_included_in_bbox_stats_result(self, places_test_file):
        """check_spatial_order_bbox_stats should include locality metrics."""
        result = check_spatial_order_bbox_stats(
            places_test_file, verbose=False, return_results=True, quiet=True
        )
        assert "estimated_skip_rate" in result
        assert "avg_bbox_area_ratio" in result
