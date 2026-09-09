"""Tests for core/check_parquet_structure.py module.

Fixtures used in this module:
    - places_test_file: Provided by conftest.py
    - buildings_test_file: Provided by conftest.py
"""

from geoparquet_io.core.check_parquet_structure import (
    GENERAL_ROW_COUNT_RANGE,
    SPATIAL_ROW_COUNT_RANGE,
    CheckProfile,
    assess_row_count,
    assess_row_group_size,
    check_compression,
    check_metadata_and_bbox,
    check_row_groups,
    get_compression_info,
    get_row_group_stats,
)


class TestAssessRowGroupSize:
    """Tests for assess_row_group_size function."""

    def test_optimal_for_small_file(self):
        """Test optimal status for small files."""
        # Small file under 64 MB
        total_size = 50 * 1024 * 1024  # 50 MB
        avg_group_size = 50 * 1024 * 1024  # 50 MB
        status, message, color = assess_row_group_size(avg_group_size, total_size)
        assert status == "optimal"
        assert color == "green"

    def test_optimal_for_64_to_256_mb(self):
        """Test optimal status for 64-256 MB row groups."""
        total_size = 500 * 1024 * 1024  # 500 MB total
        avg_group_size = 128 * 1024 * 1024  # 128 MB
        status, message, color = assess_row_group_size(avg_group_size, total_size)
        assert status == "optimal"
        assert "64-256 MB" in message
        assert color == "green"

    def test_suboptimal_for_32_to_64_mb(self):
        """Test suboptimal status for 32-64 MB row groups."""
        total_size = 500 * 1024 * 1024  # 500 MB total
        avg_group_size = 40 * 1024 * 1024  # 40 MB
        status, message, color = assess_row_group_size(avg_group_size, total_size)
        assert status == "suboptimal"
        assert color == "yellow"

    def test_suboptimal_for_256_to_512_mb(self):
        """Test suboptimal status for 256-512 MB row groups."""
        total_size = 1000 * 1024 * 1024  # 1 GB total
        avg_group_size = 300 * 1024 * 1024  # 300 MB
        status, message, color = assess_row_group_size(avg_group_size, total_size)
        assert status == "suboptimal"
        assert color == "yellow"

    def test_poor_for_very_small_groups(self):
        """Test poor status for very small row groups."""
        total_size = 500 * 1024 * 1024  # 500 MB total
        avg_group_size = 10 * 1024 * 1024  # 10 MB
        status, message, color = assess_row_group_size(avg_group_size, total_size)
        assert status == "poor"
        assert color == "red"

    def test_optimal_for_web(self):
        """Test poor status for very large row groups."""
        total_size = 2000 * 1024 * 1024  # 2 GB total
        avg_group_size = 10 * 1024 * 1024  # 10 MB
        status, message, color = assess_row_group_size(
            avg_group_size, total_size, profile=CheckProfile.web
        )
        assert status == "optimal"
        assert color == "green"

    def test_excessively_large_for_web(self):
        """Test poor status for very large row groups."""
        total_size = 2000 * 1024 * 1024  # 2 GB total
        avg_group_size = 300 * 1024 * 1024  # 300 MB
        status, message, color = assess_row_group_size(
            avg_group_size, total_size, profile=CheckProfile.web
        )
        assert status == "poor"
        assert color == "red"

    def test_suboptimal_for_excessively_small_groups_on_web(self):
        """Test poor status for very large row groups."""
        total_size = 2000 * 1024 * 1024  # 2 GB total
        avg_group_size = 1 * 1024 * 1024  # 1 MB
        status, message, color = assess_row_group_size(
            avg_group_size, total_size, profile=CheckProfile.web
        )
        assert status == "suboptimal"
        assert color == "yellow"

    def test_poor_for_very_large_groups(self):
        """Test poor status for very large row groups."""
        total_size = 2000 * 1024 * 1024  # 2 GB total
        avg_group_size = 600 * 1024 * 1024  # 600 MB
        status, message, color = assess_row_group_size(avg_group_size, total_size)
        assert status == "poor"
        assert color == "red"

    def test_no_profile_is_same_as_omitted_profile(self):
        """Test no profile gives the same result as omitting a profile entirely."""
        # Small file under 64 MB
        total_size = 50 * 1024 * 1024  # 50 MB
        avg_group_size = 50 * 1024 * 1024  # 50 MB
        statusNoProfile, message, colorNoProfile = assess_row_group_size(
            avg_group_size, total_size, profile=None
        )
        statusOmittedProfile, message, colorOmittedProfile = assess_row_group_size(
            avg_group_size, total_size
        )
        assert statusNoProfile == statusOmittedProfile == "optimal"
        assert colorNoProfile == colorOmittedProfile == "green"


class TestAssessRowCount:
    """Tests for assess_row_count function."""

    def test_optimal_for_10k_to_200k(self):
        """Test optimal status for 10,000-200,000 rows."""
        status, message, color = assess_row_count(100000)
        assert status == "optimal"
        assert color == "green"

    def test_poor_for_very_low_rows(self):
        """Test poor status for very low row count."""
        status, message, color = assess_row_count(500)
        assert status == "poor"
        assert "very low" in message
        assert color == "red"

    def test_poor_for_very_high_rows(self):
        """Test poor status for very high row count."""
        status, message, color = assess_row_count(2000000)
        assert status == "poor"
        assert "very high" in message
        assert color == "red"

    def test_suboptimal_for_between_ranges(self):
        """Test suboptimal status for row counts between optimal and poor."""
        # 2000 < rows < 10000 (below optimal range)
        status, message, color = assess_row_count(5000)
        assert status == "suboptimal"
        assert color == "yellow"

        # 200000 < rows < 1000000 (above optimal range)
        status, message, color = assess_row_count(500000)
        assert status == "suboptimal"
        assert color == "yellow"

    def test_optimal_for_small_file_single_group(self):
        """Test small file leniency: any row count is optimal for small single-group files."""
        # Small file under 64MB with single row group
        total_size = 50 * 1024 * 1024  # 50 MB
        low_row_count = 100  # Normally would be "poor"
        status, message, color = assess_row_count(
            low_row_count, total_size_bytes=total_size, num_groups=1
        )
        assert status == "optimal"
        assert color == "green"
        assert "appropriate for small file" in message


class TestRowCountBands:
    """The two row-count bands must not contradict each other (#795).

    ``gpio check`` carries a wide pass/fail band and a narrower band it
    *advises* for spatial pruning. Both are legitimate, but every place that
    quotes either one has to quote the same numbers.
    """

    def test_general_band_accepts_mainstream_writer_defaults(self):
        """The pass/fail band must not fail files at a writer's own default.

        gpio's general write default lands at 100,352 rows (100,000 rounded up
        to a multiple of 2,048), DuckDB writes 122,880, and ``gpio sort``
        writes 51,200. A verdict band that fails those is reporting on the
        ecosystem, not on the file.
        """
        for rows in (51_200, 100_352, 122_880):
            status, _message, _color = assess_row_count(rows)
            assert status == "optimal", f"{rows:,} rows per group should not fail the check"

    def test_spatial_band_top_is_the_sort_default(self):
        """``gpio sort``'s default is the top of the band ``check`` advises."""
        from geoparquet_io.core.parquet_writer import DEFAULT_SORT_ROW_GROUP_ROWS

        assert SPATIAL_ROW_COUNT_RANGE[1] == DEFAULT_SORT_ROW_GROUP_ROWS

    def test_optimization_check_scores_the_same_spatial_band(self, monkeypatch):
        """``check optimization``'s row-group factor scores this exact band."""
        from geoparquet_io.core import check_parquet_structure as structure
        from geoparquet_io.core.check_optimization import _check_row_group_size

        low, high = SPATIAL_ROW_COUNT_RANGE

        def scored(avg_rows):
            monkeypatch.setattr(
                structure,
                "get_row_group_stats",
                lambda _f: {
                    "avg_rows_per_group": avg_rows,
                    "total_size": 200 * 1024 * 1024,
                    "num_groups": 8,
                },
            )
            return _check_row_group_size("ignored.parquet")["passed"]

        assert scored(low) is True
        assert scored(high) is True
        assert scored(low - 1) is False
        assert scored(high + 1) is False

    def test_optimal_above_the_spatial_band_names_the_spatial_band(self):
        """A verdict of "optimal" must say what it is optimal *for*.

        Otherwise ``check row-group`` prints "Row count per group is optimal"
        for a 150,000-row group and, in the next breath, advises 10,000-50,000.
        """
        status, message, color = assess_row_count(150_000)
        assert status == "optimal"
        assert color == "green"
        assert "10,000-50,000" in message

    def test_optimal_inside_the_spatial_band_is_unqualified(self):
        """Inside both bands there is nothing to qualify."""
        status, message, _color = assess_row_count(30_000)
        assert status == "optimal"
        assert "10,000-50,000" not in message

    def test_printed_guidelines_label_each_band(self, monkeypatch, caplog):
        """The two printed guideline lines must say which workload each is for."""
        import logging

        from geoparquet_io.core import check_parquet_structure as structure

        monkeypatch.setattr(
            structure,
            "get_row_group_stats",
            lambda _f: {
                "avg_rows_per_group": 5_000,  # suboptimal, so guidelines print
                "total_size": 200 * 1024 * 1024,
                "num_groups": 40,
                "avg_group_size": 5 * 1024 * 1024,
            },
        )

        with caplog.at_level(logging.DEBUG):
            check_row_groups("ignored.parquet", verbose=False, return_results=False)

        printed = "\n".join(record.message for record in caplog.records)
        general_low, general_high = GENERAL_ROW_COUNT_RANGE
        spatial_low, spatial_high = SPATIAL_ROW_COUNT_RANGE
        assert f"{general_low:,}-{general_high:,} rows per group (general use)" in printed
        assert f"{spatial_low:,}-{spatial_high:,} rows per group" in printed
        assert f"gpio sort defaults to {spatial_high:,}" in printed


class TestGetRowGroupStats:
    """Tests for get_row_group_stats function."""

    def test_returns_stats_dict(self, places_test_file):
        """Test that get_row_group_stats returns expected dict with valid values."""
        import numbers

        stats = get_row_group_stats(places_test_file)
        assert isinstance(stats, dict)
        assert "num_groups" in stats
        assert "total_rows" in stats
        assert "avg_rows_per_group" in stats

        # Validate types and ranges
        assert isinstance(stats["num_groups"], int)
        assert stats["num_groups"] >= 1

        assert isinstance(stats["total_rows"], int)
        assert stats["total_rows"] >= 0

        assert isinstance(stats["avg_rows_per_group"], numbers.Number)
        assert stats["avg_rows_per_group"] >= 0

        # Sanity checks
        assert stats["total_rows"] >= stats["num_groups"]
        if stats["num_groups"] > 0:
            expected_avg = stats["total_rows"] / stats["num_groups"]
            assert abs(stats["avg_rows_per_group"] - expected_avg) < 0.01


class TestGetCompressionInfo:
    """Tests for get_compression_info function."""

    # Known Parquet compression codecs
    VALID_CODECS = {"ZSTD", "SNAPPY", "GZIP", "LZ4", "BROTLI", "UNCOMPRESSED", "LZO"}

    def test_returns_compression_dict(self, places_test_file):
        """Test that get_compression_info returns expected dict with valid codecs."""
        info = get_compression_info(places_test_file)
        assert isinstance(info, dict)
        # Should contain at least one column
        assert len(info) > 0

        # Validate all compression values are strings and known codecs
        for col_name, codec in info.items():
            assert isinstance(codec, str), f"Codec for {col_name} should be a string"
            assert codec in self.VALID_CODECS, f"Unknown codec '{codec}' for column {col_name}"

    def test_with_specific_column(self, places_test_file):
        """Test get_compression_info with specific column returns valid codec."""
        info = get_compression_info(places_test_file, "geometry")
        assert isinstance(info, dict)
        assert "geometry" in info

        # Validate geometry column codec
        geometry_codec = info["geometry"]
        assert isinstance(geometry_codec, str)
        assert geometry_codec in self.VALID_CODECS


class TestCheckCompression:
    """Tests for check_compression function."""

    # Known Parquet compression codecs
    VALID_CODECS = {"ZSTD", "SNAPPY", "GZIP", "LZ4", "BROTLI", "UNCOMPRESSED", "LZO"}

    def test_returns_results(self, places_test_file):
        """Test check_compression with return_results=True returns valid structure."""
        result = check_compression(places_test_file, verbose=False, return_results=True)
        assert isinstance(result, dict)
        assert "current_compression" in result
        assert "passed" in result

        # Validate value types
        assert isinstance(result["passed"], bool)
        assert isinstance(result["current_compression"], str)
        assert len(result["current_compression"]) > 0

        # Check if geometry_column is present and not None (if available)
        if "geometry_column" in result:
            assert result["geometry_column"] is not None

    def test_with_verbose(self, places_test_file, caplog):
        """Test check_compression with verbose flag produces output."""
        import logging

        with caplog.at_level(logging.DEBUG):
            check_compression(places_test_file, verbose=True, return_results=False)

        # Verify log output was captured
        assert len(caplog.records) > 0, "Expected log output when verbose=True"
        combined = " ".join(record.message.lower() for record in caplog.records)
        # Check for compression-related text
        assert any(
            phrase in combined
            for phrase in ["compression", "zstd", "snappy", "gzip", "uncompressed"]
        )


class TestCheckRowGroups:
    """Tests for check_row_groups function."""

    def test_returns_results(self, places_test_file):
        """Test check_row_groups with return_results=True returns valid structure."""
        result = check_row_groups(places_test_file, verbose=False, return_results=True)
        assert isinstance(result, dict)
        assert "passed" in result

        # Validate passed is a bool
        assert isinstance(result["passed"], bool)

        # Check for expected keys if present
        if "num_row_groups" in result:
            assert isinstance(result["num_row_groups"], int)
            assert result["num_row_groups"] >= 1
        if "avg_rows_per_group" in result:
            assert result["avg_rows_per_group"] >= 0

    def test_with_verbose(self, places_test_file, caplog):
        """Test check_row_groups with verbose flag produces output."""
        import logging

        with caplog.at_level(logging.DEBUG):
            check_row_groups(places_test_file, verbose=True, return_results=False)

        # Verify log output was captured
        assert len(caplog.records) > 0, "Expected log output when verbose=True"
        combined = " ".join(record.message.lower() for record in caplog.records)
        # Check for row-group-related text
        assert any(phrase in combined for phrase in ["row", "group", "rows", "size"])


class TestCheckMetadataAndBbox:
    """Tests for check_metadata_and_bbox function."""

    def test_returns_results(self, places_test_file):
        """Test check_metadata_and_bbox with return_results=True."""
        result = check_metadata_and_bbox(places_test_file, verbose=False, return_results=True)
        assert isinstance(result, dict)
        assert "has_bbox_column" in result
        assert "passed" in result

    def test_with_file_without_bbox(self, buildings_test_file):
        """Test check_metadata_and_bbox with file without bbox."""
        result = check_metadata_and_bbox(buildings_test_file, verbose=False, return_results=True)
        assert isinstance(result, dict)
        assert result["has_bbox_column"] is False

    def test_with_verbose(self, places_test_file):
        """Test check_metadata_and_bbox with verbose flag."""
        # Should not raise
        check_metadata_and_bbox(places_test_file, verbose=True, return_results=False)


class TestV2UpgradeSuggestion:
    """Tests for v2.0 upgrade suggestion in check output for v1.1 files."""

    def test_v1_file_suggests_v2_upgrade(self, places_test_file):
        """Test that checking a v1.1 file includes a v2.0 upgrade suggestion."""
        result = check_metadata_and_bbox(
            places_test_file, verbose=False, return_results=True, quiet=True
        )
        assert result["file_type"] == "geoparquet_v1"
        # Should have v2 upgrade suggestion in recommendations
        v2_recs = [r for r in result["recommendations"] if "2.0" in r]
        assert len(v2_recs) > 0, "Expected v2.0 upgrade recommendation for v1 file"

    def test_v1_file_upgrade_suggestion_mentions_benefits(self, places_test_file):
        """Test that the v2.0 upgrade suggestion mentions key benefits."""
        result = check_metadata_and_bbox(
            places_test_file, verbose=False, return_results=True, quiet=True
        )
        v2_recs = " ".join(r for r in result["recommendations"] if "2.0" in r)
        assert (
            "spatial" in v2_recs.lower()
            or "filter" in v2_recs.lower()
            or "stats" in v2_recs.lower()
        ), "v2.0 upgrade suggestion should mention spatial filtering or stats benefits"

    def test_v2_file_does_not_suggest_upgrade(self, places_test_file):
        """Test that checking a v2.0 file does NOT suggest v2.0 upgrade."""
        from unittest.mock import patch

        v2_info = {
            "has_geo_metadata": True,
            "geo_version": "2.0.0",
            "has_native_geo_types": True,
            "file_type": "geoparquet_v2",
            "bbox_recommended": False,
        }
        with patch(
            "geoparquet_io.core.check_parquet_structure.detect_geoparquet_file_type",
            return_value=v2_info,
        ):
            result = check_metadata_and_bbox(
                places_test_file, verbose=False, return_results=True, quiet=True
            )
        assert result["file_type"] == "geoparquet_v2"
        v2_recs = [
            r for r in result.get("recommendations", []) if "upgrade" in r.lower() and "2.0" in r
        ]
        assert len(v2_recs) == 0, "v2 file should NOT suggest v2.0 upgrade"

    def test_parquet_geo_only_does_not_suggest_upgrade(self, places_test_file):
        """Test that checking a parquet-geo-only file does NOT suggest v2.0 upgrade."""
        from unittest.mock import patch

        pgo_info = {
            "has_geo_metadata": False,
            "geo_version": None,
            "has_native_geo_types": True,
            "file_type": "parquet_geo_only",
            "bbox_recommended": False,
        }
        with patch(
            "geoparquet_io.core.check_parquet_structure.detect_geoparquet_file_type",
            return_value=pgo_info,
        ):
            result = check_metadata_and_bbox(
                places_test_file, verbose=False, return_results=True, quiet=True
            )
        assert result["file_type"] == "parquet_geo_only"
        v2_recs = [
            r for r in result.get("recommendations", []) if "upgrade" in r.lower() and "2.0" in r
        ]
        assert len(v2_recs) == 0, "parquet-geo-only file should NOT suggest v2.0 upgrade"

    def test_v1_file_info_output_includes_upgrade_suggestion(self, places_test_file, caplog):
        """Test that the info output for v1 files includes v2.0 upgrade suggestion."""
        import logging

        with caplog.at_level(logging.INFO):
            check_metadata_and_bbox(
                places_test_file, verbose=False, return_results=False, quiet=False
            )
        assert "2.0" in caplog.text, "Expected v2.0 mentioned in check log output for v1 file"
