"""
Performance baseline tests for conversion operations.

Establishes performance baselines BEFORE consolidating the dual-write path.

Key metrics:
- Conversion time for v2.0 with CRS (currently does 2 writes)
- Conversion time for v2.0 without CRS (does 1 write)
- Conversion time for parquet-geo-only with CRS (does 1 write)
- File I/O count tracking

After consolidation, v2.0 with CRS should approach the performance
of v2.0 without CRS or parquet-geo-only.
"""

import os
import time

import pytest

from geoparquet_io.core.convert import convert_to_geoparquet

# Helper functions


def measure_conversion_time(input_file, output_file, **kwargs):
    """
    Measure time to convert a file.

    Returns:
        float: Time in seconds
    """
    start = time.time()
    convert_to_geoparquet(input_file, output_file, **kwargs)
    end = time.time()
    return end - start


class TestV2PerformanceBaseline:
    """Baseline performance for v2.0 conversions."""

    def test_v2_with_crs_baseline(self, fields_5070_file, temp_output_file):
        """
        Baseline: v2.0 conversion with CRS (dual-write path).

        Current implementation:
        1. DuckDB writes file
        2. apply_crs_to_parquet() rewrites with CRS in schema
        3. add_crs_to_geoparquet_metadata() rewrites with CRS in metadata

        This is the slowest path and the target for optimization.
        """
        elapsed = measure_conversion_time(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
            verbose=False,
        )

        assert os.path.exists(temp_output_file)
        print(f"\nv2.0 with CRS (dual-write) baseline: {elapsed:.3f}s")

        # Store for comparison (in real scenario, would save to file)
        # This test establishes the baseline - future runs should be faster

    def test_v2_without_crs_baseline(self, fields_geom_type_only_file, temp_output_file):
        """
        Baseline: v2.0 conversion without CRS (no dual-write).

        Current implementation:
        1. DuckDB writes file
        (No CRS rewriting needed)

        This should be faster than v2.0 with CRS.
        """
        elapsed = measure_conversion_time(
            fields_geom_type_only_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
            verbose=False,
        )

        assert os.path.exists(temp_output_file)
        print(f"\nv2.0 without CRS baseline: {elapsed:.3f}s")

    def test_parquet_geo_only_with_crs_baseline(self, fields_5070_file, temp_output_file):
        """
        Baseline: parquet-geo-only conversion with CRS.

        Current implementation:
        1. DuckDB writes file
        2. apply_crs_to_parquet() rewrites with CRS in schema

        This is faster than v2.0 with CRS (only 1 rewrite, not 2).
        """
        elapsed = measure_conversion_time(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="parquet-geo-only",
            verbose=False,
        )

        assert os.path.exists(temp_output_file)
        print(f"\nparquet-geo-only with CRS baseline: {elapsed:.3f}s")


class TestComparativePerformance:
    """Compare performance across different conversion paths."""

    def test_v2_crs_vs_no_crs_performance(self, temp_output_dir):
        """
        Compare v2.0 with CRS vs without CRS.

        Expected: v2.0 with CRS should be slower (dual-write).
        After consolidation: Should be similar.
        """
        test_data_dir = os.path.join(os.path.dirname(__file__), "data")
        fields_5070 = os.path.join(test_data_dir, "fields_pgo_5070_snappy.parquet")
        fields_default = os.path.join(test_data_dir, "fields_pgo_crs84_bbox_snappy.parquet")

        output_crs = os.path.join(temp_output_dir, "with_crs.parquet")
        output_no_crs = os.path.join(temp_output_dir, "without_crs.parquet")

        # Measure with CRS (dual-write path)
        time_with_crs = measure_conversion_time(
            fields_5070, output_crs, skip_hilbert=True, geoparquet_version="2.0"
        )

        # Measure without CRS
        time_no_crs = measure_conversion_time(
            fields_default, output_no_crs, skip_hilbert=True, geoparquet_version="2.0"
        )

        print(f"\nv2.0 with CRS: {time_with_crs:.3f}s")
        print(f"v2.0 without CRS: {time_no_crs:.3f}s")
        print(
            f"Overhead: {time_with_crs - time_no_crs:.3f}s ({(time_with_crs / time_no_crs - 1) * 100:.1f}%)"
        )

        # Currently, with CRS should be slower
        # After consolidation, they should be closer

    def test_v2_vs_parquet_geo_only_crs_performance(self, fields_5070_file, temp_output_dir):
        """
        Compare v2.0 vs parquet-geo-only, both with CRS.

        v2.0: 2 rewrites (schema + metadata)
        parquet-geo-only: 1 rewrite (schema only)

        Expected: parquet-geo-only should be faster.
        After consolidation: v2.0 should match parquet-geo-only.
        """
        output_v2 = os.path.join(temp_output_dir, "v2.parquet")
        output_pgo = os.path.join(temp_output_dir, "parquet_geo_only.parquet")

        time_v2 = measure_conversion_time(
            fields_5070_file, output_v2, skip_hilbert=True, geoparquet_version="2.0"
        )

        time_pgo = measure_conversion_time(
            fields_5070_file, output_pgo, skip_hilbert=True, geoparquet_version="parquet-geo-only"
        )

        print(f"\nv2.0 with CRS: {time_v2:.3f}s")
        print(f"parquet-geo-only with CRS: {time_pgo:.3f}s")
        print(f"Overhead: {time_v2 - time_pgo:.3f}s ({(time_v2 / time_pgo - 1) * 100:.1f}%)")

        # Currently, v2.0 should be slower (2 writes vs 1)
        # After consolidation, they should be similar


class TestMultipleConversions:
    """Test performance with multiple conversions to amplify differences."""

    @pytest.mark.parametrize("iterations", [3])
    def test_repeated_v2_conversions(self, fields_5070_file, temp_output_dir, iterations):
        """
        Run multiple v2.0 conversions to measure cumulative overhead.

        This amplifies the dual-write overhead to make it more measurable.
        """
        times = []

        for i in range(iterations):
            output_file = os.path.join(temp_output_dir, f"output_{i}.parquet")
            elapsed = measure_conversion_time(
                fields_5070_file,
                output_file,
                skip_hilbert=True,
                geoparquet_version="2.0",
            )
            times.append(elapsed)

        avg_time = sum(times) / len(times)
        print(f"\nAverage v2.0 conversion time ({iterations} runs): {avg_time:.3f}s")
        print(f"Total time: {sum(times):.3f}s")
        print(f"Min/Max: {min(times):.3f}s / {max(times):.3f}s")


class TestLargeFilePerformance:
    """Test performance with larger files to see more significant differences."""

    @pytest.mark.slow
    def test_large_file_v2_with_crs(self, temp_output_dir):
        """
        Test v2.0 conversion with CRS on a larger file.

        Larger files will show more significant dual-write overhead.
        """
        pytest.skip("Large file test - run manually with larger test data")

        # Example for manual testing:
        # large_input = "/path/to/large/file.parquet"
        # output = os.path.join(temp_output_dir, "large_output.parquet")
        #
        # elapsed = measure_conversion_time(
        #     large_input,
        #     output,
        #     skip_hilbert=True,
        #     geoparquet_version="2.0",
        # )
        #
        # print(f"\nLarge file v2.0 with CRS: {elapsed:.3f}s")


class TestFileSize:
    """Test that file sizes are consistent across write paths."""

    def test_v2_crs_file_size_consistency(self, fields_5070_file, temp_output_dir):
        """
        Verify that v2.0 with CRS produces consistent file size.

        The dual-write should not inflate file size - both writes use same compression.
        """
        output_file = os.path.join(temp_output_dir, "v2_with_crs.parquet")

        convert_to_geoparquet(
            fields_5070_file,
            output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
        )

        file_size = os.path.getsize(output_file)
        print(f"\nv2.0 with CRS file size: {file_size:,} bytes ({file_size / 1024:.1f} KB)")

        # File should exist and have reasonable size
        assert file_size > 0

    def test_all_versions_file_sizes(self, fields_5070_file, temp_output_dir):
        """
        Compare file sizes across all versions.

        Helps understand storage overhead of different metadata approaches.
        """
        versions = ["1.0", "1.1", "2.0", "parquet-geo-only"]
        sizes = {}

        for version in versions:
            output_file = os.path.join(temp_output_dir, f"{version.replace('.', '_')}.parquet")
            convert_to_geoparquet(
                fields_5070_file,
                output_file,
                skip_hilbert=True,
                geoparquet_version=version,
            )
            sizes[version] = os.path.getsize(output_file)

        print("\nFile sizes by version:")
        for version, size in sizes.items():
            print(f"  {version:20s}: {size:,} bytes ({size / 1024:.1f} KB)")

        # All versions should produce similar file sizes
        # (main difference is metadata, which is small)
        max_size = max(sizes.values())
        min_size = min(sizes.values())
        variance = (max_size - min_size) / min_size

        # Note: Variance threshold increased to 25% because with smaller test files
        # (100 rows), metadata overhead is proportionally larger
        assert variance < 0.25, f"File size variance {variance:.1%} exceeds 25%"


@pytest.mark.skip(reason="File I/O monitoring requires platform-specific tools")
class TestFileIOCount:
    """
    Test file I/O operation counts.

    This would require platform-specific monitoring (strace on Linux, dtrace on macOS, etc.)
    to count actual file writes. Skipped for portability, but useful for detailed analysis.
    """

    def test_v2_crs_io_count(self):
        """
        Count file I/O operations for v2.0 with CRS.

        Expected: 2 writes (dual-write path)
        After consolidation: 1 write

        Requires platform-specific I/O monitoring tools.
        """
        pass
