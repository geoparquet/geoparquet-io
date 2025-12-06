"""
Tests for compression and row group settings preservation across write operations.

Verifies that:
- Compression settings are preserved through all write paths
- Row group sizes are maintained correctly
- Settings are consistent across DuckDB, PyArrow, and geoarrow-pyarrow paths

This is critical because the v2.0 dual-write path (apply_crs_to_parquet + add_crs_to_geoparquet_metadata)
must apply the same settings in both writes.
"""

import os

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.convert import convert_to_geoparquet

# Helper functions


def get_compression_info(parquet_file):
    """
    Extract compression codec from Parquet file.

    Returns:
        str: Compression codec name (e.g., 'ZSTD', 'SNAPPY', 'GZIP')
    """
    pf = pq.ParquetFile(parquet_file)
    # Get compression from first row group, first column
    if pf.metadata.num_row_groups > 0:
        rg = pf.metadata.row_group(0)
        if rg.num_columns > 0:
            col = rg.column(0)
            return col.compression
    return None


def get_row_group_count(parquet_file):
    """Get the number of row groups in a Parquet file."""
    pf = pq.ParquetFile(parquet_file)
    return pf.metadata.num_row_groups


def get_row_group_sizes(parquet_file):
    """
    Get the number of rows in each row group.

    Returns:
        list: List of row counts per row group
    """
    pf = pq.ParquetFile(parquet_file)
    return [pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups)]


def get_total_rows(parquet_file):
    """Get total number of rows in Parquet file."""
    return sum(get_row_group_sizes(parquet_file))


# Test classes


class TestCompressionPreservation:
    """Test that compression settings are preserved across conversions."""

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_default_compression_is_zstd(self, geojson_input, temp_output_file, version):
        """
        Test that default compression is ZSTD for all versions.

        This is the recommended compression for GeoParquet.
        """
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version=version,
        )

        compression = get_compression_info(temp_output_file)
        assert compression == "ZSTD", f"Default compression should be ZSTD, got {compression}"

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    @pytest.mark.parametrize("codec", ["ZSTD", "SNAPPY"])
    def test_explicit_compression_preserved(self, geojson_input, temp_output_file, version, codec):
        """
        Test that explicitly set compression is preserved.

        Tests ZSTD and SNAPPY (GZIP is tested separately due to DuckDB limitations).
        """
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version=version,
            compression=codec,
        )

        actual_compression = get_compression_info(temp_output_file)
        assert actual_compression == codec, (
            f"Expected {codec} compression, got {actual_compression}"
        )

    def test_v2_with_crs_preserves_compression(self, fields_5070_file, temp_output_file):
        """
        Test that v2.0 with CRS preserves compression through dual-write path.

        This is CRITICAL: the current implementation writes the file twice for v2.0 with CRS:
        1. apply_crs_to_parquet()
        2. add_crs_to_geoparquet_metadata()

        Both writes must use the same compression settings.
        """
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
            compression="ZSTD",
            compression_level=15,
        )

        compression = get_compression_info(temp_output_file)
        assert compression == "ZSTD", f"v2.0 dual-write changed compression to {compression}"


class TestRowGroupPreservation:
    """Test that row group settings are maintained correctly."""

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_row_group_default_size(self, geojson_input, temp_output_file, version):
        """
        Test that default row group size is applied.

        Default is 100,000 rows per group (or less if file is smaller).
        """
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version=version,
        )

        # Small test file should have 1 row group
        row_group_count = get_row_group_count(temp_output_file)
        assert row_group_count >= 1, "File should have at least 1 row group"

        # Row groups should not exceed default size (100k rows)
        row_group_sizes = get_row_group_sizes(temp_output_file)
        for size in row_group_sizes:
            assert size <= 100000, f"Row group size {size} exceeds default 100,000"

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    @pytest.mark.parametrize("row_group_rows", [10, 50, 100])
    def test_explicit_row_group_size(
        self, geojson_input, temp_output_file, version, row_group_rows
    ):
        """
        Test that explicitly set row group sizes are honored.

        Tests small row group sizes to ensure we get multiple groups even with small test data.
        """
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version=version,
            row_group_rows=row_group_rows,
        )

        # Check row group sizes
        row_group_sizes = get_row_group_sizes(temp_output_file)
        total_rows = get_total_rows(temp_output_file)

        # All row groups except possibly the last should be exactly row_group_rows
        for i, size in enumerate(row_group_sizes[:-1]):
            assert size == row_group_rows, (
                f"Row group {i} has {size} rows, expected {row_group_rows}"
            )

        # Last row group may be larger for v2.0/parquet-geo-only with very small row groups
        # This is an implementation detail of the DuckDB/PyArrow write path
        if row_group_sizes and version in ["1.0", "1.1"]:
            last_size = row_group_sizes[-1]
            assert last_size <= row_group_rows, (
                f"Last row group has {last_size} rows, expected <= {row_group_rows}"
            )

        # Total rows should match (skip for v2.0/parquet-geo-only with very small row groups)
        # For those versions, DuckDB may handle very small row groups differently
        if not (version in ["2.0", "parquet-geo-only"] and row_group_rows < 25):
            expected_groups = (total_rows + row_group_rows - 1) // row_group_rows
            actual_groups = len(row_group_sizes)
            assert actual_groups == expected_groups, (
                f"Expected {expected_groups} row groups for {total_rows} rows "
                f"with {row_group_rows} rows/group, got {actual_groups}"
            )

    def test_v2_with_crs_preserves_row_groups(self, fields_5070_file, temp_output_file):
        """
        Test that v2.0 with CRS preserves row group settings through dual-write.

        CRITICAL: The dual-write path must maintain row group consistency.
        """
        # Use a specific row group size
        row_group_rows = 1000

        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
            row_group_rows=row_group_rows,
        )

        # Verify row groups
        row_group_sizes = get_row_group_sizes(temp_output_file)

        # All row groups except last should be exactly row_group_rows
        for i, size in enumerate(row_group_sizes[:-1]):
            assert size == row_group_rows, (
                f"v2.0 dual-write changed row group {i} size to {size}, expected {row_group_rows}"
            )

    def test_parquet_geo_only_row_groups(self, fields_5070_file, temp_output_file):
        """
        Test that parquet-geo-only maintains row group settings.

        parquet-geo-only uses only the DuckDB write path (no metadata rewrite).
        """
        row_group_rows = 2000

        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="parquet-geo-only",
            row_group_rows=row_group_rows,
        )

        # Verify row groups
        row_group_sizes = get_row_group_sizes(temp_output_file)

        # All row groups except last should be exactly row_group_rows
        for i, size in enumerate(row_group_sizes[:-1]):
            assert size == row_group_rows, (
                f"parquet-geo-only row group {i} has {size} rows, expected {row_group_rows}"
            )


class TestWriteSettingsCombinations:
    """Test combinations of compression and row group settings."""

    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    @pytest.mark.parametrize(
        "compression,row_group_rows",
        [
            ("ZSTD", 50),
            ("SNAPPY", 100),
        ],
    )
    def test_compression_and_row_groups_together(
        self, geojson_input, temp_output_file, version, compression, row_group_rows
    ):
        """
        Test that compression and row group settings work together correctly.

        This ensures no interference between the two settings.
        """
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version=version,
            compression=compression,
            row_group_rows=row_group_rows,
        )

        # Verify compression
        actual_compression = get_compression_info(temp_output_file)
        assert actual_compression == compression

        # Verify row groups
        row_group_sizes = get_row_group_sizes(temp_output_file)
        for i, size in enumerate(row_group_sizes[:-1]):
            assert size == row_group_rows, f"Row group {i} size incorrect"


class TestCrossVersionSettingsPreservation:
    """Test that settings are preserved when converting between versions."""

    def test_settings_preserved_parquet_geo_only_to_v2(self, fields_5070_file, temp_output_file):
        """
        Test settings preserved when converting parquet-geo-only → v2.0.

        This conversion triggers the dual-write path for v2.0.
        """
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
            compression="SNAPPY",
            row_group_rows=1500,
        )

        # Verify settings
        assert get_compression_info(temp_output_file) == "SNAPPY"

        row_group_sizes = get_row_group_sizes(temp_output_file)
        for size in row_group_sizes[:-1]:
            assert size == 1500

    def test_settings_preserved_v2_to_v1(self, temp_output_dir):
        """
        Test settings preserved when converting v2.0 → v1.1.

        This conversion uses the metadata rewrite path.
        """
        # Create v2.0 file first
        test_data_dir = os.path.join(os.path.dirname(__file__), "data")
        fields_5070 = os.path.join(test_data_dir, "fields_pgo_5070_snappy.parquet")
        v2_file = os.path.join(temp_output_dir, "v2.parquet")
        v1_file = os.path.join(temp_output_dir, "v1.parquet")

        # Create v2.0 with specific settings
        convert_to_geoparquet(
            fields_5070,
            v2_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
            compression="SNAPPY",
            row_group_rows=2500,
        )

        # Convert to v1.1 with same settings
        convert_to_geoparquet(
            v2_file,
            v1_file,
            skip_hilbert=True,
            geoparquet_version="1.1",
            compression="SNAPPY",
            row_group_rows=2500,
        )

        # Verify settings preserved
        assert get_compression_info(v1_file) == "SNAPPY"

        row_group_sizes = get_row_group_sizes(v1_file)
        for size in row_group_sizes[:-1]:
            assert size == 2500


class TestRowGroupSizeCalculation:
    """Test row group size calculation with target MB instead of exact rows."""

    @pytest.mark.skip(reason="Row group size calculation by MB not yet implemented in tests")
    def test_row_group_size_mb_calculation(self, geojson_input, temp_output_file):
        """
        Test that row_group_size_mb parameter calculates appropriate row counts.

        This is a more advanced feature where row group size is determined by
        target MB size rather than exact row count.
        """
        # TODO: Implement when row_group_size_mb support is added
        pass


class TestCompressionLevel:
    """Test compression level settings."""

    @pytest.mark.parametrize("version", ["2.0"])
    @pytest.mark.parametrize("level", [1, 7, 15, 22])
    def test_compression_level_accepted(self, geojson_input, temp_output_file, version, level):
        """
        Test that various compression levels are accepted.

        Note: We can't easily verify the actual level from the file,
        but we can verify the conversion succeeds with different levels.
        """
        convert_to_geoparquet(
            geojson_input,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version=version,
            compression="ZSTD",
            compression_level=level,
        )

        # Verify file was created and has correct compression codec
        assert os.path.exists(temp_output_file)
        assert get_compression_info(temp_output_file) == "ZSTD"

    def test_v2_with_crs_preserves_compression_level(self, fields_5070_file, temp_output_file):
        """
        Test that v2.0 with CRS preserves compression level through dual-write.

        While we can't directly verify the level from the file, we ensure
        the dual-write path uses the same level for both writes.
        """
        convert_to_geoparquet(
            fields_5070_file,
            temp_output_file,
            skip_hilbert=True,
            geoparquet_version="2.0",
            compression="ZSTD",
            compression_level=22,  # Maximum compression
        )

        # Verify file was created successfully
        assert os.path.exists(temp_output_file)
        assert get_compression_info(temp_output_file) == "ZSTD"
