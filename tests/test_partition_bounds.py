"""
Tests for partition output bounds.

These tests verify that partition files have correct bounds metadata
that matches their actual data, not the input file's bounds.
"""

import json
import os
import tempfile
import uuid

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import partition


def get_geo_metadata_bounds(file_path):
    """Extract bounds from GeoParquet geo metadata."""
    pf = pq.ParquetFile(file_path)
    metadata = pf.schema_arrow.metadata
    if b"geo" not in metadata:
        return None
    geo = json.loads(metadata[b"geo"].decode("utf-8"))
    # Look for bbox in geometry column
    for _col_name, col_info in geo.get("columns", {}).items():
        if "bbox" in col_info:
            return col_info["bbox"]
    return None


def get_actual_data_bounds(file_path):
    """Calculate actual bounds from geometry data in file."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    # Use MIN/MAX instead of ST_Extent because ST_Extent in DuckDB
    # doesn't aggregate properly across rows
    result = con.execute(f"""
        SELECT
            MIN(ST_XMin(geometry)) AS xmin,
            MIN(ST_YMin(geometry)) AS ymin,
            MAX(ST_XMax(geometry)) AS xmax,
            MAX(ST_YMax(geometry)) AS ymax
        FROM read_parquet('{file_path}')
    """).fetchone()
    con.close()
    return result


class TestExistingPartitionBounds:
    """Tests for existing partition test data bounds."""

    def test_el_salvador_bounds_match_data(self, country_partition_dir):
        """Test that El Salvador partition file bounds match its actual data."""
        el_salvador_file = os.path.join(country_partition_dir, "El_Salvador.parquet")

        metadata_bounds = get_geo_metadata_bounds(el_salvador_file)
        actual_bounds = get_actual_data_bounds(el_salvador_file)

        if metadata_bounds is None:
            pytest.skip("No bbox in metadata")

        # Bounds should match within reasonable tolerance
        assert abs(metadata_bounds[0] - actual_bounds[0]) < 0.01, (
            f"xmin mismatch: metadata={metadata_bounds[0]}, actual={actual_bounds[0]}"
        )
        assert abs(metadata_bounds[1] - actual_bounds[1]) < 0.01, (
            f"ymin mismatch: metadata={metadata_bounds[1]}, actual={actual_bounds[1]}"
        )
        assert abs(metadata_bounds[2] - actual_bounds[2]) < 0.01, (
            f"xmax mismatch: metadata={metadata_bounds[2]}, actual={actual_bounds[2]}"
        )
        assert abs(metadata_bounds[3] - actual_bounds[3]) < 0.01, (
            f"ymax mismatch: metadata={metadata_bounds[3]}, actual={actual_bounds[3]}"
        )

    def test_all_partition_bounds_match_data(self, country_partition_dir):
        """Test that all partition files have bounds matching their data."""
        partition_files = [
            "El_Salvador.parquet",
            "Guatemala.parquet",
            "Honduras.parquet",
            "Nicaragua.parquet",
        ]

        for filename in partition_files:
            file_path = os.path.join(country_partition_dir, filename)
            if not os.path.exists(file_path):
                continue

            metadata_bounds = get_geo_metadata_bounds(file_path)
            actual_bounds = get_actual_data_bounds(file_path)

            if metadata_bounds is None:
                continue

            # Bounds should match within reasonable tolerance
            for i, coord in enumerate(["xmin", "ymin", "xmax", "ymax"]):
                assert abs(metadata_bounds[i] - actual_bounds[i]) < 0.01, (
                    f"{filename}: {coord} mismatch: "
                    f"metadata={metadata_bounds[i]}, actual={actual_bounds[i]}"
                )


class TestPartitionCreationBounds:
    """Tests for bounds in newly created partitions."""

    @pytest.fixture
    def output_dir(self):
        """Create a temporary directory for partition output."""
        temp_dir = tempfile.mkdtemp(prefix=f"partition_bounds_test_{uuid.uuid4().hex[:8]}_")
        yield temp_dir
        # Cleanup
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_string_partition_bounds_match_data(self, places_test_file, output_dir):
        """Test that string partitioning creates files with correct bounds."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "string",
                places_test_file,
                output_dir,
                "--column",
                "fsq_place_id",
                "--chars",
                "1",
            ],
        )
        assert result.exit_code == 0

        # Check each output file has correct bounds
        for filename in os.listdir(output_dir):
            if not filename.endswith(".parquet"):
                continue

            file_path = os.path.join(output_dir, filename)
            metadata_bounds = get_geo_metadata_bounds(file_path)
            actual_bounds = get_actual_data_bounds(file_path)

            if metadata_bounds is None:
                continue

            # Bounds should match within reasonable tolerance
            for i, coord in enumerate(["xmin", "ymin", "xmax", "ymax"]):
                assert abs(metadata_bounds[i] - actual_bounds[i]) < 0.01, (
                    f"{filename}: {coord} mismatch: "
                    f"metadata={metadata_bounds[i]}, actual={actual_bounds[i]}"
                )

    def test_h3_partition_bounds_match_data(self, buildings_test_file, output_dir):
        """Test that H3 partitioning creates files with correct bounds."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                output_dir,
                "--resolution",
                "4",  # Low resolution for fewer partitions
                "--force",  # Force partition even with small data
            ],
        )
        assert result.exit_code == 0

        # Check at least one output file has correct bounds
        parquet_files = [f for f in os.listdir(output_dir) if f.endswith(".parquet")]
        assert len(parquet_files) > 0

        for filename in parquet_files[:3]:  # Check first 3 files
            file_path = os.path.join(output_dir, filename)
            metadata_bounds = get_geo_metadata_bounds(file_path)
            actual_bounds = get_actual_data_bounds(file_path)

            if metadata_bounds is None:
                continue

            for i, coord in enumerate(["xmin", "ymin", "xmax", "ymax"]):
                assert abs(metadata_bounds[i] - actual_bounds[i]) < 0.01, (
                    f"{filename}: {coord} mismatch: "
                    f"metadata={metadata_bounds[i]}, actual={actual_bounds[i]}"
                )
