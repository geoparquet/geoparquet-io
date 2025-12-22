"""Tests for check --fix functionality."""

import os
import shutil

import pyarrow.parquet as pq
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.check_parquet_structure import (
    check_compression,
    check_metadata_and_bbox,
)
from geoparquet_io.core.check_spatial_order import check_spatial_order


class TestCheckFixCompression:
    """Tests for check compression --fix command."""

    def test_fix_compression_snappy_to_zstd(self, places_test_file, temp_output_dir):
        """Test fixing SNAPPY compression to ZSTD."""
        # Create a file with SNAPPY compression
        snappy_file = os.path.join(temp_output_dir, "snappy.parquet")
        table = pq.read_table(places_test_file)
        pq.write_table(table, snappy_file, compression="SNAPPY")

        # Verify it has SNAPPY compression
        result = check_compression(snappy_file, verbose=False, return_results=True)
        assert result["current_compression"] == "SNAPPY"
        assert result["fix_available"] is True

        # Apply fix
        runner = CliRunner()
        fixed_file = os.path.join(temp_output_dir, "fixed.parquet")
        result = runner.invoke(
            cli,
            ["check", "compression", snappy_file, "--fix", "--fix-output", fixed_file],
        )

        assert result.exit_code == 0
        assert os.path.exists(fixed_file)

        # Verify compression is now ZSTD
        final_result = check_compression(fixed_file, verbose=False, return_results=True)
        assert final_result["current_compression"] == "ZSTD"
        assert final_result["passed"] is True

    def test_fix_compression_with_backup(self, places_test_file, temp_output_dir):
        """Test fixing compression with automatic backup."""
        # Create a file with SNAPPY compression
        test_file = os.path.join(temp_output_dir, "test.parquet")
        table = pq.read_table(places_test_file)
        pq.write_table(table, test_file, compression="SNAPPY")

        runner = CliRunner()
        result = runner.invoke(cli, ["check", "compression", test_file, "--fix"])

        assert result.exit_code == 0
        assert os.path.exists(test_file)
        assert os.path.exists(test_file + ".bak")

        # Verify compression is fixed
        final_result = check_compression(test_file, verbose=False, return_results=True)
        assert final_result["current_compression"] == "ZSTD"

    def test_fix_compression_already_optimal(self, places_test_file, temp_output_dir):
        """Test fixing when compression is already optimal."""
        # Create a file with ZSTD compression
        zstd_file = os.path.join(temp_output_dir, "zstd.parquet")
        table = pq.read_table(places_test_file)
        pq.write_table(table, zstd_file, compression="ZSTD")

        runner = CliRunner()
        result = runner.invoke(cli, ["check", "compression", zstd_file, "--fix"])

        assert result.exit_code == 0
        assert "No fix needed" in result.output


class TestCheckFixBbox:
    """Tests for check bbox --fix command."""

    def test_fix_missing_bbox_column(self, places_test_file, temp_output_dir):
        """Test adding missing bbox column."""
        # Create file without bbox column
        no_bbox_file = os.path.join(temp_output_dir, "no_bbox.parquet")
        table = pq.read_table(places_test_file)
        # Remove bbox column if it exists
        if "bbox" in table.column_names:
            table = table.drop(["bbox"])
        pq.write_table(table, no_bbox_file)

        # Verify no bbox column
        result = check_metadata_and_bbox(no_bbox_file, verbose=False, return_results=True)
        assert result["needs_bbox_column"] is True

        # Apply fix
        runner = CliRunner()
        fixed_file = os.path.join(temp_output_dir, "fixed.parquet")
        result = runner.invoke(
            cli,
            ["check", "bbox", no_bbox_file, "--fix", "--fix-output", fixed_file],
        )

        assert result.exit_code == 0
        assert os.path.exists(fixed_file)

        # Verify bbox column exists
        final_result = check_metadata_and_bbox(fixed_file, verbose=False, return_results=True)
        assert final_result["has_bbox_column"] is True
        assert "bbox" in pq.read_table(fixed_file).column_names

    def test_fix_bbox_already_optimal(self, buildings_test_file, temp_output_dir):
        """Test fixing when bbox is already optimal."""
        # Use a temp copy to avoid modifying test data
        import shutil

        temp_file = os.path.join(temp_output_dir, "test.parquet")
        shutil.copy2(buildings_test_file, temp_file)

        runner = CliRunner()
        result = runner.invoke(
            cli, ["check", "bbox", temp_file, "--fix", "--no-backup"], input="y\n"
        )

        # Should report no fix needed or succeed with confirmation
        assert result.exit_code == 0 or "No fix needed" in result.output


class TestCheckFixRowGroups:
    """Tests for check row-group --fix command."""

    def test_fix_row_groups_single_large_group(self, places_test_file, temp_output_dir):
        """Test that small files with single row group are considered optimal.

        Small files (<64 MB) with a single row group don't need row group optimization,
        so the fix should not create a new file.
        """
        # Create file with single row group
        poor_file = os.path.join(temp_output_dir, "poor_groups.parquet")
        table = pq.read_table(places_test_file)
        pq.write_table(table, poor_file, row_group_size=10000)

        # Apply fix - should report no fix needed for small file
        runner = CliRunner()
        fixed_file = os.path.join(temp_output_dir, "fixed.parquet")
        result = runner.invoke(
            cli,
            ["check", "row-group", poor_file, "--fix", "--fix-output", fixed_file],
        )

        assert result.exit_code == 0
        # Small file with single row group is optimal - no fix created
        assert "No fix needed" in result.output or "optimal" in result.output.lower()


class TestCheckFixSpatial:
    """Tests for check spatial --fix command."""

    def test_fix_spatial_ordering(self, places_test_file, temp_output_dir):
        """Test applying Hilbert spatial ordering."""
        # Check current spatial ordering
        result = check_spatial_order(
            places_test_file,
            random_sample_size=50,
            limit_rows=500,
            verbose=False,
            return_results=True,
        )

        # Apply fix if needed
        runner = CliRunner()
        fixed_file = os.path.join(temp_output_dir, "fixed.parquet")
        result = runner.invoke(
            cli,
            [
                "check",
                "spatial",
                places_test_file,
                "--fix",
                "--fix-output",
                fixed_file,
                "--random-sample-size",
                "50",
            ],
        )

        assert result.exit_code == 0
        if "No fix needed" not in result.output:
            assert os.path.exists(fixed_file)


class TestCheckFixAll:
    """Tests for check all --fix command."""

    def test_fix_all_suboptimal_file(self, places_test_file, temp_output_dir):
        """Test fixing all issues in a suboptimal file."""
        # Create a suboptimal file (SNAPPY compression, no bbox)
        suboptimal_file = os.path.join(temp_output_dir, "suboptimal.parquet")
        table = pq.read_table(places_test_file)
        if "bbox" in table.column_names:
            table = table.drop(["bbox"])
        pq.write_table(table, suboptimal_file, compression="SNAPPY", row_group_size=10000)

        # Apply all fixes
        runner = CliRunner()
        fixed_file = os.path.join(temp_output_dir, "fixed.parquet")
        result = runner.invoke(
            cli,
            [
                "check",
                "all",
                suboptimal_file,
                "--fix",
                "--fix-output",
                fixed_file,
                "--random-sample-size",
                "50",
            ],
        )

        assert result.exit_code == 0
        assert os.path.exists(fixed_file)

        # Verify all fixes were applied
        compression_result = check_compression(fixed_file, verbose=False, return_results=True)
        assert compression_result["current_compression"] == "ZSTD"

        bbox_result = check_metadata_and_bbox(fixed_file, verbose=False, return_results=True)
        assert bbox_result["has_bbox_column"] is True

    def test_fix_all_with_backup(self, places_test_file, temp_output_dir):
        """Test fixing all issues with backup creation."""
        # Create a suboptimal file
        test_file = os.path.join(temp_output_dir, "test.parquet")
        table = pq.read_table(places_test_file)
        if "bbox" in table.column_names:
            table = table.drop(["bbox"])
        pq.write_table(table, test_file, compression="SNAPPY")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["check", "all", test_file, "--fix", "--random-sample-size", "50"],
        )

        assert result.exit_code == 0
        assert os.path.exists(test_file)
        assert os.path.exists(test_file + ".bak")

    def test_fix_all_no_fixes_needed(self, buildings_test_file, temp_output_dir):
        """Test check all --fix when file is already optimal."""
        # Use a temp copy to avoid modifying test data
        import shutil

        temp_file = os.path.join(temp_output_dir, "test.parquet")
        shutil.copy2(buildings_test_file, temp_file)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["check", "all", temp_file, "--fix", "--no-backup"],
            input="y\n",
        )

        # Should report no fixes needed or succeed
        assert result.exit_code == 0 or "No fix needed" in result.output


class TestCheckFixOptions:
    """Tests for check fix command options."""

    def test_fix_with_no_backup_confirmation(self, places_test_file, temp_output_dir):
        """Test that --no-backup requires confirmation."""
        test_file = os.path.join(temp_output_dir, "test.parquet")
        table = pq.read_table(places_test_file)
        pq.write_table(table, test_file, compression="SNAPPY")

        runner = CliRunner()
        # Without confirmation should abort
        result = runner.invoke(
            cli,
            ["check", "compression", test_file, "--fix", "--no-backup"],
            input="n\n",
        )

        assert result.exit_code != 0

    def test_fix_with_custom_output(self, places_test_file, temp_output_dir):
        """Test --fix-output option."""
        test_file = os.path.join(temp_output_dir, "test.parquet")
        output_file = os.path.join(temp_output_dir, "custom_output.parquet")

        table = pq.read_table(places_test_file)
        if "bbox" in table.column_names:
            table = table.drop(["bbox"])
        pq.write_table(table, test_file)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["check", "bbox", test_file, "--fix", "--fix-output", output_file],
        )

        assert result.exit_code == 0
        assert os.path.exists(output_file)
        assert os.path.exists(test_file)  # Original should remain
        assert not os.path.exists(test_file + ".bak")  # No backup needed

    def test_fix_preserves_original_data(self, places_test_file, temp_output_dir):
        """Test that fix preserves all original data."""
        test_file = os.path.join(temp_output_dir, "test.parquet")
        fixed_file = os.path.join(temp_output_dir, "fixed.parquet")

        # Copy original
        shutil.copy2(places_test_file, test_file)

        # Modify to have SNAPPY compression
        table = pq.read_table(test_file)
        original_row_count = len(table)
        original_columns = set(table.column_names)
        pq.write_table(table, test_file, compression="SNAPPY")

        # Apply fix
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["check", "compression", test_file, "--fix", "--fix-output", fixed_file],
        )

        assert result.exit_code == 0

        # Verify data preserved
        fixed_table = pq.read_table(fixed_file)
        assert len(fixed_table) == original_row_count
        # All original columns should be present (may have added bbox)
        assert original_columns.issubset(set(fixed_table.column_names))
