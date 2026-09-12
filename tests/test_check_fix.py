"""Tests for check --fix functionality.

Every test that produces a file also hands it to
:func:`tests.fix_output_oracle.assert_fix_output_is_sound` (WP-1 of #1018). The
per-fix assertion -- "the compression is ZSTD now", "there is a bbox column now"
-- only proves the fix did the one thing it advertises; the oracle proves it did
not break anything else on the way, which is the promise ``--fix`` actually
makes.
"""

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
from tests.fix_output_oracle import CRS84, assert_fix_output_is_sound

#: ``places_test.parquet``: 766 rows, CRS84, GeoParquet 1.0.0, with a bbox column.
PLACES_ROWS = 766
#: ``buildings_test.parquet``: 42 rows, CRS84, GeoParquet 1.0.0, no bbox column.
BUILDINGS_ROWS = 42


def assert_places_output_is_sound(path, *, version, covering):
    """The oracle, with the constants every places-derived fixture shares."""
    assert_fix_output_is_sound(
        path,
        expected_rows=PLACES_ROWS,
        expected_crs=CRS84,
        expects_covering=covering,
        expected_version_prefix=version,
    )


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
        # ...and the rewrite kept every row, the CRS in both carriers, and a
        # `geo` block gpio's own validator accepts.
        assert_places_output_is_sound(fixed_file, version="1.1", covering=True)

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
        assert_places_output_is_sound(test_file, version="1.1", covering=True)
        # The backup is the original, untouched: still 1.0, still no covering.
        assert_places_output_is_sound(test_file + ".bak", version="1.0", covering=False)

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
        # Declining to fix must also mean declining to touch: the file is
        # exactly the 1.0 input, not a rewrite that happens to be ZSTD too.
        assert_places_output_is_sound(zstd_file, version="1.0", covering=False)


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
        # The added column is declared in a covering that resolves, and nothing
        # else about the file moved.
        assert_places_output_is_sound(fixed_file, version="1.1", covering=True)

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
        assert_fix_output_is_sound(temp_file, expected_rows=BUILDINGS_ROWS, expected_crs=CRS84)


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
        assert not os.path.exists(fixed_file)
        assert_places_output_is_sound(poor_file, version="1.0", covering=False)


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
            assert_places_output_is_sound(fixed_file, version="1.1", covering=True)


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
        # `check all` names the output version from its own checks, so a 1.0
        # input is repaired as 1.0 rather than upgraded behind the user -- and
        # 1.0 has no `covering` key to carry.
        assert_places_output_is_sound(fixed_file, version="1.0", covering=False)

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
        assert_places_output_is_sound(test_file, version="1.0", covering=False)
        assert_places_output_is_sound(test_file + ".bak", version="1.0", covering=False)

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
        assert_fix_output_is_sound(temp_file, expected_rows=BUILDINGS_ROWS, expected_crs=CRS84)


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
        # Declined, so the file is untouched -- still the SNAPPY 1.0 input.
        assert_places_output_is_sound(test_file, version="1.0", covering=False)

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
        assert_places_output_is_sound(output_file, version="1.1", covering=True)

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
        assert_places_output_is_sound(fixed_file, version="1.1", covering=True)


class TestCheckFixMultipleFiles:
    """Tests for check --fix with multiple files."""

    def test_fix_multiple_files_row_groups(self, places_test_file, temp_output_dir):
        """Test that --fix works with multiple files for row-group check."""

        # Create a partition directory with multiple files
        partition_dir = os.path.join(temp_output_dir, "partition")
        os.makedirs(partition_dir)

        # Create 3 files with suboptimal row groups
        table = pq.read_table(places_test_file)
        for i in range(3):
            file_path = os.path.join(partition_dir, f"part{i}.parquet")
            # Write with small row groups (suboptimal)
            pq.write_table(table, file_path, compression="SNAPPY", row_group_size=10)

        # Apply fix to all files
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["check", "row-group", partition_dir, "--all-files", "--fix"],
        )

        assert result.exit_code == 0
        # Should not show the "only available for single files" warning
        assert "only available for single files" not in result.output

        # Verify all files were fixed (should have .bak backups)
        for i in range(3):
            file_path = os.path.join(partition_dir, f"part{i}.parquet")
            backup_path = file_path + ".bak"
            assert os.path.exists(backup_path), f"Backup not created for part{i}"

            # Verify the file was actually fixed (compression changed)
            metadata = pq.read_metadata(file_path)
            # ZSTD compression should be applied
            assert "ZSTD" in str(metadata.row_group(0).column(0).compression)
            assert_places_output_is_sound(file_path, version="1.1", covering=True)

    def test_fix_multiple_files_some_optimal(self, places_test_file, temp_output_dir):
        """Test that --fix skips optimal files and fixes only suboptimal ones."""
        # Create a partition directory
        partition_dir = os.path.join(temp_output_dir, "partition")
        os.makedirs(partition_dir)

        table = pq.read_table(places_test_file)

        # Create file 1: already optimal (ZSTD, good row groups)
        file1 = os.path.join(partition_dir, "part0.parquet")
        pq.write_table(table, file1, compression="ZSTD", row_group_size=100000)

        # Create file 2: suboptimal (small row groups)
        file2 = os.path.join(partition_dir, "part1.parquet")
        pq.write_table(table, file2, compression="ZSTD", row_group_size=10)

        # Create file 3: suboptimal (small row groups)
        file3 = os.path.join(partition_dir, "part2.parquet")
        pq.write_table(table, file3, compression="ZSTD", row_group_size=10)

        # Apply fix
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["check", "row-group", partition_dir, "--all-files", "--fix"],
        )

        assert result.exit_code == 0

        # File 1 should NOT have a backup (was optimal)
        assert not os.path.exists(file1 + ".bak")

        # Files 2 and 3 should have backups (were fixed)
        assert os.path.exists(file2 + ".bak")
        assert os.path.exists(file3 + ".bak")

        # The untouched file is still the 1.0 input; the two rewrites are sound.
        assert_places_output_is_sound(file1, version="1.0", covering=False)
        for rewritten in (file2, file3):
            assert_places_output_is_sound(rewritten, version="1.1", covering=True)

    def test_fix_multiple_files_compression(self, places_test_file, temp_output_dir):
        """Test compression --fix with multiple files."""
        partition_dir = os.path.join(temp_output_dir, "partition")
        os.makedirs(partition_dir)

        table = pq.read_table(places_test_file)

        # Create 2 files with SNAPPY compression
        for i in range(2):
            file_path = os.path.join(partition_dir, f"part{i}.parquet")
            pq.write_table(table, file_path, compression="SNAPPY")

        # Apply compression fix
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["check", "compression", partition_dir, "--all-files", "--fix"],
        )

        assert result.exit_code == 0

        # Verify both files were re-compressed with ZSTD
        for i in range(2):
            file_path = os.path.join(partition_dir, f"part{i}.parquet")
            metadata = pq.read_metadata(file_path)
            assert "ZSTD" in str(metadata.row_group(0).column(0).compression)
            assert os.path.exists(file_path + ".bak")
            assert_places_output_is_sound(file_path, version="1.1", covering=True)
