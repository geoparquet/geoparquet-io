"""
Tests for KD-tree partitioning commands.
"""

import os

import pyarrow.parquet as pq
from click.testing import CliRunner

from geoparquet_io.cli.main import add, partition


class TestAddKDTreeColumn:
    """Test suite for add kdtree column command."""

    def test_add_kdtree_column_basic(self, buildings_test_file, temp_output_file):
        """Test adding KD-tree column with default iterations."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file],
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify kdtree_cell column was added
        table = pq.read_table(temp_output_file)
        assert "kdtree_cell" in table.schema.names

        # Verify binary strings are 10 characters (default iterations=9 + starting '0')
        kdtree_values = table.column("kdtree_cell").to_pylist()
        for value in kdtree_values:
            if value is not None:
                assert len(value) == 10
                assert all(c in "01" for c in value)
                assert value.startswith("0")  # All start with '0'

    def test_add_kdtree_column_custom_iterations(self, buildings_test_file, temp_output_file):
        """Test adding KD-tree column with custom iterations."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--iterations", "5"],
        )
        assert result.exit_code == 0

        # Verify binary strings are 6 characters (iterations=5 + starting '0')
        table = pq.read_table(temp_output_file)
        kdtree_values = table.column("kdtree_cell").to_pylist()
        for value in kdtree_values:
            if value is not None:
                assert len(value) == 6
                assert all(c in "01" for c in value)
                assert value.startswith("0")

    def test_add_kdtree_column_custom_name(self, buildings_test_file, temp_output_file):
        """Test adding KD-tree column with custom name."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            [
                "kdtree",
                buildings_test_file,
                temp_output_file,
                "--kdtree-name",
                "my_kdtree",
            ],
        )
        assert result.exit_code == 0

        # Verify custom column name
        table = pq.read_table(temp_output_file)
        assert "my_kdtree" in table.schema.names
        assert "kdtree_cell" not in table.schema.names

    def test_add_kdtree_column_dry_run(self, buildings_test_file, temp_output_file):
        """Test dry-run mode doesn't create output file."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--dry-run"],
        )
        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        assert not os.path.exists(temp_output_file)

    def test_add_kdtree_column_invalid_iterations_low(self, buildings_test_file, temp_output_file):
        """Test validation with iterations below minimum (0)."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--iterations", "0"],
        )
        assert result.exit_code != 0
        assert "Invalid value" in result.output or "out of range" in result.output.lower()

    def test_add_kdtree_column_invalid_iterations_high(self, buildings_test_file, temp_output_file):
        """Test validation with iterations above maximum (21)."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--iterations", "21"],
        )
        assert result.exit_code != 0
        assert "Invalid value" in result.output or "out of range" in result.output.lower()

    def test_add_kdtree_column_verbose(self, buildings_test_file, temp_output_file):
        """Test verbose output."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--verbose"],
        )
        assert result.exit_code == 0
        assert "Adding column" in result.output or "Processing" in result.output


class TestPartitionKDTree:
    """Test suite for partition kdtree command."""

    def test_partition_kdtree_preview(self, buildings_test_file):
        """Test partition kdtree command with preview mode."""
        runner = CliRunner()
        result = runner.invoke(
            partition, ["kdtree", buildings_test_file, "--iterations", "9", "--preview"]
        )
        assert result.exit_code == 0
        # Preview should show partition information
        assert "Partition Preview" in result.output
        assert "Total partitions:" in result.output
        assert "Total records:" in result.output

    def test_partition_kdtree_basic(self, buildings_test_file, temp_output_dir):
        """Test partition kdtree command with auto-add KD-tree column."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                temp_output_dir,
                "--iterations",
                "5",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        # Should have created partition files
        output_files = os.listdir(temp_output_dir)
        assert len(output_files) > 0
        # All files should be .parquet
        assert all(f.endswith(".parquet") for f in output_files)
        # Binary IDs should be 6 characters (iterations=5 + starting '0')
        assert all(len(f.replace(".parquet", "")) == 6 for f in output_files)
        # Verify they are valid binary strings starting with '0'
        assert all(all(c in "01" for c in f.replace(".parquet", "")) for f in output_files)
        assert all(f.startswith("0") for f in output_files)

    def test_partition_kdtree_custom_iterations(self, buildings_test_file, temp_output_dir):
        """Test partition kdtree with different iteration counts."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                temp_output_dir,
                "--iterations",
                "7",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        # Should have created partition files
        output_files = os.listdir(temp_output_dir)
        assert len(output_files) > 0
        # Binary IDs should be 8 characters (iterations=7 + starting '0')
        assert all(len(f.replace(".parquet", "")) == 8 for f in output_files)

    def test_partition_kdtree_with_hive(self, buildings_test_file, temp_output_dir):
        """Test partition kdtree command with Hive-style partitioning."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                temp_output_dir,
                "--iterations",
                "5",
                "--hive",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        # Should have created partition directories
        items = os.listdir(temp_output_dir)
        assert len(items) > 0
        # Check that items are directories (Hive-style)
        assert any(os.path.isdir(os.path.join(temp_output_dir, item)) for item in items)

    def test_partition_kdtree_with_verbose(self, buildings_test_file, temp_output_dir):
        """Test partition kdtree command with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                temp_output_dir,
                "--iterations",
                "5",
                "--verbose",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        assert "KD-tree column" in result.output
        assert "Adding it now" in result.output or "Using existing" in result.output

    def test_partition_kdtree_preview_with_limit(self, buildings_test_file):
        """Test partition kdtree preview with custom limit."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                "--iterations",
                "9",
                "--preview",
                "--preview-limit",
                "5",
            ],
        )
        assert result.exit_code == 0
        assert "Partition Preview" in result.output

    def test_partition_kdtree_no_output_folder(self, buildings_test_file):
        """Test partition kdtree without output folder (should fail unless preview)."""
        runner = CliRunner()
        result = runner.invoke(partition, ["kdtree", buildings_test_file, "--iterations", "9"])
        # Should fail because output folder is required without --preview
        assert result.exit_code != 0

    def test_partition_kdtree_custom_column_name(self, buildings_test_file, temp_output_dir):
        """Test partition kdtree with custom KD-tree column name."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                temp_output_dir,
                "--kdtree-name",
                "custom_kdtree",
                "--iterations",
                "5",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        output_files = os.listdir(temp_output_dir)
        assert len(output_files) > 0

    def test_partition_kdtree_invalid_iterations(self, buildings_test_file, temp_output_dir):
        """Test partition kdtree with invalid iterations."""
        runner = CliRunner()
        result = runner.invoke(
            partition, ["kdtree", buildings_test_file, temp_output_dir, "--iterations", "25"]
        )
        # Should fail with invalid iterations
        assert result.exit_code != 0

    def test_partition_kdtree_excludes_column_by_default(
        self, buildings_test_file, temp_output_dir
    ):
        """Test that KD-tree column is excluded from output by default (non-Hive)."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                temp_output_dir,
                "--iterations",
                "5",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0

        # Check that output files exist
        output_files = [f for f in os.listdir(temp_output_dir) if f.endswith(".parquet")]
        assert len(output_files) > 0

        # Check that KD-tree column is NOT in the output files
        sample_file = os.path.join(temp_output_dir, output_files[0])
        table = pq.read_table(sample_file)
        column_names = table.schema.names
        assert "kdtree_cell" not in column_names, "KD-tree column should be excluded by default"

    def test_partition_kdtree_keeps_column_with_flag(self, buildings_test_file, temp_output_dir):
        """Test that KD-tree column is kept when --keep-kdtree-column flag is used."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                temp_output_dir,
                "--iterations",
                "5",
                "--keep-kdtree-column",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0

        # Check that output files exist
        output_files = [f for f in os.listdir(temp_output_dir) if f.endswith(".parquet")]
        assert len(output_files) > 0

        # Check that KD-tree column IS in the output files
        sample_file = os.path.join(temp_output_dir, output_files[0])
        table = pq.read_table(sample_file)
        column_names = table.schema.names
        assert "kdtree_cell" in column_names, (
            "KD-tree column should be kept with --keep-kdtree-column flag"
        )

    def test_partition_kdtree_hive_keeps_column_by_default(
        self, buildings_test_file, temp_output_dir
    ):
        """Test that KD-tree column is kept by default when using Hive partitioning."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                temp_output_dir,
                "--iterations",
                "5",
                "--hive",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0

        # Find a parquet file in the Hive-style directory structure
        hive_dirs = [
            d
            for d in os.listdir(temp_output_dir)
            if os.path.isdir(os.path.join(temp_output_dir, d))
        ]
        assert len(hive_dirs) > 0

        # Find a parquet file in one of the partition directories
        sample_dir = os.path.join(temp_output_dir, hive_dirs[0])
        parquet_files = [f for f in os.listdir(sample_dir) if f.endswith(".parquet")]
        assert len(parquet_files) > 0

        # Check that KD-tree column IS in the output files (default for Hive)
        sample_file = os.path.join(sample_dir, parquet_files[0])
        # Read with open() to avoid PyArrow trying to read as a Hive dataset
        with open(sample_file, "rb") as f:
            table = pq.read_table(f)
        column_names = table.schema.names
        assert "kdtree_cell" in column_names, (
            "KD-tree column should be kept by default for Hive partitioning"
        )


class TestKDTreeBinaryIDs:
    """Test suite for validating KD-tree binary ID generation."""

    def test_kdtree_binary_id_length(self, buildings_test_file, temp_output_file):
        """Test that binary IDs have correct length matching iterations+1 (starting '0')."""
        for iterations in [3, 5, 7, 10]:
            runner = CliRunner()
            result = runner.invoke(
                add,
                [
                    "kdtree",
                    buildings_test_file,
                    temp_output_file,
                    "--iterations",
                    str(iterations),
                ],
            )
            assert result.exit_code == 0

            table = pq.read_table(temp_output_file)
            kdtree_values = table.column("kdtree_cell").to_pylist()

            # Verify all values have correct length (iterations + starting '0')
            expected_length = iterations + 1
            for value in kdtree_values:
                if value is not None:
                    assert len(value) == expected_length, (
                        f"Expected binary ID length {expected_length}, got {len(value)}"
                    )
                    assert value.startswith("0"), "All binary IDs should start with '0'"

            # Clean up for next iteration
            if os.path.exists(temp_output_file):
                os.remove(temp_output_file)

    def test_kdtree_binary_id_values(self, buildings_test_file, temp_output_file):
        """Test that binary IDs contain only valid binary characters."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--iterations", "9"],
        )
        assert result.exit_code == 0

        table = pq.read_table(temp_output_file)
        kdtree_values = table.column("kdtree_cell").to_pylist()

        # Verify all values are valid binary strings
        for value in kdtree_values:
            if value is not None:
                assert all(c in "01" for c in value), (
                    f"Binary ID '{value}' contains invalid characters"
                )

    def test_kdtree_partition_count(self, buildings_test_file, temp_output_dir):
        """Test that the number of unique partitions is reasonable for the iteration count."""
        runner = CliRunner()
        iterations = 5
        result = runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                temp_output_dir,
                "--iterations",
                str(iterations),
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0

        output_files = [f for f in os.listdir(temp_output_dir) if f.endswith(".parquet")]
        # We won't have exactly 2^iterations partitions if data isn't uniformly distributed,
        # but we should have at least some partitions and no more than the theoretical max
        max_partitions = 2**iterations
        assert 0 < len(output_files) <= max_partitions, (
            f"Expected between 1 and {max_partitions} partitions, got {len(output_files)}"
        )
