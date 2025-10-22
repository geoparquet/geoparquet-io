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
        """Test adding KD-tree column with auto-selection (default behavior)."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file],
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)
        assert "Auto-selected" in result.output

        # Verify kdtree_cell column was added
        table = pq.read_table(temp_output_file)
        assert "kdtree_cell" in table.schema.names

        # Verify binary strings are valid (length depends on auto-selection)
        kdtree_values = table.column("kdtree_cell").to_pylist()
        for value in kdtree_values:
            if value is not None:
                assert all(c in "01" for c in value)
                assert value.startswith("0")  # All start with '0'

    def test_add_kdtree_column_custom_partitions(self, buildings_test_file, temp_output_file):
        """Test adding KD-tree column with custom partitions (32)."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--partitions", "32"],
        )
        assert result.exit_code == 0

        # Verify binary strings are 6 characters (32 partitions = 5 iterations + starting '0')
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

    def test_add_kdtree_column_invalid_partitions_not_power_of_2(
        self, buildings_test_file, temp_output_file
    ):
        """Test validation with partitions not power of 2."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--partitions", "100"],
        )
        assert result.exit_code != 0
        assert "power of 2" in result.output.lower()

    def test_add_kdtree_column_invalid_partitions_too_small(
        self, buildings_test_file, temp_output_file
    ):
        """Test validation with partitions below minimum (1)."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--partitions", "1"],
        )
        assert result.exit_code != 0
        assert "power of 2" in result.output.lower()

    def test_add_kdtree_column_verbose(self, buildings_test_file, temp_output_file):
        """Test verbose output."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--verbose"],
        )
        assert result.exit_code == 0
        assert "Auto-selected" in result.output


class TestPartitionKDTree:
    """Test suite for partition kdtree command."""

    def test_partition_kdtree_preview(self, buildings_test_file):
        """Test partition kdtree command with preview mode."""
        runner = CliRunner()
        result = runner.invoke(
            partition, ["kdtree", buildings_test_file, "--partitions", "512", "--preview"]
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
                "--partitions",
                "32",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        # Should have created partition files
        output_files = os.listdir(temp_output_dir)
        assert len(output_files) > 0
        # All files should be .parquet
        assert all(f.endswith(".parquet") for f in output_files)
        # Binary IDs should be 6 characters (32 partitions = 5 iterations + starting '0')
        assert all(len(f.replace(".parquet", "")) == 6 for f in output_files)
        # Verify they are valid binary strings starting with '0'
        assert all(all(c in "01" for c in f.replace(".parquet", "")) for f in output_files)
        assert all(f.startswith("0") for f in output_files)

    def test_partition_kdtree_custom_partitions(self, buildings_test_file, temp_output_dir):
        """Test partition kdtree with different partition counts."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                temp_output_dir,
                "--partitions",
                "128",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        # Should have created partition files
        output_files = os.listdir(temp_output_dir)
        assert len(output_files) > 0
        # Binary IDs should be 8 characters (128 partitions = 7 iterations + starting '0')
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
                "--partitions",
                "32",
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
                "--partitions",
                "32",
                "--verbose",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        assert "KD-tree column" in result.output or "partitions" in result.output

    def test_partition_kdtree_preview_with_limit(self, buildings_test_file):
        """Test partition kdtree preview with custom limit."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                "--partitions",
                "512",
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
        result = runner.invoke(partition, ["kdtree", buildings_test_file, "--partitions", "512"])
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
                "--partitions",
                "32",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        output_files = os.listdir(temp_output_dir)
        assert len(output_files) > 0

    def test_partition_kdtree_invalid_partitions(self, buildings_test_file, temp_output_dir):
        """Test partition kdtree with invalid partitions (not power of 2)."""
        runner = CliRunner()
        result = runner.invoke(
            partition, ["kdtree", buildings_test_file, temp_output_dir, "--partitions", "100"]
        )
        # Should fail with invalid partitions
        assert result.exit_code != 0
        assert "power of 2" in result.output.lower()

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
                "--partitions",
                "32",
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
                "--partitions",
                "32",
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
                "--partitions",
                "32",
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
        """Test that binary IDs have correct length based on partition count."""
        import math

        for partitions in [8, 32, 128, 1024]:
            runner = CliRunner()
            result = runner.invoke(
                add,
                [
                    "kdtree",
                    buildings_test_file,
                    temp_output_file,
                    "--partitions",
                    str(partitions),
                ],
            )
            assert result.exit_code == 0

            table = pq.read_table(temp_output_file)
            kdtree_values = table.column("kdtree_cell").to_pylist()

            # Verify all values have correct length (log2(partitions) + starting '0')
            iterations = int(math.log2(partitions))
            expected_length = iterations + 1
            for value in kdtree_values:
                if value is not None:
                    assert len(value) == expected_length, (
                        f"Expected binary ID length {expected_length} for {partitions} partitions, got {len(value)}"
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
            ["kdtree", buildings_test_file, temp_output_file, "--partitions", "512"],
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
        """Test that the number of unique partitions is reasonable for the partition count."""
        runner = CliRunner()
        partitions = 32
        result = runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                temp_output_dir,
                "--partitions",
                str(partitions),
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0

        output_files = [f for f in os.listdir(temp_output_dir) if f.endswith(".parquet")]
        # We won't have exactly the requested partitions if data isn't uniformly distributed,
        # but we should have at least some partitions and no more than the theoretical max
        assert 0 < len(output_files) <= partitions, (
            f"Expected between 1 and {partitions} partitions, got {len(output_files)}"
        )

    def test_add_kdtree_approx_mode(self, buildings_test_file, temp_output_file):
        """Test KD-tree with approximate mode (default)."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--partitions", "32"],
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)
        # Verify column was added
        table = pq.read_table(temp_output_file)
        assert "kdtree_cell" in table.schema.names

    def test_add_kdtree_exact_mode(self, buildings_test_file, temp_output_file):
        """Test KD-tree with exact mode."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--partitions", "8", "--exact"],
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)
        # Verify column was added
        table = pq.read_table(temp_output_file)
        assert "kdtree_cell" in table.schema.names

    def test_add_kdtree_auto_mode(self, buildings_test_file, temp_output_file):
        """Test KD-tree with auto mode."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--auto", "1000"],
        )
        assert result.exit_code == 0
        assert "Auto-selected" in result.output
        assert os.path.exists(temp_output_file)
        # Verify column was added
        table = pq.read_table(temp_output_file)
        assert "kdtree_cell" in table.schema.names

    def test_add_kdtree_mutually_exclusive_partitions_auto(
        self, buildings_test_file, temp_output_file
    ):
        """Test that --partitions and --auto are mutually exclusive."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            [
                "kdtree",
                buildings_test_file,
                temp_output_file,
                "--partitions",
                "32",
                "--auto",
                "1000",
            ],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output.lower()
