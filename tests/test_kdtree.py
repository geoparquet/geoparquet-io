"""
Tests for KD-tree partitioning commands.
"""

import os

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import add, partition


# Shared CliRunner to avoid repeated instantiation
@pytest.fixture(scope="module")
def cli_runner():
    """Module-scoped CliRunner for test efficiency."""
    return CliRunner()


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

    def test_partition_kdtree_preview(self, buildings_test_file, cli_runner):
        """Test partition kdtree command with preview mode."""
        result = cli_runner.invoke(
            partition, ["kdtree", buildings_test_file, "--partitions", "512", "--preview"]
        )
        assert result.exit_code == 0
        assert "Partition Preview" in result.output
        assert "Total partitions:" in result.output
        assert "Total records:" in result.output

    def test_partition_kdtree_preview_with_limit(self, buildings_test_file, cli_runner):
        """Test partition kdtree preview with custom limit."""
        result = cli_runner.invoke(
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

    def test_partition_kdtree_no_output_folder(self, buildings_test_file, cli_runner):
        """Test partition kdtree without output folder (should fail unless preview)."""
        result = cli_runner.invoke(
            partition, ["kdtree", buildings_test_file, "--partitions", "512"]
        )
        assert result.exit_code != 0

    def test_partition_kdtree_invalid_partitions(
        self, buildings_test_file, temp_output_dir, cli_runner
    ):
        """Test partition kdtree with invalid partitions (not power of 2)."""
        result = cli_runner.invoke(
            partition, ["kdtree", buildings_test_file, temp_output_dir, "--partitions", "100"]
        )
        assert result.exit_code != 0
        assert "power of 2" in result.output.lower()


@pytest.mark.slow
class TestPartitionKDTreeOperations:
    """Slow partition operation tests - run once and verify multiple aspects."""

    def test_partition_kdtree_flat_comprehensive(
        self, buildings_test_file, temp_output_dir, cli_runner
    ):
        """Test flat partitioning with 32 partitions - verifies multiple behaviors at once.

        This single test covers what was previously:
        - test_partition_kdtree_basic
        - test_partition_kdtree_excludes_column_by_default
        - test_partition_kdtree_with_verbose (verbose is tested via output)
        """
        result = cli_runner.invoke(
            partition,
            [
                "kdtree",
                buildings_test_file,
                temp_output_dir,
                "--partitions",
                "32",
                "--skip-analysis",
                "--verbose",
            ],
        )
        assert result.exit_code == 0
        # Check verbose output
        assert "KD-tree column" in result.output or "partitions" in result.output

        # Verify partition files were created
        output_files = [f for f in os.listdir(temp_output_dir) if f.endswith(".parquet")]
        assert len(output_files) > 0

        # Verify binary ID format (32 partitions = 5 iterations + starting '0' = 6 chars)
        for f in output_files:
            binary_id = f.replace(".parquet", "")
            assert len(binary_id) == 6, f"Expected 6 chars, got {len(binary_id)}"
            assert all(c in "01" for c in binary_id)
            assert binary_id.startswith("0")

        # Verify KD-tree column is excluded by default (non-Hive)
        sample_file = os.path.join(temp_output_dir, output_files[0])
        table = pq.read_table(sample_file)
        assert "kdtree_cell" not in table.schema.names

    def test_partition_kdtree_128_partitions(
        self, buildings_test_file, temp_output_dir, cli_runner
    ):
        """Test partitioning with 128 partitions - verifies binary ID length."""
        result = cli_runner.invoke(
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
        output_files = os.listdir(temp_output_dir)
        assert len(output_files) > 0
        # 128 partitions = 7 iterations + starting '0' = 8 chars
        assert all(len(f.replace(".parquet", "")) == 8 for f in output_files)

    def test_partition_kdtree_keeps_column_with_flag(
        self, buildings_test_file, temp_output_dir, cli_runner
    ):
        """Test --keep-kdtree-column flag keeps the column in output."""
        result = cli_runner.invoke(
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
        output_files = [f for f in os.listdir(temp_output_dir) if f.endswith(".parquet")]
        assert len(output_files) > 0

        sample_file = os.path.join(temp_output_dir, output_files[0])
        table = pq.read_table(sample_file)
        assert "kdtree_cell" in table.schema.names

    def test_partition_kdtree_custom_column_name(
        self, buildings_test_file, temp_output_dir, cli_runner
    ):
        """Test --kdtree-name with custom column name."""
        result = cli_runner.invoke(
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

    def test_partition_kdtree_hive_comprehensive(
        self, buildings_test_file, temp_output_dir, cli_runner
    ):
        """Test Hive-style partitioning - verifies multiple behaviors at once.

        This single test covers what was previously:
        - test_partition_kdtree_with_hive
        - test_partition_kdtree_hive_keeps_column_by_default
        """
        result = cli_runner.invoke(
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

        # Verify Hive directory structure
        hive_dirs = [
            d
            for d in os.listdir(temp_output_dir)
            if os.path.isdir(os.path.join(temp_output_dir, d))
        ]
        assert len(hive_dirs) > 0

        # Find and verify a parquet file in Hive partition
        sample_dir = os.path.join(temp_output_dir, hive_dirs[0])
        parquet_files = [f for f in os.listdir(sample_dir) if f.endswith(".parquet")]
        assert len(parquet_files) > 0

        # Verify KD-tree column is kept by default for Hive
        sample_file = os.path.join(sample_dir, parquet_files[0])
        with open(sample_file, "rb") as f:
            table = pq.read_table(f)
        assert "kdtree_cell" in table.schema.names


class TestKDTreeBinaryIDs:
    """Test suite for validating KD-tree binary ID generation."""

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "partitions,expected_length",
        [(8, 4), (32, 6), (128, 8)],
        ids=["8-partitions", "32-partitions", "128-partitions"],
    )
    def test_kdtree_binary_id_length(
        self, buildings_test_file, temp_output_file, cli_runner, partitions, expected_length
    ):
        """Test that binary IDs have correct length based on partition count."""
        result = cli_runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--partitions", str(partitions)],
        )
        assert result.exit_code == 0

        table = pq.read_table(temp_output_file)
        kdtree_values = table.column("kdtree_cell").to_pylist()

        for value in kdtree_values:
            if value is not None:
                assert len(value) == expected_length, (
                    f"Expected {expected_length} chars for {partitions} partitions, got {len(value)}"
                )
                assert value.startswith("0")
                assert all(c in "01" for c in value)

    def test_kdtree_binary_id_values(self, buildings_test_file, temp_output_file, cli_runner):
        """Test that binary IDs contain only valid binary characters."""
        result = cli_runner.invoke(
            add,
            ["kdtree", buildings_test_file, temp_output_file, "--partitions", "32"],
        )
        assert result.exit_code == 0

        table = pq.read_table(temp_output_file)
        kdtree_values = table.column("kdtree_cell").to_pylist()

        for value in kdtree_values:
            if value is not None:
                assert all(c in "01" for c in value)

    @pytest.mark.slow
    def test_kdtree_partition_count(self, buildings_test_file, temp_output_dir, cli_runner):
        """Test that the number of unique partitions is reasonable for the partition count."""
        partitions = 32
        result = cli_runner.invoke(
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
        assert 0 < len(output_files) <= partitions

    @pytest.mark.parametrize(
        "flags,check_output",
        [
            (["--partitions", "32"], None),  # approx mode (default)
            (["--partitions", "8", "--exact"], None),  # exact mode
            (["--auto", "1000"], "Auto-selected"),  # auto mode
        ],
        ids=["approx", "exact", "auto"],
    )
    def test_add_kdtree_modes(
        self, buildings_test_file, temp_output_file, cli_runner, flags, check_output
    ):
        """Test KD-tree with different modes (approx, exact, auto)."""
        result = cli_runner.invoke(add, ["kdtree", buildings_test_file, temp_output_file] + flags)
        assert result.exit_code == 0
        if check_output:
            assert check_output in result.output
        assert os.path.exists(temp_output_file)
        table = pq.read_table(temp_output_file)
        assert "kdtree_cell" in table.schema.names

    def test_add_kdtree_mutually_exclusive_partitions_auto(
        self, buildings_test_file, temp_output_file, cli_runner
    ):
        """Test that --partitions and --auto are mutually exclusive."""
        result = cli_runner.invoke(
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
