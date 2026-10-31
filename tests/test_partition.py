"""
Tests for partition commands.

Partition tests read the outputs back and assert two things about every
partitioning run: row-count conservation (partition rows sum to the input's
row count) and partition-key correctness (every row landed in the partition
its key says it belongs to).

Pinned fixture facts these tests rely on:

- ``places_test.parquet``: 766 rows; the first character of ``fsq_place_id``
  splits it into exactly three partitions: '4' (201 rows), '5' (523),
  '6' (42).
- ``buildings_test.parquet``: 42 rows.
"""

import os

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import partition

PLACES_ROWS = 766
BUILDINGS_ROWS = 42
# substr(fsq_place_id, 1, 1) -> row count, verified against the fixture.
PLACES_PREFIX_COUNTS = {"4": 201, "5": 523, "6": 42}


# Shared CliRunner to avoid repeated instantiation
@pytest.fixture(scope="module")
def cli_runner():
    """Module-scoped CliRunner for test efficiency."""
    return CliRunner()


def _partition_files(folder):
    """Return the .parquet files directly inside folder."""
    return sorted(f for f in os.listdir(folder) if f.endswith(".parquet"))


def _total_rows(paths):
    """Sum row counts across parquet files without reading data pages."""
    return sum(pq.ParquetFile(p).metadata.num_rows for p in paths)


def _assert_string_partitions_correct(folder, prefix=""):
    """Assert flat string-partition outputs conserve rows and honor keys."""
    files = _partition_files(folder)
    stems = {f.removeprefix(prefix).removesuffix(".parquet") for f in files}
    assert stems == set(PLACES_PREFIX_COUNTS), stems
    total = 0
    for f in files:
        stem = f.removeprefix(prefix).removesuffix(".parquet")
        table = pq.read_table(os.path.join(folder, f), columns=["fsq_place_id"])
        assert table.num_rows == PLACES_PREFIX_COUNTS[stem]
        # Partition-key correctness: every row belongs under this stem.
        values = table.column("fsq_place_id").to_pylist()
        assert all(v.startswith(stem) for v in values)
        total += table.num_rows
    assert total == PLACES_ROWS


class TestPartitionCommands:
    """Test suite for partition commands."""

    def test_partition_string_preview(self, places_test_file):
        """Preview reports the exact partition breakdown without writing."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            ["string", places_test_file, "--column", "fsq_place_id", "--chars", "1", "--preview"],
        )
        assert result.exit_code == 0
        assert "Total partitions: 3" in result.output
        assert f"Total records: {PLACES_ROWS}" in result.output
        # The dominant partition and its share are pinned fixture facts.
        assert "523" in result.output
        assert "68.28%" in result.output

    def test_partition_string_by_column(self, places_test_file, temp_output_dir):
        """Flat string partitioning conserves rows and honors the key."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "string",
                places_test_file,
                temp_output_dir,
                "--column",
                "fsq_place_id",
                "--chars",
                "1",
            ],
        )
        assert result.exit_code == 0
        _assert_string_partitions_correct(temp_output_dir)

    def test_partition_string_with_hive(self, places_test_file, temp_output_dir):
        """Hive-style partitioning writes key=value dirs with correct rows."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "string",
                places_test_file,
                temp_output_dir,
                "--column",
                "fsq_place_id",
                "--chars",
                "1",
                "--hive",
            ],
        )
        assert result.exit_code == 0
        dirs = sorted(
            d
            for d in os.listdir(temp_output_dir)
            if os.path.isdir(os.path.join(temp_output_dir, d))
        )
        assert dirs == [f"fsq_place_id_prefix={c}" for c in sorted(PLACES_PREFIX_COUNTS)]
        total = 0
        for d in dirs:
            key = d.split("=", 1)[1]
            files = _partition_files(os.path.join(temp_output_dir, d))
            assert files, f"no parquet files in {d}"
            table = pq.read_table(
                os.path.join(temp_output_dir, d, files[0]), columns=["fsq_place_id"]
            )
            assert table.num_rows == PLACES_PREFIX_COUNTS[key]
            assert all(v.startswith(key) for v in table.column("fsq_place_id").to_pylist())
            total += table.num_rows
        assert total == PLACES_ROWS

    def test_partition_string_with_verbose(self, places_test_file, temp_output_dir):
        """Verbose partitioning still writes correct, row-conserving output."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "string",
                places_test_file,
                temp_output_dir,
                "--column",
                "fsq_place_id",
                "--chars",
                "1",
                "--verbose",
            ],
        )
        assert result.exit_code == 0
        _assert_string_partitions_correct(temp_output_dir)

    def test_partition_string_preview_with_limit(self, places_test_file):
        """--preview-limit truncates the table but not the totals."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            [
                "string",
                places_test_file,
                "--column",
                "fsq_place_id",
                "--chars",
                "2",
                "--preview",
                "--preview-limit",
                "5",
            ],
        )
        assert result.exit_code == 0
        # Two-character prefixes split places into 27 partitions; the limit
        # shows 5 of them and summarizes the remaining 22.
        assert "Total partitions: 27" in result.output
        assert f"Total records: {PLACES_ROWS}" in result.output
        assert "and 22 more partition(s)" in result.output

    def test_partition_string_no_output_folder(self, places_test_file):
        """Omitting the output folder without --preview is a usage error."""
        runner = CliRunner()
        result = runner.invoke(partition, ["string", places_test_file, "--column", "fsq_place_id"])
        assert result.exit_code != 0
        assert "OUTPUT_FOLDER is required unless using --preview" in result.output

    def test_partition_string_nonexistent_column(self, places_test_file, temp_output_dir):
        """A missing partition column fails naming the available columns."""
        runner = CliRunner()
        result = runner.invoke(
            partition,
            ["string", places_test_file, temp_output_dir, "--column", "nonexistent_column"],
        )
        assert result.exit_code != 0
        assert "'nonexistent_column' not found" in result.output
        # Nothing must be written on failure.
        assert _partition_files(temp_output_dir) == []

    def test_partition_admin_no_output_folder(self, places_test_file):
        """partition admin without an output folder is the same usage error."""
        runner = CliRunner()
        result = runner.invoke(partition, ["admin", places_test_file])
        assert result.exit_code != 0
        assert "OUTPUT_FOLDER is required unless using --preview" in result.output

    # H3 partition tests - Quick tests (preview, error cases)
    def test_partition_h3_preview(self, buildings_test_file, cli_runner):
        """Test partition h3 command with preview mode."""
        result = cli_runner.invoke(
            partition, ["h3", buildings_test_file, "--resolution", "9", "--preview"]
        )
        assert result.exit_code == 0
        assert "Partition Preview" in result.output
        assert "Total partitions:" in result.output
        # Preview accounts for every input row.
        assert f"Total records: {BUILDINGS_ROWS}" in result.output

    def test_partition_h3_preview_with_limit(self, buildings_test_file, cli_runner):
        """Test partition h3 preview with custom limit."""
        result = cli_runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                "--resolution",
                "9",
                "--preview",
                "--preview-limit",
                "2",
            ],
        )
        assert result.exit_code == 0
        assert "Partition Preview" in result.output
        assert f"Total records: {BUILDINGS_ROWS}" in result.output

    def test_partition_h3_no_output_folder(self, buildings_test_file, cli_runner):
        """partition h3 without an output folder is a usage error."""
        result = cli_runner.invoke(partition, ["h3", buildings_test_file, "--resolution", "9"])
        assert result.exit_code != 0
        assert "OUTPUT_FOLDER is required unless using --preview" in result.output

    def test_partition_h3_invalid_resolution(
        self, buildings_test_file, temp_output_dir, cli_runner
    ):
        """An out-of-range H3 resolution is rejected by option validation."""
        result = cli_runner.invoke(
            partition, ["h3", buildings_test_file, temp_output_dir, "--resolution", "16"]
        )
        assert result.exit_code != 0
        assert "not in the range" in result.output


@pytest.mark.slow
class TestPartitionH3Operations:
    """Slow H3 partition operation tests - consolidated for efficiency."""

    def test_partition_h3_flat_comprehensive(
        self, buildings_test_file, temp_output_dir, cli_runner
    ):
        """Test flat H3 partitioning - verifies multiple behaviors at once.

        Consolidates: basic, excludes_column_by_default, verbose
        """
        result = cli_runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                temp_output_dir,
                "--resolution",
                "9",
                "--skip-analysis",
                "--verbose",
            ],
        )
        assert result.exit_code == 0

        # Check verbose output
        assert "H3 column" in result.output

        # Verify partition files were created
        output_files = _partition_files(temp_output_dir)
        assert len(output_files) > 0

        # Verify H3 cell ID format (always 15 characters)
        for f in output_files:
            h3_id = f.replace(".parquet", "")
            assert len(h3_id) == 15, f"Expected 15-char H3 ID, got {len(h3_id)}"

        # Row-count conservation: partitions sum to the input row count.
        paths = [os.path.join(temp_output_dir, f) for f in output_files]
        assert _total_rows(paths) == BUILDINGS_ROWS

        # Verify H3 column is excluded by default (non-Hive)
        table = pq.read_table(paths[0])
        assert "h3_cell" not in table.schema.names

    def test_partition_h3_resolution_7(self, buildings_test_file, temp_output_dir, cli_runner):
        """Test H3 partitioning with resolution 7."""
        result = cli_runner.invoke(
            partition,
            ["h3", buildings_test_file, temp_output_dir, "--resolution", "7", "--skip-analysis"],
        )
        assert result.exit_code == 0
        output_files = _partition_files(temp_output_dir)
        assert len(output_files) > 0
        assert all(len(f.replace(".parquet", "")) == 15 for f in output_files)
        paths = [os.path.join(temp_output_dir, f) for f in output_files]
        assert _total_rows(paths) == BUILDINGS_ROWS

    def test_partition_h3_keeps_column_with_flag(
        self, buildings_test_file, temp_output_dir, cli_runner
    ):
        """Test --keep-h3-column flag keeps the column in output."""
        result = cli_runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                temp_output_dir,
                "--resolution",
                "9",
                "--keep-h3-column",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        output_files = _partition_files(temp_output_dir)
        assert len(output_files) > 0

        # Partition-key correctness: with the column kept, every row's
        # h3_cell must equal the cell the file is named after.
        total = 0
        for f in output_files:
            cell = f.replace(".parquet", "")
            table = pq.read_table(os.path.join(temp_output_dir, f), columns=["h3_cell"])
            assert set(table.column("h3_cell").to_pylist()) == {cell}
            total += table.num_rows
        assert total == BUILDINGS_ROWS

    def test_partition_h3_custom_column_name(
        self, buildings_test_file, temp_output_dir, cli_runner
    ):
        """Test --h3-name with custom column name."""
        result = cli_runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                temp_output_dir,
                "--h3-name",
                "custom_h3",
                "--resolution",
                "9",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        output_files = _partition_files(temp_output_dir)
        assert len(output_files) > 0
        paths = [os.path.join(temp_output_dir, f) for f in output_files]
        assert _total_rows(paths) == BUILDINGS_ROWS
        # The custom-named partition column is excluded by default too.
        assert "custom_h3" not in pq.ParquetFile(paths[0]).schema_arrow.names

    def test_partition_h3_hive_comprehensive(
        self, buildings_test_file, temp_output_dir, cli_runner
    ):
        """Test Hive-style H3 partitioning - verifies multiple behaviors at once.

        Consolidates: with_hive, hive_keeps_column_by_default
        """
        result = cli_runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                temp_output_dir,
                "--resolution",
                "9",
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

        # Row conservation and key correctness across all Hive partitions:
        # the h3_cell column is kept by default for Hive, and every row's
        # cell must match its directory's key.
        total = 0
        for d in hive_dirs:
            key = d.split("=", 1)[1]
            part_dir = os.path.join(temp_output_dir, d)
            parquet_files = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
            assert parquet_files, f"no parquet files in {d}"
            for f in parquet_files:
                table = pq.read_table(os.path.join(part_dir, f), columns=["h3_cell"])
                assert set(table.column("h3_cell").to_pylist()) == {key}
                total += table.num_rows
        assert total == BUILDINGS_ROWS


@pytest.mark.slow
class TestPartitionPrefix:
    """Tests for --prefix option on partition commands."""

    def test_partition_string_with_prefix(self, places_test_file, temp_output_dir, cli_runner):
        """Test partition string command with custom filename prefix."""
        result = cli_runner.invoke(
            partition,
            [
                "string",
                places_test_file,
                temp_output_dir,
                "--column",
                "fsq_place_id",
                "--chars",
                "1",
                "--prefix",
                "places",
            ],
        )
        assert result.exit_code == 0
        output_files = _partition_files(temp_output_dir)
        assert len(output_files) > 0
        assert all(f.startswith("places_") for f in output_files)
        _assert_string_partitions_correct(temp_output_dir, prefix="places_")

    def test_partition_h3_with_prefix(self, buildings_test_file, temp_output_dir, cli_runner):
        """Test partition h3 command with custom filename prefix."""
        result = cli_runner.invoke(
            partition,
            [
                "h3",
                buildings_test_file,
                temp_output_dir,
                "--resolution",
                "9",
                "--prefix",
                "buildings",
                "--skip-analysis",
            ],
        )
        assert result.exit_code == 0
        output_files = _partition_files(temp_output_dir)
        assert len(output_files) > 0
        assert all(f.startswith("buildings_") for f in output_files)
        for f in output_files:
            h3_cell = f.replace("buildings_", "").replace(".parquet", "")
            assert len(h3_cell) == 15
        paths = [os.path.join(temp_output_dir, f) for f in output_files]
        assert _total_rows(paths) == BUILDINGS_ROWS

    def test_partition_string_with_prefix_and_hive(
        self, places_test_file, temp_output_dir, cli_runner
    ):
        """Test partition string with prefix and Hive-style partitioning."""
        result = cli_runner.invoke(
            partition,
            [
                "string",
                places_test_file,
                temp_output_dir,
                "--column",
                "fsq_place_id",
                "--chars",
                "1",
                "--prefix",
                "places",
                "--hive",
            ],
        )
        assert result.exit_code == 0

        items = os.listdir(temp_output_dir)
        hive_dirs = sorted(d for d in items if os.path.isdir(os.path.join(temp_output_dir, d)))
        assert hive_dirs == [f"fsq_place_id_prefix={c}" for c in sorted(PLACES_PREFIX_COUNTS)]
        total = 0
        for d in hive_dirs:
            key = d.split("=", 1)[1]
            sample_dir = os.path.join(temp_output_dir, d)
            parquet_files = [f for f in os.listdir(sample_dir) if f.endswith(".parquet")]
            assert all(f.startswith("places_") for f in parquet_files)
            for f in parquet_files:
                table = pq.read_table(os.path.join(sample_dir, f), columns=["fsq_place_id"])
                assert all(v.startswith(key) for v in table.column("fsq_place_id").to_pylist())
                total += table.num_rows
        assert total == PLACES_ROWS
