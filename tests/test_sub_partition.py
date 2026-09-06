"""Tests for sub-partition functionality."""

import os
import shutil
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import partition
from tests.conftest import skip_if_geography_unavailable


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture
def temp_partition_dir():
    """Create a temp directory with parquet files of varying sizes."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


# buildings_test.parquet splits into several A5 cells at resolution 12, so a run
# at that resolution actually exercises partitioning instead of copying one file.
A5_SPLITTING_RESOLUTION = 12
BUILDINGS_ROWS = 42


def _partition_files(directory) -> list:
    from pathlib import Path

    return sorted(Path(directory).glob("**/*.parquet"))


def _total_rows(directory) -> int:
    return sum(pq.read_metadata(f).num_rows for f in _partition_files(directory))


class TestSubPartitionCore:
    """Test sub_partition core functionality."""

    def test_find_large_files_filters_by_size(self, temp_partition_dir):
        """Test that find_large_files correctly filters by size threshold."""
        from geoparquet_io.core.sub_partition import find_large_files

        # Create test files of different sizes
        # Small file: 1KB
        small_data = pa.table({"id": [1], "geometry": [b"POINT(0 0)"]})
        small_path = os.path.join(temp_partition_dir, "small.parquet")
        pq.write_table(small_data, small_path)

        # Large file: create with more rows to exceed threshold
        large_data = pa.table({"id": list(range(10000)), "geometry": [b"POINT(0 0)" * 100] * 10000})
        large_path = os.path.join(temp_partition_dir, "large.parquet")
        pq.write_table(large_data, large_path)

        # Threshold that should only match the large file
        large_size = os.path.getsize(large_path)
        small_size = os.path.getsize(small_path)
        threshold = (large_size + small_size) // 2  # Middle value

        result = find_large_files(temp_partition_dir, min_size_bytes=threshold)

        assert len(result) == 1
        assert result[0] == large_path

    def test_find_large_files_returns_empty_for_no_matches(self, temp_partition_dir):
        """Test that find_large_files returns empty list when no files exceed threshold."""
        from geoparquet_io.core.sub_partition import find_large_files

        # Create small file
        small_data = pa.table({"id": [1]})
        small_path = os.path.join(temp_partition_dir, "small.parquet")
        pq.write_table(small_data, small_path)

        # Threshold larger than any file
        result = find_large_files(temp_partition_dir, min_size_bytes=1000000000)

        assert result == []

    def test_find_large_files_recursive(self, temp_partition_dir):
        """Test that find_large_files searches subdirectories."""
        from geoparquet_io.core.sub_partition import find_large_files

        # Create nested file
        subdir = os.path.join(temp_partition_dir, "subdir")
        os.makedirs(subdir)
        data = pa.table({"id": list(range(1000))})
        nested_path = os.path.join(subdir, "nested.parquet")
        pq.write_table(data, nested_path)

        result = find_large_files(temp_partition_dir, min_size_bytes=1)

        assert len(result) == 1
        assert result[0] == nested_path


class TestSubPartitionExecution:
    """Test sub_partition_directory function."""

    def test_sub_partition_creates_subdirectories(self, temp_partition_dir):
        """Test that sub_partition_directory creates sub-partitions for large files."""
        from pathlib import Path

        from geoparquet_io.core.sub_partition import sub_partition_directory

        # Copy the buildings test file to our temp directory
        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        large_path = os.path.join(temp_partition_dir, "large.parquet")
        shutil.copy(buildings_file, large_path)

        # Get file size and use threshold just below it
        file_size = os.path.getsize(large_path)
        threshold = file_size - 100

        result = sub_partition_directory(
            directory=temp_partition_dir,
            partition_type="h3",
            min_size_bytes=threshold,
            resolution=4,
            in_place=True,
            verbose=False,
        )

        # Original file should be gone
        assert not os.path.exists(large_path)

        # Sub-partition directory should exist
        subdir = os.path.join(temp_partition_dir, "large_h3")
        assert os.path.isdir(subdir)

        # Should have some partition files
        partition_files = list(Path(subdir).glob("*.parquet"))
        assert len(partition_files) > 0

        assert result["processed"] == 1
        assert result["skipped"] == 0

    def test_sub_partition_skips_small_files(self, temp_partition_dir):
        """Test that sub_partition_directory skips files below threshold."""
        from geoparquet_io.core.sub_partition import sub_partition_directory

        # Create small file
        data = pa.table({"id": [1], "geometry": [b"POINT(0 0)"]})
        small_path = os.path.join(temp_partition_dir, "small.parquet")
        pq.write_table(data, small_path)

        result = sub_partition_directory(
            directory=temp_partition_dir,
            partition_type="h3",
            min_size_bytes=1000000000,  # 1GB - way bigger than file
            resolution=4,
            in_place=True,
            verbose=False,
        )

        # File should still exist
        assert os.path.exists(small_path)
        assert result["processed"] == 0

    def test_sub_partition_handles_errors(self, temp_partition_dir, monkeypatch):
        """Test that sub_partition_directory captures errors and preserves files on failure."""
        from pathlib import Path

        from geoparquet_io.core.sub_partition import sub_partition_directory

        # Copy the buildings test file to our temp directory
        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        large_path = os.path.join(temp_partition_dir, "large.parquet")
        shutil.copy(buildings_file, large_path)

        # Get file size and use threshold just below it
        file_size = os.path.getsize(large_path)
        threshold = file_size - 100

        # Mock the partition function to raise an error
        def mock_partition_fail(*args, **kwargs):
            raise ValueError("Simulated partition failure")

        # Patch the h3 partition function to fail - patch where it's imported
        monkeypatch.setattr(
            "geoparquet_io.core.partition.by_h3.partition_by_h3", mock_partition_fail
        )

        result = sub_partition_directory(
            directory=temp_partition_dir,
            partition_type="h3",
            min_size_bytes=threshold,
            resolution=4,
            in_place=True,
            verbose=False,
        )

        # Original file should still exist (not deleted due to error)
        assert os.path.exists(large_path)

        # Should have captured the error
        assert result["processed"] == 0
        assert len(result["errors"]) == 1
        assert result["errors"][0]["file"] == large_path
        assert "Simulated partition failure" in result["errors"][0]["error"]


class TestSubPartitionCLI:
    """Test CLI integration for sub-partitioning."""

    def test_partition_h3_with_directory_and_min_size(self, cli_runner, temp_partition_dir):
        """Test gpio partition h3 with directory input and --min-size."""
        from pathlib import Path

        # Copy the buildings test file to our temp directory
        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings_file, test_file)

        file_size = os.path.getsize(test_file)

        # Run with --min-size just below file size (use B suffix for bytes)
        # Use --force to bypass small partition warnings (test file only has 42 rows)
        result = cli_runner.invoke(
            partition,
            [
                "h3",
                temp_partition_dir,
                "--min-size",
                f"{file_size - 100}B",
                "--resolution",
                "4",
                "--in-place",
                "--force",
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"

        # Original should be gone
        assert not os.path.exists(test_file)

        # Sub-partition dir should exist
        subdir = os.path.join(temp_partition_dir, "test_h3")
        assert os.path.isdir(subdir)

    def test_partition_h3_directory_requires_min_size(self, cli_runner, temp_partition_dir):
        """Test that directory input without --min-size gives error."""
        result = cli_runner.invoke(
            partition,
            ["h3", temp_partition_dir, "--resolution", "4"],
        )
        assert result.exit_code != 0
        assert "min-size" in result.output.lower() or "directory" in result.output.lower()

    def test_partition_s2_with_directory_and_min_size(self, cli_runner, temp_partition_dir):
        """Test gpio partition s2 with directory input and --min-size."""
        from pathlib import Path

        skip_if_geography_unavailable()

        # Copy the buildings test file to our temp directory
        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings_file, test_file)

        file_size = os.path.getsize(test_file)

        result = cli_runner.invoke(
            partition,
            [
                "s2",
                temp_partition_dir,
                "--min-size",
                f"{file_size - 100}B",
                "--level",
                "8",
                "--in-place",
                "--force",
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert not os.path.exists(test_file)
        assert os.path.isdir(os.path.join(temp_partition_dir, "test_s2"))

    def test_partition_quadkey_with_directory_and_min_size(self, cli_runner, temp_partition_dir):
        """Test gpio partition quadkey with directory input and --min-size."""
        from pathlib import Path

        # Copy the buildings test file to our temp directory
        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings_file, test_file)

        file_size = os.path.getsize(test_file)

        result = cli_runner.invoke(
            partition,
            [
                "quadkey",
                temp_partition_dir,
                "--min-size",
                f"{file_size - 100}B",
                "--auto",
                "--in-place",
                "--force",
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert not os.path.exists(test_file)
        assert os.path.isdir(os.path.join(temp_partition_dir, "test_quadkey"))


class TestSubPartitionFailuresAreReported:
    """A directory sub-partition that partitions nothing must not exit 0 (#778)."""

    @staticmethod
    def _seed(directory) -> int:
        from pathlib import Path

        buildings = Path(__file__).parent / "data" / "buildings_test.parquet"
        target = os.path.join(directory, "test.parquet")
        shutil.copy(buildings, target)
        return os.path.getsize(target)

    def test_a_failing_file_makes_the_command_exit_non_zero(
        self, cli_runner, temp_partition_dir, monkeypatch
    ):
        """Every per-file error was caught and warned about, then the exit code
        said success -- so a run that partitioned nothing looked like a clean one."""
        size = self._seed(temp_partition_dir)

        def _boom(**kwargs):
            raise RuntimeError("simulated partition failure")

        # sub_partition_directory imports it inside the function, so patch the source.
        monkeypatch.setattr("geoparquet_io.core.partition.by_quadkey.partition_by_quadkey", _boom)

        result = cli_runner.invoke(
            partition,
            [
                "quadkey",
                temp_partition_dir,
                "--min-size",
                f"{size - 100}B",
                "--auto",
                "--force",
            ],
        )

        assert result.exit_code != 0, f"failures exited 0: {result.output}"
        assert "simulated partition failure" in result.output

    def test_an_unavailable_extension_is_reported_once_not_once_per_file(
        self, cli_runner, temp_partition_dir, monkeypatch
    ):
        """The preflight sits above the file loop, so N files get one message.

        Before this, `gpio partition s2 <dir>/ --min-size` printed the whole
        extension paragraph once per file and still exited 0.
        """
        from geoparquet_io.core.exceptions import ExtensionUnavailableError

        size = self._seed(temp_partition_dir)
        for extra in ("second.parquet", "third.parquet"):
            shutil.copy(
                os.path.join(temp_partition_dir, "test.parquet"),
                os.path.join(temp_partition_dir, extra),
            )

        calls = []

        def _unavailable(name, feature=None):
            calls.append(name)
            raise ExtensionUnavailableError(name, "1.5.5", "HTTP 404", feature=feature)

        monkeypatch.setattr(
            "geoparquet_io.core.duckdb_utils.require_community_extension", _unavailable
        )

        result = cli_runner.invoke(
            partition,
            ["s2", temp_partition_dir, "--min-size", f"{size - 100}B", "--level", "8", "--force"],
        )

        assert result.exit_code != 0, f"unavailable extension exited 0: {result.output}"
        assert len(calls) == 1, f"preflight ran {len(calls)} times for 3 files"
        assert result.output.count("gpio partition a5") == 1


class TestA5SubPartitioning:
    """A5 was the one hierarchical index that could not sub-partition (#733).

    When ``gpio partition s2`` cannot load the ``geography`` extension its own
    error tells users to switch to A5 -- which made A5's missing ``--min-size``
    / ``--in-place`` the gap that mattered most.
    """

    def test_sub_partition_directory_supports_a5(self, temp_partition_dir):
        """The registry in core/sub_partition.py used to raise for 'a5'."""
        from pathlib import Path

        from geoparquet_io.core.sub_partition import sub_partition_directory

        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        large_path = os.path.join(temp_partition_dir, "large.parquet")
        shutil.copy(buildings_file, large_path)

        threshold = os.path.getsize(large_path) - 100

        result = sub_partition_directory(
            directory=temp_partition_dir,
            partition_type="a5",
            min_size_bytes=threshold,
            resolution=A5_SPLITTING_RESOLUTION,
            in_place=True,
            force=True,
            verbose=False,
        )

        assert result["errors"] == []
        assert result["processed"] == 1
        assert not os.path.exists(large_path)

        subdir = os.path.join(temp_partition_dir, "large_a5")
        assert os.path.isdir(subdir)
        # A single-file copy would satisfy "a parquet exists", so assert the file
        # was really split and that every row survived the split.
        assert len(_partition_files(subdir)) > 1
        assert _total_rows(subdir) == BUILDINGS_ROWS

    def test_partition_a5_with_directory_and_min_size(self, cli_runner, temp_partition_dir):
        """End to end: gpio partition a5 <dir>/ --min-size ... --in-place."""
        from pathlib import Path

        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings_file, test_file)

        file_size = os.path.getsize(test_file)

        result = cli_runner.invoke(
            partition,
            [
                "a5",
                temp_partition_dir,
                "--min-size",
                f"{file_size - 100}B",
                "--resolution",
                str(A5_SPLITTING_RESOLUTION),
                "--in-place",
                "--force",
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert not os.path.exists(test_file)
        subdir = os.path.join(temp_partition_dir, "test_a5")
        assert os.path.isdir(subdir)
        assert len(_partition_files(subdir)) > 1
        assert _total_rows(subdir) == BUILDINGS_ROWS

    def test_partition_a5_directory_requires_min_size(self, cli_runner, temp_partition_dir):
        result = cli_runner.invoke(
            partition,
            ["a5", temp_partition_dir, "--resolution", "4"],
        )
        assert result.exit_code != 0
        assert "min-size" in result.output.lower() or "directory" in result.output.lower()

    def test_partition_a5_directory_with_auto_resolution(self, cli_runner, temp_partition_dir):
        """--auto has to reach the a5 branch of calculate_auto_resolution."""
        from pathlib import Path

        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings_file, test_file)

        file_size = os.path.getsize(test_file)

        result = cli_runner.invoke(
            partition,
            [
                "a5",
                temp_partition_dir,
                "--min-size",
                f"{file_size - 100}B",
                "--auto",
                "--in-place",
                "--force",
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert not os.path.exists(test_file)
        subdir = os.path.join(temp_partition_dir, "test_a5")
        assert os.path.isdir(subdir)
        assert _total_rows(subdir) == BUILDINGS_ROWS

    def test_an_unavailable_a5_extension_is_reported_once_not_once_per_file(
        self, cli_runner, temp_partition_dir, monkeypatch
    ):
        """A5 needs the 'a5' community extension, so it gets the same preflight
        as S2: one message above the file loop, not one per file."""
        from pathlib import Path

        from geoparquet_io.core.exceptions import ExtensionUnavailableError

        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        first = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings_file, first)
        size = os.path.getsize(first)
        for extra in ("second.parquet", "third.parquet"):
            shutil.copy(first, os.path.join(temp_partition_dir, extra))

        calls = []

        def _unavailable(name, feature=None):
            calls.append(name)
            raise ExtensionUnavailableError(name, "1.5.5", "HTTP 404", feature=feature)

        monkeypatch.setattr(
            "geoparquet_io.core.duckdb_utils.require_community_extension", _unavailable
        )

        result = cli_runner.invoke(
            partition,
            [
                "a5",
                temp_partition_dir,
                "--min-size",
                f"{size - 100}B",
                "--resolution",
                "4",
                "--force",
            ],
        )

        assert result.exit_code != 0, f"unavailable extension exited 0: {result.output}"
        assert calls == ["a5"], f"preflight ran {len(calls)} times for 3 files"


def _seed_with_a_null_geometry_row(directory) -> str:
    """Write a copy of the buildings fixture with one extra NULL-geometry row.

    A NULL geometry produces a NULL index cell, and ``partition_by_column``
    drops rows whose partition value is NULL -- so the sub-partition output has
    fewer rows than the input it was built from.
    """
    from pathlib import Path

    buildings = Path(__file__).parent / "data" / "buildings_test.parquet"
    table = pq.read_table(buildings)
    null_row = pa.table(
        {"id": pa.array(["null-geom"]), "geometry": pa.array([None], type=pa.binary())},
        schema=table.schema.remove_metadata(),
    )
    combined = pa.concat_tables([table.replace_schema_metadata(None), null_row])
    combined = combined.replace_schema_metadata(table.schema.metadata)

    target = os.path.join(directory, "large.parquet")
    pq.write_table(combined, target)
    return target


class TestInPlaceRowCountGuard:
    """--in-place deleted the original after checking only that SOME output existed.

    Rows whose partition value is NULL are dropped, so a file with a NULL or
    empty geometry lost those rows and the original was removed anyway.
    """

    def test_in_place_keeps_the_original_when_rows_are_lost(self, temp_partition_dir):
        from geoparquet_io.core.sub_partition import sub_partition_directory

        large_path = _seed_with_a_null_geometry_row(temp_partition_dir)
        source_rows = pq.read_metadata(large_path).num_rows
        assert source_rows == BUILDINGS_ROWS + 1

        result = sub_partition_directory(
            directory=temp_partition_dir,
            partition_type="a5",
            min_size_bytes=os.path.getsize(large_path) - 100,
            resolution=A5_SPLITTING_RESOLUTION,
            in_place=True,
            force=True,
            verbose=False,
        )

        assert os.path.exists(large_path), "original deleted despite losing rows"
        assert result["processed"] == 0
        assert len(result["errors"]) == 1

        message = result["errors"][0]["error"]
        assert str(source_rows) in message
        assert str(BUILDINGS_ROWS) in message
        assert "keeping original" in message

    def test_in_place_still_removes_the_original_when_every_row_survives(self, temp_partition_dir):
        from pathlib import Path

        from geoparquet_io.core.sub_partition import sub_partition_directory

        buildings = Path(__file__).parent / "data" / "buildings_test.parquet"
        large_path = os.path.join(temp_partition_dir, "large.parquet")
        shutil.copy(buildings, large_path)

        result = sub_partition_directory(
            directory=temp_partition_dir,
            partition_type="a5",
            min_size_bytes=os.path.getsize(large_path) - 100,
            resolution=A5_SPLITTING_RESOLUTION,
            in_place=True,
            force=True,
            verbose=False,
        )

        assert result["errors"] == []
        assert result["processed"] == 1
        assert not os.path.exists(large_path)


class TestDirectorySubPartitionPreview:
    """--preview was accepted, ignored, and the originals deleted anyway."""

    def test_preview_with_in_place_leaves_everything_untouched(
        self, cli_runner, temp_partition_dir
    ):
        from pathlib import Path

        buildings = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings, test_file)
        file_size = os.path.getsize(test_file)

        result = cli_runner.invoke(
            partition,
            [
                "a5",
                temp_partition_dir,
                "--min-size",
                f"{file_size - 100}B",
                "--resolution",
                str(A5_SPLITTING_RESOLUTION),
                "--preview",
                "--in-place",
                "--force",
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert os.path.exists(test_file), "--preview deleted the original"
        assert not os.path.exists(os.path.join(temp_partition_dir, "test_a5"))
        assert "test.parquet" in result.output

    def test_preview_reports_when_nothing_matches(self, cli_runner, temp_partition_dir):
        from pathlib import Path

        buildings = Path(__file__).parent / "data" / "buildings_test.parquet"
        shutil.copy(buildings, os.path.join(temp_partition_dir, "test.parquet"))

        result = cli_runner.invoke(
            partition,
            [
                "a5",
                temp_partition_dir,
                "--min-size",
                "100MB",
                "--resolution",
                str(A5_SPLITTING_RESOLUTION),
                "--preview",
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert "No files" in result.output


class TestDirectorySubPartitionIgnoredOptions:
    """Options that only apply to single-file partitioning were silently dropped."""

    def test_custom_column_name_is_rejected(self, cli_runner, temp_partition_dir):
        from pathlib import Path

        buildings = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings, test_file)

        result = cli_runner.invoke(
            partition,
            [
                "a5",
                temp_partition_dir,
                "--min-size",
                f"{os.path.getsize(test_file) - 100}B",
                "--resolution",
                str(A5_SPLITTING_RESOLUTION),
                "--a5-name",
                "my_cell",
            ],
        )

        assert result.exit_code != 0, f"ignored option exited 0: {result.output}"
        assert "--a5-name" in result.output
        assert os.path.exists(test_file)

    def test_output_folder_is_rejected(self, cli_runner, temp_partition_dir):
        from pathlib import Path

        buildings = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings, test_file)

        result = cli_runner.invoke(
            partition,
            [
                "h3",
                temp_partition_dir,
                os.path.join(temp_partition_dir, "out"),
                "--min-size",
                f"{os.path.getsize(test_file) - 100}B",
                "--resolution",
                "4",
            ],
        )

        assert result.exit_code != 0, f"ignored OUTPUT_FOLDER exited 0: {result.output}"
        assert "OUTPUT_FOLDER" in result.output
        assert os.path.exists(test_file)


class TestH3ExtensionPreflight:
    """h3 was missing from the preflight registry that a5 and s2 were in."""

    def test_an_unavailable_h3_extension_is_reported_once_not_once_per_file(
        self, cli_runner, temp_partition_dir, monkeypatch
    ):
        from pathlib import Path

        from geoparquet_io.core.exceptions import ExtensionUnavailableError

        buildings = Path(__file__).parent / "data" / "buildings_test.parquet"
        first = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings, first)
        size = os.path.getsize(first)
        for extra in ("second.parquet", "third.parquet"):
            shutil.copy(first, os.path.join(temp_partition_dir, extra))

        calls = []

        def _unavailable(name, feature=None):
            calls.append(name)
            raise ExtensionUnavailableError(name, "1.5.5", "HTTP 404", feature=feature)

        monkeypatch.setattr(
            "geoparquet_io.core.duckdb_utils.require_community_extension", _unavailable
        )

        result = cli_runner.invoke(
            partition,
            [
                "h3",
                temp_partition_dir,
                "--min-size",
                f"{size - 100}B",
                "--resolution",
                "4",
                "--force",
            ],
        )

        assert result.exit_code != 0, f"unavailable extension exited 0: {result.output}"
        assert calls == ["h3"], f"preflight ran {len(calls)} times for 3 files"


class TestApiDirectorySubPartition:
    """Directory sub-partitioning was reachable only from the CLI (#811).

    ``ops.partition_by_*`` takes a single table, so ``--min-size`` /
    ``--in-place`` -- the mode that turns oversized admin or string partitions
    into spatially indexed subdirectories -- had no Python front door at all.
    ``ops.sub_partition_by_<index>`` is that door: a function over a *directory*,
    one per index, mirroring the CLI command it is spelled after.
    """

    @staticmethod
    def _seed(directory) -> tuple[str, str]:
        """One file over the threshold and one under it."""
        from pathlib import Path

        buildings = Path(__file__).parent / "data" / "buildings_test.parquet"
        large = os.path.join(directory, "large.parquet")
        shutil.copy(buildings, large)
        small = os.path.join(directory, "small.parquet")
        pq.write_table(pa.table({"id": [1]}), small)
        return large, small

    def test_large_files_are_split_and_small_ones_are_left_alone(self, temp_partition_dir):
        from geoparquet_io.api import ops

        large, small = self._seed(temp_partition_dir)

        result = ops.sub_partition_by_a5(
            temp_partition_dir,
            min_size=f"{os.path.getsize(large) - 100}B",
            resolution=A5_SPLITTING_RESOLUTION,
            in_place=True,
            force=True,
        )

        assert result["errors"] == []
        assert result["processed"] == 1
        assert not os.path.exists(large), "the sub-partitioned original survived in_place=True"
        assert os.path.exists(small), "a file under the threshold was touched"

        subdir = os.path.join(temp_partition_dir, "large_a5")
        assert len(_partition_files(subdir)) > 1
        assert _total_rows(subdir) == BUILDINGS_ROWS

    def test_min_size_accepts_a_byte_count(self, temp_partition_dir):
        """A Python caller has a number to hand, not the CLI's '100MB' string."""
        from geoparquet_io.api import ops

        large, _small = self._seed(temp_partition_dir)

        result = ops.sub_partition_by_a5(
            temp_partition_dir,
            min_size=os.path.getsize(large) - 100,
            resolution=A5_SPLITTING_RESOLUTION,
            force=True,
        )

        assert result["processed"] == 1
        assert os.path.exists(large), "originals are kept without in_place=True"
        assert _total_rows(os.path.join(temp_partition_dir, "large_a5")) == BUILDINGS_ROWS

    def test_quadkey_partitions_by_its_own_index(self, temp_partition_dir):
        """auto=True is what a quadkey directory run takes, exactly as on the CLI.

        The quadkey partitioner needs both a column resolution and a partition
        resolution, and directory mode forwards only one -- so a lone
        ``resolution`` fails there through either front door (see
        ``test_a_lone_quadkey_resolution_fails_the_same_way_as_the_cli``).
        """
        from geoparquet_io.api import ops

        large, _small = self._seed(temp_partition_dir)

        result = ops.sub_partition_by_quadkey(
            temp_partition_dir,
            min_size=os.path.getsize(large) - 100,
            auto=True,
            in_place=True,
            force=True,
        )

        assert result["errors"] == []
        assert result["processed"] == 1
        assert _total_rows(os.path.join(temp_partition_dir, "large_quadkey")) == BUILDINGS_ROWS

    @pytest.mark.parametrize("door", ["cli", "api"])
    @pytest.mark.parametrize("partition_resolution", [0, 6, 13])
    def test_explicit_quadkey_resolutions_preserve_rows(
        self, cli_runner, temp_partition_dir, door, partition_resolution
    ):
        """Both entrypoints must carry the requested partition precision to disk."""
        from pathlib import Path

        from geoparquet_io.api import ops

        large, small = self._seed(temp_partition_dir)
        threshold = f"{os.path.getsize(large) - 100}B"
        if door == "cli":
            result = cli_runner.invoke(
                partition,
                [
                    "quadkey",
                    temp_partition_dir,
                    "--min-size",
                    threshold,
                    "--resolution",
                    "13",
                    "--partition-resolution",
                    str(partition_resolution),
                    "--in-place",
                    "--force",
                ],
            )
            assert result.exit_code == 0, result.output
        else:
            result = ops.sub_partition_by_quadkey(
                temp_partition_dir,
                min_size=threshold,
                resolution=13,
                partition_resolution=partition_resolution,
                in_place=True,
                force=True,
            )
            assert result["processed"] == 1
            assert result["errors"] == []
        output = Path(temp_partition_dir) / "large_quadkey"
        assert _total_rows(output) == BUILDINGS_ROWS
        assert not Path(large).exists()
        assert Path(small).exists()
        # Non-Hive output filenames encode the requested quadkey prefix.
        prefixes = {f.stem for f in _partition_files(output)}
        if partition_resolution == 0:
            assert len(_partition_files(output)) == 1
        else:
            assert all(len(prefix) == partition_resolution for prefix in prefixes)
            assert all(set(prefix) <= set("0123") for prefix in prefixes)

    @pytest.mark.parametrize("door", ["cli", "api"])
    @pytest.mark.parametrize(
        "partition_resolution,auto", [(-1, False), (24, False), (14, False), (6, True)]
    )
    def test_invalid_quadkey_resolutions_never_remove_originals(
        self, cli_runner, temp_partition_dir, door, partition_resolution, auto
    ):
        from pathlib import Path

        from geoparquet_io.api import ops
        from geoparquet_io.core.exceptions import PartitionError

        large, small = self._seed(temp_partition_dir)
        originals = {path: Path(path).read_bytes() for path in (large, small)}
        threshold = f"{os.path.getsize(large) - 100}B"
        if door == "cli":
            args = [
                "quadkey",
                temp_partition_dir,
                "--min-size",
                threshold,
                "--resolution",
                "13",
                "--partition-resolution",
                str(partition_resolution),
                "--in-place",
                "--force",
            ]
            if auto:
                args.append("--auto")
            result = cli_runner.invoke(partition, args)
            assert result.exit_code != 0
        else:
            with pytest.raises(PartitionError):
                ops.sub_partition_by_quadkey(
                    temp_partition_dir,
                    min_size=threshold,
                    resolution=13,
                    partition_resolution=partition_resolution,
                    auto=auto,
                    in_place=True,
                    force=True,
                )
        assert all(Path(path).read_bytes() == data for path, data in originals.items())
        assert not (Path(temp_partition_dir) / "large_quadkey").exists()

    def test_a_lone_quadkey_resolution_fails_the_same_way_as_the_cli(
        self, cli_runner, temp_partition_dir
    ):
        """Explicit mode requires both resolutions, just like single-file mode."""
        from geoparquet_io.api import ops
        from geoparquet_io.core.exceptions import PartitionError

        large, _small = self._seed(temp_partition_dir)
        threshold = f"{os.path.getsize(large) - 100}B"

        cli = cli_runner.invoke(
            partition,
            [
                "quadkey",
                temp_partition_dir,
                "--min-size",
                threshold,
                "--resolution",
                "6",
                "--force",
            ],
        )
        assert cli.exit_code != 0
        assert "partition-resolution" in cli.output

        with pytest.raises(PartitionError) as exc:
            ops.sub_partition_by_quadkey(
                temp_partition_dir, min_size=threshold, resolution=6, force=True
            )

        assert "partition-resolution" in str(exc.value)

    def test_preview_lists_candidates_and_writes_nothing(self, temp_partition_dir):
        """The CLI's --preview: say what would happen, change nothing (#790)."""
        from geoparquet_io.api import ops

        large, small = self._seed(temp_partition_dir)

        result = ops.sub_partition_by_a5(
            temp_partition_dir,
            min_size=os.path.getsize(large) - 100,
            resolution=A5_SPLITTING_RESOLUTION,
            preview=True,
            in_place=True,
            force=True,
        )

        assert result["preview"] is True
        assert result["processed"] == 0
        assert [c["path"] for c in result["candidates"]] == [large]
        assert result["candidates"][0]["size_bytes"] == os.path.getsize(large)
        assert result["candidates"][0]["output_dir"] == os.path.join(temp_partition_dir, "large_a5")

        assert os.path.exists(large), "preview deleted the original"
        assert os.path.exists(small)
        assert not os.path.exists(os.path.join(temp_partition_dir, "large_a5"))

    def test_preview_reports_no_candidates_without_failing(self, temp_partition_dir):
        from geoparquet_io.api import ops

        self._seed(temp_partition_dir)

        result = ops.sub_partition_by_a5(
            temp_partition_dir,
            min_size="100MB",
            resolution=A5_SPLITTING_RESOLUTION,
            preview=True,
        )

        assert result["candidates"] == []
        assert result["processed"] == 0

    def test_a_custom_index_column_name_is_rejected(self, temp_partition_dir):
        """Directory mode writes the default column name; asking for another is an error."""
        from geoparquet_io.api import ops
        from geoparquet_io.core.exceptions import InvalidParameterError

        large, _small = self._seed(temp_partition_dir)

        with pytest.raises(InvalidParameterError) as exc:
            ops.sub_partition_by_a5(
                temp_partition_dir,
                min_size=os.path.getsize(large) - 100,
                resolution=A5_SPLITTING_RESOLUTION,
                column_name="my_cell",
            )

        assert "column_name" in str(exc.value)
        assert os.path.exists(large), "a rejected call still partitioned something"
        assert not os.path.exists(os.path.join(temp_partition_dir, "large_a5"))

    def test_the_default_index_column_name_is_accepted(self, temp_partition_dir):
        """Passing the name it would have used anyway is not an error."""
        from geoparquet_io.api import ops

        large, _small = self._seed(temp_partition_dir)

        result = ops.sub_partition_by_a5(
            temp_partition_dir,
            min_size=os.path.getsize(large) - 100,
            resolution=A5_SPLITTING_RESOLUTION,
            column_name="a5_cell",
            preview=True,
        )

        assert [c["path"] for c in result["candidates"]] == [large]

    def test_an_output_directory_is_rejected(self, temp_partition_dir):
        """Each file gets a sibling <file>_<index>/; one shared output is not a thing."""
        from geoparquet_io.api import ops
        from geoparquet_io.core.exceptions import InvalidParameterError

        large, _small = self._seed(temp_partition_dir)

        with pytest.raises(InvalidParameterError) as exc:
            ops.sub_partition_by_h3(
                temp_partition_dir,
                min_size=os.path.getsize(large) - 100,
                resolution=4,
                output_dir=os.path.join(temp_partition_dir, "out"),
            )

        assert "output_dir" in str(exc.value)
        assert os.path.exists(large)

    def test_preview_needs_no_resolution_exactly_like_the_cli(self, temp_partition_dir):
        """`gpio partition h3 <dir> --min-size ... --preview` plans without a
        resolution -- the CLI's preview branch never reaches the resolution
        check -- so the API's preview must plan too, and return the same shape
        it would with one. A real run without a resolution still refuses.
        """
        from geoparquet_io.api import ops
        from geoparquet_io.core.exceptions import InvalidParameterError

        large, _small = self._seed(temp_partition_dir)
        threshold = os.path.getsize(large) - 100

        planned = ops.sub_partition_by_h3(temp_partition_dir, min_size=threshold, preview=True)

        assert planned["preview"] is True
        assert planned["processed"] == 0
        assert [c["path"] for c in planned["candidates"]] == [large]
        assert planned == ops.sub_partition_by_h3(
            temp_partition_dir, min_size=threshold, resolution=4, preview=True
        )

        with pytest.raises(InvalidParameterError, match="auto"):
            ops.sub_partition_by_h3(temp_partition_dir, min_size=threshold)

    def test_a_missing_resolution_is_refused_rather_than_guessed(self, temp_partition_dir):
        from geoparquet_io.api import ops
        from geoparquet_io.core.exceptions import InvalidParameterError

        large, _small = self._seed(temp_partition_dir)

        with pytest.raises(InvalidParameterError, match="auto"):
            ops.sub_partition_by_a5(temp_partition_dir, min_size=os.path.getsize(large) - 100)

    def test_a_non_directory_input_is_refused(self, temp_partition_dir):
        from geoparquet_io.api import ops
        from geoparquet_io.core.exceptions import InvalidParameterError

        large, _small = self._seed(temp_partition_dir)

        with pytest.raises(InvalidParameterError, match="directory"):
            ops.sub_partition_by_a5(large, min_size="1B", resolution=A5_SPLITTING_RESOLUTION)

    def test_an_unparseable_min_size_is_refused(self, temp_partition_dir):
        from geoparquet_io.api import ops
        from geoparquet_io.core.exceptions import InvalidParameterError

        self._seed(temp_partition_dir)

        with pytest.raises(InvalidParameterError, match="min_size"):
            ops.sub_partition_by_a5(
                temp_partition_dir, min_size="one hundred megabytes", resolution=4
            )

    def test_in_place_keeps_the_original_when_rows_are_lost(self, temp_partition_dir):
        """The row-count guard is the API's too, and a lost row is not a silent success."""
        from geoparquet_io.api import ops
        from geoparquet_io.core.exceptions import PartitionError

        large = _seed_with_a_null_geometry_row(temp_partition_dir)

        with pytest.raises(PartitionError) as exc:
            ops.sub_partition_by_a5(
                temp_partition_dir,
                min_size=os.path.getsize(large) - 100,
                resolution=A5_SPLITTING_RESOLUTION,
                in_place=True,
                force=True,
            )

        assert "keeping original" in str(exc.value)
        assert os.path.exists(large), "original deleted despite losing rows"
        assert exc.value.result["processed"] == 0
        assert len(exc.value.result["errors"]) == 1

    def test_s2_refuses_without_the_geography_extension(self, temp_partition_dir):
        """S2 cannot run in this release (#737): wired, and refusing for that reason."""
        from geoparquet_io.api import ops
        from geoparquet_io.core.exceptions import ExtensionUnavailableError
        from tests.conftest import skip_if_geography_available

        skip_if_geography_available()

        large, _small = self._seed(temp_partition_dir)

        with pytest.raises(ExtensionUnavailableError) as exc:
            ops.sub_partition_by_s2(
                temp_partition_dir, min_size=os.path.getsize(large) - 100, level=10
            )

        assert exc.value.name == "geography"
        assert os.path.exists(large)


class TestSubPartitionFrontDoorParity:
    """`gpio partition <index> <dir> --min-size` and its `ops` twin are one operation.

    Both front doors reach `core.sub_partition.sub_partition_directory`; patch it
    and compare the calls, so a knob wired into one door and not the other fails
    here rather than in a user's script (#811).
    """

    CASES = [
        ("a5", "sub_partition_by_a5", ["--resolution", "12"], {"resolution": 12}),
        ("h3", "sub_partition_by_h3", ["--resolution", "4"], {"resolution": 4}),
        ("s2", "sub_partition_by_s2", ["--level", "10"], {"level": 10}),
        (
            "quadkey",
            "sub_partition_by_quadkey",
            ["--resolution", "13", "--partition-resolution", "6"],
            {"resolution": 13, "partition_resolution": 6},
        ),
    ]

    @pytest.mark.parametrize(
        ("index", "function", "cli_args", "api_kwargs"),
        CASES,
        ids=[case[0] for case in CASES],
    )
    def test_both_front_doors_hand_core_the_same_call(
        self, cli_runner, temp_partition_dir, index, function, cli_args, api_kwargs
    ):
        from pathlib import Path
        from unittest.mock import patch

        from geoparquet_io.api import ops
        from geoparquet_io.core import sub_partition as core_sub_partition

        buildings = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings, test_file)
        threshold = f"{os.path.getsize(test_file) - 100}B"

        clean = {"processed": 1, "skipped": 0, "errors": []}

        with patch.object(
            core_sub_partition, "sub_partition_directory", return_value=clean
        ) as fake:
            result = cli_runner.invoke(
                partition,
                [index, temp_partition_dir, "--min-size", threshold, *cli_args],
            )
            assert result.exit_code == 0, f"CLI failed: {result.output}"
            cli_call = fake.call_args.kwargs

        with patch.object(
            core_sub_partition, "sub_partition_directory", return_value=clean
        ) as fake:
            getattr(ops, function)(temp_partition_dir, min_size=threshold, **api_kwargs)
            api_call = fake.call_args.kwargs

        assert api_call == cli_call
