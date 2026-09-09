"""
Tests for KD-tree partitioning commands.
"""

import json
import os
import sys
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import add, partition
from geoparquet_io.core.validate import validate_geoparquet


# Shared CliRunner to avoid repeated instantiation
@pytest.fixture(scope="module")
def cli_runner():
    """Module-scoped CliRunner for test efficiency."""
    return CliRunner()


def _failed_checks(path: str) -> list[str]:
    """Validator failures, minus the one Windows fails for a platform reason.

    On win32 `native_geo_stats_contains_data_*` reports every geometry as
    outside its own column's geospatial statistics, for any native GEOMETRY
    column written by any code path (#721, #748). The identical write produces a
    clean result on macOS and Linux, so it is a platform read/write gap rather
    than anything the write path under test decides. Same excuse, by name and
    only on win32, as `tests/test_secondary_geometry_carriers.py`.
    """
    failed = sorted(
        {c.name for c in validate_geoparquet(path).checks if c.status.value == "failed"}
    )
    if sys.platform == "win32":
        failed = [f for f in failed if not f.startswith("native_geo_stats_contains_data")]
    return failed


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

    def test_partition_kdtree_auto_is_not_overridden_by_the_old_default(
        self, buildings_test_file, cli_runner
    ):
        """Auto mode must size the tree, not fall through to 512 partitions (#813).

        `partition_by_kdtree` used to answer an unset `iterations` with a hardcoded
        9 *before* handing the file to `add_kdtree_column`, so `--auto` -- which is
        what a bare `gpio partition kdtree` selects -- never reached the code that
        sizes the tree from the row count. Every auto run produced 512 partitions
        whatever the input, which is exactly the guess the Python API was blamed for.
        """
        result = cli_runner.invoke(partition, ["kdtree", buildings_test_file, "--preview"])

        assert result.exit_code == 0, result.output
        assert "Auto-selected 2 partitions" in result.output
        # "512" alone can appear by chance inside a random temp-file name in the
        # output (it did, on CI); pin the failure shape, not the bare digits.
        assert "512 partitions" not in result.output


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


class TestKdtreeAutoSizing:
    """Unit contracts for the auto-sizing helpers in `core/add/kdtree.py` (#813).

    These pin each branch the front doors route through: the bare-int and
    ``("mb", value)`` shapes of ``auto_target_rows``, the unstat-able-path
    fallback in ``_file_size_mb``, and the guards that turn a bad target into a
    named error instead of a ZeroDivisionError or a 2^20-partition derail.
    """

    def test_file_size_mb_returns_zero_when_the_path_cannot_be_stated(self):
        from geoparquet_io.core.add.kdtree import _file_size_mb

        assert _file_size_mb("/no/such/dir/missing.parquet") == 0.0

    def test_mb_target_sizes_from_the_file_size(self):
        """1000 rows in 4 MB at a 1 MB target -> ~250 rows/partition -> 2 splits."""
        from geoparquet_io.core.add.kdtree import resolve_auto_iterations

        assert resolve_auto_iterations(1000, ("mb", 1.0), 4.0, announce=False) == 2

    def test_bare_int_target_means_rows(self):
        """A bare ``N`` is the same target as ``("rows", N)``."""
        from geoparquet_io.core.add.kdtree import resolve_auto_iterations

        assert resolve_auto_iterations(1000, 250, 4.0, announce=False) == 2
        assert resolve_auto_iterations(1000, ("rows", 250), 4.0, announce=False) == 2

    def test_mb_target_without_a_file_size_raises_instead_of_dividing_by_zero(self):
        """A remote or in-memory input has no size to stat; say so, don't crash."""
        from geoparquet_io.core.add.kdtree import resolve_auto_iterations
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="size"):
            resolve_auto_iterations(1000, ("mb", 1.0), 0.0, announce=False)

    @pytest.mark.parametrize("target", [0, -5, ("rows", 0), ("rows", -1), ("mb", 0), ("mb", -2.5)])
    def test_non_positive_targets_are_refused(self, target):
        """target_rows=0 used to derail auto to iterations=20 (2^20 partitions)."""
        from geoparquet_io.core.add.kdtree import resolve_auto_iterations
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="positive"):
            resolve_auto_iterations(1000, target, 4.0, announce=False)

    def test_add_kdtree_table_validates_iterations_before_serializing(self, monkeypatch):
        """An out-of-range ``iterations`` must fail before the table is written out.

        The check used to run only after ``pq.write_table`` had serialized the
        whole table to a temp file, so an invalid call paid the full I/O cost of
        a valid one first.
        """
        import types

        import geoparquet_io.core.add.kdtree as kdtree_mod
        from geoparquet_io.core.exceptions import InvalidParameterError

        serialized = []
        monkeypatch.setattr(
            kdtree_mod,
            "pq",
            types.SimpleNamespace(write_table=lambda *a, **k: serialized.append(a)),
        )

        with pytest.raises(InvalidParameterError, match="between 1 and 20"):
            kdtree_mod.add_kdtree_table(pa.table({"a": [1]}), iterations=25)

        assert serialized == []

    @staticmethod
    def _pipe_in(monkeypatch, parquet_path):
        """Mock stdin to carry ``parquet_path`` as an Arrow IPC stream."""
        import io

        from pyarrow import ipc

        source = pq.read_table(parquet_path)
        ipc_buffer = io.BytesIO()
        with ipc.RecordBatchStreamWriter(ipc_buffer, source.schema) as writer:
            writer.write_table(source)
        ipc_buffer.seek(0)

        mock_stdin = mock.MagicMock()
        mock_stdin.isatty.return_value = False
        mock_stdin.buffer = ipc_buffer
        monkeypatch.setattr(sys, "stdin", mock_stdin)

    def test_streaming_auto_sizes_the_tree_from_the_row_count(
        self, buildings_test_file, monkeypatch
    ):
        """A stdin-to-stdout pipe takes `_add_kdtree_streaming`; auto must work there too."""
        import io

        from pyarrow import ipc

        from geoparquet_io.core.add.kdtree import add_kdtree_column

        self._pipe_in(monkeypatch, buildings_test_file)
        output_buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.buffer = output_buffer
        mock_stdout.isatty.return_value = False
        monkeypatch.setattr(sys, "stdout", mock_stdout)

        add_kdtree_column("-", "-", auto_target_rows=("rows", 10))

        output_buffer.seek(0)
        result = ipc.RecordBatchStreamReader(output_buffer).read_all()
        assert "kdtree_cell" in result.column_names
        # 42 rows at a 10-row target -> 2 splits -> ids of length 3 ('0' + 2)
        assert {len(c) for c in result.column("kdtree_cell").to_pylist()} == {3}

    def test_partition_reads_stdin_input(self, buildings_test_file, tmp_path, monkeypatch):
        """`partition_by_kdtree("-", ...)` spools the piped stream to a temp file."""
        from geoparquet_io.core.partition.by_kdtree import partition_by_kdtree

        self._pipe_in(monkeypatch, buildings_test_file)
        out_dir = tmp_path / "parts"

        partition_by_kdtree("-", str(out_dir), iterations=2, skip_analysis=True)

        assert len(list(out_dir.glob("*.parquet"))) == 4

    def test_partition_verbose_announces_the_partition_count(self, buildings_test_file, tmp_path):
        """The verbose pre-add debug line names the partition count it will build."""
        from geoparquet_io.core.partition.by_kdtree import partition_by_kdtree

        out_dir = tmp_path / "parts"
        partition_by_kdtree(
            buildings_test_file,
            str(out_dir),
            iterations=2,
            verbose=True,
            skip_analysis=True,
        )

        assert len(list(out_dir.glob("*.parquet"))) == 4

    def test_partition_with_existing_column_needs_no_sizing_but_refuses_both(
        self, buildings_test_file, tmp_path
    ):
        """A file already carrying the column partitions without a sizing parameter.

        The add step is skipped and the existing cells drive the partition, as
        before #813 -- only the contradiction of naming both iterations and an
        auto target is still refused.
        """
        from geoparquet_io.core.add.kdtree import add_kdtree_column
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.partition.by_kdtree import partition_by_kdtree

        enriched = str(tmp_path / "enriched.parquet")
        add_kdtree_column(buildings_test_file, enriched, iterations=2)

        with pytest.raises(InvalidParameterError, match="auto"):
            partition_by_kdtree(
                enriched, str(tmp_path / "out"), iterations=2, auto_target_rows=("rows", 100)
            )

        partition_by_kdtree(enriched, str(tmp_path / "out"), skip_analysis=True)
        assert len(list((tmp_path / "out").glob("*.parquet"))) == 4


class TestAddKDTreePythonAPI:
    """The Python API write path for kdtree (#727).

    ``add_kdtree()`` hands the writer a table whose geometry column is plain
    ``large_binary`` carrying ``ARROW:extension:name = geoarrow.wkb`` in the
    *field metadata*. DuckDB honours that metadata on ``register()`` and
    presents the column as ``GEOMETRY``, so the writer must not wrap it in
    ``ST_GeomFromWKB``.
    """

    def test_add_kdtree_then_write(self, buildings_test_file, temp_output_file):
        """read().add_kdtree().write() must produce a valid GeoParquet file."""
        import geoparquet_io as gpio

        gpio.read(buildings_test_file).add_kdtree(iterations=3).write(temp_output_file)

        assert os.path.exists(temp_output_file)
        table = pq.read_table(temp_output_file)
        assert "kdtree_cell" in table.schema.names
        assert table.num_rows == pq.read_table(buildings_test_file).num_rows
        assert b"geo" in (table.schema.metadata or {})

        # Not just "readable": the geometry has to survive intact. The bug this
        # covers lived in the writer's carrier decision for the geometry column,
        # so a file that parses but declares the wrong encoding, or carries a
        # native GEOMETRY type inside a 1.x file, is still a regression.
        geo = json.loads(table.schema.metadata[b"geo"])
        primary = geo["primary_column"]
        assert geo["columns"][primary]["encoding"] == "WKB"
        assert pa.types.is_binary(table.schema.field(primary).type)
        assert table.column(primary).null_count == 0

        # Every validator check, with nothing excused. `crs_valid_geometry` used
        # to fail here in any session that had imported `geoarrow.pyarrow`,
        # because `extract_crs_from_table` stringified the resolved geoarrow CRS
        # object into "ProjJsonCrs(OGC:CRS84)" (issue #816, fixed).
        assert _failed_checks(temp_output_file) == []


class TestKDTreeColumnNameQuoting:
    """``--kdtree-name`` reaches the KD-tree ``SELECT ... AS <name>`` verbatim.

    Both KD-tree query builders alias the computed partition id to the
    user-supplied column name: ``_build_sampling_query`` (approximate mode, the
    default) and the exact-mode branch of ``add_kdtree_column``. Interpolating
    that name bare means any name that is not a bare SQL identifier either
    breaks the query or, with a crafted value, injects into the projection
    (#924, the CLI-supplied sibling of #918).
    """

    # A name that closes the alias and appends an attacker-chosen projection
    # column. Bare, this yields an extra ``pwn`` column; quoted, it is one
    # delimited identifier that happens to contain punctuation.
    _PAYLOAD = "cell, 42 AS pwn"

    @pytest.mark.parametrize("mode", [[], ["--exact"]], ids=["approx", "exact"])
    def test_a_name_needing_quoting_round_trips(self, buildings_test_file, temp_output_file, mode):
        """A name with a space and an embedded double quote survives verbatim."""
        name = 'weird "kd" name'
        result = CliRunner().invoke(
            add,
            [
                "kdtree",
                buildings_test_file,
                temp_output_file,
                "--partitions",
                "4",
                "--kdtree-name",
                name,
                *mode,
            ],
        )
        assert result.exit_code == 0, result.output
        table = pq.read_table(temp_output_file)
        assert name in table.schema.names
        values = table.column(name).to_pylist()
        assert values and all(v is None or all(c in "01" for c in v) for v in values)

    @pytest.mark.parametrize("mode", [[], ["--exact"]], ids=["approx", "exact"])
    def test_an_injection_payload_stays_one_column(
        self, buildings_test_file, temp_output_file, mode
    ):
        """The payload must land as a single column name, not extra SQL."""
        result = CliRunner().invoke(
            add,
            [
                "kdtree",
                buildings_test_file,
                temp_output_file,
                "--partitions",
                "4",
                "--kdtree-name",
                self._PAYLOAD,
                *mode,
            ],
        )
        assert result.exit_code == 0, result.output
        names = pq.read_table(temp_output_file).schema.names
        assert "pwn" not in names
        assert self._PAYLOAD in names
