"""Guard clauses and cache management in ``gpio add`` / ``gpio partition``.

These are the branches the CLI takes *before* (or instead of) calling into
``core/``: the argument guards that turn a bad invocation into a readable
message, the ``StreamingError`` -> ``ClickException`` translations that keep a
streaming failure from surfacing as a traceback, the ``--vecorel`` overrides
that quietly rewrite ``--dataset``/``--levels``, and the ``--clear-cache``
prompt on ``gpio add admin-divisions``.

Everything here is offline. The ``core/`` entry points that would fetch remote
boundaries or open DuckDB are patched on their defining module, and the admin
cache directory is redirected into ``tmp_path``, so no real cache is touched.

Patching follows the repo convention of ``importlib.import_module`` plus
``patch.object``: dotted string targets under ``geoparquet_io.cli.*`` resolve to
the Click group rather than the module on Python 3.10.
"""

import importlib
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import add, partition
from geoparquet_io.core.streaming import StreamingError

cli_add = importlib.import_module("geoparquet_io.cli.commands.add")
cli_partition = importlib.import_module("geoparquet_io.cli.commands.partition")
streaming = importlib.import_module("geoparquet_io.core.streaming")
admin_datasets = importlib.import_module("geoparquet_io.core.admin_datasets")
admin_divisions = importlib.import_module("geoparquet_io.core.add.admin_divisions")

ONE_MB = 1024 * 1024


@pytest.fixture
def admin_cache_dir(tmp_path):
    """Redirect the admin-dataset cache into ``tmp_path`` for the whole test.

    ``clear_cache()`` reaches ``get_cache_dir()`` through the module namespace,
    so patching that one name covers both the CLI's sizing pass and the actual
    deletion.
    """
    path = tmp_path / "admin-cache"
    with patch.object(admin_datasets, "get_cache_dir", return_value=path):
        yield path


def _write_mb(path):
    """Write a file of exactly 1 MB so the reported size is predictable."""
    path.write_bytes(b"\0" * ONE_MB)
    return path


class TestAddAdminDivisionsClearCache:
    """``gpio add admin-divisions --clear-cache`` reports, prompts, then deletes."""

    def test_confirmed_clear_reports_sizes_and_deletes_only_parquet(
        self, admin_cache_dir, places_test_file, tmp_path
    ):
        admin_cache_dir.mkdir(parents=True)
        gaul = _write_mb(admin_cache_dir / "gaul.parquet")
        overture = _write_mb(admin_cache_dir / "overture.parquet")
        unrelated = admin_cache_dir / "notes.txt"
        unrelated.write_text("not a cached dataset", encoding="utf-8")

        runner = CliRunner()
        with patch.object(admin_divisions, "add_admin_divisions_multi"):
            result = runner.invoke(
                add,
                [
                    "admin-divisions",
                    places_test_file,
                    str(tmp_path / "out.parquet"),
                    "--clear-cache",
                ],
                input="y\n",
            )

        assert result.exit_code == 0, result.output
        # The user sees what is about to go, priced in MB, before the prompt.
        assert f"Cache directory: {admin_cache_dir}" in result.output
        assert "Files to delete: 2" in result.output
        assert "Total size: 2.00 MB" in result.output
        assert "Delete all cached admin datasets?" in result.output
        # ...and what actually went.
        assert "Cleared cache: 2 files, 2.00 MB freed" in result.output

        # Only the cached datasets are deleted; anything else in the directory stays.
        assert not gaul.exists()
        assert not overture.exists()
        assert unrelated.exists()

    def test_declined_clear_keeps_the_cached_files(
        self, admin_cache_dir, places_test_file, tmp_path
    ):
        admin_cache_dir.mkdir(parents=True)
        gaul = _write_mb(admin_cache_dir / "gaul.parquet")

        runner = CliRunner()
        with patch.object(admin_divisions, "add_admin_divisions_multi"):
            result = runner.invoke(
                add,
                [
                    "admin-divisions",
                    places_test_file,
                    str(tmp_path / "out.parquet"),
                    "--clear-cache",
                ],
                input="n\n",
            )

        assert result.exit_code == 0, result.output
        assert "Cache clear cancelled." in result.output
        assert "Cleared cache" not in result.output
        assert gaul.exists(), "declining the prompt must not delete anything"

    def test_empty_cache_directory_says_there_is_nothing_to_delete(
        self, admin_cache_dir, places_test_file, tmp_path
    ):
        admin_cache_dir.mkdir(parents=True)

        runner = CliRunner()
        with patch.object(admin_divisions, "add_admin_divisions_multi"):
            result = runner.invoke(
                add,
                [
                    "admin-divisions",
                    places_test_file,
                    str(tmp_path / "out.parquet"),
                    "--clear-cache",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "No cached datasets found." in result.output
        # No prompt at all when there is nothing to confirm.
        assert "Delete all cached admin datasets?" not in result.output

    def test_absent_cache_directory_says_so(self, admin_cache_dir, places_test_file, tmp_path):
        assert not admin_cache_dir.exists()

        runner = CliRunner()
        with patch.object(admin_divisions, "add_admin_divisions_multi"):
            result = runner.invoke(
                add,
                [
                    "admin-divisions",
                    places_test_file,
                    str(tmp_path / "out.parquet"),
                    "--clear-cache",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "No cache directory found." in result.output


class TestAddAdminDivisionsArgumentGuards:
    """``gpio add admin-divisions`` refuses streaming and demands an output path."""

    def test_stdout_streaming_is_refused_with_a_worked_example(self, places_test_file):
        """No OUTPUT_PARQUET and a piped stdout means "stream", which is unsupported."""
        runner = CliRunner()
        result = runner.invoke(add, ["admin-divisions", places_test_file])

        assert result.exit_code == 1
        assert "not yet supported for 'gpio add admin-divisions'" in result.output
        assert "gpio add admin-divisions input.parquet output.parquet" in result.output

    def test_stdin_input_is_refused(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(add, ["admin-divisions", "-", str(tmp_path / "out.parquet")])

        assert result.exit_code == 1
        assert "Streaming (stdin/stdout) is not yet supported" in result.output

    def test_missing_output_on_a_terminal_is_a_usage_error(self, places_test_file):
        """With stdout a terminal there is nothing to stream to, so the argument is required."""
        runner = CliRunner()
        with patch.object(streaming, "should_stream_output", return_value=False):
            result = runner.invoke(add, ["admin-divisions", places_test_file])

        assert result.exit_code == 2
        assert "Missing argument 'OUTPUT_PARQUET'." in result.output


class TestAddAdminDivisionsVecorel:
    """``--vecorel`` forces the Overture dataset and country,region levels."""

    def test_vecorel_overrides_levels_and_warns(self, places_test_file, tmp_path):
        runner = CliRunner()
        with patch.object(admin_divisions, "add_admin_divisions_multi") as impl:
            result = runner.invoke(
                add,
                [
                    "admin-divisions",
                    places_test_file,
                    str(tmp_path / "out.parquet"),
                    "--vecorel",
                    "--dataset",
                    "gaul",
                    "--levels",
                    "continent,country,department",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "--vecorel overrides --levels to 'country,region'" in result.output

        # The override is real, not just a warning: the impl is told overture/country,region.
        impl.assert_called_once()
        kwargs = impl.call_args.kwargs
        assert kwargs["dataset_name"] == "overture"
        assert kwargs["levels"] == ["country", "region"]
        assert kwargs["vecorel"] is True

    def test_vecorel_without_levels_does_not_warn(self, places_test_file, tmp_path):
        runner = CliRunner()
        with patch.object(admin_divisions, "add_admin_divisions_multi") as impl:
            result = runner.invoke(
                add,
                [
                    "admin-divisions",
                    places_test_file,
                    str(tmp_path / "out.parquet"),
                    "--vecorel",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "overrides --levels" not in result.output
        assert impl.call_args.kwargs["levels"] == ["country", "region"]


class TestAddGeometryMetricsGuards:
    """``gpio add geometry-metrics`` demands an output path when not streaming."""

    def test_missing_output_on_a_terminal_is_a_usage_error(self, places_test_file):
        runner = CliRunner()
        with patch.object(streaming, "should_stream_output", return_value=False):
            result = runner.invoke(add, ["geometry-metrics", places_test_file])

        assert result.exit_code == 2
        assert "Missing argument 'OUTPUT_PARQUET'." in result.output


# (command name, name of the core entry point imported into cli/commands/add.py)
ADD_STREAMING_COMMANDS = [
    ("bbox", "add_bbox_column_impl"),
    ("h3", "add_h3_column_impl"),
    ("a5", "add_a5_column_impl"),
    ("s2", "add_s2_column_impl"),
    ("quadkey", "add_quadkey_column_impl"),
]


class TestAddStreamingErrorsBecomeClickExceptions:
    """A ``StreamingError`` must reach the user as its message, not a traceback."""

    @pytest.mark.parametrize("command,impl_name", ADD_STREAMING_COMMANDS)
    def test_validate_output_failure_is_reported(
        self, command, impl_name, places_test_file, tmp_path
    ):
        runner = CliRunner()
        message = "Missing output. Pipe to another command or specify an output file."
        with patch.object(streaming, "validate_output", side_effect=StreamingError(message)):
            with patch.object(cli_add, impl_name) as impl:
                result = runner.invoke(
                    add, [command, places_test_file, str(tmp_path / "out.parquet")]
                )

        assert result.exit_code == 1
        assert message in result.output
        assert "Traceback" not in result.output
        impl.assert_not_called()

    @pytest.mark.parametrize("command,impl_name", ADD_STREAMING_COMMANDS)
    def test_impl_failure_is_reported(self, command, impl_name, places_test_file, tmp_path):
        runner = CliRunner()
        message = f"stdin for {command} carried no Arrow stream"
        with patch.object(cli_add, impl_name, side_effect=StreamingError(message)):
            result = runner.invoke(add, [command, places_test_file, str(tmp_path / "out.parquet")])

        assert result.exit_code == 1
        assert message in result.output
        assert "Traceback" not in result.output


class TestAddKdtreeGuards:
    """``gpio add kdtree`` rejects mutually exclusive sampling options."""

    def test_approx_and_exact_are_mutually_exclusive(self, places_test_file, tmp_path):
        runner = CliRunner()
        with patch.object(cli_add, "add_kdtree_column_impl") as impl:
            result = runner.invoke(
                add,
                [
                    "kdtree",
                    places_test_file,
                    str(tmp_path / "out.parquet"),
                    "--approx",
                    "200000",
                    "--exact",
                ],
            )

        assert result.exit_code == 2
        assert "--approx and --exact are mutually exclusive" in result.output
        impl.assert_not_called()

    def test_exact_alone_is_accepted(self, places_test_file, tmp_path):
        """The guard keys off an explicitly changed --approx, not --exact on its own."""
        runner = CliRunner()
        with patch.object(cli_add, "add_kdtree_column_impl") as impl:
            result = runner.invoke(
                add, ["kdtree", places_test_file, str(tmp_path / "out.parquet"), "--exact"]
            )

        assert result.exit_code == 0, result.output
        assert impl.call_args.kwargs["sample_size"] is None


class TestPartitionAdminVecorelOverrides:
    """``gpio partition admin --vecorel`` rewrites both --dataset and --levels."""

    def test_vecorel_overrides_dataset_and_levels_and_warns_about_each(
        self, places_test_file, tmp_path
    ):
        runner = CliRunner()
        with patch.object(cli_partition, "partition_admin_hierarchical_impl") as impl:
            result = runner.invoke(
                partition,
                [
                    "admin",
                    places_test_file,
                    str(tmp_path / "out"),
                    "--vecorel",
                    "--dataset",
                    "gaul",
                    "--levels",
                    "continent,country",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "--vecorel overrides --dataset gaul to 'overture'" in result.output
        assert "--vecorel overrides --levels to 'country,region'" in result.output

        impl.assert_called_once()
        kwargs = impl.call_args.kwargs
        assert kwargs["dataset_name"] == "overture"
        assert kwargs["levels"] == ["country", "region"]
        assert kwargs["vecorel"] is True

    def test_vecorel_with_overture_and_no_levels_warns_about_neither(
        self, places_test_file, tmp_path
    ):
        runner = CliRunner()
        with patch.object(cli_partition, "partition_admin_hierarchical_impl") as impl:
            result = runner.invoke(
                partition,
                [
                    "admin",
                    places_test_file,
                    str(tmp_path / "out"),
                    "--vecorel",
                    "--dataset",
                    "overture",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "overrides" not in result.output
        assert impl.call_args.kwargs["levels"] == ["country", "region"]


class TestPartitionStringStreamingError:
    """A ``StreamingError`` out of the string partitioner is reported, not raised."""

    def test_streaming_error_becomes_a_click_exception(self, places_test_file, tmp_path):
        runner = CliRunner()
        message = "Streaming input cannot be partitioned by string"
        with patch.object(
            cli_partition, "partition_by_string_impl", side_effect=StreamingError(message)
        ):
            result = runner.invoke(
                partition,
                ["string", places_test_file, str(tmp_path / "out"), "--column", "country"],
            )

        assert result.exit_code == 1
        assert message in result.output
        assert "Traceback" not in result.output


class TestPartitionOutputFolderRequired:
    """Without ``--preview`` every partition command needs an OUTPUT_FOLDER."""

    @pytest.mark.parametrize(
        "args,impl_name",
        [
            (["s2", "--level", "10"], "partition_by_s2_impl"),
            (["a5", "--resolution", "10"], "partition_by_a5_impl"),
            (
                ["quadkey", "--resolution", "12", "--partition-resolution", "6"],
                "partition_by_quadkey_impl",
            ),
        ],
        ids=["s2", "a5", "quadkey"],
    )
    def test_missing_output_folder_is_a_usage_error(
        self, args, impl_name, places_test_file, tmp_path
    ):
        runner = CliRunner()
        with patch.object(cli_partition, impl_name) as impl:
            result = runner.invoke(partition, [args[0], places_test_file, *args[1:]])

        assert result.exit_code == 2
        assert "OUTPUT_FOLDER is required unless using --preview" in result.output
        impl.assert_not_called()


class TestPartitionKdtreeGuards:
    """``gpio partition kdtree`` rejects mutually exclusive option pairs."""

    def test_partitions_and_auto_are_mutually_exclusive(self, places_test_file, tmp_path):
        runner = CliRunner()
        with patch.object(cli_partition, "partition_by_kdtree_impl") as impl:
            result = runner.invoke(
                partition,
                [
                    "kdtree",
                    places_test_file,
                    str(tmp_path / "out"),
                    "--partitions",
                    "4",
                    "--auto",
                    "1000",
                ],
            )

        assert result.exit_code == 2
        assert "--partitions and --auto are mutually exclusive" in result.output
        impl.assert_not_called()

    def test_approx_and_exact_are_mutually_exclusive(self, places_test_file, tmp_path):
        runner = CliRunner()
        with patch.object(cli_partition, "partition_by_kdtree_impl") as impl:
            result = runner.invoke(
                partition,
                [
                    "kdtree",
                    places_test_file,
                    str(tmp_path / "out"),
                    "--approx",
                    "200000",
                    "--exact",
                ],
            )

        assert result.exit_code == 2
        assert "--approx and --exact are mutually exclusive" in result.output
        impl.assert_not_called()
