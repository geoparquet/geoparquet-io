"""Zero-row inputs must stop with a clean message, not a traceback (issue #823).

A zero-row GeoParquet is an ordinary outcome of a spatial filter
(``gpio extract --bbox ...`` over an area with nothing in it), so every consumer
downstream has to cope with a valid file that has no rows and no row groups.
"""

from __future__ import annotations

import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import check, extract, partition
from geoparquet_io.core.check_parquet_structure import check_all, check_compression
from geoparquet_io.core.exceptions import PartitionError
from geoparquet_io.core.partition.common import (
    _calculate_size_estimates,
    analyze_partition_strategy,
    partition_by_column,
    preview_partition,
    raise_if_no_rows,
)
from tests.conftest import skip_if_grid_unavailable

# A bbox in the Southern Ocean: matches nothing in places_test.parquet.
EMPTY_BBOX = "170,-80,171,-79"


@pytest.fixture
def empty_geoparquet(tmp_path, places_test_file):
    """A valid zero-row GeoParquet, built exactly as in the issue repro."""
    out = tmp_path / "empty.parquet"
    result = CliRunner().invoke(extract, [f"--bbox={EMPTY_BBOX}", places_test_file, str(out)])
    assert result.exit_code == 0, result.output
    meta = pq.ParquetFile(out).metadata
    assert meta.num_rows == 0
    assert meta.num_row_groups == 0
    return str(out)


def _assert_clean_stop(result, message):
    assert result.exit_code == 1, result.output
    assert message in result.output
    assert "Traceback" not in result.output
    assert "TypeError" not in result.output


class TestPartitionZeroRows:
    """``gpio partition *`` on a zero-row file stops before computing a strategy."""

    def test_string_stops_cleanly(self, empty_geoparquet, tmp_path):
        out = tmp_path / "out"
        result = CliRunner().invoke(
            partition, ["string", empty_geoparquet, str(out), "--column", "name"]
        )
        _assert_clean_stop(result, "no rows to partition")
        assert not out.exists(), "nothing should be written for an empty input"

    def test_string_preview_stops_cleanly(self, empty_geoparquet, tmp_path):
        result = CliRunner().invoke(
            partition,
            ["string", empty_geoparquet, str(tmp_path / "out"), "--column", "name", "--preview"],
        )
        _assert_clean_stop(result, "no rows to partition")

    def test_string_skip_analysis_stops_cleanly(self, empty_geoparquet, tmp_path):
        """``--skip-analysis`` skips the pre-flight, so the guard cannot live only there."""
        out = tmp_path / "out"
        result = CliRunner().invoke(
            partition,
            ["string", empty_geoparquet, str(out), "--column", "name", "--skip-analysis"],
        )
        _assert_clean_stop(result, "no rows to partition")
        assert not out.exists()

    def test_string_force_stops_cleanly(self, empty_geoparquet, tmp_path):
        """``--force`` overrides analysis findings, but there is still nothing to write."""
        out = tmp_path / "out"
        result = CliRunner().invoke(
            partition,
            ["string", empty_geoparquet, str(out), "--column", "name", "--force"],
        )
        _assert_clean_stop(result, "no rows to partition")
        assert not out.exists()

    @pytest.mark.parametrize(
        "args",
        [
            ["h3", "--resolution", "2"],
            ["a5", "--resolution", "2"],
            ["quadkey", "--resolution", "2", "--partition-resolution", "2"],
            ["kdtree"],
            ["s2", "--level", "2"],
        ],
        ids=["h3", "a5", "quadkey", "kdtree", "s2"],
    )
    def test_spatial_index_partitions_stop_cleanly(self, empty_geoparquet, tmp_path, args):
        skip_if_grid_unavailable(args[0])
        out = tmp_path / "out"
        result = CliRunner().invoke(partition, [args[0], empty_geoparquet, str(out), *args[1:]])
        assert result.exit_code == 1, result.output
        assert "Traceback" not in result.output
        assert "TypeError" not in result.output
        assert "no rows to partition" in result.output
        # Stopped before the index column was added, so no file was rewritten.
        assert "Successfully added" not in result.output
        assert "Added KD-tree column" not in result.output
        assert not out.exists()

    @pytest.mark.parametrize(
        "args",
        [
            ["h3", "--auto"],
            ["a5", "--auto"],
            ["quadkey", "--auto"],
            ["s2", "--auto"],
            ["kdtree", "--auto", "1000"],
        ],
        ids=["h3", "a5", "quadkey", "s2", "kdtree"],
    )
    def test_auto_resolution_stops_cleanly(self, empty_geoparquet, tmp_path, args):
        """``--auto`` sizes the index off the row count, so it meets the empty input first.

        h3/a5/quadkey stop in ``calculate_auto_resolution`` with its own wording;
        s2/kdtree reach the shared guard. Either way: exit 1, no traceback.
        """
        skip_if_grid_unavailable(args[0])
        out = tmp_path / "out"
        result = CliRunner().invoke(partition, [args[0], empty_geoparquet, str(out), *args[1:]])
        assert result.exit_code == 1, result.output
        assert "Traceback" not in result.output
        assert "TypeError" not in result.output
        assert "no rows" in result.output
        assert not out.exists()

    def test_admin_stops_before_fetching_boundaries(self, empty_geoparquet, tmp_path):
        """``partition admin`` used to reach the network before finding nothing."""
        result = CliRunner().invoke(
            partition,
            [
                "admin",
                empty_geoparquet,
                str(tmp_path / "out"),
                "--dataset",
                "gaul",
                "--levels",
                "continent",
            ],
        )
        _assert_clean_stop(result, "no rows to partition")
        assert "spatial join" not in result.output


class TestPartitionCoreZeroRows:
    def test_size_estimates_guard_none_and_zero_rows(self):
        # DuckDB returns NULL aggregates over an empty group set; never compare None to int.
        assert _calculate_size_estimates(1000, None, 0, 0, 0) == (0, 0, 0)
        assert _calculate_size_estimates(1000, 0, 0, 0, 0) == (0, 0, 0)
        assert _calculate_size_estimates(1000, None, None, None, None) == (0, 0, 0)

    def test_guard_fails_open_when_row_count_is_unreadable(self, tmp_path):
        """An unreadable input is the normal code path's problem to report, not the guard's."""
        raise_if_no_rows(str(tmp_path / "does_not_exist.parquet"))

    def test_guard_passes_a_file_with_rows(self, places_test_file):
        raise_if_no_rows(places_test_file)

    def test_analyze_raises_partition_error(self, empty_geoparquet):
        with pytest.raises(PartitionError, match="no rows to partition"):
            analyze_partition_strategy(empty_geoparquet, "name")

    def test_preview_raises_partition_error(self, empty_geoparquet):
        with pytest.raises(PartitionError, match="no rows to partition"):
            preview_partition(empty_geoparquet, "name")

    def test_partition_by_column_raises_before_writing(self, empty_geoparquet, tmp_path):
        out = tmp_path / "out"
        with pytest.raises(PartitionError, match="no rows to partition"):
            partition_by_column(empty_geoparquet, str(out), "name", skip_analysis=True)
        assert not out.exists()

    def test_analyze_all_null_column_reports_no_values(self, places_test_file, tmp_path):
        """Rows exist but the partition column is entirely NULL.

        The same aggregate comes back all-NULL, so this must be reported the way
        ``preview_partition`` already reports it, not crash on ``None > 0``.
        """
        table = pq.read_table(places_test_file).slice(0, 50)
        table = table.append_column("all_null", pa.nulls(table.num_rows, pa.string()))
        path = tmp_path / "all_null.parquet"
        pq.write_table(table, path)
        with pytest.raises(PartitionError, match="No non-NULL values found in column 'all_null'"):
            analyze_partition_strategy(str(path), "all_null")


class TestCheckZeroRows:
    """``gpio check *`` on a zero-row file reports what it can instead of raising."""

    def test_check_compression_core_reports_no_information(self, empty_geoparquet):
        result = check_compression(empty_geoparquet, return_results=True, quiet=True)
        assert result["passed"] is True
        assert result["current_compression"] is None
        assert result["geometry_column"] == "geometry"
        assert result["fix_available"] is False
        assert result["issues"] == []

    def test_check_all_core_does_not_raise(self, empty_geoparquet):
        results = check_all(empty_geoparquet, return_results=True, quiet=True)
        assert results["compression"]["current_compression"] is None

    @pytest.mark.parametrize("subcommand", ["all", "compression", "optimization"])
    def test_check_cli_exits_cleanly(self, empty_geoparquet, subcommand):
        result = CliRunner().invoke(check, [subcommand, empty_geoparquet])
        assert result.exit_code == 0, result.output
        assert "KeyError" not in result.output
        assert "Traceback" not in result.output
        assert "No compression information" in result.output

    @pytest.mark.parametrize("subcommand", ["bbox", "row-group", "spatial", "spec"])
    def test_other_check_subcommands_still_pass(self, empty_geoparquet, subcommand):
        result = CliRunner().invoke(check, [subcommand, empty_geoparquet])
        assert result.exit_code == 0, result.output
        assert "Traceback" not in result.output


@pytest.mark.integration
def test_piped_empty_extract_into_partition_stops_cleanly(places_test_file, tmp_path):
    """The #804 pipeline: an empty producer result reaches a consumer that exits cleanly."""
    out = tmp_path / "out"
    pipeline = (
        f"gpio extract --bbox={EMPTY_BBOX} {places_test_file} - | "
        f"gpio partition string - {out} --column=name"
    )
    result = subprocess.run(pipeline, shell=True, capture_output=True, text=True, timeout=300)
    combined = result.stdout + result.stderr
    assert result.returncode == 1, combined
    assert "no rows to partition" in combined
    assert "Traceback" not in combined
    assert "TypeError" not in combined
