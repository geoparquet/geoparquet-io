"""Guard-branch tests for the ``gpio check`` group.

These branches are the early exits and validation errors that sit in front of
the real check implementations: the "no files" guard every subcommand repeats,
the ``--pmtiles`` preconditions, the multi-file ``--fix-output`` rule, the
non-GeoParquet skip, and the multi-file verbose banner. They were carried over
byte-for-byte when the group moved out of ``cli/main.py`` into
``cli/commands/check.py`` (#920) and had never been exercised offline, so each
one is pinned here on message text *and* on whether any per-file work started.

Every test is offline and unmarked, so it runs in the fast lane.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from unittest import mock

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

# NOTE: import the *module object* and patch attributes on it. A dotted target
# such as mock.patch("geoparquet_io.cli.commands.check.X") is hazardous here:
# `geoparquet_io/__init__.py` rebinds `geoparquet_io.cli` to the Click Group,
# so ``geoparquet_io.cli.commands`` getattr-walks to ``Group.commands`` (a
# dict). See tests/test_write_memory_forwarding.py for the full explanation.
from geoparquet_io.cli.commands import check as cli_check
from geoparquet_io.cli.main import cli
from geoparquet_io.core import pmtiles as core_pmtiles
from tests.fix_output_oracle import CRS84, assert_fix_output_is_sound
from tests.native_geo_probes import geo_block

#: ``places_test.parquet`` and everything derived from it: 766 rows, CRS84.
PLACES_ROWS = 766


def assert_places_output_is_sound(path, *, version, covering=True):
    """WP-1 (#1018): whatever a guard reports, the bytes it left have to be valid.

    Reporting the right filename is not the same as writing the right file, and
    every test below that lets a real fix run now checks both.
    """
    assert_fix_output_is_sound(
        path,
        expected_rows=PLACES_ROWS,
        expected_crs=CRS84,
        expects_covering=covering,
        expected_version_prefix=version,
    )


# (subcommand, extra args) for every command that repeats the empty-input guard.
EMPTY_INPUT_SUBCOMMANDS = [
    ("all", []),
    ("spatial", []),
    ("compression", []),
    ("bbox", []),
    ("row-group", []),
    ("optimization", []),
]


@pytest.fixture
def empty_partition_dir(tmp_path):
    """A directory that looks like a partition but holds no parquet files."""
    empty = tmp_path / "empty_partition"
    empty.mkdir()
    return str(empty)


@pytest.fixture
def poorly_ordered_file(tmp_path, places_test_file):
    """A writable copy of places, shuffled and split into 16 tiny row groups.

    Both ``check spatial --fix`` and ``check row-group --fix`` report a fix is
    available for this file, which the pristine fixtures do not: places is
    already Hilbert-sorted and lands in a single row group.
    """
    table = pq.read_table(places_test_file)
    shuffled = table.take(pa.array(np.random.RandomState(0).permutation(table.num_rows)))
    target = tmp_path / "poorly_ordered.parquet"
    pq.write_table(shuffled, target, row_group_size=50)
    return str(target)


@pytest.fixture
def non_geo_parquet(tmp_path):
    """A plain parquet file with no ``geo`` metadata and no geometry column."""
    path = tmp_path / "plain.parquet"
    pq.write_table(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}), path)
    return str(path)


class TestEmptyInputGuard:
    """Every check subcommand bails out before touching a runner."""

    @pytest.mark.parametrize("subcommand,extra", EMPTY_INPUT_SUBCOMMANDS)
    def test_no_parquet_files_found(self, subcommand, extra, empty_partition_dir):
        runner = CliRunner()
        with mock.patch.object(cli_check, "MultiFileCheckRunner") as fake_runner:
            result = runner.invoke(cli, ["check", subcommand, empty_partition_dir, *extra])

        assert result.exit_code == 0, result.output
        assert "No parquet files found" in result.output
        # The guard returns *before* any per-file work is set up.
        fake_runner.assert_not_called()

    def test_partition_notice_is_shown_alongside_the_guard(self, empty_partition_dir):
        """The notice from ``get_files_to_check`` is printed before the guard."""
        runner = CliRunner()
        result = runner.invoke(cli, ["check", "all", empty_partition_dir])

        assert result.exit_code == 0, result.output
        assert "No parquet files found in partition" in result.output


class TestPmtilesGuards:
    """``--pmtiles`` needs ``--fix`` and it needs tippecanoe."""

    def test_pmtiles_without_fix_is_a_usage_error(self, places_test_file):
        runner = CliRunner()
        with mock.patch.object(cli_check, "check_structure_impl") as impl:
            result = runner.invoke(cli, ["check", "all", places_test_file, "--pmtiles"])

        assert result.exit_code == 2, result.output
        assert "--pmtiles requires --fix to generate PMTiles from fixed files" in result.output
        impl.assert_not_called()

    def test_pmtiles_without_tippecanoe_is_refused(self, places_test_file):
        runner = CliRunner()
        with (
            mock.patch.object(core_pmtiles, "_check_tippecanoe", return_value=False) as probe,
            mock.patch.object(cli_check, "check_structure_impl") as impl,
        ):
            result = runner.invoke(cli, ["check", "all", places_test_file, "--pmtiles", "--fix"])

        assert result.exit_code == 1, result.output
        assert "--pmtiles requires tippecanoe." in result.output
        assert "brew install tippecanoe" in result.output
        probe.assert_called_once()
        # Refused up front, before the first file is checked.
        impl.assert_not_called()


class TestMultiFileFixOutputValidation:
    """``--fix-output`` must name a directory when several files are fixed."""

    def test_file_path_is_rejected_for_multiple_files(self, country_partition_dir, tmp_path):
        not_a_dir = tmp_path / "fixed.parquet"
        not_a_dir.write_bytes(b"")

        runner = CliRunner()
        with mock.patch.object(cli_check, "MultiFileCheckRunner") as fake_runner:
            result = runner.invoke(
                cli,
                [
                    "check",
                    "all",
                    country_partition_dir,
                    "--all-files",
                    "--fix",
                    "--fix-output",
                    str(not_a_dir),
                ],
            )

        assert result.exit_code == 1, result.output
        assert "--fix-output must be a directory, not a file path" in result.output
        assert "When fixing multiple files (4 files)" in result.output
        # Nothing was checked, and nothing was written to the bogus output path.
        fake_runner.assert_not_called()
        assert not_a_dir.read_bytes() == b""

    def test_directory_fix_output_passes_validation(self, country_partition_dir, tmp_path):
        """The same invocation with a real directory gets past the guard."""
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        runner = CliRunner()
        with mock.patch.object(cli_check, "MultiFileCheckRunner") as fake_runner:
            fake_runner.return_value.verbose = False
            fake_runner.return_value.is_multi_file = True
            result = runner.invoke(
                cli,
                [
                    "check",
                    "all",
                    country_partition_dir,
                    "--all-files",
                    "--fix",
                    "--fix-output",
                    str(out_dir),
                    "--random-sample-size",
                    "50",
                ],
            )

        # Validation did not fire: the command proceeded to build the runner.
        assert result.exit_code == 0, result.output
        assert "--fix-output must be a directory" not in result.output
        fake_runner.assert_called_once()
        assert fake_runner.call_args.args[0] == sorted(
            str(p) for p in Path(country_partition_dir).glob("*.parquet")
        )


class TestNonGeoParquetSkip:
    """``check all --pmtiles`` skips files that carry no geo metadata."""

    def test_non_geoparquet_file_is_skipped(self, non_geo_parquet):
        runner = CliRunner()
        with (
            mock.patch.object(core_pmtiles, "_check_tippecanoe", return_value=True),
            mock.patch.object(cli_check, "check_structure_impl") as impl,
            mock.patch.object(core_pmtiles, "create_pmtiles_from_geoparquet") as create,
        ):
            result = runner.invoke(cli, ["check", "all", non_geo_parquet, "--pmtiles", "--fix"])

        assert result.exit_code == 0, result.output
        assert f"Skipping {non_geo_parquet}: not a GeoParquet file" in result.output
        # The skip happens before the structure check, so neither the checks nor
        # the PMTiles generation ran on a file that would crash them.
        impl.assert_not_called()
        create.assert_not_called()


class TestVerboseMultiFileBanner:
    """``--verbose`` over a partition prints a per-file banner."""

    def test_banner_names_each_file_with_its_position(self, country_partition_dir):
        runner = CliRunner()
        result = runner.invoke(
            cli, ["check", "compression", country_partition_dir, "--all-files", "--verbose"]
        )

        assert result.exit_code == 0, result.output
        assert "File 1/4: " in result.output
        assert "File 4/4: " in result.output
        assert "El_Salvador.parquet" in result.output
        assert "Nicaragua.parquet" in result.output
        # The banner replaces the overwriting progress line in verbose mode.
        assert "Checking files... " not in result.output


class TestSpecValidationSummary:
    """``check all`` prints either the spec summary or the full spec report."""

    def test_spec_details_prints_the_full_report(self, places_test_file):
        runner = CliRunner()
        summary = runner.invoke(cli, ["check", "all", places_test_file])
        detailed = runner.invoke(cli, ["check", "all", places_test_file, "--spec-details"])

        assert detailed.exit_code == 0, detailed.output
        assert "Spec Validation:" in detailed.output
        # The summary form is a single count line; --spec-details replaces it
        # with the per-check terminal report, so it is strictly longer.
        assert len(detailed.output) > len(summary.output)
        assert "GeoParquet Validation Report" in detailed.output


class TestPmtilesGeneration:
    """After a successful ``--fix``, PMTiles are built from the fixed file."""

    def test_pmtiles_are_generated_from_the_fixed_output(self, places_test_file, tmp_path):
        fixed = tmp_path / "fixed.parquet"

        runner = CliRunner()
        with (
            mock.patch.object(core_pmtiles, "_check_tippecanoe", return_value=True),
            mock.patch.object(core_pmtiles, "create_pmtiles_from_geoparquet") as create,
        ):
            result = runner.invoke(
                cli,
                [
                    "check",
                    "all",
                    places_test_file,
                    "--fix",
                    "--fix-output",
                    str(fixed),
                    "--pmtiles",
                    "--random-sample-size",
                    "50",
                ],
            )

        assert result.exit_code == 0, result.output
        expected_pmtiles = str(tmp_path / "fixed.pmtiles")
        assert f"Generated {expected_pmtiles}" in result.output
        # PMTiles are built from the *fixed* file, not the original input.
        create.assert_called_once()
        assert create.call_args.kwargs["input_path"] == str(fixed)
        assert create.call_args.kwargs["output_path"] == expected_pmtiles
        # The file PMTiles were built from is itself a file gpio accepts.
        assert_places_output_is_sound(fixed, version="1.0", covering=False)

    def test_pmtiles_failure_is_reported_without_aborting(self, places_test_file, tmp_path):
        fixed = tmp_path / "fixed.parquet"

        runner = CliRunner()
        with (
            mock.patch.object(core_pmtiles, "_check_tippecanoe", return_value=True),
            mock.patch.object(
                core_pmtiles,
                "create_pmtiles_from_geoparquet",
                side_effect=RuntimeError("tippecanoe blew up"),
            ),
        ):
            result = runner.invoke(
                cli,
                [
                    "check",
                    "all",
                    places_test_file,
                    "--fix",
                    "--fix-output",
                    str(fixed),
                    "--pmtiles",
                    "--random-sample-size",
                    "50",
                ],
            )

        # The failure is reported per file and the run still exits cleanly.
        assert result.exit_code == 0, result.output
        assert f"PMTiles failed for {places_test_file}: tippecanoe blew up" in result.output
        # The PMTiles step failed; the fixed parquet it was handed is still good.
        assert_places_output_is_sound(fixed, version="1.0", covering=False)


class TestMultiFileFixSummary:
    """A multi-file ``--fix`` says which files it rewrote."""

    @pytest.fixture
    def poorly_ordered_partition(self, tmp_path, places_test_file):
        """Three shuffled copies of places in one directory."""
        table = pq.read_table(places_test_file)
        partition = tmp_path / "partition"
        partition.mkdir()
        for index, seed in enumerate((0, 1, 2)):
            shuffled = table.take(pa.array(np.random.RandomState(seed).permutation(table.num_rows)))
            pq.write_table(shuffled, partition / f"p{index}.parquet", row_group_size=50)
        return partition

    def test_summary_names_the_rewritten_files(self, poorly_ordered_partition):
        """``--all-files --fix`` rewrites every file in place; say so at the end.

        In multi-file mode the per-file "Optimized file:" lines are suppressed,
        so the run used to finish on ``Summary: 3 warnings (3 files checked)``
        -- which reads as if nothing had been written, when in fact every file
        in the directory had just been rewritten in place.
        """
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "check",
                "spatial",
                str(poorly_ordered_partition),
                "--all-files",
                "--fix",
                "--random-sample-size",
                "50",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "Fixed 3 files" in result.output
        for index in range(3):
            target = poorly_ordered_partition / f"p{index}.parquet"
            assert str(target) in result.output
            assert Path(f"{target}.bak").exists()
            assert_places_output_is_sound(target, version="1.1")

    def test_a_long_fix_list_is_truncated(self, capsys):
        """Past ``max_issues_shown`` the list is capped, like the issue list."""
        runner = cli_check.MultiFileCheckRunner([f"f{i}.parquet" for i in range(5)])
        for index in range(5):
            runner.record_fix(f"f{index}.parquet", f"f{index}.parquet.bak" if index else None)
        runner.print_summary()

        output = capsys.readouterr().out
        assert "Fixed 5 files:" in output
        assert "  - f0.parquet\n" in output  # no backup: no suffix
        assert "  - f1.parquet (backup: f1.parquet.bak)" in output
        assert "f3.parquet" not in output
        assert "... and 2 more" in output

    def test_a_single_fix_is_not_pluralised(self, capsys):
        runner = cli_check.MultiFileCheckRunner(["a.parquet", "b.parquet"])
        runner.record_fix("a.parquet", None)
        runner.print_summary()

        assert "Fixed 1 file:" in capsys.readouterr().out

    def test_nothing_is_reported_when_no_fix_was_applied(self, poorly_ordered_partition):
        """A check-only run says nothing about fixes."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["check", "spatial", str(poorly_ordered_partition), "--all-files"],
        )

        assert result.exit_code == 0, result.output
        assert "Fixed" not in result.output


class TestSpatialFixReporting:
    """``check spatial --fix`` reports the file it rewrote."""

    def test_fix_reports_the_optimized_output(self, poorly_ordered_file, tmp_path):
        fixed = tmp_path / "sorted.parquet"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "check",
                "spatial",
                poorly_ordered_file,
                "--fix",
                "--fix-output",
                str(fixed),
                "--random-sample-size",
                "50",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "Data may not be optimally spatially ordered" in result.output
        assert "Applying Hilbert spatial ordering..." in result.output
        assert "Spatial ordering applied successfully!" in result.output
        assert f"Optimized file: {fixed}" in result.output
        assert fixed.exists()
        assert_places_output_is_sound(fixed, version="1.1")

    def test_in_place_fix_reports_output_and_backup(self, poorly_ordered_file):
        """#941: ``--fix`` with no ``--fix-output`` rewrites the input in place.

        ``fix_spatial_ordering`` used to hand the input path straight to
        ``hilbert_order`` as its output, and ``handle_output_overwrite`` refused
        it with "Cannot overwrite input file" -- *after* the backup had already
        been written. Every other fix function routes an in-place rewrite
        through a temp file; this one now does too.
        """
        before = pq.read_table(poorly_ordered_file).column("fsq_place_id").to_pylist()

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["check", "spatial", poorly_ordered_file, "--fix", "--random-sample-size", "50"],
        )

        assert result.exit_code == 0, result.output
        assert "Cannot overwrite input file" not in result.output
        assert "Spatial ordering applied successfully!" in result.output
        assert f"Optimized file: {poorly_ordered_file}" in result.output
        assert f"Backup: {poorly_ordered_file}.bak" in result.output
        assert Path(f"{poorly_ordered_file}.bak").exists()

        # The input really was rewritten, and the backup holds the original.
        after = pq.read_table(poorly_ordered_file).column("fsq_place_id").to_pylist()
        assert sorted(after) == sorted(before)
        assert after != before
        assert (
            pq.read_table(f"{poorly_ordered_file}.bak").column("fsq_place_id").to_pylist() == before
        )
        assert_places_output_is_sound(poorly_ordered_file, version="1.1")
        # The backup is the untouched input: still 1.0, still uncovered.
        assert_places_output_is_sound(f"{poorly_ordered_file}.bak", version="1.0", covering=False)

    def test_an_aliased_fix_output_is_treated_as_an_in_place_fix(self, poorly_ordered_file):
        """``--fix-output ./same.parquet`` is in place, backup and all.

        ``handle_fix_common`` decided "am I writing over my own input?" by
        comparing the raw strings, so an aliased spelling took the *not*
        in-place branch: no ``.bak``, no confirmation under ``--no-backup`` --
        and then ``handle_output_overwrite``, which compares ``resolve()``,
        refused the write outright. Both halves have to agree.
        """
        target = Path(poorly_ordered_file)
        # Built as a string: pathlib normalises a "." component straight back out.
        alias = f"{target.parent}{os.sep}.{os.sep}{target.name}"
        assert alias != poorly_ordered_file
        before = pq.read_table(poorly_ordered_file).column("fsq_place_id").to_pylist()

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "check",
                "spatial",
                alias,
                "--fix",
                "--fix-output",
                poorly_ordered_file,
                "--random-sample-size",
                "50",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "Cannot overwrite input file" not in result.output
        backup = Path(f"{alias}.bak")
        assert backup.exists(), result.output
        assert pq.read_table(backup).column("fsq_place_id").to_pylist() == before
        after = pq.read_table(poorly_ordered_file).column("fsq_place_id").to_pylist()
        assert sorted(after) == sorted(before)
        assert after != before
        assert_places_output_is_sound(poorly_ordered_file, version="1.1")

    def test_overwrite_option_exists_for_parity_with_the_other_fixes(self):
        """``check spatial`` accepts ``--overwrite``, like its fix-capable siblings."""
        spatial = cli.commands["check"].commands["spatial"]
        assert any("--overwrite" in param.opts for param in spatial.params)


class TestRowGroupFixReporting:
    """``check row-group --fix`` reports the rewritten file and its backup."""

    def test_in_place_fix_reports_output_and_backup(self, poorly_ordered_file):
        runner = CliRunner()
        result = runner.invoke(cli, ["check", "row-group", poorly_ordered_file, "--fix"])

        assert result.exit_code == 0, result.output
        assert "Optimizing row groups..." in result.output
        assert "Row groups optimized successfully!" in result.output
        assert f"Optimized file: {poorly_ordered_file}" in result.output
        assert f"Backup: {poorly_ordered_file}.bak" in result.output
        # The 16 tiny row groups are consolidated into one.
        assert pq.ParquetFile(poorly_ordered_file).num_row_groups == 1
        assert_places_output_is_sound(poorly_ordered_file, version="1.1")


class TestBboxFixReporting:
    """``check bbox --fix`` is version-aware: it adds for 1.x, removes for geo-native."""

    def test_no_fix_needed_when_bbox_is_already_optimal(self, places_with_covering_file, tmp_path):
        target = tmp_path / "optimal.parquet"
        shutil.copy(places_with_covering_file, target)

        runner = CliRunner()
        result = runner.invoke(cli, ["check", "bbox", str(target), "--fix"])

        assert result.exit_code == 0, result.output
        assert "No fix needed - bbox is optimal!" in result.output
        # Nothing was rewritten, so no backup exists.
        assert not (tmp_path / "optimal.parquet.bak").exists()
        assert_places_output_is_sound(target, version="1.1")

    def test_v1_file_gains_a_bbox_column(self, buildings_test_file, tmp_path):
        target = tmp_path / "buildings.parquet"
        shutil.copy(buildings_test_file, target)

        runner = CliRunner()
        result = runner.invoke(cli, ["check", "bbox", str(target), "--fix"])

        assert result.exit_code == 0, result.output
        assert "Bbox optimized successfully!" in result.output
        assert f"Optimized file: {target}" in result.output
        assert f"Backup: {target}.bak" in result.output
        assert "bbox" in pq.ParquetFile(target).schema_arrow.names
        assert_fix_output_is_sound(
            target,
            expected_rows=42,
            expected_crs=CRS84,
            expects_covering=True,
            expected_version_prefix="1.1",
        )

    def test_geo_native_file_loses_its_bbox_column(self, fields_geom_type_only_file, tmp_path):
        target = tmp_path / "pgo.parquet"
        shutil.copy(fields_geom_type_only_file, target)
        assert "bbox" in pq.ParquetFile(target).schema_arrow.names

        runner = CliRunner()
        result = runner.invoke(cli, ["check", "bbox", str(target), "--fix"])

        assert result.exit_code == 0, result.output
        assert "Bbox column removed successfully!" in result.output
        assert f"Optimized file: {target}" in result.output
        assert f"Backup: {target}.bak" in result.output
        assert "bbox" not in pq.ParquetFile(target).schema_arrow.names
        # A native-geo-only input stays native-geo-only: no `geo` key invented,
        # and the CRS still read straight off the Parquet logical type.
        assert geo_block(target) is None
        assert_fix_output_is_sound(
            target, expected_rows=100, expected_crs=CRS84, expects_covering=False
        )
