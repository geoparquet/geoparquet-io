"""``gpio check --fix`` must not write a file its own checks then fail (#972).

``core/check_fixes.py`` hardcoded ``row_group_rows=100000`` at all five of its
write sites. The Parquet writer emits whole 2,048-row vectors, so that request
landed at 100,352 -- inside ``GENERAL_ROW_COUNT_RANGE`` (the pass/fail band of
``check row-group``) but well outside ``SPATIAL_ROW_COUNT_RANGE``, which is the
band ``check_optimization`` scores as one of its five factors. A ``--fix`` run
therefore produced a file that ``gpio check optimization`` marked ``[fail]`` on
its row-group factor and told the user to re-partition.

Which band ``--fix`` targets is a decision, not an oversight, so state it: it
targets the **spatial** band, via ``DEFAULT_ROW_GROUP_ROWS`` (49,152).
Three reasons.

* ``--fix`` is remedial. Its whole job is to leave a file that passes gpio's
  checks, and the spatial band is the stricter of the two, so only a file
  inside it is clean under every check gpio runs afterwards. Writing into the
  general band leaves the score exactly as ``--fix`` found it.
* ``--fix`` already Hilbert-sorts the file (``fix_spatial_ordering``). Sorting
  exists to make spatial predicates prune row groups, so a repair that sorts
  for spatial pruning and then writes groups too large to prune well is
  self-contradicting.
* Of the two bands, only ``SPATIAL_ROW_COUNT_RANGE`` has a measurement behind
  it (#775); ``GENERAL_ROW_COUNT_RANGE``'s top is inherited, as its own comment
  says.

The value is not typed here either. ``DEFAULT_ROW_GROUP_ROWS`` is
``align_to_writer_vector(SPATIAL_BAND_TOP_ROWS)`` -- the same constant and the
same snapping helper ``gpio sort`` uses since #967 -- so ``check --fix`` and
``gpio sort`` cannot drift apart, and neither can drift from the band. That is
also what #795 asked for when it noted that ``fix_spatial_order`` should
inherit the sort default.

Related: #961, #795, #967, #959.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core import check_fixes
from geoparquet_io.core.check_optimization import _check_row_group_size
from geoparquet_io.core.check_parquet_structure import SPATIAL_ROW_COUNT_RANGE
from geoparquet_io.core.parquet_writer import (
    DEFAULT_ROW_GROUP_ROWS,
    WRITER_VECTOR_ROWS,
    align_to_writer_vector,
)
from tests.fix_output_oracle import CRS84, assert_fix_output_is_sound

#: The synthetic fixtures below: 110,000 places rows, CRS84, no bbox column.
BAND_FIXTURE_ROWS = 110_000


def assert_band_output_is_sound(path, *, version):
    """WP-1 (#1018): a file inside the band still has to be a file gpio accepts."""
    assert_fix_output_is_sound(
        path,
        expected_rows=BAND_FIXTURE_ROWS,
        expected_crs=CRS84,
        expects_covering=False,
        expected_version_prefix=version,
    )


CHECK_FIXES_SOURCE = Path(check_fixes.__file__).read_text(encoding="utf-8")


def _row_group_kwarg(recorded: list[dict]) -> int:
    """The ``row_group_rows`` a fix asked its writer for."""
    assert len(recorded) == 1, f"expected exactly one write, got {len(recorded)}"
    return recorded[0]["row_group_rows"]


def _spy(monkeypatch, name: str) -> list[dict]:
    """Replace a ``check_fixes`` write entry point with a recording no-op."""
    recorded: list[dict] = []

    def record(*args, **kwargs):
        recorded.append(kwargs)
        return {"success": True}

    monkeypatch.setattr(check_fixes, name, record)
    return recorded


class TestEveryFixAsksForTheSortDefault:
    """All five write sites, so a new hardcode in any one of them fails."""

    def test_fix_compression(self, monkeypatch, places_test_file, tmp_path):
        recorded = _spy(monkeypatch, "write_parquet_with_metadata")

        check_fixes.fix_compression(places_test_file, str(tmp_path / "out.parquet"))

        assert _row_group_kwarg(recorded) == DEFAULT_ROW_GROUP_ROWS

    def test_fix_row_groups(self, monkeypatch, places_test_file, tmp_path):
        recorded = _spy(monkeypatch, "write_parquet_with_metadata")

        check_fixes.fix_row_groups(places_test_file, str(tmp_path / "out.parquet"))

        assert _row_group_kwarg(recorded) == DEFAULT_ROW_GROUP_ROWS

    def test_fix_bbox_removal(self, monkeypatch, places_test_file, tmp_path):
        recorded = _spy(monkeypatch, "write_parquet_with_metadata")

        check_fixes.fix_bbox_removal(places_test_file, str(tmp_path / "out.parquet"), "bbox")

        assert _row_group_kwarg(recorded) == DEFAULT_ROW_GROUP_ROWS

    def test_fix_bbox_column(self, monkeypatch, places_test_file, tmp_path):
        recorded = _spy(monkeypatch, "add_bbox_column")

        check_fixes.fix_bbox_column(places_test_file, str(tmp_path / "out.parquet"))

        assert _row_group_kwarg(recorded) == DEFAULT_ROW_GROUP_ROWS

    def test_fix_spatial_ordering_inherits_the_sort_default(
        self, monkeypatch, places_test_file, tmp_path
    ):
        """#795: the Hilbert repair asked for 100,000 while ``gpio sort hilbert``
        wrote 49,152, so the two spellings of the same operation disagreed."""
        recorded = _spy(monkeypatch, "hilbert_order")

        check_fixes.fix_spatial_ordering(places_test_file, str(tmp_path / "out.parquet"))

        assert _row_group_kwarg(recorded) == DEFAULT_ROW_GROUP_ROWS


class TestTheFixDefaultIsDerivedRatherThanTyped:
    """The number must stay tied to the band and to the writer's vector."""

    def test_it_sits_inside_the_band_check_optimization_scores(self):
        spatial_low, spatial_high = SPATIAL_ROW_COUNT_RANGE

        assert spatial_low <= DEFAULT_ROW_GROUP_ROWS <= spatial_high

    def test_it_is_a_whole_writer_vector_so_the_writer_passes_it_through(self):
        assert DEFAULT_ROW_GROUP_ROWS % WRITER_VECTOR_ROWS == 0
        assert align_to_writer_vector(DEFAULT_ROW_GROUP_ROWS) == DEFAULT_ROW_GROUP_ROWS

    def test_no_write_site_names_a_literal_row_count(self):
        """The bug was five copies of one literal. Pin that there are none.

        A literal here cannot be checked against the band, so it is free to
        drift out of it again -- which is exactly what 100,000 did.
        """
        requested = set(re.findall(r"row_group_rows=([^,\n)]+)", CHECK_FIXES_SOURCE))

        assert requested == {"DEFAULT_ROW_GROUP_ROWS"}, (
            "core/check_fixes.py asks a writer for a row-group size that is not "
            "the derived sort default. Use DEFAULT_ROW_GROUP_ROWS so the "
            f"value stays inside SPATIAL_ROW_COUNT_RANGE (#972). Found: {sorted(requested)}"
        )

    def test_the_summary_line_reports_the_size_actually_written(self, monkeypatch, tmp_path):
        """It said "100k rows/group" -- a number the fix no longer writes, and
        never quite wrote (the writer rounded that request to 100,352)."""
        _spy(monkeypatch, "fix_compression")

        summary = check_fixes._apply_compression_fix(
            {"row_groups": {"fix_available": True}},
            str(tmp_path / "in.parquet"),
            str(tmp_path / "out.parquet"),
            None,
            False,
            None,
        )

        assert summary == [f"Optimized row groups ({DEFAULT_ROW_GROUP_ROWS:,} rows/group)"]


@pytest.fixture
def oversized_row_group_file(places_test_file, tmp_path) -> str:
    """A file with enough rows that a 100,352-row layout has two groups.

    Below that threshold the whole question is invisible: a file under 64 MB
    with a single row group is exempt from the row-group factor, so the
    reproduction needs more rows than one 100,352-row group can hold.
    """
    source = pq.read_table(places_test_file).select(["fsq_place_id", "geometry"])
    schema_metadata = pq.ParquetFile(places_test_file).schema_arrow.metadata
    copies = -(-110_000 // source.num_rows)
    table = (
        pa.concat_tables([source] * copies)
        .slice(0, 110_000)
        .combine_chunks()
        .replace_schema_metadata(schema_metadata)
    )
    path = tmp_path / "oversized.parquet"
    pq.write_table(table, path, compression="ZSTD")
    return str(path)


class TestAFixedFilePassesTheRowGroupFactor:
    """The issue's own reproduction, end to end through the real writer."""

    def test_fix_row_groups_leaves_the_file_inside_the_spatial_band(
        self, oversized_row_group_file, tmp_path
    ):
        fixed = str(tmp_path / "fixed.parquet")

        check_fixes.fix_row_groups(oversized_row_group_file, fixed)

        verdict = _check_row_group_size(fixed)
        assert verdict["passed"], verdict["detail"]
        assert_band_output_is_sound(fixed, version="1.1")

    def test_the_old_constant_is_what_failed_it(self, oversized_row_group_file, tmp_path):
        """Control: the same file written the old way still fails, so the test
        above is measuring the fix and not the fixture."""
        fixed = str(tmp_path / "legacy.parquet")
        real_writer = check_fixes.write_parquet_with_metadata

        def write_at_100k(*args, **kwargs):
            kwargs["row_group_rows"] = 100_000
            return real_writer(*args, **kwargs)

        with mock.patch.object(check_fixes, "write_parquet_with_metadata", write_at_100k):
            check_fixes.fix_row_groups(oversized_row_group_file, fixed)

        verdict = _check_row_group_size(fixed)
        assert not verdict["passed"]
        assert "100,352" not in verdict["detail"]  # it reports the average, not the group size
        assert "55,000 rows per group" in verdict["detail"]
        # The control differs from the test above in the band and nothing else:
        # the legacy layout is still a file gpio's own validator accepts.
        assert_band_output_is_sound(fixed, version="1.1")


@pytest.fixture
def spatially_oversized_file(places_test_file, tmp_path) -> str:
    """110,000 rows in two 55,000-row groups.

    Sized so the file sits *inside* ``GENERAL_ROW_COUNT_RANGE`` and *outside*
    ``SPATIAL_ROW_COUNT_RANGE`` -- the exact band gap #972 is about. Two groups
    rather than one, because a sub-64 MB file with a single row group is exempt
    from the row-count assessment altogether.
    """
    source = pq.read_table(places_test_file).select(["fsq_place_id", "geometry"])
    schema_metadata = pq.ParquetFile(places_test_file).schema_arrow.metadata
    copies = -(-110_000 // source.num_rows)
    table = (
        pa.concat_tables([source] * copies)
        .slice(0, 110_000)
        .combine_chunks()
        .replace_schema_metadata(schema_metadata)
    )
    path = tmp_path / "spatially_oversized.parquet"
    pq.write_table(table, path, compression="ZSTD", row_group_size=55_000)
    return str(path)


def _row_group_factor(output: str) -> str:
    """The ``check optimization`` checklist line for the row-group factor."""
    lines = [line for line in output.splitlines() if "Row Group Size" in line]
    assert lines, f"no row-group line in:\n{output}"
    return lines[0]


class TestTheIssuesOwnReproductionThroughTheCli:
    """``--fix`` then ``check optimization``, as the user runs it (#972).

    The fix that closed #972 changed what ``--fix`` *writes*. It did not change
    what *triggers* it: ``fix_available`` came from ``assess_row_count``, whose
    verdict is deliberately the **general** band, so a 100,352-row-group file was
    "optimal", no fix ran, and the issue's transcript still reproduced verbatim
    after the fix. Worse, whether ``check all --fix`` left a
    ``check optimization``-clean file depended on whether some *unrelated*
    repair happened to force a rewrite: ``check spatial --fix``,
    ``check compression --fix`` and ``check bbox --fix`` all fixed the row groups
    as a side effect, while ``check row-group --fix`` -- the one named after the
    problem -- declined.

    The tests above call ``check_fixes.fix_*`` directly, so they cannot see that
    gate. These go through the CLI.
    """

    def test_check_row_group_fix_leaves_a_file_that_passes_check_optimization(
        self, spatially_oversized_file
    ):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        fix = runner.invoke(
            cli,
            ["check", "row-group", spatially_oversized_file, "--fix", "--no-backup"],
            input="y\n",
        )
        assert fix.exit_code == 0, fix.output
        assert "No fix needed" not in fix.output, fix.output

        after = runner.invoke(cli, ["check", "optimization", spatially_oversized_file])
        assert "[pass]" in _row_group_factor(after.output), after.output
        assert_band_output_is_sound(spatially_oversized_file, version="1.1")

    def test_check_all_fix_leaves_a_file_that_passes_check_optimization(
        self, spatially_oversized_file
    ):
        """The issue's literal transcript: ``check all --fix``, then ``check optimization``."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        fix = runner.invoke(
            cli,
            ["check", "all", spatially_oversized_file, "--fix", "--no-backup"],
            input="y\n",
        )
        assert fix.exit_code == 0, fix.output

        after = runner.invoke(cli, ["check", "optimization", spatially_oversized_file])
        assert "[pass]" in _row_group_factor(after.output), after.output
        # `check all` names the version from its own checks, so a 1.0 input is
        # repaired as 1.0 rather than upgraded behind the user.
        assert_band_output_is_sound(spatially_oversized_file, version="1.0")

    def test_a_file_already_in_the_spatial_band_is_left_alone(
        self, spatially_oversized_file, tmp_path
    ):
        """The other half: ``--fix`` must still decline when there is nothing to do."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        runner.invoke(
            cli,
            ["check", "row-group", spatially_oversized_file, "--fix", "--no-backup"],
            input="y\n",
        )
        before = Path(spatially_oversized_file).read_bytes()

        again = runner.invoke(
            cli,
            ["check", "row-group", spatially_oversized_file, "--fix", "--no-backup"],
            input="y\n",
        )

        assert "No fix needed" in again.output, again.output
        assert Path(spatially_oversized_file).read_bytes() == before
        assert_band_output_is_sound(spatially_oversized_file, version="1.1")


class TestTheTriggerFollowsTheSpatialBand:
    """``fix_available`` derives from the band ``--fix`` actually writes into."""

    def test_a_general_band_file_still_reads_as_optimal_but_offers_a_fix(
        self, spatially_oversized_file
    ):
        """#795/#958 settled the *verdict* on the general band. Do not move it.

        Only the trigger moves: the file is laid out the way mainstream writers
        lay files out, so it is not "wrong" -- but ``--fix`` writes 49,152 rows
        per group, so there is something for it to do.
        """
        from geoparquet_io.core.check_parquet_structure import check_row_groups

        results = check_row_groups(spatially_oversized_file, return_results=True, quiet=True)

        assert results["row_status"] == "optimal"
        assert results["passed"] is True
        assert results["fix_available"] is True

    def test_a_spatial_band_file_offers_nothing(self, spatially_oversized_file, tmp_path):
        from geoparquet_io.core.check_parquet_structure import check_row_groups

        table = pq.read_table(spatially_oversized_file)
        inside = tmp_path / "inside.parquet"
        pq.write_table(table, inside, compression="ZSTD", row_group_size=DEFAULT_ROW_GROUP_ROWS)

        results = check_row_groups(str(inside), return_results=True, quiet=True)

        assert results["row_status"] == "optimal"
        assert results["fix_available"] is False

    def test_a_small_single_group_file_is_still_exempt(self, places_test_file):
        """The leniency has to survive: 766 rows is below both bands, but
        rewriting a one-group file into 49,152-row groups changes nothing."""
        from geoparquet_io.core.check_parquet_structure import check_row_groups

        results = check_row_groups(places_test_file, return_results=True, quiet=True)

        assert results["fix_available"] is False

    def test_the_report_does_not_call_a_file_optimal_and_offer_a_fix_in_silence(
        self, spatially_oversized_file, caplog
    ):
        """The user sees *why* a green line is about to be acted on."""
        import logging

        from geoparquet_io.core.check_parquet_structure import check_row_groups

        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            results = check_row_groups(spatially_oversized_file, return_results=True)
        printed = caplog.text

        assert results["fix_available"] is True
        assert "--fix rewrites at 49,152 rows per group" in printed
        assert any("10,000-50,000" in rec for rec in results["recommendations"]), results[
            "recommendations"
        ]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
