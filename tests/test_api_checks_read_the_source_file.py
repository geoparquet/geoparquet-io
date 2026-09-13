"""`Table.check_*` / `Table.validate()` must answer about the file they were given.

gpio #1060 and #1061. Every check routed through ``Table._with_temp_file``,
which writes the in-memory table out with gpio's *default* settings and checks
that. So ``check_compression()`` re-encoded to ZSTD before measuring and could
never report a compression problem, ``check_row_groups()`` counted the one group
the writer had just produced rather than the file's fifteen, and
``Table.validate()`` validated a re-write stamped at the version gpio would
*write* (#1061) instead of the one the file declares.

The rule this module pins: a ``Table`` that came from a file answers about that
file's bytes, and its answer is the CLI's answer. A ``Table`` with no file
behind it has no layout to describe, so it answers the only question that has an
answer -- "how would this be laid out if gpio wrote it now?" -- and says so, in
``CheckResult.prospective`` and in ``to_dict()``.

Every case here compares the two front ends over the *same* bytes rather than
asserting the API merely changed: a check that agrees with itself is not the
contract, agreeing with `gpio check` is.
"""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli


@pytest.fixture
def runner():
    return CliRunner()


def _row_group_count(path) -> int:
    return pq.ParquetFile(str(path)).metadata.num_row_groups


def _geometry_codec(path) -> str:
    """The codec the file's first row group actually uses."""
    return pq.ParquetFile(str(path)).metadata.row_group(0).column(0).compression


def _declared_version(path) -> str:
    metadata = pq.read_metadata(str(path)).metadata
    return json.loads(metadata[b"geo"].decode("utf-8"))["version"]


def _cli_json(runner, args) -> dict:
    result = runner.invoke(cli, args)
    assert result.exit_code == 0, f"`gpio {' '.join(args)}` failed:\n{result.output}"
    return json.loads(result.output)


class TestChecksReadTheSourceFile:
    """#1060: the verdict describes the user's bytes, and matches the CLI's."""

    def test_row_groups_counts_the_files_groups_not_the_re_writes(self, unsorted_test_file):
        on_disk = _row_group_count(unsorted_test_file)
        assert on_disk > 1, "fixture must have several row groups for this to mean anything"

        api = gpio.read(unsorted_test_file).check_row_groups().to_dict()
        assert api["stats"]["num_groups"] == on_disk

    def test_row_groups_verdict_matches_the_cli(self, runner, unsorted_test_file):
        cli_result = runner.invoke(cli, ["check", "row-group", unsorted_test_file])
        assert cli_result.exit_code == 0, cli_result.output
        assert f"Number of row groups: {_row_group_count(unsorted_test_file)}" in cli_result.output

        api = gpio.read(unsorted_test_file).check_row_groups()
        assert api.passed() is False

    def test_compression_reports_the_files_codec(self, unsorted_test_file):
        codec = _geometry_codec(unsorted_test_file)
        assert codec == "SNAPPY", "fixture must not already be gpio's default codec"

        api = gpio.read(unsorted_test_file).check_compression().to_dict()
        assert api["current_compression"] == codec

    def test_compression_can_fail_and_the_cli_agrees(self, runner, unsorted_test_file):
        cli_result = runner.invoke(cli, ["check", "compression", unsorted_test_file])
        assert cli_result.exit_code == 0, cli_result.output
        assert "SNAPPY" in cli_result.output

        api = gpio.read(unsorted_test_file).check_compression()
        assert api.passed() is False
        assert any("SNAPPY" in failure for failure in api.failures())

    def test_optimization_scores_the_file_the_cli_scored(self, runner, unsorted_test_file):
        from geoparquet_io.core.check_optimization import check_optimization

        expected = check_optimization(
            unsorted_test_file, verbose=False, return_results=True, quiet=True
        )
        cli_result = runner.invoke(cli, ["check", "optimization", unsorted_test_file])
        assert cli_result.exit_code == 0, cli_result.output
        assert f"Score: {expected['score']}/{expected['total_checks']}" in cli_result.output

        api = gpio.read(unsorted_test_file).check_optimization().to_dict()
        assert api["score"] == expected["score"]
        # Asserted per factor too: three of the five are about how the file is
        # written, and those are exactly the ones the temp re-write replaced.
        for factor in ("compression", "row_group_size", "spatial_sorting"):
            assert api["checks"][factor]["passed"] == expected["checks"][factor]["passed"], (
                f"{factor}: CLI {expected['checks'][factor]['detail']!r} vs "
                f"API {api['checks'][factor]['detail']!r}"
            )

    def test_validate_detects_the_version_the_file_declares(self, runner, places_test_file):
        declared = _declared_version(places_test_file)
        assert declared == "1.0.0", "fixture must declare a version gpio would not write"

        cli_spec = _cli_json(runner, ["check", "spec", places_test_file, "--json"])
        assert cli_spec["detected_version"] == declared

        api = gpio.read(places_test_file).validate().to_dict()
        assert api["detected_version"] == declared

    def test_validate_reports_the_source_path_not_a_temp_path(self, places_test_file):
        api = gpio.read(places_test_file).validate().to_dict()
        assert api["file_path"] == places_test_file

    def test_bbox_check_reads_the_file_the_cli_read(self, places_test_file):
        """The bbox verdict is about the file's own `geo` block and columns."""
        from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

        expected = check_metadata_and_bbox(
            places_test_file, verbose=False, return_results=True, quiet=True
        )
        api = gpio.read(places_test_file).check_bbox().to_dict()
        assert api["passed"] == expected["passed"]
        assert api["has_bbox_column"] == expected["has_bbox_column"]
        assert api.get("issues") == expected.get("issues")

    def test_bloom_filters_read_the_file(self, unsorted_test_file):
        from geoparquet_io.core.check_parquet_structure import check_bloom_filters

        expected = check_bloom_filters(
            unsorted_test_file, verbose=False, return_results=True, quiet=True
        )
        api = gpio.read(unsorted_test_file).check_bloom_filters().to_dict()
        assert api["has_bloom_filters"] == expected["has_bloom_filters"]
        assert api["columns_without_bloom_filters"] == expected["columns_without_bloom_filters"]

    def test_spatial_pushdown_reads_the_files_row_groups(self, unsorted_test_file):
        api = gpio.read(unsorted_test_file).check_spatial_pushdown().to_dict()
        assert api["num_row_groups"] == _row_group_count(unsorted_test_file)

    def test_check_all_reads_the_file(self, unsorted_test_file):
        api = gpio.read(unsorted_test_file).check().to_dict()
        assert api["compression"]["current_compression"] == _geometry_codec(unsorted_test_file)
        assert api["row_groups"]["stats"]["num_groups"] == _row_group_count(unsorted_test_file)

    def test_a_file_backed_result_names_the_file_it_measured(self, unsorted_test_file):
        result = gpio.read(unsorted_test_file).check_compression()
        assert result.prospective is False
        assert result.source == unsorted_test_file
        assert result.to_dict()["source"] == unsorted_test_file
        assert result.to_dict()["prospective"] is False


class TestDerivedTablesAreNotTheSourceFile:
    """A transformed table is different bytes, so it must not borrow the path."""

    def test_a_transformed_table_forgets_the_source(self, unsorted_test_file):
        derived = gpio.read(unsorted_test_file).head(10)
        assert derived.source_path is None

    def test_a_transformed_tables_check_is_prospective(self, unsorted_test_file):
        result = gpio.read(unsorted_test_file).head(10).check_row_groups()
        assert result.prospective is True
        assert result.source is None
        # Ten rows cannot be the source file's fifteen groups.
        assert result.to_dict()["stats"]["num_groups"] == 1

    def test_sorting_forgets_the_source(self, unsorted_test_file):
        """The clearest case: sort_hilbert exists to change the bytes."""
        assert gpio.read(unsorted_test_file).sort_hilbert().source_path is None


class TestInMemoryTablesAnswerProspectively:
    """No file behind the table means no layout to describe -- say so."""

    def _in_memory(self, places_test_file):
        return gpio.Table(pq.read_table(places_test_file))

    def test_source_path_is_none(self, places_test_file):
        assert self._in_memory(places_test_file).source_path is None

    def test_layout_checks_are_labelled_prospective(self, places_test_file):
        result = self._in_memory(places_test_file).check_compression()
        assert result.prospective is True
        assert result.source is None

    def test_the_label_reaches_the_dict_a_notebook_user_prints(self, places_test_file):
        details = self._in_memory(places_test_file).check_row_groups().to_dict()
        assert details["prospective"] is True
        assert details["source"] is None

    def test_the_label_reaches_the_repr(self, places_test_file):
        assert "prospective" in repr(self._in_memory(places_test_file).check_optimization())

    def test_a_file_backed_repr_is_not_labelled_prospective(self, places_test_file):
        assert "prospective" not in repr(gpio.read(places_test_file).check_optimization())

    def test_it_still_answers(self, places_test_file):
        """Prospective is not "refuse": the number describes bytes gpio wrote."""
        details = self._in_memory(places_test_file).check_compression().to_dict()
        assert details["current_compression"] == "ZSTD"


class TestGeoparquetVersionReportsWhatTheFileDeclares:
    """#1061: the read property answers about the file, not the write target."""

    def test_property_matches_the_declared_version(self, places_test_file):
        assert gpio.read(places_test_file).geoparquet_version == _declared_version(places_test_file)

    def test_property_matches_the_cli_summary(self, runner, places_test_file):
        cli_summary = _cli_json(runner, ["inspect", "summary", places_test_file, "--json"])
        assert gpio.read(places_test_file).geoparquet_version == cli_summary["geoparquet_version"]

    def test_metadata_and_info_report_the_declared_version_too(self, places_test_file):
        table = gpio.read(places_test_file)
        declared = _declared_version(places_test_file)
        assert table.metadata()["geoparquet_version"] == declared
        assert table.info(verbose=False)["geoparquet_version"] == declared

    def test_a_11_file_reports_its_own_patch_component(self, places_v11_file):
        assert gpio.read(places_v11_file).geoparquet_version == _declared_version(places_v11_file)

    def test_a_plain_parquet_table_reports_none(self):
        import pyarrow as pa

        assert gpio.Table(pa.table({"a": [1, 2]})).geoparquet_version is None

    def test_the_write_target_is_a_separate_property(self, places_test_file):
        """The "what would I write" question keeps its own name (#1061)."""
        table = gpio.read(places_test_file)
        assert table.geoparquet_version == "1.0.0"
        assert table.output_geoparquet_version == "1.1"

    def test_the_write_target_is_the_version_write_actually_stamps(
        self, places_test_file, tmp_path
    ):
        table = gpio.read(places_test_file)
        out = tmp_path / "out.parquet"
        table.write(str(out))
        written = _declared_version(out)
        assert written.startswith(table.output_geoparquet_version)


class TestCheckResultLabelling:
    def test_an_unlabelled_result_dict_is_untouched(self):
        from geoparquet_io.api.check import CheckResult

        raw = {"passed": True, "some_data": 123}
        assert CheckResult(raw, check_type="test").to_dict() == raw

    def test_an_unlabelled_result_defaults_to_not_prospective(self):
        from geoparquet_io.api.check import CheckResult

        result = CheckResult({"passed": True}, check_type="test")
        assert result.prospective is False
        assert result.source is None
