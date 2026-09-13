"""Behavioural parity: the Python API and the CLI must answer the same question.

WP-6 of #1018. `tests/test_cli_api_default_parity.py` diffs *declared defaults*
and `tests/test_cli_api_call_parity_scaffold.py` diffs *values handed to core*.
Neither can see the third and largest class of drift: two front ends that agree
on every option name and every value and still return different answers, because
they read different bytes.

That is what this module tests. Each case runs `gpio <cmd>` and its
`ops.<fn>` / `Table.<method>` twin over the *same* input and compares the
answers -- row counts, the `geo` block, the row-group layout, the verdict a
check reports. Where the two disagree, the CLI's answer is pinned as the
contract and the API's is recorded with `xfail(strict=True)` plus an issue
number, so a fix fails the suite until the marker is removed.

It also gives a first call site to the API twins that the fast suite never
called at all -- `generate_stac`, `explain_analyze`, `add_geometry_metrics`,
`Table.check_spatial_pushdown`, `Table.check_bloom_filters`, `list_layers`.
Those are asserted on their return value or the file they wrote, never on a
mock.

CRS rule (project memory, #997): where a written file's CRS is asserted, the
`geo` JSON and the Parquet logical type are read **separately**. Merging them
through `source_crs_string` is how a disagreement between the two hides.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.api import ops
from geoparquet_io.cli.main import cli


@pytest.fixture
def runner():
    return CliRunner()


def _cli_json(runner, args) -> dict:
    """Run a `gpio` command that emits JSON and return the parsed document."""
    result = runner.invoke(cli, args)
    assert result.exit_code == 0, f"`gpio {' '.join(args)}` failed:\n{result.output}"
    return json.loads(result.output)


def _geo_block(path) -> dict:
    """The `geo` metadata a file actually declares, read straight off the footer."""
    metadata = pq.read_metadata(str(path)).metadata
    assert metadata is not None and b"geo" in metadata, f"{path} carries no geo metadata"
    return json.loads(metadata[b"geo"].decode("utf-8"))


def _row_group_layout(path) -> list[int]:
    """Rows per row group -- the layout two front ends must agree on (#971)."""
    parquet_file = pq.ParquetFile(str(path))
    return [parquet_file.metadata.row_group(i).num_rows for i in range(parquet_file.num_row_groups)]


# ==========================================================================
# inspect -- `gpio inspect <x>` vs `Table.<x>`
#
# These two front ends share no core function at all: the CLI goes through
# `core.inspect`, while `Table.head`/`stats`/`metadata` are implemented inline
# over the in-memory pyarrow table. Nothing but a behavioural comparison can
# tell whether they still describe the same file.
# ==========================================================================


class TestInspectGroupAgreesWithTable:
    def test_summary_row_count_and_column_count_match_table_info(self, runner, places_test_file):
        cli_summary = _cli_json(runner, ["inspect", "summary", places_test_file, "--json"])
        info = gpio.read(places_test_file).info(verbose=False)

        assert cli_summary["rows"] == info["rows"]
        assert len(cli_summary["columns"]) == info["columns"]

    def test_summary_bbox_matches_table_bounds(self, runner, places_test_file):
        cli_summary = _cli_json(runner, ["inspect", "summary", places_test_file, "--json"])
        bounds = gpio.read(places_test_file).bounds

        assert tuple(cli_summary["bbox"]) == tuple(bounds)

    def test_head_preview_is_the_same_rows_as_table_head(self, runner, places_test_file):
        cli_preview = _cli_json(runner, ["inspect", "head", places_test_file, "3", "--json"])[
            "preview"
        ]
        api_head = gpio.read(places_test_file).head(3).to_arrow()

        assert len(cli_preview) == api_head.num_rows == 3
        assert [row["fsq_place_id"] for row in cli_preview] == api_head.column(
            "fsq_place_id"
        ).to_pylist()

    def test_tail_preview_is_the_same_rows_as_table_tail(self, runner, places_test_file):
        cli_preview = _cli_json(runner, ["inspect", "tail", places_test_file, "3", "--json"])[
            "preview"
        ]
        api_tail = gpio.read(places_test_file).tail(3).to_arrow()

        assert len(cli_preview) == api_tail.num_rows == 3
        assert [row["fsq_place_id"] for row in cli_preview] == api_tail.column(
            "fsq_place_id"
        ).to_pylist()

    def test_stats_cover_the_same_columns_as_table_stats(self, runner, places_test_file):
        cli_stats = _cli_json(runner, ["inspect", "stats", places_test_file, "--json"])
        api_stats = gpio.read(places_test_file).stats()

        assert sorted(cli_stats["statistics"]) == sorted(api_stats)

    def test_stats_null_counts_match_table_stats(self, runner, places_test_file):
        """`nulls` is the one statistic both front ends compute for every column."""
        cli_stats = _cli_json(runner, ["inspect", "stats", places_test_file, "--json"])[
            "statistics"
        ]
        api_stats = gpio.read(places_test_file).stats()

        for column, api_column_stats in api_stats.items():
            cli_column_stats = cli_stats[column]
            if "nulls" not in cli_column_stats:  # pragma: no cover - defensive
                continue
            assert cli_column_stats["nulls"] == api_column_stats["nulls"], (
                f"null count for {column!r} differs between `gpio inspect stats` and Table.stats()"
            )

    def test_meta_geometry_types_match_table_metadata(self, runner, places_test_file):
        cli_summary = _cli_json(runner, ["inspect", "summary", places_test_file, "--json"])
        api_metadata = gpio.read(places_test_file).metadata()

        assert cli_summary["geometry_types"] == api_metadata["geometry_types"]

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "gpio #1061: `Table.geoparquet_version` reports the version gpio would "
            "*write*, not the one the file declares. It resolves through "
            "`core.streaming.extract_version_from_metadata`, whose documented job is to "
            "pick a `--geoparquet-version` target and which deliberately upgrades every "
            "1.x to '1.1'. `gpio inspect summary` reports the declared '1.0.0'. A "
            "library user asking a Table what version it is gets the writer's intention "
            "instead of the file's fact."
        ),
    )
    def test_summary_geoparquet_version_matches_table_property(self, runner, places_test_file):
        cli_summary = _cli_json(runner, ["inspect", "summary", places_test_file, "--json"])
        declared = _geo_block(places_test_file)["version"]
        assert cli_summary["geoparquet_version"] == declared

        assert gpio.read(places_test_file).geoparquet_version == declared


# ==========================================================================
# inspect layers -- `gpio inspect layers` vs `gpio.list_layers`
#
# `test_cli_api_default_parity.NO_API_TWIN` records this command as having no
# API twin. It does: `geoparquet_io.list_layers`, re-exported from
# `core.layers`. The name simply is not reachable by that module's candidate
# derivation, which only looks at `ops` and `Table` (gpio #1065).
# ==========================================================================


class TestListLayersIsTheInspectLayersTwin:
    def test_list_layers_finds_the_same_layers_the_cli_prints(self, runner, test_data_dir):
        source = str(test_data_dir / "multilayer_test.gpkg")
        layers = gpio.list_layers(source)
        assert layers == ["buildings", "roads"]

        result = runner.invoke(cli, ["inspect", "layers", source])
        assert result.exit_code == 0, result.output
        assert f"Found {len(layers)} layers" in result.output
        for name in layers:
            assert name in result.output

    def test_list_layers_returns_none_for_a_single_layer_source(self, gpkg_buildings):
        """The documented degenerate case: fewer than two layers means None."""
        assert gpio.list_layers(gpkg_buildings) is None

    def test_list_layers_raises_file_not_found_for_a_missing_source(self, tmp_path):
        """`core/layers.py` promises this in its docstring; nothing asserted it."""
        with pytest.raises(FileNotFoundError):
            gpio.list_layers(str(tmp_path / "does-not-exist.geojson"))


# ==========================================================================
# check -- `gpio check <x>` vs `Table.check_<x>`
#
# Every `Table.check_*` method routes through `Table._with_temp_file`, which
# writes the in-memory table out with gpio's *default* write settings and then
# checks that temp file. So the answer describes a file gpio just wrote, not
# the file the user read. On a clean canonical input the two front ends agree
# by luck; on anything the writer would change, they do not.
# ==========================================================================


class TestCheckGroupAgreesWithTableOnACleanFile:
    """The agreements that must not regress -- these are contract, not luck."""

    def test_check_bbox_agrees_on_a_file_that_already_has_a_covering(
        self, runner, places_with_covering_file
    ):
        result = runner.invoke(cli, ["check", "bbox", places_with_covering_file])
        assert result.exit_code == 0, result.output

        api = gpio.read(places_with_covering_file).check_bbox()
        assert api.passed() is True
        assert api.to_dict()["has_bbox_column"] is True

    def test_check_spatial_agrees_on_a_spatially_sorted_file(
        self, runner, places_with_covering_file
    ):
        result = runner.invoke(cli, ["check", "spatial", places_with_covering_file])
        assert result.exit_code == 0, result.output
        cli_says_ordered = "appears to be spatially ordered" in result.output

        api = gpio.read(places_with_covering_file).check_spatial()
        assert api.passed() is cli_says_ordered

    def test_check_all_reports_every_sub_check_the_cli_runs(self, places_with_covering_file):
        api = gpio.read(places_with_covering_file).check().to_dict()
        assert set(api) >= {"bbox", "compression", "row_groups"}


class TestCheckGroupDivergesFromTable:
    """Every case here is the same root cause: `Table._with_temp_file` (#1060).

    Pinned CLI-side first so the contract is asserted unconditionally; the API
    assertion is the xfail. A fix to `Table._with_temp_file` makes these fail,
    which is the point.
    """

    def test_cli_check_row_group_sees_the_files_actual_layout(self, runner, unsorted_test_file):
        layout = _row_group_layout(unsorted_test_file)
        assert len(layout) > 1, "fixture must have several row groups for this to mean anything"

        result = runner.invoke(cli, ["check", "row-group", unsorted_test_file])
        assert f"Number of row groups: {len(layout)}" in result.output

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "gpio #1060: `Table.check_row_groups()` writes the table to a temp file with "
            "gpio's default settings and counts *that* file's row groups, so a 15-group "
            "file reports 1. `gpio check row-group` reports 15 and fails the check; the "
            "API reports 1 and passes it."
        ),
    )
    def test_table_check_row_groups_sees_the_files_actual_layout(self, unsorted_test_file):
        layout = _row_group_layout(unsorted_test_file)
        api = gpio.read(unsorted_test_file).check_row_groups().to_dict()
        assert api["stats"]["num_groups"] == len(layout)

    def test_cli_check_compression_sees_the_files_actual_codec(self, runner, unsorted_test_file):
        codec = pq.ParquetFile(unsorted_test_file).metadata.row_group(0).column(0).compression
        assert codec == "SNAPPY", "fixture must not already be gpio's default codec"

        result = runner.invoke(cli, ["check", "compression", unsorted_test_file])
        assert "SNAPPY" in result.output

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "gpio #1060: `Table.check_compression()` reports the codec of the temp file "
            "it just wrote, which is always gpio's default ZSTD. The check can therefore "
            "never fail, whatever the user's file is compressed with."
        ),
    )
    def test_table_check_compression_sees_the_files_actual_codec(self, unsorted_test_file):
        codec = pq.ParquetFile(unsorted_test_file).metadata.row_group(0).column(0).compression
        api = gpio.read(unsorted_test_file).check_compression().to_dict()
        assert api["current_compression"] == codec

    def test_cli_check_optimization_scores_the_file_it_was_given(self, runner, unsorted_test_file):
        """The CLI's score is the one `check_optimization` computes for *this file*.

        Not pinned to a literal: the five factors include geo bbox statistics,
        which the readers disagree about on Windows (#770), so the number is
        platform-dependent. What must hold everywhere is that the CLI reports
        whatever core says about the file it was handed.
        """
        from geoparquet_io.core.check_optimization import check_optimization

        expected = check_optimization(
            unsorted_test_file, verbose=False, return_results=True, quiet=True
        )

        result = runner.invoke(cli, ["check", "optimization", unsorted_test_file])
        assert result.exit_code == 0, result.output
        assert f"Score: {expected['score']}/{expected['total_checks']}" in result.output

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "gpio #1060: `Table.check_optimization()` scores the temp re-write, so an "
            "unoptimized file scores 3/5 through the API and 0/5 through the CLI. This "
            "is the most user-visible face of the temp-file problem: the API tells a "
            "library user their data is partially optimized when it is not optimized at "
            "all."
        ),
    )
    def test_table_check_optimization_scores_the_file_it_was_given(self, unsorted_test_file):
        from geoparquet_io.core.check_optimization import check_optimization

        expected = check_optimization(
            unsorted_test_file, verbose=False, return_results=True, quiet=True
        )
        api = gpio.read(unsorted_test_file).check_optimization().to_dict()

        # Asserted per factor rather than on the aggregate score: three of the
        # five are about how the file is *written*, and those are exactly the
        # ones the temp re-write replaces with gpio's defaults. The aggregate
        # could coincide on a platform that reads geo statistics differently
        # (#770); these three cannot.
        for factor in ("compression", "row_group_size", "spatial_sorting"):
            assert api["checks"][factor]["passed"] == expected["checks"][factor]["passed"], (
                f"{factor}: CLI {expected['checks'][factor]['detail']!r} vs "
                f"API {api['checks'][factor]['detail']!r}"
            )

    def test_cli_check_spec_detects_the_version_the_file_declares(self, runner, places_test_file):
        declared = _geo_block(places_test_file)["version"]
        cli_spec = _cli_json(runner, ["check", "spec", places_test_file, "--json"])
        assert cli_spec["detected_version"] == declared

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "gpio #1060 (with #1061): `Table.validate()` validates a temp re-write, and "
            "that re-write is written at `Table.geoparquet_version` -- which #1061 has "
            "already upgraded from the declared 1.0.0 to 1.1. So the API reports "
            "detected_version '1.1.0' for a file that declares '1.0.0', and validates "
            "checks the user's file was never subject to."
        ),
    )
    def test_table_validate_detects_the_version_the_file_declares(self, places_test_file):
        declared = _geo_block(places_test_file)["version"]
        api = gpio.read(places_test_file).validate().to_dict()
        assert api["detected_version"] == declared


class TestApiOnlyChecks:
    """`Table.check_spatial_pushdown` and `check_bloom_filters` have no CLI leaf.

    Both ran in no lane at all before this module. They are exempt from the
    parity rule in the other direction -- the house rule is "every CLI command
    needs a Python API", not the converse -- but they still need an oracle.
    """

    def test_check_spatial_pushdown_reports_a_skip_rate(self, places_test_file):
        result = gpio.read(places_test_file).check_spatial_pushdown().to_dict()
        assert result["has_geo_bbox"] is True
        assert result["num_row_groups"] >= 1
        assert 0.0 <= result["estimated_skip_rate"] <= 1.0

    def test_check_spatial_pushdown_fails_a_single_row_group_table(self, places_test_file):
        """A single row group can skip nothing, so the check must not pass."""
        result = gpio.read(places_test_file).check_spatial_pushdown()
        details = result.to_dict()
        if details["num_row_groups"] == 1:
            assert result.passed() is False
            assert any("row group" in issue.lower() for issue in details["issues"])

    def test_check_bloom_filters_lists_every_leaf_column(self, places_test_file):
        table = gpio.read(places_test_file)
        result = table.check_bloom_filters().to_dict()

        assert result["has_bloom_filters"] is False
        assert result["columns_with_bloom_filters"] == []
        assert result["total_bloom_filter_bytes"] == 0
        # Every reported column carries a per-column detail entry.
        detailed = {entry["column_name"] for entry in result["bloom_filter_details"]}
        assert detailed == set(result["columns_without_bloom_filters"])


# ==========================================================================
# add geometry-metrics -- `gpio add geometry-metrics` vs ops / Table
#
# Both API twins ran in no lane before this module.
# ==========================================================================

GEOMETRY_METRIC_COLUMNS = ["metrics:area", "metrics:perimeter"]


class TestAddGeometryMetricsParity:
    def test_ops_and_table_add_the_same_columns_the_cli_does(
        self, runner, buildings_test_file, tmp_path
    ):
        source = gpio.read(buildings_test_file)
        original_columns = list(source.column_names)

        cli_out = tmp_path / "cli.parquet"
        result = runner.invoke(cli, ["add", "geometry-metrics", buildings_test_file, str(cli_out)])
        assert result.exit_code == 0, result.output
        cli_added = [c for c in pq.read_schema(cli_out).names if c not in original_columns]

        table_added = [
            c for c in source.add_geometry_metrics().column_names if c not in original_columns
        ]
        ops_added = [
            c
            for c in ops.add_geometry_metrics(source.to_arrow()).column_names
            if c not in original_columns
        ]

        assert cli_added == GEOMETRY_METRIC_COLUMNS
        assert table_added == cli_added
        assert ops_added == cli_added

    def test_ops_and_table_compute_the_same_values_the_cli_writes(
        self, runner, buildings_test_file, tmp_path
    ):
        cli_out = tmp_path / "cli.parquet"
        assert (
            runner.invoke(
                cli, ["add", "geometry-metrics", buildings_test_file, str(cli_out)]
            ).exit_code
            == 0
        )
        cli_table = pq.read_table(cli_out)

        source = gpio.read(buildings_test_file)
        api_table = source.add_geometry_metrics().to_arrow()
        ops_table = ops.add_geometry_metrics(source.to_arrow())

        for column in GEOMETRY_METRIC_COLUMNS:
            expected = cli_table.column(column).to_pylist()
            assert api_table.column(column).to_pylist() == expected
            assert ops_table.column(column).to_pylist() == expected

    def test_row_count_and_crs_survive_the_api_round_trip(self, buildings_test_file, tmp_path):
        source = gpio.read(buildings_test_file)
        out = tmp_path / "api.parquet"
        source.add_geometry_metrics().write(str(out))

        written = pq.ParquetFile(out)
        assert written.metadata.num_rows == source.num_rows

        # Both CRS sources, read separately (#997): the geo JSON, and the
        # Parquet logical type. Merging them is how a disagreement hides.
        written_geo = _geo_block(out)
        source_geo = _geo_block(buildings_test_file)
        primary = written_geo["primary_column"]
        assert written_geo["columns"][primary].get("crs") == source_geo["columns"][
            source_geo["primary_column"]
        ].get("crs")
        assert (
            pq.read_schema(out).field(primary).type
            == pq.read_schema(buildings_test_file).field(source_geo["primary_column"]).type
        )

    def test_the_api_output_is_a_file_gpio_accepts(self, runner, buildings_test_file, tmp_path):
        out = tmp_path / "api.parquet"
        gpio.read(buildings_test_file).add_geometry_metrics().write(str(out))

        spec = _cli_json(runner, ["check", "spec", str(out), "--json"])
        assert spec["is_valid"] is True
        assert spec["summary"]["failed"] == 0, spec["checks"]


# ==========================================================================
# benchmark explain -- `gpio benchmark explain` vs ops / Table
#
# `benchmark` is one of the seven groups with no call-parity case, and both
# twins ran in no lane.
# ==========================================================================


class TestExplainAnalyzeParity:
    def test_ops_returns_the_same_plan_shape_the_cli_renders(self, runner, places_test_file):
        cli_plan = _cli_json(runner, ["benchmark", "explain", places_test_file, "--format", "json"])
        api_plan = ops.explain_analyze(places_test_file)

        assert set(api_plan) == set(cli_plan)
        assert isinstance(api_plan["operators"], list)
        assert isinstance(api_plan["has_filter_pushdown"], bool)
        assert api_plan["raw_plan"]

    def test_table_explain_analyze_is_the_same_entry_point_as_ops(self, places_test_file):
        from geoparquet_io.api.table import Table

        assert set(Table.explain_analyze(places_test_file)) == set(
            ops.explain_analyze(places_test_file)
        )

    def test_a_custom_query_reaches_duckdb(self, places_test_file):
        query = f"SELECT COUNT(*) FROM read_parquet('{places_test_file}')"
        plan = ops.explain_analyze(places_test_file, query=query)
        assert plan["raw_plan"]
        assert plan["total_time"] is not None


# ==========================================================================
# publish stac -- `gpio publish stac` vs `gpio.generate_stac`
#
# `generate_stac` was 22 % covered: `tests/test_api.py` only asserted it was
# not None. `NO_API_TWIN` records `publish stac` as twin-less; the twin is
# `api/stac.py`, which neither `ops` nor `Table` re-exports (gpio #1065).
# ==========================================================================

STAC_BUCKET = "s3://example-bucket/data/"


class TestGenerateStacParity:
    def test_api_item_matches_the_one_the_cli_writes(self, runner, places_test_file, tmp_path):
        cli_out = tmp_path / "cli-item.json"
        result = runner.invoke(
            cli,
            ["publish", "stac", places_test_file, str(cli_out), "--bucket", STAC_BUCKET],
        )
        assert result.exit_code == 0, result.output

        api_out = gpio.generate_stac(
            places_test_file, tmp_path / "api-item.json", bucket=STAC_BUCKET
        )

        cli_item = json.loads(cli_out.read_text(encoding="utf-8"))
        api_item = json.loads(Path(api_out).read_text(encoding="utf-8"))

        # `datetime`/`created` fields are wall-clock, so compare everything else.
        volatile = {"datetime", "created", "updated"}
        assert {k: v for k, v in api_item.items() if k not in volatile} == {
            k: v for k, v in cli_item.items() if k not in volatile
        }

    def test_default_output_path_is_the_input_with_a_json_suffix(self, places_test_file, tmp_path):
        """`output_path=None` for a *file* input -- `api/stac.py:63-67`."""
        local = tmp_path / "places.parquet"
        local.write_bytes(Path(places_test_file).read_bytes())

        out = gpio.generate_stac(local, bucket=STAC_BUCKET)

        assert Path(out) == local.with_suffix(".json")
        assert json.loads(Path(out).read_text(encoding="utf-8"))["type"] == "Feature"

    def test_custom_item_id_reaches_the_document(self, places_test_file, tmp_path):
        out = gpio.generate_stac(
            places_test_file, tmp_path / "item.json", bucket=STAC_BUCKET, item_id="my-dataset"
        )
        assert json.loads(Path(out).read_text(encoding="utf-8"))["id"] == "my-dataset"

    def test_existing_collection_output_is_refused_without_overwrite(
        self, places_test_file, tmp_path
    ):
        """The collection-level guard, `api/stac.py:72-75`."""
        out = tmp_path / "item.json"
        gpio.generate_stac(places_test_file, out, bucket=STAC_BUCKET)

        with pytest.raises(ValueError, match="already exists"):
            gpio.generate_stac(places_test_file, out, bucket=STAC_BUCKET)

        # ...and is allowed through with overwrite=True, which must actually
        # rewrite the file rather than just declining to raise.
        out.write_text(json.dumps({"type": "stale"}), encoding="utf-8")
        rewritten = gpio.generate_stac(places_test_file, out, bucket=STAC_BUCKET, overwrite=True)
        assert json.loads(Path(rewritten).read_text(encoding="utf-8"))["type"] == "Feature"

    def test_a_directory_input_writes_a_collection_and_one_file_per_item(
        self, country_partition_dir, tmp_path
    ):
        """The directory branch, plus the per-item write loop."""
        out = tmp_path / "collection.json"
        gpio.generate_stac(country_partition_dir, out, bucket=STAC_BUCKET)

        collection = json.loads(out.read_text(encoding="utf-8"))
        assert collection["type"] == "Collection"

        item_files = sorted(p.name for p in tmp_path.glob("*.json") if p.name != "collection.json")
        assert item_files, "expected one JSON file per partition alongside collection.json"
        for name in item_files:
            item = json.loads((tmp_path / name).read_text(encoding="utf-8"))
            assert item["type"] == "Feature"
            assert f"{item['id']}.json" == name

    def test_an_existing_item_file_is_refused_without_overwrite(
        self, country_partition_dir, tmp_path
    ):
        """The *per-item* guard at `api/stac.py:114-117`.

        A distinct code path from the collection guard: the collection file is
        rewritten happily and the loop raises partway through.
        """
        out = tmp_path / "collection.json"
        gpio.generate_stac(country_partition_dir, out, bucket=STAC_BUCKET)
        out.unlink()  # clear the collection guard, leave the item files behind

        with pytest.raises(ValueError, match="STAC item file already exists"):
            gpio.generate_stac(country_partition_dir, out, bucket=STAC_BUCKET)

    def test_generated_stac_validates(self, runner, places_test_file, tmp_path):
        """The two halves of `api/stac.py` must agree with each other."""
        out = gpio.generate_stac(places_test_file, tmp_path / "item.json", bucket=STAC_BUCKET)
        assert gpio.validate_stac(out).passed() is True

        result = runner.invoke(cli, ["check", "stac", str(out)])
        assert result.exit_code == 0, result.output
