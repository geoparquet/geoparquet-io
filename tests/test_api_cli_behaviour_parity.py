"""Run `gpio <cmd>` and its API twin over the same file; the answers must match.

The default-parity and call-parity modules compare declared options and the
values handed to core. This one compares *answers*: two front ends that agree
on every option can still read different bytes (#1060) or resolve a fact two
ways (#1061). A known divergence is pinned with `xfail(strict=True)` and an
issue number, with the CLI side asserted unconditionally.

Where a written file's CRS is asserted, the `geo` JSON and the Parquet logical
type are read separately (#997).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.api import ops
from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import sql_path
from tests.conftest import BUILDINGS_TEST_FILE
from tests.native_geo_probes import geo_block, geo_block_crs_id, logical_crs_id, spec_problems


def _cli(*args) -> str:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, f"`gpio {' '.join(map(str, args))}` failed:\n{result.output}"
    return result.output


def _cli_json(*args) -> dict:
    return json.loads(_cli(*args))


def _row_group_layout(path) -> list[int]:
    """Rows per row group, the layout two front ends must agree on (#971)."""
    parquet_file = pq.ParquetFile(str(path))
    return [parquet_file.metadata.row_group(i).num_rows for i in range(parquet_file.num_row_groups)]


def _geometry_codec(path) -> str:
    parquet_file = pq.ParquetFile(str(path))
    index = parquet_file.schema_arrow.get_field_index("geometry")
    return parquet_file.metadata.row_group(0).column(index).compression


# ==========================================================================
# inspect: the CLI goes through core.inspect, Table.head/stats/metadata are
# inline, so only a behavioural comparison can catch drift.
# ==========================================================================


class TestInspectGroupAgreesWithTable:
    @pytest.mark.parametrize(
        ("key", "from_table"),
        [
            ("rows", lambda t: t.info(verbose=False)["rows"]),
            ("bbox", lambda t: list(t.bounds)),
            ("geometry_types", lambda t: t.metadata()["geometry_types"]),
        ],
        ids=["rows", "bbox", "geometry_types"],
    )
    def test_summary_matches_the_table(self, places_test_file, key, from_table):
        summary = _cli_json("inspect", "summary", places_test_file, "--json")
        assert summary[key] == from_table(gpio.read(places_test_file))

    def test_summary_column_count_matches_info(self, places_test_file):
        summary = _cli_json("inspect", "summary", places_test_file, "--json")
        assert len(summary["columns"]) == gpio.read(places_test_file).info(verbose=False)["columns"]

    @pytest.mark.parametrize("end", ["head", "tail"])
    def test_preview_is_the_same_rows(self, places_test_file, end):
        cli_rows = _cli_json("inspect", end, places_test_file, "3", "--json")["preview"]
        api_rows = getattr(gpio.read(places_test_file), end)(3).to_arrow()

        assert len(cli_rows) == api_rows.num_rows == 3
        assert [r["fsq_place_id"] for r in cli_rows] == api_rows.column("fsq_place_id").to_pylist()

    def test_stats_cover_the_same_columns_with_the_same_null_counts(self, places_test_file):
        cli_stats = _cli_json("inspect", "stats", places_test_file, "--json")["statistics"]
        api_stats = gpio.read(places_test_file).stats()

        assert {c: s["nulls"] for c, s in cli_stats.items()} == {
            c: s["nulls"] for c, s in api_stats.items()
        }

    def test_meta_geometry_types_match_table_metadata(self, places_test_file):
        meta = _cli_json("inspect", "meta", places_test_file, "--json")["geoparquet_metadata"]
        primary = meta["primary_column"]
        assert (
            meta["columns"][primary]["geometry_types"]
            == gpio.read(places_test_file).metadata()["geometry_types"]
        )

    def test_summary_geoparquet_version_is_the_declared_version(self, places_test_file):
        summary = _cli_json("inspect", "summary", places_test_file, "--json")
        assert summary["geoparquet_version"] == geo_block(places_test_file)["version"]

    def test_table_geoparquet_version_is_the_declared_version(self, places_test_file):
        assert (
            gpio.read(places_test_file).geoparquet_version == geo_block(places_test_file)["version"]
        )


# ==========================================================================
# check: both front ends must judge the bytes they were handed.
# ==========================================================================


class TestCheckGroupAgreesWithTable:
    def test_check_bbox_agrees_on_a_file_with_a_covering(self, places_with_covering_file):
        output = _cli("check", "bbox", places_with_covering_file)
        assert "proper metadata covering" in output

        api = gpio.read(places_with_covering_file).check_bbox()
        assert api.passed() is True
        assert api.to_dict()["has_bbox_column"] is True

    def test_cli_check_bbox_sees_that_a_1_0_file_has_no_covering(self, places_test_file):
        output = _cli("check", "bbox", places_test_file)
        assert "needs GeoParquet 1.1+" in output
        assert "proper metadata covering" not in output

    def test_table_check_bbox_sees_that_a_1_0_file_has_no_covering(self, places_test_file):
        api = gpio.read(places_test_file).check_bbox().to_dict()
        assert api["has_bbox_column"] is True
        assert api["has_bbox_metadata"] is False

    def test_check_spatial_agrees_on_a_sorted_file(self, places_with_covering_file):
        assert "appears to be spatially ordered" in _cli(
            "check", "spatial", places_with_covering_file
        )
        assert gpio.read(places_with_covering_file).check_spatial().passed() is True

    def test_table_check_reports_the_structure_sub_checks(self, places_with_covering_file):
        """`Table.check()` is `core.check_all`: structure only, no spatial or spec pass."""
        assert set(gpio.read(places_with_covering_file).check().to_dict()) >= {
            "bbox",
            "compression",
            "row_groups",
        }


class TestCheckGroupReadsTheFileItWasGiven:
    """Both front ends judge the bytes they were handed (#1060, #1061)."""

    def test_cli_check_row_group_sees_the_files_layout(self, unsorted_test_file):
        layout = _row_group_layout(unsorted_test_file)
        assert len(layout) > 1, "fixture must have several row groups"

        assert f"Number of row groups: {len(layout)}" in _cli(
            "check", "row-group", unsorted_test_file
        )

    def test_table_check_row_groups_sees_the_files_layout(self, unsorted_test_file):
        api = gpio.read(unsorted_test_file).check_row_groups().to_dict()
        assert api["stats"]["num_groups"] == len(_row_group_layout(unsorted_test_file))

    def test_cli_check_compression_sees_the_files_codec(self, unsorted_test_file):
        assert _geometry_codec(unsorted_test_file) == "SNAPPY", "fixture must not be gpio's default"
        assert "SNAPPY compression" in _cli("check", "compression", unsorted_test_file)

    def test_table_check_compression_sees_the_files_codec(self, unsorted_test_file):
        api = gpio.read(unsorted_test_file).check_compression().to_dict()
        assert api["current_compression"] == _geometry_codec(unsorted_test_file)

    def test_cli_check_spatial_sees_the_files_order(self, unsorted_test_file):
        assert "Poor spatial ordering" in _cli("check", "spatial", unsorted_test_file)

    def test_table_check_spatial_sees_the_files_order(self, unsorted_test_file):
        assert gpio.read(unsorted_test_file).check_spatial().passed() is False

    def test_cli_check_optimization_scores_the_file(self, unsorted_test_file):
        """Not a literal: geo bbox statistics read differently on Windows (#770)."""
        from geoparquet_io.core.check_optimization import check_optimization

        expected = check_optimization(
            unsorted_test_file, verbose=False, return_results=True, quiet=True
        )
        output = _cli("check", "optimization", unsorted_test_file)
        assert f"Score: {expected['score']}/{expected['total_checks']}" in output

    def test_table_check_optimization_scores_the_file(self, unsorted_test_file):
        from geoparquet_io.core.check_optimization import check_optimization

        expected = check_optimization(
            unsorted_test_file, verbose=False, return_results=True, quiet=True
        )
        api = gpio.read(unsorted_test_file).check_optimization().to_dict()
        # Per factor: these three are about how the file is written, which a
        # re-write replaces; the aggregate could coincide by platform (#770).
        for factor in ("compression", "row_group_size", "spatial_sorting"):
            assert api["checks"][factor]["passed"] == expected["checks"][factor]["passed"], factor

    def test_cli_check_spec_detects_the_declared_version(self, places_test_file):
        spec = _cli_json("check", "spec", places_test_file, "--json")
        assert spec["detected_version"] == geo_block(places_test_file)["version"]

    def test_table_validate_detects_the_declared_version(self, places_test_file):
        api = gpio.read(places_test_file).validate().to_dict()
        assert api["detected_version"] == geo_block(places_test_file)["version"]


class TestApiOnlyChecks:
    """`check_spatial_pushdown` and `check_bloom_filters` have no CLI leaf; they still need an oracle."""

    def test_check_spatial_pushdown_fails_a_single_row_group_file(self, places_test_file):
        result = gpio.read(places_test_file).check_spatial_pushdown()
        details = result.to_dict()

        assert details["num_row_groups"] == 1, "fixture must be a single row group"
        assert details["has_geo_bbox"] is True
        assert 0.0 <= details["estimated_skip_rate"] <= 1.0
        assert result.passed() is False
        assert any("row group" in issue.lower() for issue in details["issues"])

    def test_check_bloom_filters_lists_every_leaf_column(self, places_test_file):
        result = gpio.read(places_test_file).check_bloom_filters().to_dict()

        assert result["has_bloom_filters"] is False
        detailed = {entry["column_name"] for entry in result["bloom_filter_details"]}
        assert detailed == set(result["columns_without_bloom_filters"])


# ==========================================================================
# add geometry-metrics: CLI vs ops vs Table
# ==========================================================================

GEOMETRY_METRIC_COLUMNS = ["metrics:area", "metrics:perimeter"]


@pytest.fixture(scope="module")
def geometry_metrics(tmp_path_factory):
    """One CLI run, one Table run, one ops run, one API write: shared by the tests below."""
    buildings_test_file = str(BUILDINGS_TEST_FILE)
    out_dir = tmp_path_factory.mktemp("metrics")
    cli_out = out_dir / "cli.parquet"
    _cli("add", "geometry-metrics", buildings_test_file, cli_out)
    source = gpio.read(buildings_test_file)
    table = source.add_geometry_metrics()
    api_out = table.write(out_dir / "api.parquet")
    return {
        "source": source,
        "cli": pq.read_table(cli_out),
        "table": table.to_arrow(),
        "ops": ops.add_geometry_metrics(source.to_arrow()),
        "api_out": Path(api_out),
    }


class TestAddGeometryMetricsParity:
    def test_ops_and_table_write_what_the_cli_writes(self, geometry_metrics):
        original = set(geometry_metrics["source"].column_names)
        added = [c for c in geometry_metrics["cli"].column_names if c not in original]
        assert added == GEOMETRY_METRIC_COLUMNS

        expected = geometry_metrics["cli"].select(GEOMETRY_METRIC_COLUMNS)
        assert geometry_metrics["table"].select(GEOMETRY_METRIC_COLUMNS).equals(expected)
        assert geometry_metrics["ops"].select(GEOMETRY_METRIC_COLUMNS).equals(expected)

    def test_the_api_output_is_a_valid_file_with_the_source_crs(
        self, geometry_metrics, buildings_test_file
    ):
        out = geometry_metrics["api_out"]
        assert pq.ParquetFile(out).metadata.num_rows == geometry_metrics["source"].num_rows
        assert spec_problems(out) == []
        assert geo_block_crs_id(out) == geo_block_crs_id(buildings_test_file)
        assert logical_crs_id(out) == logical_crs_id(buildings_test_file)


# ==========================================================================
# benchmark explain: CLI vs ops
# ==========================================================================


class TestExplainAnalyzeParity:
    def test_ops_returns_the_plan_the_cli_renders(self, places_test_file):
        cli_plan = _cli_json("benchmark", "explain", places_test_file, "--format", "json")
        api_plan = ops.explain_analyze(places_test_file)

        assert set(api_plan) == set(cli_plan)
        assert api_plan["has_filter_pushdown"] == cli_plan["has_filter_pushdown"]
        assert [op["name"] for op in api_plan["operators"]] == [
            op["name"] for op in cli_plan["operators"]
        ]

    def test_a_custom_query_changes_the_plan(self, places_test_file):
        default = ops.explain_analyze(places_test_file)
        query = f"SELECT COUNT(*) FROM read_parquet({sql_path(places_test_file)})"
        counted = ops.explain_analyze(places_test_file, query=query)

        assert counted["raw_plan"] and counted["raw_plan"] != default["raw_plan"]


# ==========================================================================
# publish stac: CLI vs gpio.generate_stac
# ==========================================================================

STAC_BUCKET = "s3://example-bucket/data/"


def _item(path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


class TestGenerateStacParity:
    def test_api_item_matches_the_one_the_cli_writes(self, places_test_file, tmp_path):
        cli_out = tmp_path / "cli-item.json"
        _cli("publish", "stac", places_test_file, cli_out, "--bucket", STAC_BUCKET)
        api_out = gpio.generate_stac(
            places_test_file, tmp_path / "api-item.json", bucket=STAC_BUCKET
        )

        volatile = {"datetime", "created", "updated"}  # wall-clock fields
        assert {k: v for k, v in _item(api_out).items() if k not in volatile} == {
            k: v for k, v in _item(cli_out).items() if k not in volatile
        }

    def test_default_output_path_and_item_id(self, places_test_file, tmp_path):
        local = tmp_path / "places.parquet"
        shutil.copy(places_test_file, local)

        out = gpio.generate_stac(local, bucket=STAC_BUCKET, item_id="my-dataset")

        assert Path(out) == local.with_suffix(".json")
        assert _item(out)["id"] == "my-dataset"

    def test_existing_outputs_are_refused_without_overwrite(self, places_test_file, tmp_path):
        out = tmp_path / "item.json"
        gpio.generate_stac(places_test_file, out, bucket=STAC_BUCKET)

        with pytest.raises(ValueError, match="already exists"):
            gpio.generate_stac(places_test_file, out, bucket=STAC_BUCKET)

        out.write_text(json.dumps({"type": "stale"}), encoding="utf-8")
        gpio.generate_stac(places_test_file, out, bucket=STAC_BUCKET, overwrite=True)
        assert _item(out)["type"] == "Feature"

    def test_a_directory_input_writes_a_collection_and_one_file_per_item(
        self, country_partition_dir, tmp_path
    ):
        out = tmp_path / "collection.json"
        gpio.generate_stac(country_partition_dir, out, bucket=STAC_BUCKET)

        assert _item(out)["type"] == "Collection"
        item_files = sorted(p.name for p in tmp_path.glob("*.json") if p.name != "collection.json")
        assert item_files, "expected one JSON file per partition alongside collection.json"
        for name in item_files:
            item = _item(tmp_path / name)
            assert item["type"] == "Feature"
            assert f"{item['id']}.json" == name

    def test_an_existing_item_file_is_refused_without_overwrite(
        self, country_partition_dir, tmp_path
    ):
        """The per-item guard is a different branch from the collection guard."""
        part = tmp_path / "part"
        part.mkdir()
        shutil.copy(next(Path(country_partition_dir).glob("*.parquet")), part)
        out = tmp_path / "collection.json"
        gpio.generate_stac(part, out, bucket=STAC_BUCKET)
        out.unlink()  # clear the collection guard, leave the item file behind

        with pytest.raises(ValueError, match="STAC item file already exists"):
            gpio.generate_stac(part, out, bucket=STAC_BUCKET)

    def test_generated_stac_validates_through_both_front_ends(self, places_test_file, tmp_path):
        out = gpio.generate_stac(places_test_file, tmp_path / "item.json", bucket=STAC_BUCKET)
        assert gpio.validate_stac(out).passed() is True
        _cli("check", "stac", out)
