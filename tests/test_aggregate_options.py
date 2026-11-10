"""``AggregateOptions``: the ten aggregation options as one object (#1367, HANDOFFS row 59).

The CLI packs them once (``grid_aggregate_options``), ``ops`` and ``Table`` build
the same object from their keywords, and the grid engine reads it. These tests
pin the object itself and each of those three ways of getting one.
"""

from __future__ import annotations

import dataclasses
from unittest import mock

import pyarrow as pa
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.api import ops
from geoparquet_io.cli.commands import process as cli_process
from geoparquet_io.cli.main import cli
from geoparquet_io.core.process.aggregate import by_a5, by_h3
from geoparquet_io.core.process.aggregate.options import (
    BUCKET_POINT_GEOMETRY,
    AggregateOptions,
)

#: One non-default value for every field, so a dropped option shows.
NON_DEFAULTS = {
    "metric": "sum:height",
    "metric_nodata": "-999",
    "breakdown": "crop",
    "breakdown_limit": 7,
    "breakdown_metric": "sum:height",
    "out_geometry": "centroid",
    "where": "height > 1",
    "bucket_point": "bbox",
    "bbox_column": "bbox",
    "memory_limit": "512MB",
}


def test_defaults_are_the_documented_ones():
    assert dataclasses.asdict(AggregateOptions()) == {
        "metric": None,
        "metric_nodata": None,
        "breakdown": None,
        "breakdown_limit": 20,
        "breakdown_metric": None,
        "out_geometry": "polygon",
        "where": None,
        "bucket_point": BUCKET_POINT_GEOMETRY,
        "bbox_column": None,
        "memory_limit": None,
    }


def test_the_object_is_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        AggregateOptions().metric = "count"  # type: ignore[misc]


def test_every_field_has_a_non_default_in_this_table():
    """A new option must be added to NON_DEFAULTS, so the packing tests cover it."""
    assert set(NON_DEFAULTS) == {f.name for f in dataclasses.fields(AggregateOptions)}
    defaults = dataclasses.asdict(AggregateOptions())
    assert all(NON_DEFAULTS[name] != defaults[name] for name in NON_DEFAULTS)


def test_pick_takes_the_named_fields_and_ignores_the_rest():
    values = {**NON_DEFAULTS, "self": object(), "resolution": 5, "unrelated": 1}
    assert AggregateOptions.pick(values) == AggregateOptions(**NON_DEFAULTS)
    assert AggregateOptions.pick({"metric": "sum:x"}) == AggregateOptions(metric="sum:x")


@pytest.mark.parametrize("scheme", ["h3", "a5"])
def test_the_cli_packs_its_options_into_one_object(scheme):
    impl = f"aggregate_by_{scheme}_impl"
    args = ["process", "aggregate", scheme, "in.parquet", "out.parquet", "--resolution", "5"]
    args += ["--metric", "sum:height", "--metric-nodata", "-999", "--breakdown", "crop"]
    args += ["--breakdown-limit", "7", "--breakdown-metric", "sum:height"]
    args += ["--out-geometry", "centroid", "--where", "height > 1"]
    args += ["--bucket-point", "bbox", "--bbox-column", "bbox", "--write-memory", "512MB"]
    with mock.patch.object(cli_process, impl) as mocked:
        result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert mocked.call_args.kwargs["options"] == AggregateOptions(**NON_DEFAULTS)


def test_the_cli_defaults_pack_the_default_object():
    args = ["process", "aggregate", "h3", "in.parquet", "out.parquet", "--resolution", "5"]
    with mock.patch.object(cli_process, "aggregate_by_h3_impl") as mocked:
        result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert mocked.call_args.kwargs["options"] == AggregateOptions()


@pytest.mark.parametrize(("scheme", "module"), [("h3", by_h3), ("a5", by_a5)])
def test_ops_and_table_build_the_same_object_from_the_same_keywords(scheme, module):
    table = pa.table({"geometry": pa.array([b""], pa.binary())})
    seen = {}

    def record(side):
        def fake(tbl, **kwargs):
            seen[side] = kwargs["options"]
            return tbl

        return fake

    with mock.patch.object(module, f"aggregate_{scheme}_table", record("ops")):
        getattr(ops, f"aggregate_{scheme}")(table, 5, **NON_DEFAULTS)
    with mock.patch.object(module, f"aggregate_{scheme}_table", record("table")):
        getattr(gpio.Table(table), f"aggregate_{scheme}")(5, **NON_DEFAULTS)

    assert seen["ops"] == seen["table"] == AggregateOptions(**NON_DEFAULTS)
