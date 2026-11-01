"""#1121: an overview of an aggregate with a bbox column keeps the column and its covering.

``process aggregate h3`` -> ``add bbox`` -> ``process overview --overview-out``
wrote a levelled file whose rollup had dropped the bbox struct, so its ``geo``
block declared no covering although every other write of the same data does.
The rollup now rebuilds the input's bbox column over the parent cells, and the
composer declares it on every level and on the levelled file.
"""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import shapely
from click.testing import CliRunner

from geoparquet_io.cli.main import cli


def _gpio(*args):
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output[-1500:]


def _covering(path):
    geo = json.loads(pq.read_schema(str(path)).metadata[b"geo"])
    return geo["columns"][geo["primary_column"]].get("covering")


def test_levelled_overview_keeps_the_bbox_column_and_declares_it(buildings_test_file, tmp_path):
    agg, boxed, out = tmp_path / "agg.parquet", tmp_path / "aggb.parquet", tmp_path / "lev.parquet"
    _gpio("process", "aggregate", "h3", buildings_test_file, agg, "--resolution", "9")
    _gpio("add", "bbox", agg, boxed)
    _gpio("process", "overview", boxed, "--levels", "7,8", "--overview-out", out)

    for path in (out, *sorted(tmp_path.glob("aggb_r*.parquet"))):
        table = pq.read_table(str(path))
        assert "bbox" in table.column_names, path.name
        assert table.schema.field("bbox").type == pq.read_schema(str(boxed)).field("bbox").type
        assert _covering(path) == {
            "bbox": {c: ["bbox", c] for c in ("xmin", "ymin", "xmax", "ymax")}
        }, path.name
        # Each row's box is its own geometry's envelope, not the base cell's.
        for geom, box in zip(
            table.column("geometry").to_pylist(), table.column("bbox").to_pylist(), strict=True
        ):
            xmin, ymin, xmax, ymax = shapely.from_wkb(geom).bounds
            assert (box["xmin"], box["ymin"], box["xmax"], box["ymax"]) == (xmin, ymin, xmax, ymax)


def test_an_aggregate_without_a_bbox_column_gains_none(buildings_test_file, tmp_path):
    agg, out = tmp_path / "agg.parquet", tmp_path / "lev.parquet"
    _gpio("process", "aggregate", "h3", buildings_test_file, agg, "--resolution", "9")
    _gpio("process", "overview", agg, "--levels", "8", "--overview-out", out)
    assert "bbox" not in pq.read_schema(str(out)).names
    assert _covering(out) is None
