"""A written ``covering`` always carries a ``bbox`` member, or is not written.

Regression tests for #954: `gpio partition quadkey --keep-quadkey-column` wrote
a primary geometry column whose ``covering`` was ``{"quadkey": {...}}`` with no
``"bbox"`` key. The GeoParquet 1.1.0 spec's ``covering`` section says "The keys
of the 'covering' object MUST be a supported encoding. Currently the only
supported encoding is 'bbox'" — and real readers rely on that: geopandas
indexes ``covering["bbox"]["xmin"][0]`` unguarded, so every such file died with
``KeyError: 'bbox'`` in ``geopandas.read_parquet``.

gpio still records its spatial-index entries (h3/s2/a5/quadkey/kdtree) *beside*
a bbox member (#694/#738), but a covering with no bbox member to advertise is
now omitted entirely. The enforcement lives with the owner of the ``geo`` block
(:func:`geoparquet_io.core.geo_metadata.strip_bboxless_covering`), applied at
every write funnel, so h3/s2/a5/kdtree partitions and ``gpio add`` are fixed by
the same gate.
"""

import json

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.add.quadkey import add_quadkey_column
from geoparquet_io.core.geo_metadata import strip_bboxless_covering


def _geo(path):
    kv = pq.ParquetFile(str(path)).metadata.metadata or {}
    assert b"geo" in kv, f"{path} was written without a 'geo' key; keys: {sorted(kv)}"
    return json.loads(kv[b"geo"].decode("utf-8"))


def _primary_column_meta(path):
    geo = _geo(path)
    return geo["columns"][geo["primary_column"]]


class TestStripBboxlessCovering:
    """Unit contract of the owner-level gate."""

    def test_drops_a_covering_with_no_bbox_member(self):
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "covering": {"quadkey": {"column": "quadkey", "resolution": 6}},
                }
            },
        }
        result = strip_bboxless_covering(geo)
        assert "covering" not in result["columns"]["geometry"]

    def test_keeps_a_covering_that_has_a_bbox_member(self):
        covering = {
            "bbox": {
                "xmin": ["bbox", "xmin"],
                "ymin": ["bbox", "ymin"],
                "xmax": ["bbox", "xmax"],
                "ymax": ["bbox", "ymax"],
            },
            "quadkey": {"column": "quadkey", "resolution": 6},
        }
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "covering": covering}},
        }
        result = strip_bboxless_covering(geo)
        assert result["columns"]["geometry"]["covering"] == covering

    def test_never_mutates_its_input(self):
        """Partition loops reuse one metadata dict across many writes."""
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "covering": {"h3": {"column": "h3", "resolution": 9}},
                }
            },
        }
        strip_bboxless_covering(geo)
        assert "covering" in geo["columns"]["geometry"]

    def test_applies_per_column(self):
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "covering": {"bbox": {"xmin": ["bbox", "xmin"]}},
                },
                "other_geom": {
                    "encoding": "WKB",
                    "covering": {"s2": {"column": "s2_cell", "level": 10}},
                },
            },
        }
        result = strip_bboxless_covering(geo)
        assert "covering" in result["columns"]["geometry"]
        assert "covering" not in result["columns"]["other_geom"]

    def test_leaves_malformed_column_entries_alone(self):
        """Shape policing beyond the bbox member is validate's job, not this gate's."""
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": "not-a-dict"},
        }
        assert strip_bboxless_covering(geo) == geo
        assert strip_bboxless_covering({"columns": []}) == {"columns": []}


class TestAddWithoutBboxColumn:
    def test_add_quadkey_writes_no_bboxless_covering(self, buildings_test_file, tmp_path):
        """No bbox column in the output → no covering block at all (#954)."""
        output = tmp_path / "quadkey.parquet"
        add_quadkey_column(buildings_test_file, str(output), resolution=6)

        col_meta = _primary_column_meta(output)
        assert "covering" not in col_meta, (
            f"covering with no bbox member reached the file: {col_meta['covering']}"
        )

    def test_add_quadkey_output_opens_in_geopandas(self, buildings_test_file, tmp_path):
        import geopandas as gpd

        output = tmp_path / "quadkey.parquet"
        add_quadkey_column(buildings_test_file, str(output), resolution=6)

        gdf = gpd.read_parquet(str(output))
        assert len(gdf) > 0


class TestPartitionQuadkeyInterop:
    """The issue's own reproduction: geopandas must open partition quadkey output."""

    @pytest.fixture
    def partitioned_dir(self, buildings_test_file, tmp_path):
        out = tmp_path / "parts"
        result = CliRunner().invoke(
            cli,
            [
                "partition",
                "quadkey",
                buildings_test_file,
                str(out),
                "--resolution",
                "6",
                "--partition-resolution",
                "2",
                "--skip-analysis",
                # Keeping the quadkey column is what kept the covering entry
                # alive through column pruning — the #954 shape.
                "--keep-quadkey-column",
            ],
        )
        assert result.exit_code == 0, result.output
        files = sorted(out.rglob("*.parquet"))
        assert files, "partition quadkey wrote no files"
        return out, files

    def test_partition_files_never_carry_a_bboxless_covering(self, partitioned_dir):
        _, files = partitioned_dir
        for f in files:
            col_meta = _primary_column_meta(f)
            covering = col_meta.get("covering")
            if covering is not None:
                assert "bbox" in covering, f"{f} covering has no bbox member: {covering}"

    def test_geopandas_reads_a_partition_file(self, partitioned_dir):
        import geopandas as gpd

        _, files = partitioned_dir
        gdf = gpd.read_parquet(str(files[0]))
        assert len(gdf) > 0

    def test_geopandas_reads_the_partition_directory(self, partitioned_dir):
        import geopandas as gpd

        out, _ = partitioned_dir
        gdf = gpd.read_parquet(str(out))
        assert len(gdf) > 0


def test_v2_fast_path_does_not_carry_a_bboxless_covering(tmp_path):
    """The 2.0 no-rewrite path carries the input's block verbatim — gate it too."""
    from geoparquet_io.core.write_funnels import _geo_block_to_carry_on_fast_path

    carried = {
        "geo": json.dumps(
            {
                "version": "2.0.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "geometry_types": ["Point"],
                        "bbox": [0, 0, 1, 1],
                        "covering": {"h3": {"column": "h3", "resolution": 9}},
                    }
                },
            }
        )
    }
    block = _geo_block_to_carry_on_fast_path(carried, "geometry", "2.0")
    if block is not None:
        covering = block["columns"]["geometry"].get("covering")
        assert covering is None or "bbox" in covering, covering
