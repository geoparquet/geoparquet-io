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
    # With the bbox-less covering gone the block says nothing DuckDB would not,
    # so there is nothing to carry.
    assert _geo_block_to_carry_on_fast_path(carried, "geometry", "2.0") is None


def test_v2_fast_path_carries_the_rest_of_the_block_without_the_covering():
    """A block that still has something to say is carried -- minus the covering."""
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
                        "orientation": "counterclockwise",
                        "covering": {"h3": {"column": "h3", "resolution": 9}},
                    }
                },
            }
        )
    }
    block = _geo_block_to_carry_on_fast_path(carried, "geometry", "2.0")
    assert block is not None
    column = block["columns"]["geometry"]
    assert column["orientation"] == "counterclockwise"
    assert "covering" not in column


class TestIndexEntriesBesideABboxColumn:
    """With a bbox column to declare, index entries are kept -- on every path."""

    def test_a_custom_h3_column_name_reaches_the_covering(self, places_test_file, tmp_path):
        """The entry names the column actually written, at the resolution asked for."""
        output = tmp_path / "h3.parquet"
        result = CliRunner().invoke(
            cli,
            [
                "add",
                "h3",
                places_test_file,
                str(output),
                "--h3-name",
                "h3_building",
                "--resolution",
                "13",
            ],
        )
        assert result.exit_code == 0, result.output

        covering = _primary_column_meta(output)["covering"]
        assert covering["h3"] == {"column": "h3_building", "resolution": 13}
        assert "bbox" in covering

    @pytest.mark.parametrize("strategy", ["duckdb-kv", "in-memory", "disk-rewrite"])
    def test_every_strategy_declares_the_bbox_beside_the_index_entry(
        self, places_test_file, tmp_path, strategy
    ):
        """disk-rewrite used to skip the declare step and drop the index entry."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
        from geoparquet_io.core.write_funnels import write_parquet_with_metadata

        output = tmp_path / f"{strategy}.parquet"
        entry = {"column": "quadkey", "resolution": 6}
        original = dict(pq.ParquetFile(places_test_file).metadata.metadata)
        con = get_duckdb_connection(load_spatial=True)
        try:
            write_parquet_with_metadata(
                con,
                f"SELECT *, 'q' AS quadkey FROM read_parquet({sql_path(places_test_file)})",
                str(output),
                original_metadata=original,
                custom_metadata={"covering": {"quadkey": entry}},
                geoparquet_version="1.1",
                write_strategy=strategy,
            )
        finally:
            con.close()

        covering = _primary_column_meta(output)["covering"]
        assert covering["quadkey"] == entry
        assert covering["bbox"]["xmin"] == ["bbox", "xmin"]


def test_add_kdtree_writes_no_bboxless_covering(buildings_test_file, tmp_path):
    """kdtree records its own entry shape; the same gate applies (#954)."""
    import geopandas as gpd

    from geoparquet_io.core.add.kdtree import add_kdtree_column

    output = tmp_path / "kdtree.parquet"
    add_kdtree_column(buildings_test_file, str(output), iterations=2)

    assert "covering" not in _primary_column_meta(output)
    assert len(gpd.read_parquet(str(output))) > 0


def test_a_secondary_columns_bboxless_covering_is_gated_on_the_arrow_path():
    """Secondary entries merge in after create_geo_metadata; the gate runs again."""
    import pyarrow as pa

    from geoparquet_io.core.arrow_geo_metadata import _build_geo_block

    table = pa.table({"geometry": pa.array([], pa.binary()), "geom2": pa.array([], pa.binary())})
    geometry_info = {
        "primary": "geometry",
        "secondary": ["geom2"],
        "metadata": {
            "geom2": {"encoding": "WKB", "covering": {"h3": {"column": "h3", "resolution": 9}}}
        },
    }
    geo = _build_geo_block(table, "geometry", None, None, None, "1.1.0", None, geometry_info, False)

    assert "geom2" in geo["columns"]
    assert "covering" not in geo["columns"]["geom2"]


def test_a_bboxless_covering_does_not_force_the_v2_rewrite(
    buildings_test_file, tmp_path, monkeypatch
):
    """The rewrite it forced only produced a covering the gate then dropped."""
    from geoparquet_io.core import write_funnels
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

    v2 = tmp_path / "v2.parquet"
    result = CliRunner().invoke(
        cli, ["convert", buildings_test_file, str(v2), "--geoparquet-version", "2.0"]
    )
    assert result.exit_code == 0, result.output

    calls = []
    real_plain_copy = write_funnels._plain_copy_to

    def spy(*args, **kwargs):
        calls.append("plain")
        return real_plain_copy(*args, **kwargs)

    output = tmp_path / "out.parquet"
    original = dict(pq.ParquetFile(str(v2)).metadata.metadata)
    monkeypatch.setattr(write_funnels, "_plain_copy_to", spy)
    con = get_duckdb_connection(load_spatial=True)
    try:
        write_funnels.write_parquet_with_metadata(
            con,
            f"SELECT *, 'q' AS quadkey FROM read_parquet({sql_path(str(v2))})",
            str(output),
            original_metadata=original,
            custom_metadata={"covering": {"quadkey": {"column": "quadkey", "resolution": 6}}},
            geoparquet_version="2.0",
            input_file=str(v2),
        )
    finally:
        con.close()

    assert calls == ["plain"]
    assert "covering" not in _primary_column_meta(output)
