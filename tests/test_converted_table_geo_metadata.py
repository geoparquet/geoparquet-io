"""A Table from ``gpio.convert()`` describes its own geometry, like one from ``gpio.read()`` (#1222).

``gpio.convert()`` handed back a bare Arrow table: the geometry was WKB in a
``large_binary`` column with no ``geo`` schema metadata, and the CRS lived only
in a hint on the Python ``Table`` wrapper. Every ``Table`` method passes the
Arrow table, not the wrapper, down to core, so

* ``add_kdtree`` wrote it to a scratch file with ``pq.write_table``, DuckDB read
  the column back as ``BLOB``, and ``ST_Centroid(BLOB)`` failed to bind; and
* the cell-index methods (``add_h3``, ``add_quadkey``, ``add_s2``, ``add_a5``)
  found no CRS on the table, took projected coordinates for degrees and
  computed wrong cells without a word.

The converted table now carries the same ``geo`` block a read one does, and the
kdtree scratch path reads its geometry the way its siblings do.
"""

from __future__ import annotations

import json
import os

import pyarrow as pa
import pytest

import geoparquet_io as gpio
from geoparquet_io.core.add.kdtree import add_kdtree_table
from geoparquet_io.core.geo_metadata import parse_geo_metadata
from tests.conftest import skip_if_geography_unavailable

DATA = os.path.join(os.path.dirname(__file__), "data")


def _points_geojson(path) -> str:
    feats = [
        {
            "type": "Feature",
            "properties": {"id": i},
            "geometry": {
                "type": "Point",
                "coordinates": [-70 + (i % 20) * 0.01, -30 + (i // 20) * 0.01],
            },
        }
        for i in range(400)
    ]
    path.write_text(json.dumps({"type": "FeatureCollection", "features": feats}))
    return str(path)


@pytest.fixture(scope="module")
def converted(tmp_path_factory):
    """The #1222 reprex input, converted through the Python API."""
    return gpio.convert(_points_geojson(tmp_path_factory.mktemp("conv") / "p400.geojson"))


@pytest.fixture(scope="module")
def projected_pair(tmp_path_factory):
    """A projected (EPSG:6933) source, converted, and the same rows read back from GeoParquet."""
    converted = gpio.convert(os.path.join(DATA, "buildings_test_6933.gpkg"))
    out = tmp_path_factory.mktemp("proj") / "b6933.parquet"
    converted.write(str(out))
    return converted, gpio.read(str(out))


class TestConvertedTableCarriesGeo:
    def test_geo_block_names_the_geometry_column(self, converted):
        geo = parse_geo_metadata(converted.table.schema.metadata)

        assert geo is not None
        assert geo["primary_column"] == converted.geometry_column
        column = geo["columns"][converted.geometry_column]
        assert column["encoding"] == "WKB"
        # GeoJSON is CRS84 by RFC 7946, which the spec spells by omitting `crs`.
        assert "crs" not in column

    def test_projected_crs_lands_in_the_geo_block(self, projected_pair):
        converted, _ = projected_pair
        geo = parse_geo_metadata(converted.table.schema.metadata)

        crs = geo["columns"][converted.geometry_column]["crs"]
        assert crs["id"] == {"authority": "EPSG", "code": 6933}
        assert converted.crs == crs

    def test_written_file_is_unchanged_in_shape(self, projected_pair):
        """The carried block is only a description: write() still owns the output's."""
        _, read_back = projected_pair
        geo = parse_geo_metadata(read_back.table.schema.metadata)

        assert geo["version"] == "1.1.0"
        assert geo["columns"]["geometry"]["crs"]["id"]["code"] == 6933
        assert geo["columns"]["geometry"]["geometry_types"]
        assert len(geo["columns"]["geometry"]["bbox"]) == 4


_ADD_METHODS = [
    pytest.param("add_bbox", {}, id="add_bbox"),
    pytest.param("add_quadkey", {"resolution": 8}, id="add_quadkey"),
    pytest.param("add_h3", {"resolution": 6}, id="add_h3"),
    pytest.param("add_a5", {"resolution": 8}, id="add_a5"),
    pytest.param("add_s2", {"level": 8}, id="add_s2"),
    pytest.param("add_kdtree", {"iterations": 2}, id="add_kdtree-iterations"),
    pytest.param("add_kdtree", {"auto": True}, id="add_kdtree-auto"),
    pytest.param("add_geometry_metrics", {}, id="add_geometry_metrics"),
]


@pytest.mark.parametrize(("method", "kwargs"), _ADD_METHODS)
def test_every_add_method_runs_on_a_converted_table(converted, method, kwargs):
    if method == "add_s2":
        skip_if_geography_unavailable()

    result = getattr(converted, method)(**kwargs)

    assert result.num_rows == converted.num_rows
    assert len(result.column_names) > len(converted.column_names)


def test_add_bbox_metadata_runs_on_a_converted_table(converted):
    result = converted.add_bbox().add_bbox_metadata()

    geo = parse_geo_metadata(result.table.schema.metadata)
    assert "covering" in geo["columns"][result.geometry_column]


def test_kdtree_cells_split_the_converted_table(converted):
    """The #1222 reprex: 400 points in a 20x20 grid split into 4 even cells."""
    cells = converted.add_kdtree(iterations=2).table.column("kdtree_cell").to_pylist()

    assert len(set(cells)) == 4


@pytest.mark.parametrize(
    ("method", "kwargs", "column"),
    [
        pytest.param("add_h3", {"resolution": 5}, "h3_cell", id="h3"),
        pytest.param("add_quadkey", {"resolution": 10}, "quadkey", id="quadkey"),
        pytest.param("add_a5", {"resolution": 8}, "a5_cell", id="a5"),
        pytest.param("add_s2", {"level": 8}, "s2_cell", id="s2"),
        pytest.param("add_kdtree", {"iterations": 3}, "kdtree_cell", id="kdtree"),
    ],
)
def test_cells_from_a_converted_projected_table_match_a_read_one(
    projected_pair, method, kwargs, column
):
    """Converting and reading the same projected rows give the same cells."""
    if method == "add_s2":
        skip_if_geography_unavailable()
    converted, read_back = projected_pair

    from_convert = getattr(converted, method)(**kwargs).table.column(column).to_pylist()
    from_read = getattr(read_back, method)(**kwargs).table.column(column).to_pylist()

    assert from_convert == from_read


def test_kdtree_table_reads_bare_wkb_without_geo_metadata(converted):
    """Core is robust on its own: a WKB column with no `geo` key still works."""
    bare = pa.table(
        {
            "id": converted.table.column("id"),
            "geometry": converted.table.column("geometry").cast(pa.large_binary()),
        }
    )
    assert bare.schema.metadata is None

    result = add_kdtree_table(bare, iterations=2, geometry_column="geometry")

    assert len(set(result.column("kdtree_cell").to_pylist())) == 4


class TestDescribeWkbGeometryLeavesTablesAlone:
    def test_existing_geo_key_is_kept(self):
        import pyarrow as pa

        from geoparquet_io.core.arrow_geo_metadata import describe_wkb_geometry

        table = pa.table({"geometry": pa.array([b"\x01"], pa.binary())}).replace_schema_metadata(
            {b"geo": b'{"keep": true}'}
        )
        assert describe_wkb_geometry(table, "geometry") is table

    def test_missing_geometry_column_is_left_alone(self):
        import pyarrow as pa

        from geoparquet_io.core.arrow_geo_metadata import describe_wkb_geometry

        table = pa.table({"id": [1]})
        assert describe_wkb_geometry(table, "geometry") is table
