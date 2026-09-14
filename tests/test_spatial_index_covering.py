"""Spatial-index covering metadata survives alongside a bbox covering.

Regression tests for #694: `gpio add quadkey/h3/s2/a5` record their index in the
GeoParquet `covering` metadata, and the bbox covering used to *replace* that dict
rather than merge into it. On an input with a bbox column the index entry was
silently discarded; on an input without one the clobbering branch was skipped,
which is why every pre-existing covering assertion ran against
`buildings_test.parquet` (no bbox column) and passed.

Two independent mechanisms keep the two entries apart today, and each has its
own test here:

* an *undeclared* conventional `bbox` column is declared by
  ``declare_carried_bbox_column`` (``core/geo_metadata.py``), which
  uses ``setdefault`` so the index entry already in ``covering`` survives;
* a covering the input *declared* — including one whose bbox columns are not
  named ``bbox``, which nothing can re-derive — survives only because
  ``build_geo_metadata`` (``write_strategies/base.py``) merges a custom
  ``covering`` one entry deep instead of replacing the dict.
"""

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.add.a5 import add_a5_column
from geoparquet_io.core.add.h3 import add_h3_column
from geoparquet_io.core.add.quadkey import add_quadkey_column
from geoparquet_io.core.add.s2 import add_s2_column

# (index key, writer, kwargs, expected covering entry)
INDEX_COMMANDS = [
    ("h3", add_h3_column, {"h3_resolution": 8}, {"column": "h3_cell", "resolution": 8}),
    ("s2", add_s2_column, {"s2_level": 10}, {"column": "s2_cell", "level": 10}),
    ("a5", add_a5_column, {"a5_resolution": 8}, {"column": "a5_cell", "resolution": 8}),
    ("quadkey", add_quadkey_column, {"resolution": 12}, {"column": "quadkey", "resolution": 12}),
]


def _bbox_covering(column):
    """The covering entry a bbox struct column named ``column`` must produce."""
    return {
        "xmin": [column, "xmin"],
        "ymin": [column, "ymin"],
        "xmax": [column, "xmax"],
        "ymax": [column, "ymax"],
    }


def _covering(path):
    """Return the primary geometry column's ``covering`` dict from a written file."""
    kv = pq.ParquetFile(str(path)).metadata.metadata or {}
    assert b"geo" in kv, f"{path} was written without a 'geo' key; keys: {sorted(kv)}"
    geo = json.loads(kv[b"geo"].decode("utf-8"))
    return geo["columns"][geo["primary_column"]].get("covering", {})


@pytest.fixture
def places_named_covering_file(tmp_path):
    """A 1.1 input that *declares* a bbox covering over a column not named ``bbox``.

    ``declare_carried_bbox_column`` only recognises the literal name ``bbox``
    (``SELF_EVIDENT_BBOX_COLUMN``), so it cannot re-derive this covering. The
    one-entry-deep ``covering`` merge in ``build_geo_metadata`` is the only thing
    that keeps it when an ``add`` command contributes its own entry.

    Written through DuckDB's ``KV_METADATA`` for the same reasons as
    ``places_v11_file`` in ``tests/conftest.py``.
    """
    from pathlib import Path

    from geoparquet_io.core.duckdb_utils import _escape_sql_string, get_duckdb_connection

    source = Path(__file__).parent / "data" / "places_test.parquet"
    path = tmp_path / "places_named_covering.parquet"

    geo = json.loads(pq.read_metadata(str(source)).metadata[b"geo"].decode("utf-8"))
    geo["version"] = "1.1.0"
    geo["columns"][geo["primary_column"]]["covering"] = {"bbox": _bbox_covering("my_bounds")}

    con = get_duckdb_connection(load_spatial=False)
    con.execute(f"""
        COPY (SELECT * EXCLUDE (bbox), bbox AS my_bounds FROM '{source.as_posix()}')
        TO '{path.as_posix()}'
        (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE',
         KV_METADATA {{geo: '{_escape_sql_string(json.dumps(geo))}'}})
    """)
    con.close()

    # Non-vacuity: the input must really declare the covering under the odd name,
    # and must not carry a column named 'bbox' that could be re-derived instead.
    names = pq.ParquetFile(str(path)).schema_arrow.names
    assert "my_bounds" in names and "bbox" not in names, names
    assert _covering(path) == {"bbox": _bbox_covering("my_bounds")}
    return str(path)


@pytest.mark.parametrize(
    ("index_key", "writer", "kwargs", "entry"),
    INDEX_COMMANDS,
    ids=[c[0] for c in INDEX_COMMANDS],
)
class TestSpatialIndexCoveringWithBbox:
    def test_index_covering_survives_bbox_covering(
        self, places_v11_file, tmp_path, index_key, writer, kwargs, entry
    ):
        """Both the bbox covering and the spatial-index covering reach the file."""
        output = tmp_path / f"{index_key}.parquet"

        writer(places_v11_file, str(output), **kwargs)

        covering = _covering(output)
        assert index_key in covering, (
            f"{index_key} covering was dropped; got keys {sorted(covering)}"
        )
        assert "bbox" in covering, f"bbox covering was dropped; got keys {sorted(covering)}"
        assert covering[index_key] == entry
        assert covering["bbox"] == _bbox_covering("bbox")

    def test_declared_covering_over_non_bbox_column_survives(
        self, places_named_covering_file, tmp_path, index_key, writer, kwargs, entry
    ):
        """A declared covering keeps its provenance when the columns aren't named ``bbox``.

        Nothing can re-derive ``my_bounds``; only the one-entry-deep merge in
        ``build_geo_metadata`` protects it.
        """
        output = tmp_path / f"{index_key}.parquet"

        writer(places_named_covering_file, str(output), **kwargs)

        covering = _covering(output)
        assert index_key in covering, (
            f"{index_key} covering was dropped; got keys {sorted(covering)}"
        )
        assert "bbox" in covering, (
            f"declared bbox covering was dropped; got keys {sorted(covering)}"
        )
        assert covering[index_key] == entry
        assert covering["bbox"] == _bbox_covering("my_bounds")

    def test_index_covering_present_without_bbox_column(
        self, buildings_test_file, tmp_path, index_key, writer, kwargs, entry
    ):
        """The bbox-free input keeps working — this is the branch that always passed."""
        output = tmp_path / f"{index_key}.parquet"

        writer(buildings_test_file, str(output), **kwargs)

        covering = _covering(output)
        assert index_key in covering
        assert covering[index_key] == entry
        # Self-guard: if this fixture ever grew a bbox column the case above
        # would silently become a duplicate of the bbox-bearing test.
        assert "bbox" not in covering, f"control input gained a bbox covering: {sorted(covering)}"


def test_second_index_keeps_the_first(places_v11_file, tmp_path):
    """``add h3`` then ``add s2`` accumulates: bbox, h3 and s2 all survive."""
    first = tmp_path / "h3.parquet"
    second = tmp_path / "h3_s2.parquet"

    add_h3_column(places_v11_file, str(first), h3_resolution=8)
    add_s2_column(str(first), str(second), s2_level=10)

    covering = _covering(second)
    assert sorted(covering) == ["bbox", "h3", "s2"], sorted(covering)
    assert covering["h3"] == {"column": "h3_cell", "resolution": 8}
    assert covering["s2"] == {"column": "s2_cell", "level": 10}
    assert covering["bbox"] == _bbox_covering("bbox")
