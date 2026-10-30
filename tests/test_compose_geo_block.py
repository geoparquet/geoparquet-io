"""``compose_geo_block`` (the #1367 T2/T3e stub) agrees with ``create_geo_metadata``.

For now the stub is a wrapper over :func:`create_geo_metadata` and today's gate
chain (covering build -> ``strip_unsupported_covering`` ->
``strip_absent_covering`` -> ``strip_bboxless_covering`` -> edges for the
version). Where the inputs need none of the extra gates, its result must be
exactly what ``create_geo_metadata`` builds, whichever source form it is given.
"""

from __future__ import annotations

import json

import duckdb
import pyarrow as pa
import pytest

from geoparquet_io.core.geo_metadata import (
    ArrowTableSource,
    CarriedSource,
    GeoBlockOptions,
    QuerySource,
    SchemaSource,
    compose_geo_block,
    create_geo_metadata,
)

CARRIED = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {
        "geometry": {
            "encoding": "WKB",
            "geometry_types": ["Point"],
            "crs": None,
            "covering": {
                "bbox": {
                    "xmin": ["bbox", "xmin"],
                    "ymin": ["bbox", "ymin"],
                    "xmax": ["bbox", "xmax"],
                    "ymax": ["bbox", "ymax"],
                }
            },
        }
    },
}
BBOX_TYPE = pa.struct([(n, pa.float64()) for n in ("xmin", "ymin", "xmax", "ymax")])


def _table(with_bbox: bool = True, geo: dict | None = CARRIED) -> pa.Table:
    cols = {"id": pa.array([1], pa.int64()), "geometry": pa.array([b"\x01"], pa.binary())}
    if with_bbox:
        cols["bbox"] = pa.array([{"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}], BBOX_TYPE)
    table = pa.table(cols)
    if geo is not None:
        table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    return table


def _sources(table: pa.Table):
    con = duckdb.connect()
    con.register("t", table)
    return [
        ArrowTableSource(table),
        SchemaSource(table.schema, table.schema.metadata),
        QuerySource(con, "SELECT * FROM t", table.schema.metadata),
    ]


def _bbox_info(column):
    return {"has_bbox_column": column is not None, "bbox_column_name": column}


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "2.0.0"])
@pytest.mark.parametrize("bbox_column", [None, "bbox"])
@pytest.mark.parametrize("carried", [None, CARRIED], ids=["fresh", "carried"])
@pytest.mark.parametrize(
    "custom",
    [None, {"covering": {"h3": {"column": "h3", "resolution": 9}}}],
    ids=["no-custom", "h3"],
)
def test_matches_create_geo_metadata(version, bbox_column, carried, custom):
    table = _table(with_bbox=True, geo=carried)
    if custom:
        table = table.append_column("h3", pa.array(["8928308280fffff"]))
    expected = create_geo_metadata(
        table.schema.metadata, "geometry", _bbox_info(bbox_column), custom, version=version
    )
    options = GeoBlockOptions(
        geometry_column="geometry", bbox_column=bbox_column, custom_metadata=custom, version=version
    )
    for source in _sources(table):
        assert compose_geo_block(source, options) == expected, type(source).__name__


def test_drops_a_covering_over_a_column_the_source_lacks():
    """The gate ``create_geo_metadata`` does not run: ``strip_absent_covering``."""
    table = _table(with_bbox=False)
    options = GeoBlockOptions(geometry_column="geometry")
    legacy = create_geo_metadata(table.schema.metadata, "geometry", None)
    assert "covering" in legacy["columns"]["geometry"]
    for source in _sources(table):
        block = compose_geo_block(source, options)
        assert "covering" not in block["columns"]["geometry"], type(source).__name__


def test_edges_follow_the_version():
    table = _table(geo=None)
    one = compose_geo_block(
        ArrowTableSource(table),
        GeoBlockOptions(geometry_column="geometry", version="1.1.0", edges="karney"),
    )
    two = compose_geo_block(
        ArrowTableSource(table),
        GeoBlockOptions(geometry_column="geometry", version="2.0.0", edges="karney"),
    )
    assert one["columns"]["geometry"]["edges"] == "spherical"
    assert two["columns"]["geometry"]["edges"] == "karney"


def test_never_mutates_the_source_metadata():
    table = _table()
    before = table.schema.metadata[b"geo"]
    compose_geo_block(
        ArrowTableSource(table), GeoBlockOptions(geometry_column="geometry", version="1.0.0")
    )
    assert table.schema.metadata[b"geo"] == before


def test_carried_source_skips_the_absent_gate_and_can_defer_the_bboxless_one():
    """The form ``write_strategies.base.build_geo_metadata`` composes with.

    Its funnel already pruned the carried block, so there are no columns to ask;
    and its callers declare a bbox column before the bboxless gate (#954), so an
    h3-only covering must reach them intact.
    """
    carried = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "covering": {"h3": {"column": "h3"}}}},
    }
    metadata = {b"geo": json.dumps(carried).encode()}
    deferred = compose_geo_block(
        CarriedSource(metadata),
        GeoBlockOptions(geometry_column="geometry", defer_bboxless_gate=True),
    )
    assert deferred["columns"]["geometry"]["covering"] == {"h3": {"column": "h3"}}
    gated = compose_geo_block(CarriedSource(metadata), GeoBlockOptions(geometry_column="geometry"))
    assert "covering" not in gated["columns"]["geometry"]


def test_bboxless_gate_runs_again_after_the_absent_gate():
    """Step 5: dropping the absent bbox member leaves an h3-only covering, which
    the second :func:`strip_bboxless_covering` must drop (#954)."""
    carried = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "covering": {
                    **CARRIED["columns"]["geometry"]["covering"],
                    "h3": {"column": "h3", "resolution": 9},
                },
            }
        },
    }
    table = pa.table(
        {"geometry": pa.array([b"\x01"], pa.binary()), "h3": pa.array(["8a"], pa.string())}
    ).replace_schema_metadata({b"geo": json.dumps(carried).encode()})
    for source in _sources(table):
        block = compose_geo_block(source, GeoBlockOptions(geometry_column="geometry"))
        assert "covering" not in block["columns"]["geometry"], type(source).__name__
        deferred = compose_geo_block(
            source, GeoBlockOptions(geometry_column="geometry", defer_bboxless_gate=True)
        )
        assert deferred["columns"]["geometry"]["covering"] == {
            "h3": {"column": "h3", "resolution": 9}
        }, type(source).__name__


def test_encoding_applies_only_when_the_carried_block_names_none():
    fresh = compose_geo_block(
        CarriedSource(None), GeoBlockOptions(geometry_column="geometry", encoding="point")
    )
    assert fresh["columns"]["geometry"]["encoding"] == "point"
    table = _table()
    kept = compose_geo_block(
        ArrowTableSource(table), GeoBlockOptions(geometry_column="geometry", encoding="point")
    )
    assert kept["columns"]["geometry"]["encoding"] == "WKB"
