"""``compose_geo_block``: the only ``geo`` block composer (#1367 concept 4).

Unit table for the owner: every source form gives the same block, each option
does one thing, and every gate runs in the one order whatever the options say
(there is no flag that skips one). The cross-path half -- every write path
declaring the same block for one input -- is ``tests/test_geo_block_composition.py``.
"""

from __future__ import annotations

import copy
import json

import duckdb
import pyarrow as pa
import pytest

from geoparquet_io.core.geo_metadata import (
    DETECT_BBOX_COLUMN,
    KEEP_CRS,
    ArrowTableSource,
    GeoBlockOptions,
    QuerySource,
    SchemaSource,
    build_bbox_covering,
    compose_geo_block,
)

BBOX_TYPE = pa.struct([(n, pa.float64()) for n in ("xmin", "ymin", "xmax", "ymax")])
OVERTURE_BBOX = pa.struct([(n, pa.float64()) for n in ("xmin", "xmax", "ymin", "ymax")])
CARRIED = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {
        "geometry": {
            "encoding": "WKB",
            "geometry_types": ["Point"],
            "crs": None,
            "covering": {"bbox": build_bbox_covering("bbox")},
        }
    },
}


def _table(bbox_type=BBOX_TYPE, geo: dict | None = CARRIED, extra=None) -> pa.Table:
    cols = {"id": pa.array([1], pa.int64()), "geometry": pa.array([b"\x01"], pa.binary())}
    if bbox_type is not None:
        row = {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
        cols["bbox"] = pa.array([row], bbox_type)
    cols.update(extra or {})
    table = pa.table(cols)
    if geo is not None:
        table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    return table


def _sources(table: pa.Table):
    con = duckdb.connect()
    con.register("t", table.replace_schema_metadata(None))
    return [
        ArrowTableSource(table),
        SchemaSource(table.schema, table.schema.metadata),
        QuerySource(con, "SELECT * FROM t", table.schema.metadata),
    ]


def _compose(table, **options):
    blocks = [compose_geo_block(s, GeoBlockOptions("geometry", **options)) for s in _sources(table)]
    assert blocks[0] == blocks[1] == blocks[2], "the three source forms disagree"
    return blocks[0]


@pytest.mark.parametrize(
    ("version", "expected"),
    [("1.0", "1.0.0"), ("1.1", "1.1.0"), ("1.1-geoarrow", "1.1.0"), ("2.0", "2.0.0")],
)
def test_version_is_the_output_key_in_metadata_form(version, expected):
    assert _compose(_table(), version=version)["version"] == expected


def test_parquet_geo_only_composes_a_block_with_no_version():
    """Built only to key the native types; it is never written."""
    assert _compose(_table(), version="parquet-geo-only")["version"] is None


def test_a_metadata_form_version_is_refused():
    with pytest.raises(ValueError, match="version must be one of"):
        compose_geo_block(ArrowTableSource(_table()), GeoBlockOptions("geometry", version="1.1.0"))


def test_a_description_keeps_the_carried_version_and_decides_none_itself():
    """The composer resolves no version: a block-less description takes the facade's."""
    carried = {**CARRIED, "version": "1.0.0"}
    assert _compose(_table(geo=carried))["version"] == "1.0.0"
    assert _compose(_table(geo=carried), version="2.0", describe=True)["version"] == "1.0.0"
    assert _compose(_table(geo=None), version="1.1", describe=True)["version"] == "1.1.0"
    with pytest.raises(ValueError, match="declares none"):
        compose_geo_block(ArrowTableSource(_table(geo=None)), GeoBlockOptions("geometry"))


@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_detect_declares_the_conventional_bbox_column(version):
    block = _compose(_table(geo=None), version=version)
    assert block["columns"]["geometry"]["covering"] == {"bbox": build_bbox_covering("bbox")}


def test_covering_none_declares_none_and_still_gates_the_carried_one():
    """``covering=None`` is a request, not a skip: an illegal carried covering still goes."""
    assert (
        "covering"
        not in _compose(_table(geo=None), version="1.1", covering=None)["columns"]["geometry"]
    )
    block = _compose(_table(bbox_type=OVERTURE_BBOX), version="1.1", covering=None)
    assert "covering" not in block["columns"]["geometry"]


def test_v1_0_declares_no_covering_whatever_is_asked():
    custom = {"covering": {"h3": {"column": "h3", "resolution": 9}}}
    block = _compose(_table(), version="1.0", custom_metadata=custom)
    assert "covering" not in block["columns"]["geometry"]


def test_an_index_entry_without_a_bbox_member_is_dropped():
    """#954: the bboxless gate runs last, after the absent gate."""
    custom = {"covering": {"h3": {"column": "h3", "resolution": 9}}}
    extra = {"h3": pa.array(["8a"], pa.string())}
    block = _compose(
        _table(bbox_type=None, geo=None, extra=extra), version="1.1", custom_metadata=custom
    )
    assert "covering" not in block["columns"]["geometry"]
    kept = _compose(_table(geo=None, extra=extra), version="1.1", custom_metadata=custom)
    assert set(kept["columns"]["geometry"]["covering"]) == {"h3", "bbox"}


def test_a_covering_over_an_absent_column_is_dropped():
    block = _compose(_table(bbox_type=None), version="1.1")
    assert "covering" not in block["columns"]["geometry"]


def test_custom_metadata_coverings_merge_and_other_keys_go_on_top():
    custom = {"covering": {"bbox": build_bbox_covering("bbox")}, "note": "x"}
    block = _compose(_table(geo=None), version="1.1", covering=None, custom_metadata=custom)
    assert block["note"] == "x"
    assert block["columns"]["geometry"]["covering"] == {"bbox": build_bbox_covering("bbox")}


def test_crs_follows_the_output_rule_for_a_write_and_is_recorded_for_a_description():
    crs84 = {"id": {"authority": "OGC", "code": "CRS84"}, "type": "GeographicCRS"}
    write = _compose(_table(geo=None), version="1.1", crs=crs84)
    assert "crs" not in write["columns"]["geometry"]
    description = _compose(_table(geo=None), version="1.1", describe=True, crs=crs84)
    assert description["columns"]["geometry"]["crs"] == crs84
    kept = _compose(_table(), version="1.1", crs=KEEP_CRS)
    assert kept["columns"]["geometry"]["crs"] is None


def test_edges_map_to_the_version_for_a_write_and_stay_as_declared_for_a_description():
    write = _compose(_table(geo=None), version="1.1", edges="vincenty")
    assert write["columns"]["geometry"]["edges"] == "spherical"
    assert "orientation" not in write["columns"]["geometry"]  # keyed off the request
    spherical = _compose(_table(geo=None), version="1.1", edges="spherical")
    assert spherical["columns"]["geometry"]["orientation"] == "counterclockwise"
    v2 = _compose(_table(geo=None), version="2.0", edges="vincenty")
    assert v2["columns"]["geometry"]["edges"] == "vincenty"
    description = _compose(_table(geo=None), version="1.1", describe=True, edges="vincenty")
    assert description["columns"]["geometry"]["edges"] == "vincenty"
    assert "orientation" not in description["columns"]["geometry"]


def test_stats_set_and_a_none_removes():
    block = _compose(
        _table(), version="1.1", stats={"geometry": {"bbox": [0, 0, 1, 1], "geometry_types": None}}
    )
    assert block["columns"]["geometry"]["bbox"] == [0, 0, 1, 1]
    assert "geometry_types" not in block["columns"]["geometry"]


def test_encoding_given_wins_and_absent_defaults_to_wkb():
    assert _compose(_table(geo=None), version="1.1")["columns"]["geometry"]["encoding"] == "WKB"
    forced = _compose(_table(), version="1.1-geoarrow", encoding="point")
    assert forced["columns"]["geometry"]["encoding"] == "point"


def test_secondaries_get_their_declared_entry():
    info = {"secondary": ["geom2"], "metadata": {"geom2": {"crs": None}}}
    extra = {"geom2": pa.array([b"\x01"], pa.binary())}
    block = _compose(_table(extra=extra), version="1.1", geometry_info=info)
    assert block["columns"]["geom2"] == {"crs": None, "encoding": "WKB"}


GEOM2 = {"geom2": pa.array([b"\x01"], pa.binary())}


def test_a_declared_secondary_goes_through_the_steps_not_pasted_in():
    """Its edges are mapped (step 4), its crs follows the output rule (step 5),
    its stats land (step 8); only its other keys are carried as declared."""
    crs84 = {"id": {"authority": "OGC", "code": "CRS84"}, "type": "GeographicCRS"}
    declared = {
        "encoding": "point",
        "edges": "vincenty",
        "crs": crs84,
        "geometry_types": ["Point"],
        "bbox": [0, 0, 1, 1],
        "epoch": 2020.5,
    }
    info = {"secondary": ["geom2"], "metadata": {"geom2": declared}}
    block = _compose(_table(extra=GEOM2), version="1.1", geometry_info=info)
    assert block["columns"]["geom2"] == {
        "encoding": "point",
        "edges": "spherical",
        "geometry_types": ["Point"],
        "bbox": [0, 0, 1, 1],
        "epoch": 2020.5,
    }
    v2 = _compose(_table(extra=GEOM2), version="2.0", geometry_info=info)
    assert v2["columns"]["geom2"]["edges"] == "vincenty"


def test_a_carried_secondary_entry_wins_over_what_geometry_info_declares():
    carried = copy.deepcopy(CARRIED)
    carried["columns"]["geom2"] = {"encoding": "WKB", "geometry_types": ["Polygon"]}
    info = {"secondary": ["geom2"], "metadata": {"geom2": {"geometry_types": ["Point"]}}}
    block = _compose(_table(geo=carried, extra=GEOM2), version="1.1", geometry_info=info)
    assert block["columns"]["geom2"]["geometry_types"] == ["Polygon"]


def test_the_primary_is_never_a_secondary():
    info = {"secondary": ["geometry"], "metadata": {"geometry": {"crs": None}}}
    with pytest.raises(ValueError, match="primary geometry column"):
        compose_geo_block(
            ArrowTableSource(_table()),
            GeoBlockOptions("geometry", version="1.1", geometry_info=info),
        )


def test_primary_column_relabels_and_geometry_column_alone_does_not():
    carried = copy.deepcopy(CARRIED)
    carried["columns"]["geom2"] = {"encoding": "WKB", "geometry_types": ["Point"]}
    table = _table(geo=carried, extra=GEOM2)
    kept = compose_geo_block(ArrowTableSource(table), GeoBlockOptions("geom2", version="1.1"))
    assert kept["primary_column"] == "geometry"
    relabelled = compose_geo_block(
        ArrowTableSource(table), GeoBlockOptions("geom2", version="1.1", primary_column="geom2")
    )
    assert relabelled["primary_column"] == "geom2"


def test_column_edges_are_mapped_and_imply_no_orientation():
    """A restore of what the data has, not a request: no orientation is claimed."""
    carried = copy.deepcopy(CARRIED)
    carried["columns"]["geom2"] = {"encoding": "WKB", "geometry_types": ["Point"]}
    block = _compose(
        _table(geo=carried, extra=GEOM2),
        version="1.1",
        column_edges={"geometry": "spherical", "geom2": "vincenty"},
    )
    assert block["columns"]["geometry"]["edges"] == "spherical"
    assert block["columns"]["geom2"]["edges"] == "spherical"
    assert "orientation" not in block["columns"]["geometry"]
    assert "orientation" not in block["columns"]["geom2"]


def test_1_1_geoarrow_keeps_the_declared_native_encoding():
    info = {"secondary": [], "metadata": {"geometry": {"encoding": "polygon"}}}
    block = _compose(_table(), version="1.1-geoarrow", geometry_info=info)
    assert block["columns"]["geometry"]["encoding"] == "polygon"
    assert _compose(_table(), version="1.1", geometry_info=info)["columns"]["geometry"][
        "encoding"
    ] == ("WKB")


def test_covering_provenance_names_the_column_when_nothing_is_carried():
    """A table DuckDB built carries no block; the input's says which struct is the bbox (#953)."""
    provenance = copy.deepcopy(CARRIED)
    provenance["columns"]["geometry"]["covering"] = {"bbox": build_bbox_covering("bounds")}
    extra = {"bounds": pa.array([{"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}], BBOX_TYPE)}
    table = _table(bbox_type=None, geo=None, extra=extra)
    options = {"version": "1.1", "covering_provenance": {b"geo": json.dumps(provenance)}}
    block = _compose(table, **options)
    assert block["columns"]["geometry"]["covering"] == {"bbox": build_bbox_covering("bounds")}
    assert "covering" not in _compose(table, version="1.1")["columns"]["geometry"]


def test_never_mutates_the_source_metadata():
    table = _table()
    before = copy.deepcopy(table.schema.metadata)
    custom = {"covering": {"h3": {"column": "id", "resolution": 9}}}
    _compose(table, version="1.0", custom_metadata=custom, stats={"geometry": {"bbox": None}})
    assert table.schema.metadata == before


def test_detect_is_the_default_covering_request():
    assert GeoBlockOptions("geometry").covering is DETECT_BBOX_COLUMN


def test_a_carried_block_without_the_column_gets_its_entry():
    """Step 1: a carried block naming no entry for the column still gets one (was
    ``TestInitializeGeoMetadata.test_adds_missing_geometry_column``)."""
    carried = {"version": "1.1.0", "primary_column": "geometry", "columns": {}}
    block = _compose(_table(bbox_type=None, geo=carried), version="1.1")
    assert block["columns"]["geometry"]["encoding"] == "WKB"
    assert block["primary_column"] == "geometry"


def test_a_requested_bbox_column_name_is_declared():
    """A named covering request declares that column (was
    ``TestAddBboxCovering.test_custom_bbox_column_name``)."""
    bounds = {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
    table = _table(bbox_type=None, geo=None, extra={"bounds": pa.array([bounds], BBOX_TYPE)})
    block = _compose(table, version="1.1", covering="bounds")
    assert block["columns"]["geometry"]["covering"] == {"bbox": build_bbox_covering("bounds")}


def test_measure_missing_fills_only_what_the_block_lacks():
    """A writer measuring the gaps composes once: its stats land as step 8's do."""
    seen = []

    def measure(block):
        seen.append(copy.deepcopy(block["columns"]["geometry"]))
        return {"geometry": {"bbox": [0.0, 0.0, 1.0, 1.0]}}

    block = _compose(_table(), version="1.1", measure_missing=measure)
    assert block["columns"]["geometry"]["bbox"] == [0.0, 0.0, 1.0, 1.0]
    assert block["columns"]["geometry"]["geometry_types"] == ["Point"]
    assert all("bbox" not in entry for entry in seen)


def test_column_crs_resolves_another_columns_crs():
    carried = copy.deepcopy(CARRIED)
    carried["columns"]["other"] = {"encoding": "WKB", "geometry_types": [], "crs": None}
    table = _table(geo=carried, extra={"other": pa.array([b"\x01"], pa.binary())})
    block = _compose(table, version="1.1", column_crs={"other": "OGC:CRS84"})
    assert "crs" not in block["columns"]["other"]  # the default, by omission


def test_edges_and_column_edges_that_disagree_on_the_primary_are_refused():
    with pytest.raises(ValueError, match="column_edges"):
        _compose(_table(), version="1.1", edges="spherical", column_edges={"geometry": "planar"})


def test_edges_and_column_edges_that_agree_are_one_answer():
    block = _compose(
        _table(), version="1.1", edges="spherical", column_edges={"geometry": "spherical"}
    )
    assert block["columns"]["geometry"]["edges"] == "spherical"
