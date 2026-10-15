"""EsriJSON pages convert regardless of top-level member order (issue #501).

GDAL identifies an ESRIJSON file from the members near its start. With the GDAL
bundled in DuckDB spatial when these tests were written (DuckDB 1.5.6), a page
whose first member is ``features`` was read as GeoJSON at any size, and so was a
page whose ``geometryType`` started beyond roughly the first 6 KB (a large
``features`` array ahead of it). Either way the read failed. Other GDAL builds
may draw the line elsewhere; the tests assert successful conversion, not the
failure. They use real DuckDB/GDAL conversion of a recorded page, reordered in
memory, and of constructed pages.
"""

import gzip
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pyproj import CRS, Transformer

from geoparquet_io.core.arcgis import (
    _esrijson_page_to_table,
    _json_doc_to_table,
    _prepare_esrijson_for_gdal,
    convert_arcgis_to_geoparquet,
)
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from tests.http_transport import FakeTransport, json_reply
from tests.native_geo_probes import geo_block, geo_block_crs_id, spec_problems

SHORELINE = Path(__file__).parent / "data" / "arcgis_json" / "shoreline.esrijson.json.gz"
SHORELINE_SHA256 = "3c5adade08109e09059e30786fe241fcd1929b0d3926b5ab374f83aaedb939b5"

# The order reference pages are converted in. It is chosen here, not taken from
# the production rule, so a bad choice there shows up as a mismatch.
REFERENCE_ORDER = ("geometryType", "spatialReference", "fields", "objectIdFieldName")

# Top-level orders for the same page. The identifying members follow
# `features` in both problematic orders; the second resembles the response
# reported in #501, with other metadata ahead of `features`.
ORDERS = {
    "captured": (
        "objectIdFieldName",
        "uniqueIdField",
        "globalIdFieldName",
        "geometryProperties",
        "geometryType",
        "spatialReference",
        "fields",
        "features",
    ),
    "features_first": (
        "features",
        "objectIdFieldName",
        "uniqueIdField",
        "globalIdFieldName",
        "geometryProperties",
        "geometryType",
        "spatialReference",
        "fields",
    ),
    "geometry_properties_before_features": (
        "objectIdFieldName",
        "uniqueIdField",
        "globalIdFieldName",
        "geometryProperties",
        "features",
        "geometryType",
        "spatialReference",
        "fields",
    ),
}


@pytest.fixture(scope="module")
def shoreline_bytes():
    raw = gzip.decompress(SHORELINE.read_bytes())
    assert hashlib.sha256(raw).hexdigest() == SHORELINE_SHA256
    return raw


@pytest.fixture(scope="module")
def con():
    connection = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    yield connection
    connection.close()


def _reordered(raw: bytes, order: tuple[str, ...]) -> dict:
    page = json.loads(raw)
    assert sorted(order) == sorted(page)
    return {key: page[key] for key in order}


def _metadata_first(page: dict) -> dict:
    return {key: page[key] for key in REFERENCE_ORDER if key in page} | page


def _reference_table(page: dict, con):
    """Convert ``page`` metadata-first, bypassing `_esrijson_page_to_table`."""
    return _json_doc_to_table(_metadata_first(page), exclude="geom", suffix=".json", con=con)


def _assert_same_table(table, reference):
    assert table is not None
    assert table.schema.equals(reference.schema, check_metadata=True)
    assert [field.nullable for field in table.schema] == [
        field.nullable for field in reference.schema
    ]
    for name in reference.column_names:
        assert table.column(name).equals(reference.column(name)), name
    assert table.column("geometry").to_pylist() == reference.column("geometry").to_pylist()


@pytest.fixture(scope="module")
def reference(shoreline_bytes, con):
    """Metadata-first page converted without the EsriJSON page converter.

    Calling the shared converter directly keeps any preparation step inside
    `_esrijson_page_to_table` out of the reference, so a bug there cannot
    change both sides of the comparison.
    """
    table = _reference_table(json.loads(shoreline_bytes), con)

    assert table.num_rows == 1
    assert table.column("OBJECTID").to_pylist() == [11]
    assert len(table.column("geometry")[0].as_py()) > 100_000
    return table


@pytest.mark.parametrize("order", list(ORDERS))
def test_esrijson_page_converts_regardless_of_member_order(order, shoreline_bytes, con, reference):
    page = _reordered(shoreline_bytes, ORDERS[order])
    before = json.dumps(page)

    table = _esrijson_page_to_table(page, con=con)

    assert json.dumps(page) == before
    _assert_same_table(table, reference)


SHORELINE_PREPARED = [
    "geometryType",
    "objectIdFieldName",
    "uniqueIdField",
    "globalIdFieldName",
    "geometryProperties",
    "spatialReference",
    "fields",
    "features",
]


@pytest.mark.parametrize(
    ("order", "expected"),
    [
        (ORDERS["features_first"], SHORELINE_PREPARED),
        (ORDERS["geometry_properties_before_features"], SHORELINE_PREPARED),
        (
            ("fields", "geometryType", "features", "objectIdFieldName", "spatialReference")
            + ("uniqueIdField", "globalIdFieldName", "geometryProperties"),
            [
                "geometryType",
                "fields",
                "objectIdFieldName",
                "spatialReference",
                "uniqueIdField",
                "globalIdFieldName",
                "geometryProperties",
                "features",
            ],
        ),
    ],
    ids=["features_first", "geometry_properties_before_features", "interspersed"],
)
def test_prepare_puts_geometry_type_first_and_features_last(order, expected, shoreline_bytes):
    page = _reordered(shoreline_bytes, order)

    assert list(_prepare_esrijson_for_gdal(page)) == expected


def test_prepare_does_not_invent_a_features_member():
    """A page with no `features` member keeps its members and gains none."""
    page = {"fields": [], "geometryType": "esriGeometryPoint", "exceededTransferLimit": True}

    prepared = _prepare_esrijson_for_gdal(page)

    assert list(prepared) == ["geometryType", "fields", "exceededTransferLimit"]
    assert prepared == page


# ---------------------------------------------------------------------------
# Conversion of other page shapes and member sets
# ---------------------------------------------------------------------------

# When `geometryType` follows `features`, 200 features put it well past the
# ~6 KB identification window seen in the tested GDAL build, so both orders
# below fail without the fix.
SHAPES = {
    "point_z": (
        "esriGeometryPoint",
        {"hasZ": True},
        lambda i: {"x": -75.0 + i / 1000, "y": 38.0, "z": i / 2},
    ),
    "polygon_with_hole": (
        "esriGeometryPolygon",
        {},
        lambda i: {
            "rings": [
                [[i, 0], [i, 10], [i + 10, 10], [i + 10, 0], [i, 0]],
                [[i + 2, 2], [i + 8, 2], [i + 8, 8], [i + 2, 8], [i + 2, 2]],
            ]
        },
    ),
}


def _shape_page(shape: str) -> dict:
    geometry_type, extra, geometry = SHAPES[shape]
    features = [
        {"attributes": {"OBJECTID": i, "label": f"f-{i}"}, "geometry": geometry(i)}
        for i in range(1, 201)
    ]
    features.append({"attributes": {"OBJECTID": 201, "label": None}, "geometry": None})
    metadata = {
        "objectIdFieldName": "OBJECTID",
        "geometryType": geometry_type,
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        **extra,
        "fields": [
            {"name": "OBJECTID", "type": "esriFieldTypeOID"},
            {"name": "label", "type": "esriFieldTypeString", "length": 20},
        ],
    }
    return {"features": features} | metadata


@pytest.mark.parametrize("shape", list(SHAPES))
def test_constructed_features_first_page_converts(shape, con):
    page = _shape_page(shape)
    before = json.dumps(page)
    reference = _reference_table(page, con)
    assert reference.num_rows == 201
    assert reference.column("geometry").null_count == 1

    table = _esrijson_page_to_table(page, con=con)

    assert json.dumps(page) == before
    _assert_same_table(table, reference)


_LONG_WKT_FEATURES = [{"attributes": {"id": 1}, "geometry": {"x": 1, "y": 2}}]


@pytest.mark.parametrize(
    "order",
    [
        ("fields", "features", "spatialReference"),
        ("fieldAliases", "features", "spatialReference"),
        ("fieldAliases", "features", "spatialReference", "fields"),
    ],
    ids=["fields_first", "field_aliases_first", "field_aliases_first_fields_last"],
)
def test_a_page_readable_as_sent_stays_readable(order, con):
    """No `geometryType`; a recognized member first, a long WKT `spatialReference` later.

    GDAL reads these pages as sent because `fields` or `fieldAliases` is near
    the start. Moving the 10 KB `spatialReference` ahead of it would push that
    marker out of GDAL's view, so the reference is the page converted exactly
    as sent.
    """
    wkt = CRS.from_epsg(4326).to_wkt(version="WKT1_GDAL").replace("WGS 84", "X" * 10_000, 1)
    members = {
        "fields": [{"name": "id", "type": "esriFieldTypeInteger"}],
        "fieldAliases": {"id": "id"},
        "features": _LONG_WKT_FEATURES,
        "spatialReference": {"wkt": wkt},
    }
    page = {key: members[key] for key in order}
    as_sent = _json_doc_to_table(page, exclude="geom", suffix=".json", con=con)
    # Without `fields`, GDAL types `id` as a string.
    assert [str(value) for value in as_sent.column("id").to_pylist()] == ["1"]

    _assert_same_table(_esrijson_page_to_table(page, con=con), as_sent)


# ---------------------------------------------------------------------------
# Complete offline extraction
# ---------------------------------------------------------------------------

SERVICE = (
    "https://services.example.com/arcgis/rest/services/ASIS_Shoreline_2005_2020/FeatureServer/1"
)
UTM_18N = 26918


def _layer_json(page: dict) -> dict:
    """Layer metadata matching the recorded page (WGS84 polylines)."""
    return {
        "id": 1,
        "name": "Shoreline",
        "type": "Feature Layer",
        "geometryType": page["geometryType"],
        "spatialReference": page["spatialReference"],
        "maxRecordCount": 2000,
        "objectIdField": "OBJECTID",
        "fields": page["fields"],
    }


def _extraction_pages(raw: bytes, wkid: int) -> tuple[dict, dict]:
    """A small page (OBJECTID 1) and the recorded page (OBJECTID 11), metadata-first.

    The small feature is derived: the first three positions of the recorded
    line, with its own ID. For UTM 18N both pages are derived by projecting the
    recorded WGS84 coordinates, as a server honouring ``outSR`` would.
    """
    large = json.loads(raw)
    feature = large["features"][0]
    small_feature = {
        "attributes": feature["attributes"] | {"OBJECTID": 1, "LinkID": "small"},
        "geometry": {"paths": [feature["geometry"]["paths"][0][:3]]},
    }
    small = {key: value for key, value in large.items() if key != "features"}
    small["features"] = [small_feature]
    if wkid != 4326:
        to_utm = Transformer.from_crs(4326, wkid, always_xy=True)
        for page in (small, large):
            page["spatialReference"] = {"wkid": wkid, "latestWkid": wkid}
            for item in page["features"]:
                item["geometry"] = {
                    "paths": [
                        [list(to_utm.transform(x, y)) for x, y in path]
                        for path in item["geometry"]["paths"]
                    ]
                }
    return small, large


def _install_service(monkeypatch, layer: dict, pages: list[dict]) -> FakeTransport:
    http = FakeTransport.install(monkeypatch)
    http.respond(lambda r: r.path.endswith("/FeatureServer/1"), json_reply(layer))
    http.respond(
        lambda r: r.params.get("returnCountOnly") == "true", json_reply({"count": len(pages)})
    )
    http.respond(
        lambda r: r.path.endswith("/query") and r.params.get("returnCountOnly") != "true",
        lambda request: json_reply(pages[int(request.params.get("resultOffset", 0))])(request),
    )
    return http


def test_extraction_reads_a_late_metadata_page_after_a_normal_one(
    shoreline_bytes, monkeypatch, tmp_path
):
    output_crs = f"EPSG:{UTM_18N}"
    small, large = _extraction_pages(shoreline_bytes, UTM_18N)
    layer = _layer_json(json.loads(shoreline_bytes))

    _install_service(monkeypatch, layer, [small, large])
    reference_file = tmp_path / "reference.parquet"
    convert_arcgis_to_geoparquet(SERVICE, str(reference_file), output_crs=output_crs, batch_size=1)

    features_first = {"features": large["features"]} | large
    http = _install_service(monkeypatch, layer, [small, features_first])
    out = tmp_path / "shoreline.parquet"
    convert_arcgis_to_geoparquet(SERVICE, str(out), output_crs=output_crs, batch_size=1)

    pages = [r for r in http.requests if r.path.endswith("/query") and "resultOffset" in r.params]
    assert [r.params["resultOffset"] for r in pages] == ["0", "1"]
    assert {(r.params["f"], r.params["outSR"]) for r in pages} == {("json", str(UTM_18N))}
    table = pq.read_table(out)
    assert table.column("OBJECTID").to_pylist() == [1, 11]
    assert table.equals(pq.read_table(reference_file), check_metadata=True)
    assert spec_problems(out) == []
    assert geo_block(out) == geo_block(reference_file)
    assert geo_block_crs_id(out) == {"authority": "EPSG", "code": UTM_18N}
