"""EsriJSON pages convert regardless of top-level member order (issue #501).

GDAL identifies an ESRIJSON file from the members near its start. When a large
``features`` array comes before ``geometryType``/``spatialReference``/``fields``,
it can pick the GeoJSON driver and fail. These tests use real DuckDB/GDAL
conversion of a recorded page, reordered in memory.
"""

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from geoparquet_io.core.arcgis import (
    _esrijson_page_to_table,
    _json_doc_to_table,
    _prepare_esrijson_for_gdal,
)
from geoparquet_io.core.duckdb_utils import get_duckdb_connection

SHORELINE = Path(__file__).parent / "data" / "arcgis_json" / "shoreline.esrijson.json.gz"
SHORELINE_SHA256 = "3c5adade08109e09059e30786fe241fcd1929b0d3926b5ab374f83aaedb939b5"

IDENTIFYING_KEYS = ("geometryType", "spatialReference", "fields", "objectIdFieldName")

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


@pytest.fixture(scope="module")
def reference(shoreline_bytes, con):
    """Metadata-first page converted without the EsriJSON page converter.

    Calling the shared converter directly keeps any preparation step inside
    `_esrijson_page_to_table` out of the reference, so a bug there cannot
    change both sides of the comparison.
    """
    page = json.loads(shoreline_bytes)
    metadata_first = {key: page[key] for key in IDENTIFYING_KEYS}
    metadata_first.update(page)
    table = _json_doc_to_table(metadata_first, exclude="geom", suffix=".json", con=con)

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
    assert table is not None
    assert table.schema.equals(reference.schema, check_metadata=True)
    assert [field.nullable for field in table.schema] == [
        field.nullable for field in reference.schema
    ]
    for name in reference.column_names:
        assert table.column(name).equals(reference.column(name)), name
    assert table.column("geometry").to_pylist() == reference.column("geometry").to_pylist()


def _small_page() -> dict:
    """Features-first page with values whose serialization is easy to disturb."""
    return {
        "features": [
            {
                "attributes": {"OBJECTID": 2**53 + 1, "name": 'quote " and \\ é', "note": None},
                "geometry": {"x": -0.0, "y": 1e-300, "z": 12.5},
            }
        ],
        "vendorInfo": {"source": "test", "tags": ["a", "b"]},
        "objectIdFieldName": "OBJECTID",
        "hasZ": True,
        "hasM": False,
        "fields": [
            {"name": "OBJECTID", "type": "esriFieldTypeOID"},
            {"name": "name", "type": "esriFieldTypeString", "length": 50},
            {"name": "note", "type": "esriFieldTypeString"},
        ],
        "exceededTransferLimit": True,
        "spatialReference": {"wkid": 102100, "latestWkid": 3857},
        "geometryType": "esriGeometryPoint",
    }


@pytest.mark.parametrize(
    ("order", "expected"),
    [
        (
            ORDERS["features_first"],
            [
                "geometryType",
                "spatialReference",
                "fields",
                "objectIdFieldName",
                "features",
                "uniqueIdField",
                "globalIdFieldName",
                "geometryProperties",
            ],
        ),
        (
            ORDERS["geometry_properties_before_features"],
            [
                "geometryType",
                "spatialReference",
                "fields",
                "objectIdFieldName",
                "uniqueIdField",
                "globalIdFieldName",
                "geometryProperties",
                "features",
            ],
        ),
        (
            ("fields", "geometryType", "features", "objectIdFieldName", "spatialReference")
            + ("uniqueIdField", "globalIdFieldName", "geometryProperties"),
            [
                "geometryType",
                "spatialReference",
                "fields",
                "objectIdFieldName",
                "features",
                "uniqueIdField",
                "globalIdFieldName",
                "geometryProperties",
            ],
        ),
    ],
    ids=["features_first", "geometry_properties_before_features", "interspersed"],
)
def test_prepare_puts_identifying_members_first(order, expected, shoreline_bytes):
    page = _reordered(shoreline_bytes, order)

    assert list(_prepare_esrijson_for_gdal(page)) == expected


def test_prepare_keeps_every_member_and_the_order_of_the_rest():
    page = _small_page()
    before = json.dumps(page)

    prepared = _prepare_esrijson_for_gdal(page)

    assert list(prepared) == [
        "geometryType",
        "spatialReference",
        "fields",
        "objectIdFieldName",
        "features",
        "vendorInfo",
        "hasZ",
        "hasM",
        "exceededTransferLimit",
    ]
    assert prepared == json.loads(before)
    assert json.dumps(page) == before


def test_prepare_reuses_nested_values():
    page = _small_page()

    prepared = _prepare_esrijson_for_gdal(page)

    assert prepared is not page
    for key, value in page.items():
        assert prepared[key] is value, key


def test_prepare_does_not_add_missing_identifying_members():
    page = _small_page()
    del page["spatialReference"], page["objectIdFieldName"]

    prepared = _prepare_esrijson_for_gdal(page)

    assert list(prepared)[:3] == ["geometryType", "fields", "features"]
    assert "spatialReference" not in prepared
    assert "objectIdFieldName" not in prepared


def test_prepare_leaves_metadata_first_order_alone(shoreline_bytes):
    page = json.loads(shoreline_bytes)
    page = {key: page[key] for key in IDENTIFYING_KEYS} | page

    assert json.dumps(_prepare_esrijson_for_gdal(page)) == json.dumps(page)


def test_prepare_is_idempotent():
    once = _prepare_esrijson_for_gdal(_small_page())

    assert json.dumps(_prepare_esrijson_for_gdal(once)) == json.dumps(once)


@pytest.mark.parametrize("page", [{}, {"features": []}, {"features": None}, {"geometryType": "x"}])
def test_esrijson_page_without_features_returns_none(page):
    before = json.dumps(page)

    assert _esrijson_page_to_table(page) is None
    assert json.dumps(page) == before
