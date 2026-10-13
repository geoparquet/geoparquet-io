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

from geoparquet_io.core.arcgis import _esrijson_page_to_table, _json_doc_to_table
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
