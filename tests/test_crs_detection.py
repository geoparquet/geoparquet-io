"""
Tests for the CRS-detection paths in core/crs_utils.py.

These cover detection from spatial files (GPKG, Shapefile), the FileGDB
workaround's guard branches, and the GeoArrow field-metadata carrier —
all against committed fixtures or hand-built Arrow tables, fully offline.
"""

import json

import pyarrow as pa
import pytest

from geoparquet_io.core.crs_utils import (
    _crs_from_geoarrow_field,
    _detect_crs_from_filegdb,
    crs_string_from_table,
    detect_crs_from_spatial_file,
)
from geoparquet_io.core.duckdb_utils import get_duckdb_connection


@pytest.fixture(scope="module")
def spatial_con():
    con = get_duckdb_connection(load_spatial=True)
    yield con
    con.close()


class TestDetectCrsFromSpatialFile:
    def test_gpkg_epsg_4326(self, spatial_con):
        crs = detect_crs_from_spatial_file("tests/data/buildings_test.gpkg", spatial_con)
        assert crs["id"] == {"authority": "EPSG", "code": 4326}

    def test_gpkg_projected_crs(self, spatial_con):
        crs = detect_crs_from_spatial_file(
            "tests/data/buildings_test_6933.gpkg", spatial_con, verbose=True
        )
        assert crs["id"] == {"authority": "EPSG", "code": 6933}

    def test_shapefile(self, spatial_con):
        crs = detect_crs_from_spatial_file("tests/data/buildings_test.shp", spatial_con)
        assert crs["id"] == {"authority": "EPSG", "code": 4326}

    def test_nonexistent_file_returns_none(self, spatial_con):
        crs = detect_crs_from_spatial_file("does/not/exist.geojson", spatial_con, verbose=True)
        assert crs is None


class TestDetectCrsFromFilegdb:
    """The FileGDB workaround's guard branches, without a real FileGDB."""

    def test_nonexistent_dir_returns_none(self, spatial_con):
        assert _detect_crs_from_filegdb("does/not/exist.gdb", spatial_con) is None

    def test_empty_dir_returns_none(self, tmp_path, spatial_con):
        gdb = tmp_path / "empty.gdb"
        gdb.mkdir()
        assert _detect_crs_from_filegdb(str(gdb), spatial_con) is None

    # NOTE: the "ST_Read_Meta fails on a .gdbtable" branch is deliberately not
    # exercised — feeding ST_Read_Meta junk .gdbtable bytes segfaults the
    # DuckDB spatial extension (GDAL crash), so there is no safe offline way
    # to reach that except/continue.

    def test_spatial_file_dispatches_gdb_fallback(self, tmp_path, spatial_con):
        """detect_crs_from_spatial_file falls through to the .gdb workaround."""
        gdb = tmp_path / "empty.gdb"
        gdb.mkdir()
        assert detect_crs_from_spatial_file(str(gdb), spatial_con, verbose=True) is None


def _table_with_geoarrow_crs(crs_value) -> pa.Table:
    """Arrow table whose geometry field carries GeoArrow extension metadata."""
    ext_meta = json.dumps({"crs": crs_value})
    field = pa.field(
        "geometry",
        pa.binary(),
        metadata={
            b"ARROW:extension:name": b"geoarrow.wkb",
            b"ARROW:extension:metadata": ext_meta.encode(),
        },
    )
    schema = pa.schema([pa.field("id", pa.int64()), field])
    return pa.table({"id": [1], "geometry": [b"\x00"]}, schema=schema)


class TestCrsFromGeoarrowField:
    def test_reads_crs_from_extension_metadata(self):
        crs = {"id": {"authority": "EPSG", "code": 6933}}
        table = _table_with_geoarrow_crs(crs)
        assert _crs_from_geoarrow_field(table, "geometry") == crs

    def test_missing_column_returns_none(self):
        table = pa.table({"id": [1]})
        assert _crs_from_geoarrow_field(table, "geometry") is None

    def test_field_without_metadata_returns_none(self):
        table = pa.table({"geometry": [b"\x00"]})
        assert _crs_from_geoarrow_field(table, "geometry") is None

    def test_malformed_extension_metadata_returns_none(self):
        field = pa.field(
            "geometry", pa.binary(), metadata={b"ARROW:extension:metadata": b"{not json"}
        )
        table = pa.table({"geometry": [b"\x00"]}, schema=pa.schema([field]))
        assert _crs_from_geoarrow_field(table, "geometry") is None


class TestCrsStringFromTable:
    def test_geoarrow_crs_becomes_transform_string(self):
        table = _table_with_geoarrow_crs({"id": {"authority": "EPSG", "code": 6933}})
        assert crs_string_from_table(table, "geometry") == "EPSG:6933"

    def test_crs_less_table_returns_none(self):
        table = pa.table({"geometry": [b"\x00"]})
        assert crs_string_from_table(table, "geometry") is None

    def test_geo_metadata_crs_wins(self):
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {"encoding": "WKB", "crs": {"id": {"authority": "EPSG", "code": 3857}}}
            },
        }
        table = pa.table({"geometry": [b"\x00"]})
        table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
        assert crs_string_from_table(table, "geometry") == "EPSG:3857"
