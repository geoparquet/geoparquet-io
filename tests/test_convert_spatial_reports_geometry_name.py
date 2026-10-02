"""The spatial read reports the geometry column it actually emitted (#1176).

Both spatial SQL builders in ``core/convert.py`` -- the normal read and the
linearized retry -- hard-code ``ST_AsWKB(...) AS geometry``, but the non-tabular
branch of ``read_spatial_to_arrow`` returned the *requested*
``geometry_column``. A caller asking for ``geom`` on a GeoPackage got the name
``geom`` back for a table whose column is ``geometry``, so geometry repair was
silently skipped (the repair helper returns the table unchanged when the column
is absent) and the ``Table`` handed back failed ``.write()`` with
``InvalidParameterError``.

The parameter is a request; the returned name is what the read could use.
"""

import json

import duckdb
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.convert import read_spatial_to_arrow

# Two self-intersecting "bowtie" polygons (invalid) and one valid square --
# the fixture shape tests/test_geometry_repair_integration.py uses.
_INVALID_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"id": 1},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]]],
            },
        },
        {
            "type": "Feature",
            "properties": {"id": 2},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [2, 2], [2, 0], [0, 2], [0, 0]]],
            },
        },
        {
            "type": "Feature",
            "properties": {"id": 3},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
        },
    ],
}


@pytest.fixture
def invalid_geojson(tmp_path):
    path = tmp_path / "invalid.geojson"
    path.write_text(json.dumps(_INVALID_GEOJSON))
    return str(path)


@pytest.fixture
def gpkg(test_data_dir):
    return str(test_data_dir / "multilayer_test.gpkg")


def _invalid_count(table, geometry_column):
    """Rows of an in-memory WKB column that ST_IsValid rejects."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    try:
        con.register("_src", table)
        return con.execute(
            f'SELECT COUNT(*) FROM _src WHERE NOT ST_IsValid(ST_GeomFromWKB("{geometry_column}"))'
        ).fetchone()[0]
    finally:
        con.close()


class TestSpatialReadReportsTheNameItEmitted:
    def test_gpkg_reports_geometry_not_the_requested_name(self, gpkg):
        _table, _crs, geometry_column = read_spatial_to_arrow(gpkg, geometry_column="geom")
        assert geometry_column == "geometry"

    def test_the_reported_name_is_a_column_of_the_table(self, gpkg):
        table, _crs, geometry_column = read_spatial_to_arrow(gpkg, geometry_column="geom")
        assert geometry_column in table.column_names, (
            f"reported {geometry_column!r}, table has {table.column_names}"
        )

    def test_repair_still_runs_under_a_requested_name(self, invalid_geojson):
        """The repair pass keys off the reported name, so it must be the real one."""
        table, _crs, geometry_column = read_spatial_to_arrow(
            invalid_geojson, geometry_column="geom", repair_geometry=True
        )
        assert _invalid_count(table, geometry_column) == 0, (
            "invalid geometries survived: the repair pass bound to the wrong column"
        )


class TestConvertApiHandsBackAWritableTable:
    def test_write_succeeds_and_declares_the_real_column(self, gpkg, tmp_path):
        from geoparquet_io.api.table import convert

        output = tmp_path / "out.parquet"
        convert(gpkg, geometry_column="geom").write(str(output))

        geo = json.loads(pq.read_schema(str(output)).metadata[b"geo"].decode("utf-8"))
        assert geo["primary_column"] == "geometry"
