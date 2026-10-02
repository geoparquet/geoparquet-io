"""A tabular source honours the requested geometry column name (#1176).

``_tabular_geometry_to_arrow`` resolved the built geometry's name from a
hard-coded ``"geometry"`` base and never saw ``read_spatial_to_arrow``'s
``geometry_column`` argument, so the documented
``gpio.convert('pts.csv', lat_column=..., lon_column=..., geometry_column='geom')``
was a silent no-op: the Table came back with a column called ``geometry``.

The contract across the two source kinds: the parameter is a *request*, and the
returned Table carries the name actually used. A tabular source can honour it,
because it aliases the geometry it builds; a spatial source cannot, because both
of its reads hard-code ``ST_AsWKB(...) AS geometry``, so it reports that.
"""

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.api.table import convert


@pytest.fixture
def latlon_csv(tmp_path):
    path = tmp_path / "pts.csv"
    path.write_text("id,lat,lon\n1,1.0,2.0\n2,3.0,4.0\n")
    return str(path)


@pytest.fixture
def wkt_csv(tmp_path):
    path = tmp_path / "wkt.csv"
    path.write_text("id,the_wkt\n1,POINT (1 2)\n2,POINT (3 4)\n")
    return str(path)


def _primary_column(path):
    geo = json.loads(pq.read_schema(str(path)).metadata[b"geo"].decode("utf-8"))
    return geo["primary_column"]


class TestRequestedNameIsHonoured:
    def test_latlon_source(self, latlon_csv):
        result = convert(latlon_csv, lat_column="lat", lon_column="lon", geometry_column="geom")

        assert result.geometry_column == "geom"
        assert "geom" in result.table.column_names
        assert "geometry" not in result.table.column_names

    def test_wkt_source(self, wkt_csv):
        result = convert(wkt_csv, wkt_column="the_wkt", geometry_column="geom")

        assert result.geometry_column == "geom"
        assert "geom" in result.table.column_names

    def test_the_written_file_declares_it(self, latlon_csv, tmp_path):
        output = tmp_path / "out.parquet"

        convert(latlon_csv, lat_column="lat", lon_column="lon", geometry_column="geom").write(
            str(output)
        )

        assert _primary_column(output) == "geom"
        assert "geom" in pq.read_schema(str(output)).names

    def test_the_default_is_still_geometry(self, latlon_csv):
        result = convert(latlon_csv, lat_column="lat", lon_column="lon")

        assert result.geometry_column == "geometry"


class TestRequestedNameCollides:
    def test_a_carried_column_of_that_name_moves_the_built_one_aside(self, tmp_path, caplog):
        """``geom`` is a label the source carries through; the built geometry moves."""
        import logging

        source = tmp_path / "labels.csv"
        source.write_text("geom,lat,lon\nfirst,1.0,2.0\n")

        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            result = convert(
                str(source), lat_column="lat", lon_column="lon", geometry_column="geom"
            )

        assert result.geometry_column == "geom_1"
        assert result.table.column("geom").to_pylist() == ["first"]
        assert "geom" in caplog.text
        assert "geom_1" in caplog.text
