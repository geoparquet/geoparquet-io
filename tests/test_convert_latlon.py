"""Lat/lon (and WKT) geometry from tabular sources: CSV and plain Parquet.

A Parquet file with coordinate columns but no geometry column used to ignore
``--lat-column``/``--lon-column`` entirely and fail with "No geometry column
detected". It now goes through the same tabular path CSV does, and both
formats auto-detect a wider range of coordinate column names.
"""

import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.convert import (
    _try_detect_latlon_columns,
    convert_to_geoparquet,
    read_spatial_to_arrow,
)
from geoparquet_io.core.exceptions import GeoParquetError, InvalidParameterError
from geoparquet_io.core.geo_metadata import parse_geo_metadata


def _write_geoparquet(path, table, geom_column="geometry"):
    """Write ``table`` as GeoParquet so DuckDB reads its WKB column as GEOMETRY."""
    geo = {
        "version": "1.1.0",
        "primary_column": geom_column,
        "columns": {geom_column: {"encoding": "WKB", "geometry_types": ["Point"]}},
    }
    meta = {**(table.schema.metadata or {}), b"geo": json.dumps(geo).encode()}
    pq.write_table(table.replace_schema_metadata(meta), path)
    return str(path)


def _columns(*specs):
    """A DuckDB-style ``description``: (name, type) pairs, numeric by default."""
    return [(spec, "DOUBLE") if isinstance(spec, str) else spec for spec in specs]


class TestLatLonNameDetection:
    """``_try_detect_latlon_columns`` over a result description."""

    @pytest.mark.parametrize(
        ("names", "expected"),
        [
            (("LAT", "LON"), ("LAT", "LON")),
            (("latitude", "longitude"), ("latitude", "longitude")),
            (("Lat", "Lng"), ("Lat", "Lng")),
            (("lat", "long"), ("lat", "long")),
            (("y", "x"), ("y", "x")),
            (("decimalLatitude", "decimalLongitude"), ("decimalLatitude", "decimalLongitude")),
            (("lat_dd", "lon_dd"), ("lat_dd", "lon_dd")),
            (("Latitude (deg)", "Longitude (deg)"), ("Latitude (deg)", "Longitude (deg)")),
            (("location.lat", "location.lon"), ("location.lat", "location.lon")),
            (("LATITUDE_WGS84", "LONGITUDE_WGS84"), ("LATITUDE_WGS84", "LONGITUDE_WGS84")),
        ],
    )
    def test_detects_pair(self, names, expected):
        assert _try_detect_latlon_columns(_columns("id", *names)) == expected

    def test_exact_names_beat_affixed(self):
        cols = _columns("pickup_latitude", "pickup_longitude", "lat", "lon")
        assert _try_detect_latlon_columns(cols) == ("lat", "lon")

    def test_affixes_must_match(self):
        cols = _columns("pickup_latitude", "dropoff_longitude")
        assert _try_detect_latlon_columns(cols) == (None, None)

    def test_ambiguous_pairs_take_first_and_warn(self, caplog):
        cols = _columns(
            "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude"
        )
        assert _try_detect_latlon_columns(cols) == ("pickup_latitude", "pickup_longitude")
        assert "dropoff_latitude" in caplog.text

    def test_embedded_substring_is_not_a_token(self):
        # "GCLONG01" contains "long" but is not a longitude column.
        cols = _columns("LAT", "GCLONG01", "PLATFORM")
        assert _try_detect_latlon_columns(cols) == (None, None)

    def test_affixed_match_requires_numeric(self):
        cols = _columns(("lat_note", "VARCHAR"), ("lon_note", "VARCHAR"))
        assert _try_detect_latlon_columns(cols) == (None, None)

    def test_x_y_only_as_exact_names(self):
        cols = _columns("pos_x", "pos_y")
        assert _try_detect_latlon_columns(cols) == (None, None)


@pytest.fixture
def latlon_parquet(tmp_path):
    """A plain Parquet file (no geo metadata) carrying LAT/LON columns."""
    table = pa.table(
        {
            "id": [1, 2, 3, 4],
            "GCLONG01": [7, 7, 7, 7],
            "LAT": [10.0, -20.5, 45.25, None],
            "LON": [-100.0, 30.0, 120.5, 5.0],
            "value": [1.5, 2.5, 3.5, 4.5],
        }
    )
    path = tmp_path / "obs.parquet"
    pq.write_table(table, path)
    return str(path)


def _geo(path):
    metadata, _ = get_parquet_metadata(path, verbose=False)
    return parse_geo_metadata(metadata, verbose=False)


def _points(path):
    con = duckdb.connect()
    try:
        con.execute("LOAD spatial")
        return con.execute(
            f"SELECT id, ST_X(geometry), ST_Y(geometry) FROM read_parquet('{path}') ORDER BY id"
        ).fetchall()
    finally:
        con.close()


class TestParquetLatLonConvert:
    def test_explicit_columns(self, latlon_parquet, tmp_path):
        out = str(tmp_path / "out.parquet")
        convert_to_geoparquet(latlon_parquet, out, lat_column="LAT", lon_column="LON")

        geo = _geo(out)
        assert geo is not None
        assert geo["primary_column"] == "geometry"
        assert _points(out) == [
            (1, -100.0, 10.0),
            (2, 30.0, -20.5),
            (3, 120.5, 45.25),
            (4, None, None),  # missing coordinate: row kept, NULL geometry
        ]
        names = pq.read_schema(out).names
        assert "LAT" not in names and "LON" not in names
        assert {"id", "GCLONG01", "value", "bbox"} <= set(names)

    def test_auto_detected_without_flags(self, latlon_parquet, tmp_path):
        out = str(tmp_path / "out.parquet")
        convert_to_geoparquet(latlon_parquet, out)
        assert _geo(out)["primary_column"] == "geometry"
        assert len(_points(out)) == 4

    def test_skip_hilbert_and_v2(self, latlon_parquet, tmp_path):
        out = str(tmp_path / "out.parquet")
        convert_to_geoparquet(latlon_parquet, out, skip_hilbert=True, geoparquet_version="2.0")
        assert "bbox" not in pq.read_schema(out).names
        assert len(_points(out)) == 4

    def test_misspelled_column_suggests_match(self, latlon_parquet, tmp_path):
        out = str(tmp_path / "out.parquet")
        with pytest.raises(InvalidParameterError, match="Did you mean 'LON'"):
            convert_to_geoparquet(latlon_parquet, out, lat_column="LAT", lon_column="LONG")

    def test_out_of_range_rejected(self, tmp_path):
        path = tmp_path / "bad.parquet"
        pq.write_table(pa.table({"lat": [100.0], "lon": [0.0]}), path)
        with pytest.raises(InvalidParameterError, match="(?i)latitude"):
            convert_to_geoparquet(str(path), str(tmp_path / "out.parquet"))

    def test_existing_geometry_refuses_latlon(self, tmp_path):
        table = pa.table(
            {
                "geometry": [shapely.to_wkb(shapely.Point(1, 2))],
                "lat": [2.0],
                "lon": [1.0],
            }
        )
        path = _write_geoparquet(tmp_path / "geo.parquet", table)
        with pytest.raises(InvalidParameterError, match="already has a geometry column"):
            convert_to_geoparquet(
                path, str(tmp_path / "out.parquet"), lat_column="lat", lon_column="lon"
            )

    def test_existing_geometry_ignores_latlon_names(self, tmp_path):
        """Auto-detection never overrides a real geometry column."""
        table = pa.table(
            {
                "geometry": [shapely.to_wkb(shapely.Point(1, 2))],
                "lat": [50.0],
                "lon": [60.0],
            }
        )
        path = _write_geoparquet(tmp_path / "geo.parquet", table)
        out = str(tmp_path / "out.parquet")
        convert_to_geoparquet(path, out)
        assert _points_from(out, "geometry") == [(1.0, 2.0)]
        assert {"lat", "lon"} <= set(pq.read_schema(out).names)

    def test_wkt_column_in_parquet(self, tmp_path):
        path = tmp_path / "wkt.parquet"
        pq.write_table(pa.table({"id": [1], "wkt": ["POINT (3 4)"]}), path)
        out = str(tmp_path / "out.parquet")
        convert_to_geoparquet(str(path), out)
        assert _points_from(out, "geometry") == [(3.0, 4.0)]

    def test_crs_allowed_for_tabular_parquet(self, tmp_path):
        path = tmp_path / "wkt.parquet"
        pq.write_table(pa.table({"id": [1], "wkt": ["POINT (500000 4000000)"]}), path)
        out = str(tmp_path / "out.parquet")
        convert_to_geoparquet(str(path), out, crs="EPSG:32610")
        crs = _geo(out)["columns"]["geometry"]["crs"]
        assert crs["id"]["code"] == 32610

    def test_plain_parquet_error_names_the_flags(self, tmp_path):
        path = tmp_path / "plain.parquet"
        pq.write_table(pa.table({"id": [1], "value": [2.0]}), path)
        with pytest.raises(GeoParquetError, match="--lat-column"):
            convert_to_geoparquet(str(path), str(tmp_path / "out.parquet"))

    def test_cli_default_group(self, latlon_parquet, tmp_path):
        out = str(tmp_path / "out.parquet")
        result = CliRunner().invoke(
            cli, ["convert", "--lat-column", "LAT", "--lon-column", "LON", latlon_parquet, out]
        )
        assert result.exit_code == 0, result.output
        assert _geo(out) is not None


def _points_from(path, column):
    con = duckdb.connect()
    try:
        con.execute("LOAD spatial")
        return con.execute(
            f"SELECT ST_X({column}), ST_Y({column}) FROM read_parquet('{path}')"
        ).fetchall()
    finally:
        con.close()


class TestParquetLatLonArrow:
    def test_read_spatial_to_arrow(self, latlon_parquet):
        table, crs, geom_col = read_spatial_to_arrow(latlon_parquet)
        assert geom_col == "geometry"
        assert crs is None
        assert "LAT" not in table.column_names
        assert table.num_rows == 3  # the Arrow path drops rows with no coordinate, as for CSV

    def test_api_convert_explicit(self, latlon_parquet):
        table = gpio.convert(latlon_parquet, lat_column="LAT", lon_column="LON")
        assert table.geometry_column == "geometry"
        assert table.num_rows == 3


class TestCsvAffixedNames:
    def test_decimal_latitude_csv(self, tmp_path):
        path = tmp_path / "gbif.csv"
        path.write_text("id,decimalLatitude,decimalLongitude\n1,10.5,20.5\n2,-5.0,7.25\n")
        out = str(tmp_path / "out.parquet")
        convert_to_geoparquet(str(path), out)
        assert _points(out) == [(1, 20.5, 10.5), (2, 7.25, -5.0)]


def _rows(path, column):
    con = duckdb.connect()
    try:
        return con.execute(f"SELECT {column} FROM read_parquet('{path}')").fetchall()
    finally:
        con.close()


class TestParquetLatLonMeetsTheRestOfConvert:
    """The tabular Parquet path and the conversion rules it inherits."""

    def test_bbox_name_collision_resolves(self, tmp_path):
        """A source column named 'bbox' is not overwritten by the computed one (#1079)."""
        table = pa.table({"bbox": ["tile-a", "tile-b"], "lat": [1.0, 2.0], "lon": [3.0, 4.0]})
        path = tmp_path / "collide.parquet"
        pq.write_table(table, path)
        out = str(tmp_path / "out.parquet")
        convert_to_geoparquet(str(path), out)

        names = pq.read_schema(out).names
        assert "bbox" in names  # the source's own string column, untouched
        computed = _geo(out)["columns"]["geometry"]["covering"]["bbox"]["xmin"][0]
        assert computed != "bbox"
        assert computed in names
        assert _rows(out, "bbox")[0] == ("tile-a",)

    def test_hilbert_ordering_is_applied(self, tmp_path):
        """The rows come out spatially ordered, as for every other input."""
        lats = [10.0, -40.0, 11.0, -41.0, 10.5]
        lons = [20.0, -80.0, 21.0, -81.0, 20.5]
        path = tmp_path / "order.parquet"
        pq.write_table(pa.table({"id": list(range(5)), "lat": lats, "lon": lons}), path)
        out = str(tmp_path / "out.parquet")
        convert_to_geoparquet(str(path), out)

        order = [row[0] for row in _rows(out, "id")]
        assert order != list(range(5)), "rows were not reordered"
        assert sorted(order) == list(range(5)), "rows were lost or duplicated"

    def test_rows_without_a_coordinate_sort_last(self, tmp_path):
        """A row with no coordinate keeps its attributes and sorts last (#649)."""
        path = tmp_path / "gaps.parquet"
        pq.write_table(
            pa.table({"id": [1, 2, 3], "lat": [10.0, None, 12.0], "lon": [20.0, 30.0, 22.0]}),
            path,
        )
        out = str(tmp_path / "out.parquet")
        convert_to_geoparquet(str(path), out)
        assert [row[0] for row in _rows(out, "id")][-1] == 2
