"""``--bbox``/``--geometry``/``--where`` on ``gpio convert`` geopackage/flatgeobuf/csv (#1308).

The filter clause comes from extract's covering-aware builder
(``core.extract.build_spatial_filter``); these tests pin the emitted SQL
(golden), the equivalence with the extract-then-convert two-step, and the
behaviors folded into ``write_gdal_format`` from portolan-registry-qgis's
``parquet_query.py`` (Apache-2.0): the staged temp table, geometry-type
promotion, ``fid`` collision handling, and table-only GeoPackage output.
"""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import format_writers
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.exceptions import InvalidParameterError, ValidationError
from geoparquet_io.core.format_writers import write_csv, write_flatgeobuf, write_geopackage

TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES = str(TEST_DATA_DIR / "places_test.parquet")
FIELDS_5070 = str(TEST_DATA_DIR / "fields_gpq2_5070_brotli.parquet")

# A box over the middle of places_test.parquet (full extent is roughly
# -0.99, 9.80 .. 1.48, 12.39), keeping a proper subset of the 766 rows.
BBOX = "-0.5,10.0,0.5,11.0"
BBOX_TUPLE = (-0.5, 10.0, 0.5, 11.0)
BBOX_ROWS = 94
TOTAL_ROWS = 766
BBOX_AS_WKT = "POLYGON((-0.5 10.0, 0.5 10.0, 0.5 11.0, -0.5 11.0, -0.5 10.0))"


def _gpkg_table(db):
    """The one layer's table name (DuckDB's GDAL writer names it after the file)."""
    return db.execute("SELECT table_name FROM gpkg_contents").fetchone()[0]


def _gpkg_values(path, column="fsq_place_id"):
    with closing(sqlite3.connect(path)) as db:
        table = _gpkg_table(db)
        return [row[0] for row in db.execute(f'SELECT "{column}" FROM "{table}"')]


def _gpkg_columns(path):
    with closing(sqlite3.connect(path)) as db:
        table = _gpkg_table(db)
        return [row[1] for row in db.execute(f'PRAGMA table_info("{table}")')]


def _read_layer_count(path):
    con = get_duckdb_connection(load_spatial=True)
    try:
        return con.execute(f"SELECT count(*) FROM ST_Read('{path}')").fetchone()[0]
    finally:
        con.close()


class _RecordingConnection:
    """Delegates to a real DuckDB connection, keeping every executed statement."""

    def __init__(self, real, statements):
        self._real = real
        self._statements = statements

    def execute(self, query, *args, **kwargs):
        self._statements.append(query)
        return self._real.execute(query, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._real, name)


@pytest.fixture
def recorded_statements(monkeypatch):
    """Every SQL statement the format writers execute, in order."""
    statements = []
    real_factory = format_writers.get_duckdb_connection

    def _factory(*args, **kwargs):
        return _RecordingConnection(real_factory(*args, **kwargs), statements)

    monkeypatch.setattr(format_writers, "get_duckdb_connection", _factory)
    return statements


@pytest.fixture
def places_no_covering(tmp_path):
    """places_test without its bbox column, so a bbox filter must hit the geometry."""
    table = pq.read_table(PLACES).drop_columns(["bbox"])
    path = tmp_path / "places_no_covering.parquet"
    pq.write_table(table, path)
    return str(path)


@pytest.fixture
def plain_parquet(tmp_path):
    """A plain Parquet file with no geometry column at all."""
    table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    path = tmp_path / "plain.parquet"
    pq.write_table(table, path)
    return str(path)


# ---------------------------------------------------------------------------
# Golden SQL: the statements a bbox-filtered conversion emits
# ---------------------------------------------------------------------------


class TestGoldenSQL:
    def test_bbox_prunes_with_covering_column(self, tmp_path, recorded_statements):
        out = tmp_path / "out.gpkg"
        write_geopackage(PLACES, str(out), bbox=BBOX)

        staged = [s for s in recorded_statements if "CREATE OR REPLACE TEMP TABLE" in s]
        assert len(staged) == 1, recorded_statements
        sql = staged[0]
        # Four-sided overlap on the quoted covering column, no geometry scan.
        assert '"bbox".xmax >= -0.5 AND "bbox".xmin <= 0.5' in sql
        assert '"bbox".ymax >= 10.0 AND "bbox".ymin <= 11.0' in sql
        assert "ST_Intersects" not in sql
        # The GDAL COPY reads the staged table, not the source file.
        copies = [s for s in recorded_statements if "FORMAT GDAL" in s]
        assert len(copies) == 1
        assert "__gpio_convert_staged" in copies[0]
        assert "read_parquet" not in copies[0]
        assert out.exists()

    def test_bbox_without_covering_uses_intersects(
        self, tmp_path, places_no_covering, recorded_statements
    ):
        out = tmp_path / "out.gpkg"
        write_geopackage(places_no_covering, str(out), bbox=BBOX)

        staged = [s for s in recorded_statements if "CREATE OR REPLACE TEMP TABLE" in s]
        assert len(staged) == 1
        assert 'ST_Intersects("geometry", ST_MakeEnvelope(-0.5, 10.0, 0.5, 11.0))' in staged[0]

    def test_where_is_appended_as_its_own_condition(self, tmp_path, recorded_statements):
        out = tmp_path / "out.gpkg"
        write_geopackage(PLACES, str(out), bbox=BBOX, where="name IS NOT NULL")

        staged = [s for s in recorded_statements if "CREATE OR REPLACE TEMP TABLE" in s]
        assert ") AND (name IS NOT NULL)" in staged[0]

    def test_csv_filter_lands_in_the_copy_select(self, tmp_path, recorded_statements):
        out = tmp_path / "out.csv"
        write_csv(PLACES, str(out), bbox=BBOX, where="name IS NOT NULL")

        copies = [s for s in recorded_statements if "COPY" in s and "read_parquet" in s]
        assert len(copies) == 1
        sql = copies[0]
        assert '"bbox".xmax >= -0.5 AND "bbox".xmin <= 0.5' in sql
        assert ") AND (name IS NOT NULL)" in sql

    def test_unfiltered_conversion_emits_no_where(self, tmp_path, recorded_statements):
        out = tmp_path / "out.gpkg"
        write_geopackage(PLACES, str(out))

        staged = [s for s in recorded_statements if "CREATE OR REPLACE TEMP TABLE" in s]
        assert len(staged) == 1
        assert "WHERE" not in staged[0].upper().replace("WKB_GEOMETRY", "")


# ---------------------------------------------------------------------------
# Equivalence with the extract-then-convert two-step
# ---------------------------------------------------------------------------


class TestEquivalence:
    def test_one_step_equals_extract_then_convert(self, tmp_path):
        runner = CliRunner()
        one = tmp_path / "one.gpkg"
        result = runner.invoke(cli, ["convert", "geopackage", PLACES, str(one), "--bbox", BBOX])
        assert result.exit_code == 0, result.output

        mid = tmp_path / "mid.parquet"
        result = runner.invoke(cli, ["extract", PLACES, str(mid), "--bbox", BBOX])
        assert result.exit_code == 0, result.output
        two = tmp_path / "two.gpkg"
        result = runner.invoke(cli, ["convert", "geopackage", str(mid), str(two)])
        assert result.exit_code == 0, result.output

        assert sorted(_gpkg_values(one)) == sorted(_gpkg_values(two))
        assert len(_gpkg_values(one)) == BBOX_ROWS

    def test_geometry_filter_matches_bbox_for_points(self, tmp_path):
        boxed = tmp_path / "boxed.gpkg"
        write_geopackage(PLACES, str(boxed), geometry=BBOX_AS_WKT)
        # Points: intersecting the box polygon selects the same features the
        # bbox overlap does.
        assert len(_gpkg_values(boxed)) == BBOX_ROWS


# ---------------------------------------------------------------------------
# Filter semantics across the three formats
# ---------------------------------------------------------------------------


class TestFilterSemantics:
    def test_flatgeobuf_bbox(self, tmp_path):
        out = tmp_path / "out.fgb"
        write_flatgeobuf(PLACES, str(out), bbox=BBOX)
        assert _read_layer_count(out) == BBOX_ROWS

    def test_csv_where(self, tmp_path):
        out = tmp_path / "out.csv"
        write_csv(PLACES, str(out), where="name LIKE 'A%'")
        lines = Path(out).read_text(encoding="utf-8").strip().splitlines()
        assert 1 < len(lines) < TOTAL_ROWS  # header plus a proper subset

    def test_csv_bbox(self, tmp_path):
        out = tmp_path / "out.csv"
        write_csv(PLACES, str(out), bbox=BBOX)
        lines = Path(out).read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == BBOX_ROWS + 1  # header

    def test_bbox_accepts_a_tuple(self, tmp_path):
        out = tmp_path / "out.gpkg"
        write_geopackage(PLACES, str(out), bbox=BBOX_TUPLE)
        assert len(_gpkg_values(out)) == BBOX_ROWS

    def test_bbox_requires_a_geometry_column(self, plain_parquet, tmp_path):
        with pytest.raises(InvalidParameterError, match="geometry column"):
            write_geopackage(plain_parquet, str(tmp_path / "out.gpkg"), bbox=BBOX)

    def test_csv_bbox_requires_a_geometry_column(self, plain_parquet, tmp_path):
        with pytest.raises(InvalidParameterError, match="geometry column"):
            write_csv(plain_parquet, str(tmp_path / "out.csv"), bbox=BBOX)

    def test_reversed_bbox_is_rejected(self, tmp_path):
        with pytest.raises(InvalidParameterError, match="reversed"):
            write_geopackage(PLACES, str(tmp_path / "out.gpkg"), bbox="0.5,10.0,-0.5,11.0")

    def test_non_numeric_bbox_sequence_is_rejected(self, tmp_path):
        with pytest.raises(InvalidParameterError, match="numeric"):
            write_geopackage(PLACES, str(tmp_path / "out.gpkg"), bbox=(-0.5, 10.0, None, 11.0))

    def test_multi_statement_where_is_rejected(self, tmp_path):
        with pytest.raises(ValidationError):
            write_csv(
                PLACES,
                str(tmp_path / "out.csv"),
                where="1=1; COPY (SELECT 1) TO 'pwned.csv'",
            )


# ---------------------------------------------------------------------------
# Donor regressions (portolan-registry-qgis parquet_query.py, Apache-2.0)
# ---------------------------------------------------------------------------


class TestDonorRegressions:
    def test_filtered_gpkg_keeps_source_srs(self, tmp_path):
        out = tmp_path / "fields.gpkg"
        write_geopackage(FIELDS_5070, str(out), where="1=1")
        with closing(sqlite3.connect(out)) as db:
            srs = {row[0] for row in db.execute("SELECT srs_id FROM gpkg_geometry_columns")}
        assert srs == {5070}

    def test_mixed_single_and_multi_polygons_promote(self, tmp_path):
        source = _mixed_polygon_parquet(tmp_path)
        out = tmp_path / "mixed.gpkg"
        write_geopackage(source, str(out))
        con = get_duckdb_connection(load_spatial=True)
        try:
            types = {
                row[0]
                for row in con.execute(
                    f"SELECT DISTINCT ST_GeometryType(geom)::VARCHAR FROM ST_Read('{out}')"
                ).fetchall()
            }
        finally:
            con.close()
        assert types == {"MULTIPOLYGON"}

    def test_source_fid_column_survives(self, tmp_path):
        table = pq.read_table(PLACES)
        # Duplicate values: unusable as GeoPackage feature ids, so the write
        # only survives when the feature id moves aside.
        table = table.append_column("fid", pa.array([7] * table.num_rows, type=pa.int64()))
        source = tmp_path / "places_fid.parquet"
        pq.write_table(table, source)

        out = tmp_path / "fid.gpkg"
        write_geopackage(str(source), str(out), bbox=BBOX)
        columns = _gpkg_columns(out)
        assert "fid" in columns
        assert set(_gpkg_values(out, column="fid")) == {7}
        assert len(_gpkg_values(out)) == BBOX_ROWS

    def test_source_geom_attribute_survives(self, tmp_path):
        """GDAL names the written geometry column 'geom' and silently drops a
        source attribute of that name unless the write steps around it."""
        table = pq.read_table(PLACES)
        table = table.append_column("geom", pa.array(["keep"] * table.num_rows))
        source = tmp_path / "places_geom.parquet"
        pq.write_table(table, source)

        out = tmp_path / "geomcol.gpkg"
        write_geopackage(str(source), str(out))
        assert "geom" in _gpkg_columns(out)
        assert set(_gpkg_values(out, column="geom")) == {"keep"}

    def test_gpkg_without_geometry_is_an_attribute_table(self, plain_parquet, tmp_path):
        out = tmp_path / "table.gpkg"
        write_geopackage(plain_parquet, str(out))
        with closing(sqlite3.connect(out)) as db:
            data_type = db.execute("SELECT data_type FROM gpkg_contents").fetchone()[0]
            geometry_rows = db.execute("SELECT count(*) FROM gpkg_geometry_columns").fetchone()[0]
        assert data_type == "attributes"
        assert geometry_rows == 0
        assert "geom" not in _gpkg_columns(out)
        assert _gpkg_values(out, column="name") == ["a", "b", "c"]

    def test_bbox_filter_survives_a_list_column(self, tmp_path):
        table = pq.read_table(PLACES)
        tags = pa.array([["a", "b"]] * table.num_rows, type=pa.list_(pa.string()))
        # Insert before the attribute columns so the schema walk must step over it.
        table = table.add_column(1, "tags", tags)
        source = tmp_path / "places_tags.parquet"
        pq.write_table(table, source)

        out = tmp_path / "tags.gpkg"
        write_geopackage(str(source), str(out), bbox=BBOX)
        columns = _gpkg_columns(out)
        assert "tags" not in columns  # GDAL cannot hold it; dropped as before
        assert "name" in columns  # the columns after it still arrive
        assert len(_gpkg_values(out)) == BBOX_ROWS


def _mixed_polygon_parquet(tmp_path):
    """A GeoParquet file whose metadata declares Polygon and MultiPolygon."""
    import duckdb

    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial")
        rows = con.execute(
            "SELECT ST_AsWKB(ST_GeomFromText(wkt)) FROM (VALUES "
            "('POLYGON((0 0,1 0,1 1,0 1,0 0))'),"
            "('MULTIPOLYGON(((2 2,3 2,3 3,2 3,2 2)),((4 4,5 4,5 5,4 5,4 4)))')"
            ") t(wkt)"
        ).fetchall()
    finally:
        con.close()
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"]}},
    }
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "geometry": pa.array([bytes(row[0]) for row in rows], type=pa.binary()),
        }
    )
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    path = tmp_path / "mixed.parquet"
    pq.write_table(table, path)
    return str(path)


# ---------------------------------------------------------------------------
# CLI options
# ---------------------------------------------------------------------------


class TestCLI:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_geopackage_bbox_and_where(self, runner, tmp_path):
        out = tmp_path / "out.gpkg"
        result = runner.invoke(
            cli,
            [
                "convert",
                "geopackage",
                PLACES,
                str(out),
                "--bbox",
                BBOX,
                "--where",
                "name IS NOT NULL",
            ],
        )
        assert result.exit_code == 0, result.output
        assert len(_gpkg_values(out)) == BBOX_ROWS

    def test_flatgeobuf_geometry(self, runner, tmp_path):
        out = tmp_path / "out.fgb"
        result = runner.invoke(
            cli, ["convert", "flatgeobuf", PLACES, str(out), "--geometry", BBOX_AS_WKT]
        )
        assert result.exit_code == 0, result.output
        assert _read_layer_count(out) == BBOX_ROWS

    def test_csv_bbox(self, runner, tmp_path):
        out = tmp_path / "out.csv"
        result = runner.invoke(cli, ["convert", "csv", PLACES, str(out), "--bbox", BBOX])
        assert result.exit_code == 0, result.output
        assert len(Path(out).read_text(encoding="utf-8").strip().splitlines()) == BBOX_ROWS + 1

    def test_geometry_from_file(self, runner, tmp_path):
        boundary = tmp_path / "boundary.wkt"
        boundary.write_text(BBOX_AS_WKT)
        out = tmp_path / "out.gpkg"
        result = runner.invoke(
            cli, ["convert", "geopackage", PLACES, str(out), "--geometry", f"@{boundary}"]
        )
        assert result.exit_code == 0, result.output
        assert len(_gpkg_values(out)) == BBOX_ROWS


# ---------------------------------------------------------------------------
# Python API twins
# ---------------------------------------------------------------------------


class TestPythonAPI:
    def test_ops_convert_to_geopackage(self, tmp_path):
        from geoparquet_io.api import ops

        out = tmp_path / "ops.gpkg"
        ops.convert_to_geopackage(pq.read_table(PLACES), str(out), bbox=BBOX_TUPLE)
        assert len(_gpkg_values(out)) == BBOX_ROWS

    def test_ops_convert_to_flatgeobuf(self, tmp_path):
        from geoparquet_io.api import ops

        out = tmp_path / "ops.fgb"
        ops.convert_to_flatgeobuf(pq.read_table(PLACES), str(out), where="name IS NOT NULL")
        assert _read_layer_count(out) == TOTAL_ROWS

    def test_ops_convert_to_csv(self, tmp_path):
        from geoparquet_io.api import ops

        out = tmp_path / "ops.csv"
        ops.convert_to_csv(pq.read_table(PLACES), str(out), bbox=BBOX_TUPLE)
        assert len(Path(out).read_text(encoding="utf-8").strip().splitlines()) == BBOX_ROWS + 1

    def test_table_write_passes_the_filters(self, tmp_path):
        import geoparquet_io as gpio

        out = tmp_path / "table.gpkg"
        gpio.read(PLACES).write(out, bbox=BBOX_TUPLE, where="name IS NOT NULL")
        assert len(_gpkg_values(out)) == BBOX_ROWS

    def test_table_write_passes_use_first_geometry(self, tmp_path):
        import geoparquet_io as gpio

        collection = json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [[-0.5, 10.0], [0.5, 10.0], [0.5, 11.0], [-0.5, 11.0], [-0.5, 10.0]]
                            ],
                        },
                    },
                    {
                        "type": "Feature",
                        "properties": {},
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                    },
                ],
            }
        )
        out = tmp_path / "first.gpkg"
        gpio.read(PLACES).write(out, geometry=collection, use_first_geometry=True)
        assert len(_gpkg_values(out)) == BBOX_ROWS


class TestFiltersOnUnsupportedFormats:
    """A filter the writer cannot honor must refuse, not silently write everything.

    Same principle as #1176's geometry-name check: every format refuses a bad
    parameter alike. Without the guard, ``write('all.parquet', bbox=...)``
    delivered the complete dataset under the subset's filename.
    """

    def test_parquet_write_with_a_filter_raises(self, tmp_path):
        import geoparquet_io as gpio

        with pytest.raises(InvalidParameterError, match="parquet"):
            gpio.read(PLACES).write(tmp_path / "all.parquet", bbox=BBOX_TUPLE)

    def test_geojson_write_with_a_filter_raises(self, tmp_path):
        import geoparquet_io as gpio

        with pytest.raises(InvalidParameterError, match="geojson"):
            gpio.read(PLACES).write(tmp_path / "all.geojson", where="1=0")

    def test_shapefile_write_with_a_filter_raises(self, tmp_path):
        import geoparquet_io as gpio

        with pytest.raises(InvalidParameterError, match="shapefile"):
            gpio.read(PLACES).write(tmp_path / "all.shp", use_first_geometry=True)

    def test_the_supported_formats_still_accept_filters(self, tmp_path):
        import geoparquet_io as gpio

        out = tmp_path / "guarded.gpkg"
        gpio.read(PLACES).write(out, bbox=BBOX_TUPLE)
        assert len(_gpkg_values(out)) == BBOX_ROWS


class TestWriteFormatRouter:
    def test_the_csv_branch_forwards_the_filters(self, tmp_path):
        out = tmp_path / "routed.csv"
        format_writers.write_format(PLACES, str(out), "csv", bbox=BBOX)
        assert len(Path(out).read_text(encoding="utf-8").strip().splitlines()) == BBOX_ROWS + 1


class TestNonFiniteBbox:
    def test_a_nan_bbox_raises_instead_of_reaching_sql(self, tmp_path):
        """float('nan') used to be interpolated as the bare SQL token ``nan``."""
        with pytest.raises(InvalidParameterError, match="finite"):
            write_csv(PLACES, str(tmp_path / "nan.csv"), bbox=(float("nan"), 0.0, 1.0, 2.0))

    def test_a_nan_bbox_string_raises_too(self, tmp_path):
        with pytest.raises(InvalidParameterError, match="finite"):
            write_csv(PLACES, str(tmp_path / "nan.csv"), bbox="nan,0,1,2")
