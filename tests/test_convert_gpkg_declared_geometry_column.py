"""A GeoPackage names its own geometry column; ``convert`` should believe it.

``gpio convert geoparquet`` finds the geometry column of a GeoPackage by
matching names against ``STANDARD_GEOMETRY_NAMES``. A GeoPackage does not have
to use any of those names, and it does not have to be guessed at: every
GeoPackage carries a ``gpkg_geometry_columns`` table whose ``column_name`` is
the authoritative answer, in the file already, one query away.

Measured with 1.5.0, on a one-layer GeoPackage written with
``GEOMETRY_NAME=CENTRELINE_GEOMETRY``::

    $ gpio convert geoparquet link.gpkg link.parquet --layer CablewayLink
    Converting link.gpkg...
    Error: Conversion failed: No geometry column detected in input file.
    Expected column named 'geom', 'geometry', 'wkb_geometry', or 'shape'.
    Use --allow-no-geometry to convert as plain Parquet without GeoParquet metadata.

while the file itself says::

    sqlite> select table_name, column_name from gpkg_geometry_columns;
    CablewayLink|CENTRELINE_GEOMETRY

The suggested escape hatch is not one here: ``--allow-no-geometry`` produces
plain Parquet with no GeoParquet metadata, discarding the geometry that the
layer does have. There is also no ``--geometry-column`` option on
``convert geoparquet`` to name the column by hand (``--wkt-column`` /
``--lat-column`` / ``--lon-column`` are CSV/TSV-only), so a caller holding such
a file has no way through at all.

This is not a rare spelling. INSPIRE network themes model a link's geometry as
``centrelineGeometry``, so national INSPIRE GeoPackage downloads flatten it to
``CENTRELINE_GEOMETRY``: every ``*Link`` layer of the Czech (ČÚZK) rail, road,
water, cableway and hydrography datasets is affected, while their non-``Link``
layers convert fine because ``GEOMETRY`` happens to match case-insensitively.

Two ways to close it, in preference order, both left to the maintainers:

1. Fall back to ``gpkg_geometry_columns.column_name`` when no conventionally
   named column is found. Authoritative, needs no new API, and the answer is
   already inside the file being read.
2. Add a ``--geometry-column`` option to ``convert geoparquet``, useful beyond
   GeoPackage but it puts the burden on the caller.

This file only pins the behaviour; it does not choose.

The fixture carries **two** layers on purpose. Those archives are multi-layer,
and ``gpkg_geometry_columns`` is keyed by ``table_name``: a fix that reads the
table without honouring ``--layer`` -- ``SELECT column_name FROM
gpkg_geometry_columns LIMIT 1`` -- would answer ``geom`` from the other layer
and be wrong on every file this is about. Converting the *second* layer, while
the first is conventionally named, is what makes that mistake visible.

The failing expectation is marked xfail imperatively rather than with
``pytest.mark.xfail(strict=True)``. The marker would swallow *any* failure of
this test, so an unrelated breakage (a future duckdb-spatial that cannot read
the fixture at all) would keep reading as a green xfail and quietly stop
pinning anything. Calling :func:`pytest.xfail` only after the specific
"No geometry column detected" refusal has been seen keeps every other failure
loud, and lets the test turn into an ordinary passing regression test the day
the gap is closed.
"""

import json
import sqlite3
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import sql_path

LAYER = "CablewayLink"
GEOM_COLUMN = "CENTRELINE_GEOMETRY"
LAYER_ROWS = 20

OTHER_LAYER = "RoadLink"
OTHER_GEOM_COLUMN = "geom"
OTHER_LAYER_ROWS = 5

REFUSAL = "No geometry column detected"


def _write_layer(path: Path, layer: str, geom_column: str, rows: int) -> None:
    """Write a single-layer GeoPackage whose geometry column is *geom_column*."""
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(
            f"COPY (SELECT i AS id, ST_Point(i * 0.1, i * 0.1) AS geom FROM range({rows}) t(i)) "
            f"TO {sql_path(path)} (FORMAT GDAL, DRIVER 'GPKG', LAYER_NAME '{layer}', "
            f"LAYER_CREATION_OPTIONS 'GEOMETRY_NAME={geom_column}')"
        )
    finally:
        con.close()


def _write_two_layer_gpkg(path: Path) -> None:
    """A GeoPackage carrying a conventionally named layer *and* an INSPIRE one.

    DuckDB's GDAL copy rewrites the file per layer, so the second layer is
    written separately and grafted on over SQLite -- table DDL, ``gpkg_contents``
    row and ``gpkg_geometry_columns`` row -- which is all GDAL needs to read it.
    """
    donor = path.with_name(f"donor-{path.name}")
    _write_layer(path, OTHER_LAYER, OTHER_GEOM_COLUMN, OTHER_LAYER_ROWS)
    _write_layer(donor, LAYER, GEOM_COLUMN, LAYER_ROWS)

    con = sqlite3.connect(str(path))
    try:
        con.execute("ATTACH DATABASE ? AS donor", (str(donor),))
        (ddl,) = con.execute(
            "SELECT sql FROM donor.sqlite_master WHERE type = 'table' AND name = ?",
            (LAYER,),
        ).fetchone()
        con.execute(ddl)
        con.execute(f'INSERT INTO "{LAYER}" SELECT * FROM donor."{LAYER}"')
        con.execute("INSERT INTO gpkg_contents SELECT * FROM donor.gpkg_contents")
        con.execute("INSERT INTO gpkg_geometry_columns SELECT * FROM donor.gpkg_geometry_columns")
        con.commit()
    finally:
        con.close()
    donor.unlink()


def test_the_fixture_declares_the_columns_it_claims_to(tmp_path):
    """Guard the premise: the answer really is in gpkg_geometry_columns, per layer."""
    gpkg = tmp_path / "link.gpkg"
    _write_two_layer_gpkg(gpkg)

    con = sqlite3.connect(str(gpkg))
    try:
        rows = con.execute(
            "SELECT table_name, column_name FROM gpkg_geometry_columns ORDER BY table_name"
        ).fetchall()
    finally:
        con.close()
    assert rows == [(LAYER, GEOM_COLUMN), (OTHER_LAYER, OTHER_GEOM_COLUMN)]


def test_convert_reads_the_geometry_column_the_geopackage_declares(tmp_path):
    gpkg = tmp_path / "link.gpkg"
    output = tmp_path / "link.parquet"
    _write_two_layer_gpkg(gpkg)

    result = CliRunner().invoke(
        cli, ["convert", "geoparquet", str(gpkg), str(output), "--layer", LAYER]
    )
    if result.exit_code != 0:
        # Only the unrecognised-name refusal is expected to fail here. Anything
        # else is a different bug and must not hide behind the xfail.
        assert REFUSAL in result.output, result.output
        pytest.xfail(
            "gpio gap: convert geoparquet matches GeoPackage geometry columns against "
            "STANDARD_GEOMETRY_NAMES instead of reading gpkg_geometry_columns.column_name"
        )
    assert output.exists()

    parquet = pq.ParquetFile(str(output))
    geo = json.loads(parquet.metadata.metadata[b"geo"])
    # The declared column, not merely *a* column: a fix that reached for the
    # other layer's `geom`, or renamed its way to a conventional name, is not
    # the fix this asks for.
    assert geo["primary_column"] == GEOM_COLUMN
    assert GEOM_COLUMN in parquet.schema_arrow.names

    table = pq.read_table(str(output))
    assert table.num_rows == LAYER_ROWS
    assert table.column(GEOM_COLUMN).null_count == 0
