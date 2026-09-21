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
"""

import sqlite3

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli

LAYER = "CablewayLink"
GEOM_COLUMN = "CENTRELINE_GEOMETRY"


def _write_gpkg_with_declared_geometry_column(path) -> None:
    """A one-layer GeoPackage whose geometry column is ``CENTRELINE_GEOMETRY``."""
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(
            "COPY (SELECT i AS id, ST_Point(i * 0.1, i * 0.1) AS geom FROM range(20) t(i)) "
            f"TO '{path}' (FORMAT GDAL, DRIVER 'GPKG', LAYER_NAME '{LAYER}', "
            f"LAYER_CREATION_OPTIONS 'GEOMETRY_NAME={GEOM_COLUMN}')"
        )
    finally:
        con.close()


def test_the_fixture_declares_the_column_it_claims_to(tmp_path):
    """Guard the premise: the answer really is in gpkg_geometry_columns."""
    gpkg = tmp_path / "link.gpkg"
    _write_gpkg_with_declared_geometry_column(gpkg)

    con = sqlite3.connect(str(gpkg))
    try:
        rows = con.execute("SELECT table_name, column_name FROM gpkg_geometry_columns").fetchall()
    finally:
        con.close()
    assert rows == [(LAYER, GEOM_COLUMN)]


@pytest.mark.xfail(
    strict=True,
    reason="gpio gap: convert geoparquet matches GeoPackage geometry columns against "
    "STANDARD_GEOMETRY_NAMES instead of reading gpkg_geometry_columns.column_name",
)
def test_convert_reads_the_geometry_column_the_geopackage_declares(tmp_path):
    import json

    import pyarrow.parquet as pq

    gpkg = tmp_path / "link.gpkg"
    output = tmp_path / "link.parquet"
    _write_gpkg_with_declared_geometry_column(gpkg)

    result = CliRunner().invoke(
        cli, ["convert", "geoparquet", str(gpkg), str(output), "--layer", LAYER]
    )
    assert result.exit_code == 0, result.output
    assert output.exists()

    geo = json.loads(pq.ParquetFile(str(output)).metadata.metadata[b"geo"])
    assert geo["primary_column"] in pq.ParquetFile(str(output)).schema_arrow.names
    assert pq.read_table(str(output)).num_rows == 20
