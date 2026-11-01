"""Conversion of sources containing empty geometries (issue #649).

`ST_Hilbert` rejects empty geometries, and `gpio convert` ordered every row
with it, so a single `POLYGON EMPTY` failed the whole conversion:

    Error: Conversion failed: Invalid Input Error: ST_Hilbert(geom, bounds)
    does not support empty geometries

`gpio sort hilbert` never had the problem — it orders the non-empty rows and
appends the rest (`core/hilbert_order.py`). Empty geometries are common in
GeoPackage/Shapefile/FileGDB exports, and #647's linearization produces them
too (`CIRCULARSTRING EMPTY` becomes `LINESTRING EMPTY`).
"""

from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli

CURVED_GPKG = Path(__file__).parent / "data" / "curved_geometry_test.gpkg"


def _spatial_con():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("SET geometry_always_xy = true")
    return con


def _write_gpkg(path, rows):
    """Write a GeoPackage from (id, WKT-or-None) pairs, empties included."""
    con = _spatial_con()
    values = ", ".join(
        f"({i}, NULL)" if wkt is None else f"({i}, ST_GeomFromText('{wkt}'))" for i, wkt in rows
    )
    con.execute(
        f"COPY (SELECT * FROM (VALUES {values}) v(id, geom)) "
        # An explicit SRS: DuckDB 2.0 writes none for a CRS-less geometry (1.5
        # wrote "Undefined geographic SRS"), and convert refuses a CRS-less file.
        f"TO '{path}' WITH (FORMAT GDAL, DRIVER 'GPKG', SRS 'EPSG:4326')"
    )
    con.close()


def _convert(source, output, *extra):
    return CliRunner().invoke(cli, ["convert", str(source), str(output), *extra])


def _count(output):
    """Row count without touching geometry (native encodings are structs)."""
    con = _spatial_con()
    try:
        return con.execute(f"SELECT count(*) FROM '{output}'").fetchall()[0][0]
    finally:
        con.close()


def _rows(output, geom_col="geom"):
    """(id, is_empty_or_null) in stored order."""
    con = _spatial_con()
    try:
        return con.execute(
            f"SELECT id, {geom_col} IS NULL OR ST_IsEmpty({geom_col}) FROM '{output}'"
        ).fetchall()
    finally:
        con.close()


class TestEmptyGeometryConversion:
    def test_empty_geometry_no_longer_fails_the_conversion(self, tmp_path):
        source = tmp_path / "one_empty.gpkg"
        _write_gpkg(source, [(1, "POINT(1 1)"), (2, "POLYGON EMPTY")])
        output = tmp_path / "out.parquet"

        result = _convert(source, output)

        assert result.exit_code == 0, result.output
        assert len(_rows(output)) == 2

    def test_empty_and_null_geometries_sort_last(self, tmp_path):
        """Same contract as `sort hilbert`: ordered rows first, the rest after."""
        source = tmp_path / "mixed.gpkg"
        _write_gpkg(
            source,
            [
                (1, "POINT(9 9)"),
                (2, "POLYGON EMPTY"),
                (3, "POINT(1 1)"),
                (4, None),
                (5, "POINT(5 5)"),
            ],
        )
        output = tmp_path / "out.parquet"

        result = _convert(source, output)

        assert result.exit_code == 0, result.output
        rows = _rows(output)
        assert len(rows) == 5
        unorderable = [is_empty for _, is_empty in rows]
        assert unorderable == sorted(unorderable), f"empty rows are not last: {rows}"
        # The non-empty rows keep Hilbert order: (1 1) before (5 5) before (9 9).
        assert [i for i, is_empty in rows if not is_empty] == [3, 5, 1]

    def test_all_geometries_empty_converts_without_ordering(self, tmp_path):
        """With nothing to measure there is no envelope, so ordering is skipped
        rather than failing on unmeasurable bounds."""
        source = tmp_path / "all_empty.gpkg"
        _write_gpkg(source, [(1, "POLYGON EMPTY"), (2, "LINESTRING EMPTY")])
        output = tmp_path / "out.parquet"

        result = _convert(source, output)

        assert result.exit_code == 0, result.output
        assert len(_rows(output)) == 2

    def test_geoarrow_native_encoding_handles_empty_geometry(self, tmp_path):
        """The native-encoding branch orders by centroid expressions instead of
        ST_Hilbert(geometry, ...) — empties must not break it either."""
        source = tmp_path / "native.gpkg"
        _write_gpkg(source, [(1, "POLYGON((0 0, 1 0, 1 1, 0 0))"), (2, "POLYGON EMPTY")])
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--geoparquet-version", "1.1-geoarrow")

        assert result.exit_code == 0, result.output
        assert _count(output) == 2  # geometry is a nested struct here, not WKB

    def test_skip_hilbert_still_works(self, tmp_path):
        source = tmp_path / "one_empty.gpkg"
        _write_gpkg(source, [(1, "POINT(1 1)"), (2, "POLYGON EMPTY")])
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--skip-hilbert")

        assert result.exit_code == 0, result.output
        assert len(_rows(output)) == 2


class TestEmptyGeometryFromCsv:
    def test_empty_wkt_geometry_converts(self, tmp_path):
        """The CSV path builds its own Hilbert ordering over a WKT expression."""
        source = tmp_path / "points.csv"
        source.write_text(
            "id,geom\n1,POINT(1 1)\n2,POLYGON EMPTY\n3,POINT(5 5)\n",
            encoding="utf-8",
        )
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--wkt-column", "geom")

        assert result.exit_code == 0, result.output
        rows = _rows(output, geom_col="geometry")
        assert len(rows) == 3
        unorderable = [is_empty for _, is_empty in rows]
        assert unorderable == sorted(unorderable), f"empty rows are not last: {rows}"


class TestLinearizedEmptyCurve:
    def test_curved_source_with_empty_curve_converts(self, tmp_path):
        """#647 turns CIRCULARSTRING EMPTY into LINESTRING EMPTY, which then met
        this bug in the same command."""
        import shutil
        import sqlite3
        import struct

        if not CURVED_GPKG.exists():
            pytest.skip("curved_geometry_test.gpkg not available")

        source = tmp_path / "curved_with_empty.gpkg"
        shutil.copy(CURVED_GPKG, source)
        con = sqlite3.connect(source)
        for (name,) in con.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        ).fetchall():
            con.execute(f'DROP TRIGGER "{name}"')
        empty_circularstring = b"\x01" + struct.pack("<I", 8) + struct.pack("<I", 0)
        con.execute(
            "INSERT INTO curve (geom, wkt, id) VALUES (?, ?, ?)",
            (b"GP\x00\x01" + struct.pack("<i", 0) + empty_circularstring, "EMPTY", "empty"),
        )
        con.commit()
        con.close()
        output = tmp_path / "out.parquet"

        result = _convert(source, output)

        assert result.exit_code == 0, result.output
        assert len(_rows(output)) == 2

    def test_all_empty_csv_converts_without_ordering(self, tmp_path):
        """Same rescue as the spatial path: nothing to measure, so no ordering."""
        source = tmp_path / "all_empty.csv"
        source.write_text("id,geom\n1,POLYGON EMPTY\n2,LINESTRING EMPTY\n", encoding="utf-8")
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--wkt-column", "geom")

        assert result.exit_code == 0, result.output
        assert _count(output) == 2

    def test_header_only_csv_converts(self, tmp_path):
        source = tmp_path / "empty.csv"
        source.write_text("id,geom\n", encoding="utf-8")
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--wkt-column", "geom")

        assert result.exit_code == 0, result.output
        assert _count(output) == 0
