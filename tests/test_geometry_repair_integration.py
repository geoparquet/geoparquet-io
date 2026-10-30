"""End-to-end geometry-repair behavior across the pipeline (issue #506).

The shared helpers are unit-tested in ``test_geometry_repair.py``. These tests
exercise the wiring: that ``convert``, ``convert geojson`` and ``pmtiles create``
repair invalid geometry by default, honour ``--no-repair-geometry``, and that the
reported regression (invalid polygons crashing tippecanoe) no longer occurs.
"""

import json
import logging
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import duckdb
import pytest

from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.duckdb_utils import fetch_one

_DATA_DIR = Path(__file__).parent / "data"
# Real WFS extraction (IDECOR "Carlos_Paz_Peligrosidad", 64 features, 3 invalid
# MultiPolygons) captured as a WKB-encoded parquet. This exact 64-row mix
# segfaulted DuckDB 1.5.1's spatial extension under the original OR-form repair
# predicate (`col IS NULL OR parsed IS NULL OR ST_IsValid(parsed)` with
# ST_MakeValid in the ELSE branch). The crash is data-layout dependent — no
# subset of the rows reproduces it — so the genuine fixture is required.
_IDECOR_WKB_FIXTURE = _DATA_DIR / "idecor_carlospaz_invalid_wkb.parquet"

# Two self-intersecting "bowtie" polygons (invalid) and one valid square.
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


def _write_invalid_geojson(tmp_path):
    path = tmp_path / "invalid.geojson"
    path.write_text(json.dumps(_INVALID_GEOJSON))
    return str(path)


def _invalid_count(parquet_path, geom_col="geom"):
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    try:
        return fetch_one(
            con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{parquet_path}') WHERE NOT ST_IsValid({geom_col})"
            )
        )[0]
    finally:
        con.close()


def test_convert_repairs_invalid_geometry_by_default(tmp_path):
    src = _write_invalid_geojson(tmp_path)
    out = str(tmp_path / "out.parquet")
    convert_to_geoparquet(src, out)
    assert _invalid_count(out) == 0


def test_convert_opt_out_preserves_invalid_and_warns(tmp_path, caplog):
    src = _write_invalid_geojson(tmp_path)
    out = str(tmp_path / "out.parquet")
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        convert_to_geoparquet(src, out, repair_geometry=False)
    assert _invalid_count(out) == 2
    assert "Left unrepaired 2 invalid geometries" in caplog.text


def test_convert_repair_preserves_row_count(tmp_path):
    src = _write_invalid_geojson(tmp_path)
    out = str(tmp_path / "out.parquet")
    convert_to_geoparquet(src, out)
    con = duckdb.connect()
    try:
        assert fetch_one(con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')"))[0] == 3
    finally:
        con.close()


def test_repair_arrow_table_does_not_segfault_on_real_wkb(tmp_path):
    """Regression: the OR-form repair predicate segfaulted DuckDB on real WKB.

    ``repair_arrow_table_geometry`` is run in a subprocess so that a regression
    to the crashing OR-form fails this test cleanly (non-zero return code)
    instead of taking down the whole pytest worker with SIGSEGV.
    """
    out = tmp_path / "n_invalid.txt"
    script = textwrap.dedent(
        f"""
        import pyarrow.parquet as pq, duckdb
        from geoparquet_io.core.geometry_repair import (
            repair_arrow_table_geometry,
        )

        table = pq.read_table({str(_IDECOR_WKB_FIXTURE)!r})
        repaired, n = repair_arrow_table_geometry(table, "geometry")

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.register("r", repaired)
        bad = con.execute(
            "SELECT COUNT(*) FROM r "
            "WHERE NOT ST_IsValid(ST_GeomFromWKB(geometry))"
        ).fetchone()[0]
        with open({str(out)!r}, "w") as fh:
            fh.write(f"{{repaired.num_rows}},{{n}},{{bad}}")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"repair crashed (returncode={result.returncode}); stderr:\n{result.stderr}"
    )
    rows, n_invalid, still_bad = out.read_text().split(",")
    assert int(rows) == 64
    assert int(n_invalid) == 3  # three invalid geometries detected
    assert int(still_bad) == 0  # all repaired to valid


@pytest.mark.integration
@pytest.mark.skipif(shutil.which("tippecanoe") is None, reason="tippecanoe not installed")
def test_pmtiles_from_invalid_geometry_does_not_crash(tmp_path):
    """Regression: invalid polygons used to crash tippecanoe (exit 110)."""
    from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

    src = _write_invalid_geojson(tmp_path)
    # Build a parquet that still contains the invalid geometry (opt out of repair
    # at convert time) so the PMTiles step is the one that must cope.
    raw = str(tmp_path / "raw.parquet")
    convert_to_geoparquet(src, raw, repair_geometry=False)
    assert _invalid_count(raw) == 2

    out = str(tmp_path / "out.pmtiles")
    create_pmtiles_from_geoparquet(raw, out, force=True)
    assert (tmp_path / "out.pmtiles").exists()
