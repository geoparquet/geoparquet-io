"""Regression tests for the #1268 rewrite-memory work.

The table-centric helpers behind the Python API chain
(``gpio.convert(...).sort_hilbert().add_bbox().write(...)``) used to round-trip
the *entire* table through DuckDB for every step, holding two to three full
copies of the data at once (1.05 GB RSS for a 95 MB input). They now compute
only what each step needs inside DuckDB — sort indices, a bbox column, the
repaired rows — and build the result from the caller's own Arrow buffers:

* ``hilbert_order_table`` sorts (hilbert key, row index) pairs and reorders
  with ``Table.take``, so the original bytes, encodings and column order
  survive untouched;
* ``add_bbox_table`` computes just the bbox struct column and appends it;
* ``repair_arrow_table_geometry`` counts and repairs in ONE DuckDB scan that
  returns only the repaired rows (NULL elsewhere), patched into the original
  column client-side.

These tests pin the byte-fidelity those rewrites bought, and a peak-RSS
ceiling for the whole chain.
"""

import logging
import subprocess
import sys
import textwrap

import pyarrow as pa
import pytest

from geoparquet_io.core.duckdb_utils import get_duckdb_connection

BOWTIE_WKT = "POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))"


def _wkb(con, wkt: str) -> bytes:
    return bytes(con.execute(f"SELECT ST_AsWKB(ST_GeomFromText('{wkt}'))").fetchone()[0])


@pytest.fixture
def con():
    c = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    yield c
    c.close()


@pytest.fixture
def points_table(con):
    """Geometry FIRST, then attributes — order the DuckDB round-trip used to break."""
    wkbs = [_wkb(con, f"POINT({x} {x})") for x in (30, 5, 20, 10)]
    return pa.table(
        {
            "geometry": pa.array(wkbs, type=pa.binary()),
            "id": pa.array([0, 1, 2, 3], type=pa.int64()),
            "name": ["a", "b", "c", "d"],
        }
    )


class TestHilbertOrderTableFidelity:
    def test_preserves_column_order_and_bytes(self, points_table):
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        result = hilbert_order_table(points_table, geometry_column="geometry")

        # Same columns in the same positions (geometry was NOT moved last).
        assert result.column_names == points_table.column_names
        assert result.num_rows == points_table.num_rows
        # Each row's geometry bytes are the input row's bytes, untouched.
        by_id = dict(
            zip(
                points_table.column("id").to_pylist(),
                points_table.column("geometry").to_pylist(),
                strict=False,
            )
        )
        for rid, geom in zip(
            result.column("id").to_pylist(), result.column("geometry").to_pylist(), strict=False
        ):
            assert geom == by_id[rid]
        # The rows were actually reordered by the Hilbert curve.
        assert result.column("id").to_pylist() != [0, 1, 2, 3]

    def test_preserves_geometry_type_and_metadata(self, points_table):
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        meta = {b"geo": b'{"primary_column": "geometry"}', b"pandas": b"{}"}
        table = points_table.replace_schema_metadata(meta)
        result = hilbert_order_table(table, geometry_column="geometry")
        assert result.schema.metadata == meta
        # Encoding survives: binary in, binary out (no large_binary round-trip).
        assert result.schema.field("geometry").type == pa.binary()

    def test_geometry_only_table(self, con):
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        wkbs = [_wkb(con, f"POINT({x} 0)") for x in (9, 1, 5)]
        table = pa.table({"geometry": pa.array(wkbs, type=pa.binary())})
        result = hilbert_order_table(table, geometry_column="geometry")
        assert result.num_rows == 3
        assert sorted(result.column("geometry").to_pylist()) == sorted(wkbs)

    def test_row_index_name_moves_aside_on_collision(self, con):
        """A user column named like the internal row index never collides with it."""
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        wkbs = [_wkb(con, f"POINT({x} 0)") for x in (9, 1, 5)]
        table = pa.table(
            {
                "__gpio_row_idx": pa.array([7, 8, 9], type=pa.int64()),
                "geometry": pa.array(wkbs, type=pa.binary()),
            }
        )
        result = hilbert_order_table(table, geometry_column="geometry")
        assert result.column_names == table.column_names
        assert sorted(result.column("__gpio_row_idx").to_pylist()) == [7, 8, 9]


class TestAddBboxTableFidelity:
    def test_appends_bbox_without_touching_other_columns(self, points_table):
        from geoparquet_io.core.add.bbox import add_bbox_table

        result = add_bbox_table(points_table, geometry_column="geometry")

        assert result.column_names == [*points_table.column_names, "bbox"]
        for name in points_table.column_names:
            assert result.column(name).equals(points_table.column(name)), name
        box = result.column("bbox").to_pylist()[0]
        assert box["xmin"] == box["xmax"] == 30.0
        assert box["ymin"] == box["ymax"] == 30.0

    def test_null_geometry_yields_null_bbox_fields(self, con):
        from geoparquet_io.core.add.bbox import add_bbox_table

        table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "geometry": pa.array([_wkb(con, "POINT(1 2)"), None], type=pa.binary()),
            }
        )
        result = add_bbox_table(table, geometry_column="geometry")
        assert result.num_rows == 2
        box = result.column("bbox").to_pylist()[1]
        assert box == {"xmin": None, "ymin": None, "xmax": None, "ymax": None}


class TestRepairSingleScan:
    def test_repair_patches_only_invalid_rows(self, con, caplog):
        from geoparquet_io.core.geometry_repair import repair_arrow_table_geometry

        valid = _wkb(con, "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))")
        invalid = _wkb(con, BOWTIE_WKT)
        table = pa.table(
            {
                "geometry": pa.array([valid, invalid, None, valid], type=pa.binary()),
                "id": pa.array([0, 1, 2, 3], type=pa.int64()),
            }
        )
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            repaired, n = repair_arrow_table_geometry(table, "geometry", repair=True)
        assert n == 1
        assert "Repaired 1 invalid geometry" in caplog.text
        geoms = repaired.column("geometry").to_pylist()
        # Valid rows and the NULL row keep their exact original bytes.
        assert geoms[0] == valid
        assert geoms[2] is None
        assert geoms[3] == valid
        # The invalid row was repaired in place and is now valid.
        assert geoms[1] != invalid
        ok = con.execute("SELECT ST_IsValid(ST_GeomFromWKB(?))", [geoms[1]]).fetchone()[0]
        assert ok is True
        # Encoding and column order survive.
        assert repaired.schema.field("geometry").type == pa.binary()
        assert repaired.column_names == table.column_names

    def test_non_binary_geometry_column_is_passthrough(self):
        """A column DuckDB cannot parse as WKB never breaks the pipeline."""
        from geoparquet_io.core.geometry_repair import repair_arrow_table_geometry

        table = pa.table({"geometry": pa.array([1, 2, 3], type=pa.int64())})
        result, n = repair_arrow_table_geometry(table, "geometry", repair=True)
        assert result is table
        assert n == 0


@pytest.mark.slow
@pytest.mark.skipif(sys.platform == "win32", reason="resource.getrusage is POSIX-only")
def test_sort_and_bbox_peak_rss_delta_ceiling():
    """``sort_hilbert`` + ``add_bbox`` grow peak RSS by at most ~3x the table's bytes.

    Issue #1268: on a 1M-polygon / ~100 MB GeoParquet input the Python API
    rewrite chain peaked over 1 GB RSS (1052 MB on the reporter's Linux box,
    ~790 MB on macOS) because each step round-tripped the whole table through
    DuckDB. After the fix the same 1M chain peaks at ~580 MB on macOS.

    An absolute ceiling cannot gate this in CI — the interpreter-plus-DuckDB
    base (~200 MB) and allocator noise swamp a scaled-down input. What
    separates the two implementations cleanly is peak-RSS *growth across the
    two hot steps, relative to the table's in-memory size*, measured in a
    pre-warmed fresh subprocess (300k rows, 32 MB table, three runs each):

        fixed:  2.2 - 2.5x   (input + sorted copy via ``take`` + bbox column)
        broken: 4.3 - 4.4x   (plus a DuckDB-materialized full sorted result
                              and a second full copy from ``read_all()``)

    The 3.3x ceiling sits between them with near-symmetric margin. RSS, not
    tracemalloc, because the copies live in Arrow/DuckDB native memory that
    tracemalloc never sees; a subprocess, because a pytest worker's
    ``ru_maxrss`` high-water mark already sits far above anything this test
    would add.
    """
    rows = 300_000
    script = textwrap.dedent(
        f"""
        import resource, sys
        import duckdb

        def peak():
            v = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return v if sys.platform == "darwin" else v * 1024

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        table = con.execute('''
            SELECT ST_AsWKB(ST_MakePolygon(ST_MakeLine([
                    ST_Point(x, y), ST_Point(x + w, y),
                    ST_Point(x + w, y + w), ST_Point(x, y + w), ST_Point(x, y)
                ]))) AS geometry,
                CAST(i AS BIGINT) AS id
            FROM (
                SELECT i,
                    -20.0 + (hash(i) % 1000000) / 1000000.0 * 60.0 AS x,
                    -10.0 + (hash(i + 7) % 1000000) / 1000000.0 * 40.0 AS y,
                    0.0001 + (hash(i + 11) % 1000) / 1000.0 * 0.0005 AS w
                FROM range({rows}) t(i)
            )
        ''').arrow().read_all()
        con.close()

        from geoparquet_io.core.add.bbox import add_bbox_table
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        # Warm up on a tiny slice so module imports, the spatial extension and
        # DuckDB's own base allocations land before the measured window.
        warm = table.slice(0, 1000)
        add_bbox_table(
            hilbert_order_table(warm, geometry_column="geometry"), geometry_column="geometry"
        )

        before = peak()
        result = hilbert_order_table(table, geometry_column="geometry")
        result = add_bbox_table(result, geometry_column="geometry")
        assert result.num_rows == {rows}
        ratio = (peak() - before) / table.nbytes
        print(f"RSS_DELTA_RATIO={{ratio:.3f}}")
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", script], check=True, capture_output=True, text=True
    )
    line = next(ln for ln in proc.stdout.splitlines() if ln.startswith("RSS_DELTA_RATIO="))
    ratio = float(line.split("=")[1])
    assert ratio < 3.3, (
        f"sort_hilbert + add_bbox grew peak RSS by {ratio:.2f}x the table's bytes "
        f"for {rows} rows; the #1268 ceiling is 3.3x — a step is round-tripping "
        "the whole table through DuckDB again"
    )
