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

These tests pin the byte-fidelity those rewrites bought, and catch a return
of the whole-table round-trip directly: a spy on ``con.register`` sees every
column DuckDB is handed, and buffer addresses show which columns were copied.
A peak-RSS ratio used to stand in for both and separated the implementations
by little more than allocator noise. Columns past 2 GiB (32-bit offsets) are
built by repeating one chunk zero-copy.
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


# ---------------------------------------------------------------------------
# Round-trip detectors. A step that hands the whole table to DuckDB is what
# #1268 removed; these catch it deterministically, where a peak-RSS ratio only
# separated the two implementations by allocator noise.
# ---------------------------------------------------------------------------


class _RegisterSpy:
    """A DuckDB connection proxy that records the columns of every registered table."""

    def __init__(self, con):
        self._con = con
        self.registered: list[list[str]] = []

    def register(self, name, obj):
        self.registered.append(list(obj.column_names))
        return self._con.register(name, obj)

    def __getattr__(self, name):
        return getattr(self._con, name)


@pytest.fixture
def register_spy(monkeypatch):
    """``install(module)`` routes ``module.get_duckdb_connection`` through a spy."""
    spies: list[_RegisterSpy] = []

    def install(module):
        real = module.get_duckdb_connection

        def spying(*args, **kwargs):
            spy = _RegisterSpy(real(*args, **kwargs))
            spies.append(spy)
            return spy

        monkeypatch.setattr(module, "get_duckdb_connection", spying)
        return spies

    return install


def _registered_columns(spies) -> set[str]:
    return {name for spy in spies for cols in spy.registered for name in cols}


def _buffer_addresses(column: pa.ChunkedArray) -> list:
    return [b.address if b is not None else None for c in column.chunks for b in c.buffers()]


@pytest.fixture
def rich_table(con):
    """Several chunks, a tz-aware timestamp and a string column next to the geometry."""
    wkbs = [_wkb(con, f"POINT({x} {(x * 7) % 11})") for x in range(12)]
    full = pa.table(
        {
            "geometry": pa.array(wkbs, type=pa.binary()),
            "id": pa.array(range(12), type=pa.int64()),
            "name": pa.array([f"row-{i}" for i in range(12)], type=pa.string()),
            "seen": pa.array(range(12), type=pa.timestamp("us", tz="Europe/Paris")),
        }
    )
    return pa.Table.from_batches(full.to_batches(max_chunksize=5))


class TestNoWholeTableRoundTrip:
    def test_hilbert_registers_only_geometry_and_row_index(self, rich_table, register_spy):
        import geoparquet_io.core.hilbert_order as mod

        spies = register_spy(mod)
        result = mod.hilbert_order_table(rich_table, geometry_column="geometry")

        registered = _registered_columns(spies)
        assert "geometry" in registered
        assert not registered & {"id", "name", "seen"}, registered
        assert len(registered) == 2, registered  # geometry + the row index
        assert result.schema == rich_table.schema

    def test_bbox_registers_only_geometry_and_keeps_buffers(self, rich_table, register_spy):
        import geoparquet_io.core.add.bbox as mod

        spies = register_spy(mod)
        result = mod.add_bbox_table(rich_table, geometry_column="geometry")

        assert _registered_columns(spies) == {"geometry"}
        # Zero-copy: every input column is the caller's own buffers, untouched.
        for name in rich_table.column_names:
            assert _buffer_addresses(result.column(name)) == _buffer_addresses(
                rich_table.column(name)
            ), name

    def test_repair_registers_only_geometry_and_keeps_clean_chunks(self, con, register_spy, caplog):
        import geoparquet_io.core.duckdb_utils as duckdb_utils
        from geoparquet_io.core.geometry_repair import repair_arrow_table_geometry

        valid = _wkb(con, "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))")
        clean = pa.array([valid] * 4, type=pa.binary())
        dirty = pa.array([valid, _wkb(con, BOWTIE_WKT)], type=pa.binary())
        table = pa.table(
            {
                "geometry": pa.chunked_array([clean, dirty, clean]),
                "id": pa.chunked_array(
                    [pa.array(range(4)), pa.array([4, 5]), pa.array(range(6, 10))]
                ),
            }
        )
        spies = register_spy(duckdb_utils)
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            repaired, n = repair_arrow_table_geometry(table, "geometry", repair=True)

        assert n == 1
        assert _registered_columns(spies) == {"geometry"}
        before, after = table.column("geometry").chunks, repaired.column("geometry").chunks
        assert len(after) == len(before)
        # Chunks without an invalid row are passed through zero-copy; only the
        # chunk holding the bowtie is rebuilt.
        for i in (0, 2):
            assert [b.address for b in after[i].buffers() if b] == [
                b.address for b in before[i].buffers() if b
            ]
        assert _buffer_addresses(repaired.column("id")) == _buffer_addresses(table.column("id"))


@pytest.mark.skipif(sys.platform == "win32", reason="arrow pool stats are checked on POSIX only")
def test_hilbert_take_adds_no_column_sized_transient():
    """Reordering allocates the output, not a concatenated copy of each column first.

    ``Table.take`` on a multi-chunk column concatenates the chunks before taking
    (pyarrow 25), so the largest column briefly exists twice on top of the
    output. Measured as the Arrow pool's peak over the input's bytes, in a fresh
    process because the pool's high-water mark never resets: the input is built
    on numpy-owned buffers, so the pool holds only what the sort allocates.
    Measured (deterministic): concat-then-take 1.89x, the chunk-wise gather
    1.35x (its 16 MiB batches over a ~92 MB table).
    """
    script = textwrap.dedent(
        """
        import struct
        import numpy as np
        import pyarrow as pa
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        def binary(blob, width, n, typ=pa.binary()):
            data = pa.py_buffer(np.frombuffer(blob, dtype=np.uint8))
            offs = pa.py_buffer(np.arange(0, (n + 1) * width, width, dtype=np.int32))
            return pa.Array.from_buffers(typ, n, [None, offs, data])

        n = 400_000
        rng = np.random.default_rng(0)
        pts = np.zeros(n, dtype=[("o", "u1"), ("t", "<u4"), ("x", "<f8"), ("y", "<f8")])
        pts["o"], pts["t"] = 1, 1
        pts["x"], pts["y"] = rng.uniform(-50, 50, n), rng.uniform(-50, 50, n)
        geom = binary(pts.tobytes(), 21, n)
        label = binary(b"x" * (200 * n), 200, n, pa.string())
        whole = pa.table({"geometry": geom, "label": label})
        table = pa.Table.from_batches(whole.to_batches(max_chunksize=50_000))

        hilbert_order_table(table.slice(0, 100), geometry_column="geometry")
        pool = pa.default_memory_pool()
        base = pool.bytes_allocated()
        result = hilbert_order_table(table, geometry_column="geometry")
        assert result.num_rows == n
        print(f"POOL_PEAK_RATIO={(pool.max_memory() - base) / table.nbytes:.3f}")
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", script], check=True, capture_output=True, text=True
    )
    line = next(ln for ln in proc.stdout.splitlines() if ln.startswith("POOL_PEAK_RATIO="))
    ratio = float(line.split("=")[1])
    assert ratio < 1.6, f"hilbert_order_table peaked at {ratio:.2f}x the table's bytes"


# ---------------------------------------------------------------------------
# Edge cases the review found.
# ---------------------------------------------------------------------------


class TestHilbertOrderEdgeCases:
    def test_equal_keys_keep_input_order(self, con):
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        same, other = _wkb(con, "POINT(3 3)"), _wkb(con, "POINT(-9 -9)")
        n = 3000
        table = pa.table(
            {
                "geometry": pa.array([same] * n + [other], type=pa.binary()),
                "id": pa.array(range(n + 1), type=pa.int64()),
            }
        )
        ids = hilbert_order_table(table, geometry_column="geometry").column("id").to_pylist()
        tied = [i for i in ids if i != n]
        assert tied == sorted(tied)

    def test_trailing_null_and_empty_rows_keep_input_order(self, con):
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        empty = _wkb(con, "POINT EMPTY")
        geoms, ids = [], []
        for i in range(3000):
            geoms.append(
                None if i % 3 == 0 else empty if i % 3 == 1 else _wkb(con, f"POINT({i % 50} 1)")
            )
            ids.append(i)
        table = pa.table({"geometry": pa.array(geoms, type=pa.binary()), "id": ids})
        out = hilbert_order_table(table, geometry_column="geometry").column("id").to_pylist()
        trailing = out[-2000:]
        assert set(trailing) == {i for i in ids if i % 3 != 2}
        assert trailing == sorted(trailing)

    def test_geometry_named_like_row_index_in_other_case(self, con):
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        wkbs = [_wkb(con, f"POINT({x} 0)") for x in (9, 1, 5)]
        table = pa.table(
            {"__GPIO_ROW_IDX": pa.array(wkbs, type=pa.binary()), "id": pa.array([0, 1, 2])}
        )
        result = hilbert_order_table(table, geometry_column="__GPIO_ROW_IDX")
        assert result.column_names == table.column_names
        assert sorted(result.column("id").to_pylist()) == [0, 1, 2]

    def test_view_typed_columns(self, con):
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        wkbs = [_wkb(con, f"POINT({x} {x})") for x in (30, 5, 20, 10)]
        table = pa.table(
            {
                "geometry": pa.array(wkbs, type=pa.binary_view()),
                "name": pa.array(["a", "b", "c", "d"], type=pa.string_view()),
            }
        )
        result = hilbert_order_table(table, geometry_column="geometry")
        assert result.schema == table.schema
        pairs = dict(zip(table.column("name").to_pylist(), wkbs, strict=True))
        for name, geom in zip(
            result.column("name").to_pylist(), result.column("geometry").to_pylist(), strict=True
        ):
            assert geom == pairs[name]

    def test_batched_gather_matches_take(self, con, monkeypatch):
        """The chunk-wise gather, forced into many small batches, equals ``Table.take``."""
        import geoparquet_io.core.hilbert_order as mod

        monkeypatch.setattr(mod, "_GATHER_BATCH_BYTES", 64)
        wkbs = [_wkb(con, f"POINT({(x * 37) % 101} {(x * 11) % 53})") for x in range(200)]
        whole = pa.table(
            {
                "geometry": pa.array(wkbs, type=pa.binary()),
                "name": pa.array([f"n{i}" * (i % 4) for i in range(200)], type=pa.large_string()),
                "tag": pa.array([None if i % 5 == 0 else f"t{i}" for i in range(200)]),
                "kind": pa.array([f"k{i % 3}" for i in range(200)]).dictionary_encode(),
                "id": pa.array(range(200), type=pa.int32()),
            }
        )
        table = pa.Table.from_batches(whole.to_batches(max_chunksize=23))
        result = mod.hilbert_order_table(table, geometry_column="geometry")
        expected = whole.take(pa.array(result.column("id").to_pylist()))
        assert result.schema == table.schema
        assert result.equals(expected)
        assert result.column("name").num_chunks > 1


class TestRepairEdgeCases:
    def test_view_typed_geometry_is_repaired(self, con, caplog):
        from geoparquet_io.core.geometry_repair import repair_arrow_table_geometry

        valid = _wkb(con, "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))")
        table = pa.table({"geometry": pa.array([valid, _wkb(con, BOWTIE_WKT)], pa.binary_view())})
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            repaired, n = repair_arrow_table_geometry(table, "geometry", repair=True)
        assert n == 1
        assert repaired.schema == table.schema
        fixed = repaired.column("geometry").to_pylist()
        assert fixed[0] == valid
        assert con.execute("SELECT ST_IsValid(ST_GeomFromWKB(?))", [fixed[1]]).fetchone()[0]

    def test_no_repaired_claim_when_patching_fails(self, con, caplog, monkeypatch):
        """A patch that cannot be applied raises; it never logs "Repaired" and returns 0."""
        import geoparquet_io.core.geometry_repair as mod

        def broken(*_args, **_kwargs):
            raise pa.ArrowNotImplementedError("no kernel")

        monkeypatch.setattr(mod, "_patch_repaired_rows", broken)
        table = pa.table({"geometry": pa.array([_wkb(con, BOWTIE_WKT)], pa.binary())})
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            with pytest.raises(pa.ArrowNotImplementedError):
                mod.repair_arrow_table_geometry(table, "geometry", repair=True)
        assert "Repaired" not in caplog.text

    def test_unreadable_column_warns_instead_of_silently_skipping(self, caplog, monkeypatch):
        """When DuckDB cannot scan the column, the caller is told nothing was checked."""
        import duckdb

        import geoparquet_io.core.geometry_repair as mod

        def failing(*_args, **_kwargs):
            raise duckdb.InvalidInputException("cannot read")

        monkeypatch.setattr(mod, "_repaired_rows", failing)
        table = pa.table({"geometry": pa.array([b"\x01"], pa.binary())})
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            result, n = mod.repair_arrow_table_geometry(table, "geometry", repair=True)
        assert result is table and n == 0
        assert "could not check" in caplog.text.lower()


# ---------------------------------------------------------------------------
# Columns past the 2 GiB a 32-bit offset can address (#1285 review P1). Built
# by repeating one chunk zero-copy, so the logical size costs no real memory.
# ---------------------------------------------------------------------------

_ROW_BYTES = 1 << 20
_ROWS_PER_CHUNK = 210  # 210 MiB a chunk; eleven of them pass 2 GiB


def _repeated_chunk(row: bytes, typ=pa.binary()) -> pa.Array:
    import numpy as np

    blob = (row + b"\0" * _ROW_BYTES)[:_ROW_BYTES]
    data = pa.py_buffer(np.frombuffer(blob * _ROWS_PER_CHUNK, dtype=np.uint8))
    offsets = pa.py_buffer(
        np.arange(0, (_ROWS_PER_CHUNK + 1) * _ROW_BYTES, _ROW_BYTES, dtype=np.int32)
    )
    return pa.Array.from_buffers(typ, _ROWS_PER_CHUNK, [None, offsets, data])


def test_repair_past_2_gib_patches_without_combining(con, caplog):
    """The geometry column is 2.2 GiB logical; repair patches the one dirty chunk.

    The bulk rows are a point followed by padding, which DuckDB's TRY() rejects
    as unreadable in microseconds — they pass through unchanged, so the scan is
    cheap and only the last, small chunk holds a repairable geometry.
    """
    from geoparquet_io.core.geometry_repair import repair_arrow_table_geometry

    big = _repeated_chunk(_wkb(con, "POINT(1 2)"))
    tail = pa.array([_wkb(con, "POINT(5 5)"), _wkb(con, BOWTIE_WKT)], pa.binary())
    geometry = pa.chunked_array([big] * 11 + [tail])
    assert geometry.nbytes > 2**31
    table = pa.table({"geometry": geometry})

    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        repaired, n = repair_arrow_table_geometry(table, "geometry", repair=True)

    assert n == 1
    assert "Repaired 1 invalid geometry" in caplog.text
    chunks = repaired.column("geometry").chunks
    assert all(c.buffers()[2].address == big.buffers()[2].address for c in chunks[:11])
    last = chunks[-1].to_pylist()[-1]
    assert con.execute("SELECT ST_IsValid(ST_GeomFromWKB(?))", [last]).fetchone()[0]


@pytest.mark.slow
def test_hilbert_past_2_gib_attribute_column(con):
    """A 2.2 GiB string column survives the reorder (``Table.take`` overflowed).

    Slow for its memory, not its time: the input is one 210 MiB chunk repeated,
    but the reordered output is real, ~2.3 GB.
    """
    from geoparquet_io.core.hilbert_order import hilbert_order_table

    big = _repeated_chunk(b"label", pa.string())
    rows = len(big) * 11
    label = pa.chunked_array([big] * 11)
    assert label.nbytes > 2**31
    wkbs = [_wkb(con, f"POINT({x % 97} {x % 89})") for x in range(rows)]
    table = pa.table({"geometry": pa.array(wkbs, pa.binary()), "label": label})

    result = hilbert_order_table(table, geometry_column="geometry")

    assert result.num_rows == rows
    assert result.schema == table.schema
    assert result.column("label")[0].as_py() == big[0].as_py()


def test_repair_geoarrow_wkb_extension_column(con, caplog):
    """DuckDB reads a geoarrow.wkb column as GEOMETRY; it is repaired, type kept."""
    ga = pytest.importorskip("geoarrow.pyarrow")
    from geoparquet_io.core.geometry_repair import repair_arrow_table_geometry

    valid = _wkb(con, "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))")
    storage = pa.array([valid, _wkb(con, BOWTIE_WKT)], pa.binary())
    table = pa.table({"geometry": ga.wkb().wrap_array(storage)})
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        repaired, n = repair_arrow_table_geometry(table, "geometry", repair=True)
    assert n == 1
    assert repaired.schema == table.schema
    fixed = repaired.column("geometry").chunk(0).storage.to_pylist()
    assert fixed[0] == valid
    assert con.execute("SELECT ST_IsValid(ST_GeomFromWKB(?))", [fixed[1]]).fetchone()[0]


def test_bbox_of_unreadable_wkb_is_null(con):
    """Bytes that are not WKB get a NULL bbox instead of aborting the whole step."""
    from geoparquet_io.core.add.bbox import add_bbox_table

    table = pa.table({"geometry": pa.array([_wkb(con, "POINT(1 2)"), b"\x01\x02"], pa.binary())})
    boxes = add_bbox_table(table, geometry_column="geometry").column("bbox").to_pylist()
    assert boxes[0] == {"xmin": 1.0, "ymin": 2.0, "xmax": 1.0, "ymax": 2.0}
    assert boxes[1] == {"xmin": None, "ymin": None, "xmax": None, "ymax": None}


def test_hilbert_multi_chunk_geoarrow_wkb_column(con):
    """An extension-typed geometry split over chunks is gathered with its type intact."""
    ga = pytest.importorskip("geoarrow.pyarrow")
    from geoparquet_io.core.hilbert_order import hilbert_order_table

    wkbs = [_wkb(con, f"POINT({(x * 37) % 101} {(x * 11) % 53})") for x in range(40)]
    whole = pa.table(
        {"geometry": ga.wkb().wrap_array(pa.array(wkbs, pa.binary())), "id": range(40)}
    )
    table = pa.Table.from_batches(whole.to_batches(max_chunksize=7))
    result = hilbert_order_table(table, geometry_column="geometry")
    assert result.schema == table.schema
    for rid, geom in zip(
        result.column("id").to_pylist(),
        result.column("geometry").combine_chunks().storage.to_pylist(),
        strict=True,
    ):
        assert geom == wkbs[rid]


def test_repair_halves_a_chunk_whose_patch_overflows(con, monkeypatch):
    """A chunk the repaired bytes no longer fit (offset overflow) is patched in halves."""
    import geoparquet_io.core.geometry_repair as mod

    real = mod._if_else_chunk

    def overflowing(chunk, fixed):
        if len(chunk) > 1:
            raise pa.ArrowCapacityError("offset overflow")
        return real(chunk, fixed)

    monkeypatch.setattr(mod, "_if_else_chunk", overflowing)
    valid = _wkb(con, "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))")
    table = pa.table({"geometry": pa.array([valid, _wkb(con, BOWTIE_WKT), valid], pa.binary())})
    repaired, n = mod.repair_arrow_table_geometry(table, "geometry", repair=True)
    assert n == 1
    column = repaired.column("geometry")
    assert column.num_chunks == 3
    assert column.to_pylist()[0] == column.to_pylist()[2] == valid
    assert con.execute("SELECT ST_IsValid(ST_GeomFromWKB(?))", [column[1].as_py()]).fetchone()[0]


def test_registered_dictionary_geometry_reads_flat(con):
    """A dictionary-typed WKB column is decoded on registration, so flat SQL reads it."""
    from geoparquet_io.core.geometry_detection import register_geometry_column

    point = _wkb(con, "POINT(1 2)")
    geometry = pa.array([point, b"not wkb", b"", None, point]).dictionary_encode()
    table = pa.table({"geometry": geometry})
    read = register_geometry_column(con, "__dict_src", table, "geometry")
    assert read.kind == "wkb" and read.flat
    rows = con.execute(
        f"SELECT {read.parseable_sql}, ST_AsText({read.expr}) FROM __dict_src"
    ).fetchall()
    assert rows == [
        (True, "POINT (1 2)"),
        (False, None),
        (False, None),
        (False, None),
        (True, "POINT (1 2)"),
    ]
