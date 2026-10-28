"""A rewritten GeoParquet file must not carry the input's ``pandas`` block.

#710 settled the policy: ``geo``, ``ARROW:schema``, ``pandas`` and
``geo:overviews`` describe the *input's* schema and rows, so a write that
rebuilds the rows regenerates them or drops them, and never copies them through.
Every other key (fiboa, STAC, provenance) survives (#690).

The DuckDB strategies kept that rule because ``extract_preserved_kv_metadata``
builds their KV block. The Arrow strategies (``in-memory``, ``streaming``) and
the Arrow-table funnel ``write_geoparquet_table`` copied ``table.schema.metadata``
through verbatim, so ``gpio.read(path).sort_hilbert().write(out)`` wrote a
``pandas`` block whose index no longer matched the rows (#1268 item 2). The fix
filters once, in the write facade (``parquet_writer.output_schema_metadata``).
"""

from __future__ import annotations

import base64
import json
import struct

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import geoparquet_io as gpio
from geoparquet_io.core.geo_metadata import OVERVIEWS_KEY
from geoparquet_io.core.parquet_writer import (
    apply_output_kv_metadata,
    output_schema_metadata,
)
from geoparquet_io.core.write_funnels import (
    extract_preserved_kv_metadata,
    write_geoparquet_table,
)
from geoparquet_io.core.write_strategies import WriteStrategy, WriteStrategyFactory

STRATEGIES = [s.value for s in WriteStrategy]
PROVENANCE = b'{"by": "test"}'
EXPLICIT_OVERVIEWS = '{"note": "explicit overviews"}'
N = 20

STALE_GEO = {
    "version": "1.0.0",
    "primary_column": "geometry",
    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
}


def _point_wkb(x: float, y: float) -> bytes:
    return struct.pack("<BI2d", 1, 1, x, y)


def _carried_table(df: pd.DataFrame | None = None) -> pa.Table:
    """A table built the way pandas builds one, so it carries a real ``pandas`` block.

    The index is ``RangeIndex(0, 2N, 2)``, which pandas stores as metadata only.
    Points run diagonally in reverse, so a Hilbert sort reorders the rows.
    """
    if df is None:
        df = pd.DataFrame({"id": range(N)}, index=pd.RangeIndex(0, 2 * N, 2))
    table = pa.Table.from_pandas(df)
    table = table.append_column(
        "geometry", pa.array([_point_wkb(N - i, N - i) for i in range(len(df))], pa.binary())
    )
    metadata = dict(table.schema.metadata)
    assert b"pandas" in metadata
    metadata[b"geo"] = json.dumps(STALE_GEO).encode()
    metadata[b"provenance"] = PROVENANCE
    metadata[OVERVIEWS_KEY.encode()] = b'{"levels": []}'
    return table.replace_schema_metadata(metadata)


@pytest.fixture
def carried_source(tmp_path):
    """A file whose footer has geo, pandas, ARROW:schema, geo:overviews and provenance."""
    src = tmp_path / "src.parquet"
    pq.write_table(_carried_table(), src)
    assert b"ARROW:schema" in pq.ParquetFile(src).metadata.metadata
    return src


def _footer(path) -> dict[bytes, bytes]:
    return dict(pq.ParquetFile(str(path)).metadata.metadata or {})


def _assert_no_carried_keys(path, *, keeps_provenance: bool = True) -> dict:
    """Assert the carried keys are gone, inside ARROW:schema too, and return geo."""
    footer = _footer(path)
    assert b"pandas" not in footer
    assert OVERVIEWS_KEY.encode() not in footer
    if b"ARROW:schema" in footer:
        inner = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(footer[b"ARROW:schema"])))
        assert b"pandas" not in (inner.metadata or {})
    if keeps_provenance:
        assert footer[b"provenance"] == PROVENANCE
    geo = json.loads(footer[b"geo"])
    assert geo["primary_column"] == "geometry"
    # The regenerated block, not the input's 1.0.0 copy.
    assert geo["version"] == "1.1.0"
    return geo


ENTRY_POINTS = {
    "read().write": lambda src: gpio.read(str(src)),
    "read().add_bbox().write": lambda src: gpio.read(str(src)).add_bbox(),
    "read().sort_hilbert().write": lambda src: gpio.read(str(src)).sort_hilbert(),
    "convert(parquet).write": lambda src: gpio.convert(str(src)),
}


@pytest.mark.parametrize("strategy", STRATEGIES)
@pytest.mark.parametrize("entry", list(ENTRY_POINTS))
def test_table_write_drops_carried_keys(entry, strategy, carried_source, tmp_path):
    out = tmp_path / "out.parquet"

    ENTRY_POINTS[entry](carried_source).write(
        str(out), geoparquet_version="1.1", write_strategy=strategy
    )

    # convert() drops every non-geo key on main; #1280 makes it keep them.
    geo = _assert_no_carried_keys(out, keeps_provenance=entry != "convert(parquet).write")
    if entry == "read().add_bbox().write":
        assert "covering" in geo["columns"]["geometry"]


def test_write_geoparquet_table_drops_carried_keys(carried_source, tmp_path):
    out = tmp_path / "out.parquet"

    write_geoparquet_table(pq.read_table(carried_source), str(out), geoparquet_version="1.1")

    _assert_no_carried_keys(out)


def test_write_geoparquet_table_parquet_geo_only_drops_geo_too(carried_source, tmp_path):
    out = tmp_path / "out.parquet"

    write_geoparquet_table(
        pq.read_table(carried_source), str(out), geoparquet_version="parquet-geo-only"
    )

    footer = _footer(out)
    assert b"geo" not in footer
    assert b"pandas" not in footer
    assert footer[b"provenance"] == PROVENANCE


# ---------------------------------------------------------------------------
# The harm the stale block did, measured in #1268's investigation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("strategy", ["in-memory", "streaming"])
def test_index_labels_do_not_lie_after_a_hilbert_sort(strategy, carried_source, tmp_path):
    """The stale block relabelled sorted rows with the input's index 0, 2, 4, ..."""
    out = tmp_path / "sorted.parquet"

    gpio.read(str(carried_source)).sort_hilbert().write(str(out), write_strategy=strategy)

    df = pq.read_table(out).to_pandas()
    assert list(df["id"]) != list(range(N)), "fixture must make the sort reorder rows"
    assert isinstance(df.index, pd.RangeIndex)
    assert list(df.index) == list(range(N))


@pytest.mark.parametrize("strategy", ["in-memory", "streaming"])
def test_stale_timezone_does_not_shift_naive_values(strategy, tmp_path):
    """A naive timestamp under a stale ``datetimetz`` entry read back an hour off."""
    aware = pd.DataFrame(
        {"ts": pd.date_range("2024-01-01 12:00", periods=N, freq="h", tz="Europe/Paris")}
    )
    table = _carried_table(aware)
    naive = pa.array(
        pd.date_range("2024-01-01 12:00", periods=N, freq="h").to_numpy(), pa.timestamp("ns")
    )
    table = table.set_column(table.schema.get_field_index("ts"), "ts", naive)
    src = tmp_path / "stale_tz.parquet"
    pq.write_table(table, src)
    out = tmp_path / "out.parquet"

    gpio.read(str(src)).write(str(out), write_strategy=strategy)

    first = pq.read_table(out).to_pandas()["ts"].iloc[0]
    assert first == pd.Timestamp("2024-01-01 12:00")


# ---------------------------------------------------------------------------
# Guard: every registered strategy goes through the one filter
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("version", ["1.1", "2.0", "parquet-geo-only"])
@pytest.mark.parametrize("strategy", list(WriteStrategy))
def test_every_registered_strategy_drops_carried_keys(strategy, version, tmp_path):
    """Enumerates the registry, so a strategy added later is covered without edits."""
    out = tmp_path / "out.parquet"
    table = _carried_table()

    # The extras are what Table.write hands every strategy; the DuckDB strategies
    # rebuild the KV block and keep only what they are given.
    WriteStrategyFactory.get_strategy(strategy).write_from_table(
        table=table,
        output_path=str(out),
        geometry_column="geometry",
        geoparquet_version=version,
        compression="ZSTD",
        compression_level=None,
        row_group_size_mb=None,
        row_group_rows=None,
        verbose=False,
        extra_kv_metadata=extract_preserved_kv_metadata(table.schema.metadata),
    )

    footer = _footer(out)
    assert b"pandas" not in footer
    assert OVERVIEWS_KEY.encode() not in footer
    assert footer[b"provenance"] == PROVENANCE
    if version == "parquet-geo-only":
        assert b"geo" not in footer
    else:
        assert json.loads(footer[b"geo"])["version"] == f"{version}.0"


# ---------------------------------------------------------------------------
# Unit: output_schema_metadata / apply_output_kv_metadata
# ---------------------------------------------------------------------------


class TestOutputSchemaMetadata:
    def test_none_metadata(self):
        assert output_schema_metadata(None, "1.1") == {}

    def test_only_carried_keys_keeps_geo(self):
        metadata = {
            b"geo": b"{}",
            b"pandas": b"{}",
            b"ARROW:schema": b"x",
            OVERVIEWS_KEY.encode(): b"{}",
        }
        assert output_schema_metadata(metadata, "1.1") == {b"geo": b"{}"}

    def test_parquet_geo_only_drops_geo(self):
        metadata = {b"geo": b"{}", b"pandas": b"{}", b"fiboa": b"1"}
        assert output_schema_metadata(metadata, "parquet-geo-only") == {b"fiboa": b"1"}

    def test_str_keys_are_filtered_and_encoded(self):
        metadata = {"geo": "{}", "pandas": "{}", "fiboa": "1"}
        assert output_schema_metadata(metadata, "parquet-geo-only") == {b"fiboa": b"1"}

    def test_keeps_geo_prefixed_keys_that_are_not_carried(self):
        metadata = {b"geography_note": b"kept", b"pandas": b"{}"}
        assert output_schema_metadata(metadata, "1.1") == {b"geography_note": b"kept"}

    def test_extra_kv_overrides_a_kept_key(self):
        metadata = {b"fiboa": b"old", b"pandas": b"{}"}
        result = output_schema_metadata(metadata, "1.1", {"fiboa": "new", b"stac": b"s"})
        assert result == {b"fiboa": b"new", b"stac": b"s"}

    def test_apply_is_zero_copy(self):
        table = _carried_table()
        result = apply_output_kv_metadata(table, "1.1")
        assert b"pandas" not in result.schema.metadata
        assert b"geo" in result.schema.metadata
        assert result.column("id").chunk(0).buffers()[1].address == (
            table.column("id").chunk(0).buffers()[1].address
        )

    def test_apply_with_no_metadata(self):
        table = pa.table({"id": [1]})
        assert not apply_output_kv_metadata(table, "1.1").schema.metadata


def test_assembled_geo_block_keeps_geo_prefixed_sidecars():
    """The in-memory pre-step used ``startswith("geo")`` and dropped unrelated keys."""
    from geoparquet_io.core.arrow_geo_metadata import _assemble_and_apply_geo_metadata

    table = pa.table({"id": [1]}).replace_schema_metadata(
        {
            b"geo": b'{"stale": true}',
            b"geography_note": b"kept",
            b"pandas": b"{}",
            OVERVIEWS_KEY.encode(): b"{}",
        }
    )

    result = _assemble_and_apply_geo_metadata(table, {"version": "1.1.0"}, "1.1.0", False)

    assert result.schema.metadata == {
        b"geography_note": b"kept",
        b"geo": b'{"version": "1.1.0"}',
    }


def test_disk_rewrite_lets_an_explicit_geo_extra_win(tmp_path):
    """The caller's extras merge last, over the generated ``geo``, as before the fix."""
    out = tmp_path / "out.parquet"
    explicit = json.dumps({**STALE_GEO, "version": "1.1.0", "note": "explicit"})

    WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE).write_from_table(
        table=_carried_table(),
        output_path=str(out),
        geometry_column="geometry",
        geoparquet_version="1.1",
        compression="ZSTD",
        compression_level=None,
        row_group_size_mb=None,
        row_group_rows=None,
        verbose=False,
        extra_kv_metadata={"geo": explicit},
    )

    footer = _footer(out)
    assert json.loads(footer[b"geo"])["note"] == "explicit"
    assert b"pandas" not in footer


@pytest.mark.parametrize("strategy", list(WriteStrategy))
def test_caller_extras_named_like_carried_keys_survive(strategy, tmp_path):
    """The drop is for keys carried from the input, never for the caller's extras.

    The in-memory path applied the facade twice and the second pass, with no
    extras, dropped a ``pandas`` extra the first pass had merged in.
    """
    out = tmp_path / "out.parquet"
    explicit_pandas = '{"note": "explicit"}'

    WriteStrategyFactory.get_strategy(strategy).write_from_table(
        table=_carried_table(),
        output_path=str(out),
        geometry_column="geometry",
        geoparquet_version="1.1",
        compression="ZSTD",
        compression_level=None,
        row_group_size_mb=None,
        row_group_rows=None,
        verbose=False,
        extra_kv_metadata={
            "pandas": explicit_pandas,
            OVERVIEWS_KEY: EXPLICIT_OVERVIEWS,
            "provenance": PROVENANCE.decode(),
        },
    )

    footer = _footer(out)
    assert footer[b"pandas"] == explicit_pandas.encode()
    assert footer[OVERVIEWS_KEY.encode()] == EXPLICIT_OVERVIEWS.encode()
    assert footer[b"provenance"] == PROVENANCE


def test_query_write_in_memory_keeps_a_caller_pandas_extra(tmp_path):
    """The in-memory ``write_from_query`` must hand its extras to the final write too.

    ``_write_table_with_settings`` applies the facade again just before
    ``pq.write_table``. Without the extras there, it drops a ``pandas`` extra.
    """
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.write_funnels import write_parquet_with_metadata

    out = tmp_path / "out.parquet"
    explicit_pandas = '{"note": "explicit"}'
    con = get_duckdb_connection(load_spatial=True)
    try:
        write_parquet_with_metadata(
            con,
            "SELECT 1 AS id, ST_Point(1, 2) AS geometry",
            str(out),
            geoparquet_version="1.1",
            write_strategy="in-memory",
            extra_kv_metadata={"pandas": explicit_pandas, "provenance": PROVENANCE.decode()},
        )
    finally:
        con.close()

    footer = _footer(out)
    assert footer[b"pandas"] == explicit_pandas.encode()
    assert footer[b"provenance"] == PROVENANCE
