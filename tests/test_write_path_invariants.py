"""G7: every write path gives one fixture the same output policy (#1367).

One input -- 60,000 points in ITRF2014 (EPSG:9000) with an undeclared ``bbox`` column, ``collection`` and
``fiboa`` sidecar keys, and a 1.1 ``geo`` block carrying ``epoch`` and
``edges: spherical`` -- written SNAPPY in 10,000-row groups, so a path that
passes the input's layout through is visible against the facade's ZSTD and
49,152-row groups. It goes through every write path the consolidation touches:
``convert geoparquet``, ``sort hilbert``, ``extract``, ``check bbox --fix``,
``add bbox-metadata``, ``process aggregate h3 --out-geometry none``,
``partition h3`` (first output file) and ``Table.write`` with each of the four
strategies. Only real CLI calls and the public API; nothing is mocked.

Each invariant is its own parametrized assert, so a strict xfail names exactly
the (path, invariant) cell that fails today and the issue that owns it. Cells
keyed to #1367 have no open issue of their own: the fix lands in T2/T3 of the
consolidation.

Compression *level* is not stored in a Parquet file. It is read off the
geometry column chunk instead: the output's own geometry values are re-encoded
at ZSTD level 3 and level 15, once by pyarrow and once by DuckDB (the two
encoders gpio writes with), with the output's row-group sizes, and the level of
the nearest reference is the level the path wrote. The Arrow strategies land
exactly on pyarrow's level-15 size and ``duckdb-kv`` exactly on DuckDB's level
15 (it landed on DuckDB's 3 before #1335); paths that stream through batches land within a few hundred bytes of a
reference and thousands away from the other level.
"""

from __future__ import annotations

import json
import math
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.parquet_writer import DEFAULT_ROW_GROUP_ROWS, resolve_row_group_rows
from tests._defrag_fixtures import (
    EPOCH,
    SIDECAR_KEYS,
    footer_kv,
    geo_block,
    grid_points_table,
    write_with_geo,
)
from tests.native_geo_probes import projjson

N_ROWS = 60_000
#: ITRF2014 geographic 2D: a dynamic datum, not an ensemble, so an ``epoch`` on
#: it is one ``check spec`` accepts (an epoch on CRS84's WGS 84 ensemble fails).
DYNAMIC_CRS_EPSG = 9000
EDGES = "spherical"
STRATEGIES = ("duckdb-kv", "in-memory", "streaming", "disk-rewrite")

CLI_PATHS = (
    "convert_geoparquet",
    "sort_hilbert",
    "extract",
    "check_bbox_fix",
    "add_bbox_metadata",
    "aggregate_h3_none",
    "partition_h3",
)
PATHS = CLI_PATHS + tuple(f"table_write_{s}" for s in STRATEGIES)

#: Paths whose output has no geometry by request: no ``geo`` block to compare.
GEOMETRY_FREE = {"aggregate_h3_none"}
#: An aggregate is a derived table, not a rewrite of its input, so it carries
#: the same file-level keys whichever geometry it is asked for (the lead's
#: policy for #1367 T2c): the geometry-free output is held to the geometry
#: branch's keys, not to the input's sidecar keys.
#:
#: A divergence guard, not a repro: this cell passes on the source before T2c
#: too, because both branches already wrote the same keys. It pins that the
#: geometry-free door (``write_plain_parquet_table``) keeps agreeing with the
#: geometry branch from here on; it does not show the T2c change.
SIDECAR_REFERENCE = {"aggregate_h3_none": "aggregate_h3_polygon"}

#: Metadata-only commands: a footer patch, not a write. Their pages are the
#: input's, byte for byte, so on the codec, row-group and level axes they are
#: held to the input, not to the facade (the lead's policy for #1367 T2d: only a
#: footer that cannot be patched is rewritten, and that rewrite goes through the
#: funnel and takes the facade's policy). This matrix runs the patchable cell
#: only. The unpatchable cell -- the facade's codec and row groups, every
#: metadata-only command -- is owned by tests/test_patch_or_rewrite.py's table
#: (``test_an_unpatchable_footer_rewrites_through_the_funnel_keeping_the_same_keys``),
#: so it is not repeated here.
METADATA_ONLY = {"check_bbox_fix", "add_bbox_metadata"}
#: The invariants a footer patch answers by passing the input's pages through.
LAYOUT_INVARIANTS = {"codec", "row_groups", "level"}

XFAIL: dict[tuple[str, str], str] = {}

# ---------------------------------------------------------------------------
# Running every path once
# ---------------------------------------------------------------------------


def _invoke(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, (
        f"gpio {' '.join(map(str, args))} failed: {result.output[-1500:]} {result.exception!r}"
    )


def _run_cli(name: str, src: Path, out_dir: Path) -> Path:
    out = out_dir / f"{name}.parquet"
    if name == "convert_geoparquet":
        _invoke("convert", "geoparquet", src, out)
    elif name == "sort_hilbert":
        _invoke("sort", "hilbert", src, out)
    elif name == "extract":
        _invoke("extract", src, out)
    elif name == "check_bbox_fix":
        _invoke("check", "bbox", src, "--fix", "--fix-output", out)
    elif name == "add_bbox_metadata":
        shutil.copy(src, out)
        _invoke("add", "bbox-metadata", out)
    elif name == "aggregate_h3_none":
        # Resolution 15 puts every point in its own cell, so the geometry-free
        # output has as many rows as the input and the row-group policy shows.
        _invoke(
            "process", "aggregate", "h3", src, out, "--resolution", "15", "--out-geometry", "none"
        )
    elif name == "aggregate_h3_polygon":
        _invoke(
            "process",
            "aggregate",
            "h3",
            src,
            out,
            "--resolution",
            "15",
            "--out-geometry",
            "polygon",
        )
    elif name == "partition_h3":
        part = out_dir / "partition_h3"
        _invoke("partition", "h3", src, part, "--resolution", "1", "--skip-analysis")
        out = sorted(part.rglob("*.parquet"))[0]
    return out


@pytest.fixture(scope="module")
def source(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("g7") / "source.parquet"
    write_with_geo(
        path,
        grid_points_table(N_ROWS),
        geo_block(epoch=EPOCH, edges=EDGES, crs=json.loads(projjson(DYNAMIC_CRS_EPSG))),
        extra_kv=SIDECAR_KEYS,
        compression="snappy",
        row_group_size=10_000,
    )
    return path


@pytest.fixture(scope="module")
def outputs(source, tmp_path_factory) -> dict[str, Path]:
    out_dir = tmp_path_factory.mktemp("g7_out")
    result = {
        name: _run_cli(name, source, out_dir) for name in (*CLI_PATHS, *SIDECAR_REFERENCE.values())
    }
    for strategy in STRATEGIES:
        out = out_dir / f"table_write_{strategy}.parquet"
        gpio.read(str(source)).write(str(out), write_strategy=strategy)
        result[f"table_write_{strategy}"] = out
    return result


# ---------------------------------------------------------------------------
# Reading what a path wrote
# ---------------------------------------------------------------------------


def _columns(path: Path) -> list[pq.ColumnChunkMetaData]:
    md = pq.ParquetFile(str(path)).metadata
    return [
        md.row_group(r).column(c) for r in range(md.num_row_groups) for c in range(md.num_columns)
    ]


def _geo(path: Path) -> dict | None:
    raw = footer_kv(path).get(b"geo")
    return None if raw is None else json.loads(raw)


def _primary_column_meta(path: Path) -> dict:
    geo = _geo(path)
    assert geo is not None, f"{path.name} has no geo block"
    return geo["columns"][geo["primary_column"]]


def _geometry_chunk_bytes(path: Path) -> int:
    return sum(
        chunk.total_compressed_size
        for chunk in _columns(path)
        if chunk.path_in_schema == "geometry"
    )


def _reference_sizes(path: Path, tmp: Path) -> dict[tuple[str, int], int]:
    """The output's geometry values re-encoded at ZSTD 3 and 15 by each encoder."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

    md = pq.ParquetFile(str(path)).metadata
    groups = [md.row_group(r).num_rows for r in range(md.num_row_groups)]
    column = pq.read_table(str(path), columns=["geometry"]).column("geometry")
    if not pa.types.is_binary(column.type):
        column = (
            column.cast(pa.binary())
            if not hasattr(column.type, "storage_type")
            else (
                pa.chunked_array(
                    [chunk.storage for chunk in column.chunks], column.type.storage_type
                )
            )
        )
    table = pa.table({"geometry": column})
    sizes: dict[tuple[str, int], int] = {}
    for level in (3, 15):
        arrow_ref = tmp / f"arrow_{level}.parquet"
        with pq.ParquetWriter(
            str(arrow_ref),
            table.schema,
            compression="zstd",
            compression_level=level,
            use_dictionary=False,
        ) as writer:
            offset = 0
            for rows in groups:
                writer.write_table(table.slice(offset, rows), row_group_size=rows)
                offset += rows
        sizes[("pyarrow", level)] = _geometry_chunk_bytes(arrow_ref)

        duck_ref = tmp / f"duckdb_{level}.parquet"
        con = get_duckdb_connection()
        try:
            con.register("ref", table)
            con.execute(
                f"COPY ref TO {sql_path(str(duck_ref))} (FORMAT parquet, COMPRESSION zstd, "
                f"COMPRESSION_LEVEL {level}, ROW_GROUP_SIZE {groups[0]})"
            )
        finally:
            con.close()
        sizes[("duckdb", level)] = _geometry_chunk_bytes(duck_ref)
    return sizes


# ---------------------------------------------------------------------------
# The invariants
# ---------------------------------------------------------------------------


def _data_section(path: Path) -> bytes:
    """Every byte below the footer: the pages a footer patch copies verbatim."""
    raw = path.read_bytes()
    footer_length = int.from_bytes(raw[-8:-4], "little")
    return raw[: len(raw) - 8 - footer_length]


def _check_pages_passed_through(path: Path, source: Path, _tmp: Path) -> None:
    assert _data_section(path) == _data_section(source), (
        f"{path.name}: a metadata-only command re-encoded the input's pages"
    )


def _check_codec(path: Path, _source: Path, _tmp: Path) -> None:
    codecs = {(c.path_in_schema, c.compression) for c in _columns(path)}
    assert {codec for _, codec in codecs} == {"ZSTD"}, sorted(codecs)


def _check_row_groups(path: Path, _source: Path, _tmp: Path) -> None:
    md = pq.ParquetFile(str(path)).metadata
    rows_per_group = resolve_row_group_rows(None, None)
    assert rows_per_group == DEFAULT_ROW_GROUP_ROWS
    expected = math.ceil(md.num_rows / rows_per_group)
    actual = [md.row_group(r).num_rows for r in range(md.num_row_groups)]
    assert md.num_row_groups == expected, f"{md.num_rows} rows in groups of {actual}"


def _check_sidecar_keys(path: Path, _source: Path, _tmp: Path) -> None:
    kv = footer_kv(path)
    carried = {key: kv.get(key) for key in SIDECAR_KEYS}
    assert carried == SIDECAR_KEYS, f"footer keys: {sorted(kv)}"


def _check_epoch(path: Path, _source: Path, _tmp: Path) -> None:
    assert _primary_column_meta(path).get("epoch") == EPOCH


def _check_edges(path: Path, _source: Path, _tmp: Path) -> None:
    assert _primary_column_meta(path).get("edges") == EDGES


def _check_primary_column(path: Path, source: Path, _tmp: Path) -> None:
    assert _geo(path)["primary_column"] == _geo(source)["primary_column"] == "geometry"


def _check_spec(path: Path, _source: Path, _tmp: Path) -> None:
    result = CliRunner().invoke(cli, ["check", "spec", str(path)])
    assert result.exit_code == 0, result.output[-2000:]


def _check_level(path: Path, _source: Path, tmp: Path) -> None:
    codecs = {c.compression for c in _columns(path) if c.path_in_schema == "geometry"}
    assert codecs == {"ZSTD"}, f"geometry codec {codecs}: no ZSTD level to read"
    refs = _reference_sizes(path, tmp)
    if all(refs[(enc, 3)] == refs[(enc, 15)] for enc in ("pyarrow", "duckdb")):
        pytest.skip(f"{path.name}: levels 3 and 15 encode its geometry identically: {refs}")
    actual = _geometry_chunk_bytes(path)
    nearest = min(refs, key=lambda key: abs(refs[key] - actual))
    assert nearest[1] == 15, f"geometry chunk {actual} B is nearest {nearest}: {refs}"


INVARIANTS = {
    "codec": _check_codec,
    "row_groups": _check_row_groups,
    "sidecar_keys": _check_sidecar_keys,
    "epoch": _check_epoch,
    "edges": _check_edges,
    "primary_column": _check_primary_column,
    "check_spec": _check_spec,
    "level": _check_level,
}
#: Invariants about the geo block, the geometry column or the spec, which a
#: geometry-free output cannot have. For those paths the facade rule is that
#: no ``geo`` key is written at all, asserted instead.
GEO_INVARIANTS = {"epoch", "edges", "primary_column", "check_spec", "level"}


def _cases():
    for path in PATHS:
        for invariant in INVARIANTS:
            if path in GEOMETRY_FREE and invariant in GEO_INVARIANTS:
                continue
            marks = []
            if (path, invariant) in XFAIL:
                marks.append(pytest.mark.xfail(strict=True, reason=XFAIL[(path, invariant)]))
            yield pytest.param(path, invariant, marks=marks, id=f"{path}-{invariant}")


@pytest.mark.parametrize(("path", "invariant"), list(_cases()))
def test_write_path_invariant(path, invariant, outputs, source, tmp_path):
    if invariant == "sidecar_keys" and path in SIDECAR_REFERENCE:
        reference = outputs[SIDECAR_REFERENCE[path]]
        keys = set(footer_kv(outputs[path])) - {b"geo"}
        assert keys == set(footer_kv(reference)) - {b"geo"}, sorted(keys)
        return
    if path in METADATA_ONLY and invariant in LAYOUT_INVARIANTS:
        _check_pages_passed_through(outputs[path], source, tmp_path)
        return
    INVARIANTS[invariant](outputs[path], source, tmp_path)


@pytest.mark.parametrize("path", sorted(GEOMETRY_FREE))
def test_geometry_free_output_carries_no_geo_key(path, outputs):
    out = outputs[path]
    assert "geometry" not in pq.read_schema(str(out)).names
    assert _geo(out) is None


def test_the_fixture_makes_every_invariant_observable(source):
    """Non-vacuity: the input differs from the facade's policy on every axis."""
    md = pq.ParquetFile(str(source)).metadata
    assert md.num_rows > DEFAULT_ROW_GROUP_ROWS
    assert md.num_row_groups != math.ceil(md.num_rows / DEFAULT_ROW_GROUP_ROWS)
    assert {c.compression for c in _columns(source)} == {"SNAPPY"}
    column = _primary_column_meta(source)
    assert column["epoch"] == EPOCH and column["edges"] == EDGES
    assert "covering" not in column, "the bbox column must be undeclared for the fix paths"
    assert all(footer_kv(source).get(k) == v for k, v in SIDECAR_KEYS.items())
