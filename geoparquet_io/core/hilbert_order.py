#!/usr/bin/env python3

from __future__ import annotations

from itertools import pairwise

import pyarrow as pa

from geoparquet_io.core.arrow_types import view_offset_type
from geoparquet_io.core.bbox_structure import dataset_bounds
from geoparquet_io.core.column_selection import unique_name
from geoparquet_io.core.duckdb_utils import (
    fetch_one,
    get_duckdb_connection,
    quote_identifier,
)
from geoparquet_io.core.geo_metadata import QuerySource, bounds_of
from geoparquet_io.core.geometry_detection import (
    register_geometry_column,
    resolve_geometry_column,
    resolve_geometry_read,
)
from geoparquet_io.core.logging_config import info, warn
from geoparquet_io.core.optional_deps import load_module
from geoparquet_io.core.parquet_writer import (
    DEFAULT_ROW_GROUP_ROWS,
    WriteOptions,
    resolve_row_group_rows,
)
from geoparquet_io.core.sizing import BYTES_PER_MIB
from geoparquet_io.core.sort_common import advised_output_version, run_sort

# Bytes of output a gather batch builds at once. The chunk-wise gather holds at
# most about twice this beyond the output, against a whole column for a
# concatenate-then-take, and each output chunk stays far below the 2 GiB a
# 32-bit offset addresses.
_GATHER_BATCH_BYTES = 16 * BYTES_PER_MIB
_GATHER_THREADS = 4


def _row_index_name(geom_col: str) -> str:
    """An index column name DuckDB cannot confuse with ``geom_col`` (identifiers fold case)."""
    return unique_name("__gpio_row_idx", [geom_col])


def _row_bytes(column: pa.ChunkedArray):
    """Approximate bytes each row of ``column`` adds to an output: exact for binary/string."""
    np = load_module("numpy")
    import pyarrow.compute as pc

    sizes = []
    for chunk in column.chunks:
        if (
            pa.types.is_binary(chunk.type)
            or pa.types.is_string(chunk.type)
            or (pa.types.is_large_binary(chunk.type) or pa.types.is_large_string(chunk.type))
        ):
            lengths = pc.binary_length(chunk).fill_null(0).to_numpy(zero_copy_only=False)
            sizes.append(lengths.astype(np.int64) + 8)
        else:
            per_row = chunk.nbytes // max(len(chunk), 1) + 1
            sizes.append(np.full(len(chunk), per_row, dtype=np.int64))
    return np.concatenate(sizes) if sizes else np.zeros(0, dtype=np.int64)


def _gather_batch(chunks, starts, batch):
    """``batch`` (global row positions) gathered from ``chunks``, chunk by chunk."""
    np = load_module("numpy")

    owner = np.searchsorted(starts, batch, side="right") - 1
    order = np.argsort(owner, kind="stable")
    counts = np.bincount(owner, minlength=len(chunks))
    pieces, pos = [], 0
    for c, count in enumerate(counts):
        if count:
            local = batch[order[pos : pos + count]] - starts[c]
            pieces.append(chunks[c].take(pa.array(local)))
            pos += count
    grouped = pa.concat_arrays(pieces) if len(pieces) > 1 else pieces[0]
    if len(pieces) == 1:
        return grouped
    inverse = np.empty_like(order)
    inverse[order] = np.arange(len(order))
    return grouped.take(pa.array(inverse))


def _gather_column(column: pa.ChunkedArray, indices) -> pa.ChunkedArray:
    """``column.take(indices)`` without concatenating ``column``'s chunks first.

    pyarrow's ``take`` on a multi-chunk column concatenates the chunks, which
    for a binary or string column past 2 GiB overflows its 32-bit offsets
    (ArrowInvalid) and otherwise holds a second copy of the column. Gathering
    each output batch straight from the chunks it touches avoids both.
    """
    np = load_module("numpy")

    twin = view_offset_type(column.type)
    if twin is not None:
        gathered = _gather_column(column.cast(twin), indices)
        return pa.chunked_array([c.cast(column.type) for c in gathered.chunks], type=column.type)
    if column.num_chunks == 1:
        return pa.chunked_array([column.chunk(0).take(pa.array(indices))], type=column.type)
    if pa.types.is_dictionary(column.type) or column.num_chunks == 0:
        return column.take(pa.array(indices))

    chunks = column.chunks
    starts = np.cumsum([0] + [len(c) for c in chunks])
    # Cut the output wherever its running byte total crosses another batch.
    cumulative = np.cumsum(_row_bytes(column)[indices])
    marks = np.arange(_GATHER_BATCH_BYTES, cumulative[-1], _GATHER_BATCH_BYTES)
    ends = np.unique(np.searchsorted(cumulative, marks) + 1)
    bounds = [0, *ends[ends < len(indices)].tolist(), len(indices)]
    out = [_gather_batch(chunks, starts, indices[lo:hi]) for lo, hi in pairwise(bounds)]
    return pa.chunked_array(out, type=column.type)


def _take_rows(table: pa.Table, indices) -> pa.Table:
    """``table`` reordered to ``indices`` (numpy int64, one per row).

    Columns are gathered on a few threads (pyarrow releases the GIL), which
    keeps the reorder as fast as ``Table.take``; each holds at most a couple
    of batches beyond the output, so the transient stays bounded.
    """
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=_GATHER_THREADS) as pool:
        columns = list(pool.map(lambda col: _gather_column(col, indices), table.columns))
    return pa.Table.from_arrays(columns, schema=table.schema)


def hilbert_order_table(
    table: pa.Table,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Reorder an Arrow Table using Hilbert curve ordering.

    This is the table-centric version for the Python API.

    Empty or NULL geometries (and WKB that does not parse) are placed at the
    end of the result, in their input order, since they have no spatial extent
    and cannot be assigned a Hilbert curve index. Rows with equal Hilbert keys
    keep their input order too, so the result is deterministic.

    DuckDB sees only the geometry column and a row index, and hands back the
    sorted *indices*; the result is then gathered from the caller's own Arrow
    chunks (``_take_rows``). Round-tripping the whole table through DuckDB
    instead — sort the full rows, ``read_all()`` the sorted copy — held three
    copies of the data at once and peaked at ~11x the input file's size
    (#1268). The gather also keeps what a round-trip destroyed: column order,
    the geometry column's exact bytes and encoding, and every attribute's
    Arrow type. The geometry bytes are not re-encoded, so EWKB stays EWKB.

    Args:
        table: Input PyArrow Table
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with rows reordered by Hilbert curve
    """
    geom_col = resolve_geometry_column(table, geometry_column, where="the table")
    idx_col = _row_index_name(geom_col)
    quoted_idx = quote_identifier(idx_col)

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        read = register_geometry_column(con, "__input_table", table, geom_col, row_index=idx_col)
        # One projection of the sort key, shared by both queries below. The
        # owner's read expression, not the raw column: a GeoArrow coordinate
        # struct is no argument to ST_IsEmpty or ST_Hilbert, and `expr` hands
        # those its representative point instead of dying on it (#1438).
        source_ref = f"(SELECT {quoted_idx}, {read.expr} AS __g FROM __input_table)"

        # The empty/NULL count, then the owner's bounds of everything else. The
        # bounds are measured on the column itself, so a GeoArrow extent is the
        # geometry's and not its representative point's.
        (empty_count,) = fetch_one(
            con.execute(f"SELECT COUNT(*) FROM {source_ref} WHERE __g IS NULL OR ST_IsEmpty(__g)")
        )
        bbox = bounds_of(QuerySource(con, "SELECT * FROM __input_table"), geom_col)

        if empty_count > 0:
            warn(
                f"Found {empty_count} empty/null geometries. "
                "These will be placed at the end of the sorted output."
            )
        if bbox is None:
            warn("All geometries are empty or null. Returning table without Hilbert ordering.")
            return table
        xmin, ymin, xmax, ymax = bbox

        # Sorted row indices: non-empty geometries in Hilbert order, then the
        # empty/NULL rows (a NULL key, sorted last); the index breaks every tie.
        # DuckDB sorts (key, index) pairs — 16 bytes a row — never the rows.
        ordered = con.execute(f"""
            SELECT {quoted_idx} FROM {source_ref}
            ORDER BY CASE WHEN __g IS NULL OR ST_IsEmpty(__g) THEN NULL
                ELSE ST_Hilbert(__g, ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})))
                END NULLS LAST,
                {quoted_idx}
        """).fetchnumpy()[idx_col]
    finally:
        con.close()

    # The gather keeps the schema, so the caller's metadata travels as-is.
    return _take_rows(table, ordered.astype("int64", copy=False))


def hilbert_order(
    input_parquet: str,
    output_parquet: str | None = None,
    geometry_column: str | None = None,
    add_bbox_flag: bool = False,
    verbose: bool = False,
    compression: str | None = None,
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    memory_limit: str | None = None,
) -> None:
    """
    Reorder a GeoParquet file using Hilbert curve ordering.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Args:
        input_parquet: Path to input GeoParquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        geometry_column: Name of geometry column (default: the primary geometry column)
        add_bbox_flag: Add bbox column before sorting if not present
        verbose: Print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group. When neither this
            nor ``row_group_size_mb`` is given, the sort default
            (``DEFAULT_ROW_GROUP_ROWS``) applies.
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        memory_limit: DuckDB memory limit for streaming writes (e.g., '2GB', '512MB')
    """
    row_group_rows = resolve_row_group_rows(row_group_rows, row_group_size_mb)
    effective_version = advised_output_version(input_parquet, geoparquet_version, verbose, profile)

    if (
        row_group_rows
        and row_group_rows > DEFAULT_ROW_GROUP_ROWS
        and effective_version in ("2.0", "parquet-geo-only")
    ):
        info(
            "For optimal spatial filter pushdown with Hilbert sorting, consider using "
            f"--row-group-size between 10,000 and {DEFAULT_ROW_GROUP_ROWS:,}. Smaller row "
            "groups create tighter bounding boxes that enable more row group skipping."
        )

    run_sort(
        input_parquet,
        output_parquet,
        command="sort hilbert",
        label="Hilbert curve",
        effective_version=effective_version,
        build_query=lambda con, source, geom_col, verbose, working: _hilbert_query(
            con, source, geom_col, verbose, working, advise=not add_bbox_flag
        ),
        geometry_column=geometry_column,
        add_bbox_flag=add_bbox_flag,
        verbose=verbose,
        write=WriteOptions.pick(locals()),
        overwrite=overwrite,
    )


def _hilbert_query(con, source, geom_col, verbose, working, *, advise=True):
    key_expr = resolve_geometry_read(con, source, geom_col, relation=True).expr
    (empty_count,) = fetch_one(
        con.execute(
            f"SELECT COUNT(*) FROM {source} WHERE {key_expr} IS NULL OR ST_IsEmpty({key_expr})"
        )
    )
    if empty_count:
        warn(
            f"Found {empty_count} empty/null geometries. "
            "These will be placed at the end of the sorted output."
        )
    bounds = (
        dataset_bounds(working, geom_col, verbose=verbose, advise=advise)
        if working
        else bounds_of(QuerySource(con, f"SELECT * FROM {source}"), geom_col)
    )
    if bounds is None:
        warn("All geometries are empty or null. Writing file without Hilbert ordering.")
        return f"SELECT * FROM {source}", False
    xmin, ymin, xmax, ymax = bounds
    query = f"""
        WITH non_empty AS (
            SELECT * FROM {source}
            WHERE {key_expr} IS NOT NULL AND NOT ST_IsEmpty({key_expr})
            ORDER BY ST_Hilbert({key_expr},
                ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})))
        ),
        empty_or_null AS (
            SELECT * FROM {source}
            WHERE {key_expr} IS NULL OR ST_IsEmpty({key_expr})
        )
        SELECT * FROM non_empty
        UNION ALL
        SELECT * FROM empty_or_null
    """

    return query, True
