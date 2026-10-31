#!/usr/bin/env python3

from __future__ import annotations

import os
from itertools import pairwise

import duckdb
import pyarrow as pa

from geoparquet_io.core.bbox_structure import dataset_bounds, get_bbox_advice
from geoparquet_io.core.common import add_bbox, get_parquet_metadata
from geoparquet_io.core.duckdb_utils import (
    describe_query,
    fetch_one,
    get_duckdb_connection,
    quote_identifier,
    sql_path,
    uses_aws_profile,
)
from geoparquet_io.core.exceptions import RemoteAccessError
from geoparquet_io.core.file_utils import handle_output_overwrite, resolve_file_url
from geoparquet_io.core.geo_metadata import QuerySource, bounds_of, sanitized_carried_geo
from geoparquet_io.core.geometry_detection import (
    register_geometry_column,
    resolve_geometry_column,
)
from geoparquet_io.core.logging_config import debug, info, success, warn
from geoparquet_io.core.optional_deps import load_module
from geoparquet_io.core.parquet_writer import (
    DEFAULT_ROW_GROUP_ROWS,
    WriteOptions,
    resolve_output_geoparquet_version,
    resolve_row_group_rows,
)
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.remote import (
    get_remote_error_hint,
    is_remote_url,
    needs_httpfs,
    show_remote_read_message,
    validate_profile_for_urls,
)
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import (
    is_stdin,
    should_stream_output,
)
from geoparquet_io.core.write_funnels import write_parquet_with_metadata


@uses_aws_profile
def _advised_output_version(
    input_parquet: str,
    geoparquet_version: str | None,
    verbose: bool,
    profile: str | None = None,
) -> str | None:
    """The GeoParquet version this run will actually write, or None if unknowable.

    Whether ``gpio sort hilbert`` and ``gpio sort str`` advise
    ``--geoparquet-version 2.0``, and whether they offer their row-group
    guidance at all, both hang off this answer -- so it has to be the *write's*
    answer. It asks the write facade the same question the write asks it, with
    the same two witnesses (the input file, then its carried metadata), and the
    facade's answer includes the default the write would fall back to.

    It used to re-implement that decision instead, reading only the carried
    ``geo`` key. Once the facade started consulting the file, the two answers
    diverged on exactly the input class the facade exists to fix: for a
    native-geo-only input this said "1.1" and advised switching to 2.0 over a
    file it was writing as 2.0 -- #738's bug arriving from the other side --
    while the row-group guidance, gated on 2.0, could never fire for it.

    Returns None rather than a default when the version cannot be established:
    a stdin stream (whose version the writer resolves from the stream's own
    metadata, not from anything visible here) or an input that will not open.
    Callers use None to stay silent. Defaulting instead is what produced the
    original wrong warning -- a failed read is not evidence of a 1.1 input.
    The facade cannot make that distinction on its own: it swallows a failed
    read and answers with the default, which is the guess. So the read stays
    here, as the gate on saying anything at all.
    """
    if geoparquet_version:
        return geoparquet_version
    if is_stdin(input_parquet):
        return None

    try:
        metadata, _ = get_parquet_metadata(input_parquet, verbose)
    except Exception as e:  # noqa: BLE001 - any read failure means "stay quiet"
        debug(f"Could not read {input_parquet} to resolve output version: {e}")
        return None
    return resolve_output_geoparquet_version(
        None,
        input_file=input_parquet,
        original_metadata=metadata,
        verbose=verbose,
    )


# Bytes of output a gather batch builds at once. The chunk-wise gather holds at
# most about twice this beyond the output, against a whole column for a
# concatenate-then-take, and each output chunk stays far below the 2 GiB a
# 32-bit offset addresses.
_GATHER_BATCH_BYTES = 16 * 1024 * 1024
_GATHER_THREADS = 4


def _view_twin(typ: pa.DataType) -> pa.DataType | None:
    """The offset-based twin of a view layout pyarrow 25 cannot ``take``, else None."""
    if pa.types.is_binary_view(typ):
        return pa.large_binary()
    if pa.types.is_string_view(typ):
        return pa.large_string()
    return None


def _row_index_name(geom_col: str) -> str:
    """An index column name DuckDB cannot confuse with ``geom_col`` (identifiers fold case)."""
    idx_col = "__gpio_row_idx"
    while idx_col.lower() == geom_col.lower():
        idx_col += "_"
    return idx_col


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

    twin = _view_twin(column.type)
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
        # One projection of the parsed geometry, shared by both queries below.
        source_ref = f"(SELECT {quoted_idx}, {read.shape_expr} AS __g FROM __input_table)"

        # The empty/NULL count, then the owner's bounds of everything else.
        (empty_count,) = fetch_one(
            con.execute(f"SELECT COUNT(*) FROM {source_ref} WHERE __g IS NULL OR ST_IsEmpty(__g)")
        )
        bbox = bounds_of(QuerySource(con, f"SELECT __g FROM {source_ref}"), "__g")

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


def _prepare_working_file(input_parquet, add_bbox_flag, verbose):
    """Prepare working file, adding bbox if needed.

    Returns:
        tuple: (working_parquet, temp_file_created, temp_file_path_or_none)
    """
    import shutil
    import tempfile

    bbox_advice = get_bbox_advice(input_parquet, "bounds_calculation", verbose)

    if add_bbox_flag and not bbox_advice["has_bbox_column"]:
        info("\nAdding bbox column to enable fast bounds calculation...")
        temp_fd, temp_file = tempfile.mkstemp(suffix=".parquet")
        os.close(temp_fd)
        shutil.copy2(input_parquet, temp_file)
        add_bbox(temp_file, "bbox", verbose)
        success("Added bbox column for optimized processing")
        return temp_file, True, temp_file

    if bbox_advice["needs_warning"]:
        warn(f"\nWarning: {bbox_advice['message']}")
        if not add_bbox_flag:
            for suggestion in bbox_advice["suggestions"]:
                info(f"Tip: {suggestion}")

    return input_parquet, False, None


def _cleanup_temp_file(temp_file, verbose):
    """Clean up temporary file if it exists."""
    if temp_file and os.path.exists(temp_file):
        try:
            os.remove(temp_file)
            if verbose:
                debug("Cleaned up temporary file")
        except OSError as e:
            if verbose:
                warn(f"Warning: Could not remove temporary file: {e}")


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
    effective_version = _advised_output_version(input_parquet, geoparquet_version, verbose, profile)
    if effective_version == "1.1":
        warn(
            "Hilbert sorting to GeoParquet v1.1 provides no spatial filter pushdown benefit. "
            "Consider using --geoparquet-version 2.0 to enable native geo_bbox row group statistics."
        )

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

    # Check for streaming mode (stdin input or stdout output)
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    write = WriteOptions.pick(locals())
    if is_streaming:
        _hilbert_order_streaming(
            input_parquet,
            output_parquet,
            geometry_column,
            verbose,
            write,
        )
        return

    # File-based mode
    _hilbert_order_file_based(
        input_parquet,
        output_parquet,
        geometry_column,
        add_bbox_flag,
        verbose,
        write,
        overwrite,
    )


def _hilbert_order_streaming(
    input_path: str,
    output_path: str | None,
    geometry_column: str,
    verbose: bool,
    write: WriteOptions,
) -> None:
    """Handle streaming input/output for hilbert_order."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    with open_input(input_path, verbose=verbose) as (source, metadata, is_stream, con):
        # Get column names from query result (works with both table names and read_parquet)
        described = describe_query(con, f"SELECT * FROM {source}")

        # Find geometry column: the owner over the typed columns and the
        # stream's own `geo` block, as the file path asks it.
        geom_col = resolve_geometry_column(
            described, geometry_column, geo=sanitized_carried_geo(metadata), where=input_path
        )
        quoted_geom = quote_identifier(geom_col)

        if verbose:
            debug(f"Using geometry column: {geom_col}")
            debug("Calculating dataset bounds for Hilbert ordering...")

        # Count empty/null geometries
        empty_count_result = fetch_one(
            con.execute(f"""
            SELECT COUNT(*) FROM {source}
            WHERE {quoted_geom} IS NULL OR ST_IsEmpty({quoted_geom})
        """)
        )
        empty_count = empty_count_result[0] if empty_count_result else 0

        if empty_count > 0:
            warn(
                f"Found {empty_count} empty/null geometries. "
                "These will be placed at the end of the sorted output."
            )

        # Dataset bounds of the located geometries (the owner skips NULL/EMPTY)
        bounds_result = bounds_of(QuerySource(con, f"SELECT * FROM {source}"), geom_col)

        # If all geometries are empty/null, pass through unchanged
        if bounds_result is None:
            warn("All geometries are empty or null. Writing file without Hilbert ordering.")
            # Pass through data unchanged
            passthrough_query = f"SELECT * FROM {source}"
            write_output(
                con,
                passthrough_query,
                output_path,
                original_metadata=metadata,
                geometry_column=geom_col,
                verbose=verbose,
                **write.funnel_kwargs(),
            )
            if not should_stream_output(output_path):
                success(f"Wrote data without Hilbert ordering to: {output_path}")
            return

        xmin, ymin, xmax, ymax = bounds_result
        if verbose:
            debug(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")
            debug("Reordering data using Hilbert curve...")

        # Build Hilbert ordering query with empty geometry handling
        # Non-empty geometries are ordered by Hilbert curve, empty/null appended at end
        query = f"""
            WITH non_empty AS (
                SELECT * FROM {source}
                WHERE {quoted_geom} IS NOT NULL AND NOT ST_IsEmpty({quoted_geom})
                ORDER BY ST_Hilbert({quoted_geom},
                    ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})))
            ),
            empty_or_null AS (
                SELECT * FROM {source}
                WHERE {quoted_geom} IS NULL OR ST_IsEmpty({quoted_geom})
            )
            SELECT * FROM non_empty
            UNION ALL
            SELECT * FROM empty_or_null
        """

        if verbose:
            debug(f"Streaming hilbert query: {query}")

        # Write output
        write_output(
            con,
            query,
            output_path,
            original_metadata=metadata,
            geometry_column=geom_col,
            verbose=verbose,
            **write.funnel_kwargs(),
        )

        if not should_stream_output(output_path):
            success(f"Successfully reordered data using Hilbert curve to: {output_path}")


@uses_aws_profile
def _hilbert_order_file_based(
    input_parquet: str,
    output_parquet: str | None,
    geometry_column: str,
    add_bbox_flag: bool,
    verbose: bool,
    write: WriteOptions,
    overwrite: bool = False,
) -> None:
    """Handle file-based hilbert_order operation."""
    # Check if output file exists and handle overwrite (fixes issue #278)
    handle_output_overwrite(output_parquet, overwrite, input_parquet)

    # Check for partition input (not supported)
    require_single_file(input_parquet, "sort hilbert")

    working_parquet, temp_file_created, temp_file = _prepare_working_file(
        input_parquet, add_bbox_flag, verbose
    )

    validate_profile_for_urls(write.profile, input_parquet, output_parquet)
    show_remote_read_message(working_parquet, verbose)

    working_url = resolve_file_url(working_parquet, verbose)
    # Read the metadata of the file the query actually reads. With --add-bbox
    # that is the working copy, whose `geo` block `add_bbox` just extended with
    # the `covering` describing the column it wrote. Reading the *original*
    # input here threw that covering away before the write could carry it, so
    # the output shipped a bbox column nothing declared (#738).
    metadata, _ = get_parquet_metadata(working_parquet, verbose)

    geometry_column = resolve_geometry_column(working_parquet, geometry_column, verbose=verbose)
    if verbose:
        debug(f"Using geometry column: {geometry_column}")

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(working_parquet))

    if verbose:
        debug("Calculating dataset bounds for Hilbert ordering...")

    # Count empty/null geometries
    empty_count_result = fetch_one(
        con.execute(f"""
        SELECT COUNT(*) FROM {sql_path(working_url)}
        WHERE {quote_identifier(geometry_column)} IS NULL OR ST_IsEmpty({quote_identifier(geometry_column)})
    """)
    )
    empty_count = empty_count_result[0] if empty_count_result else 0

    if empty_count > 0:
        warn(
            f"Found {empty_count} empty/null geometries. "
            "These will be placed at the end of the sorted output."
        )

    # Get bounds (empty geometries naturally excluded since ST_XMin returns NULL for them)
    bounds = dataset_bounds(working_parquet, geometry_column, verbose=verbose)

    # If all geometries are empty/null, copy file unchanged
    if not bounds:
        warn("All geometries are empty or null. Writing file without Hilbert ordering.")
        passthrough_query = f"SELECT * FROM {sql_path(working_url)}"
        write_parquet_with_metadata(
            con,
            passthrough_query,
            output_parquet,
            original_metadata=metadata,
            verbose=verbose,
            input_file=input_parquet,
            **write.funnel_kwargs(),
        )
        con.close()
        if temp_file_created and temp_file:
            _cleanup_temp_file(temp_file, verbose)
        success(f"Wrote data without Hilbert ordering to: {output_parquet}")
        return

    xmin, ymin, xmax, ymax = bounds
    if verbose:
        debug(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")
        debug("Reordering data using Hilbert curve...")

    # Build Hilbert ordering query with empty geometry handling
    # Non-empty geometries are ordered by Hilbert curve, empty/null appended at end
    order_query = f"""
        WITH non_empty AS (
            SELECT * FROM {sql_path(working_url)}
            WHERE {quote_identifier(geometry_column)} IS NOT NULL AND NOT ST_IsEmpty({quote_identifier(geometry_column)})
            ORDER BY ST_Hilbert({quote_identifier(geometry_column)},
                ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})))
        ),
        empty_or_null AS (
            SELECT * FROM {sql_path(working_url)}
            WHERE {quote_identifier(geometry_column)} IS NULL OR ST_IsEmpty({quote_identifier(geometry_column)})
        )
        SELECT * FROM non_empty
        UNION ALL
        SELECT * FROM empty_or_null
    """

    try:
        write_parquet_with_metadata(
            con,
            order_query,
            output_parquet,
            original_metadata=metadata,
            verbose=verbose,
            input_file=input_parquet,
            **write.funnel_kwargs(),
        )
        if verbose:
            debug("Hilbert ordering completed successfully")
        if add_bbox_flag and temp_file_created:
            success("Output includes bbox column and metadata for optimal performance")
        if verbose:
            debug(f"Successfully wrote ordered data to: {output_parquet}")
    except duckdb.IOException as e:
        con.close()
        if is_remote_url(input_parquet):
            hints = get_remote_error_hint(e, input_parquet)
            raise RemoteAccessError(input_parquet, f"{hints}\n\nOriginal error: {str(e)}") from e
        raise
    finally:
        _cleanup_temp_file(temp_file, verbose)


if __name__ == "__main__":
    hilbert_order()
