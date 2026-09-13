#!/usr/bin/env python3

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.common import (
    add_computed_column,
    check_bbox_structure,
    detect_geoparquet_file_type,
)
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier
from geoparquet_io.core.file_utils import copy_file, handle_output_overwrite
from geoparquet_io.core.geometry_detection import (
    STANDARD_GEOMETRY_NAMES,
    find_primary_geometry_column,
)
from geoparquet_io.core.logging_config import progress, success, warn
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import (
    find_geometry_column_from_table,
    is_stdin,
    should_stream_output,
)


def _bbox_metadata_advice(parquet_file: str) -> str:
    """Advice for a file that has a bbox column but no covering metadata.

    'covering' is 1.1-only, so a 1.0 file cannot be fixed by 'add bbox-metadata'
    (which refuses) — it needs a version upgrade first.
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata
    from geoparquet_io.core.geo_metadata import covering_supported

    geo_meta = get_geo_metadata(parquet_file) or {}
    version = geo_meta.get("version", "")
    if covering_supported(version):
        from geoparquet_io.core.bbox_structure import check_bbox_structure

        if check_bbox_structure(parquet_file).get("covering_problem"):
            return "The existing column cannot carry a 'covering'; use --force to rewrite it."
        return "Run 'gpio add bbox-metadata' to add metadata, or use --force to replace."
    return (
        f"'covering' requires GeoParquet 1.1+ (this file is {version}). Use --force to "
        "rewrite the bbox column at 1.1 with covering, or convert first: "
        "gpio convert geoparquet IN.parquet OUT.parquet --geoparquet-version 1.1"
    )


def _has_bbox_struct_column(con, source: str, bbox_column_name: str) -> bool:
    """Whether ``source`` already has a usable bbox struct under ``bbox_column_name``.

    Mirrors what :func:`check_bbox_structure` decides for files, but from a live
    DuckDB relation, so the streaming path can take the same "already has a bbox"
    decision as the file-based one.
    """
    for name, col_type, *_ in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall():
        if name != bbox_column_name:
            continue
        upper = col_type.upper()
        return upper.startswith("STRUCT") and all(
            field in upper for field in ("XMIN", "YMIN", "XMAX", "YMAX")
        )
    return False


def add_bbox_table(
    table: pa.Table,
    bbox_column_name: str = "bbox",
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add a bbox struct column to an Arrow Table.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table
        bbox_column_name: Name for the bbox column (default: 'bbox')
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with bbox column added
    """
    # Find geometry column
    geom_col = geometry_column or find_geometry_column_from_table(table)
    if not geom_col:
        geom_col = "geometry"

    # Check if bbox column already exists
    if bbox_column_name in table.column_names:
        # Drop existing column (replace behavior)
        idx = table.column_names.index(bbox_column_name)
        table = table.remove_column(idx)

    # Register table and execute query
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.register("__input_table", table)

        # Check if geometry column is BLOB (needs conversion)
        columns_info = con.execute("DESCRIBE __input_table").fetchall()
        geom_is_blob = any(col[0] == geom_col and "BLOB" in col[1].upper() for col in columns_info)

        quoted_geom_col = quote_identifier(geom_col)
        quoted_bbox_col = quote_identifier(bbox_column_name)

        if geom_is_blob and geom_col in table.column_names:
            # Create view with geometry conversion
            # Quote column names to handle special characters (colons, spaces,
            # embedded quotes, etc.)
            other_cols = [quote_identifier(c) for c in table.column_names if c != geom_col]
            col_defs = other_cols + [f"ST_GeomFromWKB({quoted_geom_col}) AS {quoted_geom_col}"]
            view_query = (
                f"CREATE VIEW __input_view AS SELECT {', '.join(col_defs)} FROM __input_table"
            )
            con.execute(view_query)
            source_ref = "__input_view"
        else:
            source_ref = "__input_table"

        # Build query to add bbox column
        bbox_expr = f"""STRUCT_PACK(
            xmin := ST_XMin({quoted_geom_col}),
            ymin := ST_YMin({quoted_geom_col}),
            xmax := ST_XMax({quoted_geom_col}),
            ymax := ST_YMax({quoted_geom_col})
        )"""

        # Get non-geometry columns
        other_cols = [quote_identifier(c) for c in table.column_names if c != geom_col]
        select_cols = ", ".join(other_cols) if other_cols else ""

        # Build SELECT with geometry converted back to WKB
        if select_cols:
            query = f"""
                SELECT {select_cols},
                       ST_AsWKB({quoted_geom_col}) AS {quoted_geom_col},
                       {bbox_expr} AS {quoted_bbox_col}
                FROM {source_ref}
            """
        else:
            query = f"""
                SELECT ST_AsWKB({quoted_geom_col}) AS {quoted_geom_col},
                       {bbox_expr} AS {quoted_bbox_col}
                FROM {source_ref}
            """
        result = con.execute(query).arrow().read_all()

        # Preserve metadata
        if table.schema.metadata:
            result = result.replace_schema_metadata(table.schema.metadata)

        return result
    finally:
        con.close()


def _bbox_covering_metadata(bbox_column_name: str) -> dict:
    """The GeoParquet 1.1+ ``covering`` block pointing at ``bbox_column_name``."""
    return {
        "covering": {
            "bbox": {
                "xmin": [bbox_column_name, "xmin"],
                "ymin": [bbox_column_name, "ymin"],
                "xmax": [bbox_column_name, "xmax"],
                "ymax": [bbox_column_name, "ymax"],
            }
        }
    }


def _write_options_requested(
    compression: str | None,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    geoparquet_version: str | None,
) -> bool:
    """Whether the caller explicitly asked for write settings the input may not have.

    A verbatim copy of the input cannot honour ``--geoparquet-version``,
    ``--compression``, ``--compression-level`` or ``--row-group-size``: it
    reproduces whatever the input was written with. So when one of those is
    explicitly requested, the copy shortcut is not an acceptable answer and the
    column is recomputed instead. Every one of these arrives as ``None`` unless
    the caller set it (the CLI passes ``compression=None`` when ``--compression``
    was left at its default).
    """
    return any(
        option is not None
        for option in (
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            geoparquet_version,
        )
    )


def _passthrough_version(metadata: dict | None, geoparquet_version: str | None) -> str | None:
    """The version to write for a stream that is being copied, not rewritten.

    The normal streaming write upgrades a 1.0 input to 1.1, which is right for a
    file gpio actually rewrote but wrong for a pass-through: nothing about the
    data changed, so nothing about its declared version should either. An
    explicitly requested version still wins.
    """
    if geoparquet_version is not None:
        return geoparquet_version
    if not metadata or b"geo" not in metadata:
        return None

    import json

    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(geo_meta, dict):
        return None
    version = geo_meta.get("version")
    if not isinstance(version, str):
        return None
    parts = version.split(".")
    if len(parts) < 2:
        return None
    return f"{parts[0]}.{parts[1]}"


def _make_add_bbox_query(
    source: str,
    geometry_column: str,
    bbox_column_name: str,
    replace_existing: bool = False,
) -> str:
    """Build query to add bbox column to a source."""
    quoted_geom_col = quote_identifier(geometry_column)
    quoted_bbox_col = quote_identifier(bbox_column_name)
    bbox_expr = f"""STRUCT_PACK(
        xmin := ST_XMin({quoted_geom_col}),
        ymin := ST_YMin({quoted_geom_col}),
        xmax := ST_XMax({quoted_geom_col}),
        ymax := ST_YMax({quoted_geom_col})
    )"""

    if replace_existing:
        return (
            f"SELECT * EXCLUDE ({quoted_bbox_col}), {bbox_expr} AS {quoted_bbox_col} FROM {source}"
        )
    else:
        return f"SELECT *, {bbox_expr} AS {quoted_bbox_col} FROM {source}"


def add_bbox_column(
    input_parquet: str,
    output_parquet: str | None = None,
    bbox_column_name: str = "bbox",
    dry_run: bool = False,
    verbose: bool = False,
    compression: str | None = None,
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    force: bool = False,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    memory_limit: str | None = None,
) -> None:
    """
    Add a bbox struct column to a GeoParquet file.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Checks for existing bbox columns before adding. If a bbox column already exists,
    nothing is recomputed, but the requested output is still produced: the input is
    copied verbatim to ``output_parquet`` (or passed through unchanged when
    streaming), so a pipeline step never silently ends with no output (#728).

    - **With covering metadata**: Reports it and copies the input to the output
    - **Without metadata**: Also suggests `gpio add bbox-metadata`
    - **With --force**: Replaces the existing bbox column with a fresh computation
    - **With no output path**: Reports only -- there is nothing to write
    - **With an explicit write option** (``compression``, ``compression_level``,
      ``row_group_size_mb``/``row_group_rows`` or ``geoparquet_version``): the
      column is recomputed rather than copied, since a verbatim copy cannot
      honour a write setting the input does not already have

    Args:
        input_parquet: Path to the input parquet file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        bbox_column_name: Name for the bbox column (default: 'bbox')
        dry_run: Whether to print SQL commands without executing them
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED);
            None means "not requested" and writes the ZSTD default

        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only, optional)
        force: Whether to replace an existing bbox column
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        memory_limit: DuckDB memory limit for the write (e.g., '2GB', '512MB')

    Note:
        Bbox covering metadata is automatically added when the file is written,
        except for GeoParquet 1.0 output: 'covering' was introduced in 1.1, so a
        1.0 file gets the bbox column without the covering key.
    """
    # Whether a copy of the input could satisfy this call at all, decided before
    # 'compression' is defaulted -- None here means "the caller did not ask".
    write_options_requested = _write_options_requested(
        compression, compression_level, row_group_size_mb, row_group_rows, geoparquet_version
    )
    compression = compression or "ZSTD"

    # Check for streaming mode (stdin input or stdout output)
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if is_streaming and not dry_run:
        _add_bbox_streaming(
            input_parquet,
            output_parquet,
            bbox_column_name,
            verbose,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            force,
            geoparquet_version,
            memory_limit=memory_limit,
        )
        return

    # File-based mode
    _add_bbox_file_based(
        input_parquet,
        output_parquet,
        bbox_column_name,
        dry_run,
        verbose,
        compression,
        compression_level,
        row_group_size_mb,
        row_group_rows,
        profile,
        force,
        geoparquet_version,
        overwrite,
        memory_limit=memory_limit,
        write_options_requested=write_options_requested,
    )


def _add_bbox_streaming(
    input_path: str,
    output_path: str | None,
    bbox_column_name: str,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    force: bool,
    geoparquet_version: str | None,
    memory_limit: str | None,
) -> None:
    """Handle streaming input/output for add_bbox."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    # The pass-through decision needs the source open, but it also decides what
    # metadata the write may declare -- so drive open_input/write_output directly
    # rather than through execute_transform, which fixes both before the query is
    # built. (add_bbox's query builder takes no source CRS, so nothing is lost.)
    with open_input(input_path, verbose=verbose) as (source, metadata, _is_stream, con):
        query, passed_through = _make_streaming_bbox_query(
            source, con, bbox_column_name, force=force
        )

        write_output(
            con,
            query,
            output_path,
            original_metadata=metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=verbose,
            profile=profile,
            # A covering gpio did not compute must not be declared, and a copy is
            # not a rewrite, so it does not upgrade the input's version either.
            custom_metadata=None if passed_through else _bbox_covering_metadata(bbox_column_name),
            geoparquet_version=(
                _passthrough_version(metadata, geoparquet_version)
                if passed_through
                else geoparquet_version
            ),
            memory_limit=memory_limit,
        )

    if should_stream_output(output_path):
        return

    if passed_through:
        success(
            f"Wrote: {output_path} - the existing bbox column "
            f"'{bbox_column_name}' was carried over unchanged."
        )
    else:
        success(f"Successfully added bbox column '{bbox_column_name}' to: {output_path}")


def _make_streaming_bbox_query(
    source: str, con, bbox_column_name: str, force: bool
) -> tuple[str, bool]:
    """Build the add-bbox query for a streaming source.

    Returns ``(query, passed_through)``. ``passed_through`` means the source
    already carries a usable bbox struct, so the query is a plain copy and the
    caller must not claim anything about a column gpio did not compute.
    """
    # Same guard as the file-based path (#728): a source that already carries a
    # bbox struct is passed through untouched rather than silently gaining a
    # second column named 'bbox_1'.
    if not force and _has_bbox_struct_column(con, source, bbox_column_name):
        progress(
            f"Input already has bbox column '{bbox_column_name}'; "
            "passed it through unchanged - the existing bbox column was not recomputed."
        )
        progress("Use --force to recompute and replace the existing bbox column.")
        return f"SELECT * FROM {source}", True

    # Get column names from query result (works with both table names and read_parquet)
    sample = con.execute(f"SELECT * FROM {source} LIMIT 0").description
    col_names = [col[0] for col in sample]

    # Find geometry column from common names
    geom_col = None
    for name in STANDARD_GEOMETRY_NAMES:
        if name in col_names:
            geom_col = name
            break
    if not geom_col:
        geom_col = "geometry"

    return (
        _make_add_bbox_query(source, geom_col, bbox_column_name, replace_existing=force),
        False,
    )


def _add_bbox_file_based(
    input_parquet: str,
    output_parquet: str | None,
    bbox_column_name: str,
    dry_run: bool,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    profile: str | None,
    force: bool,
    geoparquet_version: str | None,
    overwrite: bool,
    memory_limit: str | None,
    write_options_requested: bool = False,
) -> None:
    """Handle file-based add_bbox operation."""
    # Check if output file exists and handle overwrite (fixes issue #278)
    handle_output_overwrite(output_parquet, overwrite, input_parquet)

    # Check for partition input (not supported)
    require_single_file(input_parquet, "add bbox")

    # Check for parquet-geo-only input and warn user (skip in dry-run mode)
    if not dry_run:
        file_type_info = detect_geoparquet_file_type(input_parquet, verbose)
        if file_type_info["file_type"] == "parquet_geo_only":
            warn(
                "Note: Input file uses native Parquet geometry types without GeoParquet metadata. "
                "Bbox column is not required for spatial statistics as native geo types provide "
                "row group statistics. Proceeding with bbox addition anyway."
            )

    # Check for an existing bbox column. Dry-run takes the same decision, so the
    # preview describes what the real run would do (a copy) rather than SQL it
    # would never execute; nothing is written either way.
    done, replace_column = _handle_existing_bbox(
        check_bbox_structure(input_parquet, verbose),
        bbox_column_name,
        input_parquet,
        output_parquet,
        force,
        verbose,
        # A copy cannot carry a write setting the input does not already have,
        # and with no OUTPUT_FILE there is nothing to write it to either.
        write_options_requested=write_options_requested and output_parquet is not None,
        dry_run=dry_run,
    )
    if done:
        return

    # Get geometry column for the SQL expression
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Define the SQL expression (the only unique part)
    quoted_geom_col = quote_identifier(geom_col)
    sql_expression = f"""STRUCT_PACK(
        xmin := ST_XMin({quoted_geom_col}),
        ymin := ST_YMin({quoted_geom_col}),
        xmax := ST_XMax({quoted_geom_col}),
        ymax := ST_YMax({quoted_geom_col})
    )"""

    # Build covering metadata for the bbox column (GeoParquet 1.1+ spec)
    covering_metadata = _bbox_covering_metadata(bbox_column_name)

    # Use the generic helper for all boilerplate
    add_computed_column(
        input_parquet=input_parquet,
        output_parquet=output_parquet,
        column_name=bbox_column_name,
        sql_expression=sql_expression,
        extensions=None,
        dry_run=dry_run,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        dry_run_description="Bounding box struct (xmin, ymin, xmax, ymax)",
        profile=profile,
        replace_column=replace_column,
        geoparquet_version=geoparquet_version,
        custom_metadata=covering_metadata,
        memory_limit=memory_limit,
    )

    if not dry_run:
        success(f"Successfully added bbox column '{bbox_column_name}' to: {output_parquet}")


def _handle_existing_bbox(
    bbox_info: dict,
    bbox_column_name: str,
    input_parquet: str,
    output_parquet: str | None,
    force: bool,
    verbose: bool,
    write_options_requested: bool = False,
    dry_run: bool = False,
) -> tuple[bool, str | None]:
    """Decide what a bbox column the input already has means for this run.

    Returns ``(done, replace_column)``. ``done`` means the command is finished --
    the output it was asked for has been written as a copy of the input (or, in
    dry-run, that copy has been described). Otherwise the caller computes the
    column, excluding ``replace_column`` from the copy of the input schema.
    """
    if bbox_info["status"] not in ("optimal", "suboptimal"):
        return False, None

    existing_bbox_col = bbox_info.get("bbox_column_name")

    if force:
        return False, _handle_existing_bbox_force(bbox_column_name, existing_bbox_col)

    if bbox_column_name != existing_bbox_col:
        # Not a conflict: the requested column does not exist yet, so compute it
        # and leave the existing one alone. Copying the input would not satisfy
        # the request, since the copy would not carry the requested column.
        warn(
            f"Warning: Adding '{bbox_column_name}' alongside existing "
            f"'{existing_bbox_col}'. File will have 2 bbox columns."
        )
        return False, None

    if write_options_requested:
        # A verbatim copy reproduces the input's compression, row groups and
        # GeoParquet version, so it cannot answer a request for different ones.
        # Rewrite instead, replacing the column that is already there.
        progress(
            f"File already has bbox column '{existing_bbox_col}', but the requested "
            "output settings need a rewrite, so it is recomputed rather than copied."
        )
        return False, existing_bbox_col

    _report_bbox_copy(bbox_info, existing_bbox_col, input_parquet, output_parquet, verbose, dry_run)
    return True, None


def _report_bbox_copy(
    bbox_info: dict,
    existing_bbox_col: str,
    input_parquet: str,
    output_parquet: str | None,
    verbose: bool,
    dry_run: bool,
) -> None:
    """Announce -- and, outside dry-run, perform -- the copy that answers this run."""
    if bbox_info["status"] == "optimal":
        headline = f"File already has bbox column '{existing_bbox_col}' with covering metadata."
        advice = "Use --force to recompute and replace the existing bbox column."
    else:
        headline = f"File has bbox column '{existing_bbox_col}' but lacks covering metadata."
        advice = _bbox_metadata_advice(input_parquet)

    progress(headline)
    if dry_run:
        _preview_copy_instead_of_recomputing(input_parquet, output_parquet, existing_bbox_col)
    else:
        _copy_instead_of_recomputing(input_parquet, output_parquet, verbose)
    progress(advice)


def _preview_copy_instead_of_recomputing(
    input_parquet: str, output_parquet: str | None, existing_bbox_col: str
) -> None:
    """Describe the copy a real run would make, instead of SQL it would not run."""
    if not output_parquet:
        return

    warn("\n=== DRY RUN MODE - no SQL would be executed ===\n")
    progress(
        f'Would copy "{input_parquet}" to "{output_parquet}" unchanged '
        f"(existing bbox column '{existing_bbox_col}')."
    )


def _copy_instead_of_recomputing(
    input_parquet: str, output_parquet: str | None, verbose: bool
) -> None:
    """Write OUTPUT_FILE as a verbatim copy of an input that already has a bbox (#728).

    The end state the caller asked for -- a file at OUTPUT_FILE carrying a bbox
    column -- is already satisfiable from the input, so satisfy it instead of
    exiting 0 with no output at all and breaking the pipeline step that follows.
    The copy is announced so nobody has to wonder whether the bbox was rebuilt.

    With no OUTPUT_FILE (the report-only form) there is nothing to write.
    """
    if not output_parquet:
        return

    copy_file(input_parquet, output_parquet, verbose)
    progress(
        f'Copied "{input_parquet}" to "{output_parquet}" unchanged - '
        "the existing bbox column was not recomputed."
    )


def _handle_existing_bbox_force(bbox_column_name: str, existing_bbox_col: str) -> str | None:
    """Handle force mode when bbox column exists. Returns column to replace or None."""
    if bbox_column_name == existing_bbox_col:
        progress(f"Replacing existing bbox column '{existing_bbox_col}'...")
        return existing_bbox_col
    else:
        warn(
            f"Warning: Adding '{bbox_column_name}' alongside existing "
            f"'{existing_bbox_col}'. File will have 2 bbox columns."
        )
        return None


if __name__ == "__main__":
    add_bbox_column()
