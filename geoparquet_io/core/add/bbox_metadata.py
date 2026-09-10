#!/usr/bin/env python3
"""Add bbox covering metadata to GeoParquet files.

This module adds bbox covering metadata to existing GeoParquet files,
enabling spatial filtering optimizations in readers that support it.

Uses DuckDB COPY TO with KV_METADATA to preserve file properties including
bloom filters, native GEOMETRY logical type, and existing key-value metadata
(fixes #433).

Note: This operation only supports local files. Remote URLs (S3, GCS, Azure)
are not supported for in-place metadata modification.
"""

import json
import os
from typing import TYPE_CHECKING

import duckdb

from geoparquet_io.core.check_parquet_structure import get_compression_info, get_row_group_stats
from geoparquet_io.core.common import (
    check_bbox_structure,
    get_duckdb_connection,
    get_parquet_metadata,
)
from geoparquet_io.core.duckdb_utils import (
    _escape_sql_string,
    _wrap_query_with_blob_conversion,
    build_kv_metadata_clause,
    sql_path,
)
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geo_metadata import (
    covering_supported,
    parse_geo_metadata,
    sanitize_geo_metadata,
    sanitized_carried_geo,
)
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, success

if TYPE_CHECKING:
    import pyarrow as pa


def _is_remote_url(path: str) -> bool:
    """Check if the path is a remote URL (S3, GCS, Azure, HTTP)."""
    remote_prefixes = ("s3://", "gs://", "az://", "azure://", "http://", "https://")
    return path.lower().startswith(remote_prefixes)


def _detect_native_geometry(
    conn: duckdb.DuckDBPyConnection, parquet_file: str, geometry_column: str
) -> bool:
    """Detect if the file uses native GEOMETRY logical type (GeoParquet 2.0).

    Args:
        conn: DuckDB connection (reused to avoid repeated INSTALL/LOAD)
        parquet_file: RAW (unescaped) path to the parquet file
        geometry_column: Name of the geometry column to check

    Returns:
        True if the file has native GEOMETRY type, False otherwise
    """
    # The column name comes from the file's own `geo` metadata, so it is
    # untrusted input, not a constant: escape it like any other SQL literal.
    result = conn.execute(f"""
        SELECT logical_type
        FROM parquet_schema({sql_path(parquet_file)})
        WHERE name = '{_escape_sql_string(geometry_column)}'
    """).fetchone()

    if result and result[0]:
        logical_type = str(result[0])
        return "Geometry" in logical_type or "Geography" in logical_type
    return False


def _get_existing_kv_metadata(conn: duckdb.DuckDBPyConnection, parquet_file: str) -> dict:
    """Read existing key-value metadata from a parquet file.

    Args:
        conn: DuckDB connection
        parquet_file: RAW (unescaped) path to the parquet file

    Returns:
        Dict mapping metadata keys to values (both as strings)
    """
    result = conn.execute(
        f"SELECT key, value FROM parquet_kv_metadata({sql_path(parquet_file)})"
    ).fetchall()

    metadata = {}
    for key, value in result:
        # Keys and values come as bytes, decode them
        key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        value_str = value.decode("utf-8") if isinstance(value, bytes) else str(value)
        metadata[key_str] = value_str

    return metadata


def _build_kv_metadata_clause(existing_metadata: dict, new_geo_meta: dict) -> str:
    """Build the KV_METADATA clause preserving existing metadata.

    DuckDB's KV_METADATA replaces all metadata, so we must include all existing
    keys we want to preserve, plus the updated geo key.

    The clause itself comes from the shared ``build_kv_metadata_clause()``: it
    quotes key names as well as escaping them, where this function used to emit
    bare keys and hand-roll ``value.replace("'", "''")``. A preserved key
    containing ``:`` -- ``stac:collection``, or pyarrow's ``ARROW:schema`` --
    made DuckDB's parser reject the whole COPY (#756).

    Args:
        existing_metadata: Dict of existing key-value pairs (raw, unescaped)
        new_geo_meta: The new geo metadata dict to set

    Returns:
        KV_METADATA clause string for DuckDB COPY
    """
    # Carry every existing key except 'geo', which this rewrite replaces.
    pairs = {key: value for key, value in existing_metadata.items() if key != "geo"}
    pairs["geo"] = json.dumps(new_geo_meta)

    clause = build_kv_metadata_clause(pairs)
    # `pairs` always holds at least 'geo', so the helper never returns None.
    assert clause is not None
    return clause


def require_geo_metadata_for_covering(geo_meta: object, source: str | None = None) -> dict:
    """Refuse an input that carries no usable GeoParquet metadata (#713).

    A file with no `geo` key is not GeoParquet, and adding covering metadata
    cannot make it one: the covering key is written and nothing else, so the
    synthesised block used to go out with no `encoding` and no `geometry_types`
    -- a file that claims to be GeoParquet 1.1.0 and fails five of its own spec
    checks. Refuse instead, the same way a 1.0 input is refused (#686), and name
    the command that does produce valid metadata.

    A `geo` key holding valid JSON that is not an object (a bare string, a list)
    is treated as absent rather than crashing on the first `.get()`.

    Shared by the file path (`add_bbox_metadata`) and the Python API
    (`Table.add_bbox_metadata`, `ops.add_bbox_metadata`) so both refuse the same
    input with the same error. `source` names the file when there is one; the
    in-memory API has no path, so it falls back to naming the table, exactly as
    the 1.0 gate below says "this file" / "this table".

    Args:
        geo_meta: Parsed `geo` metadata, or None/whatever JSON produced
        source: Path of the file being modified, if any

    Returns:
        dict: The validated geo metadata

    Raises:
        GeoParquetError: If there is no usable `geo` object
    """
    if isinstance(geo_meta, dict) and geo_meta:
        return geo_meta

    subject = source if source else "this table"
    target = source if source else "input.parquet"
    raise GeoParquetError(
        f"Cannot add bbox covering metadata: {subject} has no GeoParquet "
        "metadata, so there is nothing to describe the geometry column "
        "(encoding, geometry types) that the 'covering' key attaches to.\n"
        f"Convert it first: gpio convert geoparquet {target} out.parquet"
    )


def _covering_for(bbox_column: str) -> dict:
    """The `covering` value pointing at a bbox struct column."""
    return {
        "bbox": {
            "xmin": [bbox_column, "xmin"],
            "ymin": [bbox_column, "ymin"],
            "xmax": [bbox_column, "xmax"],
            "ymax": [bbox_column, "ymax"],
        }
    }


def add_bbox_metadata_table(
    table: "pa.Table",
    bbox_column: str = "bbox",
    geometry_column: str | None = None,
) -> "pa.Table":
    """Add bbox covering metadata to an in-memory Arrow table.

    The in-memory counterpart of :func:`add_bbox_metadata`, and the single
    implementation behind `Table.add_bbox_metadata` and `ops.add_bbox_metadata`
    so the Python API refuses exactly what the CLI refuses (#713).

    Args:
        table: Arrow table carrying GeoParquet `geo` schema metadata
        bbox_column: Name of the existing bbox struct column
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        pa.Table: A new table whose `geo` metadata carries the covering key

    Raises:
        GeoParquetError: If the table carries no usable GeoParquet metadata
        ValueError: If the geometry or bbox column is missing, or the declared
            version predates GeoParquet 1.1
    """
    from geoparquet_io.core.streaming import find_geometry_column_from_table

    geom_col = geometry_column or find_geometry_column_from_table(table)
    if geom_col is None:
        raise ValueError(
            "Cannot add bbox metadata: no geometry column detected. "
            "Ensure the table has a valid geometry column."
        )

    if bbox_column not in table.column_names:
        raise ValueError(f"Bbox column '{bbox_column}' not found. Use add_bbox() first.")

    schema_metadata = dict(table.schema.metadata) if table.schema.metadata else {}

    # A write path: this block is re-serialized onto the returned table, so a
    # malformed carried one goes through the shared shape check first (#947).
    # Without it a `columns` mapping to a non-object let the covering be
    # assigned into a string, and a wrong-typed `crs` was carried straight into
    # the output.
    geo_meta: object = sanitized_carried_geo(schema_metadata)

    # Same refusal as the file path: a table with no usable `geo` block has
    # nothing describing the geometry column, and the skeleton this used to
    # invent claimed GeoParquet 1.1.0 while failing five of its own spec checks.
    geo_meta = require_geo_metadata_for_covering(geo_meta)
    if not isinstance(geo_meta.get("columns"), dict):
        geo_meta["columns"] = {}

    # 'covering' was introduced in GeoParquet 1.1. This path exists solely to
    # write that key, so a 1.0 table gets a clear error naming the conflict
    # rather than silently returning a table without the metadata it asked for.
    table_version = geo_meta.get("version", "")
    if not covering_supported(table_version):
        raise ValueError(
            f"Cannot add bbox covering metadata: this table declares GeoParquet "
            f"{table_version}, and the 'covering' key requires GeoParquet 1.1 or later. "
            f"Write the table at 1.1 first (e.g. write(..., geoparquet_version='1.1'))."
        )

    geo_col_meta = geo_meta["columns"].get(str(geom_col))
    if not isinstance(geo_col_meta, dict):
        geo_col_meta = {}
        geo_meta["columns"][str(geom_col)] = geo_col_meta
    geo_col_meta["covering"] = _covering_for(bbox_column)

    # Metadata-only change, not a cast
    schema_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    return table.replace_schema_metadata(schema_metadata)


def add_bbox_metadata(
    parquet_file: str,
    verbose: bool = False,
) -> None:
    """Add bbox covering metadata to a GeoParquet file.

    Updates the GeoParquet metadata to include bbox covering information,
    which enables spatial filtering optimizations in readers that support it.

    This operation preserves all file properties including:
    - Bloom filters on all columns
    - Native GEOMETRY logical type (GeoParquet 2.0)
    - Compression settings
    - Row group structure
    - All existing key-value metadata (pandas, ARROW:schema, custom, etc.)

    Note: Only local files are supported. Remote URLs will raise an error.

    Args:
        parquet_file: Path to the parquet file (will be modified in place)
        verbose: Print verbose output

    Raises:
        GeoParquetError: If the file is remote or the operation fails
    """
    # Reject remote URLs - in-place modification requires local filesystem
    if _is_remote_url(parquet_file):
        raise GeoParquetError(
            f"Remote URLs are not supported for in-place metadata modification: {parquet_file}\n"
            "Download the file locally, modify it, then upload."
        )

    # RAW path throughout: the metadata helpers each escape their own argument
    # (pre-escaping here sent ``bm''s.parquet`` on to pyarrow, issue #718), and
    # the SQL below escapes at the point of interpolation via ``sql_path``.
    read_url = resolve_file_url(parquet_file, verbose)

    # Check current bbox structure
    bbox_info = check_bbox_structure(parquet_file, verbose)

    if bbox_info["has_bbox_metadata"]:
        success(
            f"Bbox covering metadata already exists for column '{bbox_info['bbox_column_name']}'"
        )
        return

    # Reporting a failure and then exiting 0 is the same defect as #713, one
    # branch earlier: there is no covering to write without a bbox column, so
    # say so and fail, rather than letting a script read success from $?.
    if not bbox_info["has_bbox_column"]:
        raise GeoParquetError(
            "No valid bbox column found in the file. Please add a bbox column first.\n"
            f"Add one: gpio add bbox {parquet_file}"
        )

    # Get existing metadata
    metadata, _ = get_parquet_metadata(parquet_file)
    # `parse_geo_metadata` is the read-only reader and hands the block back as
    # the file holds it; this command rewrites the file from it, so the
    # malformed parts are dropped here the way every other write path drops
    # them (#947). A `columns` entry that is not an object used to fail with
    # `'str' object does not support item assignment` at the assignment below.
    geo_meta = sanitize_geo_metadata(parse_geo_metadata(metadata, False))

    geo_meta = require_geo_metadata_for_covering(geo_meta, parquet_file)

    # 'covering' was introduced in GeoParquet 1.1. Unlike the write paths — where
    # covering is an implicit side effect of writing a bbox column and is simply
    # omitted — this command exists solely to write that key, so a 1.0 file gets a
    # clear error naming the conflict rather than a silent no-op reporting success.
    file_version = geo_meta.get("version", "")
    if not covering_supported(file_version):
        raise GeoParquetError(
            f"Cannot add bbox covering metadata: this file declares GeoParquet "
            f"{file_version}, and the 'covering' key requires GeoParquet 1.1 or later.\n"
            f"Convert the file first: gpio convert geoparquet {parquet_file} out.parquet "
            f"--geoparquet-version 1.1"
        )

    # Find primary geometry column
    primary_col = find_primary_geometry_column(parquet_file, verbose)

    # Update or create the columns section. A `columns` value that is not an
    # object is as unusable as a missing one, so replace it rather than indexing
    # into it.
    if not isinstance(geo_meta.get("columns"), dict):
        geo_meta["columns"] = {}

    if primary_col not in geo_meta["columns"]:
        geo_meta["columns"][primary_col] = {}

    # Add bbox covering metadata
    geo_meta["columns"][primary_col]["covering"] = _covering_for(bbox_info["bbox_column_name"])

    if verbose:
        debug("\nUpdated geo metadata:")
        debug(json.dumps(geo_meta, indent=2))

    # Get original file properties
    row_group_stats = get_row_group_stats(parquet_file)
    compression_info = get_compression_info(parquet_file, primary_col)
    row_group_size = int(row_group_stats["avg_rows_per_group"])
    compression = compression_info[primary_col]

    # Create a temporary file for the rewrite
    temp_file = parquet_file + ".tmp"
    try:
        # Use DuckDB COPY TO with KV_METADATA to preserve file properties
        # This preserves bloom filters and native GEOMETRY logical type (fixes #433)
        conn = get_duckdb_connection()

        # Detect if file uses native GEOMETRY type (GeoParquet 2.0)
        # Pass the actual geometry column name - it might not be 'geometry'
        has_native_geometry = _detect_native_geometry(conn, read_url, primary_col)

        # Read existing KV metadata to preserve non-geo keys
        existing_kv = _get_existing_kv_metadata(conn, read_url)

        if verbose:
            debug("\nPreserving file properties:")
            debug(f"Row group size: {row_group_size:,} rows")
            debug(f"Compression: {compression}")
            debug(f"Native GEOMETRY type: {has_native_geometry}")
            debug(f"Existing metadata keys: {list(existing_kv.keys())}")

        # Build KV_METADATA clause preserving all existing metadata
        kv_metadata_clause = _build_kv_metadata_clause(existing_kv, geo_meta)

        # Build COPY options.
        # 'V2' keeps the native GEOMETRY logical type of a 2.0 input. 'NONE' means
        # "don't manage geo metadata" — without it DuckDB writes its own V1 geo
        # block and clobbers the covering this command exists to add. Note 'NONE'
        # governs the *metadata* only; keeping the v1.x geometry columns
        # physically unchanged is what _wrap_query_with_blob_conversion below is
        # for (#712).
        geoparquet_version = "V2" if has_native_geometry else "NONE"

        copy_options = [
            "FORMAT PARQUET",
            f"COMPRESSION {compression}",
            f"GEOPARQUET_VERSION '{geoparquet_version}'",
            kv_metadata_clause,
            f"ROW_GROUP_SIZE {row_group_size}",
        ]

        # This command is documented as metadata-only, so the geometry columns'
        # physical types have to survive it. With the spatial extension loaded --
        # and GEOPARQUET_VERSION above loads it whether or not we ask -- DuckDB
        # reads a v1.x WKB column as its own GEOMETRY type, and the COPY then
        # writes a native Parquet GEOMETRY logical type back out while the declared
        # version stays 1.1.0. gpio's own validator rejects that combination
        # (#712). Casting straight back to BLOB keeps the columns plain WKB, which
        # is what a 1.x file must carry. Every column the file declares as geometry
        # needs that cast, not just the primary one -- the validator names any
        # native column, and a 1.1 file may carry several. A file that is *already*
        # native is left alone: there the native type is the correct output.
        source_query = f"SELECT * FROM {sql_path(read_url)}"
        if not has_native_geometry:
            secondary_cols = [col for col in geo_meta["columns"] if col != primary_col]
            source_query = _wrap_query_with_blob_conversion(
                source_query, primary_col, conn, secondary_columns=secondary_cols
            )

        copy_sql = f"""
            COPY ({source_query})
            TO {sql_path(temp_file)}
            ({", ".join(copy_options)})
        """

        if verbose:
            debug(f"\nDuckDB COPY SQL:\n{copy_sql}")

        conn.execute(copy_sql)
        conn.close()

        # Replace original file atomically
        os.replace(temp_file, parquet_file)

        success(f"Added bbox covering metadata for column '{bbox_info['bbox_column_name']}'")

    except Exception as e:
        # Clean up temporary file if something goes wrong
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise GeoParquetError(f"Failed to update metadata: {str(e)}") from e
