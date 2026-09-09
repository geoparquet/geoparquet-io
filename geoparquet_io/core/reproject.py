"""Core reprojection logic for GeoParquet files."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.common import (
    check_bbox_structure,
    collect_nonplanar_edges,
    get_parquet_metadata,
    validate_compression_settings,
    write_parquet_with_metadata,
)
from geoparquet_io.core.crs_utils import (
    NULL_CRS_NO_FLAG_ERROR,
    apply_target_crs_to_geo_meta,
    crs_is_explicitly_null,
    extract_crs_from_parquet,
    geoparquet_crs_is_null,
    is_geographic_crs,
    parse_crs_string_to_projjson,
    resolve_crs_to_string,
)
from geoparquet_io.core.duckdb_utils import (
    _escape_sql_string,
    get_duckdb_connection,
    quote_identifier,
    sql_path,
)
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geo_metadata import strip_derived_stats
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import debug, info, success, warn
from geoparquet_io.core.remote import (
    needs_httpfs,
    remote_write_context,
    setup_aws_profile_if_needed,
    upload_if_remote,
    validate_profile_for_urls,
)
from geoparquet_io.core.stream_io import write_output
from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file, should_stream_output

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class ReprojectResult:
    """Result of a reprojection operation."""

    output_path: Path
    source_crs: str
    target_crs: str
    feature_count: int


def _detect_geometry_column_from_table(table: pa.Table) -> str:
    """Detect geometry column from table metadata.

    Args:
        table: PyArrow Table with geo metadata

    Returns:
        Geometry column name, defaults to 'geometry'
    """
    if table.schema.metadata and b"geo" in table.schema.metadata:
        try:
            geo_meta = json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
            if "primary_column" in geo_meta:
                return geo_meta["primary_column"]
        except (json.JSONDecodeError, KeyError):
            pass
    return "geometry"


def _resolve_crs_to_string(crs_info) -> str | None:
    """Resolve CRS info (PROJJSON dict or string) to a CRS string for ST_Transform.

    Thin wrapper over :func:`crs_utils.resolve_crs_to_string` (single source of
    truth) kept for the module-local callers below.
    """
    return resolve_crs_to_string(crs_info)


def _detect_crs_from_table(table: pa.Table, geom_col: str) -> str:
    """Detect CRS from table metadata.

    Args:
        table: PyArrow Table with geo metadata
        geom_col: Geometry column name

    Returns:
        CRS string like "EPSG:4326" or PROJJSON string for ST_Transform
    """
    if table.schema.metadata and b"geo" in table.schema.metadata:
        try:
            geo_meta = json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
            columns = geo_meta.get("columns", {})
            if geom_col in columns:
                crs_info = columns[geom_col].get("crs")
                resolved = _resolve_crs_to_string(crs_info)
                if resolved:
                    return resolved
        except (json.JSONDecodeError, KeyError):
            pass
    # Default to WGS84 per GeoParquet spec
    return "EPSG:4326"


def _table_geo_column_meta(table: pa.Table, geom_col: str) -> dict:
    """Return the geo-metadata dict for ``geom_col``, or ``{}`` if unavailable."""
    if table.schema.metadata and b"geo" in table.schema.metadata:
        try:
            geo_meta = json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
            return geo_meta.get("columns", {}).get(geom_col, {})
        except (json.JSONDecodeError, KeyError):
            return {}
    return {}


def _target_crs_is_projected(target_crs: str, con=None) -> bool:
    """True when reprojecting into ``target_crs`` lands in projected space.

    Resolves the CRS string to PROJJSON first: ``is_geographic_crs`` reads the
    CRS *type* from PROJJSON, while its string fallback only recognizes the
    WGS84 spellings and would call a geographic datum like EPSG:4269 projected.
    An unresolvable CRS falls back to that string heuristic.
    """
    return not is_geographic_crs(parse_crs_string_to_projjson(target_crs, con) or target_crs)


def _warn_edges_dropped(col_name: str, edges: str, target_crs: str) -> None:
    warn(
        f"Dropped 'edges: {edges}' on column '{col_name}': the reprojected "
        f"edges are straight lines in {target_crs}. Densify before reprojecting "
        "if great-circle edges must be preserved."
    )


def _edges_columns_to_drop(
    input_file: str | None, target_crs: str, geom_col: str, con=None
) -> set[str]:
    """Columns whose non-planar ``edges`` declaration this reprojection voids (#601).

    Reprojection transforms vertices, not edges: gpio does not densify along
    great circles, so in a projected destination the output's edges *are*
    straight lines and planar (the spec default) is the truthful description.
    A geographic destination is only a datum shift, so the declaration stands.

    Only ``geom_col`` — the one column reproject actually transforms — is ever
    dropped: an untransformed secondary geometry column keeps its geographic
    CRS, so its great-circle declaration still holds. Warns when ``geom_col``
    loses a declaration the input actually made.
    """
    if not _target_crs_is_projected(target_crs, con):
        return set()
    if input_file:
        edges = collect_nonplanar_edges(input_file).get(geom_col)
        if edges:
            _warn_edges_dropped(geom_col, edges, target_crs)
    return {geom_col}


def _drop_nonplanar_edges_from_geo_meta(geo_meta: dict, target_crs: str, geom_col: str) -> None:
    """Strip ``geom_col``'s non-planar ``edges`` from an in-memory geo dict (#601).

    The counterpart of ``_edges_columns_to_drop`` for the write paths that hand
    the input's geo metadata straight through (Arrow tables, streaming). Other
    columns are untransformed and keep their declaration.
    """
    col_meta = (geo_meta.get("columns") or {}).get(geom_col)
    edges = col_meta.get("edges") if isinstance(col_meta, dict) else None
    if edges and edges != "planar":
        del col_meta["edges"]
        _warn_edges_dropped(geom_col, edges, target_crs)


#: Raised by the Arrow/Python-API path on an explicit ``crs: null`` input.
NULL_CRS_TABLE_ERROR = (
    "Input table has an explicit null CRS (unknown). Pass assume_crs84=True to "
    "treat the coordinates as OGC:CRS84 (lon/lat WGS84), or set source_crs."
)


def reproject_table(
    table: pa.Table,
    target_crs: str = "EPSG:4326",
    source_crs: str | None = None,
    geometry_column: str | None = None,
    assume_crs84: bool = False,
) -> pa.Table:
    """
    Reproject an Arrow Table to a different CRS.

    This is the table-centric version for the Python API.

    Args:
        table: Input PyArrow Table with geometry column
        target_crs: Target CRS (default: EPSG:4326)
        source_crs: Source CRS. If None, detected from table metadata.
        geometry_column: Geometry column name. If None, detected from metadata.
        assume_crs84: If True, treat an unknown/null input CRS as OGC:CRS84
            instead of raising. Useful for fixing files that carry ``crs: null``.

    Returns:
        New table with reprojected geometry

    Raises:
        ValueError: If the input has an explicit ``crs: null`` (unknown CRS) and
            neither ``source_crs`` nor ``assume_crs84`` is set. This mirrors the
            CLI contract so the Python API never silently treats unknown data as
            the default.
    """
    # Detect geometry column
    geom_col = geometry_column or _detect_geometry_column_from_table(table)

    # Resolve source CRS. An explicit crs:null is *unknown*, not the default — so
    # mirror the CLI contract: require source_crs or assume_crs84, else raise,
    # rather than silently transforming unknown coordinates as if lon/lat WGS84.
    if source_crs:
        effective_source_crs = source_crs
    elif crs_is_explicitly_null(_table_geo_column_meta(table, geom_col)):
        if not assume_crs84:
            raise ValueError(NULL_CRS_TABLE_ERROR)
        effective_source_crs = "OGC:CRS84"
    else:
        effective_source_crs = _detect_crs_from_table(table, geom_col)

    # Create connection and register table
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.register("__input_table", table)

        # Check if geometry column is BLOB type (needs conversion to GEOMETRY)
        geom_is_blob = False
        if geom_col in table.column_names:
            col_idx = table.column_names.index(geom_col)
            col_type = str(table.schema.field(col_idx).type)
            geom_is_blob = "large_binary" in col_type.lower() or "binary" in col_type.lower()

        # Create view with geometry conversion if needed
        source_table = "__input_table"
        if geom_is_blob:
            # Quote all column names to handle special characters (colons, spaces, etc.)
            other_cols = [quote_identifier(c) for c in table.column_names if c != geom_col]
            col_defs = other_cols + [
                f"ST_GeomFromWKB({quote_identifier(geom_col)}) AS {quote_identifier(geom_col)}"
            ]
            view_query = (
                f"CREATE VIEW __input_view AS SELECT {', '.join(col_defs)} FROM __input_table"
            )
            con.execute(view_query)
            source_table = "__input_view"

        # Build reprojection query
        # Use ST_AsWKB to convert back to WKB format for GeoParquet compatibility
        # The CRS values come from file metadata (or the caller) and may be
        # free-form strings, so they are escaped like any other SQL literal.
        source_crs_literal = _escape_sql_string(effective_source_crs)
        target_crs_literal = _escape_sql_string(target_crs)
        query = f"""
            SELECT
                * EXCLUDE ({quote_identifier(geom_col)}),
                ST_AsWKB(
                    ST_Transform(
                        {quote_identifier(geom_col)},
                        '{source_crs_literal}',
                        '{target_crs_literal}'
                    )
                ) AS {quote_identifier(geom_col)}
            FROM {source_table}
        """

        result = con.execute(query).arrow().read_all()

        # Update geo metadata with new CRS
        if table.schema.metadata:
            new_metadata = dict(table.schema.metadata)
            if b"geo" in new_metadata:
                try:
                    geo_meta = json.loads(new_metadata[b"geo"].decode("utf-8"))
                    apply_target_crs_to_geo_meta(geo_meta, geom_col, target_crs, con)
                    if _target_crs_is_projected(target_crs, con):
                        _drop_nonplanar_edges_from_geo_meta(geo_meta, target_crs, geom_col)
                    new_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
                    result = result.replace_schema_metadata(new_metadata)
                except (json.JSONDecodeError, KeyError) as e:
                    debug(f"Could not update CRS in geo metadata, leaving as-is: {e}")
            else:
                result = result.replace_schema_metadata(table.schema.metadata)

        return result
    finally:
        con.close()


def _detect_source_crs(input_path: str, verbose: bool) -> str:
    """Detect source CRS from GeoParquet metadata.

    Args:
        input_path: RAW (unescaped) path/URL to the input file —
            ``extract_crs_from_parquet`` escapes its own argument (#718)
        verbose: Whether to print verbose output

    Returns:
        CRS string like "EPSG:4326" or PROJJSON string for ST_Transform
    """
    crs_info = extract_crs_from_parquet(input_path, verbose=verbose)

    resolved = _resolve_crs_to_string(crs_info)
    if resolved:
        return resolved

    # Default to WGS84 per GeoParquet spec (missing CRS = WGS84)
    if verbose:
        debug("No CRS found in metadata, assuming EPSG:4326 (WGS84)")
    return "EPSG:4326"


def _get_bbox_column_name(input_path: str, verbose: bool) -> str | None:
    """Get bbox column name if it exists.

    Args:
        input_path: RAW (unescaped) path/URL to the input file —
            ``check_bbox_structure`` escapes its own argument (#718)
        verbose: Whether to print verbose output

    Returns:
        Bbox column name or None
    """
    bbox_info = check_bbox_structure(input_path, verbose=verbose)
    if bbox_info.get("has_bbox_column"):
        return bbox_info.get("bbox_column_name")
    return None


def _row_group_compression(parquet_file: pq.ParquetFile) -> str:
    """Return the file's first-column compression codec for ParquetWriter.

    Preserves the input codec so the sanitized copy isn't silently re-encoded to
    a different (e.g. larger) compression. Falls back to ZSTD for empty files.
    """
    md = parquet_file.metadata
    if md.num_row_groups > 0 and md.row_group(0).num_columns > 0:
        codec = md.row_group(0).column(0).compression
        if codec:
            codec = codec.lower()
            return "none" if codec == "uncompressed" else codec
    return "zstd"


def _sanitize_null_crs_file(raw_path: str, verbose: bool = False) -> tuple[str, str | None]:
    """Strip an explicit ``crs: null`` so the file presents as default OGC:CRS84.

    DuckDB's GeoParquet reader rejects ``crs: null`` outright, so for the
    ``assume_crs84`` path we rewrite the geo metadata (no coordinate change) to a
    temp file and read from that instead. The rewrite streams row group by row
    group so memory stays bounded — the rest of reproject is built to handle
    larger-than-RAM files and a whole-file load would defeat that.

    ``raw_path`` must be the *unescaped* path/URL (it is handed to PyArrow, not
    SQL).

    Returns:
        ``(path_to_read, temp_path_or_None)`` — the second element, when set, is a
        temp file the caller must delete.
    """
    if not geoparquet_crs_is_null(raw_path):
        return raw_path, None

    parquet_file = pq.ParquetFile(raw_path)
    metadata = dict(parquet_file.schema_arrow.metadata or {})
    geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
    for col in geo_meta.get("columns", {}).values():
        if crs_is_explicitly_null(col):
            col.pop("crs", None)
    metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    new_schema = parquet_file.schema_arrow.with_metadata(metadata)

    # mkstemp creates the file atomically with 0600 perms (it holds a full copy
    # of the user's data) and a collision-free name.
    fd, temp_path = tempfile.mkstemp(suffix=".parquet", prefix="gpio_assume_crs84_")
    os.close(fd)
    try:
        writer = pq.ParquetWriter(
            temp_path, new_schema, compression=_row_group_compression(parquet_file)
        )
        try:
            for i in range(parquet_file.num_row_groups):
                writer.write_table(parquet_file.read_row_group(i))
        finally:
            writer.close()
    except BaseException:
        # Self-clean on any failure after creation so we never orphan the copy.
        Path(temp_path).unlink(missing_ok=True)
        raise
    if verbose:
        debug(f"Treating unknown (null) CRS as OGC:CRS84 for {raw_path}")
    return temp_path, temp_path


def _resolve_null_crs_input(
    raw_path: str, assume_crs84: bool, verbose: bool
) -> tuple[str, str | None]:
    """Resolve an explicit null CRS before DuckDB reads the file.

    Without ``assume_crs84`` an explicit ``crs: null`` raises a friendly error
    (DuckDB can't read it); with it, the file is sanitized to a temp copy.

    ``raw_path`` must be the *unescaped* path/URL.

    Returns:
        ``(path_to_read, temp_path_or_None)`` — both are raw (unescaped) paths.
    """
    if assume_crs84:
        return _sanitize_null_crs_file(raw_path, verbose=verbose)
    if geoparquet_crs_is_null(raw_path):
        raise ValueError(NULL_CRS_NO_FLAG_ERROR)
    return raw_path, None


def reproject_impl(
    input_parquet: str,
    output_parquet: str | None = None,
    target_crs: str = "EPSG:4326",
    source_crs: str | None = None,
    overwrite: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    verbose: bool = False,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    on_progress: Callable[[str], None] | None = None,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
    memory_limit: str | None = None,
    assume_crs84: bool = False,
) -> ReprojectResult:
    """
    Reproject a GeoParquet file to a different CRS using DuckDB.

    Args:
        input_parquet: Path to input GeoParquet file (local or remote URL)
        output_parquet: Path to output file. If None, generates name from input.
        target_crs: Target CRS (default: EPSG:4326)
        source_crs: Override source CRS. If None, detected from metadata.
        assume_crs84: Treat an unknown/null input CRS as OGC:CRS84 (rewrites the
            metadata so DuckDB can read the file; no coordinate change).
        overwrite: If True and output_parquet is None, overwrite input file
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        verbose: Whether to print verbose output
        profile: AWS profile name for S3 operations
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        on_progress: Optional callback for progress messages
        row_group_size_mb: Row group size in MB (mutually exclusive with row_group_rows)
        row_group_rows: Row group size in number of rows (mutually exclusive with row_group_size_mb)
        memory_limit: DuckDB memory limit for write operations (e.g., "2GB")

    Returns:
        ReprojectResult with information about the operation

    Raises:
        ValueError: If CRS parsing fails or invalid parameters provided
        FileNotFoundError: If input file doesn't exist
        RuntimeError: If reprojection operation fails
    """

    def log(msg: str) -> None:
        if on_progress:
            on_progress(msg)
        elif verbose:
            info(msg)

    # Validate profile usage
    validate_profile_for_urls(profile, input_parquet, output_parquet)

    # Setup AWS profile if needed
    setup_aws_profile_if_needed(profile, input_parquet, output_parquet)

    # Resolve an explicit null CRS before DuckDB reads the file: raise a friendly
    # error without --assume-crs84, or sanitize to a temp copy with it. Pass the
    # raw path (not the SQL-escaped URL) to the PyArrow/metadata helpers.
    read_source, temp_sanitized = _resolve_null_crs_input(
        input_parquet, assume_crs84, verbose=verbose
    )

    # Safe URL for SQL interpolation, computed from the (possibly sanitized) path.
    input_url = resolve_file_url(read_source, verbose=verbose)

    # Create DuckDB connection with spatial extension
    con = get_duckdb_connection(
        load_spatial=True,
        load_httpfs=needs_httpfs(input_parquet),
    )

    try:
        # Detect geometry column
        geom_col = find_primary_geometry_column(read_source, verbose=verbose)
        log(f"Geometry column: {geom_col}")

        # Detect source CRS from metadata
        detected_crs = _detect_source_crs(read_source, verbose)

        # Use override if provided, otherwise use detected
        if source_crs is not None:
            info(f"Detected CRS: {detected_crs}")
            info(f"Overriding with source CRS: {source_crs}")
            effective_source_crs = source_crs
        else:
            effective_source_crs = detected_crs
            log(f"Source CRS: {effective_source_crs}")

        log(f"Target CRS: {target_crs}")

        # Get feature count
        count = con.execute(f"SELECT COUNT(*) FROM {sql_path(input_url)}").fetchone()[0]
        log(f"Features: {count:,}")

        # Check for existing bbox column to exclude (will be regenerated)
        bbox_col = _get_bbox_column_name(read_source, verbose)
        exclude_cols = [geom_col]
        if bbox_col:
            exclude_cols.append(bbox_col)
            if verbose:
                debug(f"Excluding bbox column '{bbox_col}' (will be regenerated)")
        # Quote column names to handle special characters (colons, spaces, etc.)
        exclude_clause = ", ".join(quote_identifier(c) for c in exclude_cols)

        # Build SQL query with ST_Transform
        # geometry_always_xy is set at connection level (DuckDB 1.5+)
        log("Reprojecting...")

        # The detected source CRS may be a free-form string a file carried in
        # its metadata or Parquet geo type (#866), so both CRS values are
        # escaped like any other SQL literal.
        source_crs_literal = _escape_sql_string(effective_source_crs)
        target_crs_literal = _escape_sql_string(target_crs)

        # Build bbox regeneration clause if input had bbox column
        if bbox_col:
            # Use CTE to avoid computing ST_Transform multiple times
            query = f"""
                WITH reprojected AS (
                    SELECT
                        * EXCLUDE ({exclude_clause}),
                        ST_Transform(
                            {quote_identifier(geom_col)},
                            '{source_crs_literal}',
                            '{target_crs_literal}'
                        ) AS {quote_identifier(geom_col)}
                    FROM {sql_path(input_url)}
                )
                SELECT
                    *,
                    STRUCT_PACK(
                        xmin := ST_XMin({quote_identifier(geom_col)}),
                        ymin := ST_YMin({quote_identifier(geom_col)}),
                        xmax := ST_XMax({quote_identifier(geom_col)}),
                        ymax := ST_YMax({quote_identifier(geom_col)})
                    ) AS {quote_identifier(bbox_col)}
                FROM reprojected
            """
            if verbose:
                debug(f"Regenerating bbox column '{bbox_col}' with reprojected coordinates")
        else:
            query = f"""
                SELECT
                    * EXCLUDE ({exclude_clause}),
                    ST_Transform(
                        {quote_identifier(geom_col)},
                        '{source_crs_literal}',
                        '{target_crs_literal}'
                    ) AS {quote_identifier(geom_col)}
                FROM {sql_path(input_url)}
            """

        # Determine output path
        if output_parquet:
            out_path = Path(output_parquet).resolve()
        elif overwrite:
            out_path = Path(input_parquet).resolve()
        else:
            # Generate output name: input_epsg_4326.parquet
            input_path = Path(input_parquet)
            target_suffix = target_crs.replace(":", "_").lower()
            out_path = input_path.parent / f"{input_path.stem}_{target_suffix}.parquet"

        log(f"Output: {out_path}")

        # Validate compression settings
        compression, compression_level, _ = validate_compression_settings(
            compression, compression_level, verbose
        )

        # Get target CRS as PROJJSON for metadata
        target_crs_projjson = parse_crs_string_to_projjson(target_crs, con)

        # Read original metadata to preserve non-geo KV metadata (e.g., vecorel)
        original_metadata, _ = get_parquet_metadata(input_parquet, verbose)

        # A projected destination invalidates the transformed column's
        # non-planar `edges` declaration: the reprojected edges are straight
        # lines there (#601). Untransformed columns keep theirs.
        drop_edges_columns = _edges_columns_to_drop(read_source, target_crs, geom_col, con)

        # Auto version mode: match convert's contract (todo 040) — preserve the
        # input's GeoParquet version and upgrade native-geo-only inputs to 2.0
        # instead of silently stripping native types to 1.1 WKB.
        if geoparquet_version is None:
            from geoparquet_io.core.common import resolve_geoparquet_version_from_file

            geoparquet_version = resolve_geoparquet_version_from_file(read_source, verbose)
            if verbose and geoparquet_version:
                debug(f"Auto-detected GeoParquet version from input: {geoparquet_version}")

        # Handle in-place overwrite
        is_overwrite = str(out_path) == str(Path(input_parquet).resolve())

        if is_overwrite:
            # Write to temp file first, then replace
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = Path(tmp.name)

            try:
                write_parquet_with_metadata(
                    con,
                    query,
                    str(tmp_path),
                    original_metadata=original_metadata,
                    compression=compression,
                    compression_level=compression_level,
                    row_group_size_mb=row_group_size_mb,
                    row_group_rows=row_group_rows,
                    verbose=verbose,
                    profile=profile,
                    geoparquet_version=geoparquet_version,
                    input_crs=target_crs_projjson,
                    memory_limit=memory_limit,
                    input_file=read_source,
                    invalidate_derived_stats=True,
                    invalidate_derived_stats_columns={geom_col},
                    drop_nonplanar_edges_columns=drop_edges_columns,
                )
                # Replace original with temp file
                shutil.move(str(tmp_path), str(out_path))
            except Exception:
                if tmp_path.exists():
                    tmp_path.unlink()
                raise
        else:
            # Write directly to output
            with remote_write_context(str(out_path), verbose=verbose) as (
                actual_output,
                is_remote,
            ):
                write_parquet_with_metadata(
                    con,
                    query,
                    actual_output,
                    original_metadata=original_metadata,
                    compression=compression,
                    compression_level=compression_level,
                    row_group_size_mb=row_group_size_mb,
                    row_group_rows=row_group_rows,
                    verbose=verbose,
                    profile=profile,
                    geoparquet_version=geoparquet_version,
                    input_crs=target_crs_projjson,
                    memory_limit=memory_limit,
                    input_file=read_source,
                    invalidate_derived_stats=True,
                    invalidate_derived_stats_columns={geom_col},
                    drop_nonplanar_edges_columns=drop_edges_columns,
                )

                if is_remote:
                    upload_if_remote(
                        actual_output,
                        str(out_path),
                        profile=profile,
                        is_directory=False,
                        verbose=verbose,
                    )

        if verbose:
            success(f"Reprojected {count:,} features from {effective_source_crs} to {target_crs}")

        return ReprojectResult(
            output_path=out_path,
            source_crs=effective_source_crs,
            target_crs=target_crs,
            feature_count=count,
        )

    finally:
        con.close()
        # unlink(missing_ok) avoids a TOCTOU and can't mask the real exception.
        if temp_sanitized:
            Path(temp_sanitized).unlink(missing_ok=True)


def _reproject_streaming(
    input_path: str,
    output_path: str | None,
    target_crs: str,
    source_crs: str | None,
    compression: str,
    compression_level: int | None,
    verbose: bool,
    profile: str | None,
    geoparquet_version: str | None,
    assume_crs84: bool = False,
) -> None:
    """Handle streaming input/output for reproject."""
    # Suppress verbose when streaming to stdout
    if should_stream_output(output_path):
        verbose = False

    temp_input_file = None
    temp_sanitized = None

    try:
        # If reading from stdin, write to temp file first. The shared bridge
        # reconciles the stream's geo metadata with its schema on the way, so
        # the temp file is one DuckDB will actually open (#722).
        if is_stdin(input_path):
            temp_input_file = read_stdin_to_temp_file(verbose)
            working_file = temp_input_file
        else:
            working_file = input_path

        # Resolve an explicit null CRS (raise without the flag, sanitize with it),
        # passing the raw path — not the SQL-escaped URL — to PyArrow helpers.
        working_file, temp_sanitized = _resolve_null_crs_input(
            working_file, assume_crs84, verbose=verbose
        )

        # Get safe URL for SQL interpolation
        working_url = resolve_file_url(working_file, verbose=False)

        # Create connection
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(working_file))

        try:
            # Detect geometry column
            geom_col = find_primary_geometry_column(working_file, verbose=False)

            # Detect source CRS
            detected_crs = _detect_source_crs(working_file, verbose=False)
            effective_source_crs = source_crs if source_crs else detected_crs

            # Check for existing bbox column to exclude (will be regenerated)
            bbox_col = _get_bbox_column_name(working_file, verbose=False)
            exclude_cols = [geom_col]
            if bbox_col:
                exclude_cols.append(bbox_col)
            # Quote column names to handle special characters (colons, spaces, etc.)
            exclude_clause = ", ".join(quote_identifier(c) for c in exclude_cols)

            # The detected source CRS may be a free-form string a file carried
            # in its metadata or Parquet geo type (#866), so both CRS values
            # are escaped like any other SQL literal.
            source_crs_literal = _escape_sql_string(effective_source_crs)
            target_crs_literal = _escape_sql_string(target_crs)

            # Build reprojection query with bbox regeneration if input had bbox
            if bbox_col:
                # Use CTE to compute reprojected geometry once, then regenerate bbox
                query = f"""
                    WITH reprojected AS (
                        SELECT
                            * EXCLUDE ({exclude_clause}),
                            ST_Transform(
                                {quote_identifier(geom_col)},
                                '{source_crs_literal}',
                                '{target_crs_literal}'
                            ) AS {quote_identifier(geom_col)}
                        FROM {sql_path(working_url)}
                    )
                    SELECT
                        *,
                        STRUCT_PACK(
                            xmin := ST_XMin({quote_identifier(geom_col)}),
                            ymin := ST_YMin({quote_identifier(geom_col)}),
                            xmax := ST_XMax({quote_identifier(geom_col)}),
                            ymax := ST_YMax({quote_identifier(geom_col)})
                        ) AS {quote_identifier(bbox_col)}
                    FROM reprojected
                """
            else:
                query = f"""
                    SELECT
                        * EXCLUDE ({exclude_clause}),
                        ST_Transform(
                            {quote_identifier(geom_col)},
                            '{source_crs_literal}',
                            '{target_crs_literal}'
                        ) AS {quote_identifier(geom_col)}
                    FROM {sql_path(working_url)}
                """

            # Get original metadata for preservation
            from geoparquet_io.core.common import get_parquet_metadata

            metadata, _ = get_parquet_metadata(working_file, verbose=False)

            # Auto version mode: same contract as the file-based path (todo 040).
            if geoparquet_version is None:
                from geoparquet_io.core.common import resolve_geoparquet_version_from_file

                geoparquet_version = resolve_geoparquet_version_from_file(working_file, False)

            # Update metadata with target CRS
            if metadata and b"geo" in metadata:
                try:
                    geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
                    apply_target_crs_to_geo_meta(geo_meta, geom_col, target_crs, con)
                    if _target_crs_is_projected(target_crs, con):
                        _drop_nonplanar_edges_from_geo_meta(geo_meta, target_crs, geom_col)
                    metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
                except (json.JSONDecodeError, KeyError) as e:
                    debug(f"Could not update CRS in geo metadata, leaving as-is: {e}")

            # Reprojection moves coordinates, so the carried bbox (and, for a
            # geometry-repairing transform, geometry_types) no longer describes
            # the output; drop them so they are recomputed from the written data.
            # Only the transformed column, though: a secondary geometry column
            # is passed through untouched, and its stats must survive (#890).
            metadata = strip_derived_stats(metadata, columns={geom_col})

            # Write output using stream_io
            write_output(
                con,
                query,
                output_path,
                original_metadata=metadata,
                compression=compression,
                compression_level=compression_level,
                verbose=verbose,
                profile=profile,
                geoparquet_version=geoparquet_version,
            )

            if not should_stream_output(output_path):
                success(f"Reprojected from {effective_source_crs} to {target_crs}: {output_path}")

        finally:
            con.close()

    finally:
        # Clean up temp input file(s). unlink(missing_ok) avoids masking errors.
        if temp_input_file:
            Path(temp_input_file).unlink(missing_ok=True)
        if temp_sanitized:
            Path(temp_sanitized).unlink(missing_ok=True)


def reproject(
    input_parquet: str,
    output_parquet: str | None = None,
    target_crs: str = "EPSG:4326",
    source_crs: str | None = None,
    overwrite: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    verbose: bool = False,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    on_progress: Callable[[str], None] | None = None,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
    memory_limit: str | None = None,
    assume_crs84: bool = False,
) -> ReprojectResult | None:
    """
    Reproject a GeoParquet file to a different CRS.

    Supports Arrow IPC streaming:
    - Input "-" reads from stdin
    - Output "-" or None (with piped stdout) streams to stdout

    Args:
        input_parquet: Path to input file (local, remote URL, or "-" for stdin)
        output_parquet: Path to output file, "-" for stdout, or None for auto-detect
        target_crs: Target CRS (default: EPSG:4326)
        source_crs: Override source CRS. If None, detected from metadata.
        overwrite: If True and output_parquet is None, overwrite input file
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        verbose: Whether to print verbose output
        profile: AWS profile name for S3 operations
        geoparquet_version: GeoParquet version to write
        on_progress: Optional callback for progress messages
        row_group_size_mb: Row group size in MB (mutually exclusive with row_group_rows)
        row_group_rows: Row group size in number of rows (mutually exclusive with row_group_size_mb)
        memory_limit: DuckDB memory limit for write operations (e.g., "2GB")

    Returns:
        ReprojectResult with information, or None for streaming output
    """
    # Check for streaming mode
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if is_streaming:
        _reproject_streaming(
            input_parquet,
            output_parquet,
            target_crs,
            source_crs,
            compression,
            compression_level,
            verbose,
            profile,
            geoparquet_version,
            assume_crs84=assume_crs84,
        )
        return None

    # Use the original implementation for file-based mode
    return reproject_impl(
        input_parquet,
        output_parquet,
        target_crs,
        source_crs,
        overwrite,
        compression,
        compression_level,
        verbose,
        profile,
        geoparquet_version,
        on_progress,
        row_group_size_mb,
        row_group_rows,
        memory_limit,
        assume_crs84=assume_crs84,
    )
