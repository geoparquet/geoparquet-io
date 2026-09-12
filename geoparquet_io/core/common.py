"""Shared helpers for reading and writing GeoParquet.

This module is being taken apart. It grew to 4,138 lines with 50 importers, and
the deferred (function-level) imports written across ``core/`` to dodge cycles
through it are the symptom -- a module this central is one every new helper has
to route around. Blocks that answer a single question and carry no coupling to
the rest have moved out to modules named for that question.

Every moved name that a caller imports is still importable from here, so that
no caller had to change in the commit that moved the code; module-private
helpers nothing else reached went with their module and were not shimmed. Those
re-exports are written
``import X as X`` -- the explicit re-export form -- which is both how a reader
tells a compatibility shim from a helper this module actually uses, and how
ruff and vulture tell the same thing. The shims are deleted, and their callers
repointed at the real home, in a later pass. What went where:

* :mod:`geoparquet_io.core.arrow_geo_metadata` -- the ``geo`` block for an Arrow table
* :mod:`geoparquet_io.core.arrow_types` -- Arrow type promotion and casting
* :mod:`geoparquet_io.core.bbox_structure` -- bbox column / covering detection
* :mod:`geoparquet_io.core.compression` -- compression option validation
* :mod:`geoparquet_io.core.file_type` -- GeoParquet file-type detection
* :mod:`geoparquet_io.core.format_writers` -- ``create_shapefile_zip``
* :mod:`geoparquet_io.core.geo_metadata_repair` -- the ``geo`` block for a written file
* :mod:`geoparquet_io.core.parquet_write` -- ``write_parquet_with_metadata`` and
  ``write_geoparquet_table``, the two write funnels
* :mod:`geoparquet_io.core.sizing` -- byte-size parsing and formatting
"""

import os

import duckdb
import pyarrow.parquet as pq

from geoparquet_io.core.arrow_geo_metadata import (
    _CARRIED_SCHEMA_METADATA_KEYS as _CARRIED_SCHEMA_METADATA_KEYS,
)
from geoparquet_io.core.arrow_geo_metadata import (
    _CARRIED_SCHEMA_METADATA_KEYS_BYTES as _CARRIED_SCHEMA_METADATA_KEYS_BYTES,
)
from geoparquet_io.core.arrow_geo_metadata import _DIMENSION_SUFFIXES as _DIMENSION_SUFFIXES
from geoparquet_io.core.arrow_geo_metadata import _GEOMETRY_TYPE_CODES as _GEOMETRY_TYPE_CODES
from geoparquet_io.core.arrow_geo_metadata import (
    _apply_geoparquet_metadata,
    _detect_version_from_table,
    _normalize_arrow_large_types,
    _write_table_with_settings,
)
from geoparquet_io.core.arrow_geo_metadata import (
    _canonicalize_wkb_columns as _canonicalize_wkb_columns,
)
from geoparquet_io.core.arrow_geo_metadata import _compute_bbox_from_data as _compute_bbox_from_data
from geoparquet_io.core.arrow_geo_metadata import _compute_geometry_types as _compute_geometry_types
from geoparquet_io.core.arrow_geo_metadata import (
    _detect_bbox_column_from_table as _detect_bbox_column_from_table,
)
from geoparquet_io.core.arrow_geo_metadata import _estimate_row_size as _estimate_row_size
from geoparquet_io.core.arrow_geo_metadata import (
    _get_geometry_type_name as _get_geometry_type_name,
)
from geoparquet_io.core.arrow_geo_metadata import (
    _parse_geo_metadata_quietly as _parse_geo_metadata_quietly,
)
from geoparquet_io.core.arrow_geo_metadata import (
    _process_geometry_column_for_version as _process_geometry_column_for_version,
)
from geoparquet_io.core.arrow_geo_metadata import (
    _strip_geo_metadata_key as _strip_geo_metadata_key,
)
from geoparquet_io.core.arrow_geo_metadata import (
    _strip_geoarrow_to_plain_wkb as _strip_geoarrow_to_plain_wkb,
)
from geoparquet_io.core.arrow_types import _cast_table_to_schema as _cast_table_to_schema
from geoparquet_io.core.arrow_types import _compute_unified_schema as _compute_unified_schema
from geoparquet_io.core.arrow_types import _promote_numeric_type as _promote_numeric_type
from geoparquet_io.core.bbox_structure import BboxInfo as BboxInfo
from geoparquet_io.core.bbox_structure import (
    _bbox_column_from_covering as _bbox_column_from_covering,
)
from geoparquet_io.core.bbox_structure import check_bbox_structure
from geoparquet_io.core.bbox_structure import get_bbox_advice as get_bbox_advice
from geoparquet_io.core.compression import validate_compression_settings

# Internal imports - used by functions in this module
from geoparquet_io.core.duckdb_utils import (
    _DuckDBSchemaWrapper,
    _get_query_column_type,
    _get_query_columns,
    _wrap_query_with_wkb_conversion,
    get_duckdb_connection,
    load_community_extension,
    quote_identifier,
    sql_path,
)
from geoparquet_io.core.exceptions import (
    GeoParquetError,
    InvalidParameterError,
)
from geoparquet_io.core.file_type import detect_geoparquet_file_type
from geoparquet_io.core.file_type import (
    detect_geoparquet_file_type_cache_clear as detect_geoparquet_file_type_cache_clear,
)
from geoparquet_io.core.file_utils import (
    get_first_parquet_file,
    is_partition_path,
    resolve_file_url,
)
from geoparquet_io.core.format_writers import create_shapefile_zip as create_shapefile_zip
from geoparquet_io.core.geo_metadata import (
    DEFAULT_GEOPARQUET_VERSION as DEFAULT_GEOPARQUET_VERSION,
)
from geoparquet_io.core.geo_metadata import GEOPARQUET_VERSIONS as GEOPARQUET_VERSIONS
from geoparquet_io.core.geo_metadata import (
    build_bbox_covering,
)
from geoparquet_io.core.geo_metadata_repair import _crs_from_geo_logical as _crs_from_geo_logical
from geoparquet_io.core.geo_metadata_repair import (
    _ensure_v2_geo_metadata as _ensure_v2_geo_metadata,
)
from geoparquet_io.core.geo_metadata_repair import (
    _geography_edges_from_logical as _geography_edges_from_logical,
)
from geoparquet_io.core.geo_metadata_repair import (
    _rewrite_file_with_geo_metadata as _rewrite_file_with_geo_metadata,
)
from geoparquet_io.core.geometry_detection import (
    _detect_geometry_from_query,
    find_primary_geometry_column,
)
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    error,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.parquet_write import (
    _DUCKDB_GENERATED_COLUMN_FIELDS as _DUCKDB_GENERATED_COLUMN_FIELDS,
)
from geoparquet_io.core.parquet_write import (
    _REQUIRED_CARRIED_GEO_FIELDS as _REQUIRED_CARRIED_GEO_FIELDS,
)
from geoparquet_io.core.parquet_write import _apply_nonplanar_edges as _apply_nonplanar_edges
from geoparquet_io.core.parquet_write import (
    _auto_fix_vecorel_if_needed as _auto_fix_vecorel_if_needed,
)
from geoparquet_io.core.parquet_write import (
    _carries_more_than_duckdb_generates as _carries_more_than_duckdb_generates,
)
from geoparquet_io.core.parquet_write import (
    _collect_nonplanar_edges_from_metadata as _collect_nonplanar_edges_from_metadata,
)
from geoparquet_io.core.parquet_write import (
    _edges_for_output_version as _edges_for_output_version,
)
from geoparquet_io.core.parquet_write import (
    _geo_block_to_carry_on_fast_path as _geo_block_to_carry_on_fast_path,
)
from geoparquet_io.core.parquet_write import _plain_copy_to as _plain_copy_to
from geoparquet_io.core.parquet_write import (
    _preserve_edges_after_write as _preserve_edges_after_write,
)
from geoparquet_io.core.parquet_write import (
    _prune_metadata_to_output_columns as _prune_metadata_to_output_columns,
)
from geoparquet_io.core.parquet_write import (
    collect_nonplanar_edges as collect_nonplanar_edges,
)
from geoparquet_io.core.parquet_write import (
    extract_preserved_kv_metadata as extract_preserved_kv_metadata,
)
from geoparquet_io.core.parquet_write import (
    read_preserved_kv_metadata as read_preserved_kv_metadata,
)
from geoparquet_io.core.parquet_write import write_geoparquet_table as write_geoparquet_table
from geoparquet_io.core.parquet_write import write_parquet_with_metadata
from geoparquet_io.core.remote import (
    _sanitize_url_for_logging,
    is_remote_url,
    needs_httpfs,
    remote_write_context,
    upload_if_remote,
)
from geoparquet_io.core.sizing import format_size as format_size
from geoparquet_io.core.sizing import parse_size_string as parse_size_string
from geoparquet_io.core.streaming import extract_version_from_metadata


def should_skip_bbox(geoparquet_version):
    """Check if bbox column should be skipped for this GeoParquet version.

    For GeoParquet 2.0 and parquet-geo-only, bbox columns are not needed because
    native Parquet geo types provide row group statistics for spatial filtering.

    Args:
        geoparquet_version: Version string (e.g., "1.1", "2.0", "parquet-geo-only")

    Returns:
        bool: True if bbox should be skipped, False if bbox should be added
    """
    return geoparquet_version in ("2.0", "parquet-geo-only", "1.1-geoarrow")


def get_parquet_metadata(parquet_file, verbose=False):
    """
    Get Parquet file metadata using DuckDB for kv_metadata and PyArrow for schema.

    For partitioned datasets (directories or glob patterns), reads metadata
    from the first file.

    Returns:
        tuple: (kv_metadata dict, PyArrow schema)

    Note: Uses DuckDB for metadata extraction but returns PyArrow schema for
    backward compatibility with code that expects schema.field() methods.
    """

    from geoparquet_io.core.duckdb_metadata import get_kv_metadata

    # For partitions, use first file for metadata
    file_to_check = parquet_file
    if is_partition_path(parquet_file):
        first_file = get_first_parquet_file(parquet_file)
        if first_file:
            file_to_check = first_file

    # Get key-value metadata (returns dict like {b'geo': b'...'})
    kv_metadata = get_kv_metadata(file_to_check)

    # Get PyArrow schema for backward compatibility
    # (some code uses schema.field(i).name patterns)
    if is_remote_url(file_to_check):
        # For remote files, use a DuckDB-based approach to read schema
        from geoparquet_io.core.duckdb_metadata import get_schema_info

        schema_info = get_schema_info(file_to_check)
        # Create a simple object that mimics PyArrow schema for basic usage
        schema = _DuckDBSchemaWrapper(schema_info)
    else:
        pf = pq.ParquetFile(file_to_check)
        schema = pf.schema_arrow

    if verbose and kv_metadata:
        debug("\nParquet metadata key-value pairs:")
        for key, value in kv_metadata.items():
            key_str = key.decode("utf-8") if isinstance(key, bytes) else key
            debug(f"{key_str}: {value}")

    return kv_metadata, schema


def calculate_file_bounds(file_path, geom_column=None, verbose=False):
    """
    Calculate the bounding box of all geometries in a parquet file.

    Uses DuckDB's spatial extension to compute the extent of all geometries.

    Args:
        file_path: Path to the parquet file (local or remote URL)
        geom_column: Name of geometry column (auto-detected if None)
        verbose: Print verbose output

    Returns:
        tuple: (xmin, ymin, xmax, ymax) or None if calculation fails
    """
    if geom_column is None:
        geom_column = find_primary_geometry_column(file_path, verbose=False)

    # RAW path: sql_path escapes it at the point of interpolation, so nothing
    # downstream can escape it a second time (#802).
    read_url = resolve_file_url(file_path, verbose=False)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(file_path))

    try:
        bounds_query = f"""
            SELECT
                MIN(ST_XMin({quote_identifier(geom_column)})) as xmin,
                MIN(ST_YMin({quote_identifier(geom_column)})) as ymin,
                MAX(ST_XMax({quote_identifier(geom_column)})) as xmax,
                MAX(ST_YMax({quote_identifier(geom_column)})) as ymax
            FROM read_parquet({sql_path(read_url)})
        """
        result = con.execute(bounds_query).fetchone()

        if result and all(v is not None for v in result):
            if verbose:
                debug(
                    f"Calculated bounds: ({result[0]:.6f}, {result[1]:.6f}, "
                    f"{result[2]:.6f}, {result[3]:.6f})"
                )
            return result
        return None
    except Exception as e:
        if verbose:
            debug(f"Failed to calculate bounds: {e}")
        return None
    finally:
        con.close()


def zm_suffix_sql(geom_expr: str, sep: str = " ") -> str:
    """SQL fragment producing the dimension suffix (''/'{sep}Z'/'{sep}M'/'{sep}ZM').

    The GeoParquet spec treats "LineString" and "LineString ZM" as distinct
    geometry_types entries, so any SQL type scan must append this suffix to
    ST_GeometryType() output instead of collapsing dimensions.
    """
    return (
        f"CASE WHEN ST_HasZ({geom_expr}) AND ST_HasM({geom_expr}) THEN '{sep}ZM' "
        f"WHEN ST_HasZ({geom_expr}) THEN '{sep}Z' "
        f"WHEN ST_HasM({geom_expr}) THEN '{sep}M' "
        f"ELSE '' END"
    )


def split_zm_suffix(name: str) -> tuple[str, str]:
    """Split a spec geometry type into (base, suffix): 'Point ZM' -> ('Point', ' ZM').

    Names without a dimension suffix (and non-string inputs) return ('' suffix).
    """
    if isinstance(name, str):
        for suffix in (" ZM", " Z", " M"):
            if name.endswith(suffix):
                return name[: -len(suffix)], suffix
    return name, ""


# DuckDB ST_GeometryType names ("POINT") -> GeoParquet spec names ("Point").
_DUCKDB_TO_SPEC_TYPE = {
    "POINT": "Point",
    "LINESTRING": "LineString",
    "POLYGON": "Polygon",
    "MULTIPOINT": "MultiPoint",
    "MULTILINESTRING": "MultiLineString",
    "MULTIPOLYGON": "MultiPolygon",
    "GEOMETRYCOLLECTION": "GeometryCollection",
}


def compute_geometry_types_via_sql(
    con,
    query: str,
    geometry_column: str,
) -> list[str]:
    """
    Compute distinct geometry types from query using DuckDB.

    Args:
        con: DuckDB connection with spatial extension loaded
        query: SQL query containing geometry column
        geometry_column: Name of geometry column

    Returns:
        List of spec geometry type names with dimension suffixes
        (e.g., ["Point", "LineString ZM"]) or empty list if column not in query
    """
    # Check if geometry column exists in query result
    try:
        columns = _get_query_columns(con, query)
        if geometry_column not in columns:
            return []
    except (duckdb.Error, RuntimeError, ValueError, AttributeError):
        # If we can't determine schema, return empty list rather than failing
        return []

    # GeoArrow native types (STRUCT(x DOUBLE, y DOUBLE)[N]) can't use ST_GeometryType.
    # Returning [] is valid — GeoParquet allows omitting geometry_types.
    col_type = _get_query_column_type(con, query, geometry_column) or ""
    if "STRUCT" in col_type:
        return []

    quoted_col = quote_identifier(geometry_column)
    typed_expr = f"ST_GeometryType({quoted_col}) || {zm_suffix_sql(quoted_col)}"
    types_query = f"""
        SELECT DISTINCT {typed_expr} as geom_type
        FROM ({query})
        WHERE {quoted_col} IS NOT NULL
    """
    results = con.execute(types_query).fetchall()

    types = []
    for (geom_type,) in results:
        if geom_type:
            base, suffix = split_zm_suffix(geom_type)
            normalized = _DUCKDB_TO_SPEC_TYPE.get(base.upper(), base)
            types.append(normalized + suffix)

    return sorted(set(types))


# DuckDB ST_ZMFlag codes -> geoarrow.types.Dimensions values.
# DuckDB: 0=XY, 1=XYM, 2=XYZ, 3=XYZM. geoarrow: 1=XY, 2=XYZ, 3=XYM, 4=XYZM.
_DUCKDB_ZMFLAG_TO_GEOARROW_DIM = {0: 1, 1: 3, 2: 2, 3: 4}


def compute_geometry_dimensions_via_sql(
    con,
    query: str,
    geometry_column: str,
) -> set[int]:
    """Compute the distinct coordinate dimensions of a geometry column via DuckDB.

    Used by the 1.1-geoarrow native path to pick a Z/M-aware GeoArrow type rather
    than silently coercing 3D/measured coordinates down to 2D.

    Returns:
        Set of geoarrow dimension codes (1=XY, 2=XYZ, 3=XYM, 4=XYZM). Empty set when
        the column is absent, native nested (STRUCT), or the dimension is undetectable.
    """
    try:
        columns = _get_query_columns(con, query)
        if geometry_column not in columns:
            return set()
    except (duckdb.Error, RuntimeError, ValueError, AttributeError):
        return set()

    # Native nested GeoArrow (STRUCT) cannot use ST_ZMFlag; leave dimension unknown.
    col_type = _get_query_column_type(con, query, geometry_column) or ""
    if "STRUCT" in col_type:
        return set()

    quoted_col = quote_identifier(geometry_column)
    geom_expr = quoted_col
    if "BLOB" in col_type or "BINARY" in col_type:
        geom_expr = f"ST_GeomFromWKB({geom_expr})"

    dims_query = f"""
        SELECT DISTINCT ST_ZMFlag({geom_expr}) AS zm
        FROM ({query})
        WHERE {quoted_col} IS NOT NULL
    """
    try:
        results = con.execute(dims_query).fetchall()
    except (duckdb.Error, RuntimeError, ValueError):
        return set()

    dims: set[int] = set()
    for (zm,) in results:
        if zm is not None and zm in _DUCKDB_ZMFLAG_TO_GEOARROW_DIM:
            dims.add(_DUCKDB_ZMFLAG_TO_GEOARROW_DIM[zm])
    return dims


def _resolve_auto_version(detected: str | None) -> str | None:
    """Shared auto-mode decision for CLI and API write paths.

    Preserve a detected 1.x/2.0 version; upgrade native-geo-only inputs to 2.0
    (the documented --geoparquet-version default contract).
    """
    if detected == "parquet-geo-only":
        return "2.0"
    return detected


def resolve_geoparquet_version_from_file(parquet_file: str, verbose: bool = False) -> str | None:
    """
    Resolve the auto-mode GeoParquet write version from a parquet input file.

    Implements the documented --geoparquet-version default: preserve the input
    version (1.x normalizes to 1.1, matching extract_version_from_metadata),
    upgrade native-geo-only inputs to 2.0, otherwise None (caller defaults).

    Returns None when the file cannot be inspected (e.g., remote without
    credentials) so callers fall back to the default.
    """
    try:
        info = detect_geoparquet_file_type(parquet_file, verbose)
    except Exception as e:
        debug(f"Version auto-detect failed for {parquet_file}: {e}; falling back to default")
        return None

    detected = {
        "geoparquet_v2": "2.0",
        "parquet_geo_only": "parquet-geo-only",
        "geoparquet_v1": "1.1",
    }.get(info.get("file_type"))
    return _resolve_auto_version(detected)


def resolve_geoparquet_version_from_table(table, verbose: bool = False) -> str | None:
    """
    Resolve the auto-mode GeoParquet write version from an Arrow table.

    API-side counterpart of resolve_geoparquet_version_from_file, sharing the
    same decision (_resolve_auto_version) so ``gpio.read(f).write(out)`` picks
    the same version the CLI would for the same input file (todo 043).
    """
    return _resolve_auto_version(_detect_version_from_table(table, verbose))


def write_geoparquet_via_arrow(
    con,
    query: str,
    output_file: str,
    geometry_column: str | None = None,
    original_metadata: dict | None = None,
    compression: str = "ZSTD",
    compression_level: int = 15,
    row_group_size_mb: int | None = None,
    row_group_rows: int | None = None,
    custom_metadata: dict | None = None,
    verbose: bool = False,
    show_sql: bool = False,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    input_crs: dict | None = None,
) -> None:
    """
    Write a GeoParquet file using Arrow as the internal transfer format.

    This is more efficient than the COPY-then-rewrite approach because it:
    1. Fetches query results directly as an Arrow Table
    2. Applies GeoParquet metadata in memory
    3. Writes once to disk

    Args:
        con: DuckDB connection with spatial extension loaded
        query: SQL SELECT query to execute
        output_file: Path to output file (local or remote URL)
        geometry_column: Name of geometry column (auto-detected if None)
        original_metadata: Original metadata from source file for preservation
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        custom_metadata: Optional dict with custom metadata (e.g., H3 covering info)
        verbose: Whether to print verbose output
        show_sql: Whether to print SQL statements before execution
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        input_crs: PROJJSON dict with CRS from input file
    """
    # Detect geometry column if not provided
    if geometry_column is None:
        geometry_column = _detect_geometry_from_query(con, query, original_metadata, verbose)

    # Auto-detect GeoParquet version from input metadata if not explicitly provided
    if geoparquet_version is None:
        geoparquet_version = extract_version_from_metadata(original_metadata)

    # Check if geometry column actually exists in the query result
    query_columns = _get_query_columns(con, query)
    has_geometry = geometry_column in query_columns

    # No AWS_PROFILE env mutation here: the write target inside this block is a
    # local (temp) file, and the upload at the end is credentialed by passing
    # profile= straight through to upload().
    with remote_write_context(output_file, is_directory=False, verbose=verbose) as (
        actual_output,
        is_remote,
    ):
        # Validate compression settings
        compression, compression_level, compression_desc = validate_compression_settings(
            compression, compression_level, verbose
        )

        if verbose:
            debug(f"Writing output with {compression_desc} compression (Arrow path)...")
            if geoparquet_version:
                debug(f"Using GeoParquet version: {geoparquet_version}")

        # Wrap query with WKB conversion only if geometry column exists
        if has_geometry:
            final_query = _wrap_query_with_wkb_conversion(query, geometry_column, con)
        else:
            final_query = query
            if verbose:
                debug(
                    f"Geometry column '{geometry_column}' not in query - writing as regular Parquet"
                )

        if show_sql:
            info("\n-- Arrow query (with WKB conversion):" if has_geometry else "\n-- Arrow query:")
            progress(final_query)

        # Fetch as Arrow table
        if verbose:
            debug("Fetching query results as Arrow table...")

        result = con.execute(final_query)
        table = result.arrow().read_all()

        # Normalize large_string/large_binary back to string/binary for Parquet compatibility
        table = _normalize_arrow_large_types(table)

        if verbose:
            debug(f"Fetched {table.num_rows:,} rows, {len(table.column_names)} columns")

        # Apply GeoParquet metadata only if geometry column exists
        if has_geometry:
            table = _apply_geoparquet_metadata(
                table,
                geometry_column=geometry_column,
                geoparquet_version=geoparquet_version,
                original_metadata=original_metadata,
                input_crs=input_crs,
                custom_metadata=custom_metadata,
                verbose=verbose,
            )

        # Write to disk
        _write_table_with_settings(
            table,
            actual_output,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            row_group_size_mb=row_group_size_mb,
            geoparquet_version=geoparquet_version,
            geometry_column=geometry_column,
            verbose=verbose,
        )

        # Upload to remote if needed
        if is_remote:
            upload_if_remote(
                actual_output,
                output_file,
                profile=profile,
                is_directory=False,
                verbose=verbose,
            )


def _build_bounds_query(parquet_path, bbox_info, geometry_column, verbose):
    """Build query for bounds calculation.

    ``parquet_path`` is RAW: ``sql_path`` quotes and escapes it here, at the one
    point it becomes SQL (#802).
    """
    if bbox_info["has_bbox_column"]:
        bbox_col = bbox_info["bbox_column_name"]
        if verbose:
            debug(f"Using bbox column '{bbox_col}' for fast bounds calculation")

        q_bbox = quote_identifier(bbox_col)
        return f"""
        SELECT
            MIN({q_bbox}.xmin) as xmin,
            MIN({q_bbox}.ymin) as ymin,
            MAX({q_bbox}.xmax) as xmax,
            MAX({q_bbox}.ymax) as ymax
        FROM {sql_path(parquet_path)}
        """
    else:
        warn(
            f"⚠️  No bbox column found - calculating bounds from geometry column '{geometry_column}' (this may be slow)"
        )
        info("💡 Tip: Add a bbox column for faster operations with 'gpio add bbox'")

        q_geom = quote_identifier(geometry_column)
        return f"""
        SELECT
            MIN(ST_XMin({q_geom})) as xmin,
            MIN(ST_YMin({q_geom})) as ymin,
            MAX(ST_XMax({q_geom})) as xmax,
            MAX(ST_YMax({q_geom})) as ymax
        FROM {sql_path(parquet_path)}
        """


def get_dataset_bounds(parquet_file, geometry_column=None, verbose=False):
    """
    Calculate the bounding box of the entire dataset.

    Uses bbox column if available for fast calculation, otherwise calculates
    from geometry column (slower).

    Args:
        parquet_file: Path to the parquet file
        geometry_column: Geometry column name (if None, will auto-detect)
        verbose: Whether to print verbose output

    Returns:
        tuple: (xmin, ymin, xmax, ymax) or None if error
    """
    configure_verbose(verbose)
    read_url = resolve_file_url(parquet_file, verbose)

    # Get geometry column if not specified
    if not geometry_column:
        geometry_column = find_primary_geometry_column(parquet_file, verbose)

    # Check for bbox column
    bbox_info = check_bbox_structure(parquet_file, verbose)

    # Create DuckDB connection with httpfs if needed
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))

    try:
        query = _build_bounds_query(read_url, bbox_info, geometry_column, verbose)
        result = con.execute(query).fetchone()

        if result and all(v is not None for v in result):
            xmin, ymin, xmax, ymax = result
            if verbose:
                debug(f"Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")
            return (xmin, ymin, xmax, ymax)
        else:
            if verbose:
                warn("Could not calculate bounds (empty dataset or null geometries)")
            return None

    except Exception as e:
        if verbose:
            error(f"Error calculating bounds: {e}")
        return None
    finally:
        con.close()


def add_computed_column(
    input_parquet,
    output_parquet,
    column_name,
    sql_expression,
    extensions=None,
    dry_run=False,
    verbose=False,
    compression="ZSTD",
    compression_level=None,
    row_group_size_mb=None,
    row_group_rows=None,
    dry_run_description=None,
    custom_metadata=None,
    profile=None,
    replace_column=None,
    geoparquet_version=None,
    memory_limit: str | None = None,
):
    """
    Add a computed column to a GeoParquet file using SQL expression.

    Handles all boilerplate for adding columns derived from existing data:
    - Input validation
    - Schema checking
    - DuckDB connection and extension loading
    - Query execution
    - Metadata preservation
    - Dry-run support
    - Remote input/output support

    Args:
        input_parquet: Path to input file (local or remote URL)
        output_parquet: Path to output file (local or remote URL)
        column_name: Name for the new column
        sql_expression: SQL expression to compute column value
        extensions: DuckDB extensions to load beyond 'spatial' (e.g., ['h3'])
        dry_run: Whether to print SQL without executing
        verbose: Whether to print verbose output
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        dry_run_description: Optional description for dry-run output
        custom_metadata: Optional dict with custom metadata (e.g., H3 info)
        profile: AWS profile name (S3 only, optional)
        replace_column: Name of existing column to replace (uses EXCLUDE in query)
        memory_limit: DuckDB memory limit for the write (e.g., '2GB', '512MB');
            None auto-detects based on available system/container memory

    Example:
        add_computed_column(
            'input.parquet', 'output.parquet',
            column_name='h3_cell',
            sql_expression="h3_latlng_to_cell(ST_Y(ST_Centroid(geometry)), "
                          "ST_X(ST_Centroid(geometry)), 9)",
            extensions=['h3'],
            custom_metadata={'covering': {'h3': {'column': 'h3_cell', 'resolution': 9}}}
        )
    """
    # RAW path: the dry-run header below shows it to the user, and every SQL
    # interpolation escapes it through sql_path (#802).
    input_path = resolve_file_url(input_parquet, verbose)

    # Get geometry column (for reference)
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Dry-run mode header
    if dry_run:
        warn("\n=== DRY RUN MODE - SQL Commands that would be executed ===\n")
        display_input = (
            _sanitize_url_for_logging(input_path) if is_remote_url(input_path) else input_path
        )
        display_output = (
            _sanitize_url_for_logging(output_parquet)
            if is_remote_url(output_parquet)
            else output_parquet
        )
        info(f"-- Input file: {display_input}")
        info(f"-- Output file: {display_output}")
        info(f"-- Geometry column: {geom_col}")
        info(f"-- New column: {column_name}")
        if dry_run_description:
            info(f"-- Description: {dry_run_description}")
        progress("")

    # Check if column already exists (skip in dry-run or when replacing)
    if not dry_run:
        from geoparquet_io.core.duckdb_metadata import get_column_names

        # Only check for column collision if not replacing
        if not replace_column:
            column_names = get_column_names(input_parquet)
            if column_name in column_names:
                raise InvalidParameterError(
                    "column_name",
                    f"Column '{column_name}' already exists in the file. "
                    f"Please choose a different name.",
                )

        # Get metadata before processing
        metadata, _ = get_parquet_metadata(input_parquet, verbose)

        if verbose:
            if replace_column:
                debug(f"Replacing column '{replace_column}' with '{column_name}'...")
            else:
                debug(f"Adding column '{column_name}'...")

    # Create DuckDB connection with httpfs if needed
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))
    # Ensure ST_Transform (used by CRS-aware grid keying) emits lon/lat order.
    con.execute("SET geometry_always_xy = true;")

    # Load additional extensions if specified
    if extensions:
        for ext in extensions:
            if verbose and not dry_run:
                debug(f"Loading DuckDB extension: {ext}")
            load_community_extension(con, ext)

    # Get total count (skip in dry-run)
    if not dry_run:
        total_count = con.execute(f"SELECT COUNT(*) FROM {sql_path(input_path)}").fetchone()[0]
        progress(f"Processing {total_count:,} features...")

    # Build the query
    # Quote column name to handle special characters (e.g., colons in "metrics:area")
    quoted_col = quote_identifier(column_name)

    # Use EXCLUDE to drop existing column when replacing
    if replace_column:
        query = f"""
        SELECT
            * EXCLUDE ({quote_identifier(replace_column)}),
            {sql_expression} AS {quoted_col}
        FROM {sql_path(input_path)}
    """
    else:
        query = f"""
        SELECT
            *,
            {sql_expression} AS {quoted_col}
        FROM {sql_path(input_path)}
    """

    # Handle dry-run display
    if dry_run:
        # Show formatted query with COPY wrapper
        compression_desc = compression
        if compression in ["GZIP", "ZSTD", "BROTLI"] and compression_level:
            compression_desc = f"{compression}:{compression_level}"

        duckdb_compression = (
            compression.lower() if compression != "UNCOMPRESSED" else "uncompressed"
        )
        display_query = f"""COPY ({query.strip()})
TO {sql_path(output_parquet)}
(FORMAT PARQUET, COMPRESSION '{duckdb_compression}');"""

        info("-- Main query:")
        progress(display_query)
        info(f"\n-- Note: Using {compression_desc} compression")
        info("-- This query creates a new parquet file with the computed column added")
        info("-- Metadata would also be updated with proper GeoParquet covering information")
        return

    # Execute the query using existing write helper
    if verbose:
        debug(f"Creating column '{column_name}'...")

    write_parquet_with_metadata(
        con,
        query,
        output_parquet,
        original_metadata=metadata,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        custom_metadata=custom_metadata,
        verbose=verbose,
        profile=profile,
        geoparquet_version=geoparquet_version,
        # The precondition: the query above is `SELECT *` plus one computed
        # column, so the rows this write reads are `input_parquet`'s own.
        input_file=input_parquet,
        memory_limit=memory_limit,
    )


def add_bbox(parquet_file, bbox_column_name="bbox", verbose=False):
    """
    Add a bbox struct column to a GeoParquet file in-place.

    Internal helper function used by --add-bbox flags in other commands
    (hilbert_order, add_country_codes). Modifies the file in-place by
    writing to a temporary file and replacing the original.

    Raises an error if the bbox column already exists.

    Args:
        parquet_file: Path to the parquet file (will be modified in-place)
        bbox_column_name: Name for the bbox column (default: 'bbox')
        verbose: Whether to print verbose output

    Returns:
        bool: True if bbox was added successfully

    Raises:
        click.ClickException: If column already exists or operation fails
    """
    from geoparquet_io.core.duckdb_metadata import get_column_names

    # Check if column already exists using DuckDB
    column_names = get_column_names(parquet_file)

    if bbox_column_name in column_names:
        raise InvalidParameterError(
            "bbox_column_name",
            f"Column '{bbox_column_name}' already exists in the file. "
            f"Please choose a different name.",
        )

    # Get geometry column for SQL expression
    geom_col = find_primary_geometry_column(parquet_file, verbose)

    if verbose:
        debug(f"Adding bbox column for geometry column: {geom_col}")

    # Define SQL expression
    quoted_geom_col = quote_identifier(geom_col)
    sql_expression = f"""STRUCT_PACK(
        xmin := ST_XMin({quoted_geom_col}),
        ymin := ST_YMin({quoted_geom_col}),
        xmax := ST_XMax({quoted_geom_col}),
        ymax := ST_YMax({quoted_geom_col})
    )"""

    # Create temporary file path
    temp_file = parquet_file + ".tmp"

    try:
        # Use add_computed_column to write to temp file
        # Declare the covering explicitly rather than leaving a writer to infer
        # it from the column's name. gpio computed this column from the geometry
        # in this same statement, so the assertion "these values bound that
        # geometry" is one we can actually make here -- and nowhere else (#738).
        # `custom_metadata` is the existing route for that, and it is already
        # version-gated: strip_unsupported_covering drops the key for 1.0 output.
        bbox_covering = {"covering": {"bbox": build_bbox_covering(bbox_column_name)}}

        add_computed_column(
            input_parquet=parquet_file,
            output_parquet=temp_file,
            column_name=bbox_column_name,
            sql_expression=sql_expression,
            extensions=None,
            dry_run=False,
            verbose=verbose,
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            dry_run_description=None,
            custom_metadata=bbox_covering,
        )

        # Replace original file with updated file
        os.replace(temp_file, parquet_file)

        if verbose:
            success(f"Successfully added bbox column '{bbox_column_name}'")

        return True

    except Exception as e:
        # Clean up temporary file if something goes wrong
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise GeoParquetError(f"Failed to add bbox: {str(e)}") from e
