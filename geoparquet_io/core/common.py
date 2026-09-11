import copy
import json
import os
import re
from collections.abc import Collection
from pathlib import Path
from typing import Any, Literal, TypedDict

import duckdb
import pyarrow.parquet as pq

# Internal imports - used by functions in this module
from geoparquet_io.core.crs_utils import (
    NULL_CRS_HINT,
    _format_crs_display,
    _wrap_query_with_crs,
    apply_output_crs,
    is_default_crs,
)
from geoparquet_io.core.duckdb_utils import (
    _DuckDBSchemaWrapper,
    _get_query_column_type,
    _get_query_columns,
    _wrap_query_with_wkb_conversion,
    build_kv_metadata_clause,
    get_duckdb_connection,
    load_community_extension,
    quote_identifier,
    sql_path,
    validate_compression_level,
)
from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeoParquetError,
    InvalidParameterError,
)
from geoparquet_io.core.file_utils import (
    _get_file_cache_key,
    get_first_parquet_file,
    is_partition_path,
    resolve_file_url,
)
from geoparquet_io.core.geo_metadata import (
    DEFAULT_GEOPARQUET_VERSION,
    GEOPARQUET_VERSIONS,
    build_bbox_covering,
    carried_geometry_column,
    carried_version,
    create_geo_metadata,
    detect_bbox_column_from_schema,
    geoarrow_wkb_codes,
    prune_geo_metadata_to_columns,
    sanitize_geo_metadata,
    sanitized_carried_geo,
    strip_derived_stats,
    strip_nonplanar_edges,
)
from geoparquet_io.core.geoarrow_encoding import (
    is_geoarrow_extension_field,
    is_wkb_extension_field,
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
from geoparquet_io.core.parquet_writer import (
    ParquetWriteSettings,
    note_duckdb_copy_rounding,
    resolve_output_geoparquet_version,
    resolve_row_group_rows,
)
from geoparquet_io.core.remote import (
    _sanitize_url_for_logging,
    is_remote_url,
    needs_httpfs,
    remote_write_context,
    upload_if_remote,
)
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


# LRU cache for detect_geoparquet_file_type results
# Using a simple dict cache with manual mtime tracking for invalidation
_file_type_cache: dict[str, tuple[float, dict]] = {}
_FILE_TYPE_CACHE_MAX_SIZE = 100


def _check_file_type_cache(parquet_file: str) -> dict | None:
    """Check cache for file type detection result."""
    cache_key, mtime = _get_file_cache_key(parquet_file)
    if cache_key in _file_type_cache:
        cached_mtime, result = _file_type_cache[cache_key]
        # For remote files (mtime=0), always use cache
        # For local files, invalidate if file changed
        if mtime == 0 or cached_mtime == mtime:
            return result
    return None


def _update_file_type_cache(parquet_file: str, result: dict) -> None:
    """Update cache with file type detection result."""
    global _file_type_cache
    cache_key, mtime = _get_file_cache_key(parquet_file)

    # Simple LRU: if cache is full, clear half of it
    if len(_file_type_cache) >= _FILE_TYPE_CACHE_MAX_SIZE:
        # Remove oldest half
        keys_to_remove = list(_file_type_cache.keys())[: _FILE_TYPE_CACHE_MAX_SIZE // 2]
        for k in keys_to_remove:
            del _file_type_cache[k]

    _file_type_cache[cache_key] = (mtime, result)


def detect_geoparquet_file_type(parquet_file, verbose=False, con=None):
    """
    Detect the GeoParquet/Parquet-geo type of a file.

    Determines whether a file is:
    - GeoParquet 1.x (has geo metadata with version 1.x)
    - GeoParquet 2.0 (has geo metadata with version 2.x, uses native Parquet geo types)
    - Parquet-geo-only (has native Parquet geo types but NO geo metadata)
    - Unknown (no geo indicators found)

    Performance notes (Issue #232):
    - For local files, uses PyArrow for ~300x faster metadata reads
    - Results are cached with mtime-based invalidation
    - Pass `con` to reuse an existing DuckDB connection for remote files

    Args:
        parquet_file: Path to the parquet file
        verbose: Whether to print verbose output
        con: Optional DuckDB connection to reuse (for remote file operations)

    Returns:
        dict with:
            - has_geo_metadata: bool - Has 'geo' key in metadata
            - geo_version: str - GeoParquet version from metadata (e.g., "1.1.0", "2.0.0") or None
            - has_native_geo_types: bool - Has Parquet GEOMETRY/GEOGRAPHY logical types
            - file_type: str - One of: "geoparquet_v1", "geoparquet_v2", "parquet_geo_only", "unknown"
            - bbox_recommended: bool - Whether bbox column is recommended for this file type
    """
    from geoparquet_io.core.duckdb_metadata import (
        detect_geometry_columns,
        get_geo_metadata,
    )

    # Check cache first (skip if connection provided - caller wants fresh read)
    if con is None:
        cached_result = _check_file_type_cache(parquet_file)
        if cached_result is not None:
            if verbose:
                debug(f"File type detection (cached): {cached_result}")
            return cached_result.copy()  # Return copy to prevent mutation

    result = {
        "has_geo_metadata": False,
        "geo_version": None,
        "has_native_geo_types": False,
        "file_type": "unknown",
        "bbox_recommended": True,  # Default for v1.x
    }

    # Check for geo metadata (uses PyArrow for local files, DuckDB for remote)
    geo_meta = get_geo_metadata(parquet_file, con=con)
    if geo_meta:
        result["has_geo_metadata"] = True
        if isinstance(geo_meta, dict) and "version" in geo_meta:
            # A version that is not a string is no version this can report:
            # `2` reached `.startswith` below and took `check spec`,
            # `check bbox`, `check optimization` and `add bbox` down with a bare
            # `AttributeError` (#979). `geo_version` is documented as the
            # version *or None*, and every consumer already handles None.
            result["geo_version"] = carried_version(geo_meta["version"], source=str(parquet_file))

    # Check for native Parquet geo types using schema
    geo_columns = detect_geometry_columns(parquet_file, con=con)
    if geo_columns:
        result["has_native_geo_types"] = True

    # Determine file type
    if result["has_geo_metadata"]:
        version = result["geo_version"]
        if version and version.startswith("2."):
            result["file_type"] = "geoparquet_v2"
            result["bbox_recommended"] = False  # V2 uses native geo row group stats
        else:
            result["file_type"] = "geoparquet_v1"
            result["bbox_recommended"] = True  # V1.x needs bbox for spatial filtering
    elif result["has_native_geo_types"]:
        result["file_type"] = "parquet_geo_only"
        result["bbox_recommended"] = False  # Native geo types provide row group stats
    # else: remains "unknown"

    # Cache the result (only if no connection provided)
    if con is None:
        _update_file_type_cache(parquet_file, result)

    if verbose:
        debug(f"File type detection: {result}")

    return result


def detect_geoparquet_file_type_cache_clear():
    """Clear the file type detection cache."""
    global _file_type_cache
    _file_type_cache = {}


# Add cache_clear method to the function for compatibility
detect_geoparquet_file_type.cache_clear = detect_geoparquet_file_type_cache_clear


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
    import pyarrow.parquet as pq

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


def parse_size_string(size_str):
    """
    Parse a human-readable size string into bytes.

    Args:
        size_str: String like '256MB', '1GB', '128' (assumed MB if no unit)

    Returns:
        int: Size in bytes
    """
    if not size_str:
        return None

    # Handle plain numbers (assume MB)
    try:
        return int(size_str) * 1024 * 1024
    except ValueError:
        pass

    # Parse with units
    size_str = size_str.strip().upper()
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT]?B?)$", size_str)
    if not match:
        raise ValueError(f"Invalid size format: {size_str}")

    value = float(match.group(1))
    unit = match.group(2)

    # Convert to bytes
    multipliers = {
        "B": 1,
        "KB": 1024,
        "MB": 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
        "TB": 1024 * 1024 * 1024 * 1024,
        "K": 1024,
        "M": 1024 * 1024,
        "G": 1024 * 1024 * 1024,
        "T": 1024 * 1024 * 1024 * 1024,
    }

    multiplier = multipliers.get(unit, 1024 * 1024)  # Default to MB
    return int(value * multiplier)


def calculate_row_group_size(
    total_rows, file_size_bytes, target_row_group_size_mb=None, target_row_group_rows=None
):
    """
    Calculate optimal row group size for parquet file.

    Args:
        total_rows: Total number of rows in the file
        file_size_bytes: Current file size in bytes
        target_row_group_size_mb: Target size per row group in MB
        target_row_group_rows: Exact number of rows per row group

    Returns:
        int: Number of rows per row group
    """
    if target_row_group_rows:
        # Use exact row count if specified
        return min(target_row_group_rows, total_rows)

    if not target_row_group_size_mb:
        target_row_group_size_mb = 130  # Default 130MB

    # Convert target size to bytes
    target_bytes = target_row_group_size_mb * 1024 * 1024

    # Calculate average bytes per row
    if total_rows > 0 and file_size_bytes > 0:
        bytes_per_row = file_size_bytes / total_rows
        # Calculate number of rows that would fit in target size
        rows_per_group = int(target_bytes / bytes_per_row)
        # Ensure at least 1 row per group but not more than total rows
        return max(1, min(rows_per_group, total_rows))
    else:
        # Default to all rows in one group if we can't calculate
        return max(1, total_rows)


def validate_compression_settings(compression, compression_level, verbose=False):
    """
    Validate and normalize compression settings.

    Args:
        compression: Compression type string
        compression_level: Compression level (can be None for defaults)
        verbose: Whether to print verbose output

    Returns:
        tuple: (normalized_compression, validated_level, compression_desc)
    """
    compression = compression.upper()
    valid_compressions = ["ZSTD", "GZIP", "BROTLI", "LZ4", "SNAPPY", "UNCOMPRESSED"]

    if compression not in valid_compressions:
        raise InvalidParameterError(
            "compression",
            f"Invalid compression '{compression}'. Must be one of: {', '.join(valid_compressions)}",
        )

    # Handle compression level based on format
    compression_ranges = {
        "GZIP": (1, 9, 6),  # min, max, default
        "ZSTD": (1, 22, 15),  # min, max, default
        "BROTLI": (1, 11, 6),  # min, max, default
    }

    if compression in compression_ranges:
        min_level, max_level, default_level = compression_ranges[compression]

        # Use default if not specified
        if compression_level is None:
            compression_level = default_level

        if compression_level < min_level or compression_level > max_level:
            raise InvalidParameterError(
                "compression_level",
                f"{compression} compression level must be between {min_level} and {max_level}, got {compression_level}",
            )
        compression_desc = f"{compression}:{compression_level}"
    elif compression in ["LZ4", "SNAPPY"]:
        if compression_level and compression_level != 15 and verbose:  # Not default
            warn(
                f"Note: {compression} does not support compression levels. Ignoring level {compression_level}."
            )
        compression_level = None  # These formats don't use compression levels
        compression_desc = compression
    else:
        compression_level = None  # UNCOMPRESSED doesn't use levels
        compression_desc = compression

    return compression, compression_level, compression_desc


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


def _rebuild_array_with_type(
    chunked_array,
    new_type,
):
    """
    Rebuild a chunked array with a new extension type.

    This preserves CRS and other type metadata, unlike cast() which may reset them.
    Used when applying CRS to geoarrow geometry arrays.

    Args:
        chunked_array: PyArrow ChunkedArray to rebuild
        new_type: New PyArrow ExtensionType to apply

    Returns:
        pa.ChunkedArray: New chunked array with the new type
    """
    import pyarrow as pa

    new_chunks = []
    for chunk in chunked_array.chunks:
        new_chunk = pa.ExtensionArray.from_storage(new_type, chunk.storage)
        new_chunks.append(new_chunk)

    return pa.chunked_array(new_chunks, type=new_type)


def _type_category(arrow_type) -> str:
    """Classify a PyArrow type into a single promotion category.

    Categories are mutually exclusive, so two types in the same category are
    treated alike by the promotion rules. Returns "other" for types that have
    no special promotion handling (caller falls back to the first type).
    """
    import pyarrow as pa

    if pa.types.is_null(arrow_type):
        return "null"
    if pa.types.is_boolean(arrow_type):
        return "bool"
    if pa.types.is_integer(arrow_type):
        return "int"
    if pa.types.is_floating(arrow_type):
        return "float"
    if pa.types.is_decimal(arrow_type):
        return "decimal"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "string"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "binary"
    if pa.types.is_timestamp(arrow_type):
        return "timestamp"
    return "other"


def _promote_special(type_a, type_b, cat_a: str, cat_b: str):
    """Promote null / binary / string categories. Returns None if neither applies.

    Order matters: binary is handled before string so that binary geometry
    data is never coerced to string.
    """
    import pyarrow as pa

    if cat_a == "null":
        return type_b
    if cat_b == "null":
        return type_a

    # Binary types: don't promote to string (would corrupt geometry data)
    if cat_a == "binary" or cat_b == "binary":
        if cat_a == "binary" and cat_b == "binary":
            return pa.large_binary()  # Safest binary type
        return type_a  # Can't promote binary + non-binary safely

    # String wins over numeric (safest fallback for text data)
    if cat_a == "string" or cat_b == "string":
        return pa.string()

    return None


def _promote_int_pair(type_a, type_b):
    """Promote two integer types, guarding against uint64/signed overflow."""
    import pyarrow as pa

    a_unsigned = pa.types.is_unsigned_integer(type_a)
    b_unsigned = pa.types.is_unsigned_integer(type_b)
    # uint64 mixed with signed int → float64 (avoids overflow for large uint64)
    if (a_unsigned and type_a == pa.uint64() and not b_unsigned) or (
        b_unsigned and type_b == pa.uint64() and not a_unsigned
    ):
        return pa.float64()
    return pa.int64()


def _promote_numeric_pair(type_a, type_b, cat_a: str, cat_b: str):
    """Promote bool / int / float / decimal combinations. None if not numeric."""
    import pyarrow as pa

    cats = {cat_a, cat_b}

    # bool + int → int64 (bool can safely cast to int)
    if cats == {"bool", "int"}:
        return pa.int64()

    # bool + float → float64
    if cats == {"bool", "float"}:
        return pa.float64()

    # int + int → check for unsigned overflow risk
    if cat_a == "int" and cat_b == "int":
        return _promote_int_pair(type_a, type_b)

    # Any remaining int/float/decimal mix → float64 (safest common type)
    numeric = {"int", "float", "decimal"}
    if cat_a in numeric and cat_b in numeric:
        return pa.float64()

    return None


def _promote_timestamp_pair(type_a, type_b, cat_a: str, cat_b: str):
    """Promote two timestamps to the finer unit. None if not both timestamps."""
    if cat_a != "timestamp" or cat_b != "timestamp":
        return None

    # ns > us > ms > s — return the finer-grained unit (lower number = finer)
    unit_order = {"ns": 0, "us": 1, "ms": 2, "s": 3}
    if unit_order.get(type_a.unit, 99) <= unit_order.get(type_b.unit, 99):
        return type_a
    return type_b


def _promote_numeric_type(type_a, type_b):
    """
    Compute a common type that both input types can safely cast to.

    Type promotion rules (ordered by preference):
    - Same types → return as-is
    - null + any → any
    - bool + int* → int64
    - int8/16/32 + int64 → int64
    - float32 + float64 → float64
    - int* + float* → float64
    - int* + decimal128 → float64
    - decimal128 + float* → float64
    - timestamp[*] + timestamp[*] → timestamp with finest unit
    - any numeric + string → string (but not binary + string)
    - incompatible types → first type (caller handles cast errors)

    Args:
        type_a: First PyArrow type
        type_b: Second PyArrow type

    Returns:
        PyArrow type that both can safely cast to
    """
    if type_a == type_b:
        return type_a

    cat_a = _type_category(type_a)
    cat_b = _type_category(type_b)

    for handler in (_promote_special, _promote_numeric_pair, _promote_timestamp_pair):
        result = handler(type_a, type_b, cat_a, cat_b)
        if result is not None:
            return result

    # Fallback: return first type (caller handles cast errors with context)
    return type_a


def _compute_unified_schema(schemas: list):
    """
    Compute unified schema from multiple schemas with safe type promotion.

    Used when merging paginated results where different pages may have
    different inferred types for the same field (e.g., int64 vs decimal128).

    Args:
        schemas: List of PyArrow schemas to unify

    Returns:
        Unified PyArrow schema that all input schemas can safely cast to
    """
    import pyarrow as pa

    if not schemas:
        return pa.schema([])

    if len(schemas) == 1:
        return schemas[0]

    # Build field name → list of types mapping
    field_types: dict[str, list] = {}
    field_order: list[str] = []

    for schema in schemas:
        for field in schema:
            if field.name not in field_types:
                field_types[field.name] = []
                field_order.append(field.name)
            field_types[field.name].append(field.type)

    # Compute unified type for each field
    unified_fields = []
    for name in field_order:
        types = field_types[name]
        unified_type = types[0]
        for t in types[1:]:
            unified_type = _promote_numeric_type(unified_type, t)
        unified_fields.append(pa.field(name, unified_type))

    return pa.schema(unified_fields)


def _parse_time_strings(col, target_type):
    """Parse HH:MM:SS[.fff] strings into a time32/time64 array.

    PyArrow has no string → time cast, but GDAL delivers ArcGIS TimeOnly
    values as strings (ESRIJSON attributes, and all-null GeoJSON pages are
    inferred as string), so the conversion must be explicit. Whole-second
    values take the vectorized strptime path; fractional seconds fall back
    to Python's ISO parser, which strptime cannot express.

    Raises ValueError on a value that is not a time of day.
    """
    import datetime

    import pyarrow as pa
    import pyarrow.compute as pc

    parsed = pc.strptime(col, format="%H:%M:%S", unit="ms", error_is_null=True)
    unmatched = pc.and_(pc.is_valid(col), pc.is_null(parsed))
    if pc.any(unmatched).as_py():
        values = []
        for value in col.to_pylist():
            if value is None:
                values.append(None)
                continue
            try:
                values.append(datetime.time.fromisoformat(value))
            except ValueError as e:
                raise ValueError(f"{value!r} is not a time of day: {e}") from e
        return pa.array(values, type=target_type)
    return parsed.cast(target_type)


def _cast_table_to_schema(table, target_schema, *, page_info: str | None = None):
    """
    Cast a table's columns to match target schema types.

    Handles type conversions that PyArrow's concat_tables(promote=True) cannot,
    such as int64 → float64 when another table has decimal128.

    Args:
        table: PyArrow table to cast
        target_schema: Target schema with desired types
        page_info: Optional context string for error messages (e.g., "page 3")

    Returns:
        Table with columns cast to target schema types

    Raises:
        ValueError: If a column cannot be cast to the target type
    """
    import pyarrow as pa

    from geoparquet_io.core.logging_config import warn

    if table.schema.equals(target_schema):
        return table

    new_columns = []
    for field in target_schema:
        if field.name in table.column_names:
            col = table.column(field.name)
            if col.type != field.type:
                # Warn about precision loss for decimal → float64
                if pa.types.is_decimal(col.type) and pa.types.is_floating(field.type):
                    warn(
                        f"Column '{field.name}' cast from {col.type} to {field.type} "
                        f"may lose precision for large values"
                    )
                # PyArrow cannot cast int32 → timestamp directly (it needs int64 as
                # an intermediate) and raises ArrowNotImplementedError. This happens
                # when a source (e.g. DuckDB) infers an epoch-zero/null date column as
                # int32 while the target schema declares timestamp. Upcast to int64
                # first so the timestamp cast below succeeds (issue #516).
                if (
                    pa.types.is_timestamp(field.type)
                    and pa.types.is_integer(col.type)
                    and col.type.bit_width < 64
                ):
                    col = col.cast(pa.int64())
                # Cast to target type with error context. pa.cast() raises siblings
                # ArrowInvalid (bad value) and ArrowNotImplementedError (unsupported
                # conversion); catch both so neither escapes uncontextualized.
                # ValueError also covers the explicit time-string parse, which
                # PyArrow cannot do as a cast.
                try:
                    if pa.types.is_time(field.type) and (
                        pa.types.is_string(col.type) or pa.types.is_large_string(col.type)
                    ):
                        col = _parse_time_strings(col, field.type)
                    else:
                        col = col.cast(field.type, safe=True)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError, ValueError) as e:
                    context = f" ({page_info})" if page_info else ""
                    raise ValueError(
                        f"Failed to cast column '{field.name}' from {col.type} to "
                        f"{field.type}{context}: {e}"
                    ) from e
            new_columns.append(col)
        else:
            # Missing column - create null array
            null_array = pa.nulls(table.num_rows, type=field.type)
            new_columns.append(null_array)

    return pa.table(dict(zip([f.name for f in target_schema], new_columns, strict=True)))


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


def _detect_version_from_table(table, verbose: bool = False) -> str | None:
    """
    Detect GeoParquet version from table's schema metadata.

    Checks the table's schema metadata for existing geo metadata and extracts
    the version. This allows preserving v2.0 or parquet-geo-only formats when
    writing a table that was read from such a source.

    Also checks for native geoarrow extension types which indicate v2.0 or
    parquet-geo-only format.

    Args:
        table: PyArrow Table to check
        verbose: Whether to print verbose output

    Returns:
        Version string (e.g., "1.1", "2.0", "parquet-geo-only") or None if not detected
    """
    import json

    # Check for native geoarrow extension types (indicates v2.0 or parquet-geo-only).
    # `is_geoarrow_extension_field` reads the field, not just the type, so a
    # metadata-declared geoarrow column is detected too -- without that, the
    # same table resolved to "parquet-geo-only" or to the 1.1 default purely
    # according to whether geoarrow.pyarrow had been imported (#792).
    has_native_geo = any(is_geoarrow_extension_field(field) for field in table.schema)

    # Check schema metadata for geo version
    metadata = table.schema.metadata
    if not metadata:
        if has_native_geo:
            # Native geo types but no metadata suggests parquet-geo-only
            if verbose:
                debug("Detected parquet-geo-only format from native geo types")
            return "parquet-geo-only"
        return None

    if b"geo" not in metadata:
        if has_native_geo:
            # Native geo types but no geo metadata = parquet-geo-only
            if verbose:
                debug("Detected parquet-geo-only format (native types, no geo metadata)")
            return "parquet-geo-only"
        return None

    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        if isinstance(geo_meta, dict):
            # Guarded, not truthiness-checked: a non-string version crashed
            # `.split` here on the write path too -- `write_geoparquet_table`
            # resolves auto-mode through this function (#979).
            version = carried_version(geo_meta.get("version"))
            if version:
                parts = version.split(".")
                if len(parts) >= 2:
                    major = parts[0]
                    if major == "2":
                        if verbose:
                            debug("Detected GeoParquet version 2.0 from table metadata")
                        return "2.0"
                    # Upgrade all 1.x versions to 1.1 (backwards compatible)
                    if major == "1":
                        if verbose:
                            debug("Detected GeoParquet version 1.x from table metadata")
                        return "1.1"
        return None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _table_bbox_struct_ok(table, column_name: str) -> bool:
    """True when ``column_name`` is a struct column with the bbox fields."""
    import pyarrow as pa

    try:
        field = table.schema.field(column_name)
    except KeyError:
        return False
    return pa.types.is_struct(field.type) and {"xmin", "ymin", "xmax", "ymax"}.issubset(
        {f.name for f in field.type}
    )


def _detect_bbox_column_from_table(table, verbose: bool = False) -> str | None:
    """
    Detect bbox struct column from Arrow table schema.

    Consults the GeoParquet ``covering.bbox`` metadata first (the authoritative
    pointer, which may use a non-conventional name), then falls back to columns
    with conventional names (bbox, bounds, extent) that have the required
    struct fields (xmin, ymin, xmax, ymax).

    Args:
        table: PyArrow Table to check
        verbose: Whether to print verbose output

    Returns:
        str: Name of bbox column if found, None otherwise
    """
    from geoparquet_io.core.crs_utils import parse_geo_metadata_from_schema

    geo_meta = parse_geo_metadata_from_schema(table.schema.metadata)
    covering_column = _bbox_column_from_covering(geo_meta)
    if covering_column and _table_bbox_struct_ok(table, covering_column):
        if verbose:
            debug(f"Found bbox column from covering metadata: {covering_column}")
        return covering_column

    return detect_bbox_column_from_schema(table.schema, verbose)


# WKB geometry type codes to GeoParquet base names (2D types)
_GEOMETRY_TYPE_CODES = {
    0: "Unknown",
    1: "Point",
    2: "LineString",
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
}

# Dimensional suffixes based on WKB type code modifier
_DIMENSION_SUFFIXES = {
    0: "",  # 2D (no suffix)
    1: " Z",  # Z dimension (codes 1001-1007)
    2: " M",  # M dimension (codes 2001-2007)
    3: " ZM",  # ZM dimensions (codes 3001-3007)
}


def _get_geometry_type_name(code: int) -> str:
    """
    Convert WKB geometry type code to GeoParquet geometry type name.

    Handles 2D types (0-7) and Z/M/ZM variants (1001-1007, 2001-2007, 3001-3007).

    Args:
        code: WKB geometry type code

    Returns:
        GeoParquet geometry type name (e.g., "Point", "Point Z", "Polygon ZM")
    """
    # Extract base type (0-7) and dimensional modifier (0, 1, 2, or 3)
    base_type = code % 1000
    dimension = code // 1000

    base_name = _GEOMETRY_TYPE_CODES.get(base_type, "Unknown")
    if base_name == "Unknown":
        return "Unknown"

    suffix = _DIMENSION_SUFFIXES.get(dimension, "")
    return base_name + suffix


def _strip_geoarrow_to_plain_wkb(table, geometry_column: str, verbose: bool):
    """
    Convert geoarrow extension type back to plain binary WKB.

    Used for GeoParquet 1.x output which uses plain binary geometry
    with CRS only in metadata (not in schema).
    """
    import pyarrow as pa

    geom_col = table.column(geometry_column)

    # Check if it's a WKB extension type, in EITHER carrier shape: the resolved
    # Arrow type, or a plain binary type whose field metadata declares
    # `ARROW:extension:name`. Reading only the type left the metadata-declared
    # shape in place, so the same column got a different carrier depending on
    # whether the process had imported geoarrow.pyarrow (#792).
    #
    # WKB names only, not any geoarrow.* name: this rewrites the column to plain
    # `binary`, which is a lossless re-carrier for WKB bytes and destructive for
    # anything else -- `geoarrow.point` over `struct<x, y>` cannot be cast at all,
    # and `geoarrow.wkt` over `string` casts into a binary column still declared
    # `encoding: WKT`. A native carrier is simply not this function's business.
    if not is_wkb_extension_field(table.schema.field(geometry_column)):
        return table  # Already plain binary, or a non-WKB carrier to leave alone

    if verbose:
        debug("v1.x: stripping geoarrow extension type to plain binary WKB")

    # geoarrow.wkb's storage is `large_binary`, so building a `binary`
    # chunked_array straight from the storage chunks raises -- and the raise used
    # to be swallowed here, silently leaving the extension type in place. That is
    # invisible on the primary column (it reaches the writer as plain
    # `large_binary` via the WKB query wrapper and is narrowed downstream) and
    # was the reason a SECONDARY geometry column kept a native Parquet GEOMETRY
    # type inside a 1.x file (#706). `_to_plain_wkb_array` unwraps the extension
    # and narrows the offsets, and is the same helper the streaming strategy uses.
    from geoparquet_io.core.write_strategies.arrow_streaming import _to_plain_wkb_array

    try:
        plain_col = _to_plain_wkb_array(geom_col)
        col_index = table.schema.get_field_index(geometry_column)
        # Passing the bare name rebuilds the field from scratch, which is what
        # drops a stale `ARROW:extension:name` on the metadata-declared shape --
        # left behind, DuckDB reads it back off `register()` and the COPY writes
        # a native Parquet GEOMETRY type into a 1.x file (#706, #727).
        return table.set_column(col_index, geometry_column, plain_col)

    except (TypeError, ValueError, AttributeError, pa.ArrowInvalid) as e:
        if verbose:
            debug(f"Could not strip geoarrow type: {e}")
        return table


def _crs_as_projjson(crs):
    """Normalize a geoarrow CRS object to a PROJJSON dict, or return it as-is."""
    if crs is None:
        return None
    to_json_dict = getattr(crs, "to_json_dict", None)
    if to_json_dict is None:
        return crs
    try:
        return to_json_dict()
    except (TypeError, ValueError):
        return None


def _process_geometry_column_for_version(
    table,
    geometry_column: str,
    geoparquet_version: str | None,
    input_crs: dict | None,
    verbose: bool,
    *,
    crs_resolved: bool = False,
):
    """
    Process geometry column based on GeoParquet version.

    Handles different GeoParquet versions:
    - v1.x: Plain binary WKB (no extension type), CRS only in metadata
    - v2.0/parquet-geo-only: geoarrow extension type with CRS in schema

    When streaming data enters with geoarrow extension type:
    - v1.x output: strips geoarrow to plain binary WKB
    - v2.0 output: preserves/enhances geoarrow extension type

    Args:
        table: PyArrow Table to modify
        geometry_column: Name of the geometry column
        geoparquet_version: GeoParquet version
        input_crs: PROJJSON dict with CRS
        verbose: Whether to print verbose output
        crs_resolved: True when ``input_crs`` is the geo block's already-resolved
            answer for this column, so ``None`` means "spec default, declared by
            omission" and must not be second-guessed from the Arrow field's type

    Returns:
        pa.Table: Table with geometry column processed
    """
    import geoarrow.pyarrow as ga

    try:
        geom_col = table.column(geometry_column)

        if geoparquet_version in ("2.0", "parquet-geo-only"):
            # For v2.0/parquet-geo-only: use geoarrow extension type with CRS
            wkb_arr = ga.as_wkb(geom_col)

            # Set the CRS explicitly in BOTH directions. Applying it only when
            # non-default left whatever the reader had attached in place, so the
            # same input produced a different schema type depending on whether
            # the process had imported geoarrow.pyarrow — DuckDB hands over a
            # GEOMETRY column carrying OGC:CRS84 when it is registered and a bare
            # one when it is not (#706). A default CRS is the spec default and is
            # declared by the geo block, so the schema type carries none.
            # The field-CRS fallback is only for columns nothing has resolved:
            # there a missing CRS does not mean "no CRS" -- clearing it
            # unconditionally relabelled projected data as the CRS84 default,
            # silent corruption nothing validates. But when `crs_resolved` says
            # the geo block just resolved this column (`apply_output_crs` on the
            # write path), `input_crs=None` IS the answer -- the spec default,
            # declared by omission -- and falling back to the field's CRS would
            # resurrect exactly what the block dropped, e.g. an explicit CRS84
            # `input_crs` over a field carrying EPSG:3857: the type would say
            # 3857 beside a block claiming the default, and gpio's own
            # `v2_crs_consistency_geometry` check fails on the file it wrote.
            #
            # geoarrow hands the field CRS back as a CRS object, which
            # `is_default_crs` does not recognize; normalizing to PROJJSON is
            # what keeps the reader's incidental OGC:CRS84 classified as the
            # default and thus cleared, so the output stays independent of
            # import state (#706).
            resolved_crs = input_crs
            if not crs_resolved:
                resolved_crs = input_crs or _crs_as_projjson(getattr(wkb_arr.type, "crs", None))
            if resolved_crs and not is_default_crs(resolved_crs):
                if verbose:
                    debug(
                        "Applying CRS to geometry schema type: "
                        f"{_format_crs_display(input_crs) if input_crs else resolved_crs}"
                    )
                new_type = wkb_arr.type.with_crs(resolved_crs)
            else:
                new_type = ga.wkb()
            # Always rebuild: geoarrow's WkbType.__eq__ ignores the CRS, so a
            # "types are equal" shortcut would skip precisely the case this is
            # here to normalize.
            wkb_arr = _rebuild_array_with_type(wkb_arr, new_type)

            # Replace geometry column in table
            col_index = table.schema.get_field_index(geometry_column)
            table = table.set_column(col_index, geometry_column, wkb_arr)
        else:
            # For v1.x: ensure plain binary WKB (strip a WKB extension carrier if
            # present). CRS goes only in metadata, not in schema.
            # A non-WKB geoarrow carrier is left alone: casting it here is either
            # impossible (`geoarrow.point` over `struct<x, y>`) or silently wrong
            # (`geoarrow.wkt` over `string`), so the broad geoarrow predicate is
            # the wrong gate for a rewrite (#792).
            if is_wkb_extension_field(table.schema.field(geometry_column)):
                table = _strip_geoarrow_to_plain_wkb(table, geometry_column, verbose)
            elif verbose:
                debug("v1.x: geometry is already plain binary WKB (CRS in metadata only)")

    except (TypeError, ValueError, AttributeError) as e:
        if verbose:
            debug(f"Could not process geometry column: {e}")
        # Continue without conversion - geometry is already WKB

    return table


def _compute_geometry_types(table, geometry_column: str, verbose: bool) -> list[str]:
    """
    Compute geometry types from a geometry column using geoarrow.

    Args:
        table: PyArrow Table containing the geometry column
        geometry_column: Name of the geometry column
        verbose: Whether to print verbose output

    Returns:
        list: List of GeoParquet geometry type names (e.g., ["Point", "Polygon"])
    """
    import geoarrow.pyarrow as ga
    import pyarrow.compute as pc

    # Skip for empty tables (geoarrow crashes on empty arrays)
    if table.num_rows == 0:
        return []

    try:
        geom_col = table.column(geometry_column)

        # Filter out NULL values to avoid geoarrow errors on invalid geometries
        # This handles cases where BigQuery returns NULL or empty geometries
        non_null_mask = pc.is_valid(geom_col)
        if pc.any(non_null_mask).as_py():
            geom_col = pc.filter(geom_col, non_null_mask)
        else:
            # All values are NULL
            return []

        # Skip if no valid geometries remain after filtering
        if len(geom_col) == 0:
            return []

        wkb_arr = ga.as_wkb(geom_col)
        types_struct = ga.unique_geometry_types(wkb_arr)

        # Extract geometry type codes from struct array. geoarrow reports the
        # base type and the dimensions separately, so the two are recombined
        # into a WKB code -- reading `geometry_type` alone dropped the " Z" /
        # " M" / " ZM" the spec makes part of the type name (#892).
        type_codes = geoarrow_wkb_codes(types_struct)

        # Map codes to GeoParquet standard names (avoid duplicates)
        type_names = []
        for code in type_codes:
            name = _get_geometry_type_name(code)
            if name not in type_names:
                type_names.append(name)

        if verbose:
            debug(f"Computed geometry_types from data: {type_names}")
        return type_names

    except Exception as e:
        # Catch all exceptions including geoarrow C++ errors
        # (e.g., "Expected valid geometry type code but found 0")
        if verbose:
            debug(f"Could not compute geometry_types: {e}")
        # Return empty list as fallback (allowed by spec - means any type)
        return []


def _compute_bbox_from_data(table, geometry_column: str, verbose: bool) -> list[float] | None:
    """
    Compute bounding box from geometry column data.

    Args:
        table: PyArrow Table containing the geometry column
        geometry_column: Name of the geometry column
        verbose: Whether to print verbose output

    Returns:
        list: [xmin, ymin, xmax, ymax] or None if computation fails
    """
    import geoarrow.pyarrow as ga
    import pyarrow.compute as pc

    # Skip for empty tables
    if table.num_rows == 0:
        return None

    try:
        geom_col = table.column(geometry_column)

        # Filter out NULL values to avoid geoarrow errors on invalid geometries
        non_null_mask = pc.is_valid(geom_col)
        if pc.any(non_null_mask).as_py():
            geom_col = pc.filter(geom_col, non_null_mask)
        else:
            # All values are NULL
            return None

        # Skip if no valid geometries remain after filtering
        if len(geom_col) == 0:
            return None

        wkb_arr = ga.as_wkb(geom_col)
        box_arr = ga.box(wkb_arr)

        # Combine chunks and get storage (underlying struct array)
        combined = box_arr.combine_chunks()
        storage = combined.storage

        # Extract struct fields and compute min/max
        xmin = pc.min(pc.struct_field(storage, "xmin")).as_py()
        ymin = pc.min(pc.struct_field(storage, "ymin")).as_py()
        xmax = pc.max(pc.struct_field(storage, "xmax")).as_py()
        ymax = pc.max(pc.struct_field(storage, "ymax")).as_py()

        if all(v is not None for v in [xmin, ymin, xmax, ymax]):
            if verbose:
                debug(f"Computed bbox from data: [{xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f}]")
            return [xmin, ymin, xmax, ymax]

    except Exception as e:
        # Catch all exceptions including geoarrow C++ errors
        if verbose:
            debug(f"Could not compute bbox: {e}")

    return None


def _assemble_and_apply_geo_metadata(
    table,
    geo_meta: dict,
    metadata_version: str,
    verbose: bool,
):
    """
    Apply the finished geo metadata to the table's schema.

    The geometry column's ``crs`` is already resolved by the caller, which has to
    do it before this point: the native Parquet GEOMETRY logical types are built
    from what the block declares, so the block is finished first (#848).

    Args:
        table: PyArrow Table to modify
        geo_meta: Geo metadata dict to apply
        metadata_version: GeoParquet metadata version string
        verbose: Whether to print verbose output

    Returns:
        pa.Table: Table with geo metadata applied
    """
    # Apply metadata to table
    existing_metadata = dict(table.schema.metadata) if table.schema.metadata else {}
    new_metadata = {}

    # Copy non-geo metadata from existing
    for k, v in existing_metadata.items():
        key_str = k.decode("utf-8") if isinstance(k, bytes) else k
        if not key_str.startswith("geo"):
            new_metadata[k] = v

    # Add geo metadata
    new_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    table = table.replace_schema_metadata(new_metadata)

    if verbose:
        debug(f"Applied geo metadata with version {metadata_version}")

    return table


# Schema-metadata keys that describe the *input's* schema rather than the
# output's, so they must never ride along into a write.
#
# `geo` would declare the input's GeoParquet version over output the writer has
# just given a different shape; `ARROW:schema` and `pandas` are serialized
# descriptors that pyarrow writes through verbatim rather than regenerating,
# leaving the output naming the input's columns and CRS.
#
# One definition, used by both places that need it -- the parquet-geo-only path
# below (which sees `bytes` keys, straight off a pyarrow schema) and
# write_parquet_with_metadata's preserved-keys loop (which sees them decoded).
# They were separate literals; a key added to one and not the other is a silent
# metadata leak, so the bytes form is derived rather than written out again.
_CARRIED_SCHEMA_METADATA_KEYS = frozenset({"geo", "ARROW:schema", "pandas"})
_CARRIED_SCHEMA_METADATA_KEYS_BYTES = frozenset(
    key.encode("utf-8") for key in _CARRIED_SCHEMA_METADATA_KEYS
)


def _strip_geo_metadata_key(table, verbose: bool = False):
    """Drop the input's ``geo``/``ARROW:schema``/``pandas`` keys, keeping other KV.

    Used for parquet-geo-only output, which carries its geometry typing in the
    Parquet schema and must not also declare a GeoParquet version.

    The exclusion set matches ``write_parquet_with_metadata``'s (see the
    preserved-keys loop in that function): besides ``geo``, a carried
    ``ARROW:schema``/``pandas`` describes the *input's* schema and is written
    through verbatim, leaving the output with a serialized descriptor naming
    columns and a CRS the file does not have.

    Args:
        table: PyArrow Table whose schema metadata to clean
        verbose: Whether to print verbose output

    Returns:
        pa.Table: Table without carried geo/schema-descriptor metadata keys
    """
    existing_metadata = table.schema.metadata
    if not existing_metadata:
        return table

    dropped = [k for k in existing_metadata if k in _CARRIED_SCHEMA_METADATA_KEYS_BYTES]
    if not dropped:
        return table

    new_metadata = {k: v for k, v in existing_metadata.items() if k not in dropped}
    if verbose:
        names = ", ".join(sorted(k.decode("utf-8") for k in dropped))
        debug(f"parquet-geo-only: dropped the input's {names} metadata key(s)")
    return table.replace_schema_metadata(new_metadata)


def _canonicalize_wkb_columns(table, geometry_columns, verbose: bool = False, native_columns=None):
    """Give every 1.x geometry column the canonical plain-``binary`` carrier.

    ``_process_geometry_column_for_version`` handles the case where PyArrow
    resolved the geoarrow extension type. It cannot see the other shape DuckDB
    emits: plain ``large_binary`` carrying a raw ``ARROW:extension:name`` field
    metadata key, which appears when nothing in the process registered
    ``geoarrow.pyarrow``. Left in place, that key rides into the output's
    ``ARROW:schema``, so the same input produced different bytes depending on
    what the process had imported (#688's shape, on a secondary column — #706).

    Reuses the streaming strategy's ``canonicalize_wkb_fields`` so both paths
    agree on what "canonical" means -- including its ``_WKB_EXTENSION_NAMES``
    guard, which is what keeps a native nested carrier (``geoarrow.point`` over
    ``struct<x, y>``) out of a binary cast that PyArrow cannot perform.

    Args:
        table: PyArrow Table to rewrite.
        geometry_columns: Names of the declared geometry columns.
        verbose: Whether to log a skipped column.
        native_columns: Columns to leave exactly as they are, because the target
            version writes them as a native geo type or because the caller
            converts them itself. Passed straight through to
            ``canonicalize_wkb_fields``.
    """
    import pyarrow as pa

    from geoparquet_io.core.write_strategies.arrow_streaming import (
        _to_plain_wkb_array,
        canonicalize_wkb_fields,
    )

    target = canonicalize_wkb_fields(
        table.schema, set(geometry_columns), native_columns=set(native_columns or ())
    )
    # check_metadata=True matters: the leak this exists to stop is a stale
    # ARROW:extension:name on a field whose *type* is already plain binary, and
    # the default comparison ignores metadata entirely.
    if target.equals(table.schema, check_metadata=True):
        return table

    for index, field in enumerate(target):
        current = table.schema.field(index)
        if field.equals(current, check_metadata=True):
            continue
        try:
            table = table.set_column(index, field, _to_plain_wkb_array(table.column(index)))
        except (TypeError, ValueError, pa.ArrowInvalid) as e:
            if verbose:
                debug(f"Could not canonicalize WKB carrier for '{field.name}': {e}")
    return table


def _parse_geo_metadata_quietly(original_metadata) -> dict:
    """Best-effort parse of a carried ``geo`` key; ``{}`` when absent or unreadable."""
    if not original_metadata:
        return {}
    raw = original_metadata.get(b"geo") or original_metadata.get("geo")
    if not raw:
        return {}
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        parsed = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        return {}
    # Callers read ``columns`` as a mapping of column name to a dict of that
    # column's metadata. A carried block is arbitrary JSON from the input file,
    # so anything else is dropped rather than allowed to raise a TypeError three
    # frames away, in the middle of a write. The check lives in one place --
    # ``sanitize_geo_metadata`` -- that every write-path reader goes through
    # (#771); this function is one of them.
    return sanitize_geo_metadata(parsed) or {}


def _apply_geoparquet_metadata(
    table,
    geometry_column: str,
    geoparquet_version: str | None,
    original_metadata: dict | None = None,
    input_crs: dict | None = None,
    custom_metadata: dict | None = None,
    verbose: bool = False,
    edges: str | None = None,
    geometry_info: dict | None = None,
    geo_bbox: list[float] | None = None,
):
    """
    Apply GeoParquet metadata to an Arrow Table based on version.

    Handles different GeoParquet versions:
    - v1.x: Apply geo metadata to schema, CRS via geoarrow type
    - v2.0: Apply CRS to schema type AND geo metadata
    - parquet-geo-only: Apply CRS to schema type only, no geo metadata

    When geoparquet_version is None, the function will detect the version from
    the table's existing schema metadata, preserving v2.0 or parquet-geo-only
    formats when present.

    Args:
        table: PyArrow Table to modify
        geometry_column: Name of the geometry column
        geoparquet_version: GeoParquet version (1.0, 1.1, 2.0, parquet-geo-only),
            or None to auto-detect from existing table metadata
        original_metadata: Original metadata to preserve
        input_crs: PROJJSON dict with CRS
        custom_metadata: Custom metadata (e.g., H3 covering info)
        verbose: Whether to print verbose output
        edges: Edge interpretation, "spherical" or "planar" (default None = planar).
               Use "spherical" for data from BigQuery or other S2-based sources.
        geometry_info: Dict containing multi-geometry column info with keys:
            - "primary": primary geometry column name
            - "secondary": list of secondary geometry column names
            - "metadata": dict mapping column names to their metadata
        geo_bbox: Pre-computed [xmin, ymin, xmax, ymax] for the primary column,
            used instead of the plain min/max taken off the data. A caller that
            knows the data crosses the antimeridian passes the RFC 7946 5.2
            wrap form (xmin > xmax) here, which cannot be recovered from
            per-geometry extents alone.

    Returns:
        pa.Table: Table with GeoParquet metadata applied
    """
    # Auto-detect version from table schema metadata if not specified
    effective_version = geoparquet_version
    if effective_version is None:
        effective_version = _detect_version_from_table(table, verbose)

    version_config = GEOPARQUET_VERSIONS.get(
        effective_version, GEOPARQUET_VERSIONS[DEFAULT_GEOPARQUET_VERSION]
    )
    metadata_version = version_config["metadata_version"]
    should_add_geo_metadata = effective_version != "parquet-geo-only"

    if verbose:
        debug(f"Applying GeoParquet metadata for version: {effective_version or 'default (1.1)'}")

    # Check if geometry column exists in table
    if geometry_column not in table.column_names:
        # An explicit parquet-geo-only request still has to drop a carried geo
        # key, even with nothing to convert: the key names a primary_column the
        # file does not contain (issue #701). Restricted to the explicit request
        # so auto mode (geoparquet_version=None) keeps whatever it resolves to,
        # which is issue #600's territory.
        #
        # The three strategies behind Table.write() had the same-shaped guard
        # and leaked the key (#773); they now route their file-level metadata
        # through parquet_writer.apply_output_kv_metadata, which applies this
        # same rule before any such guard can be reached. This branch stays
        # because write_geoparquet_table does not go through a strategy.
        if geoparquet_version == "parquet-geo-only":
            if verbose:
                debug(
                    f"Geometry column '{geometry_column}' not found in table; "
                    "dropping carried geo metadata for parquet-geo-only"
                )
            return _strip_geo_metadata_key(table, verbose)
        if verbose:
            debug(f"Geometry column '{geometry_column}' not found in table, skipping metadata")
        return table

    from geoparquet_io.core.write_strategies.base import (
        native_geometry_crs,
        resolve_geometry_columns,
    )

    # The table entry points (`write_geoparquet_table`, the strategies'
    # `write_from_table`) get no `geometry_info`, so fall back to naming the
    # secondaries from the carried geo metadata -- otherwise they would silently
    # keep the single-column behaviour the loop below exists to replace.
    # `original_metadata` is deliberately None on the table entry points (passing
    # it there would smuggle the input's stale geo block into the output), so the
    # secondary names come from the table's own carried key instead.
    carried_geo = _parse_geo_metadata_quietly(original_metadata) or _parse_geo_metadata_quietly(
        table.schema.metadata
    )

    # Step 1: Build the geo metadata, BEFORE the geometry columns are retyped.
    #
    # The 2.0 / parquet-geo-only native Parquet GEOMETRY types take each column's
    # CRS from the entry that declares it, so that entry has to exist first --
    # otherwise the primary is typed from `input_crs`, which only a reprojection
    # supplies, and an ordinary write leaves the type bare beside a block still
    # declaring the source's EPSG:3857 (#848).
    #
    # Built for parquet-geo-only too, which drops it again below: there the
    # logical type is the column's only geometry identity, so losing the CRS is
    # not a disagreement but a silent relabelling of projected coordinates.
    geo_meta = _build_geo_block(
        table,
        geometry_column,
        original_metadata,
        input_crs,
        custom_metadata,
        metadata_version,
        edges,
        geometry_info,
        verbose,
    )

    # A column the block does not name falls back to what the input declared for
    # it: on the table entry points the block is built from `original_metadata`,
    # which is None there, while the carrier decision still has to see the
    # secondaries the table's own carried key names.
    declared_crs = dict(carried_geo.get("columns") or {})
    declared_crs.update((geometry_info or {}).get("metadata", {}))
    declared_crs.update(geo_meta.get("columns") or {})
    native_crs = native_geometry_crs(
        effective_version, {"columns": declared_crs}, geometry_column, geometry_info
    )

    # Which columns' CRS the block-building actually RESOLVED, as opposed to
    # merely giving an entry. Only two things count as a source: an explicit
    # `crs` value in a column's entry, and -- for the primary alone, the one
    # column `apply_output_crs` runs on -- a caller-supplied `input_crs`
    # (a requested spec default is dropped from the block, so that entry is
    # crs-less yet still resolved). A crs-less entry produced without
    # consulting either is "unknown", NOT "default": at 2.0 the schema type is
    # authoritative and a block may legitimately omit the key, so the field-CRS
    # fallback in `_process_geometry_column_for_version` must stay live there.
    crs_resolved_columns = {name for name, meta in declared_crs.items() if (meta or {}).get("crs")}
    if input_crs is not None:
        crs_resolved_columns.add(geometry_column)

    # Step 2: Handle geometry columns based on version.
    #
    # EVERY geometry column, not just the primary: validation applies the same
    # per-version requirements to each column in geo["columns"], and a secondary
    # left with whatever carrier the reader produced is both wrong for the target
    # version and dependent on whether anything imported geoarrow.pyarrow (#706).
    # Each column carries its OWN crs -- giving a secondary the primary's fails
    # v2_crs_consistency.
    for column in sorted(resolve_geometry_columns(geometry_column, geometry_info, carried_geo)):
        if column not in table.column_names:
            continue
        table = _process_geometry_column_for_version(
            table,
            column,
            effective_version,
            native_crs.get(column),
            verbose,
            # For a column whose CRS the block-building genuinely resolved, a
            # None from `native_crs` is "default by omission" and the field-CRS
            # fallback must not resurrect what the block dropped.
            crs_resolved=column in crs_resolved_columns,
        )

    if effective_version in ("1.0", "1.1"):
        table = _canonicalize_wkb_columns(
            table, resolve_geometry_columns(geometry_column, geometry_info, carried_geo), verbose
        )

    # Step 3: Apply the geo metadata (unless parquet-geo-only)
    if not should_add_geo_metadata:
        # parquet-geo-only means no GeoParquet metadata at all. Step 2 has just
        # given the column a native Parquet GEOMETRY logical type, so a 'geo'
        # key carried in from the input would declare a version whose spec
        # forbids that type (issue #687). Drop it, keeping unrelated KV
        # metadata, to match the other write paths.
        return _strip_geo_metadata_key(table, verbose)

    # Stats are read off the written data, so they are filled in after step 2.
    col_meta = geo_meta["columns"][geometry_column]
    if "geometry_types" not in col_meta:
        col_meta["geometry_types"] = _compute_geometry_types(table, geometry_column, verbose)

    computed_bbox = geo_bbox or _compute_bbox_from_data(table, geometry_column, verbose)
    if computed_bbox:
        col_meta["bbox"] = computed_bbox

    return _assemble_and_apply_geo_metadata(table, geo_meta, metadata_version, verbose)


def _build_geo_block(
    table,
    geometry_column: str,
    original_metadata: dict | None,
    input_crs: dict | None,
    custom_metadata: dict | None,
    metadata_version: str,
    edges: str | None,
    geometry_info: dict | None,
    verbose: bool,
) -> dict:
    """Build the output's ``geo`` block, with the primary column's CRS resolved.

    Everything here reads the table's *schema* only -- the bbox-covering probe --
    so it can run before the geometry columns are retyped, which is what lets the
    native Parquet GEOMETRY types be built from the CRS this declares (#848).
    The per-column stats that need the data (``geometry_types``, ``bbox``) are
    filled in by the caller afterwards.
    """
    bbox_column = _detect_bbox_column_from_table(table, verbose)
    geo_meta = create_geo_metadata(
        original_metadata,
        geometry_column,
        {"has_bbox_column": bbox_column is not None, "bbox_column_name": bbox_column},
        custom_metadata,
        verbose,
        version=metadata_version,
        edges=edges,
    )

    # Set/clear the geometry column's crs per the GeoParquet null-vs-default rule
    # (shared helper is the single source of truth across all write paths).
    columns = geo_meta.setdefault("columns", {})
    apply_output_crs(columns.setdefault(geometry_column, {}), input_crs)
    if verbose and input_crs and not is_default_crs(input_crs):
        debug(f"Added CRS to geo metadata: {_format_crs_display(input_crs)}")

    # Secondary entries are merged in here rather than after the stats pass, so
    # each one's own `crs` is available to the native-type decision.
    from geoparquet_io.core.write_strategies.base import merge_secondary_geometry_metadata

    merge_secondary_geometry_metadata(geo_meta, geometry_info)
    return geo_meta


def _estimate_row_size(table) -> int:
    """
    Estimate bytes per row from PyArrow table memory usage.

    Uses table.get_total_buffer_size() if available (PyArrow >= 0.17),
    falls back to table.nbytes, and uses a default of 100 bytes if
    neither is available or returns 0.

    Args:
        table: PyArrow Table

    Returns:
        int: Estimated bytes per row (minimum 1)
    """
    default_row_size = 100
    num_rows = max(1, table.num_rows)

    # Try get_total_buffer_size() first (more accurate, includes all buffers)
    if hasattr(table, "get_total_buffer_size"):
        try:
            total_bytes = table.get_total_buffer_size()
            if total_bytes > 0:
                return max(1, total_bytes // num_rows)
        except Exception:
            pass

    # Fall back to nbytes property
    if hasattr(table, "nbytes"):
        try:
            total_bytes = table.nbytes
            if total_bytes > 0:
                return max(1, total_bytes // num_rows)
        except Exception:
            pass

    return default_row_size


def _write_table_with_settings(
    table,
    output_path: str,
    compression: str,
    compression_level: int | None,
    row_group_rows: int | None,
    row_group_size_mb: int | None,
    geoparquet_version: str | None,
    geometry_column: str,
    verbose: bool = False,
) -> None:
    """
    Write Arrow table to Parquet with proper settings.

    Uses pq.write_table directly since we've already applied all GeoParquet
    metadata to the table. This preserves the metadata we set (including version,
    geometry_types, CRS, etc.) without geoarrow overwriting it.

    Args:
        table: PyArrow Table to write
        output_path: Output file path
        compression: Compression type (ZSTD, GZIP, etc.)
        compression_level: Compression level
        row_group_rows: Exact number of rows per row group
        row_group_size_mb: Target row group size in MB
        geoparquet_version: GeoParquet version
        geometry_column: Name of the geometry column
        verbose: Whether to print verbose output
    """
    # Calculate row group size
    rows_per_group = row_group_rows
    if not rows_per_group and row_group_size_mb and table.num_rows > 0:
        # Estimate bytes per row from actual table memory usage
        estimated_row_size = _estimate_row_size(table)
        target_bytes = row_group_size_mb * 1024 * 1024
        rows_per_group = max(1, int(target_bytes // estimated_row_size))
        rows_per_group = min(rows_per_group, table.num_rows)

    # Use central configuration for write settings
    settings = ParquetWriteSettings(
        compression=compression,
        compression_level=compression_level,
        row_group_rows=row_group_rows,
        row_group_size_mb=row_group_size_mb,
    )
    write_kwargs = settings.get_pyarrow_kwargs(calculated_row_group_size=rows_per_group)

    if verbose:
        compression_desc = (
            f"{compression}:{compression_level}" if compression_level else compression
        )
        debug(f"Writing with {compression_desc} compression")
        if rows_per_group:
            debug(f"Row group size: {rows_per_group:,} rows")

    # Use pq.write_table for all versions - we've already applied all metadata
    # Using geoarrow's write_geoparquet_table would overwrite our carefully constructed metadata
    pq.write_table(table, output_path, **write_kwargs)

    if verbose:
        success(f"Wrote {table.num_rows:,} rows to {output_path}")


def _normalize_arrow_large_types(table):
    """
    Convert large Arrow types to standard types for Parquet compatibility.

    DuckDB with arrow_large_buffer_size=true exports strings as large_string
    (LargeUtf8) and binaries as large_binary (LargeBinary). While this allows
    handling >2GB buffers in memory, it causes compatibility issues when:
    - Reading Hive-partitioned datasets (partition columns inferred as string)
    - Merging schemas from different sources

    This function casts large types back to standard types before writing to
    Parquet. The Parquet format itself handles large values fine - the 2GB
    limit is only for Arrow's in-memory representation.

    Args:
        table: PyArrow table potentially containing large types

    Returns:
        Table with large_string → string and large_binary → binary
    """
    import pyarrow as pa

    new_fields = []
    needs_cast = False

    for field in table.schema:
        if pa.types.is_large_string(field.type):
            new_fields.append(pa.field(field.name, pa.string(), field.nullable, field.metadata))
            needs_cast = True
        elif pa.types.is_large_binary(field.type):
            new_fields.append(pa.field(field.name, pa.binary(), field.nullable, field.metadata))
            needs_cast = True
        else:
            new_fields.append(field)

    if not needs_cast:
        return table

    new_schema = pa.schema(new_fields, metadata=table.schema.metadata)
    return table.cast(new_schema)


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


_GEO_TYPE_CODE_BASES = {
    1: "Point",
    2: "LineString",
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
}
_GEO_TYPE_CODE_SUFFIXES = {0: "", 1: " Z", 2: " M", 3: " ZM"}


def _geo_code_to_type_name(code: int) -> str | None:
    """Map a Parquet geospatial type code (e.g. 3002) to 'LineString ZM'."""
    dim, base = divmod(code, 1000)
    name = _GEO_TYPE_CODE_BASES.get(base)
    suffix = _GEO_TYPE_CODE_SUFFIXES.get(dim)
    if name is None or suffix is None:
        return None
    return name + suffix


def _geography_edges_from_logical(logical: str) -> str | None:
    """Edges value implied by a native Geography logical type string.

    Returns the declared algorithm (e.g. "spherical", "vincenty"), defaulting
    to "spherical" (the Parquet spec default) when none is spelled out.
    Returns None for non-Geography logical types.
    """
    if not logical.startswith("Geography"):
        return None
    match = re.search(r"algorithm=([A-Za-z_]+)", logical)
    return match.group(1).lower() if match else "spherical"


def _crs_from_geo_logical(logical: str, parquet_file: str) -> tuple[bool, Any]:
    """The CRS a native Geometry/Geography logical type declares.

    Returns ``(present, crs)``. ``present`` is False when the type names no CRS
    or names the OGC:CRS84 / EPSG:4326 default, both of which GeoParquet spells
    by *omitting* the key. Otherwise ``crs`` is the resolved PROJJSON dict, or
    ``None`` for a reference this build cannot resolve -- an explicit
    ``"crs": null`` (CRS unknown), which is the honest reading and never the
    silent CRS84 claim that omitting it would make (#785).
    """
    from geoparquet_io.core.duckdb_metadata import (
        parse_geometry_logical_type,
        resolve_crs_reference,
    )

    parsed = parse_geometry_logical_type(logical) or {}
    raw = parsed.get("crs")
    if raw is None:
        return False, None
    crs = resolve_crs_reference(parquet_file, raw)
    if isinstance(crs, dict):
        return (False, None) if is_default_crs(crs) else (True, crs)
    warn(
        f"Native geometry type declares a CRS this build cannot resolve to PROJJSON: {crs!r}. "
        "Writing an explicit null CRS (unknown) rather than claiming the OGC:CRS84 default. "
        + NULL_CRS_HINT
    )
    return True, None


def _geo_col_meta_from_stats(pf, col_index: int, logical: str, parquet_file: str) -> dict:
    """Build one geo column metadata dict from a column's native geo statistics."""
    codes: set[int] = set()
    bbox = None
    zrange = None
    for rg in range(pf.metadata.num_row_groups):
        stats = pf.metadata.row_group(rg).column(col_index).geo_statistics
        if stats is None:
            continue
        codes.update(stats.geospatial_types or [])
        if stats.xmin is not None:
            # Limitation: min/max-merging row-group extents assumes planar
            # edges; geography data crossing the antimeridian can yield an
            # over-wide (though never under-covering) bbox here.
            if bbox is None:
                bbox = [stats.xmin, stats.ymin, stats.xmax, stats.ymax]
            else:
                bbox = [
                    min(bbox[0], stats.xmin),
                    min(bbox[1], stats.ymin),
                    max(bbox[2], stats.xmax),
                    max(bbox[3], stats.ymax),
                ]
        if stats.zmin is not None:
            if zrange is None:
                zrange = [stats.zmin, stats.zmax]
            else:
                zrange = [min(zrange[0], stats.zmin), max(zrange[1], stats.zmax)]

    geometry_types = sorted(t for t in (_geo_code_to_type_name(c) for c in codes) if t is not None)
    col_meta: dict = {"encoding": "WKB", "geometry_types": geometry_types}
    if bbox is not None:
        # RFC 7946 order; 6 values when a Z range exists (M is never
        # part of bbox per spec).
        if zrange is not None:
            col_meta["bbox"] = [bbox[0], bbox[1], zrange[0], bbox[2], bbox[3], zrange[1]]
        else:
            col_meta["bbox"] = bbox
    # The rebuilt block is the file's only geo metadata, so a CRS left behind
    # here is not merely missing: an absent `crs` *means* OGC:CRS84, which
    # relabels projected coordinates as lon/lat while the Parquet logical type
    # still names the real CRS (#785). Mirror the logical type, which is the
    # authority at 2.0.
    crs_present, crs = _crs_from_geo_logical(logical, parquet_file)
    if crs_present:
        col_meta["crs"] = crs
    # A Geography logical type carries edge semantics the geo metadata must
    # not drop: synthesize the matching edges declaration (#588).
    edges = _geography_edges_from_logical(logical)
    if edges:
        col_meta["edges"] = edges
    return col_meta


def _ensure_v2_geo_metadata(
    output_path: str,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
    verbose: bool = False,
    primary_column: str | None = None,
) -> None:
    """Attach GeoParquet 2.0 geo metadata if the writer omitted it (#589).

    DuckDB 1.5.4's V2 writer skips the geo KV metadata for M/ZM geometries.
    Rebuild it from the file's native geospatial statistics and logical types,
    mirroring the shape DuckDB writes for XY/XYZ data, and rewrite the file in
    place. ``primary_column`` names the real geometry column for multi-geometry
    files; without it the fallback is "geometry", then alphabetical.
    """
    pf = pq.ParquetFile(output_path)
    try:
        kv = pf.metadata.metadata or {}
        if b"geo" in kv:
            return
        schema = pf.metadata.schema
        geo_cols: dict[int, tuple[str, str]] = {}
        for i in range(len(schema)):
            logical = str(schema.column(i).logical_type)
            if logical.startswith(("Geometry", "Geography")):
                geo_cols[i] = (schema.column(i).name, logical)
        if not geo_cols:
            return

        columns = {
            name: _geo_col_meta_from_stats(pf, i, logical, output_path)
            for i, (name, logical) in geo_cols.items()
        }

        if primary_column and primary_column in columns:
            primary = primary_column
        elif "geometry" in columns:
            primary = "geometry"
        else:
            primary = sorted(columns)[0]
        geo_meta = {"version": "2.0.0", "primary_column": primary, "columns": columns}
    finally:
        # Release the read handle before rewriting (Windows requires it).
        pf.close()

    _rewrite_file_with_geo_metadata(
        output_path, geo_meta, compression, compression_level, row_group_rows
    )
    if verbose:
        debug("Re-attached geo metadata (writer omitted it for M/ZM geometries)")


def _infer_row_group_size(output_path: str) -> int | None:
    """Max rows per existing row group, so a rewrite can mirror the file's layout."""
    pf = pq.ParquetFile(output_path)
    try:
        num_groups = pf.metadata.num_row_groups
        if num_groups == 0:
            return None
        return max(pf.metadata.row_group(i).num_rows for i in range(num_groups))
    finally:
        # Release the read handle before any rewrite (Windows requires it).
        pf.close()


def _rewrite_file_with_geo_metadata(
    output_path: str,
    geo_meta: dict,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
) -> None:
    """Rewrite a parquet file in place with the given geo metadata attached."""
    # geoarrow registration makes pyarrow round-trip the native GEOMETRY/
    # GEOGRAPHY logical types (and their CRS) instead of demoting to binary.
    import geoarrow.pyarrow  # noqa: F401

    # Preserve the file's own row-group layout when the caller didn't specify
    # one — pyarrow's ~1Mi-row default would otherwise collapse the groups.
    if not row_group_rows:
        row_group_rows = _infer_row_group_size(output_path)

    table = pq.read_table(output_path)
    new_meta = dict(table.schema.metadata or {})
    new_meta[b"geo"] = json.dumps(geo_meta).encode()
    table = table.replace_schema_metadata(new_meta)

    # Keys cover every normalized name callers can pass (DuckDB COPY names
    # from _plain_copy_to's compression_map plus pyarrow-style variants).
    codec_map = {
        "ZSTD": "zstd",
        "GZIP": "gzip",
        "BROTLI": "brotli",
        "SNAPPY": "snappy",
        "UNCOMPRESSED": "none",
        "NONE": "none",
        "LZ4": "lz4",
        "LZ4_RAW": "lz4",
    }
    write_kwargs: dict = {"compression": codec_map.get(compression.upper(), "zstd")}
    if compression_level is not None and write_kwargs["compression"] in ("zstd", "gzip", "brotli"):
        write_kwargs["compression_level"] = compression_level
    if row_group_rows:
        write_kwargs["row_group_size"] = row_group_rows

    tmp_path = f"{output_path}.geometa.tmp"
    try:
        pq.write_table(table, tmp_path, **write_kwargs)
        os.replace(tmp_path, output_path)
    finally:
        # os.replace consumes the tmp file on success; clean it up on failure.
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def collect_nonplanar_edges(input_file: str) -> dict[str, str]:
    """Map geometry column -> non-planar edges value declared by the input.

    Sources: the native GEOGRAPHY logical type's algorithm, and any non-planar
    ``edges`` in the input's geo column metadata.
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_geo_metadata,
        get_schema_info,
        parse_geometry_logical_type,
    )

    edges_by_col: dict[str, str] = {}
    try:
        for col in get_schema_info(input_file):
            logical = col.get("logical_type") or ""
            if "GeographyType" in logical:
                parsed = parse_geometry_logical_type(logical) or {}
                edges_by_col[col["name"]] = parsed.get("algorithm") or "spherical"
        geo_meta = get_geo_metadata(input_file) or {}
        for name, col_meta in (geo_meta.get("columns") or {}).items():
            edges = col_meta.get("edges") if isinstance(col_meta, dict) else None
            if edges and edges != "planar":
                edges_by_col.setdefault(name, edges)
    except Exception as e:
        debug(f"Could not collect edges metadata from {input_file}: {e}")
        return {}
    return edges_by_col


def _edges_for_output_version(edges: str, geo_version: str) -> str:
    """Map an edges value to one representable in the output's spec version.

    GeoParquet 1.x only allows {"planar", "spherical"}; ellipsoidal algorithms
    (vincenty/karney/andoyer/thomas) from 2.0/native inputs degrade to
    "spherical" with a warning. 2.0 outputs keep the algorithm verbatim.
    """
    if not str(geo_version).startswith("1."):
        return edges
    if edges in ("planar", "spherical"):
        return edges
    warn(f"edges algorithm '{edges}' is not representable in GeoParquet 1.x; writing 'spherical'")
    return "spherical"


def _collect_nonplanar_edges_from_metadata(original_metadata: dict | None) -> dict[str, str]:
    """Map geometry column -> non-planar edges declared in an input KV metadata dict.

    Fallback for write paths that carry the input's KV metadata but not its
    path (the native GEOGRAPHY logical-type half needs the file itself).
    """
    if not original_metadata:
        return {}
    geo_data = original_metadata.get("geo") or original_metadata.get(b"geo")
    if not geo_data:
        return {}
    try:
        if isinstance(geo_data, bytes):
            geo_data = geo_data.decode("utf-8")
        geo_meta = json.loads(geo_data) if isinstance(geo_data, str) else geo_data
        edges_by_col: dict[str, str] = {}
        for name, col_meta in (geo_meta.get("columns") or {}).items():
            edges = col_meta.get("edges") if isinstance(col_meta, dict) else None
            if edges and edges != "planar":
                edges_by_col[name] = edges
        return edges_by_col
    except Exception:
        return {}


def _apply_nonplanar_edges(
    edges_by_col: dict[str, str],
    output_file: str,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
    verbose: bool = False,
) -> None:
    """Re-attach non-planar edges declarations to an output's geo metadata (#588).

    DuckDB has no GEOGRAPHY type, so rewrites demote native GEOGRAPHY columns
    to GEOMETRY and drop ``edges`` from regenerated metadata. Losing the edge
    interpretation silently corrupts semantics (great-circle edges read as
    planar), so re-attach it to the output's geo metadata.
    """
    if not edges_by_col:
        return

    pf = pq.ParquetFile(output_file)
    try:
        kv = pf.metadata.metadata or {}
        if b"geo" not in kv:
            warn(
                "Input declares non-planar edges but output has no geo metadata "
                f"to carry them ({', '.join(sorted(edges_by_col))}). Edge "
                "interpretation is lost; avoid parquet-geo-only for geography data."
            )
            return
        geo_meta = json.loads(kv[b"geo"].decode("utf-8"))
    finally:
        # Release the read handle before rewriting (Windows requires it).
        pf.close()

    geo_version = str(geo_meta.get("version") or "")
    changed = False
    for name, edges in edges_by_col.items():
        edges = _edges_for_output_version(edges, geo_version)
        col_meta = geo_meta.get("columns", {}).get(name)
        if isinstance(col_meta, dict) and col_meta.get("edges") != edges:
            col_meta["edges"] = edges
            changed = True

    if not changed:
        return

    _rewrite_file_with_geo_metadata(
        output_file, geo_meta, compression, compression_level, row_group_rows
    )
    # Neutral wording: the input may have declared edges via metadata only,
    # without ever having a native GEOGRAPHY logical type.
    warn(
        "Preserved non-planar edges declaration in geo metadata for: "
        f"{', '.join(sorted(edges_by_col))}. Note: DuckDB has no geography "
        "type, so any native GEOGRAPHY input is rewritten as GEOMETRY (the "
        "edges metadata carries the interpretation)."
    )


def _plain_copy_to(
    con,
    query: str,
    output_path: str,
    compression: str = "ZSTD",
    verbose: bool = False,
    geoparquet_version: str = "1.1",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
    input_crs: dict | None = None,
    geometry_column: str | None = None,
    carry_geo_metadata: dict | None = None,
    extra_kv_metadata: dict[str, str] | None = None,
) -> None:
    """
    Execute a plain DuckDB COPY TO without geo metadata manipulation.

    Used when no metadata rewrite is needed (parquet-geo-only, 2.0 passthrough).
    This is the fastest possible write path.

    DuckDB 1.5+: If input_crs is non-default, wraps the query with ST_SetCRS()
    so CRS is written into the Parquet schema natively during COPY TO.

    Args:
        con: DuckDB connection
        query: SQL query to execute
        output_path: Path to output file
        compression: Compression type
        verbose: Whether to print verbose output
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        compression_level: Compression level (codec-specific)
        row_group_rows: Target number of rows per row group
        input_crs: PROJJSON dict with CRS information (optional)
        geometry_column: Name of the geometry column (required if input_crs is set)
        carry_geo_metadata: A `geo` block to write verbatim instead of the one
            DuckDB would generate. Used to preserve a `covering` the input
            declared, which DuckDB's own generated metadata does not include.
            Supplying it alongside GEOPARQUET_VERSION 'V2' keeps the native
            GEOMETRY logical type and its geospatial statistics — only the KV
            entry is replaced.
        extra_kv_metadata: Non-geo sidecar keys (fiboa, vecorel, STAC fragments)
            to carry into the output. DuckDB accepts KV_METADATA alongside
            GEOPARQUET_VERSION, so these ride along on the fast path instead of
            forcing a full metadata rewrite for an unrelated key (#709).
    """
    compression_map = {
        "zstd": "ZSTD",
        "gzip": "GZIP",
        "snappy": "SNAPPY",
        "lz4": "LZ4",
        "none": "UNCOMPRESSED",
        "uncompressed": "UNCOMPRESSED",
        "brotli": "BROTLI",
    }
    duckdb_compression = compression_map.get(compression.lower(), "ZSTD")

    # Map version to DuckDB GEOPARQUET_VERSION parameter
    version_config = GEOPARQUET_VERSIONS.get(geoparquet_version, GEOPARQUET_VERSIONS["1.1"])
    duckdb_version = version_config.get("duckdb_param", "V1")

    # DuckDB 1.5+: Apply CRS via ST_SetCRS so it's written natively into the
    # Parquet schema during COPY TO — no post-processing file rewrite needed.
    final_query = _wrap_query_with_crs(query, geometry_column, input_crs)

    # Build options list
    options = [
        "FORMAT PARQUET",
        f"COMPRESSION {duckdb_compression}",
        f"GEOPARQUET_VERSION '{duckdb_version}'",
    ]
    # DuckDB accepts COMPRESSION_LEVEL for ZSTD only; pairing it with GZIP or
    # BROTLI is a binder error, not a silently ignored option.
    if compression_level is not None and duckdb_compression.upper() == "ZSTD":
        options.append(f"COMPRESSION_LEVEL {validate_compression_level(compression_level)}")
    if row_group_rows is not None:
        options.append(f"ROW_GROUP_SIZE {row_group_rows}")
    kv_pairs = dict(extra_kv_metadata or {})
    if carry_geo_metadata is not None:
        # Applied last, so the carried block wins over an explicit `geo` in
        # `extra_kv_metadata` -- a deliberate flip of the previous precedence.
        # Preserved keys never collide here (`extract_preserved_kv_metadata`
        # excludes `geo`); only a caller that hand-passes `geo` is affected, and
        # `carry_geo_metadata` is the block this write decided to emit.
        kv_pairs["geo"] = json.dumps(carry_geo_metadata)
    kv_clause = build_kv_metadata_clause(kv_pairs)
    if kv_clause:
        options.append(kv_clause)

    copy_query = f"""
        COPY ({final_query})
        TO {sql_path(output_path)}
        ({", ".join(options)})
    """

    if verbose:
        debug(f"Executing plain COPY TO with {duckdb_compression} compression...")

    con.execute(copy_query)

    # DuckDB 1.5.4's V2 writer omits the geo KV metadata for geometries with
    # an M dimension (XY/XYZ are written correctly). Without it the output
    # silently degrades to parquet-geo-only, so repair it from the file's own
    # native geospatial statistics (#589).
    if duckdb_version == "V2":
        _ensure_v2_geo_metadata(
            output_path,
            compression=duckdb_compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            verbose=verbose,
            primary_column=geometry_column,
        )

    if verbose:
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(output_path)
        success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")


def _prune_metadata_to_output_columns(
    con,
    query: str,
    original_metadata: dict | None,
    geometry_column: str | None,
    verbose: bool,
) -> tuple[dict | None, list[str] | None]:
    """Drop geo metadata that references columns the output query does not emit.

    Best-effort: a schema probe that fails leaves the metadata untouched rather
    than aborting the write.

    Returns the pruned metadata and the output's column names, so a later step
    that only needs to know which columns exist does not probe the query again.
    ``None`` column names mean the probe failed, not that there are no columns.
    """
    if not original_metadata:
        return original_metadata, None

    try:
        output_columns = _get_query_columns(con, query)
    except (duckdb.Error, RuntimeError, ValueError, AttributeError) as e:
        if verbose:
            debug(f"Could not read output schema to prune geo metadata: {e}")
        return original_metadata, None

    pruned = prune_geo_metadata_to_columns(original_metadata, output_columns)
    had_geo = "geo" in original_metadata or b"geo" in original_metadata
    if had_geo and geometry_column is None:
        warn(
            "Output has no geometry column - writing plain Parquet without geo metadata "
            f"(columns: {', '.join(output_columns)})"
        )
    return pruned, output_columns


def extract_preserved_kv_metadata(metadata: dict | None) -> dict[str, str]:
    """Return an input's non-geo file-level KV metadata as ``{str: str}``.

    Sidecar payloads (``fiboa``, ``vecorel``, STAC fragments, collection
    records) live in Parquet's file-level key/value metadata next to ``geo``.
    Every write path that rebuilds the KV block must carry them over, so this
    is the single definition of which keys survive a rewrite (#690).

    Keys and values that are not valid UTF-8 are skipped: the write paths pass
    KV metadata as text (DuckDB ``KV_METADATA`` takes SQL string literals), so a
    binary payload cannot be round-tripped and dropping it beats failing the
    write.

    Args:
        metadata: KV metadata dict from a Parquet file or Arrow schema. Keys
            and values may be ``bytes`` or ``str``; ``None`` is accepted.

    Returns:
        Decoded ``{key: value}`` for every key outside
        ``_CARRIED_SCHEMA_METADATA_KEYS``.
    """
    if not metadata:
        return {}

    preserved: dict[str, str] = {}
    for key, value in metadata.items():
        # The key is decoded inside the try as well: Parquet KV keys are
        # arbitrary bytes, and a non-UTF-8 key must skip like a non-UTF-8
        # value rather than raise out of a metadata-preservation helper.
        try:
            key_str = key.decode("utf-8") if isinstance(key, bytes) else key
            if key_str in _CARRIED_SCHEMA_METADATA_KEYS:
                continue
            val_str = value.decode("utf-8") if isinstance(value, bytes) else value
        except UnicodeDecodeError:
            debug(f"Skipping non-UTF-8 KV metadata entry {key!r} (cannot be preserved as text)")
            continue
        preserved[key_str] = val_str
    return preserved


def read_preserved_kv_metadata(parquet_file: str, verbose: bool = False) -> dict[str, str]:
    """Read a Parquet input's preservable non-geo KV metadata.

    Returns an empty dict when the file cannot be inspected (e.g. a remote
    input without credentials): losing sidecar keys is bad, but failing an
    otherwise valid conversion because of them would be worse.
    """
    from geoparquet_io.core.duckdb_metadata import get_kv_metadata

    try:
        # Both the read and the decode are guarded: a malformed KV block is an
        # input-data problem, and losing sidecar keys must never be the reason
        # an otherwise valid write fails.
        preserved = extract_preserved_kv_metadata(get_kv_metadata(parquet_file))
    except Exception as e:  # noqa: BLE001 - any read failure degrades to "nothing to preserve"
        warn(f"Could not read input KV metadata from {parquet_file}: {e}")
        return {}

    if verbose and preserved:
        debug(f"Preserving input KV metadata keys: {sorted(preserved)}")
    return preserved


# Fields a carried `geo` block must still have for the 2.0 fast path to be able
# to write it verbatim. When a caller invalidated the derived stats (reproject,
# a row filter, a multi-file merge) they are stripped, and only the rewrite path
# can recompute them — so we fall back to it rather than ship a thinner block
# than DuckDB would have generated.
_REQUIRED_CARRIED_GEO_FIELDS = ("encoding", "geometry_types")

# Column keys DuckDB's own `GEOPARQUET_VERSION 'V2'` block already writes. A
# carried block is only worth substituting when it says something more than
# these; otherwise DuckDB's freshly generated block is the better one, because
# it describes the data actually written rather than the input.
_DUCKDB_GENERATED_COLUMN_FIELDS = frozenset({"encoding", "geometry_types", "bbox"})


def _carries_more_than_duckdb_generates(geo_dict: dict) -> bool:
    """Whether any column in ``geo_dict`` holds a key DuckDB would not write itself."""
    return any(
        isinstance(col_meta, dict) and not set(col_meta) <= _DUCKDB_GENERATED_COLUMN_FIELDS
        for col_meta in (geo_dict.get("columns") or {}).values()
    )


def _geo_block_to_carry_on_fast_path(
    original_metadata: dict | None,
    geometry_column: str | None,
    effective_version: str,
    con=None,
    query: str | None = None,
    verbose: bool = False,
    input_crs=None,
    input_file: str | None = None,
    output_columns: list[str] | None = None,
) -> dict | None:
    """The `geo` block the 2.0 fast path must write instead of DuckDB's generated one.

    DuckDB regenerates the `geo` key on the fast path, and its generated block
    carries only `version`, `primary_column`, `encoding`, `geometry_types` and
    `bbox`. Everything else the input declared — a `covering` (#738), `epoch`,
    `orientation` (#772) — is silently dropped. Returning the carried block here
    lets `_plain_copy_to` write it verbatim, keeping the fast path's write
    configuration intact: forcing the rewrite instead would clamp threads and
    memory, drop `--compression-level`, and can add a full stats rescan.

    The carried block goes through `apply_output_crs`, the single source of truth
    for the null-vs-default CRS rule, so the fast path cannot write a `crs: null`
    or an explicit CRS84 that the rewrite path would have stripped.

    With `con` and `query`, a conventional `bbox` column the output still carries
    is also declared as a covering, exactly as the rewrite path does. That is the
    only covering gpio invents: `covering` asserts a relationship between a bbox
    column and the geometry that cannot be verified from a column name, so
    `declare_carried_bbox_column` recognises the single universal convention and
    checks the struct's fields before declaring anything. It runs *before* the
    "says more than DuckDB would" gate below, because for a plain 2.0 input with
    an undeclared bbox column the derived covering is the whole difference
    between the two paths (`tests/test_covering_v2.py::
    test_a_conventional_bbox_column_is_declared_at_v2`). `output_columns` lets it
    skip its schema probe when the output has no bbox column to declare at all.

    Returns None when the version is not 2.0 (1.x already rewrites), when the
    input resolves to more than one file (see below), when the carried block is
    too thin to stand in for DuckDB's (a caller invalidated the derived stats and
    only the rewrite path can recompute them), or when it says nothing DuckDB
    would not write itself — the caller then keeps its existing behaviour.
    """
    from geoparquet_io.core.crs_utils import apply_output_crs
    from geoparquet_io.core.geo_metadata import declare_carried_bbox_column

    if effective_version != "2.0" or not geometry_column or not original_metadata:
        return None

    # A glob/directory input merges several files, but `original_metadata` was
    # read from the FIRST file's footer only. Carrying its bbox/geometry_types
    # as the merged output's stats UNDER-covers the result, which makes
    # conformant readers skip data; only the rewrite path can recompute them
    # over everything written. Same test `extract` uses for the same reason.
    if input_file and is_partition_path(input_file):
        if verbose:
            debug(
                "Not carrying the input's geo block: a multi-file input's merged "
                "stats cannot come from the first file's footer"
            )
        return None

    # A write-path reader in the strongest sense: whatever comes back is written
    # to the output file verbatim, so the block goes through the shared shape
    # check first. `columns` as a list or a string used to abort the write with
    # `'list' object has no attribute 'get'` on the next line (#947).
    geo_dict = sanitized_carried_geo(original_metadata)
    if not geo_dict:
        return None
    col_meta = (geo_dict.get("columns") or {}).get(geometry_column)
    if not isinstance(col_meta, dict):
        return None
    if any(field not in col_meta for field in _REQUIRED_CARRIED_GEO_FIELDS):
        return None
    carried = copy.deepcopy(geo_dict)
    carried["version"] = "2.0.0"
    # Before the gate: a block whose only extra key was a default or null
    # `crs` says nothing DuckDB would not write once that key is stripped.
    apply_output_crs(carried["columns"][geometry_column], input_crs)
    if con is not None and query is not None:
        declare_carried_bbox_column(
            con,
            query,
            carried["columns"][geometry_column],
            verbose,
            effective_version,
            output_columns=output_columns,
        )
    if not _carries_more_than_duckdb_generates(carried):
        return None
    return carried


def write_parquet_with_metadata(
    con,
    query,
    output_file,
    original_metadata=None,
    compression="ZSTD",
    compression_level=15,
    row_group_size_mb=None,
    row_group_rows=None,
    custom_metadata=None,
    verbose=False,
    show_sql=False,
    profile=None,
    geoparquet_version=None,
    input_crs=None,
    write_strategy: str = "duckdb-kv",
    memory_limit: str | None = None,
    geometry_info: dict | None = None,
    extra_kv_metadata: dict[str, str] | None = None,
    input_file: str | None = None,
    invalidate_derived_stats: bool = False,
    invalidate_derived_stats_columns: Collection[str] | None = None,
    drop_nonplanar_edges_columns: Collection[str] | None = None,
):
    """
    Write a parquet file with proper compression and metadata handling.

    Supports multiple write strategies with different memory and performance
    characteristics. The default "duckdb-kv" strategy uses DuckDB's native
    KV_METADATA for fast streaming writes.

    Supports both local and remote outputs (S3, GCS, Azure). Remote outputs
    are written to a temporary local file, then uploaded.

    Args:
        con: DuckDB connection
        query: SQL query to execute
        output_file: Path to output file (local path or remote URL)
        original_metadata: Original metadata from source file
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        custom_metadata: Optional dict with custom metadata (e.g., H3 info)
        verbose: Whether to print verbose output
        show_sql: Whether to print SQL statements before execution
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        input_crs: PROJJSON dict with CRS from input file
        write_strategy: Write strategy to use. Options:
            - "duckdb-kv" (default): Use DuckDB COPY TO with KV_METADATA
            - "in-memory": Load entire dataset into memory
            - "streaming": Stream Arrow RecordBatches
            - "disk-rewrite": Write with DuckDB, then rewrite with PyArrow
        memory_limit: DuckDB memory limit for streaming writes (e.g., '2GB', '512MB').
            If None, auto-detects based on available system/container memory.
        geometry_info: Dict containing multi-geometry column info with keys:
            - "primary": primary geometry column name
            - "secondary": list of secondary geometry column names
            - "metadata": dict mapping column names to their metadata (crs, encoding, etc.)
        extra_kv_metadata: Additional Parquet file-level KV metadata as {key: json_string}.
            Written alongside the 'geo' key (e.g., for Vecorel collection metadata).
        input_file: Path to the input parquet file, when the write rewrites an
            existing file. Enables full-fidelity non-planar edges preservation
            (native GEOGRAPHY logical types are only visible in the file's
            schema); without it, edges still fall back to original_metadata.
        invalidate_derived_stats: When True, strip the carried per-column
            ``bbox`` and ``geometry_types`` from ``original_metadata`` before
            building output geo metadata. Set by callers that transform geometry
            (reproject), filter rows (extract), or merge several inputs whose
            carried metadata came from only the first file: the input's stats no
            longer describe the output, so they must be recomputed from the
            written data or omitted rather than carried.
        invalidate_derived_stats_columns: Restricts that invalidation to these
            geometry columns. Set by a caller that changes coordinates in some
            geometry columns but not others — reproject transforms only the
            primary column, so a secondary column reaches the output unchanged
            and its carried stats still describe it (#890). None (the default)
            invalidates every column, which is right for a merge: its carried
            stats under-cover every column of the output. An invalidated column
            other than the primary keeps ``geometry_types`` as the spec's empty
            "not known" list rather than losing the key, since nothing here
            recomputes it (#934).
        drop_nonplanar_edges_columns: Geometry columns whose non-planar
            ``edges`` declaration must neither be carried through nor
            re-attached to the output. Set by writes that invalidate the edge
            interpretation for the columns they transform — reprojecting the
            primary geometry column to a projected CRS turns its great-circle
            edges into straight lines (#601) — where planar (the spec default)
            is the truthful description of the output. Columns not named here
            (e.g. an untransformed secondary geometry column) keep their
            declaration. The caller warns; this only stops the carry.

    Returns:
        None
    """
    from geoparquet_io.core.write_strategies import (
        WriteStrategy,
        WriteStrategyFactory,
        needs_metadata_rewrite,
    )

    configure_verbose(verbose)

    # Use geometry column from geometry_info if provided, otherwise auto-detect
    # This ensures original column names are preserved (fixes #328)
    # Resolved before the invalidation below, which has to name the one column
    # the write strategies recompute derived stats for.
    if geometry_info and geometry_info.get("primary"):
        geometry_column = geometry_info["primary"]
    else:
        geometry_column = _detect_geometry_from_query(con, query, original_metadata, verbose)

    # Callers that transform geometry, filter rows, or merge multiple inputs
    # invalidate the carried bbox/geometry_types; drop them so the write
    # strategies recompute (or omit) them instead of describing the input. A
    # caller that transforms only some geometry columns names them, so an
    # untouched secondary column keeps the stats that still describe it (#890).
    #
    # Every strategy recomputes `geometry_column` and only that, so a stripped
    # SECONDARY column would be left with no `geometry_types` at all -- a key
    # GeoParquet 1.1 requires and DuckDB refuses to open a file without. Naming
    # the recomputed column leaves the others the "not known" sentinel (#934).
    if invalidate_derived_stats:
        original_metadata = strip_derived_stats(
            original_metadata,
            columns=invalidate_derived_stats_columns,
            recomputed_columns={geometry_column} if geometry_column else set(),
        )

    # A write that invalidates the edge interpretation for the columns it
    # transforms (reprojecting into a projected CRS, #601) must neither carry
    # those columns' declaration through nor re-attach it afterwards; planar is
    # then the truthful description. Untransformed columns keep theirs.
    if drop_nonplanar_edges_columns:
        original_metadata = strip_nonplanar_edges(
            original_metadata, columns=drop_nonplanar_edges_columns
        )

    # A column projection (e.g. ``extract --exclude-cols``) can drop the bbox
    # column a ``covering`` points at, a secondary geometry column, or the
    # geometry column itself. Carrying those references produces metadata that
    # names schema roots the output does not have.
    original_metadata, output_columns = _prune_metadata_to_output_columns(
        con, query, original_metadata, geometry_column, verbose
    )

    # The facade owns both of these: how many rows a row group gets, and which
    # version auto mode resolves to. Every caller of this function -- 21 of them
    # -- used to inherit whatever its writer defaulted to and whatever its own
    # `geo` key happened to say, which is how `convert` came to write 122,880-row
    # groups (#981) and how `sort`/`extract`/`partition` came to rewrite a
    # native-geo-only input as 1.1 WKB while `convert` kept it native (#600).
    row_group_rows = resolve_row_group_rows(row_group_rows, row_group_size_mb)
    geoparquet_version = resolve_output_geoparquet_version(
        geoparquet_version,
        input_file=input_file,
        original_metadata=original_metadata,
        verbose=verbose,
    )

    effective_version = geoparquet_version or "1.1"

    # Check if we need to add/rewrite geo metadata
    rewrite_needed = needs_metadata_rewrite(effective_version, original_metadata)

    # Force rewrite if custom_metadata contains covering (e.g., bbox, H3, S2)
    # This ensures covering metadata is written even for 2.0→2.0 operations
    if custom_metadata and "covering" in custom_metadata:
        rewrite_needed = True
        if verbose:
            debug("Forcing metadata rewrite for covering metadata")

    # Preserve non-geo KV metadata from input (e.g., vecorel, fiboa).
    # Build a merged local dict rather than mutating the caller-supplied
    # extra_kv_metadata: partition loops reuse one dict across writes, and
    # writing into it in place leaked prior files' keys into later ones.
    preserved_keys = extract_preserved_kv_metadata(original_metadata)
    if preserved_keys:
        # Caller-supplied entries win over preserved keys of the same name.
        extra_kv_metadata = {**preserved_keys, **(extra_kv_metadata or {})}

    # Sidecar keys no longer force a rewrite: DuckDB accepts KV_METADATA in the
    # same COPY as GEOPARQUET_VERSION, so `_plain_copy_to` writes them itself
    # (#709). A 2.0 -> 2.0 copy that happens to carry a fiboa or vecorel payload
    # used to pay for a full metadata rewrite -- an extra scan of the geometry
    # column to recompute bbox and geometry types -- for a key that has nothing
    # to do with the geo block.
    #
    # The fast path is no longer thinner than the rewrite: DuckDB regenerates
    # the `geo` key, but `_geo_block_to_carry_on_fast_path` substitutes the
    # input's own block -- `epoch`, `orientation`, a declared `covering`, plus
    # one auto-declared for a conventional bbox column the output carries -- so
    # for the same input, and absent `custom_metadata`, both paths write the
    # same block (#772). `custom_metadata` is the exception: only the rewrite
    # path merges it, and a `covering` in it already forces that path above.
    if extra_kv_metadata and verbose:
        debug(f"Carrying extra KV metadata: {list(extra_kv_metadata.keys())}")

    if show_sql:
        info("\n-- Query:")
        progress(query)

    # No AWS_PROFILE env mutation here: the write target inside this block is a
    # local (temp) file, and the upload at the end is credentialed by passing
    # profile= straight through to upload().
    with remote_write_context(output_file, is_directory=False, verbose=verbose) as (
        actual_output,
        is_remote,
    ):
        if not rewrite_needed:
            # Fast path: plain DuckDB COPY TO without geo metadata manipulation
            if verbose:
                debug(f"Writing GeoParquet version: {effective_version}")
                debug(f"No metadata rewrite needed for {effective_version} - using plain COPY TO")

            # Nothing but a bare DuckDB COPY writes this file, so a sub-vector
            # request lands at 2,048 whatever was asked for. Say so here, where
            # that is certain, rather than let the footer be the first to tell
            # the user (#986).
            note_duckdb_copy_rounding(row_group_rows)
            _plain_copy_to(
                con=con,
                query=query,
                output_path=actual_output,
                compression=compression,
                compression_level=compression_level,
                row_group_rows=row_group_rows,
                verbose=verbose,
                geoparquet_version=effective_version,
                input_crs=input_crs,
                geometry_column=geometry_column,
                carry_geo_metadata=_geo_block_to_carry_on_fast_path(
                    original_metadata,
                    geometry_column,
                    effective_version,
                    con=con,
                    query=query,
                    verbose=verbose,
                    input_crs=input_crs,
                    input_file=input_file,
                    output_columns=output_columns,
                ),
                extra_kv_metadata=extra_kv_metadata,
            )
        else:
            # Metadata rewrite needed - use strategy pattern

            # 1.1-geoarrow produces native GeoArrow encoding from WKB/text inputs,
            # which requires the arrow-streaming strategy (DuckDB COPY TO cannot emit
            # nested GeoArrow types). Already-native inputs keep their preservation
            # path (duckdb-kv passes the native column through unchanged).
            auto_routed_strategy = False
            if geoparquet_version == "1.1-geoarrow":
                primary = (geometry_info or {}).get("primary")
                input_encoding = (
                    (geometry_info or {}).get("metadata", {}).get(primary, {}).get("encoding")
                )
                already_native = bool(
                    input_encoding and input_encoding.lower() not in ("wkb", "wkt")
                )
                if not already_native and write_strategy != "streaming":
                    if verbose:
                        debug("Routing 1.1-geoarrow WKB input through arrow-streaming")
                    write_strategy = "streaming"
                    auto_routed_strategy = True

            strategy_enum = WriteStrategy(write_strategy)
            strategy = WriteStrategyFactory.get_strategy(strategy_enum)

            # Only duckdb-kv can honour a memory limit. If *we* rerouted the
            # strategy (1.1-geoarrow above), the user did nothing wrong: warn and
            # drop the limit rather than aborting a command that worked before
            # --write-memory was plumbed through (#663). A strategy the user
            # explicitly asked for is a real error — raised as a core exception so
            # the CLI shows a clean message instead of a traceback.
            if memory_limit is not None and strategy_enum != WriteStrategy.DUCKDB_KV:
                if auto_routed_strategy:
                    warn(
                        "--write-memory is ignored for GeoParquet 1.1-geoarrow output: "
                        "GeoArrow encoding requires the arrow-streaming write strategy, "
                        "which does not support a memory limit."
                    )
                    memory_limit = None
                else:
                    raise InvalidParameterError(
                        "--write-memory",
                        f"a memory limit is only supported with the 'duckdb-kv' "
                        f"write strategy, not '{write_strategy}'",
                    )

            if verbose:
                debug(f"Writing GeoParquet version: {effective_version}")
                debug(f"Using write strategy: {strategy.name}")

            # Build kwargs - only pass memory_limit for duckdb-kv
            write_kwargs = {
                "con": con,
                "query": query,
                "output_path": actual_output,
                # None (no geometry in the output) is meaningful: every strategy
                # falls back to a plain-Parquet write rather than advertising a
                # geometry column that is not there.
                "geometry_column": geometry_column,
                "original_metadata": original_metadata,
                "geoparquet_version": effective_version,
                "compression": compression,
                "compression_level": compression_level,
                "row_group_size_mb": row_group_size_mb,
                "row_group_rows": row_group_rows,
                "input_crs": input_crs,
                "verbose": verbose,
                "custom_metadata": custom_metadata,
                "geometry_info": geometry_info,
                "extra_kv_metadata": extra_kv_metadata,
            }
            if strategy_enum == WriteStrategy.DUCKDB_KV:
                write_kwargs["memory_limit"] = memory_limit
            # duckdb-kv alone lets DuckDB's COPY have the last word on row-group
            # size, so it alone rounds a sub-vector request up to 2,048 (#986).
            # Measured with --row-group-size 249 on a 10,000-row input:
            # duckdb-kv writes 2,048-row groups; in-memory, streaming and
            # disk-rewrite all write 249-row ones -- disk-rewrite because its
            # pyarrow pass regroups the COPY output at the requested size.
            if strategy_enum == WriteStrategy.DUCKDB_KV:
                note_duckdb_copy_rounding(row_group_rows)

            strategy.write_from_query(**write_kwargs)

        # Non-planar edges must survive every rewrite path (#588): DuckDB
        # regenerates geo metadata without `edges`, silently demoting geography
        # data to planar. Runs on the local temp file, so remote outputs are
        # patched before upload. Columns whose declaration this write
        # invalidated (#601) are excluded rather than re-attached.
        _preserve_edges_after_write(
            input_file,
            original_metadata,
            actual_output,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            verbose=verbose,
            exclude_columns=drop_nonplanar_edges_columns,
        )

        # Auto-fix vecorel schema compliance when collection metadata is present
        if not is_remote:
            _auto_fix_vecorel_if_needed(actual_output, extra_kv_metadata, original_metadata)

        if is_remote:
            upload_if_remote(
                actual_output,
                output_file,
                profile=profile,
                is_directory=False,
                verbose=verbose,
            )


def _preserve_edges_after_write(
    input_file: str | None,
    original_metadata: dict | None,
    output_path: str,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
    verbose: bool = False,
    exclude_columns: Collection[str] | None = None,
) -> None:
    """Restore non-planar edges dropped by the writer, on any rewrite path.

    Prefers the input file (sees native GEOGRAPHY logical types as well as geo
    metadata); falls back to the input's KV metadata dict when only that is
    available (e.g. partition staging rewrites). ``exclude_columns`` names
    columns whose declaration the write itself invalidated (#601): the input
    still declares them, but they must not come back.
    """
    edges_by_col = collect_nonplanar_edges(input_file) if input_file else {}
    if not edges_by_col:
        edges_by_col = _collect_nonplanar_edges_from_metadata(original_metadata)
    if exclude_columns:
        edges_by_col = {k: v for k, v in edges_by_col.items() if k not in exclude_columns}
    _apply_nonplanar_edges(
        edges_by_col,
        output_path,
        compression=compression,
        compression_level=compression_level,
        row_group_rows=row_group_rows,
        verbose=verbose,
    )


def _auto_fix_vecorel_if_needed(
    output_path: str,
    extra_kv_metadata: dict | None,
    original_metadata: dict | None,
) -> None:
    """Fix vecorel schema compliance if the output has collection metadata."""
    has_collection = False
    if extra_kv_metadata and "collection" in extra_kv_metadata:
        has_collection = True
    elif original_metadata:
        if "collection" in original_metadata or b"collection" in original_metadata:
            has_collection = True

    if has_collection:
        import pyarrow.parquet as pq

        from geoparquet_io.core.constants import VECOREL_NON_NULLABLE, _fix_vecorel_schema

        pf = pq.ParquetFile(output_path)
        columns = set(pf.schema_arrow.names)
        # Close before _fix_vecorel_schema rewrites the file in place; an open
        # handle here would make its os.replace() fail on Windows.
        pf.close()
        non_nullable = [c for c in VECOREL_NON_NULLABLE if c in columns]
        if non_nullable:
            _fix_vecorel_schema(output_path, non_nullable)


def write_geoparquet_table(
    table,
    output_file: str,
    geometry_column: str | None = None,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    profile: str | None = None,
    edges: str | None = None,
    geo_bbox: list[float] | None = None,
) -> None:
    """
    Write a PyArrow Table to a GeoParquet file with proper metadata.

    This is the table-centric version for writing GeoParquet files.
    It applies proper GeoParquet metadata and handles compression settings.

    Args:
        table: PyArrow Table to write
        output_file: Path to output file (local path or remote URL)
        geometry_column: Name of geometry column (auto-detected if None)
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0)
        verbose: Whether to print verbose output
        profile: AWS profile name (S3 only, optional)
        edges: Edge interpretation, "spherical" or "planar" (default None = planar).
               Use "spherical" for data from BigQuery or other S2-based sources.
        geo_bbox: Pre-computed [xmin, ymin, xmax, ymax] for the geometry column.
               Pass the RFC 7946 5.2 wrap form (xmin > xmax) for data that
               crosses the antimeridian; None computes a plain extent.
    """
    # A write path: the column name is quoted into the output's metadata and the
    # CRS is written to the output file, so the carried block goes through the
    # shared shape check rather than being indexed raw. `columns: null` used to
    # raise `argument of type 'NoneType' is not iterable` here, and a list-,
    # string- or non-object-entry `columns` a `TypeError` one line later (#947).
    geo_meta = sanitized_carried_geo(table.schema.metadata)

    # The facade's row-group decision, so an Arrow-side write (arcgis, carto,
    # wfs, bigquery, aggregate, overview) lands on the same number a DuckDB-side
    # one does. Without it these fell through to ParquetWriteSettings' old
    # 100,000 while the COPY paths took DuckDB's 122,880.
    row_group_rows = resolve_row_group_rows(row_group_rows, row_group_size_mb)

    if geometry_column is None:
        geometry_column = carried_geometry_column(geo_meta, table.column_names) or "geometry"

    # Check if geometry column exists
    has_geometry = geometry_column in table.column_names

    # Extract original metadata for preservation
    original_metadata = table.schema.metadata

    # Extract CRS from original metadata if available
    col_meta = (geo_meta.get("columns") or {}).get(geometry_column)
    input_crs = col_meta.get("crs") if isinstance(col_meta, dict) else None

    # Validate and normalize compression settings
    validated_compression, validated_level, _ = validate_compression_settings(
        compression or "ZSTD", compression_level, verbose
    )
    # Handle UNCOMPRESSED - pass None for compression when uncompressed
    if validated_compression == "UNCOMPRESSED":
        validated_compression = None

    # Normalize large_string/large_binary back to string/binary for Parquet compatibility
    table = _normalize_arrow_large_types(table)

    # No AWS_PROFILE env mutation here: the write target inside this block is a
    # local (temp) file, and the upload at the end is credentialed by passing
    # profile= straight through to upload().
    with remote_write_context(output_file, is_directory=False, verbose=verbose) as (
        actual_output,
        is_remote,
    ):
        # Apply GeoParquet metadata if the geometry column exists, or when
        # parquet-geo-only was explicitly requested: that request has to strip a
        # carried geo key whether or not the column survived a projection (#701).
        if has_geometry or geoparquet_version == "parquet-geo-only":
            table = _apply_geoparquet_metadata(
                table,
                geometry_column=geometry_column,
                geoparquet_version=geoparquet_version,
                original_metadata=original_metadata,
                input_crs=input_crs,
                custom_metadata=None,
                verbose=verbose,
                edges=edges,
                geo_bbox=geo_bbox,
            )

        # Write to disk with proper settings
        _write_table_with_settings(
            table,
            actual_output,
            compression=validated_compression or "UNCOMPRESSED",
            compression_level=validated_level,
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


def format_size(size_bytes):
    """Convert bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


#: Struct fields a bbox covering column must expose.
_BBOX_REQUIRED_FIELDS = frozenset({"xmin", "ymin", "xmax", "ymax"})


def _bbox_column_from_covering(geo_meta) -> str | None:
    """Return the bbox column name referenced by GeoParquet ``covering.bbox``.

    The covering metadata is the authoritative pointer to a file's bbox column
    (its name need not follow any convention). Returns ``None`` when absent or
    malformed.
    """
    if not isinstance(geo_meta, dict):
        return None
    columns = geo_meta.get("columns", {})
    if not isinstance(columns, dict):
        return None
    for col_info in columns.values():
        if not isinstance(col_info, dict):
            continue
        covering = col_info.get("covering")
        bbox_refs = covering.get("bbox") if isinstance(covering, dict) else None
        if (
            isinstance(bbox_refs, dict)
            and _BBOX_REQUIRED_FIELDS.issubset(bbox_refs)
            and all(isinstance(ref, list) and len(ref) == 2 for ref in bbox_refs.values())
        ):
            return bbox_refs["xmin"][0]
    return None


def _schema_struct_child_names(schema_info, column_name) -> set | None:
    """Child field names of struct column ``column_name`` from a flat
    ``parquet_schema()`` listing; ``None`` if the column is absent or not a struct."""
    for i, col in enumerate(schema_info):
        if col.get("name") != column_name:
            continue
        num_children = col.get("num_children") or 0
        if num_children < 1:
            return None
        return {
            schema_info[i + j].get("name", "")
            for j in range(1, num_children + 1)
            if i + j < len(schema_info)
        }
    return None


def _find_bbox_column_in_schema(schema_info, verbose):
    """Find bbox column in schema by conventional names or structure.

    Args:
        schema_info: List of column dicts from get_schema_info()
        verbose: Whether to print verbose output

    Note:
        DuckDB's parquet_schema() returns nested struct fields without parent prefix.
        For a struct column 'bbox' with fields xmin/ymin/xmax/ymax:
        - bbox appears with num_children=4
        - Child fields appear as 'xmin', 'ymin', 'xmax', 'ymax' (not 'bbox.xmin')
    """
    # Check for columns ending with these suffixes (e.g., geometry_bbox, bbox)
    conventional_suffixes = ["bbox", "bounds", "extent"]
    required_fields = {"xmin", "ymin", "xmax", "ymax"}

    for i, col in enumerate(schema_info):
        name = col.get("name", "")
        num_children = col.get("num_children", 0)

        if not name:
            continue

        # Check if column name ends with conventional suffixes and has struct children
        is_bbox_name = any(name.endswith(suffix) for suffix in conventional_suffixes)
        if is_bbox_name and num_children >= 4:
            # Get the next num_children entries as the struct's child fields
            child_names = set()
            for j in range(1, num_children + 1):
                if i + j < len(schema_info):
                    child_name = schema_info[i + j].get("name", "")
                    child_names.add(child_name)

            # Check if all required fields are present
            if required_fields.issubset(child_names):
                if verbose:
                    debug(f"Found bbox column: {name} with children: {child_names}")
                return name

    return None


def _check_bbox_metadata_covering(geo_meta, has_bbox_column, verbose, bbox_column_name=None):
    """Check if geo metadata contains proper bbox covering.

    Args:
        geo_meta: Parsed geo metadata dict (from get_geo_metadata())
        has_bbox_column: Whether a bbox column was found in schema
        verbose: Whether to print verbose output
        bbox_column_name: The bbox column actually present in the file. A
            covering only counts as declaring *this* file's bbox column when it
            names it; a covering pointing at some other (or absent) column is a
            dangling reference, and treating it as "declared" let a broken file
            pass `check` while the success message named a different column
            entirely (#738).
    """
    if not (geo_meta and has_bbox_column):
        return False

    if verbose:
        debug("\nParsed geo metadata:")
        debug(json.dumps(geo_meta, indent=2))

    # Validation-shared, so the block stays as the file really holds it (#883's
    # line) and this guards instead of sanitizing: `gpio check bbox` and `check
    # all` report through here. A `columns` that is not an object, or a
    # `covering` that is not one, simply declares no covering -- the truthful
    # answer, where iterating it raised an `AttributeError` from someone else's
    # file (#947).
    columns = geo_meta.get("columns") if isinstance(geo_meta, dict) else None
    if isinstance(columns, dict):
        for _col_name, col_info in columns.items():
            covering = col_info.get("covering") if isinstance(col_info, dict) else None
            if isinstance(covering, dict) and covering.get("bbox"):
                bbox_refs = covering["bbox"]
                # Check if the bbox covering has the required structure
                if (
                    isinstance(bbox_refs, dict)
                    and all(key in bbox_refs for key in ["xmin", "ymin", "xmax", "ymax"])
                    and all(isinstance(ref, list) and len(ref) == 2 for ref in bbox_refs.values())
                ):
                    referenced_bbox_column = bbox_refs["xmin"][0]
                    if bbox_column_name is not None and referenced_bbox_column != bbox_column_name:
                        if verbose:
                            debug(
                                f"Covering references column '{referenced_bbox_column}', "
                                f"but the file's bbox column is '{bbox_column_name}' - "
                                "treating the bbox column as undeclared"
                            )
                        continue
                    if verbose:
                        debug(
                            f"Found bbox covering in metadata referencing column: {referenced_bbox_column}"
                        )
                    return True

    return False


def _determine_bbox_status(has_bbox_column, bbox_column_name, has_bbox_metadata):
    """Determine bbox status and message."""
    if has_bbox_column and has_bbox_metadata:
        return "optimal", f"✓ Found bbox column '{bbox_column_name}' with proper metadata covering"
    elif has_bbox_column:
        return (
            "suboptimal",
            f"⚠️  Found bbox column '{bbox_column_name}' but no bbox covering metadata (recommended for better performance)",
        )
    else:
        return "poor", "❌ No valid bbox column found"


class BboxInfo(TypedDict, total=False):
    """Bbox structure information returned by check_bbox_structure."""

    has_bbox_column: bool
    bbox_column_name: str | None
    has_bbox_metadata: bool
    status: Literal["optimal", "suboptimal", "poor", "native"]
    message: str


def check_bbox_structure(parquet_file, verbose=False) -> BboxInfo:
    """
    Check bbox structure and metadata coverage in a GeoParquet file.

    Returns:
        dict: Results including:
            - has_bbox_column (bool): Whether a valid bbox struct column exists
            - bbox_column_name (str): Name of the bbox column if found
            - has_bbox_metadata (bool): Whether bbox covering is specified in metadata
            - status (str): "optimal", "suboptimal", or "poor"
            - message (str): Human readable description
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata, get_schema_info

    # Get schema info using DuckDB
    schema_info = get_schema_info(parquet_file)

    if verbose:
        debug("\nSchema fields:")
        for col in schema_info:
            name = col.get("name", "")
            col_type = col.get("type", "")
            if name:  # Skip empty names
                debug(f"  {name}: {col_type}")

    # Find the bbox column: the authoritative covering metadata first (spec-valid
    # files may use non-conventional names), then the naming-convention fallback.
    geo_meta = get_geo_metadata(parquet_file)
    bbox_column_name = None
    covering_column = _bbox_column_from_covering(geo_meta)
    if covering_column:
        children = _schema_struct_child_names(schema_info, covering_column)
        if children and _BBOX_REQUIRED_FIELDS.issubset(children):
            bbox_column_name = covering_column
            if verbose:
                debug(f"Found bbox column from covering metadata: {covering_column}")
    if bbox_column_name is None:
        bbox_column_name = _find_bbox_column_in_schema(schema_info, verbose)
    has_bbox_column = bbox_column_name is not None

    # Check for bbox covering in the geo metadata
    has_bbox_metadata = _check_bbox_metadata_covering(
        geo_meta, has_bbox_column, verbose, bbox_column_name
    )

    # Determine status and message
    status, message = _determine_bbox_status(has_bbox_column, bbox_column_name, has_bbox_metadata)

    if verbose:
        debug("\nFinal results:")
        debug(f"  has_bbox_column: {has_bbox_column}")
        debug(f"  bbox_column_name: {bbox_column_name}")
        debug(f"  has_bbox_metadata: {has_bbox_metadata}")
        debug(f"  status: {status}")
        debug(f"  message: {message}")

    return {
        "has_bbox_column": has_bbox_column,
        "bbox_column_name": bbox_column_name if has_bbox_column else None,
        "has_bbox_metadata": has_bbox_metadata,
        "status": status,
        "message": message,
    }


def get_bbox_advice(
    parquet_file: str,
    operation: str,
    verbose: bool = False,
) -> dict:
    """
    Get version-aware bbox optimization advice.

    Provides context-aware recommendations based on file type and operation:
    - For GeoParquet 2.0/parquet-geo with spatial_filtering: No bbox pre-filter
      needed -- a bare ST_Intersects ON clause lets DuckDB's SPATIAL_JOIN operator
      engage, which is dramatically faster than a bbox-overlap NL join (issue #538).
    - For GeoParquet 2.0/parquet-geo with bounds_calculation: bbox still recommended (faster)
    - For GeoParquet 1.x without bbox: Suggest adding bbox OR upgrading to 2.0

    Args:
        parquet_file: Path to the parquet file
        operation: One of:
            - "spatial_filtering": For ST_Intersects, spatial joins, etc.
            - "bounds_calculation": For centroid, extent, quadkey, etc.
            - "check": For validation/inspection
        verbose: Whether to print verbose output

    Returns:
        dict with:
            - needs_warning: bool - Whether to show a warning to the user
            - skip_bbox_prefilter: bool - Whether to skip bbox pre-filtering in queries.
              Only True for spatial_filtering with native geometry. There a bare
              ST_Intersects ON clause is recognized by DuckDB's SPATIAL_JOIN operator
              and is much faster than ANDing a bbox-overlap test in front of it, which
              defeats SPATIAL_JOIN and forces a BLOCKWISE_NL_JOIN (issue #538). For
              bounds_calculation, always False since the bbox column provides
              pre-computed values that are faster than geometry stats.
            - has_native_geometry: bool - Whether file uses native Parquet geometry types
            - message: str - User-facing message (if needs_warning)
            - suggestions: list[str] - Suggested actions for the user
    """
    file_info = detect_geoparquet_file_type(parquet_file, verbose)
    bbox_info = check_bbox_structure(parquet_file, verbose)

    # Read native-geometry capability directly rather than inferring from file_type:
    # a 1.1-geoarrow file carries native geometry columns but is labelled
    # "geoparquet_v1" (version starts with "1."), so a file_type membership check
    # would misclassify it as non-native. .get() also avoids a KeyError.
    has_native_geo = file_info.get("has_native_geo_types", False)
    has_bbox = bbox_info["has_bbox_column"]

    # Only skip bbox pre-filtering for spatial_filtering operations with native geometry.
    # For bounds_calculation, bbox column provides pre-computed values that are faster.
    skip_bbox = has_native_geo and operation == "spatial_filtering"

    result = {
        "needs_warning": False,
        "skip_bbox_prefilter": skip_bbox,
        "has_native_geometry": has_native_geo,
        "has_bbox_column": has_bbox,
        "bbox_column_name": bbox_info.get("bbox_column_name"),
        "message": "",
        "suggestions": [],
    }

    if operation == "spatial_filtering":
        if has_native_geo:
            # Native geometry -> bare ST_Intersects ON clause engages DuckDB's
            # SPATIAL_JOIN operator (the fast path, issue #538) - no warning needed.
            if verbose:
                debug("Native geometry: bare ST_Intersects engages DuckDB SPATIAL_JOIN")
        elif not has_bbox:
            # 1.x without bbox - warn and suggest options
            result["needs_warning"] = True
            result["message"] = "No bbox column found"
            result["suggestions"] = [
                "Add a bbox column: gpio add bbox <file>",
                "Or upgrade to GeoParquet 2.0: gpio convert <file> --geoparquet-version 2.0",
            ]

    elif operation == "bounds_calculation":
        # bbox column is still faster for bounds/centroid calculation (pre-computed values)
        if not has_bbox:
            result["needs_warning"] = True
            result["message"] = "No bbox column - computing from geometry (slower)"
            result["suggestions"] = [
                "Add a bbox column for 3-4x faster bounds/centroid: gpio add bbox <file>"
            ]

    elif operation == "check":
        if has_native_geo:
            # Native geometry - bbox optional but can help with bounds queries
            if not has_bbox and verbose:
                debug("Native geometry type detected - bbox column optional for spatial queries")
        elif not has_bbox:
            # 1.x without bbox
            result["needs_warning"] = True
            result["message"] = "No bbox column found"
            result["suggestions"] = [
                "Add a bbox column: gpio add bbox <file>",
                "Or upgrade to GeoParquet 2.0: gpio convert <file> --geoparquet-version 2.0",
            ]

    return result


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


def create_shapefile_zip(shapefile_path: str | Path, verbose: bool = False) -> Path:
    """
    Create a zip archive containing a shapefile and all its sidecar files.

    Shapefiles consist of multiple files with different extensions (.shp, .shx, .dbf, .prj, .cpg).
    This function creates a single .shp.zip archive containing all related files.

    Args:
        shapefile_path: Path to the main .shp file
        verbose: Print verbose output

    Returns:
        Path to the created .shp.zip file

    Raises:
        click.ClickException: If shapefile or required sidecars are missing
    """
    import zipfile

    configure_verbose(verbose)
    shapefile_path = Path(shapefile_path)

    if not shapefile_path.exists():
        raise FileNotFoundGeoParquetError(str(shapefile_path))

    # Shapefile extensions: .shp (main), .shx (index), .dbf (attributes) are required
    # Optional: .prj (projection), .cpg (encoding), .sbn/.sbx (spatial index)
    stem = shapefile_path.stem
    parent = shapefile_path.parent

    # Find all files with the same stem
    sidecar_files = list(parent.glob(f"{stem}.*"))

    # Filter to only shapefile-related extensions
    shapefile_extensions = {".shp", ".shx", ".dbf", ".prj", ".cpg", ".sbn", ".sbx"}
    sidecar_files = [f for f in sidecar_files if f.suffix.lower() in shapefile_extensions]

    if not sidecar_files:
        raise FileNotFoundGeoParquetError(str(shapefile_path), "no shapefile components found")

    # Verify required files exist
    required_extensions = {".shp", ".shx", ".dbf"}
    found_extensions = {f.suffix.lower() for f in sidecar_files}
    missing = required_extensions - found_extensions

    if missing:
        raise FileNotFoundGeoParquetError(
            str(shapefile_path), f"missing required shapefile components: {', '.join(missing)}"
        )

    # Create zip file
    zip_path = parent / f"{stem}.shp.zip"

    if verbose:
        debug(f"Creating shapefile archive: {zip_path}")
        debug(f"Including {len(sidecar_files)} files: {[f.name for f in sidecar_files]}")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for sidecar in sidecar_files:
            zipf.write(sidecar, sidecar.name)
            if verbose:
                debug(f"  Added: {sidecar.name}")

    if verbose:
        zip_size = zip_path.stat().st_size
        success(f"Created shapefile archive: {zip_path} ({format_size(zip_size)})")

    return zip_path
