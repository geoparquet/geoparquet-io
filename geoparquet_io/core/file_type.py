"""What kind of geo-bearing Parquet file is this, and is a bbox column wanted?

One answer -- GeoParquet 1.x, GeoParquet 2.0, native-geo-only, or unknown --
derived from the file's ``geo`` metadata and its Parquet logical types, cached
by path and mtime because nearly every command asks it at least once.
"""

from typing import Any

from geoparquet_io.core.duckdb_metadata import detect_geometry_columns, get_geo_metadata
from geoparquet_io.core.file_utils import _get_file_cache_key
from geoparquet_io.core.geo_metadata import carried_version
from geoparquet_io.core.logging_config import debug

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
    # Check cache first (skip if connection provided - caller wants fresh read)
    if con is None:
        cached_result = _check_file_type_cache(parquet_file)
        if cached_result is not None:
            if verbose:
                debug(f"File type detection (cached): {cached_result}")
            return cached_result.copy()  # Return copy to prevent mutation

    result: dict[str, Any] = {
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
detect_geoparquet_file_type.cache_clear = detect_geoparquet_file_type_cache_clear  # type: ignore[attr-defined]
