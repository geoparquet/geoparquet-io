"""
DuckDB-based Parquet metadata extraction.

This module provides functions to read Parquet metadata using DuckDB's
built-in parquet functions instead of fsspec/PyArrow. This simplifies
remote file access (S3, HTTP, etc.) by leveraging DuckDB's httpfs extension.
"""

import json
import re
from typing import Any


def _get_connection_for_file(parquet_file: str, existing_con=None):
    """Get or create DuckDB connection appropriate for file location."""
    from geoparquet_io.core.common import (
        get_duckdb_connection,
        get_duckdb_connection_for_s3,
        is_s3_url,
        needs_httpfs,
    )

    if existing_con:
        return existing_con, False  # False = don't close

    if is_s3_url(parquet_file):
        return get_duckdb_connection_for_s3(parquet_file, load_spatial=True), True
    else:
        return get_duckdb_connection(
            load_spatial=True, load_httpfs=needs_httpfs(parquet_file)
        ), True


def _safe_url(parquet_file: str) -> str:
    """Get safe URL for DuckDB queries.

    For partitioned datasets (directories or glob patterns), returns
    path to the first file for metadata operations that require a single file.
    """
    from geoparquet_io.core.common import (
        get_first_parquet_file,
        is_partition_path,
        safe_file_url,
    )

    # For partitions, use first file for metadata operations
    if is_partition_path(parquet_file):
        first_file = get_first_parquet_file(parquet_file)
        if first_file:
            parquet_file = first_file

    return safe_file_url(parquet_file, verbose=False)


def get_kv_metadata(parquet_file: str, con=None) -> dict[bytes, bytes]:
    """
    Extract key-value metadata using parquet_kv_metadata().

    Returns dict like {b'geo': b'{"version": "1.1.0", ...}'}
    """
    safe_url = _safe_url(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        # Get raw bytes to avoid DuckDB's VARCHAR escaping
        result = connection.execute(f"""
            SELECT key, value
            FROM parquet_kv_metadata('{safe_url}')
        """).fetchall()

        # Convert to dict with bytes keys for backward compatibility
        kv_dict = {}
        for k, v in result:
            # Handle both bytes and string types
            key_bytes = k if isinstance(k, bytes) else k.encode("utf-8") if k else None
            val_bytes = v if isinstance(v, bytes) else v.encode("utf-8") if v else None
            if key_bytes:
                kv_dict[key_bytes] = val_bytes
        return kv_dict
    finally:
        if should_close:
            connection.close()


def get_geo_metadata(parquet_file: str, con=None) -> dict | None:
    """
    Extract and parse 'geo' metadata key.

    Returns parsed GeoParquet metadata dict or None if not present.
    """
    safe_url = _safe_url(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        # Get raw bytes and decode manually to avoid DuckDB's VARCHAR escaping
        result = connection.execute(f"""
            SELECT value
            FROM parquet_kv_metadata('{safe_url}')
            WHERE key::VARCHAR = 'geo'
        """).fetchone()

        if result and result[0]:
            # Decode raw bytes to string and parse JSON
            if isinstance(result[0], bytes):
                json_str = result[0].decode("utf-8")
            else:
                json_str = str(result[0])
            return json.loads(json_str)
        return None
    except json.JSONDecodeError:
        return None
    finally:
        if should_close:
            connection.close()


def get_file_metadata(parquet_file: str, con=None) -> dict:
    """
    Get file-level metadata (num_rows, num_row_groups).

    Uses parquet_file_metadata() for fast access.
    """
    safe_url = _safe_url(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        result = connection.execute(f"""
            SELECT * FROM parquet_file_metadata('{safe_url}')
        """).fetchone()

        columns = [desc[0] for desc in connection.description]
        return dict(zip(columns, result, strict=True)) if result else {}
    finally:
        if should_close:
            connection.close()


def get_schema_info(parquet_file: str, con=None) -> list[dict]:
    """
    Get schema column info using parquet_schema().

    Returns list of dicts with 'name', 'type', 'logical_type', etc.
    """
    safe_url = _safe_url(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        result = connection.execute(f"""
            SELECT * FROM parquet_schema('{safe_url}')
        """).fetchall()

        columns = [desc[0] for desc in connection.description]
        return [dict(zip(columns, row, strict=True)) for row in result]
    finally:
        if should_close:
            connection.close()


def get_column_names(parquet_file: str, con=None) -> list[str]:
    """Get list of column names from schema."""
    schema = get_schema_info(parquet_file, con)
    # Filter out empty names (schema root element) and nested struct fields
    return [col["name"] for col in schema if col.get("name") and "." not in col["name"]]


def get_usable_columns(parquet_file: str, con=None) -> list[dict]:
    """
    Get user-facing columns as DuckDB exposes them.

    Uses DESCRIBE SELECT * FROM read_parquet() which returns the actual
    columns users can query. This handles quirky Parquet files that wrap
    all data in a struct (e.g., a 'schema' struct containing all fields).

    Returns list of dicts with 'name' and 'type' keys.
    """
    safe_url = _safe_url(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        result = connection.execute(f"""
            DESCRIBE SELECT * FROM read_parquet('{safe_url}')
        """).fetchall()

        # DESCRIBE returns: (column_name, column_type, null, key, default, extra)
        return [{"name": row[0], "type": row[1]} for row in result]
    finally:
        if should_close:
            connection.close()


def get_row_group_metadata(parquet_file: str, con=None) -> list[dict]:
    """
    Get per-row-group statistics using parquet_metadata().

    Returns list of dicts with row_group_id, path_in_schema, stats_min, stats_max, etc.
    """
    safe_url = _safe_url(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        result = connection.execute(f"""
            SELECT * FROM parquet_metadata('{safe_url}')
        """).fetchall()

        columns = [desc[0] for desc in connection.description]
        return [dict(zip(columns, row, strict=True)) for row in result]
    finally:
        if should_close:
            connection.close()


def get_row_count(parquet_file: str, con=None) -> int:
    """Get total row count from file metadata."""
    metadata = get_file_metadata(parquet_file, con)
    return metadata.get("num_rows", 0)


def get_num_row_groups(parquet_file: str, con=None) -> int:
    """Get number of row groups from file metadata."""
    metadata = get_file_metadata(parquet_file, con)
    return metadata.get("num_row_groups", 0)


def parse_geometry_logical_type(logical_type: str) -> dict | None:
    """
    Parse Geometry/Geography logical type string from DuckDB schema.

    Handles strings like:
    - GeometryType(crs={"$schema": "...", "id": {"authority": "EPSG", "code": 4326}})
    - GeographyType(algorithm=spherical)
    - GeometryType(crs=<null>)

    Returns dict with keys: geo_type, geometry_type, coordinate_dimension, crs, algorithm
    """
    if not logical_type:
        return None

    # Match GeometryType(...) or GeographyType(...) - DuckDB's format from parquet_schema()
    match = re.match(r"(Geometry|Geography)Type\((.*)\)$", logical_type, re.DOTALL)
    if not match:
        return None

    geo_type = match.group(1)
    params = match.group(2)

    result: dict[str, Any] = {"geo_type": geo_type}

    # Parse CRS if present (handle nested JSON with brace counting)
    # DuckDB returns:
    # - crs=<null> for files without CRS
    # - crs={...} for inline PROJJSON
    # - crs=projjson:key_name for reference to metadata field
    crs_start = params.find("crs=")
    if crs_start != -1:
        crs_start += 4  # Skip "crs="
        crs_value = params[crs_start:]

        # Check for <null> - DuckDB's way of indicating no CRS
        if crs_value.startswith("<null>"):
            pass  # No CRS specified, leave result without "crs" key
        elif crs_value.startswith("projjson:") or crs_value.startswith("srid:"):
            # Reference to metadata field: projjson:key_name
            # Or SRID reference: srid:XXXX (interpreted as EPSG:XXXX)
            # Extract the reference (up to next comma or end of string)
            end_pos = crs_value.find(",")
            if end_pos == -1:
                end_pos = crs_value.find(")")
            if end_pos == -1:
                end_pos = len(crs_value)
            # Store the full reference string for later resolution
            result["crs"] = crs_value[:end_pos].strip()
        elif crs_value.startswith("{"):
            # Find matching closing brace for inline PROJJSON
            brace_count = 0
            end_pos = 0
            for i, char in enumerate(crs_value):
                if char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = i + 1
                        break

            if end_pos > 0:
                crs_json_str = crs_value[:end_pos]
                try:
                    result["crs"] = json.loads(crs_json_str)
                except json.JSONDecodeError:
                    result["crs"] = crs_json_str

    # Parse algorithm for Geography
    algo_match = re.search(r"algorithm=(planar|spherical)", params)
    if algo_match:
        result["algorithm"] = algo_match.group(1)

    # Parse geometry type and coordinate dimension from positional params
    # Split by comma but respect braces
    parts = []
    current_part: list[str] = []
    brace_depth = 0
    for char in params:
        if char == "{":
            brace_depth += 1
            current_part.append(char)
        elif char == "}":
            brace_depth -= 1
            current_part.append(char)
        elif char == "," and brace_depth == 0:
            parts.append("".join(current_part).strip())
            current_part = []
        else:
            current_part.append(char)
    if current_part:
        parts.append("".join(current_part).strip())

    # First positional param is geometry type
    valid_geom_types = [
        "Point",
        "LineString",
        "Polygon",
        "MultiPoint",
        "MultiLineString",
        "MultiPolygon",
        "GeometryCollection",
    ]
    if len(parts) >= 1:
        geom_type = parts[0].strip()
        if geom_type in valid_geom_types:
            result["geometry_type"] = geom_type

    # Second positional param is coordinate dimension
    valid_coord_dims = ["XY", "XYZ", "XYM", "XYZM"]
    if len(parts) >= 2:
        coord_dim = parts[1].strip()
        if coord_dim in valid_coord_dims:
            result["coordinate_dimension"] = coord_dim

    return result


def is_geometry_column(logical_type: str) -> bool:
    """Check if logical_type indicates a geometry column."""
    if not logical_type:
        return False
    # DuckDB returns GeometryType(...) and GeographyType(...) from parquet_schema()
    return logical_type.startswith("GeometryType(") or logical_type.startswith("GeographyType(")


def resolve_crs_reference(parquet_file: str, crs_value: Any) -> Any:
    """
    Resolve a CRS value, looking up references from file metadata if needed.

    The Parquet geo spec allows CRS to be specified as:
    - Inline PROJJSON (dict)
    - Reference to metadata field: "projjson:key_name"
    - SRID reference: "srid:XXXX" (interpreted as EPSG:XXXX)

    Args:
        parquet_file: Path to the parquet file
        crs_value: The CRS value from parse_geometry_logical_type, either:
            - A dict (already resolved PROJJSON)
            - A string like "projjson:key_name" (reference to metadata field)
            - A string like "srid:5070" (interpreted as EPSG:5070)
            - None

    Returns:
        Resolved CRS as dict (PROJJSON) or None if not found/applicable
    """
    if crs_value is None:
        return None

    # If already a dict (inline PROJJSON), return as-is
    if isinstance(crs_value, dict):
        return crs_value

    # Handle projjson:key_name reference to file metadata
    if isinstance(crs_value, str) and crs_value.startswith("projjson:"):
        key_name = crs_value[9:]  # Skip "projjson:"
        try:
            import pyarrow.parquet as pq

            pf = pq.ParquetFile(parquet_file)
            file_metadata = pf.metadata.metadata
            if file_metadata:
                # Keys are bytes in PyArrow metadata
                key_bytes = key_name.encode("utf-8")
                if key_bytes in file_metadata:
                    projjson_str = file_metadata[key_bytes].decode("utf-8")
                    return json.loads(projjson_str)
        except Exception:
            pass  # Fall through to return the reference string
        return crs_value  # Return the reference string if resolution failed

    # Handle srid:XXXX format - convert to PROJJSON using pyproj
    if isinstance(crs_value, str) and crs_value.startswith("srid:"):
        srid = crs_value[5:]  # Skip "srid:"
        try:
            from pyproj import CRS

            crs = CRS.from_authority("EPSG", int(srid))
            return crs.to_json_dict()
        except Exception:
            pass  # Fall through to return the original value
        return crs_value  # Return the srid string if resolution failed

    # Return as-is for other string values
    return crs_value


def detect_geometry_columns(parquet_file: str, con=None) -> dict[str, str]:
    """
    Detect GEOMETRY/GEOGRAPHY logical types from schema.

    Returns dict mapping column name to geo type ('Geometry' or 'Geography').
    """
    schema = get_schema_info(parquet_file, con)
    geo_columns = {}

    for col in schema:
        logical_type = col.get("logical_type") or ""
        name = col.get("name", "")

        if not name:
            continue

        # DuckDB returns GeometryType(...) and GeographyType(...) from parquet_schema()
        if logical_type.startswith("GeometryType("):
            geo_columns[name] = "Geometry"
        elif logical_type.startswith("GeographyType("):
            geo_columns[name] = "Geography"

    return geo_columns


def get_bbox_from_row_group_stats(
    parquet_file: str, bbox_column: str = "bbox", con=None
) -> list[float] | None:
    """
    Extract overall bbox from row group statistics.

    Queries parquet_metadata() for bbox columns (format: 'bbox, xmin' etc).
    Returns [xmin, ymin, xmax, ymax] or None if not available.
    """
    safe_url = _safe_url(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        # DuckDB uses 'bbox, xmin' format for path_in_schema (comma-space)
        # Use FILTER to aggregate stats for each bbox component
        # Use TRY_CAST to handle non-numeric stats gracefully
        result = connection.execute(f"""
            SELECT
                MIN(TRY_CAST(stats_min AS DOUBLE))
                    FILTER (WHERE path_in_schema = '{bbox_column}, xmin') as xmin,
                MIN(TRY_CAST(stats_min AS DOUBLE))
                    FILTER (WHERE path_in_schema = '{bbox_column}, ymin') as ymin,
                MAX(TRY_CAST(stats_max AS DOUBLE))
                    FILTER (WHERE path_in_schema = '{bbox_column}, xmax') as xmax,
                MAX(TRY_CAST(stats_max AS DOUBLE))
                    FILTER (WHERE path_in_schema = '{bbox_column}, ymax') as ymax
            FROM parquet_metadata('{safe_url}')
        """).fetchone()

        if result and all(v is not None for v in result):
            return [float(v) for v in result]
        return None
    finally:
        if should_close:
            connection.close()


def get_per_row_group_bbox_stats(
    parquet_file: str, bbox_column: str = "bbox", con=None
) -> list[dict]:
    """
    Get bbox statistics per row group.

    Returns list of dicts with row_group_id, xmin, ymin, xmax, ymax.
    """
    safe_url = _safe_url(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        # DuckDB uses 'bbox, xmin' format for path_in_schema (comma-space)
        # Use TRY_CAST to handle non-numeric stats gracefully
        result = connection.execute(f"""
            SELECT
                row_group_id,
                MIN(TRY_CAST(stats_min AS DOUBLE))
                    FILTER (WHERE path_in_schema = '{bbox_column}, xmin') as xmin,
                MIN(TRY_CAST(stats_min AS DOUBLE))
                    FILTER (WHERE path_in_schema = '{bbox_column}, ymin') as ymin,
                MAX(TRY_CAST(stats_max AS DOUBLE))
                    FILTER (WHERE path_in_schema = '{bbox_column}, xmax') as xmax,
                MAX(TRY_CAST(stats_max AS DOUBLE))
                    FILTER (WHERE path_in_schema = '{bbox_column}, ymax') as ymax
            FROM parquet_metadata('{safe_url}')
            GROUP BY row_group_id
            ORDER BY row_group_id
        """).fetchall()

        return [
            {
                "row_group_id": row[0],
                "xmin": row[1],
                "ymin": row[2],
                "xmax": row[3],
                "ymax": row[4],
            }
            for row in result
            if all(v is not None for v in row[1:])
        ]
    finally:
        if should_close:
            connection.close()


def get_compression_info(parquet_file: str, column_name: str | None = None, con=None) -> dict:
    """
    Get compression information for columns.

    Returns dict mapping column path to compression algorithm.
    """
    safe_url = _safe_url(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        query = f"""
            SELECT DISTINCT path_in_schema, compression
            FROM parquet_metadata('{safe_url}')
        """
        if column_name:
            query += f" WHERE path_in_schema = '{column_name}'"

        result = connection.execute(query).fetchall()
        return {row[0]: row[1] for row in result}
    finally:
        if should_close:
            connection.close()


def get_row_group_stats_summary(parquet_file: str, con=None) -> dict:
    """
    Get summary statistics about row groups.

    Returns dict with num_groups, total_rows, avg_rows_per_group, total_size, avg_group_size.
    """
    safe_url = _safe_url(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        # Get file-level info
        file_meta = get_file_metadata(parquet_file, connection)
        num_groups = file_meta.get("num_row_groups", 0)
        total_rows = file_meta.get("num_rows", 0)

        # Get total size from row groups
        result = connection.execute(f"""
            SELECT
                SUM(total_compressed_size) as total_size
            FROM parquet_metadata('{safe_url}')
        """).fetchone()

        total_size = result[0] if result and result[0] else 0

        return {
            "num_groups": num_groups,
            "total_rows": total_rows,
            "avg_rows_per_group": total_rows / num_groups if num_groups > 0 else 0,
            "total_size": total_size,
            "avg_group_size": total_size / num_groups if num_groups > 0 else 0,
        }
    finally:
        if should_close:
            connection.close()


def get_column_stats(parquet_file: str, column_name: str, con=None) -> list[dict]:
    """
    Get per-row-group statistics for a specific column.

    Returns list of dicts with row_group_id, stats_min, stats_max, stats_null_count.
    """
    safe_url = _safe_url(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        result = connection.execute(f"""
            SELECT
                row_group_id,
                stats_min,
                stats_max,
                stats_null_count
            FROM parquet_metadata('{safe_url}')
            WHERE path_in_schema = '{column_name}'
            ORDER BY row_group_id
        """).fetchall()

        return [
            {
                "row_group_id": row[0],
                "stats_min": row[1],
                "stats_max": row[2],
                "stats_null_count": row[3],
            }
            for row in result
        ]
    finally:
        if should_close:
            connection.close()


def has_bbox_column(parquet_file: str, con=None) -> tuple[bool, str | None]:
    """
    Check if file has a bbox struct column with proper structure.

    Returns (has_bbox, bbox_column_name).

    Note:
        DuckDB's parquet_schema() returns nested struct fields without parent prefix.
        For a struct column 'bbox' with fields xmin/ymin/xmax/ymax:
        - bbox appears with num_children=4
        - Child fields appear as 'xmin', 'ymin', 'xmax', 'ymax' (not 'bbox.xmin')
    """
    schema = get_schema_info(parquet_file, con)

    # Check for columns ending with these suffixes (e.g., geometry_bbox, bbox)
    conventional_suffixes = ["bbox", "bounds", "extent"]
    required_fields = {"xmin", "ymin", "xmax", "ymax"}

    for i, col in enumerate(schema):
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
                if i + j < len(schema):
                    child_name = schema[i + j].get("name", "")
                    child_names.add(child_name)

            # Check if all required fields are present
            if required_fields.issubset(child_names):
                return True, name

    return False, None


def find_primary_geometry_column_duckdb(parquet_file: str, con=None) -> str:
    """
    Find primary geometry column from GeoParquet metadata.

    Falls back to 'geometry' if no metadata found.
    """
    geo_meta = get_geo_metadata(parquet_file, con)

    if not geo_meta:
        return "geometry"

    if isinstance(geo_meta, dict):
        return geo_meta.get("primary_column", "geometry")
    elif isinstance(geo_meta, list):
        for col in geo_meta:
            if isinstance(col, dict) and col.get("primary", False):
                return col.get("name", "geometry")

    return "geometry"


def get_native_geo_statistics(parquet_file: str, geometry_column: str, con=None) -> dict | None:
    """
    Get native Parquet GeospatialStatistics for a geometry column.

    This retrieves the geo_bbox and geo_types from the Parquet column metadata,
    which is part of the native Parquet geospatial support (not GeoParquet metadata).

    Args:
        parquet_file: Path to the parquet file
        geometry_column: Name of the geometry column
        con: Optional existing DuckDB connection

    Returns:
        dict with 'bbox' (list of 4 floats) and 'geometry_types' (list of strings),
        or None if not available
    """
    safe_url = _safe_url(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        result = connection.execute(f"""
            SELECT geo_bbox, geo_types
            FROM parquet_metadata('{safe_url}')
            WHERE path_in_schema = '{geometry_column}'
            LIMIT 1
        """).fetchone()

        if result is None:
            return None

        geo_bbox, geo_types = result

        # Build the result dict
        stats = {}

        # Process geo_bbox - aggregate across all row groups
        if geo_bbox and geo_bbox.get("xmin") is not None:
            # For single row group, just use the values
            stats["bbox"] = [
                geo_bbox["xmin"],
                geo_bbox["ymin"],
                geo_bbox["xmax"],
                geo_bbox["ymax"],
            ]

            # Include Z bounds if present
            if geo_bbox.get("zmin") is not None:
                stats["bbox"].extend([geo_bbox["zmin"], geo_bbox["zmax"]])

        # Process geo_types - format nicely
        if geo_types:
            stats["geometry_types"] = _format_geo_types(geo_types)

        return stats if stats else None

    except Exception:
        return None
    finally:
        if should_close:
            connection.close()


def get_aggregated_native_geo_stats(parquet_file: str, geometry_column: str, con=None) -> dict:
    """
    Get aggregated native Parquet GeospatialStatistics across all row groups.

    Returns overall bbox and combined geometry types for the entire file.

    Args:
        parquet_file: Path to the parquet file
        geometry_column: Name of the geometry column
        con: Optional existing DuckDB connection

    Returns:
        dict with 'bbox' (list of 4+ floats) and 'geometry_types' (list of strings)
    """
    safe_url = _safe_url(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        # Aggregate bbox across all row groups
        result = connection.execute(f"""
            SELECT
                MIN(geo_bbox.xmin) as xmin,
                MIN(geo_bbox.ymin) as ymin,
                MAX(geo_bbox.xmax) as xmax,
                MAX(geo_bbox.ymax) as ymax,
                MIN(geo_bbox.zmin) as zmin,
                MAX(geo_bbox.zmax) as zmax
            FROM parquet_metadata('{safe_url}')
            WHERE path_in_schema = '{geometry_column}'
              AND geo_bbox IS NOT NULL
        """).fetchone()

        stats = {}

        if result and result[0] is not None:
            xmin, ymin, xmax, ymax, zmin, zmax = result
            stats["bbox"] = [xmin, ymin, xmax, ymax]
            if zmin is not None:
                stats["bbox"].extend([zmin, zmax])

        # Get unique geometry types across all row groups
        types_result = connection.execute(f"""
            SELECT DISTINCT unnest(geo_types) as geo_type
            FROM parquet_metadata('{safe_url}')
            WHERE path_in_schema = '{geometry_column}'
              AND geo_types IS NOT NULL
        """).fetchall()

        if types_result:
            raw_types = [row[0] for row in types_result if row[0]]
            stats["geometry_types"] = _format_geo_types(raw_types)

        return stats

    except Exception:
        return {}
    finally:
        if should_close:
            connection.close()


def _format_geo_types(geo_types: list) -> list[str]:
    """
    Format geo_types from parquet_metadata into human-readable strings.

    DuckDB may return strings like 'polygon' or WKB integer codes.
    This normalizes them to standard names like 'Polygon', 'Point Z', etc.
    """
    # WKB type code to name mapping (from Parquet geo spec)
    WKB_TYPE_CODES = {
        # XY
        1: "Point",
        2: "LineString",
        3: "Polygon",
        4: "MultiPoint",
        5: "MultiLineString",
        6: "MultiPolygon",
        7: "GeometryCollection",
        # XYZ (add 1000)
        1001: "Point Z",
        1002: "LineString Z",
        1003: "Polygon Z",
        1004: "MultiPoint Z",
        1005: "MultiLineString Z",
        1006: "MultiPolygon Z",
        1007: "GeometryCollection Z",
        # XYM (add 2000)
        2001: "Point M",
        2002: "LineString M",
        2003: "Polygon M",
        2004: "MultiPoint M",
        2005: "MultiLineString M",
        2006: "MultiPolygon M",
        2007: "GeometryCollection M",
        # XYZM (add 3000)
        3001: "Point ZM",
        3002: "LineString ZM",
        3003: "Polygon ZM",
        3004: "MultiPoint ZM",
        3005: "MultiLineString ZM",
        3006: "MultiPolygon ZM",
        3007: "GeometryCollection ZM",
    }

    formatted = []
    for t in geo_types:
        if isinstance(t, int):
            # WKB integer code
            formatted.append(WKB_TYPE_CODES.get(t, f"Unknown({t})"))
        elif isinstance(t, str):
            # DuckDB returns lowercase strings like 'polygon'
            # Capitalize properly
            t_lower = t.lower()
            if t_lower == "point":
                formatted.append("Point")
            elif t_lower == "linestring":
                formatted.append("LineString")
            elif t_lower == "polygon":
                formatted.append("Polygon")
            elif t_lower == "multipoint":
                formatted.append("MultiPoint")
            elif t_lower == "multilinestring":
                formatted.append("MultiLineString")
            elif t_lower == "multipolygon":
                formatted.append("MultiPolygon")
            elif t_lower == "geometrycollection":
                formatted.append("GeometryCollection")
            elif t_lower.endswith(" zm") or t_lower.endswith("zm"):
                # Handle ZM variants (check first - most specific)
                if t_lower.endswith(" zm"):
                    base = t_lower[:-3]  # Strip " zm"
                else:
                    base = t_lower[:-2]  # Strip "zm"
                formatted.append(_capitalize_geom_type(base) + " ZM")
            elif t_lower.endswith(" z") or t_lower.endswith("z"):
                # Handle Z variants
                if t_lower.endswith(" z"):
                    base = t_lower[:-2]  # Strip " z"
                else:
                    base = t_lower[:-1]  # Strip "z"
                formatted.append(_capitalize_geom_type(base) + " Z")
            elif t_lower.endswith(" m") or t_lower.endswith("m"):
                # Handle M variants
                if t_lower.endswith(" m"):
                    base = t_lower[:-2]  # Strip " m"
                else:
                    base = t_lower[:-1]  # Strip "m"
                formatted.append(_capitalize_geom_type(base) + " M")
            else:
                # Fallback: just capitalize
                formatted.append(t.title())
        else:
            formatted.append(str(t))

    return sorted(set(formatted))


def _capitalize_geom_type(name: str) -> str:
    """Capitalize geometry type name properly."""
    name_map = {
        "point": "Point",
        "linestring": "LineString",
        "polygon": "Polygon",
        "multipoint": "MultiPoint",
        "multilinestring": "MultiLineString",
        "multipolygon": "MultiPolygon",
        "geometrycollection": "GeometryCollection",
    }
    return name_map.get(name.lower().strip(), name.title())
