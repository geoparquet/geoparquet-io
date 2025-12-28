"""
Utilities for inspecting GeoParquet files.

Provides functions to extract metadata, preview data, calculate statistics,
and format output for terminal, JSON, and Markdown.
"""

import json
import os
import struct
from typing import Any

import duckdb
import pyarrow as pa
from rich.console import Console
from rich.table import Table
from rich.text import Text

from geoparquet_io.core.common import (
    format_size,
    is_remote_url,
    safe_file_url,
)
from geoparquet_io.core.metadata_utils import (
    extract_bbox_from_row_group_stats,
)


def extract_file_info(parquet_file: str) -> dict[str, Any]:
    """
    Extract basic file information from a Parquet file.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        dict: File info including size, rows, row_groups, compression
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_compression_info,
        get_file_metadata,
    )

    # Get file metadata using DuckDB
    file_meta = get_file_metadata(parquet_file)
    num_rows = file_meta.get("num_rows", 0)
    num_row_groups = file_meta.get("num_row_groups", 0)

    # Get compression from first column
    compression_info = get_compression_info(parquet_file)
    compression = None
    if compression_info:
        # Get compression from first column (any column will do)
        compression = next(iter(compression_info.values()), None)

    # Get file size - handle both local and remote files
    if is_remote_url(parquet_file):
        # For remote files, approximate from metadata
        size_bytes = None
        size_human = "N/A (remote)"
    else:
        size_bytes = os.path.getsize(parquet_file)
        size_human = format_size(size_bytes)

    return {
        "file_path": parquet_file,
        "size_bytes": size_bytes,
        "size_human": size_human,
        "rows": num_rows,
        "row_groups": num_row_groups,
        "compression": compression,
    }


def _extract_crs_string(crs_info: Any) -> str | None:
    """Extract CRS string from various formats."""
    if isinstance(crs_info, dict):
        if "id" in crs_info:
            crs_id = crs_info["id"]
            if isinstance(crs_id, dict):
                authority = crs_id.get("authority", "EPSG")
                code = crs_id.get("code")
                if code:
                    return f"{authority}:{code}"
            else:
                return str(crs_id)
        elif "$schema" in crs_info:
            return "PROJJSON"
        elif "wkt" in crs_info:
            return "WKT"
    elif crs_info:
        return str(crs_info)
    return None


def _format_crs_for_display(crs_info: Any, include_default: bool = True) -> str:
    """
    Format CRS for display output.

    Converts any CRS format (PROJJSON dict, EPSG string, None) to a
    consistent display string like "EPSG:31287" or "OGC:CRS84 (default)".

    Args:
        crs_info: CRS in any format (PROJJSON dict, EPSG string, None)
        include_default: Whether to show "(default)" for None CRS

    Returns:
        Display string like "EPSG:31287" or "OGC:CRS84 (default)"
    """
    if crs_info is None:
        return "OGC:CRS84 (default)" if include_default else "Not specified"

    # Try to extract EPSG code from PROJJSON
    identifier = _extract_crs_identifier(crs_info)
    if identifier:
        authority, code = identifier
        return f"{authority}:{code}"

    # Fallback to existing extraction
    result = _extract_crs_string(crs_info)
    if result:
        return result

    # Last resort - truncate if too long
    crs_str = str(crs_info)
    return crs_str[:50] + "..." if len(crs_str) > 50 else crs_str


def _extract_crs_identifier(crs_info: Any) -> tuple[str, int] | None:
    """
    Extract normalized CRS identifier (authority, code) from various formats.

    Handles:
    - PROJJSON dicts with id.authority and id.code
    - Strings like "EPSG:31287", "epsg:31287"
    - URN format like "urn:ogc:def:crs:EPSG::31287"

    Returns:
        tuple of (authority, code) like ("EPSG", 31287), or None if not extractable
    """
    if isinstance(crs_info, dict):
        # PROJJSON format - look for id.authority and id.code
        if "id" in crs_info:
            crs_id = crs_info["id"]
            if isinstance(crs_id, dict):
                authority = crs_id.get("authority", "").upper()
                code = crs_id.get("code")
                if authority and code:
                    return (authority, int(code))
        return None

    if isinstance(crs_info, str):
        crs_str = crs_info.strip().upper()

        # Handle "EPSG:31287" format
        if ":" in crs_str and not crs_str.startswith("URN:"):
            parts = crs_str.split(":")
            if len(parts) == 2:
                try:
                    return (parts[0], int(parts[1]))
                except ValueError:
                    pass

        # Handle URN format "urn:ogc:def:crs:EPSG::31287"
        if crs_str.startswith("URN:OGC:DEF:CRS:"):
            parts = crs_str.split(":")
            if len(parts) >= 7:
                authority = parts[4]
                try:
                    code = int(parts[-1])
                    return (authority, code)
                except ValueError:
                    pass

    return None


def _crs_are_equivalent(crs1: Any, crs2: Any) -> bool:
    """
    Check if two CRS values are equivalent.

    Compares by extracting authority and code from both values.
    Handles PROJJSON dicts, "EPSG:31287" strings, and URN formats.

    Returns:
        True if CRS values represent the same coordinate system
    """
    id1 = _extract_crs_identifier(crs1)
    id2 = _extract_crs_identifier(crs2)

    if id1 is None or id2 is None:
        return False

    return id1 == id2


def _detect_metadata_mismatches(
    parquet_geo_info: dict[str, Any],
    geoparquet_info: dict[str, Any],
) -> list[str]:
    """
    Detect mismatches between Parquet native geo metadata and GeoParquet metadata.

    Returns a list of warning messages for any mismatches found.
    """
    warnings = []

    parquet_crs = parquet_geo_info.get("crs")
    geoparquet_crs = geoparquet_info.get("crs")

    # Compare CRS - only warn if both are set and different
    if parquet_crs and geoparquet_crs:
        # Use semantic comparison (handles PROJJSON vs "EPSG:31287" etc.)
        if not _crs_are_equivalent(parquet_crs, geoparquet_crs):
            # Extract display strings for the warning message
            parquet_crs_display = _extract_crs_string(parquet_crs) or str(parquet_crs)
            geoparquet_crs_display = _extract_crs_string(geoparquet_crs) or str(geoparquet_crs)
            warnings.append(
                f"CRS mismatch: Parquet geo type has '{parquet_crs_display}' "
                f"but GeoParquet metadata has '{geoparquet_crs_display}'"
            )
    elif parquet_crs and not geoparquet_crs:
        parquet_crs_display = _extract_crs_string(parquet_crs) or str(parquet_crs)
        warnings.append(
            f"CRS in Parquet geo type ('{parquet_crs_display}') but missing in GeoParquet metadata"
        )
    elif geoparquet_crs and not parquet_crs:
        # GeoParquet has CRS but Parquet type doesn't - might be expected
        pass

    # Compare edges (only relevant for Geography type)
    parquet_edges = parquet_geo_info.get("edges")
    geoparquet_edges = geoparquet_info.get("edges")

    if parquet_edges and geoparquet_edges:
        if parquet_edges.lower() != geoparquet_edges.lower():
            warnings.append(
                f"Edges mismatch: Parquet geo type has '{parquet_edges}' "
                f"but GeoParquet metadata has '{geoparquet_edges}'"
            )

    # Compare geometry types
    parquet_geom_type = parquet_geo_info.get("geometry_type")
    geoparquet_geom_types = geoparquet_info.get("geometry_types")

    if parquet_geom_type and geoparquet_geom_types:
        if isinstance(geoparquet_geom_types, list):
            geom_types_lower = [g.lower() for g in geoparquet_geom_types]
            if parquet_geom_type.lower() not in geom_types_lower:
                warnings.append(
                    f"Geometry type mismatch: Parquet geo type restricts to '{parquet_geom_type}' "
                    f"but GeoParquet metadata allows {geoparquet_geom_types}"
                )

    return warnings


def extract_geo_info(parquet_file: str) -> dict[str, Any]:
    """
    Extract geospatial information from both Parquet native types and GeoParquet metadata.

    This function detects:
    1. Native Parquet GEOMETRY/GEOGRAPHY logical types
    2. GeoParquet metadata from the 'geo' key
    3. Mismatches between the two (returned as warnings)

    Args:
        parquet_file: Path to the parquet file

    Returns:
        dict: Geo info including:
            - parquet_type: "Geometry", "Geography", or "No Parquet geo logical type"
            - has_geo_metadata: Whether GeoParquet metadata exists
            - version: GeoParquet version
            - crs: CRS (from GeoParquet or Parquet type, with source noted)
            - bbox: Bounding box
            - primary_column: Primary geometry column name
            - geometry_types: List of geometry types (from GeoParquet metadata)
            - edges: Edge interpretation (for Geography type)
            - warnings: List of mismatch warnings (if any)
    """
    from geoparquet_io.core.duckdb_metadata import (
        detect_geometry_columns,
        get_aggregated_native_geo_stats,
        get_geo_metadata,
        get_schema_info,
        parse_geometry_logical_type,
        resolve_crs_reference,
    )

    # Get metadata using DuckDB
    geo_meta = get_geo_metadata(parquet_file)
    schema_info = get_schema_info(parquet_file)
    geo_columns = detect_geometry_columns(parquet_file)

    # Detect Parquet native geo type
    parquet_type = "No Parquet geo logical type"
    parquet_geo_info = {}
    geometry_column = None

    # Find geometry column and parse its logical type
    for col in schema_info:
        col_name = col.get("name", "")
        if col_name in geo_columns:
            parquet_type = geo_columns[col_name]
            geometry_column = col_name

            # Parse additional details from logical type
            logical_type = col.get("logical_type", "")
            if logical_type:
                geom_details = parse_geometry_logical_type(logical_type)
                if geom_details:
                    parquet_geo_info["geometry_type"] = geom_details.get("geometry_type")
                    parquet_geo_info["coordinate_dimension"] = geom_details.get(
                        "coordinate_dimension"
                    )
                    # Resolve CRS reference if needed (e.g., "projjson:key_name")
                    raw_crs = geom_details.get("crs")
                    parquet_geo_info["crs"] = resolve_crs_reference(parquet_file, raw_crs)
                    parquet_geo_info["edges"] = geom_details.get("algorithm")
            break

    # Extract GeoParquet metadata
    geoparquet_info = {}
    if geo_meta:
        version = geo_meta.get("version")
        primary_column = geo_meta.get("primary_column", "geometry")
        columns_meta = geo_meta.get("columns", {})

        crs = None
        bbox = None
        geometry_types = None
        edges = None

        if primary_column in columns_meta:
            col_meta = columns_meta[primary_column]
            crs = col_meta.get("crs")  # Keep raw PROJJSON, don't convert to string
            bbox = col_meta.get("bbox")
            geometry_types = col_meta.get("geometry_types")
            edges = col_meta.get("edges")

        # Note: Keep crs as None if not specified - default handling is a display concern

        geoparquet_info = {
            "version": version,
            "crs": crs,
            "bbox": bbox,
            "primary_column": primary_column,
            "geometry_types": geometry_types,
            "edges": edges,
        }

        # Use GeoParquet primary_column if we didn't find one from Parquet type
        if not geometry_column:
            geometry_column = primary_column

    # Determine the effective primary column
    primary_column = geometry_column or "geometry"

    # Determine effective CRS (prefer GeoParquet, fallback to Parquet type)
    # Keep as raw PROJJSON - display functions will handle default formatting
    effective_crs = geoparquet_info.get("crs") or parquet_geo_info.get("crs")

    # Determine effective bbox and geometry_types
    # Priority: GeoParquet metadata, then native geo stats, then bbox column stats
    effective_bbox = geoparquet_info.get("bbox")
    effective_geometry_types = geoparquet_info.get("geometry_types")

    # Try native Parquet GeospatialStatistics for files with native geo types
    if parquet_type != "No Parquet geo logical type":
        native_stats = get_aggregated_native_geo_stats(parquet_file, primary_column)

        # Use native bbox if GeoParquet bbox not available
        if not effective_bbox and native_stats.get("bbox"):
            effective_bbox = native_stats["bbox"]

        # Use native geometry_types if GeoParquet geometry_types not available
        if not effective_geometry_types and native_stats.get("geometry_types"):
            effective_geometry_types = native_stats["geometry_types"]

    # Last resort: try bbox struct column stats (for GeoParquet 1.x with covering)
    if not effective_bbox:
        effective_bbox = extract_bbox_from_row_group_stats(parquet_file, primary_column)

    # Detect mismatches
    warnings = []
    if geo_meta and parquet_type != "No Parquet geo logical type":
        warnings = _detect_metadata_mismatches(parquet_geo_info, geoparquet_info)

    return {
        "parquet_type": parquet_type,
        "has_geo_metadata": geo_meta is not None,
        "version": geoparquet_info.get("version"),
        "crs": effective_crs,
        "bbox": effective_bbox,
        "primary_column": primary_column,
        "geometry_types": effective_geometry_types,
        "edges": geoparquet_info.get("edges") or parquet_geo_info.get("edges"),
        "warnings": warnings,
    }


def extract_columns_info(schema: pa.Schema, primary_geom_col: str | None) -> list[dict[str, Any]]:
    """
    Extract column information from schema.

    Args:
        schema: PyArrow schema
        primary_geom_col: Name of primary geometry column (if known)

    Returns:
        list: Column info dicts with name, type, is_geometry
    """
    columns = []
    for field in schema:
        is_geometry = field.name == primary_geom_col
        columns.append(
            {
                "name": field.name,
                "type": str(field.type),
                "is_geometry": is_geometry,
            }
        )
    return columns


def parse_wkb_type(wkb_bytes: bytes) -> str:
    """
    Parse WKB bytes to extract geometry type.

    Args:
        wkb_bytes: WKB binary data

    Returns:
        str: Geometry type name (POINT, LINESTRING, POLYGON, etc.)
    """
    if not wkb_bytes or len(wkb_bytes) < 5:
        return "GEOMETRY"

    try:
        # WKB format: byte_order (1 byte) + geometry_type (4 bytes) + ...
        byte_order = wkb_bytes[0]

        # Determine endianness
        if byte_order == 0:  # Big endian
            geom_type = struct.unpack(">I", wkb_bytes[1:5])[0]
        else:  # Little endian
            geom_type = struct.unpack("<I", wkb_bytes[1:5])[0]

        # Base type (ignore Z, M, ZM flags)
        base_type = geom_type % 1000

        type_map = {
            1: "POINT",
            2: "LINESTRING",
            3: "POLYGON",
            4: "MULTIPOINT",
            5: "MULTILINESTRING",
            6: "MULTIPOLYGON",
            7: "GEOMETRYCOLLECTION",
        }

        return type_map.get(base_type, "GEOMETRY")
    except (struct.error, IndexError):
        return "GEOMETRY"


def wkb_to_wkt_preview(wkb_bytes: bytes, max_length: int = 60) -> str:
    """
    Convert WKB bytes to WKT and truncate for preview display.

    Handles both standard ISO WKB format and DuckDB's internal GEOMETRY format.

    Args:
        wkb_bytes: WKB binary data (ISO WKB or DuckDB GEOMETRY format)
        max_length: Maximum length of WKT string to return

    Returns:
        str: Truncated WKT string or fallback geometry type
    """
    if not wkb_bytes or len(wkb_bytes) < 5:
        return "<GEOMETRY>"

    try:
        with duckdb.connect() as con:
            con.execute("LOAD spatial;")

            # Check if this is DuckDB's internal GEOMETRY format (starts with 0x02)
            # vs standard ISO WKB (starts with 0x00 or 0x01 for byte order)
            if wkb_bytes[0] == 0x02:
                # DuckDB internal GEOMETRY format - cast directly
                result = con.execute("SELECT ST_AsText(?::GEOMETRY)", [wkb_bytes]).fetchone()
            else:
                # Standard ISO WKB format
                result = con.execute(
                    "SELECT ST_AsText(ST_GeomFromWKB(?::BLOB))", [wkb_bytes]
                ).fetchone()

        if result and result[0]:
            wkt = result[0]
            if len(wkt) > max_length:
                return wkt[: max_length - 3] + "..."
            return wkt
        else:
            # Fall back to geometry type
            return f"<{parse_wkb_type(wkb_bytes)}>"
    except Exception:
        # Fall back to geometry type on any error
        return f"<{parse_wkb_type(wkb_bytes)}>"


def format_geometry_display(value: Any, max_length: int = 60) -> str:
    """
    Format a geometry value for display.

    Args:
        value: Geometry value (WKB bytes or other)
        max_length: Maximum length for WKT preview

    Returns:
        str: Formatted geometry display string (WKT preview or fallback)
    """
    if value is None:
        return "NULL"

    if isinstance(value, bytes):
        return wkb_to_wkt_preview(value, max_length)

    return str(value)


def format_bbox_display(value: dict, max_length: int = 60) -> str:
    """
    Format a bbox struct value for display.

    Args:
        value: Dict with xmin, ymin, xmax, ymax keys
        max_length: Maximum length of output string

    Returns:
        str: Formatted bbox string like [xmin, ymin, xmax, ymax]
    """
    if not isinstance(value, dict):
        return str(value)
    try:
        xmin = value.get("xmin", 0)
        ymin = value.get("ymin", 0)
        xmax = value.get("xmax", 0)
        ymax = value.get("ymax", 0)
        formatted = f"[{xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f}]"
        if len(formatted) > max_length:
            return formatted[: max_length - 3] + "..."
        return formatted
    except (TypeError, ValueError, AttributeError):
        return str(value)


def is_bbox_value(value: Any) -> bool:
    """Check if a value is a bbox struct (dict with xmin, ymin, xmax, ymax)."""
    if not isinstance(value, dict):
        return False
    bbox_keys = {"xmin", "ymin", "xmax", "ymax"}
    return bbox_keys.issubset(value.keys())


def format_value_for_display(
    value: Any, column_type: str, is_geometry: bool, max_length: int = 60
) -> str:
    """
    Format a cell value for terminal display.

    Args:
        value: Cell value
        column_type: Column type string
        is_geometry: Whether this is a geometry column
        max_length: Maximum length for geometry WKT preview

    Returns:
        str: Formatted display string
    """
    if value is None:
        return "NULL"

    if is_geometry:
        return format_geometry_display(value, max_length)

    # Format bbox struct columns nicely
    if is_bbox_value(value):
        return format_bbox_display(value, max_length)

    # Truncate long strings
    value_str = str(value)
    if len(value_str) > 50:
        return value_str[:47] + "..."

    return value_str


def format_value_for_json(value: Any, is_geometry: bool) -> Any:
    """
    Format a cell value for JSON output.

    Args:
        value: Cell value
        is_geometry: Whether this is a geometry column

    Returns:
        JSON-serializable value
    """
    if value is None:
        return None

    if is_geometry:
        if isinstance(value, bytes):
            return format_geometry_display(value)
        return str(value)

    # Handle various types
    if isinstance(value, (int, float, str, bool)):
        return value

    # Convert other types to string
    return str(value)


def get_preview_data(
    parquet_file: str, head: int | None = None, tail: int | None = None
) -> tuple[pa.Table, str]:
    """
    Read preview data from a Parquet file.

    Geometry columns are automatically converted to WKT strings for display.

    Args:
        parquet_file: Path to the parquet file
        head: Number of rows from start (mutually exclusive with tail)
        tail: Number of rows from end (mutually exclusive with head)

    Returns:
        tuple: (PyArrow table with data, mode: "head" or "tail")
    """
    from geoparquet_io.core.common import get_duckdb_connection, needs_httpfs
    from geoparquet_io.core.duckdb_metadata import (
        detect_geometry_columns,
        get_geo_metadata,
        get_row_count,
    )

    safe_url = safe_file_url(parquet_file, verbose=False)
    total_rows = get_row_count(parquet_file)

    # Create DuckDB connection
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))

    try:
        # Detect geometry columns from native Parquet types
        geo_columns = set(detect_geometry_columns(parquet_file).keys())

        # Also detect geometry columns from GeoParquet metadata
        geo_meta = get_geo_metadata(parquet_file)
        if geo_meta:
            columns_meta = geo_meta.get("columns", {})
            geo_columns.update(columns_meta.keys())

        # Get all column names from the parquet file
        schema_result = con.execute(
            f"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{safe_url}'))"
        ).fetchall()
        all_columns = [row[0] for row in schema_result]

        # Build column list, converting geometry columns to WKT
        column_expressions = []
        for col in all_columns:
            # Escape double quotes in column names for SQL identifiers
            escaped_col = col.replace('"', '""')
            if col in geo_columns:
                # Convert geometry to WKT for display
                column_expressions.append(f'ST_AsText("{escaped_col}") AS "{escaped_col}"')
            else:
                column_expressions.append(f'"{escaped_col}"')

        select_clause = ", ".join(column_expressions)

        if tail:
            # Read from end
            start_row = max(0, total_rows - tail)
            num_rows = min(tail, total_rows)
            query = (
                f"SELECT {select_clause} FROM read_parquet('{safe_url}') "
                f"OFFSET {start_row} LIMIT {num_rows}"
            )
            mode = "tail"
        else:
            # Read from start (default if head is None, use 10)
            num_rows = head if head is not None else 10
            num_rows = min(num_rows, total_rows)
            query = f"SELECT {select_clause} FROM read_parquet('{safe_url}') LIMIT {num_rows}"
            mode = "head"

        # Execute query and convert to PyArrow table
        table = con.execute(query).fetch_arrow_table()
    finally:
        con.close()

    return table, mode


def get_column_statistics(
    parquet_file: str, columns_info: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """
    Calculate column statistics using DuckDB.

    Args:
        parquet_file: Path to the parquet file
        columns_info: Column information from extract_columns_info

    Returns:
        dict: Statistics per column
    """
    safe_url = safe_file_url(parquet_file, verbose=False)
    con = duckdb.connect()

    try:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")

        stats = {}

        for col in columns_info:
            col_name = col["name"]
            is_geometry = col["is_geometry"]

            # Build stats query based on column type
            if is_geometry:
                # For geometry columns, only count nulls
                query = f"""
                    SELECT
                        COUNT(*) FILTER (WHERE "{col_name}" IS NULL) as null_count
                    FROM '{safe_url}'
                """
                result = con.execute(query).fetchone()
                stats[col_name] = {
                    "nulls": result[0] if result else 0,
                    "min": None,
                    "max": None,
                    "unique": None,
                }
            else:
                # For non-geometry columns, get full stats
                query = f"""
                    SELECT
                        COUNT(*) FILTER (WHERE "{col_name}" IS NULL) as null_count,
                        MIN("{col_name}") as min_val,
                        MAX("{col_name}") as max_val,
                        APPROX_COUNT_DISTINCT("{col_name}") as unique_count
                    FROM '{safe_url}'
                """
                try:
                    result = con.execute(query).fetchone()
                    if result:
                        stats[col_name] = {
                            "nulls": result[0],
                            "min": result[1],
                            "max": result[2],
                            "unique": result[3],
                        }
                    else:
                        stats[col_name] = {
                            "nulls": 0,
                            "min": None,
                            "max": None,
                            "unique": None,
                        }
                except Exception:
                    # If stats fail for this column, provide basic info
                    stats[col_name] = {
                        "nulls": 0,
                        "min": None,
                        "max": None,
                        "unique": None,
                    }

        return stats

    finally:
        con.close()


def format_terminal_output(
    file_info: dict[str, Any],
    geo_info: dict[str, Any],
    columns_info: list[dict[str, Any]],
    preview_table: pa.Table | None = None,
    preview_mode: str | None = None,
    stats: dict[str, dict[str, Any]] | None = None,
) -> None:
    """
    Format and print terminal output using Rich.

    Args:
        file_info: File information dict
        geo_info: Geo information dict
        columns_info: Column information list
        preview_table: Optional preview data table
        preview_mode: "head" or "tail" (when preview_table is provided)
        stats: Optional statistics dict
    """
    console = Console()

    # File header
    file_name = os.path.basename(file_info["file_path"])
    console.print()
    console.print(f"📄 [bold]{file_name}[/bold] ({file_info['size_human']})")
    console.print("━" * 60)

    # Metadata section
    console.print(f"Rows: [cyan]{file_info['rows']:,}[/cyan]")
    console.print(f"Row Groups: [cyan]{file_info['row_groups']}[/cyan]")

    # Compression
    if file_info.get("compression"):
        console.print(f"Compression: [cyan]{file_info['compression']}[/cyan]")

    # Parquet type (new field)
    parquet_type = geo_info.get("parquet_type", "No Parquet geo logical type")
    if parquet_type in ("Geometry", "Geography"):
        console.print(f"Parquet Type: [cyan]{parquet_type}[/cyan]")
    else:
        console.print(f"Parquet Type: [dim]{parquet_type}[/dim]")

    if geo_info["has_geo_metadata"]:
        # GeoParquet version
        if geo_info.get("version"):
            console.print(f"GeoParquet Version: [cyan]{geo_info['version']}[/cyan]")

        crs_display = _format_crs_for_display(geo_info["crs"])
        console.print(f"CRS: [cyan]{crs_display}[/cyan]")

        # Geometry types (if available)
        if geo_info.get("geometry_types"):
            geom_types = ", ".join(geo_info["geometry_types"])
            console.print(f"Geometry Types: [cyan]{geom_types}[/cyan]")

        if geo_info["bbox"]:
            bbox = geo_info["bbox"]
            if len(bbox) == 4:
                console.print(
                    f"Bbox: [cyan][{bbox[0]:.6f}, {bbox[1]:.6f}, {bbox[2]:.6f}, {bbox[3]:.6f}][/cyan]"
                )
            else:
                console.print(f"Bbox: [cyan]{bbox}[/cyan]")
    elif parquet_type in ("Geometry", "Geography"):
        # Has Parquet geo type but no GeoParquet metadata
        console.print("[dim]No GeoParquet metadata (using Parquet geo type)[/dim]")
        crs_display = _format_crs_for_display(geo_info["crs"])
        console.print(f"CRS: [cyan]{crs_display}[/cyan]")

        # Geometry types from native Parquet geo stats
        if geo_info.get("geometry_types"):
            geom_types = ", ".join(geo_info["geometry_types"])
            console.print(f"Geometry Types: [cyan]{geom_types}[/cyan]")

        # Display bbox from native geo stats
        if geo_info["bbox"]:
            bbox = geo_info["bbox"]
            if len(bbox) == 4:
                console.print(
                    f"Bbox: [cyan][{bbox[0]:.6f}, {bbox[1]:.6f}, {bbox[2]:.6f}, {bbox[3]:.6f}][/cyan]"
                )
            else:
                console.print(f"Bbox: [cyan]{bbox}[/cyan]")
    else:
        console.print("[yellow]No GeoParquet metadata found[/yellow]")

    # Display warnings for metadata mismatches
    warnings = geo_info.get("warnings", [])
    if warnings:
        console.print()
        for warning in warnings:
            console.print(f"[yellow]⚠ {warning}[/yellow]")

    console.print()

    # Columns table
    num_cols = len(columns_info)
    console.print(f"Columns ({num_cols}):")

    table = Table(show_header=True, header_style="bold")
    table.add_column("Name", style="white")
    table.add_column("Type", style="blue")

    for col in columns_info:
        name = col["name"]
        if col["is_geometry"]:
            name = f"{name} 🌍"
            name_display = Text(name, style="cyan bold")
        else:
            name_display = name

        table.add_row(name_display, col["type"])

    console.print(table)

    # Preview table
    if preview_table is not None and preview_table.num_rows > 0:
        console.print()
        preview_label = (
            f"Preview (first {preview_table.num_rows} rows)"
            if preview_mode == "head"
            else f"Preview (last {preview_table.num_rows} rows)"
        )
        console.print(f"{preview_label}:")

        # Create preview table
        preview = Table(show_header=True, header_style="bold")

        # Add columns
        for col in columns_info:
            preview.add_column(col["name"], style="white", overflow="fold")

        # Add rows
        for i in range(preview_table.num_rows):
            row_data = []
            for col in columns_info:
                value = preview_table.column(col["name"])[i].as_py()
                formatted = format_value_for_display(value, col["type"], col["is_geometry"])
                row_data.append(formatted)
            preview.add_row(*row_data)

        console.print(preview)

    # Statistics table
    if stats:
        console.print()
        console.print("Statistics:")

        stats_table = Table(show_header=True, header_style="bold")
        stats_table.add_column("Column", style="white")
        stats_table.add_column("Nulls", style="yellow")
        stats_table.add_column("Min", style="blue")
        stats_table.add_column("Max", style="blue")
        stats_table.add_column("Unique", style="green")

        for col in columns_info:
            col_name = col["name"]
            col_stats = stats.get(col_name, {})

            nulls = col_stats.get("nulls", 0)
            min_val = col_stats.get("min")
            max_val = col_stats.get("max")
            unique = col_stats.get("unique")

            # Format values
            min_str = str(min_val) if min_val is not None else "-"
            max_str = str(max_val) if max_val is not None else "-"
            unique_str = f"~{unique:,}" if unique is not None else "-"

            # Truncate long values
            if len(min_str) > 20:
                min_str = min_str[:17] + "..."
            if len(max_str) > 20:
                max_str = max_str[:17] + "..."

            stats_table.add_row(
                col_name,
                f"{nulls:,}",
                min_str,
                max_str,
                unique_str,
            )

        console.print(stats_table)

    console.print()


def format_json_output(
    file_info: dict[str, Any],
    geo_info: dict[str, Any],
    columns_info: list[dict[str, Any]],
    preview_table: pa.Table | None = None,
    stats: dict[str, dict[str, Any]] | None = None,
) -> str:
    """
    Format output as JSON.

    Args:
        file_info: File information dict
        geo_info: Geo information dict
        columns_info: Column information list
        preview_table: Optional preview data table
        stats: Optional statistics dict

    Returns:
        str: JSON string
    """
    output = {
        "file": file_info["file_path"],
        "size_bytes": file_info["size_bytes"],
        "size_human": file_info["size_human"],
        "rows": file_info["rows"],
        "row_groups": file_info["row_groups"],
        "compression": file_info.get("compression"),
        "parquet_type": geo_info.get("parquet_type", "No Parquet geo logical type"),
        "geoparquet_version": geo_info.get("version"),
        "crs": _format_crs_for_display(geo_info.get("crs"), include_default=False),
        "geometry_types": geo_info.get("geometry_types"),
        "bbox": geo_info.get("bbox"),
        "warnings": geo_info.get("warnings", []),
        "columns": [
            {
                "name": col["name"],
                "type": col["type"],
                "is_geometry": col["is_geometry"],
            }
            for col in columns_info
        ],
    }

    # Add preview data if available
    if preview_table is not None and preview_table.num_rows > 0:
        preview_rows = []
        for i in range(preview_table.num_rows):
            row = {}
            for col in columns_info:
                value = preview_table.column(col["name"])[i].as_py()
                row[col["name"]] = format_value_for_json(value, col["is_geometry"])
            preview_rows.append(row)
        output["preview"] = preview_rows
    else:
        output["preview"] = None

    # Add statistics if available
    if stats:
        output["statistics"] = stats
    else:
        output["statistics"] = None

    return json.dumps(output, indent=2)


def extract_partition_summary(files: list[str], verbose: bool = False) -> dict[str, Any]:
    """
    Extract aggregated summary from all files in a partition.

    Args:
        files: List of parquet file paths
        verbose: Print debug messages

    Returns:
        dict: Aggregated info including:
            - file_count: Number of files
            - total_rows: Sum of rows across all files
            - total_size_bytes: Sum of file sizes
            - total_size_human: Human-readable total size
            - combined_bbox: Union of all bboxes
            - schema_consistent: Whether schemas match
            - compressions: Set of compression types used
            - geoparquet_versions: Set of versions found
            - per_file_info: List of per-file details
    """
    from geoparquet_io.core.logging_config import debug

    total_rows = 0
    total_size_bytes = 0
    combined_bbox = None  # [xmin, ymin, xmax, ymax]
    compressions = set()
    geoparquet_versions = set()
    per_file_info = []
    schema_columns = None
    schema_consistent = True

    for file_path in files:
        if verbose:
            debug(f"Processing file: {file_path}")

        # Get file info
        try:
            file_info = extract_file_info(file_path)
            geo_info = extract_geo_info(file_path)
        except Exception as e:
            if verbose:
                debug(f"Error processing {file_path}: {e}")
            continue

        # Accumulate totals
        total_rows += file_info.get("rows", 0)
        if file_info.get("size_bytes"):
            total_size_bytes += file_info["size_bytes"]

        # Track compression
        if file_info.get("compression"):
            compressions.add(file_info["compression"])

        # Track GeoParquet version
        if geo_info.get("version"):
            geoparquet_versions.add(geo_info["version"])

        # Merge bbox
        bbox = geo_info.get("bbox")
        if bbox and len(bbox) >= 4:
            if combined_bbox is None:
                combined_bbox = list(bbox[:4])
            else:
                combined_bbox[0] = min(combined_bbox[0], bbox[0])  # xmin
                combined_bbox[1] = min(combined_bbox[1], bbox[1])  # ymin
                combined_bbox[2] = max(combined_bbox[2], bbox[2])  # xmax
                combined_bbox[3] = max(combined_bbox[3], bbox[3])  # ymax

        # Check schema consistency
        from geoparquet_io.core.duckdb_metadata import get_usable_columns

        try:
            columns = get_usable_columns(file_path)
            col_names = tuple(c["name"] for c in columns)
            if schema_columns is None:
                schema_columns = col_names
            elif schema_columns != col_names:
                schema_consistent = False
        except Exception:
            pass

        # Store per-file info
        per_file_info.append(
            {
                "file": file_path,
                "file_name": os.path.basename(file_path),
                "rows": file_info.get("rows", 0),
                "size_bytes": file_info.get("size_bytes"),
                "size_human": file_info.get("size_human", "N/A"),
            }
        )

    return {
        "file_count": len(per_file_info),
        "total_rows": total_rows,
        "total_size_bytes": total_size_bytes if total_size_bytes > 0 else None,
        "total_size_human": format_size(total_size_bytes) if total_size_bytes > 0 else "N/A",
        "combined_bbox": combined_bbox,
        "schema_consistent": schema_consistent,
        "compressions": sorted(compressions) if compressions else [],
        "geoparquet_versions": sorted(geoparquet_versions) if geoparquet_versions else [],
        "per_file_info": per_file_info,
    }


def format_partition_terminal_output(
    partition_summary: dict[str, Any],
    geo_info: dict[str, Any],
    columns_info: list[dict[str, Any]],
) -> None:
    """
    Format and print partition summary terminal output using Rich.

    Args:
        partition_summary: Aggregated partition info from extract_partition_summary
        geo_info: Geo info from first file
        columns_info: Column info from first file
    """
    console = Console()

    console.print()
    console.print("[bold]Partition Summary[/bold]")
    console.print("━" * 60)

    # File count and totals
    console.print(f"Files: [cyan]{partition_summary['file_count']} parquet files[/cyan]")
    console.print(f"Total rows: [cyan]{partition_summary['total_rows']:,}[/cyan]")
    console.print(f"Total size: [cyan]{partition_summary['total_size_human']}[/cyan]")

    # Combined bbox
    if partition_summary["combined_bbox"]:
        bbox = partition_summary["combined_bbox"]
        console.print(
            f"Combined bounds: [cyan][{bbox[0]:.6f}, {bbox[1]:.6f}, "
            f"{bbox[2]:.6f}, {bbox[3]:.6f}][/cyan]"
        )

    console.print()

    # Schema consistency
    if partition_summary["schema_consistent"]:
        console.print("Schema: [green]Consistent across all files[/green]")
    else:
        console.print("Schema: [yellow]Varies between files[/yellow]")

    # Compression
    compressions = partition_summary.get("compressions", [])
    if compressions:
        if len(compressions) == 1:
            console.print(f"Compression: [cyan]{compressions[0]} (all files)[/cyan]")
        else:
            console.print(f"Compression: [yellow]{', '.join(compressions)} (mixed)[/yellow]")

    # GeoParquet version
    versions = partition_summary.get("geoparquet_versions", [])
    if versions:
        if len(versions) == 1:
            console.print(f"GeoParquet: [cyan]{versions[0]} (all files)[/cyan]")
        else:
            console.print(f"GeoParquet: [yellow]{', '.join(versions)} (mixed)[/yellow]")
    elif geo_info.get("parquet_type") in ("Geometry", "Geography"):
        console.print("GeoParquet: [dim]No GeoParquet metadata (Parquet geo type)[/dim]")

    console.print()

    # Per-file breakdown
    console.print("Per-file breakdown:")
    table = Table(show_header=True, header_style="bold")
    table.add_column("File", style="white")
    table.add_column("Rows", style="cyan", justify="right")
    table.add_column("Size", style="blue", justify="right")

    for file_info in partition_summary["per_file_info"]:
        table.add_row(
            file_info["file_name"],
            f"{file_info['rows']:,}",
            file_info["size_human"],
        )

    console.print(table)
    console.print()

    # Columns table (from first file)
    num_cols = len(columns_info)
    console.print(f"Columns ({num_cols}):")

    col_table = Table(show_header=True, header_style="bold")
    col_table.add_column("Name", style="white")
    col_table.add_column("Type", style="blue")

    for col in columns_info:
        name = col["name"]
        if col["is_geometry"]:
            name = f"{name} 🌍"
            name_display = Text(name, style="cyan bold")
        else:
            name_display = name

        col_table.add_row(name_display, col["type"])

    console.print(col_table)
    console.print()


def format_partition_json_output(
    partition_summary: dict[str, Any],
    geo_info: dict[str, Any],
    columns_info: list[dict[str, Any]],
) -> str:
    """
    Format partition summary as JSON.

    Args:
        partition_summary: Aggregated partition info
        geo_info: Geo info from first file
        columns_info: Column info from first file

    Returns:
        str: JSON string
    """
    output = {
        "partition": True,
        "file_count": partition_summary["file_count"],
        "total_rows": partition_summary["total_rows"],
        "total_size_bytes": partition_summary["total_size_bytes"],
        "total_size_human": partition_summary["total_size_human"],
        "combined_bbox": partition_summary["combined_bbox"],
        "schema_consistent": partition_summary["schema_consistent"],
        "compressions": partition_summary["compressions"],
        "geoparquet_versions": partition_summary["geoparquet_versions"],
        "parquet_type": geo_info.get("parquet_type"),
        "crs": _format_crs_for_display(geo_info.get("crs"), include_default=False),
        "columns": [
            {
                "name": col["name"],
                "type": col["type"],
                "is_geometry": col["is_geometry"],
            }
            for col in columns_info
        ],
        "files": partition_summary["per_file_info"],
    }

    return json.dumps(output, indent=2)


def format_partition_markdown_output(
    partition_summary: dict[str, Any],
    geo_info: dict[str, Any],
    columns_info: list[dict[str, Any]],
) -> str:
    """
    Format partition summary as Markdown.

    Args:
        partition_summary: Aggregated partition info
        geo_info: Geo info from first file
        columns_info: Column info from first file

    Returns:
        str: Markdown string
    """
    lines = []

    lines.append("## Partition Summary")
    lines.append("")

    lines.append(f"- **Files:** {partition_summary['file_count']} parquet files")
    lines.append(f"- **Total rows:** {partition_summary['total_rows']:,}")
    lines.append(f"- **Total size:** {partition_summary['total_size_human']}")

    if partition_summary["combined_bbox"]:
        bbox = partition_summary["combined_bbox"]
        lines.append(
            f"- **Combined bounds:** [{bbox[0]:.6f}, {bbox[1]:.6f}, {bbox[2]:.6f}, {bbox[3]:.6f}]"
        )

    lines.append("")

    # Schema consistency
    if partition_summary["schema_consistent"]:
        lines.append("- **Schema:** Consistent across all files")
    else:
        lines.append("- **Schema:** ⚠️ Varies between files")

    # Compression
    compressions = partition_summary.get("compressions", [])
    if compressions:
        if len(compressions) == 1:
            lines.append(f"- **Compression:** {compressions[0]} (all files)")
        else:
            lines.append(f"- **Compression:** {', '.join(compressions)} (mixed)")

    # GeoParquet version
    versions = partition_summary.get("geoparquet_versions", [])
    if versions:
        if len(versions) == 1:
            lines.append(f"- **GeoParquet:** {versions[0]} (all files)")
        else:
            lines.append(f"- **GeoParquet:** {', '.join(versions)} (mixed)")

    lines.append("")

    # Per-file breakdown
    lines.append("### Per-file breakdown")
    lines.append("")
    lines.append("| File | Rows | Size |")
    lines.append("|------|------|------|")

    for file_info in partition_summary["per_file_info"]:
        lines.append(
            f"| {file_info['file_name']} | {file_info['rows']:,} | {file_info['size_human']} |"
        )

    lines.append("")

    # Columns table
    num_cols = len(columns_info)
    lines.append(f"### Columns ({num_cols})")
    lines.append("")
    lines.append("| Name | Type |")
    lines.append("|------|------|")

    for col in columns_info:
        name = col["name"]
        if col["is_geometry"]:
            name = f"{name} 🌍"
        lines.append(f"| {name} | {col['type']} |")

    lines.append("")

    return "\n".join(lines)


def format_markdown_output(
    file_info: dict[str, Any],
    geo_info: dict[str, Any],
    columns_info: list[dict[str, Any]],
    preview_table: pa.Table | None = None,
    preview_mode: str | None = None,
    stats: dict[str, dict[str, Any]] | None = None,
) -> str:
    """
    Format output as Markdown for README files or documentation.

    Args:
        file_info: File information dict
        geo_info: Geo information dict
        columns_info: Column information list
        preview_table: Optional preview data table
        preview_mode: "head" or "tail" (when preview_table is provided)
        stats: Optional statistics dict

    Returns:
        str: Markdown string
    """
    lines = []

    # File header
    file_name = os.path.basename(file_info["file_path"])
    lines.append(f"## {file_name}")
    lines.append("")

    # Metadata section
    lines.append("### Metadata")
    lines.append("")
    lines.append(f"- **Size:** {file_info['size_human']}")
    lines.append(f"- **Rows:** {file_info['rows']:,}")
    lines.append(f"- **Row Groups:** {file_info['row_groups']}")

    if file_info.get("compression"):
        lines.append(f"- **Compression:** {file_info['compression']}")

    # Parquet type (new field)
    parquet_type = geo_info.get("parquet_type", "No Parquet geo logical type")
    lines.append(f"- **Parquet Type:** {parquet_type}")

    if geo_info["has_geo_metadata"]:
        if geo_info.get("version"):
            lines.append(f"- **GeoParquet Version:** {geo_info['version']}")

        crs_display = _format_crs_for_display(geo_info["crs"])
        lines.append(f"- **CRS:** {crs_display}")

        # Geometry types (if available)
        if geo_info.get("geometry_types"):
            geom_types = ", ".join(geo_info["geometry_types"])
            lines.append(f"- **Geometry Types:** {geom_types}")

        if geo_info["bbox"]:
            bbox = geo_info["bbox"]
            if len(bbox) == 4:
                lines.append(
                    f"- **Bbox:** [{bbox[0]:.6f}, {bbox[1]:.6f}, {bbox[2]:.6f}, {bbox[3]:.6f}]"
                )
            else:
                lines.append(f"- **Bbox:** {bbox}")
    elif parquet_type in ("Geometry", "Geography"):
        # Has Parquet geo type but no GeoParquet metadata
        lines.append("")
        lines.append("*No GeoParquet metadata (using Parquet geo type)*")
        crs_display = _format_crs_for_display(geo_info["crs"])
        lines.append(f"- **CRS:** {crs_display}")
        # Display bbox calculated from row group stats
        if geo_info["bbox"]:
            bbox = geo_info["bbox"]
            if len(bbox) == 4:
                lines.append(
                    f"- **Bbox:** [{bbox[0]:.6f}, {bbox[1]:.6f}, {bbox[2]:.6f}, {bbox[3]:.6f}]"
                )
            else:
                lines.append(f"- **Bbox:** {bbox}")
    else:
        lines.append("")
        lines.append("*No GeoParquet metadata found*")

    # Display warnings for metadata mismatches
    warnings = geo_info.get("warnings", [])
    if warnings:
        lines.append("")
        lines.append("**Warnings:**")
        for warning in warnings:
            lines.append(f"- ⚠️ {warning}")

    lines.append("")

    # Columns table
    num_cols = len(columns_info)
    lines.append(f"### Columns ({num_cols})")
    lines.append("")
    lines.append("| Name | Type |")
    lines.append("|------|------|")

    for col in columns_info:
        name = col["name"]
        if col["is_geometry"]:
            name = f"{name} 🌍"
        lines.append(f"| {name} | {col['type']} |")

    lines.append("")

    # Preview table
    if preview_table is not None and preview_table.num_rows > 0:
        preview_label = (
            f"Preview (first {preview_table.num_rows} rows)"
            if preview_mode == "head"
            else f"Preview (last {preview_table.num_rows} rows)"
        )
        lines.append(f"### {preview_label}")
        lines.append("")

        # Build header row
        header_row = "| " + " | ".join(col["name"] for col in columns_info) + " |"
        lines.append(header_row)

        # Build separator row
        separator_row = "|" + "|".join("------" for _ in columns_info) + "|"
        lines.append(separator_row)

        # Build data rows
        for i in range(preview_table.num_rows):
            row_values = []
            for col in columns_info:
                value = preview_table.column(col["name"])[i].as_py()
                formatted = format_value_for_display(value, col["type"], col["is_geometry"])
                # Escape markdown special characters in table cells
                formatted = formatted.replace("|", "\\|")
                formatted = formatted.replace("\n", " ")
                formatted = formatted.replace("\r", "")
                row_values.append(formatted)
            lines.append("| " + " | ".join(row_values) + " |")

        lines.append("")

    # Statistics table
    if stats:
        lines.append("### Statistics")
        lines.append("")
        lines.append("| Column | Nulls | Min | Max | Unique |")
        lines.append("|--------|-------|-----|-----|--------|")

        for col in columns_info:
            col_name = col["name"]
            col_stats = stats.get(col_name, {})

            nulls = col_stats.get("nulls", 0)
            min_val = col_stats.get("min")
            max_val = col_stats.get("max")
            unique = col_stats.get("unique")

            # Format values
            min_str = str(min_val) if min_val is not None else "-"
            max_str = str(max_val) if max_val is not None else "-"
            unique_str = f"~{unique:,}" if unique is not None else "-"

            # Truncate long values
            if len(min_str) > 20:
                min_str = min_str[:17] + "..."
            if len(max_str) > 20:
                max_str = max_str[:17] + "..."

            lines.append(f"| {col_name} | {nulls:,} | {min_str} | {max_str} | {unique_str} |")

        lines.append("")

    return "\n".join(lines)
