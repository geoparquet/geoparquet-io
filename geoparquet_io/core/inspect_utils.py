"""
Utilities for inspecting GeoParquet files.

Provides functions to extract metadata, preview data, calculate statistics,
and format output for terminal, JSON, and Markdown.
"""

import json
import os
import struct
from typing import Any, Optional

import duckdb
import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console
from rich.table import Table
from rich.text import Text

from geoparquet_io.core.common import (
    format_size,
    get_parquet_metadata,
    parse_geo_metadata,
    safe_file_url,
)
from geoparquet_io.core.metadata_utils import (
    detect_geo_logical_type,
    extract_bbox_from_row_group_stats,
    parse_geometry_type_from_schema,
)


def extract_file_info(parquet_file: str) -> dict[str, Any]:
    """
    Extract basic file information from a Parquet file.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        dict: File info including size, rows, row_groups, compression
    """
    safe_url = safe_file_url(parquet_file, verbose=False)

    with fsspec.open(safe_url, "rb") as f:
        pf = pq.ParquetFile(f)
        num_rows = pf.metadata.num_rows
        num_row_groups = pf.metadata.num_row_groups

        # Get compression from first row group, first column
        compression = None
        if num_row_groups > 0:
            row_group = pf.metadata.row_group(0)
            if row_group.num_columns > 0:
                column = row_group.column(0)
                compression = column.compression

    # Get file size - handle both local and remote files
    # Import here to avoid circular import
    from geoparquet_io.core.common import is_remote_url

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


def _extract_crs_string(crs_info: Any) -> Optional[str]:
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


def _extract_crs_identifier(crs_info: Any) -> Optional[tuple[str, int]]:
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
    safe_url = safe_file_url(parquet_file, verbose=False)

    # Read schema and metadata
    with fsspec.open(safe_url, "rb") as f:
        pf = pq.ParquetFile(f)
        schema = pf.schema_arrow
        parquet_schema_str = str(pf.metadata.schema)

    metadata, _ = get_parquet_metadata(parquet_file, verbose=False)
    geo_meta = parse_geo_metadata(metadata, verbose=False)

    # Detect Parquet native geo type
    parquet_type = "No Parquet geo logical type"
    parquet_geo_info = {}
    geometry_column = None

    for field in schema:
        geo_type = detect_geo_logical_type(field, parquet_schema_str)
        if geo_type:
            parquet_type = geo_type
            geometry_column = field.name

            # Parse additional details from Parquet schema
            geom_details = parse_geometry_type_from_schema(field.name, parquet_schema_str)
            if geom_details:
                parquet_geo_info["geometry_type"] = geom_details.get("geometry_type")
                parquet_geo_info["coordinate_dimension"] = geom_details.get("coordinate_dimension")
                parquet_geo_info["crs"] = geom_details.get("crs")
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

    # Determine effective bbox
    # Priority: GeoParquet metadata bbox, then calculate from row group stats
    effective_bbox = geoparquet_info.get("bbox")
    if not effective_bbox and parquet_type != "No Parquet geo logical type":
        # Try to calculate bbox from row group statistics (bbox struct column)
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
        "geometry_types": geoparquet_info.get("geometry_types"),
        "edges": geoparquet_info.get("edges") or parquet_geo_info.get("edges"),
        "warnings": warnings,
    }


def extract_columns_info(
    schema: pa.Schema, primary_geom_col: Optional[str]
) -> list[dict[str, Any]]:
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


def format_geometry_display(value: Any) -> str:
    """
    Format a geometry value for display.

    Args:
        value: Geometry value (WKB bytes or other)

    Returns:
        str: Formatted geometry display string
    """
    if value is None:
        return "NULL"

    if isinstance(value, bytes):
        geom_type = parse_wkb_type(value)
        return f"<{geom_type}>"

    return str(value)


def format_value_for_display(value: Any, column_type: str, is_geometry: bool) -> str:
    """
    Format a cell value for terminal display.

    Args:
        value: Cell value
        column_type: Column type string
        is_geometry: Whether this is a geometry column

    Returns:
        str: Formatted display string
    """
    if value is None:
        return "NULL"

    if is_geometry:
        return format_geometry_display(value)

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
    parquet_file: str, head: Optional[int] = None, tail: Optional[int] = None
) -> tuple[pa.Table, str]:
    """
    Read preview data from a Parquet file.

    Args:
        parquet_file: Path to the parquet file
        head: Number of rows from start (mutually exclusive with tail)
        tail: Number of rows from end (mutually exclusive with head)

    Returns:
        tuple: (PyArrow table with data, mode: "head" or "tail")
    """
    safe_url = safe_file_url(parquet_file, verbose=False)

    with fsspec.open(safe_url, "rb") as f:
        pf = pq.ParquetFile(f)
        total_rows = pf.metadata.num_rows

        if tail:
            # Read from end
            start_row = max(0, total_rows - tail)
            num_rows = min(tail, total_rows)
            table = pf.read_row_groups(list(range(pf.num_row_groups)), use_threads=True).slice(
                start_row, num_rows
            )
            mode = "tail"
        else:
            # Read from start (default if head is None, use 10)
            num_rows = head if head is not None else 10
            num_rows = min(num_rows, total_rows)
            table = pf.read(use_threads=True).slice(0, num_rows)
            mode = "head"

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
    preview_table: Optional[pa.Table] = None,
    preview_mode: Optional[str] = None,
    stats: Optional[dict[str, dict[str, Any]]] = None,
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
        # Display bbox calculated from row group stats
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
    preview_table: Optional[pa.Table] = None,
    stats: Optional[dict[str, dict[str, Any]]] = None,
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


def format_markdown_output(
    file_info: dict[str, Any],
    geo_info: dict[str, Any],
    columns_info: list[dict[str, Any]],
    preview_table: Optional[pa.Table] = None,
    preview_mode: Optional[str] = None,
    stats: Optional[dict[str, dict[str, Any]]] = None,
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
