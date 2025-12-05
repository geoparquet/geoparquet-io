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


def extract_geo_info(parquet_file: str) -> dict[str, Any]:
    """
    Extract GeoParquet-specific information from metadata.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        dict: Geo info including CRS, bbox, primary_column, version
    """
    metadata, _ = get_parquet_metadata(parquet_file, verbose=False)
    geo_meta = parse_geo_metadata(metadata, verbose=False)

    if not geo_meta:
        return {
            "has_geo_metadata": False,
            "version": None,
            "crs": None,
            "bbox": None,
            "primary_column": None,
        }

    # Extract version
    version = geo_meta.get("version")

    # Extract CRS and bbox from primary geometry column
    primary_column = geo_meta.get("primary_column", "geometry")
    columns_meta = geo_meta.get("columns", {})

    crs = None
    bbox = None
    geometry_types = None

    if primary_column in columns_meta:
        col_meta = columns_meta[primary_column]
        crs_info = col_meta.get("crs")

        # Extract CRS string (handle both dict and simple formats)
        if isinstance(crs_info, dict):
            # Try different CRS formats
            if "id" in crs_info:
                crs_id = crs_info["id"]
                if isinstance(crs_id, dict):
                    authority = crs_id.get("authority", "EPSG")
                    code = crs_id.get("code")
                    if code:
                        crs = f"{authority}:{code}"
                else:
                    crs = str(crs_id)
            elif "$schema" in crs_info:
                # PROJJSON format
                crs = "PROJJSON"
            elif "wkt" in crs_info:
                crs = "WKT"
        elif crs_info:
            crs = str(crs_info)

        # Extract bbox
        bbox = col_meta.get("bbox")

        # Extract geometry_types
        geometry_types = col_meta.get("geometry_types")

    # Per GeoParquet spec, if CRS is not specified, it defaults to EPSG:4326
    if crs is None and primary_column in columns_meta:
        crs = "EPSG:4326 (default)"

    return {
        "has_geo_metadata": True,
        "version": version,
        "crs": crs,
        "bbox": bbox,
        "primary_column": primary_column,
        "geometry_types": geometry_types,
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

    Args:
        wkb_bytes: WKB binary data
        max_length: Maximum length of WKT string to return

    Returns:
        str: Truncated WKT string or fallback geometry type
    """
    if not wkb_bytes or len(wkb_bytes) < 5:
        return "<GEOMETRY>"

    try:
        con = duckdb.connect()
        con.execute("LOAD spatial;")
        result = con.execute("SELECT ST_AsText(ST_GeomFromWKB(?::BLOB))", [wkb_bytes]).fetchone()
        con.close()

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
    try:
        xmin = value.get("xmin", 0)
        ymin = value.get("ymin", 0)
        xmax = value.get("xmax", 0)
        ymax = value.get("ymax", 0)
        formatted = f"[{xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f}]"
        if len(formatted) > max_length:
            return formatted[: max_length - 3] + "..."
        return formatted
    except (TypeError, ValueError):
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


def format_value_for_json(value: Any, is_geometry: bool, max_length: int = 80) -> Any:
    """
    Format a cell value for JSON output.

    Args:
        value: Cell value
        is_geometry: Whether this is a geometry column
        max_length: Maximum length for geometry WKT preview

    Returns:
        JSON-serializable value
    """
    if value is None:
        return None

    if is_geometry:
        if isinstance(value, bytes):
            return format_geometry_display(value, max_length)
        return str(value)

    # Format bbox struct columns nicely
    if is_bbox_value(value):
        return format_bbox_display(value, max_length)

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

    if geo_info["has_geo_metadata"]:
        # GeoParquet version
        if geo_info.get("version"):
            console.print(f"GeoParquet Version: [cyan]{geo_info['version']}[/cyan]")

        crs_display = geo_info["crs"] if geo_info["crs"] else "Not specified"
        console.print(f"CRS: [cyan]{crs_display}[/cyan]")

        if geo_info["bbox"]:
            bbox = geo_info["bbox"]
            if len(bbox) == 4:
                console.print(
                    f"Bbox: [cyan][{bbox[0]:.6f}, {bbox[1]:.6f}, {bbox[2]:.6f}, {bbox[3]:.6f}][/cyan]"
                )
            else:
                console.print(f"Bbox: [cyan]{bbox}[/cyan]")

        if geo_info.get("geometry_types"):
            geom_types_str = ", ".join(geo_info["geometry_types"])
            console.print(f"Geometry Types: [cyan]{geom_types_str}[/cyan]")
    else:
        console.print("[yellow]No GeoParquet metadata found[/yellow]")

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

        # Calculate dynamic max_length for geometry WKT based on terminal width
        # Reserve space for table borders, padding, and other columns
        terminal_width = console.width
        num_cols = len(columns_info)
        # Estimate overhead: ~3 chars per column for borders/padding, plus some margin
        overhead = num_cols * 4 + 10
        available_width = terminal_width - overhead
        # Divide among columns, with min 40 and max 120 for geometry
        geom_max_length = max(40, min(120, available_width // max(num_cols, 1)))

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
                formatted = format_value_for_display(
                    value, col["type"], col["is_geometry"], geom_max_length
                )
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
        "geoparquet_version": geo_info.get("version"),
        "crs": geo_info.get("crs"),
        "bbox": geo_info.get("bbox"),
        "geometry_types": geo_info.get("geometry_types"),
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
                row[col["name"]] = format_value_for_json(value, col["is_geometry"], max_length=80)
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

    if geo_info["has_geo_metadata"]:
        if geo_info.get("version"):
            lines.append(f"- **GeoParquet Version:** {geo_info['version']}")

        crs_display = geo_info["crs"] if geo_info["crs"] else "Not specified"
        lines.append(f"- **CRS:** {crs_display}")

        if geo_info["bbox"]:
            bbox = geo_info["bbox"]
            if len(bbox) == 4:
                lines.append(
                    f"- **Bbox:** [{bbox[0]:.6f}, {bbox[1]:.6f}, {bbox[2]:.6f}, {bbox[3]:.6f}]"
                )
            else:
                lines.append(f"- **Bbox:** {bbox}")

        if geo_info.get("geometry_types"):
            geom_types_str = ", ".join(geo_info["geometry_types"])
            lines.append(f"- **Geometry Types:** {geom_types_str}")
    else:
        lines.append("")
        lines.append("*No GeoParquet metadata found*")

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
                formatted = format_value_for_display(
                    value, col["type"], col["is_geometry"], max_length=80
                )
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
