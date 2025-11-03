"""
Utilities for inspecting GeoParquet files.

Provides functions to extract metadata, preview data, calculate statistics,
and format output for terminal and JSON.
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
    if parquet_file.startswith(("http://", "https://")):
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

    # Per GeoParquet spec, if CRS is not specified, it defaults to EPSG:4326
    if crs is None and primary_column in columns_meta:
        crs = "EPSG:4326 (default)"

    return {
        "has_geo_metadata": True,
        "version": version,
        "crs": crs,
        "bbox": bbox,
        "primary_column": primary_column,
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
        "geoparquet_version": geo_info.get("version"),
        "crs": geo_info.get("crs"),
        "bbox": geo_info.get("bbox"),
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


def format_geo_metadata_output(parquet_file: str, json_output: bool) -> None:
    """
    Format and output GeoParquet metadata from the 'geo' key.

    Args:
        parquet_file: Path to the parquet file
        json_output: Whether to output as JSON
    """
    import click

    metadata, _ = get_parquet_metadata(parquet_file, verbose=False)
    geo_meta = parse_geo_metadata(metadata, verbose=False)

    if not geo_meta:
        if json_output:
            click.echo(json.dumps(None, indent=2))
        else:
            click.echo("No GeoParquet metadata found in this file.")
        return

    if json_output:
        # Output the exact geo metadata as JSON
        click.echo(json.dumps(geo_meta, indent=2))
    else:
        # Human-readable output
        console = Console()
        console.print()
        console.print("[bold]GeoParquet Metadata[/bold]")
        console.print("━" * 60)

        # Version
        if "version" in geo_meta:
            console.print(f"Version: [cyan]{geo_meta['version']}[/cyan]")

        # Primary column
        if "primary_column" in geo_meta:
            console.print(f"Primary Column: [cyan]{geo_meta['primary_column']}[/cyan]")

        console.print()

        # Columns
        if "columns" in geo_meta and geo_meta["columns"]:
            console.print("[bold]Columns:[/bold]")
            for col_name, col_meta in geo_meta["columns"].items():
                console.print(f"\n  [cyan bold]{col_name}[/cyan bold]:")

                # Encoding
                if "encoding" in col_meta:
                    console.print(f"    Encoding: {col_meta['encoding']}")

                # Geometry types
                if "geometry_types" in col_meta:
                    types = ", ".join(col_meta["geometry_types"])
                    console.print(f"    Geometry Types: {types}")

                # CRS - simplified output
                if "crs" in col_meta:
                    crs_info = col_meta["crs"]
                    if isinstance(crs_info, dict):
                        # Check if it's PROJJSON (has $schema)
                        if "$schema" in crs_info:
                            # Extract name and id if available
                            crs_name = crs_info.get("name", "Unknown")
                            console.print(f"    CRS Name: {crs_name}")

                            # Extract id (authority and code)
                            if "id" in crs_info:
                                id_info = crs_info["id"]
                                if isinstance(id_info, dict):
                                    authority = id_info.get("authority", "")
                                    code = id_info.get("code", "")
                                    console.print(f"    CRS ID: {authority}:{code}")

                            console.print("    [dim](PROJJSON format - use --json to see full CRS definition)[/dim]")
                        else:
                            # Other CRS format
                            console.print(f"    CRS: {json.dumps(crs_info, indent=6)}")
                    else:
                        console.print(f"    CRS: {crs_info}")
                else:
                    # Default CRS per GeoParquet spec
                    console.print(f"    CRS: [dim]Not present - OGC:CRS84 (default value)[/dim]")

                # Orientation
                if "orientation" in col_meta:
                    console.print(f"    Orientation: {col_meta['orientation']}")
                else:
                    console.print(f"    Orientation: [dim]Not present - counterclockwise (default value)[/dim]")

                # Edges
                if "edges" in col_meta:
                    console.print(f"    Edges: {col_meta['edges']}")
                else:
                    console.print(f"    Edges: [dim]Not present - planar (default value)[/dim]")

                # Bbox
                if "bbox" in col_meta:
                    bbox = col_meta["bbox"]
                    if isinstance(bbox, list) and len(bbox) == 4:
                        console.print(f"    Bbox: [{bbox[0]:.6f}, {bbox[1]:.6f}, {bbox[2]:.6f}, {bbox[3]:.6f}]")
                    else:
                        console.print(f"    Bbox: {bbox}")

                # Epoch
                if "epoch" in col_meta:
                    console.print(f"    Epoch: {col_meta['epoch']}")
                else:
                    console.print(f"    Epoch: [dim]Not present[/dim]")

                # Covering
                if "covering" in col_meta:
                    console.print("    Covering:")
                    covering = col_meta["covering"]
                    for cover_type, cover_info in covering.items():
                        console.print(f"      {cover_type}: {json.dumps(cover_info, indent=8)}")
                else:
                    console.print(f"    Covering: [dim]Not present[/dim]")

        console.print()


def format_parquet_metadata_output(parquet_file: str, json_output: bool) -> None:
    """
    Format and output Parquet file metadata.

    Args:
        parquet_file: Path to the parquet file
        json_output: Whether to output as JSON
    """
    import click

    safe_url = safe_file_url(parquet_file, verbose=False)

    with fsspec.open(safe_url, "rb") as f:
        pf = pq.ParquetFile(f)
        parquet_metadata = pf.metadata

    if json_output:
        # Convert metadata to JSON-serializable format
        metadata_dict = {
            "num_rows": parquet_metadata.num_rows,
            "num_row_groups": parquet_metadata.num_row_groups,
            "num_columns": parquet_metadata.num_columns,
            "serialized_size": parquet_metadata.serialized_size,
            "schema": str(parquet_metadata.schema),
            "row_groups": []
        }

        # Add row group metadata
        for i in range(parquet_metadata.num_row_groups):
            rg = parquet_metadata.row_group(i)
            rg_dict = {
                "id": i,
                "num_rows": rg.num_rows,
                "num_columns": rg.num_columns,
                "total_byte_size": rg.total_byte_size,
                "columns": []
            }

            # Add column metadata for this row group
            for j in range(rg.num_columns):
                col = rg.column(j)
                col_dict = {
                    "path_in_schema": col.path_in_schema,
                    "file_offset": col.file_offset,
                    "file_path": col.file_path,
                    "physical_type": col.physical_type,
                    "num_values": col.num_values,
                    "total_compressed_size": col.total_compressed_size,
                    "total_uncompressed_size": col.total_uncompressed_size,
                    "compression": col.compression,
                    "encodings": [str(enc) for enc in col.encodings] if hasattr(col, 'encodings') else []
                }

                # Add statistics if available
                if col.is_stats_set:
                    col_dict["statistics"] = {
                        "has_min_max": col.statistics.has_min_max if hasattr(col.statistics, 'has_min_max') else False,
                        "has_null_count": col.statistics.has_null_count if hasattr(col.statistics, 'has_null_count') else False,
                        "null_count": col.statistics.null_count if hasattr(col.statistics, 'null_count') else None,
                    }

                rg_dict["columns"].append(col_dict)

            metadata_dict["row_groups"].append(rg_dict)

        click.echo(json.dumps(metadata_dict, indent=2))
    else:
        # Human-readable output
        console = Console()
        console.print()
        console.print("[bold]Parquet File Metadata[/bold]")
        console.print("━" * 60)

        console.print(f"Total Rows: [cyan]{parquet_metadata.num_rows:,}[/cyan]")
        console.print(f"Row Groups: [cyan]{parquet_metadata.num_row_groups}[/cyan]")
        console.print(f"Columns: [cyan]{parquet_metadata.num_columns}[/cyan]")
        console.print(f"Serialized Size: [cyan]{format_size(parquet_metadata.serialized_size)}[/cyan]")

        console.print()
        console.print("[bold]Schema:[/bold]")
        console.print(f"  {parquet_metadata.schema}")

        # Row groups
        console.print()
        console.print(f"[bold]Row Groups ({parquet_metadata.num_row_groups}):[/bold]")

        for i in range(parquet_metadata.num_row_groups):
            rg = parquet_metadata.row_group(i)
            console.print(f"\n  [cyan bold]Row Group {i}[/cyan bold]:")
            console.print(f"    Rows: {rg.num_rows:,}")
            console.print(f"    Total Size: {format_size(rg.total_byte_size)}")

            # Create a table for columns in this row group
            table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
            table.add_column("Column", style="white")
            table.add_column("Type", style="blue")
            table.add_column("Compressed", style="yellow", justify="right")
            table.add_column("Uncompressed", style="yellow", justify="right")
            table.add_column("Compression", style="green")

            for j in range(rg.num_columns):
                col = rg.column(j)
                table.add_row(
                    col.path_in_schema,
                    col.physical_type,
                    format_size(col.total_compressed_size),
                    format_size(col.total_uncompressed_size),
                    col.compression
                )

            console.print(table)

        console.print()


def format_parquet_geo_metadata_output(parquet_file: str, json_output: bool) -> None:
    """
    Format and output geospatial metadata from Parquet footer.

    Args:
        parquet_file: Path to the parquet file
        json_output: Whether to output as JSON
    """
    import click

    safe_url = safe_file_url(parquet_file, verbose=False)

    with fsspec.open(safe_url, "rb") as f:
        pf = pq.ParquetFile(f)
        schema = pf.schema_arrow
        parquet_metadata = pf.metadata

    # Extract geospatial metadata from schema and statistics
    geo_columns = {}

    # Check for GEOMETRY or GEOGRAPHY logical types in schema
    for i, field in enumerate(schema):
        field_metadata = {}

        # Check logical type
        if hasattr(field.type, 'logical_type'):
            logical_type = str(field.type.logical_type)
            if 'GEOMETRY' in logical_type.upper() or 'GEOGRAPHY' in logical_type.upper():
                field_metadata['logical_type'] = logical_type

        # Extract statistics from row groups
        statistics_list = []

        for rg_idx in range(parquet_metadata.num_row_groups):
            rg = parquet_metadata.row_group(rg_idx)

            # Find column in this row group
            for col_idx in range(rg.num_columns):
                col = rg.column(col_idx)
                if col.path_in_schema == field.name:
                    col_stats = {}

                    # Check if statistics are available
                    if col.is_stats_set:
                        stats = col.statistics

                        # Try to extract geospatial statistics
                        # PyArrow may expose these through the statistics object
                        if hasattr(stats, 'min') and hasattr(stats, 'max'):
                            # For now, we'll check if there are custom key-value metadata
                            pass

                        # Check for null count
                        if hasattr(stats, 'null_count'):
                            col_stats['null_count'] = stats.null_count

                    # Check column metadata for custom geospatial info
                    # This would be stored in key-value metadata
                    if col_stats:
                        statistics_list.append({
                            'row_group': rg_idx,
                            'statistics': col_stats
                        })

        if field_metadata or statistics_list:
            geo_columns[field.name] = {
                'metadata': field_metadata,
                'statistics': statistics_list
            }

    # Also check for custom key-value metadata at file level
    custom_metadata = {}
    if parquet_metadata.metadata:
        for key in parquet_metadata.metadata:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
            # Look for geospatial-related keys
            if any(geo_key in key_str.lower() for geo_key in ['geo', 'crs', 'bbox', 'geometry', 'geography']):
                value = parquet_metadata.metadata[key]
                value_str = value.decode('utf-8') if isinstance(value, bytes) else str(value)
                custom_metadata[key_str] = value_str

    if json_output:
        # JSON output
        output = {
            'geospatial_columns': geo_columns,
            'custom_metadata': custom_metadata
        }
        click.echo(json.dumps(output, indent=2))
    else:
        # Human-readable output
        console = Console()
        console.print()
        console.print("[bold]Parquet Geospatial Metadata[/bold]")
        console.print("━" * 60)

        if not geo_columns and not custom_metadata:
            console.print("[yellow]No geospatial metadata found in Parquet footer.[/yellow]")
            console.print()
            console.print("[dim]Note: This shows metadata from the Parquet footer (column statistics).[/dim]")
            console.print("[dim]For GeoParquet metadata, use --geo-metadata instead.[/dim]")
        else:
            # Display geospatial columns
            if geo_columns:
                console.print(f"\n[bold]Geospatial Columns ({len(geo_columns)}):[/bold]")

                for col_name, col_info in geo_columns.items():
                    console.print(f"\n  [cyan bold]{col_name}[/cyan bold]:")

                    # Logical type
                    if 'logical_type' in col_info['metadata']:
                        console.print(f"    Logical Type: {col_info['metadata']['logical_type']}")

                    # Statistics
                    if col_info['statistics']:
                        console.print(f"    Statistics available for {len(col_info['statistics'])} row group(s)")

                        # Show statistics per row group
                        for stat_info in col_info['statistics']:
                            rg_id = stat_info['row_group']
                            stats = stat_info['statistics']
                            console.print(f"      Row Group {rg_id}:")
                            for key, value in stats.items():
                                console.print(f"        {key}: {value}")

            # Display custom metadata
            if custom_metadata:
                console.print("\n[bold]Custom Geospatial Metadata:[/bold]")
                for key, value in custom_metadata.items():
                    # Try to parse as JSON for better display
                    try:
                        parsed = json.loads(value)
                        console.print(f"  {key}:")
                        console.print(f"    {json.dumps(parsed, indent=6)}")
                    except (json.JSONDecodeError, TypeError):
                        console.print(f"  {key}: {value}")

        console.print()
