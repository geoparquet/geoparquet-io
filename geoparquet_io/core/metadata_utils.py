"""
Utilities for extracting and formatting GeoParquet metadata.

Provides functions to extract and format metadata from GeoParquet files,
including Parquet file metadata, Parquet geospatial metadata, and GeoParquet metadata.
"""

import json
from typing import Any, Optional

import click
import fsspec
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


def _check_parquet_schema_string(field_name, parquet_schema_str):
    """Check Parquet schema string for geo types."""
    import re

    escaped_name = re.escape(field_name)
    pattern = rf"{escaped_name}\s+[^(]*\(Geography"
    if re.search(pattern, parquet_schema_str):
        return "Geography"
    pattern = rf"{escaped_name}\s+[^(]*\(Geometry"
    if re.search(pattern, parquet_schema_str):
        return "Geometry"
    return None


def _check_extension_type(field):
    """Check PyArrow extension type for geo types."""
    if hasattr(field.type, "id") and hasattr(field.type, "extension_name"):
        ext_name = getattr(field.type, "extension_name", None)
        if ext_name:
            ext_name_lower = ext_name.lower()
            if "geography" in ext_name_lower:
                return "Geography"
            elif "geometry" in ext_name_lower:
                return "Geometry"
    return None


def detect_geo_logical_type(field, parquet_schema_str: Optional[str] = None) -> Optional[str]:
    """
    Detect if a field has a GEOMETRY or GEOGRAPHY logical type.

    Args:
        field: PyArrow field
        parquet_schema_str: Optional Parquet schema string to parse

    Returns:
        str: "Geometry" or "Geography" if detected, None otherwise
    """
    # First check the Parquet schema string if provided
    if parquet_schema_str:
        result = _check_parquet_schema_string(field.name, parquet_schema_str)
        if result:
            return result

    # Check the field type string representation for Geography/Geometry
    type_str = str(field.type)
    if "Geography" in type_str:
        return "Geography"
    elif "Geometry" in type_str:
        return "Geometry"

    # Check for logical type in PyArrow field (extension types)
    return _check_extension_type(field)


def parse_geometry_type_from_schema(
    field_name: str, parquet_schema_str: str
) -> Optional[dict[str, Any]]:
    """
    Parse geometry type details from Parquet schema string.

    According to the Parquet geospatial spec, the format is:
    field_name (Geometry(geom_type, coord_dimension, crs=..., ...))
    or
    field_name (Geography(geom_type, coord_dimension, crs=..., algorithm=...))

    Args:
        field_name: Name of the field to parse
        parquet_schema_str: Parquet schema string

    Returns:
        dict with 'geometry_type', 'coordinate_dimension', and 'crs', or None if not present
    """
    import re

    # Escape special regex characters in field name
    escaped_name = re.escape(field_name)

    # Pattern to match the full Geometry/Geography annotation
    # We need to capture everything inside Geometry(...) including nested structures
    pattern = rf"{escaped_name}\s+[^(]*\((Geometry|Geography)\((.*)\)\)"
    match = re.search(pattern, parquet_schema_str)

    if not match:
        return None

    params_str = match.group(2)  # Get the full parameters string

    result = {}

    # Parse CRS if present - look for crs={...} or crs="..."
    # CRS can be a complex JSON object, so we need to find the matching braces
    crs_match = re.search(r'crs=(\{.*?\}(?=\s*[,)])|"[^"]*"|\S+)', params_str)
    if crs_match:
        crs_value = crs_match.group(1)
        # Skip if CRS is empty (just a comma or closing paren after =)
        if crs_value and crs_value != "," and crs_value != ")":
            # Try to parse as JSON if it starts with {
            if crs_value.startswith("{"):
                try:
                    # Find the complete CRS object by counting braces
                    start_pos = params_str.find("crs={") + 4  # Position after "crs="
                    brace_count = 0
                    end_pos = start_pos
                    for i, char in enumerate(params_str[start_pos:], start=start_pos):
                        if char == "{":
                            brace_count += 1
                        elif char == "}":
                            brace_count -= 1
                            if brace_count == 0:
                                end_pos = i + 1
                                break

                    if end_pos > start_pos:
                        crs_json_str = params_str[start_pos:end_pos]
                        try:
                            result["crs"] = json.loads(crs_json_str)
                        except Exception:
                            result["crs"] = crs_json_str
                except Exception:
                    pass
            elif crs_value.startswith('"') and crs_value.endswith('"'):
                result["crs"] = crs_value.strip('"')
            else:
                result["crs"] = crs_value

    # Parse algorithm parameter (for Geography type) - planar or spherical
    algorithm_match = re.search(r"algorithm=(planar|spherical)", params_str)
    if algorithm_match:
        result["algorithm"] = algorithm_match.group(1)

    # Split by comma, but be careful about commas inside JSON objects
    # For simplicity, we'll look for positional parameters at the start
    # before any = signs
    parts = []
    depth = 0
    current_part = []

    for char in params_str:
        if char == "{":
            depth += 1
            current_part.append(char)
        elif char == "}":
            depth -= 1
            current_part.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current_part).strip())
            current_part = []
        else:
            current_part.append(char)

    if current_part:
        parts.append("".join(current_part).strip())

    # First parameter (if present and not a key=value pair) is geometry type
    # Valid types: Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, GeometryCollection
    valid_geom_types = [
        "Point",
        "LineString",
        "Polygon",
        "MultiPoint",
        "MultiLineString",
        "MultiPolygon",
        "GeometryCollection",
    ]

    positional_params = []
    for part in parts:
        if "=" not in part:
            positional_params.append(part.strip())

    # First positional parameter is geometry type
    if len(positional_params) > 0:
        geom_type = positional_params[0]
        if geom_type in valid_geom_types:
            result["geometry_type"] = geom_type

    # Second positional parameter is coordinate dimension
    # Valid dimensions: XY, XYZ, XYM, XYZM
    valid_coord_dims = ["XY", "XYZ", "XYM", "XYZM"]

    if len(positional_params) > 1:
        coord_dim = positional_params[1]
        if coord_dim in valid_coord_dims:
            result["coordinate_dimension"] = coord_dim

    return result if result else None


def format_parquet_metadata_enhanced(
    parquet_file: str,
    json_output: bool,
    row_groups_limit: Optional[int] = 1,
    primary_geom_col: Optional[str] = None,
) -> None:
    """
    Format and output enhanced Parquet file metadata with geo column highlighting.

    Args:
        parquet_file: Path to the parquet file
        json_output: Whether to output as JSON
        row_groups_limit: Number of row groups to display (None for all)
        primary_geom_col: Primary geometry column name (for highlighting)
    """
    safe_url = safe_file_url(parquet_file, verbose=False)

    with fsspec.open(safe_url, "rb") as f:
        pf = pq.ParquetFile(f)
        parquet_metadata = pf.metadata
        schema = pf.schema_arrow

    # Get Parquet schema string for better type detection
    parquet_schema_str = str(parquet_metadata.schema)

    # Detect geo columns - ONLY those with Parquet Geometry/Geography logical types
    # Do NOT use GeoParquet metadata or make assumptions based on column names
    geo_columns = {}
    for field in schema:
        geo_type = detect_geo_logical_type(field, parquet_schema_str)
        if geo_type:
            geo_columns[field.name] = geo_type

    # Find bbox struct columns associated with geometry columns
    bbox_columns = {}  # Maps geometry column name to bbox struct column name
    for field in schema:
        if str(field.type).startswith("struct<") and "xmin" in str(field.type):
            # This looks like a bbox struct - try to find associated geometry column
            # Pattern 1: geometry -> geometry_bbox
            if field.name.endswith("_bbox"):
                base_name = field.name[:-5]  # Remove '_bbox'
                if base_name in [f.name for f in schema]:
                    bbox_columns[base_name] = field.name
            # Pattern 2: Just named 'bbox' - associate with any geometry column found
            elif field.name == "bbox":
                # Find geometry columns to associate with
                for geom_name in geo_columns.keys():
                    bbox_columns[geom_name] = field.name

    if json_output:
        # JSON output
        metadata_dict = {
            "num_rows": parquet_metadata.num_rows,
            "num_row_groups": parquet_metadata.num_row_groups,
            "num_columns": parquet_metadata.num_columns,
            "serialized_size": parquet_metadata.serialized_size,
            "schema": str(parquet_metadata.schema),
            "row_groups": [],
        }

        # Determine how many row groups to include
        num_rg_to_show = parquet_metadata.num_row_groups
        if row_groups_limit is not None:
            num_rg_to_show = min(row_groups_limit, parquet_metadata.num_row_groups)

        # Add row group metadata
        for i in range(num_rg_to_show):
            rg = parquet_metadata.row_group(i)
            rg_dict = {
                "id": i,
                "num_rows": rg.num_rows,
                "num_columns": rg.num_columns,
                "total_byte_size": rg.total_byte_size,
                "columns": [],
            }

            # Add column metadata for this row group
            for j in range(rg.num_columns):
                col = rg.column(j)
                col_name = col.path_in_schema
                is_geo = col_name in geo_columns

                col_dict = {
                    "path_in_schema": col_name,
                    "file_offset": col.file_offset,
                    "file_path": col.file_path,
                    "physical_type": col.physical_type,
                    "num_values": col.num_values,
                    "total_compressed_size": col.total_compressed_size,
                    "total_uncompressed_size": col.total_uncompressed_size,
                    "compression": col.compression,
                    "encodings": [str(enc) for enc in col.encodings]
                    if hasattr(col, "encodings")
                    else [],
                    "is_geo": is_geo,
                    "geo_type": geo_columns.get(col_name),
                }

                # Add statistics if available
                if col.is_stats_set:
                    stats = col.statistics
                    col_dict["statistics"] = {
                        "has_min_max": stats.has_min_max
                        if hasattr(stats, "has_min_max")
                        else False,
                        "has_null_count": stats.has_null_count
                        if hasattr(stats, "has_null_count")
                        else False,
                        "null_count": stats.null_count if hasattr(stats, "null_count") else None,
                    }

                    # Add min/max if available (only for non-geo columns)
                    # Geo columns get bbox from struct columns, not from their own statistics
                    if stats.has_min_max and not is_geo:
                        try:
                            col_dict["statistics"]["min"] = str(stats.min)
                            col_dict["statistics"]["max"] = str(stats.max)
                        except Exception:
                            pass

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
        console.print(
            f"Serialized Size: [cyan]{format_size(parquet_metadata.serialized_size)}[/cyan]"
        )

        console.print()
        console.print("[bold]Schema:[/bold]")
        console.print(f"  {parquet_metadata.schema}")

        # Determine how many row groups to show
        num_rg_to_show = parquet_metadata.num_row_groups
        if row_groups_limit is not None:
            num_rg_to_show = min(row_groups_limit, parquet_metadata.num_row_groups)

        # Row groups
        console.print()
        if row_groups_limit is not None and row_groups_limit < parquet_metadata.num_row_groups:
            console.print(
                f"[bold]Row Groups (showing {num_rg_to_show} of {parquet_metadata.num_row_groups}):[/bold]"
            )
        else:
            console.print(f"[bold]Row Groups ({parquet_metadata.num_row_groups}):[/bold]")

        for i in range(num_rg_to_show):
            rg = parquet_metadata.row_group(i)
            console.print(f"\n  [cyan bold]Row Group {i}[/cyan bold]:")
            console.print(f"    Rows: {rg.num_rows:,}")
            console.print(f"    Total Size: {format_size(rg.total_byte_size)}")

            # Create a table for columns in this row group
            table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
            table.add_column("Column", style="white")
            table.add_column("Type", style="blue", min_width=24)
            table.add_column("Compressed", style="yellow", justify="right")
            table.add_column("Uncompressed", style="yellow", justify="right")
            table.add_column("Compression", style="green")
            table.add_column("MinValue", style="magenta")
            table.add_column("MaxValue", style="magenta")

            for j in range(rg.num_columns):
                col = rg.column(j)
                col_name = col.path_in_schema

                # Check if this is a geo column
                is_geo = col_name in geo_columns
                geo_type = geo_columns.get(col_name)

                # Format column name
                if is_geo:
                    col_name_display = Text(f"🌍 {col_name}", style="cyan bold")
                else:
                    col_name_display = col_name

                # Format type with geo annotation
                if is_geo and geo_type:
                    type_display = f"{col.physical_type}({geo_type})"
                else:
                    type_display = col.physical_type

                # Get min/max values if available
                min_val = "-"
                max_val = "-"

                # For geometry columns, ONLY get bbox from associated struct column
                if is_geo and col_name in bbox_columns:
                    bbox_col_name = bbox_columns[col_name]
                    # Look for the bbox struct components in this row group
                    xmin_val = None
                    ymin_val = None
                    xmax_val = None
                    ymax_val = None

                    for k in range(rg.num_columns):
                        bbox_component = rg.column(k)
                        path = bbox_component.path_in_schema

                        if path == f"{bbox_col_name}.xmin" and bbox_component.is_stats_set:
                            if bbox_component.statistics.has_min_max:
                                xmin_val = bbox_component.statistics.min
                        elif path == f"{bbox_col_name}.ymin" and bbox_component.is_stats_set:
                            if bbox_component.statistics.has_min_max:
                                ymin_val = bbox_component.statistics.min
                        elif path == f"{bbox_col_name}.xmax" and bbox_component.is_stats_set:
                            if bbox_component.statistics.has_min_max:
                                xmax_val = bbox_component.statistics.max
                        elif path == f"{bbox_col_name}.ymax" and bbox_component.is_stats_set:
                            if bbox_component.statistics.has_min_max:
                                ymax_val = bbox_component.statistics.max

                    if all(v is not None for v in [xmin_val, ymin_val, xmax_val, ymax_val]):
                        min_val = f"({xmin_val:.6f}, {ymin_val:.6f})"
                        max_val = f"({xmax_val:.6f}, {ymax_val:.6f})"

                # For non-geo columns, get min/max from column statistics
                if not is_geo and col.is_stats_set:
                    stats = col.statistics
                    if stats.has_min_max:
                        try:
                            min_val = str(stats.min)
                            max_val = str(stats.max)
                            # Truncate if too long
                            if len(min_val) > 20:
                                min_val = min_val[:17] + "..."
                            if len(max_val) > 20:
                                max_val = max_val[:17] + "..."
                        except Exception:
                            pass

                table.add_row(
                    col_name_display,
                    type_display,
                    format_size(col.total_compressed_size),
                    format_size(col.total_uncompressed_size),
                    col.compression,
                    min_val,
                    max_val,
                )

            console.print(table)

        # Show info about remaining row groups if limited
        if row_groups_limit is not None and num_rg_to_show < parquet_metadata.num_row_groups:
            remaining = parquet_metadata.num_row_groups - num_rg_to_show
            console.print()
            console.print(f"  [dim]... and {remaining} more row group(s)[/dim]")
            console.print(
                f"  [dim]Use --row-groups {parquet_metadata.num_row_groups} to see all row groups[/dim]"
            )

        console.print()


def format_parquet_geo_metadata(
    parquet_file: str, json_output: bool, row_groups_limit: Optional[int] = 1
) -> None:
    """
    Format and output geospatial metadata from Parquet format specification.

    Reads metadata according to the Apache Parquet geospatial specification:
    https://github.com/apache/parquet-format/blob/master/Geospatial.md

    Args:
        parquet_file: Path to the parquet file
        json_output: Whether to output as JSON
        row_groups_limit: Number of row groups to read stats from
    """
    safe_url = safe_file_url(parquet_file, verbose=False)

    with fsspec.open(safe_url, "rb") as f:
        pf = pq.ParquetFile(f)
        schema = pf.schema_arrow
        parquet_metadata = pf.metadata

    # Get Parquet schema string for better type detection
    parquet_schema_str = str(parquet_metadata.schema)

    # Extract geospatial metadata from schema
    # ONLY include columns with Parquet Geometry/Geography logical types
    # Do NOT make assumptions based on column names
    geo_columns_info = {}

    # Find bbox struct columns associated with geometry columns
    bbox_columns = {}  # Maps geometry column name to bbox struct column name
    for field in schema:
        if str(field.type).startswith("struct<") and "xmin" in str(field.type):
            # This looks like a bbox struct - try to find associated geometry column
            # Pattern 1: geometry -> geometry_bbox
            if field.name.endswith("_bbox"):
                base_name = field.name[:-5]  # Remove '_bbox'
                if base_name in [f.name for f in schema]:
                    bbox_columns[base_name] = field.name
            # Pattern 2: Just named 'bbox' - associate with any geometry column found
            elif field.name == "bbox":
                # Will associate after we find geometry columns
                pass

    for field in schema:
        geo_type = detect_geo_logical_type(field, parquet_schema_str)
        if geo_type:
            # Check if there's a bbox struct named just 'bbox' to associate
            for f in schema:
                if f.name == "bbox" and str(f.type).startswith("struct<") and "xmin" in str(f.type):
                    bbox_columns[field.name] = f.name

            col_info = {
                "logical_type": geo_type,
                "geometry_type": None,
                "coordinate_dimension": None,
                "crs": None,
                "edges": None,
                "row_group_stats": [],
            }

            # Parse geometry type details from Parquet schema
            geom_details = parse_geometry_type_from_schema(field.name, parquet_schema_str)
            if geom_details:
                col_info["geometry_type"] = geom_details.get("geometry_type")
                col_info["coordinate_dimension"] = geom_details.get("coordinate_dimension")
                # CRS from Parquet schema takes precedence
                if geom_details.get("crs"):
                    col_info["crs"] = geom_details.get("crs")
                # Algorithm (edge interpretation) for Geography type
                if geom_details.get("algorithm"):
                    col_info["edges"] = geom_details.get("algorithm")

            # If no CRS from schema, try to extract from field metadata
            if not col_info["crs"] and field.metadata:
                for key, value in field.metadata.items():
                    key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
                    if "crs" in key_str.lower():
                        value_str = (
                            value.decode("utf-8") if isinstance(value, bytes) else str(value)
                        )
                        try:
                            col_info["crs"] = json.loads(value_str)
                        except Exception:
                            col_info["crs"] = value_str

            geo_columns_info[field.name] = col_info

    # Extract statistics from row groups
    # Always read ALL row groups for overall bbox calculation
    num_rg_to_check = parquet_metadata.num_row_groups

    # Determine how many row groups to show in output
    num_rg_to_show = parquet_metadata.num_row_groups
    if row_groups_limit is not None:
        num_rg_to_show = min(row_groups_limit, parquet_metadata.num_row_groups)

    for col_name in geo_columns_info.keys():
        for rg_idx in range(num_rg_to_check):
            rg = parquet_metadata.row_group(rg_idx)
            rg_stats = {"row_group": rg_idx}

            # ONLY get bbox from associated struct column (not from geometry column's own WKB statistics)
            if col_name in bbox_columns:
                bbox_col_name = bbox_columns[col_name]
                # Look for the bbox struct components in this row group
                xmin_val = None
                ymin_val = None
                xmax_val = None
                ymax_val = None

                for col_idx in range(rg.num_columns):
                    bbox_component = rg.column(col_idx)
                    path = bbox_component.path_in_schema

                    if path == f"{bbox_col_name}.xmin" and bbox_component.is_stats_set:
                        if bbox_component.statistics.has_min_max:
                            xmin_val = bbox_component.statistics.min
                    elif path == f"{bbox_col_name}.ymin" and bbox_component.is_stats_set:
                        if bbox_component.statistics.has_min_max:
                            ymin_val = bbox_component.statistics.min
                    elif path == f"{bbox_col_name}.xmax" and bbox_component.is_stats_set:
                        if bbox_component.statistics.has_min_max:
                            xmax_val = bbox_component.statistics.max
                    elif path == f"{bbox_col_name}.ymax" and bbox_component.is_stats_set:
                        if bbox_component.statistics.has_min_max:
                            ymax_val = bbox_component.statistics.max

                if all(v is not None for v in [xmin_val, ymin_val, xmax_val, ymax_val]):
                    rg_stats["xmin"] = xmin_val
                    rg_stats["ymin"] = ymin_val
                    rg_stats["xmax"] = xmax_val
                    rg_stats["ymax"] = ymax_val

            # Get null count from geometry column
            for col_idx in range(rg.num_columns):
                col = rg.column(col_idx)
                if col.path_in_schema == col_name:
                    if col.is_stats_set:
                        stats = col.statistics
                        if stats.has_null_count:
                            rg_stats["null_count"] = stats.null_count
                    break

            if rg_stats:
                geo_columns_info[col_name]["row_group_stats"].append(rg_stats)

    if json_output:
        # JSON output
        output = {
            "geospatial_columns": geo_columns_info,
            "row_groups_examined": num_rg_to_check,
            "total_row_groups": parquet_metadata.num_row_groups,
        }
        click.echo(json.dumps(output, indent=2))
    else:
        # Human-readable output
        console = Console()
        console.print()
        console.print("[bold]Parquet Geo Metadata[/bold]")
        console.print("━" * 60)

        if not geo_columns_info:
            console.print("[yellow]No geospatial columns detected in Parquet metadata.[/yellow]")
            console.print()
            console.print(
                "[dim]Note: This shows metadata from the Parquet format specification.[/dim]"
            )
            console.print(
                "[dim]For GeoParquet metadata, see the 'GeoParquet Metadata' section.[/dim]"
            )
        else:
            # Show message about row groups being read
            if row_groups_limit is not None and row_groups_limit < parquet_metadata.num_row_groups:
                console.print(
                    f"\n[dim]Showing statistics for {num_rg_to_show} of {parquet_metadata.num_row_groups} row group(s)[/dim]"
                )
                console.print(
                    f"[dim](Overall bbox calculated from all {parquet_metadata.num_row_groups} row groups)[/dim]"
                )
            else:
                console.print(
                    f"\n[dim]Reading from {parquet_metadata.num_row_groups} row group(s)[/dim]"
                )

            for col_name, col_info in geo_columns_info.items():
                console.print(f"\n  [cyan bold]{col_name}[/cyan bold]:")

                # Logical type (GEOMETRY or GEOGRAPHY)
                if col_info["logical_type"]:
                    console.print(f"    Type: {col_info['logical_type']}")
                else:
                    console.print("    Type: [dim]Not present - assumed Geometry[/dim]")

                # Geometry type and coordinate dimension
                geom_type = col_info.get("geometry_type")
                coord_dim = col_info.get("coordinate_dimension")

                if geom_type and coord_dim:
                    console.print(f"    Geometry Type: {geom_type} {coord_dim}")
                elif geom_type:
                    console.print(f"    Geometry Type: {geom_type}")
                elif coord_dim:
                    console.print(f"    Coordinate Dimension: {coord_dim}")
                else:
                    console.print(
                        "    Geometry Type: [dim]Not present - geometry types are unknown[/dim]"
                    )

                # CRS
                if col_info["crs"]:
                    console.print(f"    CRS: {col_info['crs']}")
                else:
                    console.print("    CRS: [dim]Not present - OGC:CRS84 (default value)[/dim]")

                # Edge interpretation (only for GEOGRAPHY)
                if col_info["logical_type"] == "Geography":
                    if col_info["edges"]:
                        console.print(f"    Edges: {col_info['edges']}")
                    else:
                        console.print(
                            "    Edges: [dim]Not present - spherical (default value)[/dim]"
                        )
                else:
                    console.print("    Edges: [dim]N/A (only applies to Geography type)[/dim]")

                # Calculate overall bbox from all row groups
                overall_xmin = None
                overall_ymin = None
                overall_xmax = None
                overall_ymax = None

                for rg_stat in col_info["row_group_stats"]:
                    if all(k in rg_stat for k in ["xmin", "ymin", "xmax", "ymax"]):
                        if overall_xmin is None:
                            overall_xmin = rg_stat["xmin"]
                            overall_ymin = rg_stat["ymin"]
                            overall_xmax = rg_stat["xmax"]
                            overall_ymax = rg_stat["ymax"]
                        else:
                            overall_xmin = min(overall_xmin, rg_stat["xmin"])
                            overall_ymin = min(overall_ymin, rg_stat["ymin"])
                            overall_xmax = max(overall_xmax, rg_stat["xmax"])
                            overall_ymax = max(overall_ymax, rg_stat["ymax"])

                if overall_xmin is not None:
                    console.print(
                        f"    Overall Bbox: [{overall_xmin:.6f}, {overall_ymin:.6f}, "
                        f"{overall_xmax:.6f}, {overall_ymax:.6f}]"
                    )

                # Row group statistics (only show first num_rg_to_show)
                if col_info["row_group_stats"]:
                    console.print("    Row Group Statistics:")
                    for idx, rg_stat in enumerate(col_info["row_group_stats"]):
                        # Only show first num_rg_to_show row groups
                        if idx >= num_rg_to_show:
                            break

                        rg_id = rg_stat["row_group"]
                        console.print(f"      Row Group {rg_id}:")
                        if "null_count" in rg_stat:
                            console.print(f"        Null Count: {rg_stat['null_count']}")

                        # Display bbox if we extracted it
                        if all(k in rg_stat for k in ["xmin", "ymin", "xmax", "ymax"]):
                            console.print(
                                f"        Bbox: [{rg_stat['xmin']:.6f}, {rg_stat['ymin']:.6f}, "
                                f"{rg_stat['xmax']:.6f}, {rg_stat['ymax']:.6f}]"
                            )
                        elif rg_stat.get("has_min_max"):
                            console.print(
                                "        [dim]Bbox statistics available but format not parseable[/dim]"
                            )

                    # Show info about remaining row groups if limited
                    if len(col_info["row_group_stats"]) > num_rg_to_show:
                        remaining = len(col_info["row_group_stats"]) - num_rg_to_show
                        console.print(f"      [dim]... and {remaining} more row group(s)[/dim]")
                        console.print(
                            f"      [dim]Use --row-groups {parquet_metadata.num_row_groups} to see all row groups[/dim]"
                        )

        console.print()


def format_geoparquet_metadata(parquet_file: str, json_output: bool) -> None:
    """
    Format and output GeoParquet metadata from the 'geo' key.

    Args:
        parquet_file: Path to the parquet file
        json_output: Whether to output as JSON
    """
    metadata, _ = get_parquet_metadata(parquet_file, verbose=False)
    geo_meta = parse_geo_metadata(metadata, verbose=False)

    if not geo_meta:
        if json_output:
            click.echo(json.dumps(None, indent=2))
        else:
            console = Console()
            console.print()
            console.print("[bold]GeoParquet Metadata[/bold]")
            console.print("━" * 60)
            console.print("[yellow]No GeoParquet metadata found in this file.[/yellow]")
            console.print()
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

                            console.print(
                                "    [dim](PROJJSON format - use --json to see full CRS definition)[/dim]"
                            )
                        else:
                            # Other CRS format
                            console.print(f"    CRS: {json.dumps(crs_info, indent=6)}")
                    else:
                        console.print(f"    CRS: {crs_info}")
                else:
                    # Default CRS per GeoParquet spec
                    console.print("    CRS: [dim]Not present - OGC:CRS84 (default value)[/dim]")

                # Orientation
                if "orientation" in col_meta:
                    console.print(f"    Orientation: {col_meta['orientation']}")
                else:
                    console.print(
                        "    Orientation: [dim]Not present - counterclockwise (default value)[/dim]"
                    )

                # Edges
                if "edges" in col_meta:
                    console.print(f"    Edges: {col_meta['edges']}")
                else:
                    console.print("    Edges: [dim]Not present - planar (default value)[/dim]")

                # Bbox
                if "bbox" in col_meta:
                    bbox = col_meta["bbox"]
                    if isinstance(bbox, list) and len(bbox) == 4:
                        console.print(
                            f"    Bbox: [{bbox[0]:.6f}, {bbox[1]:.6f}, {bbox[2]:.6f}, {bbox[3]:.6f}]"
                        )
                    else:
                        console.print(f"    Bbox: {bbox}")

                # Epoch
                if "epoch" in col_meta:
                    console.print(f"    Epoch: {col_meta['epoch']}")
                else:
                    console.print("    Epoch: [dim]Not present[/dim]")

                # Covering
                if "covering" in col_meta:
                    console.print("    Covering:")
                    covering = col_meta["covering"]
                    for cover_type, cover_info in covering.items():
                        if cover_type == "bbox" and isinstance(cover_info, dict):
                            # Format bbox covering more concisely
                            if all(k in cover_info for k in ["xmin", "ymin", "xmax", "ymax"]):
                                # All bbox components present
                                bbox_col = cover_info["xmin"][0]  # Get the column name
                                console.print("      bbox:")
                                console.print(f"        Column: {bbox_col}")
                                console.print(f"        xmin: {bbox_col}.xmin")
                                console.print(f"        ymin: {bbox_col}.ymin")
                                console.print(f"        xmax: {bbox_col}.xmax")
                                console.print(f"        ymax: {bbox_col}.ymax")
                            else:
                                # Partial bbox, show as JSON
                                console.print(
                                    f"      {cover_type}: {json.dumps(cover_info, indent=8)}"
                                )
                        else:
                            # Other covering types (e.g., H3, S2)
                            if isinstance(cover_info, dict):
                                console.print(f"      {cover_type}:")
                                for key, value in cover_info.items():
                                    console.print(f"        {key}: {value}")
                            else:
                                console.print(f"      {cover_type}: {cover_info}")
                else:
                    console.print("    Covering: [dim]Not present[/dim]")

        console.print()


def format_all_metadata(
    parquet_file: str, json_output: bool, row_groups_limit: Optional[int] = 1
) -> None:
    """
    Format and output all three metadata sections.

    Args:
        parquet_file: Path to the parquet file
        json_output: Whether to output as JSON
        row_groups_limit: Number of row groups to display
    """
    if json_output:
        # For JSON, combine all metadata into one object
        # Get primary geometry column for highlighting
        metadata, _ = get_parquet_metadata(parquet_file, verbose=False)
        geo_meta = parse_geo_metadata(metadata, verbose=False)
        primary_col = geo_meta.get("primary_column") if geo_meta else None

        # We need to manually construct the combined JSON output
        # This is a simplified version - in production you'd want to extract the actual data
        output = {
            "parquet_metadata": "See --parquet flag for full output",
            "parquet_geo_metadata": "See --parquet-geo flag for full output",
            "geoparquet_metadata": geo_meta,
        }
        click.echo(json.dumps(output, indent=2))
    else:
        # Terminal output - show all three sections
        # Get primary geometry column for Parquet metadata highlighting
        metadata, _ = get_parquet_metadata(parquet_file, verbose=False)
        geo_meta = parse_geo_metadata(metadata, verbose=False)
        primary_col = geo_meta.get("primary_column") if geo_meta else None

        # Section 1: Parquet File Metadata
        format_parquet_metadata_enhanced(parquet_file, False, row_groups_limit, primary_col)

        # Section 2: Parquet Geo Metadata
        format_parquet_geo_metadata(parquet_file, False, row_groups_limit)

        # Section 3: GeoParquet Metadata
        format_geoparquet_metadata(parquet_file, False)
