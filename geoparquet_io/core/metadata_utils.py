"""
Utilities for extracting and formatting GeoParquet metadata.

Provides functions to extract and format metadata from GeoParquet files,
including Parquet file metadata, Parquet geospatial metadata, and GeoParquet metadata.
"""

import json
from typing import Any

from rich.console import Console
from rich.table import Table
from rich.text import Text

from geoparquet_io.core.common import (
    format_size,
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


def detect_geo_logical_type(field, parquet_schema_str: str | None = None) -> str | None:
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
) -> dict[str, Any] | None:
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


def _detect_geo_columns(schema, parquet_schema_str: str) -> dict[str, str]:
    """Detect geometry/geography columns from schema."""
    geo_columns = {}
    for field in schema:
        geo_type = detect_geo_logical_type(field, parquet_schema_str)
        if geo_type:
            geo_columns[field.name] = geo_type
    return geo_columns


def _detect_bbox_columns(schema, geo_columns: dict[str, str]) -> dict[str, str]:
    """Find bbox struct columns associated with geometry columns."""
    bbox_columns = {}
    for field in schema:
        type_str = str(field.type)
        if not (type_str.startswith("struct<") and "xmin" in type_str):
            continue
        # Pattern 1: geometry -> geometry_bbox
        if field.name.endswith("_bbox"):
            base_name = field.name[:-5]
            if base_name in [f.name for f in schema]:
                bbox_columns[base_name] = field.name
        # Pattern 2: Just named 'bbox' - associate with geometry columns
        elif field.name == "bbox":
            for geom_name in geo_columns.keys():
                bbox_columns[geom_name] = field.name
    return bbox_columns


def _extract_rg_bbox(rg, bbox_col_name: str) -> dict[str, float] | None:
    """Extract bbox values from a row group's bbox struct column."""
    values = {"xmin": None, "ymin": None, "xmax": None, "ymax": None}
    for col_idx in range(rg.num_columns):
        col = rg.column(col_idx)
        path = col.path_in_schema
        if not col.is_stats_set or not col.statistics.has_min_max:
            continue
        if path == f"{bbox_col_name}.xmin":
            values["xmin"] = col.statistics.min
        elif path == f"{bbox_col_name}.ymin":
            values["ymin"] = col.statistics.min
        elif path == f"{bbox_col_name}.xmax":
            values["xmax"] = col.statistics.max
        elif path == f"{bbox_col_name}.ymax":
            values["ymax"] = col.statistics.max
    if all(v is not None for v in values.values()):
        return values
    return None


def _build_column_dict(col, is_geo: bool, geo_type: str | None) -> dict[str, Any]:
    """Build column metadata dictionary for JSON output."""
    col_dict = {
        "path_in_schema": col.path_in_schema,
        "file_offset": col.file_offset,
        "file_path": col.file_path,
        "physical_type": col.physical_type,
        "num_values": col.num_values,
        "total_compressed_size": col.total_compressed_size,
        "total_uncompressed_size": col.total_uncompressed_size,
        "compression": col.compression,
        "encodings": [str(enc) for enc in col.encodings] if hasattr(col, "encodings") else [],
        "is_geo": is_geo,
        "geo_type": geo_type,
    }
    if col.is_stats_set:
        stats = col.statistics
        col_dict["statistics"] = {
            "has_min_max": getattr(stats, "has_min_max", False),
            "has_null_count": getattr(stats, "has_null_count", False),
            "null_count": getattr(stats, "null_count", None),
        }
        if stats.has_min_max and not is_geo:
            try:
                col_dict["statistics"]["min"] = str(stats.min)
                col_dict["statistics"]["max"] = str(stats.max)
            except Exception:
                pass
    return col_dict


def _extract_crs_from_field_metadata(field) -> Any | None:
    """Extract CRS from field metadata if present."""
    if not field.metadata:
        return None
    for key, value in field.metadata.items():
        key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        if "crs" in key_str.lower():
            value_str = value.decode("utf-8") if isinstance(value, bytes) else str(value)
            try:
                return json.loads(value_str)
            except Exception:
                return value_str
    return None


def _build_geo_column_info(field, parquet_schema_str: str) -> dict[str, Any]:
    """Build geo column info dictionary from a schema field."""
    geo_type = detect_geo_logical_type(field, parquet_schema_str)
    col_info = {
        "logical_type": geo_type,
        "geometry_type": None,
        "coordinate_dimension": None,
        "crs": None,
        "edges": None,
        "row_group_stats": [],
    }
    geom_details = parse_geometry_type_from_schema(field.name, parquet_schema_str)
    if geom_details:
        col_info["geometry_type"] = geom_details.get("geometry_type")
        col_info["coordinate_dimension"] = geom_details.get("coordinate_dimension")
        if geom_details.get("crs"):
            col_info["crs"] = geom_details.get("crs")
        if geom_details.get("algorithm"):
            col_info["edges"] = geom_details.get("algorithm")
    if not col_info["crs"]:
        col_info["crs"] = _extract_crs_from_field_metadata(field)
    return col_info


def _extract_rg_stats(rg, col_name: str, bbox_columns: dict) -> dict[str, Any]:
    """Extract row group statistics for a geometry column."""
    rg_stats: dict[str, Any] = {}
    # Get bbox from associated struct column
    if col_name in bbox_columns:
        bbox = _extract_rg_bbox(rg, bbox_columns[col_name])
        if bbox:
            rg_stats.update(bbox)
    # Get null count from geometry column
    for col_idx in range(rg.num_columns):
        col = rg.column(col_idx)
        if col.path_in_schema == col_name and col.is_stats_set:
            if col.statistics.has_null_count:
                rg_stats["null_count"] = col.statistics.null_count
            break
    return rg_stats


def _calculate_overall_bbox(row_group_stats: list[dict]) -> dict[str, float] | None:
    """Calculate overall bbox from row group statistics."""
    overall = {"xmin": None, "ymin": None, "xmax": None, "ymax": None}
    for rg_stat in row_group_stats:
        if not all(k in rg_stat for k in ["xmin", "ymin", "xmax", "ymax"]):
            continue
        if overall["xmin"] is None:
            overall = {k: rg_stat[k] for k in ["xmin", "ymin", "xmax", "ymax"]}
        else:
            overall["xmin"] = min(overall["xmin"], rg_stat["xmin"])
            overall["ymin"] = min(overall["ymin"], rg_stat["ymin"])
            overall["xmax"] = max(overall["xmax"], rg_stat["xmax"])
            overall["ymax"] = max(overall["ymax"], rg_stat["ymax"])
    return overall if overall["xmin"] is not None else None


def _get_column_minmax(col, is_geo: bool, bbox_columns: dict, rg) -> tuple[str, str]:
    """Get min/max display values for a column."""
    col_name = col.path_in_schema
    if is_geo and col_name in bbox_columns:
        bbox = _extract_rg_bbox(rg, bbox_columns[col_name])
        if bbox:
            return (
                f"({bbox['xmin']:.6f}, {bbox['ymin']:.6f})",
                f"({bbox['xmax']:.6f}, {bbox['ymax']:.6f})",
            )
    elif not is_geo and col.is_stats_set and col.statistics.has_min_max:
        try:
            min_val = str(col.statistics.min)
            max_val = str(col.statistics.max)
            if len(min_val) > 20:
                min_val = min_val[:17] + "..."
            if len(max_val) > 20:
                max_val = max_val[:17] + "..."
            return min_val, max_val
        except Exception:
            pass
    return "-", "-"


def has_parquet_geo_row_group_stats(parquet_file: str, geometry_column: str | None = None) -> dict:
    """
    Check if file has row group statistics for geometry columns.

    For files with native Parquet geo types, checks if bbox struct columns exist
    with proper min/max statistics in row groups that can be used for spatial filtering.

    Args:
        parquet_file: Path to the parquet file
        geometry_column: Name of the geometry column (auto-detected if None)

    Returns:
        dict with:
            - has_stats: bool - Whether valid row group stats exist
            - stats_source: str - "bbox_struct" if bbox struct column has stats, None otherwise
            - sample_bbox: list - [xmin, ymin, xmax, ymax] from first row group, or None
    """
    from geoparquet_io.core.duckdb_metadata import (
        detect_geometry_columns,
        get_per_row_group_bbox_stats,
        has_bbox_column,
    )

    result = {
        "has_stats": False,
        "stats_source": None,
        "sample_bbox": None,
    }

    safe_url = safe_file_url(parquet_file, verbose=False)

    # Auto-detect geometry column if not specified
    if not geometry_column:
        geo_columns = detect_geometry_columns(safe_url)
        if geo_columns:
            geometry_column = next(iter(geo_columns.keys()))

    if not geometry_column:
        return result

    # Check for bbox column using DuckDB
    has_bbox, bbox_col_name = has_bbox_column(safe_url)

    if not has_bbox or not bbox_col_name:
        return result

    # Get row group stats for first row group
    rg_stats = get_per_row_group_bbox_stats(safe_url, bbox_col_name)

    if rg_stats and len(rg_stats) > 0:
        first_rg = rg_stats[0]
        result["has_stats"] = True
        result["stats_source"] = "bbox_struct"
        result["sample_bbox"] = [
            first_rg["xmin"],
            first_rg["ymin"],
            first_rg["xmax"],
            first_rg["ymax"],
        ]

    return result


def extract_bbox_from_row_group_stats(
    parquet_file: str,
    geometry_column: str,
) -> list[float] | None:
    """
    Extract overall bbox from row group statistics for a geometry column.

    This looks for a bbox struct column associated with the geometry column
    and calculates the overall bbox from the min/max statistics across all row groups.

    Args:
        parquet_file: Path to the parquet file
        geometry_column: Name of the geometry column

    Returns:
        list: [xmin, ymin, xmax, ymax] or None if bbox cannot be calculated
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_bbox_from_row_group_stats,
        has_bbox_column,
    )

    safe_url = safe_file_url(parquet_file, verbose=False)

    # Check for bbox column using DuckDB
    has_bbox, bbox_col_name = has_bbox_column(safe_url)

    if not has_bbox or not bbox_col_name:
        return None

    # Get overall bbox from row group stats using DuckDB
    return get_bbox_from_row_group_stats(safe_url, bbox_col_name)


def _build_row_group_json(rg_id: int, cols_in_rg: list, geo_columns: dict) -> dict:
    """Build JSON representation for a single row group."""
    total_size = sum(c.get("total_compressed_size", 0) or 0 for c in cols_in_rg)
    rg_dict = {
        "id": rg_id,
        "num_columns": len({c.get("path_in_schema", "") for c in cols_in_rg}),
        "total_byte_size": total_size,
        "columns": [],
    }

    for col in cols_in_rg:
        col_name = col.get("path_in_schema", "")
        is_geo = col_name in geo_columns
        col_dict = {
            "path_in_schema": col_name,
            "physical_type": col.get("type", ""),
            "total_compressed_size": col.get("total_compressed_size", 0),
            "total_uncompressed_size": col.get("total_uncompressed_size", 0),
            "compression": col.get("compression", ""),
            "is_geo": is_geo,
            "geo_type": geo_columns.get(col_name),
        }
        if col.get("stats_min") is not None:
            col_dict["statistics"] = {
                "min": str(col.get("stats_min")),
                "max": str(col.get("stats_max")),
            }
        rg_dict["columns"].append(col_dict)

    return rg_dict


def _format_parquet_metadata_json(
    file_meta: dict,
    num_columns: int,
    schema_str: str,
    rg_columns: dict,
    geo_columns: dict,
    row_groups_limit: int | None,
) -> None:
    """Output Parquet metadata as JSON."""
    num_rows = file_meta.get("num_rows", 0)
    num_row_groups = file_meta.get("num_row_groups", 0)
    serialized_size = file_meta.get("file_size_bytes", 0)

    metadata_dict = {
        "num_rows": num_rows,
        "num_row_groups": num_row_groups,
        "num_columns": num_columns,
        "serialized_size": serialized_size,
        "schema": schema_str,
        "row_groups": [],
    }

    num_rg_to_show = num_row_groups
    if row_groups_limit is not None:
        num_rg_to_show = min(row_groups_limit, num_row_groups)

    for i in range(num_rg_to_show):
        cols_in_rg = rg_columns.get(i, [])
        rg_dict = _build_row_group_json(i, cols_in_rg, geo_columns)
        metadata_dict["row_groups"].append(rg_dict)

    print(json.dumps(metadata_dict, indent=2))


def _print_row_group_table(console: Console, cols_in_rg: list, geo_columns: dict) -> None:
    """Print a table of columns for a row group."""
    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
    table.add_column("Column", style="white")
    table.add_column("Type", style="blue", min_width=24)
    table.add_column("Compressed", style="yellow", justify="right")
    table.add_column("Uncompressed", style="yellow", justify="right")
    table.add_column("Compression", style="green")
    table.add_column("MinValue", style="magenta")
    table.add_column("MaxValue", style="magenta")

    for col in cols_in_rg:
        col_name = col.get("path_in_schema", "")
        is_geo = col_name in geo_columns
        geo_type = geo_columns.get(col_name)

        col_name_display = Text(f"🌍 {col_name}", style="cyan bold") if is_geo else col_name
        type_display = (
            f"{col.get('type', '')}({geo_type})" if is_geo and geo_type else col.get("type", "")
        )

        min_val = str(col.get("stats_min", "-"))[:20] if col.get("stats_min") else "-"
        max_val = str(col.get("stats_max", "-"))[:20] if col.get("stats_max") else "-"

        table.add_row(
            col_name_display,
            type_display,
            format_size(col.get("total_compressed_size", 0) or 0),
            format_size(col.get("total_uncompressed_size", 0) or 0),
            col.get("compression", ""),
            min_val,
            max_val,
        )

    console.print(table)


def _format_parquet_metadata_terminal(
    file_meta: dict,
    num_columns: int,
    schema_str: str,
    rg_columns: dict,
    geo_columns: dict,
    row_groups_limit: int | None,
) -> None:
    """Output Parquet metadata as human-readable terminal output."""
    console = Console()
    num_rows = file_meta.get("num_rows", 0)
    num_row_groups = file_meta.get("num_row_groups", 0)

    console.print()
    console.print("[bold]Parquet File Metadata[/bold]")
    console.print("━" * 60)
    console.print(f"Total Rows: [cyan]{num_rows:,}[/cyan]")
    console.print(f"Row Groups: [cyan]{num_row_groups}[/cyan]")
    console.print(f"Columns: [cyan]{num_columns}[/cyan]")
    console.print()
    console.print("[bold]Schema:[/bold]")
    console.print(f"  {schema_str}")

    num_rg_to_show = num_row_groups
    if row_groups_limit is not None:
        num_rg_to_show = min(row_groups_limit, num_row_groups)

    console.print()
    if row_groups_limit is not None and row_groups_limit < num_row_groups:
        console.print(f"[bold]Row Groups (showing {num_rg_to_show} of {num_row_groups}):[/bold]")
    else:
        console.print(f"[bold]Row Groups ({num_row_groups}):[/bold]")

    for i in range(num_rg_to_show):
        cols_in_rg = rg_columns.get(i, [])
        total_size = sum(c.get("total_compressed_size", 0) or 0 for c in cols_in_rg)
        console.print(f"\n  [cyan bold]Row Group {i}[/cyan bold]:")
        console.print(f"    Total Size: {format_size(total_size)}")
        _print_row_group_table(console, cols_in_rg, geo_columns)

    if row_groups_limit is not None and num_rg_to_show < num_row_groups:
        remaining = num_row_groups - num_rg_to_show
        console.print()
        console.print(f"  [dim]... and {remaining} more row group(s)[/dim]")
        console.print(f"  [dim]Use --row-groups {num_row_groups} to see all row groups[/dim]")

    console.print()


def format_parquet_metadata_enhanced(
    parquet_file: str,
    json_output: bool,
    row_groups_limit: int | None = 1,
    primary_geom_col: str | None = None,
) -> None:
    """
    Format and output enhanced Parquet file metadata with geo column highlighting.

    Args:
        parquet_file: Path to the parquet file
        json_output: Whether to output as JSON
        row_groups_limit: Number of row groups to display (None for all)
        primary_geom_col: Primary geometry column name (for highlighting)
    """
    from geoparquet_io.core.duckdb_metadata import (
        detect_geometry_columns,
        get_file_metadata,
        get_row_group_metadata,
        get_schema_info,
    )

    safe_url = safe_file_url(parquet_file, verbose=False)

    file_meta = get_file_metadata(safe_url)
    schema_info = get_schema_info(safe_url)
    row_group_meta = get_row_group_metadata(safe_url)
    geo_columns = detect_geometry_columns(safe_url)

    num_columns = len([c for c in schema_info if c.get("name") and "." not in c.get("name", "")])
    schema_str = ", ".join(
        f"{c['name']}: {c.get('type', 'unknown')}"
        for c in schema_info
        if c.get("name") and "." not in c.get("name", "")
    )

    rg_columns: dict[int, list] = {}
    for col in row_group_meta:
        rg_id = col.get("row_group_id", 0)
        if rg_id not in rg_columns:
            rg_columns[rg_id] = []
        rg_columns[rg_id].append(col)

    if json_output:
        _format_parquet_metadata_json(
            file_meta, num_columns, schema_str, rg_columns, geo_columns, row_groups_limit
        )
    else:
        _format_parquet_metadata_terminal(
            file_meta, num_columns, schema_str, rg_columns, geo_columns, row_groups_limit
        )


def _print_geo_column_info(console: Console, col_name: str, col_info: dict) -> None:
    """Print basic info for a geo column (type, geometry type, CRS, edges)."""
    console.print(f"\n  [cyan bold]{col_name}[/cyan bold]:")

    # Logical type
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
        console.print("    Geometry Type: [dim]Not present - geometry types are unknown[/dim]")

    # CRS
    if col_info["crs"]:
        console.print(f"    CRS: {col_info['crs']}")
    else:
        console.print("    CRS: [dim]Not present - OGC:CRS84 (default value)[/dim]")

    # Edge interpretation
    if col_info["logical_type"] == "Geography":
        if col_info["edges"]:
            console.print(f"    Edges: {col_info['edges']}")
        else:
            console.print("    Edges: [dim]Not present - spherical (default value)[/dim]")
    else:
        console.print("    Edges: [dim]N/A (only applies to Geography type)[/dim]")


def _print_geo_column_stats(
    console: Console, col_info: dict, num_rg_to_show: int, num_row_groups: int
) -> None:
    """Print bbox and row group statistics for a geo column."""
    overall_bbox = _calculate_overall_bbox(col_info["row_group_stats"])
    if overall_bbox:
        console.print(
            f"    Overall Bbox: [{overall_bbox['xmin']:.6f}, {overall_bbox['ymin']:.6f}, "
            f"{overall_bbox['xmax']:.6f}, {overall_bbox['ymax']:.6f}]"
        )

    if not col_info["row_group_stats"]:
        return

    console.print("    Row Group Statistics:")
    for idx, rg_stat in enumerate(col_info["row_group_stats"]):
        if idx >= num_rg_to_show:
            break
        rg_id = rg_stat["row_group"]
        console.print(f"      Row Group {rg_id}:")
        if "null_count" in rg_stat:
            console.print(f"        Null Count: {rg_stat['null_count']}")
        if all(k in rg_stat for k in ["xmin", "ymin", "xmax", "ymax"]):
            console.print(
                f"        Bbox: [{rg_stat['xmin']:.6f}, {rg_stat['ymin']:.6f}, "
                f"{rg_stat['xmax']:.6f}, {rg_stat['ymax']:.6f}]"
            )
        elif rg_stat.get("has_min_max"):
            console.print("        [dim]Bbox statistics available but format not parseable[/dim]")

    if len(col_info["row_group_stats"]) > num_rg_to_show:
        remaining = len(col_info["row_group_stats"]) - num_rg_to_show
        console.print(f"      [dim]... and {remaining} more row group(s)[/dim]")
        console.print(f"      [dim]Use --row-groups {num_row_groups} to see all row groups[/dim]")


def _format_parquet_geo_terminal(
    geo_columns_info: dict, num_row_groups: int, num_rg_to_show: int, row_groups_limit: int | None
) -> None:
    """Output Parquet geo metadata as human-readable terminal output."""
    console = Console()
    console.print()
    console.print("[bold]Parquet Geo Metadata[/bold]")
    console.print("━" * 60)

    if not geo_columns_info:
        console.print("[yellow]No geospatial columns detected in Parquet metadata.[/yellow]")
        console.print()
        console.print("[dim]Note: This shows metadata from the Parquet format specification.[/dim]")
        console.print("[dim]For GeoParquet metadata, see the 'GeoParquet Metadata' section.[/dim]")
        console.print()
        return

    if row_groups_limit is not None and row_groups_limit < num_row_groups:
        console.print(
            f"\n[dim]Showing statistics for {num_rg_to_show} of {num_row_groups} row group(s)[/dim]"
        )
        console.print(f"[dim](Overall bbox calculated from all {num_row_groups} row groups)[/dim]")
    else:
        console.print(f"\n[dim]Reading from {num_row_groups} row group(s)[/dim]")

    for col_name, col_info in geo_columns_info.items():
        _print_geo_column_info(console, col_name, col_info)
        _print_geo_column_stats(console, col_info, num_rg_to_show, num_row_groups)

    console.print()


def _build_geo_columns_info(schema_info: list, geo_columns: dict) -> dict:
    """Build geo column info dictionary from schema and detected geo columns."""
    from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type

    geo_columns_info = {}
    for col in schema_info:
        col_name = col.get("name", "")
        if col_name in geo_columns:
            logical_type = col.get("logical_type", "")
            parsed = parse_geometry_logical_type(logical_type) if logical_type else {}
            geo_columns_info[col_name] = {
                "logical_type": geo_columns.get(col_name),
                "geometry_type": parsed.get("geometry_type") if parsed else None,
                "coordinate_dimension": parsed.get("coordinate_dimension") if parsed else None,
                "crs": parsed.get("crs") if parsed else None,
                "edges": parsed.get("algorithm") if parsed else None,
                "row_group_stats": [],
            }
    return geo_columns_info


def format_parquet_geo_metadata(
    parquet_file: str, json_output: bool, row_groups_limit: int | None = 1
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
    from geoparquet_io.core.duckdb_metadata import (
        detect_geometry_columns,
        get_file_metadata,
        get_per_row_group_bbox_stats,
        get_schema_info,
        has_bbox_column,
    )

    safe_url = safe_file_url(parquet_file, verbose=False)

    file_meta = get_file_metadata(safe_url)
    schema_info = get_schema_info(safe_url)
    num_row_groups = file_meta.get("num_row_groups", 0)

    geo_columns = detect_geometry_columns(safe_url)
    has_bbox, bbox_col_name = has_bbox_column(safe_url)

    geo_columns_info = _build_geo_columns_info(schema_info, geo_columns)

    # Add bbox row group stats if bbox column exists
    if has_bbox and bbox_col_name:
        rg_bbox_stats = get_per_row_group_bbox_stats(safe_url, bbox_col_name)
        for col_name in geo_columns_info:
            for rg_stat in rg_bbox_stats:
                geo_columns_info[col_name]["row_group_stats"].append(
                    {
                        "row_group": rg_stat["row_group_id"],
                        "xmin": rg_stat["xmin"],
                        "ymin": rg_stat["ymin"],
                        "xmax": rg_stat["xmax"],
                        "ymax": rg_stat["ymax"],
                    }
                )

    num_rg_to_show = num_row_groups
    if row_groups_limit is not None:
        num_rg_to_show = min(row_groups_limit, num_row_groups)

    if json_output:
        output = {
            "geospatial_columns": geo_columns_info,
            "row_groups_examined": num_row_groups,
            "total_row_groups": num_row_groups,
        }
        print(json.dumps(output, indent=2))
    else:
        _format_parquet_geo_terminal(
            geo_columns_info, num_row_groups, num_rg_to_show, row_groups_limit
        )


def format_geoparquet_metadata(parquet_file: str, json_output: bool) -> None:
    """
    Format and output GeoParquet metadata from the 'geo' key.

    Args:
        parquet_file: Path to the parquet file
        json_output: Whether to output as JSON
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata

    safe_url = safe_file_url(parquet_file, verbose=False)
    geo_meta = get_geo_metadata(safe_url)

    if not geo_meta:
        if json_output:
            print(json.dumps(None, indent=2))
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
        print(json.dumps(geo_meta, indent=2))
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
    parquet_file: str, json_output: bool, row_groups_limit: int | None = 1
) -> None:
    """
    Format and output all three metadata sections.

    Args:
        parquet_file: Path to the parquet file
        json_output: Whether to output as JSON
        row_groups_limit: Number of row groups to display
    """
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata

    safe_url = safe_file_url(parquet_file, verbose=False)

    if json_output:
        # For JSON, combine all metadata into one object
        geo_meta = get_geo_metadata(safe_url)
        primary_col = geo_meta.get("primary_column") if geo_meta else None

        # We need to manually construct the combined JSON output
        # This is a simplified version - in production you'd want to extract the actual data
        output = {
            "parquet_metadata": "See --parquet flag for full output",
            "parquet_geo_metadata": "See --parquet-geo flag for full output",
            "geoparquet_metadata": geo_meta,
        }
        print(json.dumps(output, indent=2))
    else:
        # Terminal output - show all three sections
        geo_meta = get_geo_metadata(safe_url)
        primary_col = geo_meta.get("primary_column") if geo_meta else None

        # Section 1: Parquet File Metadata
        format_parquet_metadata_enhanced(parquet_file, False, row_groups_limit, primary_col)

        # Section 2: Parquet Geo Metadata
        format_parquet_geo_metadata(parquet_file, False, row_groups_limit)

        # Section 3: GeoParquet Metadata
        format_geoparquet_metadata(parquet_file, False)
