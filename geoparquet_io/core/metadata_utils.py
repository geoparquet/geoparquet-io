"""
Utilities for extracting and formatting GeoParquet metadata.

Provides functions to extract and format metadata from GeoParquet files,
including Parquet file metadata, Parquet geospatial metadata, and GeoParquet metadata.
"""

import json

from rich.console import Console
from rich.table import Table
from rich.text import Text

from geoparquet_io.core.common import format_size
from geoparquet_io.core.parquet_schema import root_schema_columns


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

    # Auto-detect geometry column if not specified
    if not geometry_column:
        geo_columns = detect_geometry_columns(parquet_file)
        if geo_columns:
            geometry_column = next(iter(geo_columns.keys()))

    if not geometry_column:
        return result

    # Check for bbox column using DuckDB
    has_bbox, bbox_col_name = has_bbox_column(parquet_file)

    if not has_bbox or not bbox_col_name:
        return result

    # Get row group stats for first row group
    rg_stats = get_per_row_group_bbox_stats(parquet_file, bbox_col_name)

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

    # Check for bbox column using DuckDB
    has_bbox, bbox_col_name = has_bbox_column(parquet_file)

    if not has_bbox or not bbox_col_name:
        return None

    # Get overall bbox from row group stats using DuckDB
    return get_bbox_from_row_group_stats(parquet_file, bbox_col_name)


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
        elif is_geo and col.get("geo_bbox"):
            geo_bbox = col["geo_bbox"]
            if geo_bbox.get("xmin") is not None:
                col_dict["statistics"] = {
                    "min": f"({geo_bbox['xmin']}, {geo_bbox['ymin']})",
                    "max": f"({geo_bbox['xmax']}, {geo_bbox['ymax']})",
                    "source": "geo_bbox",
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
    bloom_filter_info: list | None = None,
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

    # Add bloom filter info
    if bloom_filter_info:
        columns_with = [
            entry for entry in bloom_filter_info if entry["row_groups_with_bloom_filter"] > 0
        ]
        metadata_dict["bloom_filters"] = {
            "columns_with_bloom_filters": len(columns_with),
            "total_columns": len(bloom_filter_info),
            "details": bloom_filter_info,
        }

    print(json.dumps(metadata_dict, indent=2))


def _format_bbox_corner(x: float, y: float) -> str:
    """Format a bbox corner as (x, y), adapting precision to fit."""
    for fmt in (".2f", ".1f", ".0f"):
        result = f"({x:{fmt}}, {y:{fmt}})"
        if len(result) <= 24:
            return result
    return f"({x:.0f}, {y:.0f})"


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

        if is_geo and min_val == "-" and col.get("geo_bbox"):
            geo_bbox = col["geo_bbox"]
            if geo_bbox.get("xmin") is not None:
                min_val = _format_bbox_corner(geo_bbox["xmin"], geo_bbox["ymin"])
                max_val = _format_bbox_corner(geo_bbox["xmax"], geo_bbox["ymax"])

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


def _print_bloom_filter_summary(console: Console, bloom_filter_info: list | None) -> None:
    """Print bloom filter summary for inspect meta output."""
    if not bloom_filter_info:
        return

    columns_with = [
        entry for entry in bloom_filter_info if entry["row_groups_with_bloom_filter"] > 0
    ]

    console.print()
    console.print("[bold]Bloom Filters:[/bold]")

    if not columns_with:
        console.print("  [dim]No bloom filters detected[/dim]")
        return

    total_bytes = sum(entry["total_bloom_filter_bytes"] for entry in columns_with)
    console.print(
        f"  {len(columns_with)} of {len(bloom_filter_info)} column(s) "
        f"have bloom filters ({format_size(total_bytes)} total)"
    )

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
    table.add_column("Column", style="white")
    table.add_column("Coverage", style="green", justify="right")
    table.add_column("Size", style="yellow", justify="right")

    for entry in columns_with:
        table.add_row(
            entry["column_name"],
            f"{entry['bloom_filter_coverage_pct']:.0f}%",
            format_size(entry["total_bloom_filter_bytes"]),
        )

    console.print(table)


def _format_parquet_metadata_terminal(
    file_meta: dict,
    num_columns: int,
    schema_str: str,
    rg_columns: dict,
    geo_columns: dict,
    row_groups_limit: int | None,
    bloom_filter_info: list | None = None,
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

    # Bloom filter summary
    _print_bloom_filter_summary(console, bloom_filter_info)

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
        get_bloom_filter_info,
        get_file_metadata,
        get_row_group_metadata,
        get_schema_info,
    )

    file_meta = get_file_metadata(parquet_file)
    schema_info = get_schema_info(parquet_file)
    row_group_meta = get_row_group_metadata(parquet_file)
    geo_columns = detect_geometry_columns(parquet_file)
    bloom_filter_info = get_bloom_filter_info(parquet_file)

    root_columns = root_schema_columns(schema_info)
    num_columns = len(root_columns)
    schema_str = ", ".join(f"{c['name']}: {c.get('type', 'unknown')}" for c in root_columns)

    rg_columns: dict[int, list] = {}
    for col in row_group_meta:
        rg_id = col.get("row_group_id", 0)
        if rg_id not in rg_columns:
            rg_columns[rg_id] = []
        rg_columns[rg_id].append(col)

    if json_output:
        _format_parquet_metadata_json(
            file_meta,
            num_columns,
            schema_str,
            rg_columns,
            geo_columns,
            row_groups_limit,
            bloom_filter_info,
        )
    else:
        _format_parquet_metadata_terminal(
            file_meta,
            num_columns,
            schema_str,
            rg_columns,
            geo_columns,
            row_groups_limit,
            bloom_filter_info,
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
            logical_type = col.get("logical_type") or ""
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
        get_per_row_group_native_geo_stats,
        get_schema_info,
        has_bbox_column,
    )

    file_meta = get_file_metadata(parquet_file)
    schema_info = get_schema_info(parquet_file)
    num_row_groups = file_meta.get("num_row_groups", 0)

    geo_columns = detect_geometry_columns(parquet_file)
    has_bbox, bbox_col_name = has_bbox_column(parquet_file)

    geo_columns_info = _build_geo_columns_info(schema_info, geo_columns)

    # Add row group stats: try bbox column first, then native geo_bbox
    if has_bbox and bbox_col_name:
        rg_bbox_stats = get_per_row_group_bbox_stats(parquet_file, bbox_col_name)
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
    else:
        for col_name in geo_columns_info:
            native_stats = get_per_row_group_native_geo_stats(
                parquet_file, geometry_column=col_name
            )
            if native_stats:
                for rg_stat in native_stats:
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

    geo_meta = get_geo_metadata(parquet_file)

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

    if json_output:
        # For JSON, combine all metadata into one object
        geo_meta = get_geo_metadata(parquet_file)
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
        geo_meta = get_geo_metadata(parquet_file)
        primary_col = geo_meta.get("primary_column") if geo_meta else None

        # Section 1: Parquet File Metadata
        format_parquet_metadata_enhanced(parquet_file, False, row_groups_limit, primary_col)

        # Section 2: Parquet Geo Metadata
        format_parquet_geo_metadata(parquet_file, False, row_groups_limit)

        # Section 3: GeoParquet Metadata
        format_geoparquet_metadata(parquet_file, False)


def format_row_group_geo_stats(
    parquet_file: str, json_output: bool = False, row_groups: int | None = None
) -> None:
    """
    Format and display per-row-group geo_bbox statistics.

    Shows a table with row_group_id, num_rows, xmin, ymin, xmax, ymax for
    each row group. Useful for verifying spatial locality after Hilbert sorting.

    Tries native Parquet geo stats first (GeoParquet 2.0), then falls back to
    bbox column statistics if no native stats are available.

    Args:
        parquet_file: Path to the parquet file
        json_output: Whether to output as JSON
        row_groups: Limit output to first N row groups (None = all)
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_file_metadata,
        get_per_row_group_bbox_stats,
        get_per_row_group_native_geo_stats,
        has_bbox_column,
    )

    # Try native geo stats first (GeoParquet 2.0 / parquet-geo-only)
    rg_stats = get_per_row_group_native_geo_stats(parquet_file)

    # Fall back to bbox column if no native stats
    if not rg_stats:
        has_bbox, bbox_col_name = has_bbox_column(parquet_file)
        if has_bbox and bbox_col_name:
            rg_stats = get_per_row_group_bbox_stats(parquet_file, bbox_col_name)

    if not rg_stats:
        if json_output:
            print(json.dumps({"row_group_geo_stats": [], "message": "No geo stats found"}))
        else:
            console = Console()
            console.print()
            console.print("[bold]Per-Row-Group geo_bbox Statistics[/bold]")
            console.print("━" * 60)
            console.print("[yellow]No geo statistics found in this file.[/yellow]")
            console.print("[dim]For native stats: use GeoParquet 2.0 or parquet-geo-only[/dim]")
            console.print("[dim]For bbox column: gpio add bbox <file>[/dim]")
            console.print()
        return

    file_meta = get_file_metadata(parquet_file)
    num_rows_per_rg = _get_num_rows_per_row_group(parquet_file, file_meta)

    # Merge num_rows into stats
    stats_with_rows = _merge_row_counts(rg_stats, num_rows_per_rg)

    total_row_groups = len(stats_with_rows)
    effective_limit = row_groups if row_groups is not None else 1
    stats_with_rows = stats_with_rows[:effective_limit]

    if json_output:
        print(json.dumps({"row_group_geo_stats": stats_with_rows}, indent=2))
    else:
        _format_geo_stats_terminal(stats_with_rows, total_row_groups)


def _get_num_rows_per_row_group(parquet_file: str, file_meta: dict) -> dict[int, int]:
    """Get num_rows per row group from file metadata.

    Takes a RAW path: ``sql_path`` is the single SQL-escaping point.

    Returns a mapping of row_group_id to row count.
    """
    from geoparquet_io.core.duckdb_metadata import _get_connection_for_file, _metadata_path
    from geoparquet_io.core.duckdb_utils import sql_path

    connection, should_close = _get_connection_for_file(parquet_file)
    try:
        result = connection.execute(f"""
            SELECT row_group_id, row_group_num_rows
            FROM parquet_metadata({sql_path(_metadata_path(parquet_file))})
            GROUP BY row_group_id, row_group_num_rows
            ORDER BY row_group_id
        """).fetchall()
        return {row[0]: row[1] for row in result}
    finally:
        if should_close:
            connection.close()


def _merge_row_counts(rg_stats: list[dict], num_rows_per_rg: dict[int, int]) -> list[dict]:
    """Merge row counts into row group stats."""
    merged = []
    for stat in rg_stats:
        rg_id = stat["row_group_id"]
        merged.append(
            {
                "row_group_id": rg_id,
                "num_rows": num_rows_per_rg.get(rg_id, 0),
                "xmin": stat["xmin"],
                "ymin": stat["ymin"],
                "xmax": stat["xmax"],
                "ymax": stat["ymax"],
            }
        )
    return merged


def _format_geo_stats_terminal(stats: list[dict], total_row_groups: int) -> None:
    """Render per-row-group geo_bbox stats as a Rich table."""
    console = Console()
    console.print()
    console.print("[bold]Per-Row-Group geo_bbox Statistics[/bold]")
    console.print("━" * 60)

    if not stats:
        console.print("[yellow]No geo_bbox statistics found.[/yellow]")
        console.print()
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("Row Group", justify="right")
    table.add_column("Rows", justify="right")
    table.add_column("xmin", justify="right")
    table.add_column("ymin", justify="right")
    table.add_column("xmax", justify="right")
    table.add_column("ymax", justify="right")

    for stat in stats:
        table.add_row(
            str(stat["row_group_id"]),
            f"{stat['num_rows']:,}",
            f"{stat['xmin']:.6f}",
            f"{stat['ymin']:.6f}",
            f"{stat['xmax']:.6f}",
            f"{stat['ymax']:.6f}",
        )

    console.print(table)

    shown = len(stats)
    remaining = total_row_groups - shown
    if remaining > 0:
        console.print(f"\n  [dim]... and {remaining} more row group(s)[/dim]")
        console.print(f"  [dim]Use --row-groups {total_row_groups} to see all row groups[/dim]")

    console.print()
