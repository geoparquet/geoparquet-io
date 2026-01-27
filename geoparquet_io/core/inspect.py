"""Core inspect functionality for GeoParquet files.

This module contains the business logic for inspecting GeoParquet files,
including summary, preview (head/tail), statistics, and metadata operations.
"""

from geoparquet_io.core.common import (
    get_duckdb_connection,
    get_parquet_metadata,
    needs_httpfs,
    parse_geo_metadata,
)
from geoparquet_io.core.duckdb_metadata import get_usable_columns
from geoparquet_io.core.inspect_utils import (
    extract_file_info,
    extract_geo_info,
    extract_partition_summary,
    format_json_output,
    format_markdown_output,
    format_partition_json_output,
    format_partition_markdown_output,
    format_partition_terminal_output,
    format_terminal_output,
    get_column_statistics,
    get_preview_data,
)
from geoparquet_io.core.partition_reader import get_partition_info


def get_primary_geometry_column(parquet_file: str) -> str | None:
    """Get primary geometry column for metadata highlighting.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        Name of the primary geometry column, or None if not found
    """
    metadata, _ = get_parquet_metadata(parquet_file, verbose=False)
    geo_meta = parse_geo_metadata(metadata, verbose=False)
    return geo_meta.get("primary_column") if geo_meta else None


def _build_columns_info(usable_columns: list[dict], primary_geom_col: str | None) -> list[dict]:
    """Build columns info list with geometry flag.

    Args:
        usable_columns: List of column metadata dicts from get_usable_columns
        primary_geom_col: Name of the primary geometry column

    Returns:
        List of column info dicts with name, type, and is_geometry flag
    """
    return [
        {
            "name": col["name"],
            "type": col["type"],
            "is_geometry": col["name"] == primary_geom_col,
        }
        for col in usable_columns
    ]


def inspect_summary(
    parquet_file: str,
    check_all_files: bool = False,
    profile: str | None = None,
) -> dict:
    """Get summary information for a GeoParquet file.

    Args:
        parquet_file: Path to the parquet file or partition directory
        check_all_files: For partitioned data, aggregate info from all files
        profile: AWS profile name for S3 operations

    Returns:
        Dict with file_info, geo_info, columns_info, and optionally partition_summary
    """
    from geoparquet_io.core.common import (
        setup_aws_profile_if_needed,
        validate_profile_for_urls,
    )

    validate_profile_for_urls(profile, parquet_file)
    setup_aws_profile_if_needed(profile, parquet_file)

    partition_info = get_partition_info(parquet_file, verbose=False)

    if partition_info["is_partition"] and check_all_files:
        all_files = partition_info["all_files"]
        if not all_files:
            raise ValueError("No parquet files found in partition")

        partition_summary = extract_partition_summary(all_files, verbose=False)
        first_file = partition_info["first_file"]

        # Create shared connection for all metadata operations
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(first_file))
        try:
            geo_info = extract_geo_info(first_file, con=con)
            usable_columns = get_usable_columns(first_file, con=con)
            primary_geom_col = geo_info.get("primary_column")
            columns_info = _build_columns_info(usable_columns, primary_geom_col)
        finally:
            con.close()

        return {
            "is_partition": True,
            "partition_summary": partition_summary,
            "geo_info": geo_info,
            "columns_info": columns_info,
        }

    file_to_inspect = parquet_file
    partition_notice = None
    if partition_info["is_partition"]:
        file_to_inspect = partition_info["first_file"]
        partition_notice = (
            f"Inspecting first file (of {partition_info['file_count']} total). "
            "Use --check-all to aggregate all files."
        )

    # Create shared connection for all metadata operations
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(file_to_inspect))
    try:
        file_info = extract_file_info(file_to_inspect, con=con)
        geo_info = extract_geo_info(file_to_inspect, con=con)
        usable_columns = get_usable_columns(file_to_inspect, con=con)
        primary_geom_col = geo_info.get("primary_column")
        columns_info = _build_columns_info(usable_columns, primary_geom_col)
    finally:
        con.close()

    return {
        "is_partition": partition_info["is_partition"],
        "partition_notice": partition_notice,
        "file_info": file_info,
        "geo_info": geo_info,
        "columns_info": columns_info,
    }


def format_summary_output(
    result: dict,
    json_output: bool = False,
    markdown_output: bool = False,
) -> str | None:
    """Format summary result for output.

    Args:
        result: Dict from inspect_summary
        json_output: Output as JSON
        markdown_output: Output as Markdown

    Returns:
        Formatted string for json/markdown, or None for terminal (prints directly)
    """
    if result.get("is_partition") and "partition_summary" in result:
        if json_output:
            return format_partition_json_output(
                result["partition_summary"], result["geo_info"], result["columns_info"]
            )
        elif markdown_output:
            return format_partition_markdown_output(
                result["partition_summary"], result["geo_info"], result["columns_info"]
            )
        else:
            format_partition_terminal_output(
                result["partition_summary"], result["geo_info"], result["columns_info"]
            )
            return None

    if json_output:
        return format_json_output(
            result["file_info"], result["geo_info"], result["columns_info"], None, None
        )
    elif markdown_output:
        return format_markdown_output(
            result["file_info"], result["geo_info"], result["columns_info"], None, None, None
        )
    else:
        format_terminal_output(
            result["file_info"], result["geo_info"], result["columns_info"], None, None, None
        )
        return None


def inspect_preview(
    parquet_file: str,
    count: int = 10,
    mode: str = "head",
    profile: str | None = None,
) -> dict:
    """Get preview data (head or tail) for a GeoParquet file.

    Args:
        parquet_file: Path to the parquet file
        count: Number of rows to preview
        mode: "head" or "tail"
        profile: AWS profile name for S3 operations

    Returns:
        Dict with file_info, geo_info, columns_info, preview_table, preview_mode
    """
    from geoparquet_io.core.common import (
        setup_aws_profile_if_needed,
        validate_profile_for_urls,
    )

    validate_profile_for_urls(profile, parquet_file)
    setup_aws_profile_if_needed(profile, parquet_file)

    partition_info = get_partition_info(parquet_file, verbose=False)
    file_to_inspect = parquet_file
    partition_notice = None

    if partition_info["is_partition"]:
        file_to_inspect = partition_info["first_file"]
        partition_notice = f"Previewing first file (of {partition_info['file_count']} total)."

    # Create shared connection for all metadata operations
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(file_to_inspect))
    try:
        file_info = extract_file_info(file_to_inspect, con=con)
        geo_info = extract_geo_info(file_to_inspect, con=con)
        usable_columns = get_usable_columns(file_to_inspect, con=con)
        primary_geom_col = geo_info.get("primary_column")
        columns_info = _build_columns_info(usable_columns, primary_geom_col)
    finally:
        con.close()

    head_val = count if mode == "head" else None
    tail_val = count if mode == "tail" else None
    preview_table, preview_mode = get_preview_data(file_to_inspect, head=head_val, tail=tail_val)

    return {
        "is_partition": partition_info["is_partition"],
        "partition_notice": partition_notice,
        "file_info": file_info,
        "geo_info": geo_info,
        "columns_info": columns_info,
        "preview_table": preview_table,
        "preview_mode": preview_mode,
    }


def format_preview_output(
    result: dict,
    json_output: bool = False,
    markdown_output: bool = False,
) -> str | None:
    """Format preview result for output.

    Args:
        result: Dict from inspect_preview
        json_output: Output as JSON
        markdown_output: Output as Markdown

    Returns:
        Formatted string for json/markdown, or None for terminal (prints directly)
    """
    if json_output:
        return format_json_output(
            result["file_info"],
            result["geo_info"],
            result["columns_info"],
            result["preview_table"],
            None,
        )
    elif markdown_output:
        return format_markdown_output(
            result["file_info"],
            result["geo_info"],
            result["columns_info"],
            result["preview_table"],
            result["preview_mode"],
            None,
        )
    else:
        format_terminal_output(
            result["file_info"],
            result["geo_info"],
            result["columns_info"],
            result["preview_table"],
            result["preview_mode"],
            None,
        )
        return None


def inspect_stats(
    parquet_file: str,
    profile: str | None = None,
) -> dict:
    """Get column statistics for a GeoParquet file.

    Args:
        parquet_file: Path to the parquet file
        profile: AWS profile name for S3 operations

    Returns:
        Dict with file_info, geo_info, columns_info, statistics
    """
    from geoparquet_io.core.common import (
        setup_aws_profile_if_needed,
        validate_profile_for_urls,
    )

    validate_profile_for_urls(profile, parquet_file)
    setup_aws_profile_if_needed(profile, parquet_file)

    partition_info = get_partition_info(parquet_file, verbose=False)
    file_to_inspect = parquet_file
    partition_notice = None

    if partition_info["is_partition"]:
        file_to_inspect = partition_info["first_file"]
        partition_notice = (
            f"Showing stats for first file (of {partition_info['file_count']} total)."
        )

    # Create shared connection for all metadata operations
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(file_to_inspect))
    try:
        file_info = extract_file_info(file_to_inspect, con=con)
        geo_info = extract_geo_info(file_to_inspect, con=con)
        usable_columns = get_usable_columns(file_to_inspect, con=con)
        primary_geom_col = geo_info.get("primary_column")
        columns_info = _build_columns_info(usable_columns, primary_geom_col)
    finally:
        con.close()

    statistics = get_column_statistics(file_to_inspect, columns_info)

    return {
        "is_partition": partition_info["is_partition"],
        "partition_notice": partition_notice,
        "file_info": file_info,
        "geo_info": geo_info,
        "columns_info": columns_info,
        "statistics": statistics,
    }


def format_stats_output(
    result: dict,
    json_output: bool = False,
    markdown_output: bool = False,
) -> str | None:
    """Format stats result for output.

    Args:
        result: Dict from inspect_stats
        json_output: Output as JSON
        markdown_output: Output as Markdown

    Returns:
        Formatted string for json/markdown, or None for terminal (prints directly)
    """
    if json_output:
        return format_json_output(
            result["file_info"],
            result["geo_info"],
            result["columns_info"],
            None,
            result["statistics"],
        )
    elif markdown_output:
        return format_markdown_output(
            result["file_info"],
            result["geo_info"],
            result["columns_info"],
            None,
            None,
            result["statistics"],
        )
    else:
        format_terminal_output(
            result["file_info"],
            result["geo_info"],
            result["columns_info"],
            None,
            None,
            result["statistics"],
        )
        return None


def display_metadata(
    parquet_file: str,
    parquet: bool = False,
    geoparquet: bool = False,
    parquet_geo: bool = False,
    row_groups: int = 1,
    json_output: bool = False,
) -> None:
    """Display metadata for a GeoParquet file.

    Args:
        parquet_file: Path to the parquet file
        parquet: Show only Parquet file metadata
        geoparquet: Show only GeoParquet 'geo' metadata
        parquet_geo: Show only Parquet geospatial metadata
        row_groups: Number of row groups to display
        json_output: Output as JSON
    """
    from geoparquet_io.core.metadata_utils import (
        format_all_metadata,
        format_geoparquet_metadata,
        format_parquet_geo_metadata,
        format_parquet_metadata_enhanced,
    )

    # Count how many specific flags were set
    specific_flags = sum([parquet, geoparquet, parquet_geo])

    if specific_flags == 0:
        # Show all sections
        format_all_metadata(parquet_file, json_output, row_groups)
    elif specific_flags > 1:
        # Multiple specific flags - show each requested section
        primary_col = get_primary_geometry_column(parquet_file)

        if parquet:
            format_parquet_metadata_enhanced(parquet_file, json_output, row_groups, primary_col)
        if parquet_geo:
            format_parquet_geo_metadata(parquet_file, json_output, row_groups)
        if geoparquet:
            format_geoparquet_metadata(parquet_file, json_output)
    else:
        # Single specific flag
        if parquet:
            primary_col = get_primary_geometry_column(parquet_file)
            format_parquet_metadata_enhanced(parquet_file, json_output, row_groups, primary_col)
        elif geoparquet:
            format_geoparquet_metadata(parquet_file, json_output)
        elif parquet_geo:
            format_parquet_geo_metadata(parquet_file, json_output, row_groups)
