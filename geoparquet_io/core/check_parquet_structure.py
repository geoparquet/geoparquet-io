#!/usr/bin/env python3


import click
import fsspec
import pyarrow.parquet as pq

from geoparquet_io.core.common import (
    check_bbox_structure,
    find_primary_geometry_column,
    format_size,
    get_parquet_metadata,
    parse_geo_metadata,
    safe_file_url,
)


def get_row_group_stats(parquet_file):
    """
    Get basic row group statistics from a parquet file.

    Returns:
        dict: Statistics including:
            - num_groups: Number of row groups
            - total_rows: Total number of rows
            - avg_rows_per_group: Average rows per group
            - total_size: Total file size in bytes
            - avg_group_size: Average group size in bytes
    """
    with fsspec.open(safe_file_url(parquet_file), "rb") as f:
        metadata = pq.ParquetFile(f).metadata

        total_rows = metadata.num_rows
        num_groups = metadata.num_row_groups
        avg_rows_per_group = total_rows / num_groups if num_groups > 0 else 0
        total_size = sum(metadata.row_group(i).total_byte_size for i in range(num_groups))
        avg_group_size = total_size / num_groups if num_groups > 0 else 0

        return {
            "num_groups": num_groups,
            "total_rows": total_rows,
            "avg_rows_per_group": avg_rows_per_group,
            "total_size": total_size,
            "avg_group_size": avg_group_size,
        }


def assess_row_group_size(avg_group_size_bytes, total_size_bytes):
    """
    Assess if row group size is optimal.

    Returns:
        tuple: (status, message, color) where status is one of:
            - "optimal"
            - "suboptimal"
            - "poor"
    """
    avg_group_size_mb = avg_group_size_bytes / (1024 * 1024)
    total_size_mb = total_size_bytes / (1024 * 1024)

    if total_size_mb < 128:
        return "optimal", "Row group size is appropriate for small file", "green"

    if 128 <= avg_group_size_mb <= 256:
        return "optimal", "Row group size is optimal (128-256 MB)", "green"
    elif 64 <= avg_group_size_mb < 128 or 256 < avg_group_size_mb <= 512:
        return (
            "suboptimal",
            "Row group size is suboptimal. Recommended size is 128-256 MB",
            "yellow",
        )
    else:
        return (
            "poor",
            "Row group size is outside recommended range. Target 128-256 MB for best performance",
            "red",
        )


def assess_row_count(avg_rows):
    """
    Assess if average row count per group is optimal.

    Returns:
        tuple: (status, message, color) where status is one of:
            - "optimal"
            - "suboptimal"
            - "poor"
    """
    if avg_rows < 2000:
        return (
            "poor",
            "Row count per group is very low. Target 50,000-200,000 rows per group",
            "red",
        )
    elif avg_rows > 1000000:
        return (
            "poor",
            "Row count per group is very high. Target 50,000-200,000 rows per group",
            "red",
        )
    elif 50000 <= avg_rows <= 200000:
        return "optimal", "Row count per group is optimal", "green"
    else:
        return (
            "suboptimal",
            "Row count per group is outside recommended range (50,000-200,000)",
            "yellow",
        )


def get_compression_info(parquet_file, column_name=None):
    """
    Get compression information for specified column(s).

    Returns:
        dict: Mapping of column names to their compression algorithms
    """
    with fsspec.open(safe_file_url(parquet_file), "rb") as f:
        metadata = pq.ParquetFile(f).metadata

        compression_info = {}
        for i in range(metadata.num_columns):
            col = metadata.schema.column(i)
            if column_name is None or col.name == column_name:
                compression = metadata.row_group(0).column(i).compression
                compression_info[col.name] = compression

        return compression_info


def check_row_groups(parquet_file, verbose=False, return_results=False):
    """Check row group optimization and print results.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict instead of only printing

    Returns:
        dict if return_results=True, containing:
            - passed: bool
            - stats: dict with file statistics
            - size_status: str (optimal/suboptimal/poor)
            - row_status: str (optimal/suboptimal/poor)
            - issues: list of issue descriptions
            - recommendations: list of recommendations
    """
    stats = get_row_group_stats(parquet_file)

    size_status, size_message, size_color = assess_row_group_size(
        stats["avg_group_size"], stats["total_size"]
    )
    row_status, row_message, row_color = assess_row_count(stats["avg_rows_per_group"])

    # Build results dict
    passed = size_status == "optimal" and row_status == "optimal"
    issues = []
    recommendations = []

    if size_status != "optimal":
        issues.append(size_message)
        recommendations.append("Rewrite with optimal row group size (128-256 MB)")

    if row_status != "optimal":
        issues.append(row_message)
        recommendations.append("Target 50,000-200,000 rows per group")

    results = {
        "passed": passed,
        "stats": stats,
        "size_status": size_status,
        "row_status": row_status,
        "issues": issues,
        "recommendations": recommendations,
        "fix_available": not passed,
    }

    # Print results
    click.echo("\nRow Group Analysis:")
    click.echo(f"Number of row groups: {stats['num_groups']}")

    click.echo(
        click.style(f"Average group size: {format_size(stats['avg_group_size'])}", fg=size_color)
    )
    click.echo(click.style(size_message, fg=size_color))

    click.echo(
        click.style(f"Average rows per group: {stats['avg_rows_per_group']:,.0f}", fg=row_color)
    )
    click.echo(click.style(row_message, fg=row_color))

    click.echo(f"\nTotal file size: {format_size(stats['total_size'])}")

    if size_status != "optimal" or row_status != "optimal":
        click.echo("\nRow Group Guidelines:")
        click.echo("- Optimal size: 128-256 MB per row group")
        click.echo("- Optimal rows: 50,000-200,000 rows per group")
        click.echo("- Small files (<128 MB): single row group is fine")

    if return_results:
        return results


def check_metadata_and_bbox(parquet_file, verbose=False, return_results=False):
    """Check GeoParquet metadata version and bbox structure.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict

    Returns:
        dict if return_results=True, containing:
            - passed: bool
            - has_geo_metadata: bool
            - version: str
            - has_bbox_column: bool
            - has_bbox_metadata: bool
            - bbox_column_name: str or None
            - issues: list of issue descriptions
            - recommendations: list of recommendations
    """
    metadata, _ = get_parquet_metadata(parquet_file)
    geo_meta = parse_geo_metadata(metadata, False)

    if not geo_meta:
        click.echo(click.style("\n❌ No GeoParquet metadata found", fg="red"))
        if return_results:
            return {
                "passed": False,
                "has_geo_metadata": False,
                "issues": ["No GeoParquet metadata found"],
                "recommendations": [],
                "fix_available": False,
            }
        return

    version = geo_meta.get("version", "0.0.0")
    bbox_info = check_bbox_structure(parquet_file, verbose)

    # Build results
    issues = []
    recommendations = []

    if version < "1.1.0":
        issues.append(f"GeoParquet version {version} is outdated")
        recommendations.append("Upgrade to version 1.1.0+")

    needs_bbox_column = not bbox_info["has_bbox_column"]
    needs_bbox_metadata = bbox_info["has_bbox_column"] and not bbox_info["has_bbox_metadata"]

    if needs_bbox_column:
        issues.append("No bbox column found")
        recommendations.append("Add bbox column for better query performance")

    if needs_bbox_metadata:
        issues.append("Bbox column exists but missing metadata covering")
        recommendations.append("Add bbox covering to metadata")

    passed = version >= "1.1.0" and not needs_bbox_column and not needs_bbox_metadata

    results = {
        "passed": passed,
        "has_geo_metadata": True,
        "version": version,
        "has_bbox_column": bbox_info["has_bbox_column"],
        "has_bbox_metadata": bbox_info["has_bbox_metadata"],
        "bbox_column_name": bbox_info.get("bbox_column_name"),
        "needs_bbox_column": needs_bbox_column,
        "needs_bbox_metadata": needs_bbox_metadata,
        "issues": issues,
        "recommendations": recommendations,
        "fix_available": needs_bbox_column or needs_bbox_metadata,
    }

    # Print results
    click.echo("\nGeoParquet Metadata:")
    version_color = "green" if version >= "1.1.0" else "yellow"
    version_prefix = "✓" if version >= "1.1.0" else "⚠️"
    version_suffix = "" if version >= "1.1.0" else " (upgrade to 1.1.0+ recommended)"

    click.echo(click.style(f"{version_prefix} Version {version}{version_suffix}", fg=version_color))

    if bbox_info["has_bbox_column"]:
        if bbox_info["has_bbox_metadata"]:
            click.echo(
                click.style(
                    f"✓ Found bbox column '{bbox_info['bbox_column_name']}' with proper metadata covering",
                    fg="green",
                )
            )
        else:
            click.echo(
                click.style(
                    f"⚠️  Found bbox column '{bbox_info['bbox_column_name']}' but missing bbox covering metadata "
                    "(add to metadata to help inform clients)",
                    fg="yellow",
                )
            )
    else:
        click.echo(
            click.style("❌ No bbox column found (recommended for better performance)", fg="red")
        )

    if return_results:
        return results


def check_compression(parquet_file, verbose=False, return_results=False):
    """Check compression settings for geometry column.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return structured results dict

    Returns:
        dict if return_results=True, containing:
            - passed: bool
            - current_compression: str
            - geometry_column: str
            - issues: list of issue descriptions
            - recommendations: list of recommendations
    """
    primary_col = find_primary_geometry_column(parquet_file, verbose)
    if not primary_col:
        click.echo(click.style("\n❌ No geometry column found", fg="red"))
        if return_results:
            return {
                "passed": False,
                "current_compression": None,
                "geometry_column": None,
                "issues": ["No geometry column found"],
                "recommendations": [],
                "fix_available": False,
            }
        return

    compression = get_compression_info(parquet_file, primary_col)[primary_col]
    passed = compression == "ZSTD"

    issues = []
    recommendations = []
    if not passed:
        issues.append(f"{compression} compression instead of ZSTD")
        recommendations.append("Re-compress with ZSTD for better performance")

    results = {
        "passed": passed,
        "current_compression": compression,
        "geometry_column": primary_col,
        "issues": issues,
        "recommendations": recommendations,
        "fix_available": not passed,
    }

    # Print results
    click.echo("\nCompression Analysis:")
    if compression == "ZSTD":
        click.echo(
            click.style(f"✓ ZSTD compression on geometry column '{primary_col}'", fg="green")
        )
    else:
        click.echo(
            click.style(
                f"⚠️  {compression} compression on geometry column '{primary_col}' (ZSTD recommended)",
                fg="yellow",
            )
        )

    if return_results:
        return results


def check_all(parquet_file, verbose=False, return_results=False):
    """Run all structure checks.

    Args:
        parquet_file: Path to parquet file
        verbose: Print additional information
        return_results: If True, return aggregated results dict

    Returns:
        dict if return_results=True, containing results from all checks
    """
    row_groups_result = check_row_groups(parquet_file, verbose, return_results=True)
    bbox_result = check_metadata_and_bbox(parquet_file, verbose, return_results=True)
    compression_result = check_compression(parquet_file, verbose, return_results=True)

    if return_results:
        return {
            "row_groups": row_groups_result,
            "bbox": bbox_result,
            "compression": compression_result,
        }


if __name__ == "__main__":
    check_all()
