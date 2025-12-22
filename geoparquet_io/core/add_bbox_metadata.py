#!/usr/bin/env python3

import json
import os

import click
import pyarrow.parquet as pq

from geoparquet_io.core.check_parquet_structure import get_compression_info, get_row_group_stats
from geoparquet_io.core.common import (
    check_bbox_structure,
    find_primary_geometry_column,
    get_parquet_metadata,
    parse_geo_metadata,
    safe_file_url,
)
from geoparquet_io.core.logging_config import debug, error, success


def add_bbox_metadata(parquet_file, verbose=False):
    """
    Add bbox covering metadata to a GeoParquet file.

    Updates the GeoParquet metadata to include bbox covering information,
    which enables spatial filtering optimizations in readers that support it.

    Args:
        parquet_file: Path to the parquet file (will be modified in place)
        verbose: Print verbose output
    """
    safe_url = safe_file_url(parquet_file, verbose)

    # Check current bbox structure
    bbox_info = check_bbox_structure(parquet_file, verbose)

    if bbox_info["has_bbox_metadata"]:
        success(
            f"✓ Bbox covering metadata already exists for column '{bbox_info['bbox_column_name']}'"
        )
        return

    if not bbox_info["has_bbox_column"]:
        error("❌ No valid bbox column found in the file. Please add a bbox column first.")
        return

    # Get existing metadata
    metadata, _ = get_parquet_metadata(safe_url)
    geo_meta = parse_geo_metadata(metadata, False)

    if not geo_meta:
        geo_meta = {"version": "1.1.0", "primary_column": "geometry", "columns": {}}

    # Find primary geometry column
    primary_col = find_primary_geometry_column(safe_url, verbose)

    # Update or create the columns section
    if "columns" not in geo_meta:
        geo_meta["columns"] = {}

    if primary_col not in geo_meta["columns"]:
        geo_meta["columns"][primary_col] = {}

    # Add bbox covering metadata
    geo_meta["columns"][primary_col]["covering"] = {
        "bbox": {
            "xmin": [bbox_info["bbox_column_name"], "xmin"],
            "ymin": [bbox_info["bbox_column_name"], "ymin"],
            "xmax": [bbox_info["bbox_column_name"], "xmax"],
            "ymax": [bbox_info["bbox_column_name"], "ymax"],
        }
    }

    if verbose:
        debug("\nUpdated geo metadata:")
        debug(json.dumps(geo_meta, indent=2))

    # Get original file properties
    row_group_stats = get_row_group_stats(parquet_file)
    compression_info = get_compression_info(parquet_file, primary_col)
    row_group_size = row_group_stats["avg_rows_per_group"]
    compression = compression_info[primary_col]

    if verbose:
        debug("\nPreserving file properties:")
        debug(f"Row group size: {row_group_size:,.0f} rows")
        debug(f"Compression: {compression}")

    # Create a temporary file for the new metadata
    temp_file = parquet_file + ".tmp"
    try:
        # Read the original file
        table = pq.read_table(parquet_file)

        # Update metadata
        existing_metadata = table.schema.metadata or {}
        new_metadata = {
            k: v for k, v in existing_metadata.items() if not k.decode("utf-8").startswith("geo")
        }
        new_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")

        # Create new table with updated metadata
        new_table = table.replace_schema_metadata(new_metadata)

        # Write to temporary file with same properties as original
        pq.write_table(
            new_table,
            temp_file,
            row_group_size=int(row_group_size),
            compression=compression,
            write_statistics=True,
            use_dictionary=True,
            version="2.6",
            write_page_index=False,
        )

        # Replace original file
        os.replace(temp_file, parquet_file)

        success(f"✓ Added bbox covering metadata for column '{bbox_info['bbox_column_name']}'")

    except Exception as e:
        # Clean up temporary file if something goes wrong
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise click.ClickException(f"Failed to update metadata: {str(e)}") from e
