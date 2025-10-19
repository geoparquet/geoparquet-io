#!/usr/bin/env python3
"""
Basic usage examples for geoparquet-io.

This script demonstrates common operations using the Python API.
"""

import os
import tempfile

import pyarrow.parquet as pq

# Import geoparquet-io functions
from geoparquet_io.core.add_bbox_column import add_bbox_column
from geoparquet_io.core.check_parquet_structure import check_all
from geoparquet_io.core.common import get_dataset_bounds
from geoparquet_io.core.hilbert_order import hilbert_order


def example_1_add_bbox():
    """Example 1: Add a bbox column to a GeoParquet file."""
    print("=" * 60)
    print("Example 1: Adding bbox column")
    print("=" * 60)

    # Input and output files
    input_file = "input.parquet"
    output_file = "output_with_bbox.parquet"

    # Check if input exists (for demo purposes, skip if not)
    if not os.path.exists(input_file):
        print(f"⚠️  Input file '{input_file}' not found. Skipping example.")
        return

    # Add bbox column
    print(f"Adding bbox column to {input_file}...")
    add_bbox_column(
        input_parquet=input_file,
        output_parquet=output_file,
        bbox_name="bbox",  # Default name
        dry_run=False,
        verbose=True,
        compression="ZSTD",
        compression_level=15,
    )

    print(f"✓ Output written to {output_file}")
    print()


def example_2_hilbert_sort():
    """Example 2: Sort a file using Hilbert curve ordering."""
    print("=" * 60)
    print("Example 2: Hilbert curve sorting")
    print("=" * 60)

    input_file = "input.parquet"
    output_file = "output_sorted.parquet"

    if not os.path.exists(input_file):
        print(f"⚠️  Input file '{input_file}' not found. Skipping example.")
        return

    print(f"Sorting {input_file} using Hilbert curve...")
    hilbert_order(
        input_parquet=input_file,
        output_parquet=output_file,
        geometry_column="geometry",  # Default geometry column
        add_bbox_flag=True,  # Automatically add bbox if missing
        verbose=True,
        compression="ZSTD",
        compression_level=15,
    )

    print(f"✓ Sorted output written to {output_file}")
    print()


def example_3_check_file():
    """Example 3: Check a GeoParquet file for best practices."""
    print("=" * 60)
    print("Example 3: Checking file quality")
    print("=" * 60)

    input_file = "input.parquet"

    if not os.path.exists(input_file):
        print(f"⚠️  Input file '{input_file}' not found. Skipping example.")
        return

    print(f"Checking {input_file}...")
    check_all(parquet_file=input_file, verbose=True)

    print()


def example_4_get_bounds():
    """Example 4: Calculate dataset bounds."""
    print("=" * 60)
    print("Example 4: Getting dataset bounds")
    print("=" * 60)

    input_file = "input.parquet"

    if not os.path.exists(input_file):
        print(f"⚠️  Input file '{input_file}' not found. Skipping example.")
        return

    print(f"Calculating bounds for {input_file}...")
    bounds = get_dataset_bounds(input_file, geometry_column="geometry", verbose=True)

    if bounds:
        xmin, ymin, xmax, ymax = bounds
        print(f"\n✓ Dataset bounds: ({xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f})")
        print(f"  Width: {xmax - xmin:.6f}")
        print(f"  Height: {ymax - ymin:.6f}")
    else:
        print("❌ Could not calculate bounds")

    print()


def example_5_compression_options():
    """Example 5: Using different compression options."""
    print("=" * 60)
    print("Example 5: Compression options")
    print("=" * 60)

    input_file = "input.parquet"

    if not os.path.exists(input_file):
        print(f"⚠️  Input file '{input_file}' not found. Skipping example.")
        return

    # Create temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        compressions = [
            ("ZSTD", 15),
            ("GZIP", 6),
            ("BROTLI", 6),
            ("LZ4", None),
            ("SNAPPY", None),
        ]

        print("Testing different compression formats...")
        print()

        for compression, level in compressions:
            output_file = os.path.join(tmpdir, f"output_{compression.lower()}.parquet")

            # Add bbox with different compression
            add_bbox_column(
                input_parquet=input_file,
                output_parquet=output_file,
                bbox_name="bbox",
                dry_run=False,
                verbose=False,
                compression=compression,
                compression_level=level,
            )

            # Get file size
            size_bytes = os.path.getsize(output_file)
            size_mb = size_bytes / (1024 * 1024)

            # Read to verify
            pf = pq.ParquetFile(output_file)
            actual_compression = str(pf.metadata.row_group(0).column(0).compression)

            print(
                f"  {compression:10s} ({level if level else 'default':>2}): "
                f"{size_mb:6.2f} MB - {actual_compression}"
            )

    print()


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("GeoParquet-IO Python API Examples")
    print("=" * 60)
    print()
    print("These examples demonstrate basic usage of the Python API.")
    print("Make sure you have a file named 'input.parquet' in the current directory.")
    print()
    print("You can also use the CLI commands directly:")
    print("  gpio add bbox input.parquet output.parquet")
    print("  gpio sort hilbert input.parquet output.parquet")
    print("  gpio check all input.parquet")
    print()

    # Run examples
    example_1_add_bbox()
    example_2_hilbert_sort()
    example_3_check_file()
    example_4_get_bounds()
    example_5_compression_options()

    print("=" * 60)
    print("Examples complete!")
    print("=" * 60)
