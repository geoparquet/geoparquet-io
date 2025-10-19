#!/usr/bin/env python3
"""
Batch processing example for geoparquet-io.

This script demonstrates how to process multiple files in a directory.
"""

import os
from pathlib import Path

from geoparquet_io.core.add_bbox_column import add_bbox_column
from geoparquet_io.core.hilbert_order import hilbert_order


def process_directory(
    input_dir: str,
    output_dir: str,
    operation: str = "add-bbox",
    verbose: bool = True,
) -> None:
    """
    Process all GeoParquet files in a directory.

    Args:
        input_dir: Directory containing input .parquet files
        output_dir: Directory for output files
        operation: Operation to perform ('add-bbox', 'sort', or 'both')
        verbose: Print progress information
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Find all parquet files
    input_path = Path(input_dir)
    parquet_files = list(input_path.glob("*.parquet"))

    if not parquet_files:
        print(f"No .parquet files found in {input_dir}")
        return

    print(f"Found {len(parquet_files)} files to process")
    print(f"Operation: {operation}")
    print()

    # Process each file
    for i, input_file in enumerate(parquet_files, 1):
        print(f"[{i}/{len(parquet_files)}] Processing {input_file.name}...")

        output_file = Path(output_dir) / input_file.name

        try:
            if operation == "add-bbox":
                # Add bbox column
                add_bbox_column(
                    input_parquet=str(input_file),
                    output_parquet=str(output_file),
                    bbox_name="bbox",
                    dry_run=False,
                    verbose=verbose,
                    compression="ZSTD",
                    compression_level=15,
                )

            elif operation == "sort":
                # Sort using Hilbert curve
                hilbert_order(
                    input_parquet=str(input_file),
                    output_parquet=str(output_file),
                    geometry_column="geometry",
                    add_bbox_flag=False,
                    verbose=verbose,
                    compression="ZSTD",
                    compression_level=15,
                )

            elif operation == "both":
                # First sort, then add bbox
                temp_file = str(output_file) + ".tmp"

                # Sort
                hilbert_order(
                    input_parquet=str(input_file),
                    output_parquet=temp_file,
                    geometry_column="geometry",
                    add_bbox_flag=False,
                    verbose=False,
                    compression="ZSTD",
                    compression_level=15,
                )

                # Add bbox
                add_bbox_column(
                    input_parquet=temp_file,
                    output_parquet=str(output_file),
                    bbox_name="bbox",
                    dry_run=False,
                    verbose=False,
                    compression="ZSTD",
                    compression_level=15,
                )

                # Clean up temp file
                os.remove(temp_file)

            else:
                print(f"Unknown operation: {operation}")
                continue

            # Report file sizes
            input_size = input_file.stat().st_size / (1024 * 1024)
            output_size = output_file.stat().st_size / (1024 * 1024)
            ratio = output_size / input_size if input_size > 0 else 0

            print(f"  ✓ {input_file.name}: {input_size:.2f}MB → {output_size:.2f}MB ({ratio:.1%})")

        except Exception as e:
            print(f"  ❌ Error processing {input_file.name}: {e}")
            continue

    print()
    print(f"✓ Batch processing complete! Output in {output_dir}/")


def example_parallel_processing():
    """
    Example of parallel processing using multiprocessing.

    This is useful for processing many large files on multi-core machines.
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed
    from pathlib import Path

    def process_single_file(args):
        """Process a single file (designed for multiprocessing)."""
        input_file, output_dir, operation = args
        output_file = Path(output_dir) / input_file.name

        try:
            if operation == "add-bbox":
                add_bbox_column(
                    input_parquet=str(input_file),
                    output_parquet=str(output_file),
                    bbox_name="bbox",
                    dry_run=False,
                    verbose=False,  # Disable verbose in parallel mode
                    compression="ZSTD",
                    compression_level=15,
                )
            elif operation == "sort":
                hilbert_order(
                    input_parquet=str(input_file),
                    output_parquet=str(output_file),
                    geometry_column="geometry",
                    add_bbox_flag=True,
                    verbose=False,
                    compression="ZSTD",
                    compression_level=15,
                )

            return (True, input_file.name, None)

        except Exception as e:
            return (False, input_file.name, str(e))

    # Example usage
    input_dir = "input_files"
    output_dir = "output_files"
    operation = "sort"

    if not os.path.exists(input_dir):
        print(f"Directory {input_dir} not found. Skipping parallel processing example.")
        return

    os.makedirs(output_dir, exist_ok=True)

    # Find all files
    parquet_files = list(Path(input_dir).glob("*.parquet"))

    if not parquet_files:
        print(f"No files found in {input_dir}")
        return

    print(f"Processing {len(parquet_files)} files in parallel...")

    # Prepare arguments
    args_list = [(f, output_dir, operation) for f in parquet_files]

    # Process in parallel
    max_workers = os.cpu_count() or 4  # Use all available cores
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single_file, args): args[0] for args in args_list}

        completed = 0
        for future in as_completed(futures):
            completed += 1
            success, filename, error = future.result()

            if success:
                print(f"  [{completed}/{len(parquet_files)}] ✓ {filename}")
            else:
                print(f"  [{completed}/{len(parquet_files)}] ❌ {filename}: {error}")

    print()
    print("✓ Parallel processing complete!")


if __name__ == "__main__":
    import sys

    print("\n" + "=" * 60)
    print("GeoParquet-IO Batch Processing Examples")
    print("=" * 60)
    print()

    if len(sys.argv) < 3:
        print("Usage:")
        print("  python batch_processing.py <input_dir> <output_dir> [operation]")
        print()
        print("Operations:")
        print("  add-bbox  - Add bbox column to all files")
        print("  sort      - Sort files using Hilbert curve")
        print("  both      - Sort and add bbox (recommended)")
        print()
        print("Example:")
        print("  python batch_processing.py ./input ./output both")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    operation = sys.argv[3] if len(sys.argv) > 3 else "both"

    # Run batch processing
    process_directory(input_dir, output_dir, operation, verbose=False)

    print()
    print("For parallel processing on multi-core machines, see the")
    print("example_parallel_processing() function in this script.")
