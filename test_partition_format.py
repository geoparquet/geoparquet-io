#!/usr/bin/env python3
"""Test script to verify partition output format."""

import tempfile
import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner
from geoparquet_tools.cli.main import cli
from geoparquet_tools.core.common import check_bbox_structure, parse_geo_metadata

def create_test_file_with_bbox():
    """Create a test GeoParquet file with bbox column."""
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        wkb_point = bytes.fromhex('0101000000000000000000000000000000000000000000000000000000')

        # Create bbox struct type
        bbox_type = pa.struct([
            ('xmin', pa.float64()),
            ('ymin', pa.float64()),
            ('xmax', pa.float64()),
            ('ymax', pa.float64())
        ])

        # Create table with bbox column
        table = pa.table({
            'id': ['1', '2', '3', '4'],
            'admin:country_code': ['US', 'US', 'CA', 'MX'],
            'geometry': [wkb_point] * 4,
            'bbox': [
                {'xmin': 0.0, 'ymin': 0.0, 'xmax': 1.0, 'ymax': 1.0},
                {'xmin': 0.0, 'ymin': 0.0, 'xmax': 1.0, 'ymax': 1.0},
                {'xmin': 1.0, 'ymin': 1.0, 'xmax': 2.0, 'ymax': 2.0},
                {'xmin': 2.0, 'ymin': 2.0, 'xmax': 3.0, 'ymax': 3.0}
            ]
        })

        # Add GeoParquet 1.0 metadata (intentionally old version)
        metadata = {
            b'geo': json.dumps({
                "version": "1.0.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "geometry_types": ["Point"]
                    }
                }
            }).encode('utf-8')
        }

        table = table.replace_schema_metadata(metadata)
        pq.write_table(table, tmp.name)

        print(f"Created test file: {tmp.name}")
        return tmp.name

def check_partition_output(output_dir):
    """Check the format of partition output files."""
    print(f"\nChecking partition output in {output_dir}")

    # Find all parquet files
    parquet_files = []
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_files.append(os.path.join(root, file))

    print(f"Found {len(parquet_files)} parquet files")

    for file_path in parquet_files[:2]:  # Check first 2 files
        print(f"\n--- Checking {os.path.basename(os.path.dirname(file_path))}/{os.path.basename(file_path)} ---")

        # Read file
        with open(file_path, 'rb') as f:
            pf = pq.ParquetFile(f)
            metadata = pf.schema_arrow.metadata

            # Check GeoParquet metadata
            if metadata and b'geo' in metadata:
                geo_meta = json.loads(metadata[b'geo'].decode('utf-8'))
                print(f"GeoParquet version: {geo_meta.get('version', 'MISSING')}")
                print(f"Primary column: {geo_meta.get('primary_column', 'MISSING')}")

                # Check bbox covering
                columns = geo_meta.get('columns', {})
                geom_col = geo_meta.get('primary_column', 'geometry')
                if geom_col in columns:
                    covering = columns[geom_col].get('covering', {})
                    if 'bbox' in covering:
                        print("✓ Has bbox covering metadata")
                    else:
                        print("✗ Missing bbox covering metadata")
            else:
                print("✗ No GeoParquet metadata found!")

            # Check compression
            if pf.num_row_groups > 0:
                row_group = pf.metadata.row_group(0)
                if row_group.num_columns > 0:
                    # Find geometry column
                    for i in range(row_group.num_columns):
                        col_meta = row_group.column(i)
                        if 'geometry' in pf.schema_arrow.field(i).name:
                            compression = str(col_meta.compression)
                            print(f"Geometry compression: {compression}")
                            break

            # Check row groups
            print(f"Row groups: {pf.num_row_groups}")
            if pf.num_row_groups > 0:
                total_rows = sum(pf.metadata.row_group(i).num_rows for i in range(pf.num_row_groups))
                avg_rows = total_rows / pf.num_row_groups
                print(f"Average rows per group: {avg_rows:.0f}")

def main():
    # Create test file
    test_file = create_test_file_with_bbox()

    try:
        # Test with regular partition
        with tempfile.TemporaryDirectory() as temp_dir:
            print("\n=== Testing regular partition ===")
            runner = CliRunner()
            result = runner.invoke(cli, [
                'partition', 'admin',
                test_file,
                temp_dir
            ])

            if result.exit_code != 0:
                print(f"Error: {result.output}")
            else:
                print(f"Success: {result.output}")
                check_partition_output(temp_dir)

        # Test with Hive partition
        with tempfile.TemporaryDirectory() as temp_dir:
            print("\n=== Testing Hive partition ===")
            runner = CliRunner()
            result = runner.invoke(cli, [
                'partition', 'admin',
                test_file,
                temp_dir,
                '--hive'
            ])

            if result.exit_code != 0:
                print(f"Error: {result.output}")
            else:
                print(f"Success: {result.output}")
                check_partition_output(temp_dir)

    finally:
        # Cleanup
        if os.path.exists(test_file):
            os.unlink(test_file)

if __name__ == "__main__":
    main()