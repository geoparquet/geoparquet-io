#!/usr/bin/env python3
"""
Smoke test for basic GeoParquet read/write operations.

This test verifies that pyarrow can successfully read and write
GeoParquet files with proper metadata handling.
"""

import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main():
    """Run basic GeoParquet read/write smoke test."""
    # Create a simple test parquet file with geo metadata
    table = pa.table({
        'id': [1, 2, 3],
        'geometry': [b'POINT(0 0)', b'POINT(1 1)', b'POINT(2 2)']
    })
    
    # Add minimal GeoParquet metadata
    geo_metadata = {
        'version': '1.0.0',
        'primary_column': 'geometry',
        'columns': {
            'geometry': {
                'encoding': 'WKB',
                'geometry_types': ['Point']
            }
        }
    }
    
    metadata = table.schema.metadata or {}
    metadata[b'geo'] = json.dumps(geo_metadata).encode('utf-8')
    table = table.replace_schema_metadata(metadata)
    
    # Write and read back
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / 'test.parquet'
        pq.write_table(table, test_file)
        
        # Read back
        result = pq.read_table(test_file)
        assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
        assert b'geo' in result.schema.metadata, "GeoParquet metadata not found"
        
        # Verify metadata integrity
        geo_meta_back = json.loads(result.schema.metadata[b'geo'].decode('utf-8'))
        assert geo_meta_back['version'] == '1.0.0', "Version mismatch"
        assert geo_meta_back['primary_column'] == 'geometry', "Primary column mismatch"
        
    print('[PASS] Basic read/write test passed')
    print(f'[PASS] PyArrow version {pa.__version__} works correctly')


if __name__ == '__main__':
    main()
