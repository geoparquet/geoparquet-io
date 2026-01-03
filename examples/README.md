# GeoParquet-IO Examples

This directory contains example scripts and Jupyter notebooks demonstrating how to use geoparquet-io.

## Jupyter Notebooks

Interactive notebooks for learning the Python API:

| Notebook | Description |
|----------|-------------|
| [01_getting_started.ipynb](01_getting_started.ipynb) | Reading files, inspecting properties, basic transformations |
| [02_python_api_chaining.ipynb](02_python_api_chaining.ipynb) | Method chaining, pipelines, ops module |
| [03_spatial_indices.ipynb](03_spatial_indices.ipynb) | Bbox, H3, quadkey, and KD-tree columns |
| [04_partitioning.ipynb](04_partitioning.ipynb) | Splitting data into multiple files |
| [05_cloud_workflows.ipynb](05_cloud_workflows.ipynb) | Uploading to S3, GCS, Azure |

The notebooks use sample data from the `data/` subdirectory.

To run the notebooks:
```bash
pip install jupyter
cd examples
jupyter notebook
```

## Python Scripts

### 1. Basic Usage (`basic_usage.py`)

Demonstrates fundamental operations using the Python API:

- Adding bbox columns
- Hilbert curve sorting
- Checking file quality
- Getting dataset bounds
- Using different compression options

**Usage:**
```bash
# Ensure you have a sample file named 'input.parquet'
python basic_usage.py
```

### 2. Batch Processing (`batch_processing.py`)

Shows how to process multiple files in a directory:

- Sequential batch processing
- Parallel processing with multiprocessing
- Progress tracking
- Error handling

**Usage:**
```bash
# Process all files in a directory
python batch_processing.py ./input_dir ./output_dir both

# Operations: add-bbox, sort, or both
python batch_processing.py ./data ./processed add-bbox
```

## CLI Examples

The examples above use the Python API, but you can also use the CLI:

### Quick Operations

```bash
# Add bbox to a single file
gpio add bbox input.parquet output.parquet

# Sort using Hilbert curve
gpio sort hilbert input.parquet output_sorted.parquet --add-bbox

# Check file quality
gpio check all myfile.parquet --verbose

# Partition by country
gpio add admin-divisions input.parquet output.parquet

# Partition into separate files
gpio partition admin buildings.parquet output/ --column country_code
```

### Advanced Options

```bash
# Custom compression
gpio add bbox input.parquet output.parquet \
  --compression BROTLI \
  --compression-level 11

# Custom row group sizes
gpio sort hilbert input.parquet output.parquet \
  --row-group-size-mb 256MB

# Dry run to see what would happen
gpio add admin-divisions input.parquet output.parquet --dry-run

# Preview partitions before creating
gpio partition string data.parquet --column category --preview
```

### Batch Processing with Shell

```bash
# Process all files in a directory
for file in input/*.parquet; do
    gpio add bbox "$file" "output/$(basename "$file")"
done

# With parallel (GNU parallel)
parallel gpio add bbox {} output/{/} ::: input/*.parquet

# With xargs
find input -name "*.parquet" | \
  xargs -I {} -P 4 gpio sort hilbert {} output/$(basename {})
```

## Creating Test Data

If you need sample GeoParquet files for testing:

```python
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point
import struct

# Create WKB for a simple point
def point_to_wkb(x, y):
    """Convert coordinates to WKB format."""
    # WKB format: byte order (1) + type (4) + x (8) + y (8)
    return struct.pack('<BId d', 1, 1, x, y)

# Create sample data
data = {
    'id': range(100),
    'name': [f'Feature {i}' for i in range(100)],
    'geometry': [point_to_wkb(i * 0.1, i * 0.1) for i in range(100)]
}

table = pa.table(data)

# Add GeoParquet metadata
geo_metadata = {
    "version": "1.0.0",
    "primary_column": "geometry",
    "columns": {
        "geometry": {
            "encoding": "WKB",
            "geometry_types": ["Point"]
        }
    }
}

import json
table = table.replace_schema_metadata({
    b"geo": json.dumps(geo_metadata).encode()
})

# Write file
pq.write_table(table, 'input.parquet')
print("✓ Created input.parquet")
```

## Performance Tips

1. **Use bbox columns**: Significantly faster for spatial queries
   ```bash
   gpio add bbox input.parquet output.parquet
   ```

2. **Sort for spatial locality**: Improves query performance
   ```bash
   gpio sort hilbert input.parquet output.parquet
   ```

3. **Choose appropriate compression**: ZSTD for balance, BROTLI for size
   ```bash
   gpio add bbox input.parquet output.parquet --compression ZSTD
   ```

4. **Optimize row groups**: ~128-256MB is usually optimal
   ```bash
   gpio sort hilbert input.parquet output.parquet --row-group-size-mb 256MB
   ```

## Questions?

- Check the [main README](../README.md) for documentation
- See [CONTRIBUTING.md](../CONTRIBUTING.md) for development info
- Open an issue on GitHub for bugs or feature requests
