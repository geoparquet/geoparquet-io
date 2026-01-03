# CLI vs Python API

geoparquet-io offers two ways to work with GeoParquet files. This guide helps you choose the right approach for your use case.

## Quick Comparison

| Feature | CLI | Python API |
|---------|-----|------------|
| **Performance** | Good (with piping) | Best (in-memory) |
| **Ease of use** | Simple commands | Fluent chaining |
| **Integration** | Shell scripts, CI/CD | Python applications |
| **Interactivity** | Terminal | Jupyter notebooks |
| **Remote files** | Full support | Partial support |

## When to Use the CLI

### One-off File Operations

Quick transformations without writing code:

```bash
# Add bbox and sort
gpio add bbox input.parquet | gpio sort hilbert - output.parquet

# Check file quality
gpio check all myfile.parquet

# Inspect metadata
gpio inspect myfile.parquet --stats
```

### Shell Scripts and Automation

CI/CD pipelines, cron jobs, and data processing scripts:

```bash
#!/bin/bash
for file in *.parquet; do
    gpio add bbox "$file" | gpio sort hilbert - "optimized/$file"
done
```

### Remote File Processing

Read from and write to cloud storage:

```bash
gpio add bbox s3://bucket/input.parquet s3://bucket/output.parquet --profile my-aws
gpio sort hilbert https://example.com/data.parquet s3://bucket/sorted.parquet
```

### Piping Multiple Commands

Chain operations with Unix pipes:

```bash
gpio extract --bbox "-122.5,37.5,-122.0,38.0" input.parquet | \
    gpio add bbox - | \
    gpio add h3 --resolution 9 - | \
    gpio sort hilbert - output.parquet
```

## When to Use the Python API

### Python Applications

Integrate with existing Python code:

```python
import geoparquet_io as gpio

def process_data(input_path: str, output_path: str):
    gpio.read(input_path) \
        .add_bbox() \
        .sort_hilbert() \
        .write(output_path)
```

### Maximum Performance

The Python API is up to 5x faster than CLI:

```python
import geoparquet_io as gpio

# Data stays in memory - no intermediate file I/O
result = gpio.read('input.parquet') \
    .extract(limit=10000) \
    .add_bbox() \
    .add_h3(resolution=9) \
    .sort_hilbert()

result.write('output.parquet')
```

### Jupyter Notebooks

Interactive exploration and analysis:

```python
import geoparquet_io as gpio

# Read and explore
table = gpio.read('data.parquet')
table.info()

# Transform and inspect
result = table.add_bbox().sort_hilbert()
print(f"Processed {result.num_rows} rows")
print(f"Bounds: {result.bounds}")
```

### Conditional Processing

Apply different operations based on data characteristics:

```python
import geoparquet_io as gpio

table = gpio.read('input.parquet')

# Apply different processing based on size
if table.num_rows > 1_000_000:
    # Large file: add H3 for later partitioning
    result = table.add_bbox().add_h3(resolution=9).sort_hilbert()
else:
    # Small file: just optimize
    result = table.add_bbox().sort_hilbert()

result.write('output.parquet')
```

### Integration with PyArrow

Combine with other Arrow-based tools:

```python
import pyarrow.parquet as pq
import geoparquet_io as gpio
from geoparquet_io.api import Table

# Read with PyArrow
arrow_table = pq.read_table('input.parquet')

# Process with gpio
table = Table(arrow_table)
result = table.add_bbox().sort_hilbert()

# Continue with PyArrow or other tools
arrow_result = result.to_arrow()
```

### Reusable Pipelines

Define and apply standard processing pipelines:

```python
from geoparquet_io.api import pipe, read

# Define reusable pipeline
optimize = pipe(
    lambda t: t.add_bbox(),
    lambda t: t.add_h3(resolution=9),
    lambda t: t.sort_hilbert(),
)

# Apply to any file
result = optimize(read('input.parquet'))
result.write('output.parquet')
```

## Performance Comparison

Benchmark on a 75MB file with 400K rows (add bbox + add quadkey + sort hilbert):

| Approach | Time | Relative Speed |
|----------|------|----------------|
| CLI (file-based) | 34s | 1x (baseline) |
| CLI (piped) | 16s | 2x faster |
| **Python API** | **7s** | **5x faster** |

The Python API is faster because:
- Data stays in memory as Arrow tables
- No intermediate file I/O
- Zero-copy operations where possible

## Mixing CLI and Python

You can use both together:

```python
import subprocess
import geoparquet_io as gpio

# Use CLI for remote file download
subprocess.run([
    'gpio', 'extract', '--limit', '10000',
    's3://bucket/huge.parquet', 'local_subset.parquet'
])

# Use Python API for processing
gpio.read('local_subset.parquet') \
    .add_bbox() \
    .sort_hilbert() \
    .write('processed.parquet')
```

## Equivalent Commands

Here are common operations in both styles:

### Add Bbox and Sort

```bash
# CLI
gpio add bbox input.parquet | gpio sort hilbert - output.parquet
```

```python
# Python
gpio.read('input.parquet').add_bbox().sort_hilbert().write('output.parquet')
```

### Filter by Bounding Box

```bash
# CLI
gpio extract --bbox "-122.5,37.5,-122.0,38.0" input.parquet output.parquet
```

```python
# Python
gpio.read('input.parquet').extract(bbox=(-122.5, 37.5, -122.0, 38.0)).write('output.parquet')
```

### Add Multiple Indices

```bash
# CLI
gpio add bbox input.parquet | gpio add h3 --resolution 9 - | gpio add quadkey - output.parquet
```

```python
# Python
gpio.read('input.parquet').add_bbox().add_h3(resolution=9).add_quadkey().write('output.parquet')
```

### Partition Data

```bash
# CLI
gpio partition h3 input.parquet output_dir/ --resolution 6
```

```python
# Python
gpio.read('input.parquet').add_h3(resolution=9).partition_by_h3('output/', resolution=6)
```

## Summary

| Use Case | Recommendation |
|----------|----------------|
| Quick one-off transformations | CLI |
| Shell scripts and CI/CD | CLI with piping |
| Remote file processing | CLI |
| Python applications | Python API |
| Jupyter notebooks | Python API |
| Maximum performance | Python API |
| Conditional processing | Python API |
| Integration with PyArrow | Python API |

## See Also

- [Quick Start Tutorial](quickstart.md) - Get started with both approaches
- [Command Piping](../guide/piping.md) - CLI piping guide
- [Python API Reference](../api/python-api.md) - Full Python API documentation
