# Write Strategies for Large Files

gpio handles larger-than-memory GeoParquet files efficiently through a pluggable write strategy system. The default strategy streams data directly to disk with constant memory usage, allowing you to process files of any size.

## The Default: DuckDB Streaming

gpio uses the `duckdb-kv` strategy by default. This strategy:

- Uses **O(1) constant memory** regardless of file size
- Streams data through DuckDB's native COPY TO command
- Embeds GeoParquet metadata directly in the Parquet footer
- Handles files of any size without running out of memory
- Is **container-aware**, automatically detecting Docker/Kubernetes memory limits

For most users, the default just works. No configuration is needed:

=== "CLI"

    ```bash
    # Process a 50GB file on a machine with 4GB RAM
    gpio extract huge_dataset.parquet filtered.parquet --bbox -122.5,37.5,-122.0,38.0

    # Convert a massive shapefile to GeoParquet
    gpio convert large_file.shp output.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Process large files with the fluent API
    gpio.read('huge_dataset.parquet') \
        .extract(bbox=(-122.5, 37.5, -122.0, 38.0)) \
        .write('filtered.parquet')
    ```

## Strategy Comparison

| Strategy | Memory Usage | Speed | Best For |
|----------|--------------|-------|----------|
| `duckdb-kv` | O(1) constant | Fastest | **Default for all use cases** |
| `streaming` | O(batch) constant | Moderate | Alternative when duckdb-kv has issues |
| `disk-rewrite` | O(rowgroup) | Slowest | Maximum compatibility fallback |
| `in-memory` | O(n) proportional | Fast | Legacy/verification mode |

### Strategy Details

**duckdb-kv** (Default)

The recommended strategy for all production workloads. Uses DuckDB's native COPY TO with KV_METADATA option for a single atomic write operation with no post-processing.

**streaming**

Uses PyArrow's streaming writer to process data in batches. Good alternative if you encounter issues with the DuckDB strategy. Memory usage is proportional to batch size, not file size.

**disk-rewrite**

Writes data with DuckDB, then rewrites row-group by row-group using PyArrow to add proper GeoParquet metadata. Uses more memory than streaming but provides maximum compatibility.

**in-memory**

Loads the entire dataset into memory before writing. Only use this for small files or when you need to verify that another strategy is producing correct output.

## Selecting a Strategy

Override the default strategy when needed:

=== "CLI"

    ```bash
    # Use streaming strategy
    gpio extract input.parquet output.parquet --write-strategy streaming

    # Use in-memory for verification
    gpio extract input.parquet output.parquet --write-strategy in-memory
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Use streaming strategy
    gpio.read('input.parquet').write('output.parquet', write_strategy='streaming')

    # Use in-memory for verification
    gpio.read('input.parquet').write('output.parquet', write_strategy='in-memory')
    ```

## When to Use Alternative Strategies

### Decision Flowchart

1. **Start with the default** (`duckdb-kv`) - it handles any file size efficiently
2. **Output seems wrong?** Try `in-memory` to verify correct behavior
3. **`in-memory` works but `duckdb-kv` doesn't?** Report a bug, use `streaming` as workaround
4. **Need maximum compatibility?** Try `disk-rewrite`

### Specific Scenarios

| Scenario | Recommended Strategy |
|----------|---------------------|
| Large file, limited memory | `duckdb-kv` (default) |
| Debugging output differences | `in-memory` to verify |
| DuckDB issues with specific data | `streaming` |
| Older tools can't read output | `disk-rewrite` |

## Memory Configuration

### Automatic Detection

gpio automatically detects available memory and configures DuckDB to use 50% of it. This detection is container-aware:

- **cgroup v2** (modern Docker, Kubernetes): Reads `/sys/fs/cgroup/memory.max`
- **cgroup v1** (older Docker): Reads `/sys/fs/cgroup/memory/memory.limit_in_bytes`
- **Bare metal**: Falls back to psutil for system memory detection

### Explicit Memory Limits

Override auto-detection when needed:

=== "CLI"

    ```bash
    # Limit DuckDB to 2GB for streaming writes
    gpio extract input.parquet output.parquet --write-memory 2GB

    # Smaller limit for restricted environments
    gpio extract input.parquet output.parquet --write-memory 512MB

    # Combine with strategy selection
    gpio extract input.parquet output.parquet \
        --write-strategy streaming \
        --write-memory 1GB
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Limit DuckDB memory
    gpio.read('input.parquet').write('output.parquet', write_memory='2GB')

    # Combine with strategy selection
    gpio.read('input.parquet').write(
        'output.parquet',
        write_strategy='streaming',
        write_memory='1GB'
    )
    ```

### Memory Sizing Guidelines

| Environment | Recommended `--write-memory` |
|-------------|------------------------------|
| Laptop (8GB RAM) | `2GB` - `4GB` |
| Workstation (32GB RAM) | `8GB` - `16GB` (default auto-detects) |
| Docker container | Auto-detected from cgroup limits |
| Kubernetes pod | Auto-detected from cgroup limits |
| AWS Lambda (1GB) | `384MB` |
| Cloud Run (2GB) | `768MB` |

!!! tip "Container Environments"
    gpio automatically respects container memory limits. If you're running in Docker or Kubernetes with memory limits set, you typically don't need to specify `--write-memory` manually.

## Container Environments

### Docker

gpio detects Docker memory limits automatically via cgroups. No extra configuration needed:

```bash
# Docker automatically limits memory, gpio respects it
docker run -m 2g my-gpio-image gpio extract input.parquet output.parquet
```

If you need explicit control:

```bash
docker run -m 2g my-gpio-image gpio extract input.parquet output.parquet --write-memory 1GB
```

### Kubernetes

Memory limits from pod specifications are detected via cgroups:

```yaml
resources:
  limits:
    memory: "4Gi"
```

gpio will automatically use approximately 2GB (50% of the limit) for DuckDB operations.

### Serverless (AWS Lambda, Cloud Run)

For serverless environments with tight memory constraints:

=== "CLI"

    ```bash
    # AWS Lambda with 1GB memory
    gpio extract input.parquet output.parquet --write-memory 384MB

    # Cloud Run with 2GB memory
    gpio extract input.parquet output.parquet --write-memory 768MB
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    def handler(event, context):
        # Lambda with 1GB memory
        gpio.read('s3://bucket/input.parquet') \
            .extract(bbox=event['bbox']) \
            .write('/tmp/output.parquet', write_memory='384MB')
    ```

## Examples

### Process a Large Dataset

=== "CLI"

    ```bash
    # 100GB dataset on a 16GB machine - just works
    gpio extract large_dataset.parquet filtered.parquet \
        --bbox -122.5,37.5,-122.0,38.0 \
        --where "population > 1000"
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Large file processing with the fluent API
    gpio.read('large_dataset.parquet') \
        .extract(
            bbox=(-122.5, 37.5, -122.0, 38.0),
            where="population > 1000"
        ) \
        .write('filtered.parquet')
    ```

### Troubleshoot Output Issues

If you suspect the default strategy is producing incorrect output:

=== "CLI"

    ```bash
    # 1. Write with in-memory strategy (loads full dataset)
    gpio extract input.parquet test_inmemory.parquet --write-strategy in-memory

    # 2. Compare with default strategy output
    gpio inspect test_inmemory.parquet --stats
    gpio inspect output.parquet --stats
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Verify with in-memory strategy
    table = gpio.read('input.parquet')

    # Write with default
    table.write('output_default.parquet')

    # Write with in-memory for comparison
    table.write('output_inmemory.parquet', write_strategy='in-memory')

    # Compare
    default = gpio.read('output_default.parquet')
    inmemory = gpio.read('output_inmemory.parquet')
    print(f"Default rows: {default.num_rows}, In-memory rows: {inmemory.num_rows}")
    ```

### Batch Processing in Constrained Environment

=== "CLI"

    ```bash
    # Process multiple files with limited memory
    for f in data/*.parquet; do
        gpio extract "$f" "output/$(basename $f)" \
            --write-memory 512MB \
            --bbox -122.5,37.5,-122.0,38.0
    done
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio
    from pathlib import Path

    # Batch processing with explicit memory limit
    for input_file in Path('data').glob('*.parquet'):
        gpio.read(input_file) \
            .extract(bbox=(-122.5, 37.5, -122.0, 38.0)) \
            .write(f'output/{input_file.name}', write_memory='512MB')
    ```

## See Also

- [Extracting Data](extract.md) - Full extract command documentation
- [Best Practices](../concepts/best-practices.md) - GeoParquet optimization tips
- [Troubleshooting](../troubleshooting.md) - Common issues and solutions
