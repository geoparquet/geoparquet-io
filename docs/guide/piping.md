# Command Piping

gpio supports Unix-style command piping using Arrow IPC streaming. This allows you to chain multiple commands together without creating intermediate files, resulting in faster execution and reduced disk I/O.

## Basic Piping

Use `-` as the input to read from stdin. Output is **auto-detected** - when stdout is piped to another command, gpio automatically streams Arrow IPC:

```bash
# Add bbox, then sort by Hilbert curve
gpio add bbox input.parquet | gpio sort hilbert - output.parquet

# Extract, add bbox, then add quadkey
gpio extract --limit 1000 input.parquet | gpio add bbox - | gpio add quadkey - output.parquet
```

You can also explicitly use `-` for output if preferred:

```bash
gpio add bbox input.parquet - | gpio sort hilbert - output.parquet
```

## Supported Commands

All transformation commands support Arrow IPC piping:

| Command | Stdin Input | Stdout Output |
|---------|-------------|---------------|
| `extract` | Yes | Yes |
| `add bbox` | Yes | Yes |
| `add quadkey` | Yes | Yes |
| `add h3` | Yes | Yes |
| `add kdtree` | Yes | Yes |
| `add admin-divisions` | Yes | Yes |
| `sort hilbert` | Yes | Yes |
| `sort quadkey` | Yes | Yes |
| `sort column` | Yes | Yes |
| `reproject` | Yes | Yes |
| `convert geojson` | Yes | No (outputs GeoJSON to stdout) |
| `partition string` | Yes | No (writes to directory) |
| `partition quadkey` | Yes | No (writes to directory) |
| `partition h3` | Yes | No (writes to directory) |
| `partition kdtree` | Yes | No (writes to directory) |
| `partition admin` | Yes | No (writes to directory) |

## Performance Benefits

Piping eliminates intermediate file I/O, providing significant speedups for multi-step workflows:

| Workflow | File-based | Piped | Speedup |
|----------|------------|-------|---------|
| add bbox → add quadkey → sort hilbert | 34s | 16s | 53% faster |

For even better performance, use the [Python API](../api/python-api.md) which keeps data in memory.

## Common Patterns

### Transform Pipeline

Chain transformations without intermediate files:

```bash
gpio add bbox input.parquet | \
  gpio add quadkey - | \
  gpio sort hilbert - output.parquet
```

### Extract and Transform

Filter data before applying transformations:

```bash
gpio extract --limit 10000 large_file.parquet | \
  gpio add bbox - | \
  gpio sort hilbert - subset.parquet
```

### Spatial Filter and Partition

Filter by bounding box then partition:

```bash
gpio extract --bbox "-122.5,37.5,-122.0,38.0" input.parquet | \
  gpio add quadkey - | \
  gpio partition string --column quadkey --chars 4 - output_dir/
```

### Column Selection Through Pipe

Select columns first, then add computed columns:

```bash
gpio extract --include-cols name,address input.parquet | \
  gpio add bbox - output.parquet
```

### Add Multiple Spatial Indices

Chain multiple `add` commands to add several spatial indices:

```bash
gpio add bbox input.parquet | \
  gpio add h3 --resolution 9 - | \
  gpio add quadkey - | \
  gpio sort hilbert - output.parquet
```

### Reproject and Transform

Reproject to a different CRS before adding indices:

```bash
gpio convert reproject --dst-crs EPSG:4326 input.parquet | \
  gpio add bbox - | \
  gpio sort hilbert - output.parquet
```

### Full Processing Pipeline

Combine extract, reproject, add indices, sort, and partition:

```bash
gpio extract --bbox "-122.5,37.5,-122.0,38.0" input.parquet | \
  gpio add bbox - | \
  gpio add h3 --resolution 8 - | \
  gpio sort hilbert - | \
  gpio partition h3 --resolution 4 - output_dir/
```

## How It Works

When you use `-` for output, gpio writes data in [Arrow IPC streaming format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) instead of Parquet. This format:

- Supports streaming (no need to buffer entire dataset)
- Preserves schema and metadata
- Enables zero-copy data transfer between processes
- Is compatible with any Arrow-based tool

The receiving command reads the Arrow IPC stream, processes the data, and outputs either another Arrow stream (for further piping) or a Parquet file.

## Auto-Detection

gpio automatically detects when stdout is piped to another process. You don't need to specify `-` for output:

```bash
# Output is auto-detected when piped
gpio add bbox input.parquet | gpio sort hilbert - output.parquet

# Explicit '-' also works
gpio add bbox input.parquet - | gpio sort hilbert - output.parquet
```

When output is omitted and stdout is piped, gpio streams Arrow IPC. When stdout is a terminal, gpio requires an explicit output path.

## Error Handling

If a command in the pipeline fails, the error is propagated:

```bash
# If the file doesn't exist, the first command fails
gpio add bbox nonexistent.parquet - | gpio sort hilbert - output.parquet
# Error: File not found: nonexistent.parquet
```

For debugging, you can save intermediate results:

```bash
# Debug: save intermediate result
gpio add bbox input.parquet intermediate.parquet
gpio inspect intermediate.parquet
gpio sort hilbert intermediate.parquet output.parquet
```

## Limitations

- **Partition commands**: `partition string`, `partition quadkey`, etc. can read from stdin but always write to a directory (not stdout)
- **Remote output**: Streaming to remote destinations (S3, HTTP) is not supported; use file output then `gpio publish upload`
- **Memory**: Large datasets are streamed, but some operations (like Hilbert sorting) require loading the full dataset into memory

## Python API Alternative

For maximum performance, use the Python API which keeps data in memory:

```python
import geoparquet_io as gpio

# Equivalent to:
# gpio extract --bbox "..." input.parquet | gpio add bbox - | gpio sort hilbert - output.parquet

gpio.read('input.parquet') \
    .extract(bbox=(-122.5, 37.5, -122.0, 38.0)) \
    .add_bbox() \
    .sort_hilbert() \
    .write('output.parquet')
```

### Performance Comparison

| Approach | Time (75MB, 400K rows) | Notes |
|----------|------------------------|-------|
| CLI (file-based) | 34s | Each command writes intermediate file |
| CLI (piped) | 16s | Arrow IPC streaming between commands |
| **Python API** | **7s** | In-memory, no I/O overhead |

The Python API is up to 5x faster than file-based CLI operations because data stays in memory as Arrow tables. Use the Python API when:

- You're building Python applications
- Performance is critical
- You need to integrate with other Python tools
- You're working in Jupyter notebooks

See [Python API Reference](../api/python-api.md) for full documentation.

## See Also

- [Python API](../api/python-api.md) - For programmatic access with even better performance
- [Extract Command](extract.md) - Filtering and column selection
- [Add Command](../cli/add.md) - Add bbox, H3, quadkey, KD-tree, and admin division columns
- [Sort Command](sort.md) - Hilbert, quadkey, and column sorting
- [Reproject Guide](../cli/convert.md) - Reprojection options
- [Partition Command](partition.md) - Partitioning strategies
