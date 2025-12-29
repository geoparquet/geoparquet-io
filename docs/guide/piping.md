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

The following commands support Arrow IPC piping:

| Command | Stdin Input | Stdout Output |
|---------|-------------|---------------|
| `extract` | Yes | Yes |
| `add bbox` | Yes | Yes |
| `add quadkey` | Yes | Yes |
| `sort hilbert` | Yes | Yes |
| `partition string` | Yes | No (writes to directory) |

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
- **Remote output**: Streaming to remote destinations (S3, HTTP) is not supported; use file output then `gpio upload`
- **Memory**: Large datasets are streamed, but some operations (like Hilbert sorting) require loading the full dataset into memory

## See Also

- [Python API](../api/python-api.md) - For programmatic access with even better performance
- [Extract Command](extract.md) - Filtering and column selection
- [Sort Command](sort.md) - Hilbert and other sorting options
- [Partition Command](partition.md) - Partitioning strategies
