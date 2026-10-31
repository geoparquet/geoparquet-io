# GeoParquet Best Practices

This guide explains the optimizations that make GeoParquet files fast and efficient for spatial queries.

## Quick Checklist

Run `gpio check all myfile.parquet` to verify your file follows these best practices:

- [ ] Spatial ordering (Hilbert curve)
- [ ] Bbox column with covering metadata
- [ ] ZSTD compression
- [ ] Appropriate row group sizes

## Spatial Ordering

### What It Is

Spatial ordering arranges rows so that geographically nearby features are stored together in the file. gpio uses **Hilbert curve** ordering, which maps 2D space to 1D while preserving locality.

### Why It Matters

Without spatial ordering:
```
Row 1: New York
Row 2: Tokyo
Row 3: London
Row 4: Sydney
...
```

With Hilbert ordering:
```
Row 1: New York
Row 2: Boston
Row 3: Philadelphia
Row 4: Washington DC
...
```

**Benefits:**
- Spatial queries read fewer row groups
- Better compression (similar coordinates compress well)
- Reduced I/O for bounding box filters

### How to Apply

```bash
# Sort existing file
gpio sort hilbert input.parquet sorted.parquet

# Convert with automatic Hilbert ordering (default)
gpio convert input.shp output.parquet

# Convert without Hilbert ordering (faster but less optimal)
gpio convert input.shp output.parquet --skip-hilbert
```

## Bounding Box Columns

### What They Are

A bbox column stores the bounding box for each feature as a struct:

```
bbox: {xmin: -122.5, ymin: 37.5, xmax: -122.0, ymax: 38.0}
```

### Why They Matter

Spatial queries typically need to check "does this feature intersect my area of interest?"

Without bbox: Must decode WKB geometry and compute intersection (slow)
With bbox: Compare 4 numbers (fast), only decode geometry for candidates

**Performance difference:** 10-100x faster for spatial filters on large files.

### Covering Metadata

GeoParquet 1.1+ includes "covering" metadata that tells query engines how to use bbox columns:

```json
"covering": {
  "bbox": {
    "xmin": ["bbox", "xmin"],
    "ymin": ["bbox", "ymin"],
    "xmax": ["bbox", "xmax"],
    "ymax": ["bbox", "ymax"]
  }
}
```

This enables automatic optimization in tools like DuckDB and BigQuery.

### How to Apply

```bash
# Add bbox column with metadata
gpio add bbox input.parquet output.parquet

# Add bbox metadata to existing bbox column
gpio add bbox-metadata myfile.parquet

# Convert with automatic bbox (default)
gpio convert input.shp output.parquet
```

## Compression

### Recommendations

| Use Case | Compression | Level | Rationale |
|----------|-------------|-------|-----------|
| General purpose | ZSTD | 15 | Best balance of size and speed |
| Computed output (`gpio process`) | ZSTD | 9 | Near-identical size, far faster write |
| Maximum compression | ZSTD | 22 | Smaller files, slower write |
| Fast decompression | LZ4 | - | Analytics workloads |
| Wide compatibility | GZIP | 6 | Older tools |

gpio writes **ZSTD level 15** by default on every write path, the plain
DuckDB `COPY` paths included, except for the `gpio process` commands, which
default to **level 9**. One owner (`core/compression.resolve_codec`) resolves
the codec and level for every writer, so an unset level is never an encoder's
own default (DuckDB's is 3, pyarrow's is 1).

### Why the process commands differ

`gpio process` computes a whole table and then writes it, so the write is a
large share of the command's wall clock — 67-90% of it for `polygonize` and
`contour`. Measured on their own outputs, level 9 writes 2.8-8.2x faster than
level 15 for at most 4.9% more bytes, and on a 217k-row polygonize table it is
both faster *and* smaller — level 15's long-range matching buys nothing on a
column of short, high-entropy WKB blobs and costs bytes of window bookkeeping.

Level 9 is the only level that is close to free. Below it the size cost stops
being noise — level 3 is 15-20% larger on geometry-heavy tables and level 1 is
34-35% larger — so reach lower only for intermediate files you intend to
delete.

### Why ZSTD?

- 3-5x faster decompression than GZIP
- Similar or better compression ratio
- Widely supported in modern tools

### How to Apply

=== "CLI"

    ```bash
    # Default ZSTD compression
    gpio convert input.shp output.parquet

    # The process group's default (level 9)
    gpio process simplify input.parquet output.parquet --tolerance 0.001

    # Ask a process command for the global default explicitly
    gpio process simplify input.parquet output.parquet --tolerance 0.001 \
        --compression-level 15

    # Maximum compression
    gpio convert input.shp output.parquet --compression ZSTD --compression-level 22

    # Fast decompression
    gpio convert input.shp output.parquet --compression LZ4
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio
    from geoparquet_io import ops

    # Default ZSTD compression
    gpio.convert('input.shp').write('output.parquet')

    # A process function that writes the file takes the group's default (level 9)
    ops.create_overviews('cells.parquet', levels=[4, 7])

    # ...and still honours an explicit level
    ops.create_overviews('cells.parquet', levels=[4, 7], compression_level=15)

    # Maximum compression
    gpio.convert('input.shp').write(
        'output.parquet', compression='ZSTD', compression_level=22
    )
    ```

!!! note "Which Python calls get level 9"

    The level-9 default belongs to the functions that write the output file
    themselves: every `gpio process` CLI command, and `ops.create_overviews` /
    `ops.create_overview_file` in the Python API.

    `ops.simplify`, `ops.polygonize`, `ops.contour`, `ops.aggregate_*` and
    `Table.simplify` hand you a table instead. Writing that table with
    `.write()` is an ordinary write at gpio's global default (ZSTD 15) — pass
    `compression_level=9` explicitly to match the process CLI's level from the
    Python API.

## Row Group Sizing

### What Row Groups Are

Parquet files are divided into row groups - independent chunks that can be read separately. Each row group has its own statistics (min/max values).

### Optimal Sizes

| Metric | Recommendation |
|--------|----------------|
| Compressed size | 50-100 MB per row group |
| Row count | 50,000-150,000 rows (depends on data) |

### Why Size Matters

**Too small:**
- Excessive metadata overhead
- More seeks for sequential reads
- Reduced compression efficiency

**Too large:**
- Must read entire row group even for small queries
- Higher memory usage during processing

### How to Control

```bash
# Target row group size in MB
gpio extract input.parquet output.parquet --row-group-size-mb 64MB

# Exact row count
gpio extract input.parquet output.parquet --row-group-size 100000
```

## Complete Optimization Pipeline

For a new file:

```bash
# 1. Convert with all optimizations (default)
gpio convert input.shp optimized.parquet

# 2. Verify optimizations
gpio check all optimized.parquet
```

For an existing GeoParquet file:

```bash
# 1. Check current state
gpio check all existing.parquet

# 2. Add bbox if missing
gpio add bbox existing.parquet with_bbox.parquet

# 3. Apply spatial ordering
gpio sort hilbert with_bbox.parquet optimized.parquet

# 4. Verify
gpio check all optimized.parquet
```

Or let gpio fix everything:

```bash
# Auto-fix all issues
gpio check all existing.parquet --fix --fix-output optimized.parquet
```

## Measuring Improvement

Compare query performance before and after optimization:

```bash
# Time a spatial query
time duckdb -c "
  SELECT COUNT(*)
  FROM 'unoptimized.parquet'
  WHERE ST_Intersects(geometry, ST_GeomFromText('POLYGON(...)'))
"

time duckdb -c "
  SELECT COUNT(*)
  FROM 'optimized.parquet'
  WHERE ST_Intersects(geometry, ST_GeomFromText('POLYGON(...)'))
"
```

Typical improvements: 5-20x faster for spatial queries.

## Large File Processing

gpio handles larger-than-memory files efficiently through its write strategy system.

### Default Behavior

The default `duckdb-kv` strategy uses constant memory regardless of file size:

```bash
# Process a 100GB file on a 8GB machine - just works
gpio extract huge_file.parquet filtered.parquet --bbox -122.5,37.5,-122.0,38.0
```

### Memory Configuration

gpio automatically detects available memory and uses 50% of it for streaming operations. This is container-aware, respecting Docker/Kubernetes memory limits.

For explicit control:

```bash
# Limit memory in constrained environments
gpio extract input.parquet output.parquet --write-memory 512MB
```

### Strategy Selection

| Use Case | Recommended Approach |
|----------|---------------------|
| Production workloads | Use default (`duckdb-kv`) |
| Container deployments | Default + explicit `--write-memory` if needed |
| Debugging output issues | Use `--write-strategy in-memory` to verify |
| DuckDB compatibility issues | Try `--write-strategy streaming` |

### Tips for Large Files

1. **Skip Hilbert for faster writes**: `--skip-hilbert` during initial conversion
2. **Use bbox column**: Enables row group pruning for spatial queries
3. **Appropriate row group sizes**: Target 50-100 MB compressed per row group
4. **Process locally when possible**: Download remote files for very large datasets (>50GB)

See the [Write Strategies Guide](../guide/write-strategies.md) for detailed information.

## See Also

- [What is GeoParquet?](geoparquet-overview.md) - Format overview
- [Sorting Data](../guide/sort.md) - Hilbert ordering details
- [Adding Spatial Indices](../guide/add.md) - Bbox and other indices
- [Checking Best Practices](../guide/check.md) - Validation and auto-fix
- [Write Strategies Guide](../guide/write-strategies.md) - Large file handling and memory configuration
