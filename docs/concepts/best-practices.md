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
| Maximum compression | ZSTD | 22 | Smaller files, slower write |
| Fast decompression | LZ4 | - | Analytics workloads |
| Wide compatibility | GZIP | 6 | Older tools |

gpio uses **ZSTD level 15** by default.

### Why ZSTD?

- 3-5x faster decompression than GZIP
- Similar or better compression ratio
- Widely supported in modern tools

### How to Apply

```bash
# Default ZSTD compression
gpio convert input.shp output.parquet

# Maximum compression
gpio convert input.shp output.parquet --compression ZSTD --compression-level 22

# Fast decompression
gpio convert input.shp output.parquet --compression LZ4
```

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

## See Also

- [What is GeoParquet?](geoparquet-overview.md) - Format overview
- [Sorting Data](../guide/sort.md) - Hilbert ordering details
- [Adding Spatial Indices](../guide/add.md) - Bbox and other indices
- [Checking Best Practices](../guide/check.md) - Validation and auto-fix
