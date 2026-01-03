# Spatial Performance in GeoParquet

GeoParquet doesn't have a traditional spatial index like R-tree or quad-tree. Instead, it achieves spatial performance through **row group statistics** and **data organization**. Understanding these concepts helps you optimize files effectively.

## How GeoParquet Spatial Filtering Works

Unlike databases with spatial indices, GeoParquet relies on:

1. **Row Group Statistics**: Parquet stores min/max values per column per row group
2. **Bbox Column + Covering Metadata**: GeoParquet 1.1's `covering` metadata tells query engines which columns contain bounding box coordinates
3. **Spatial Sorting**: Data sorted spatially clusters nearby features in same row groups

### Query Flow

When you run a spatial query:

1. Query engine reads bbox column statistics (xmin_min, xmax_max, etc.) for each row group
2. Row groups that can't possibly intersect the query bbox are skipped entirely
3. Only potentially matching row groups are read and filtered

This is called "predicate pushdown" - filtering happens before reading data from disk.

### Why This Matters

A well-optimized GeoParquet file can skip 90%+ of data for localized spatial queries. But this only works when:

- A **bbox column** exists with proper metadata
- Data is **spatially sorted** so nearby features share row groups
- Row groups are appropriately sized (50-100MB compressed)

## Bbox Column: The Foundation (Not an Index)

The **bbox column** is NOT a spatial index - it's metadata that enables index-like behavior through Parquet's built-in statistics.

### What It Is

A struct column storing each feature's bounding box:

```
bbox: struct<xmin: double, ymin: double, xmax: double, ymax: double>
```

### What It Enables

With a bbox column and GeoParquet 1.1 `covering` metadata, query engines can:

- Read row group statistics to determine spatial extent of each chunk
- Skip row groups that don't intersect the query area
- Avoid expensive geometry decoding for non-matching features

### The Critical Requirement: Spatial Sorting

**A bbox column on unsorted data provides minimal performance benefit.**

Without spatial sorting:
```
Row Group 1: Features from New York, Tokyo, London, Sydney
Row Group 2: Features from Paris, Beijing, Cairo, Toronto
...
```
Every row group spans the entire world, so nothing can be skipped.

With spatial sorting (Hilbert curve):
```
Row Group 1: Features from New York, Boston, Philadelphia
Row Group 2: Features from Washington DC, Baltimore, Richmond
...
```
A query for Boston only reads Row Group 1.

### Adding Bbox

```bash
# CLI
gpio add bbox input.parquet output.parquet

# Python
gpio.read('input.parquet').add_bbox().write('output.parquet')
```

## Spatial Sorting Methods

Spatial sorting reorders rows so geographically nearby features are stored together.

### Hilbert Curve (Recommended)

The Hilbert curve is a space-filling curve that maps 2D space to 1D while preserving locality. Features that are close in 2D space tend to be close in the sorted order.

**Best for:** General-purpose spatial optimization, most query patterns

```bash
# CLI
gpio sort hilbert input.parquet output.parquet

# Python
gpio.read('input.parquet').sort_hilbert().write('output.parquet')
```

### Quadkey Sorting

Sorts by quadkey (tile) coordinates. Features in the same web map tile are grouped together.

**Best for:** Web mapping workflows, tile-based access patterns

```bash
# CLI
gpio sort quadkey input.parquet output.parquet

# Python
gpio.read('input.parquet').sort_quadkey().write('output.parquet')
```

### Comparison

| Method | Locality Preservation | Best Use Case |
|--------|----------------------|---------------|
| **Hilbert** | Excellent | General purpose, arbitrary spatial queries |
| **Quadkey** | Good | Web map tiles, zoom-level aligned queries |

**Recommendation:** Use Hilbert sorting for most datasets.

## Spatial Cell Columns (For Analysis, Not Query Performance)

These columns add cell IDs for spatial analysis, aggregation, and joins - **not for query performance**.

### H3 Hexagonal Cells

Uber's H3 indexing system divides the world into hexagonal cells at multiple resolutions.

**Use for:**
- Aggregating data to hexagonal grid (e.g., heatmaps)
- Joining datasets using H3 as a common key
- Uniform global coverage analysis

```bash
# CLI
gpio add h3 input.parquet output.parquet --resolution 9

# Python
gpio.read('input.parquet').add_h3(resolution=9).write('output.parquet')
```

### Quadkey Tiles

Quadkeys identify web map tiles at specific zoom levels.

**Use for:**
- Web mapping integration
- Tile-based workflows
- Zoom-level aligned analysis

```bash
# CLI
gpio add quadkey input.parquet output.parquet --resolution 12

# Python
gpio.read('input.parquet').add_quadkey(resolution=12).write('output.parquet')
```

### KD-tree Cells

Data-adaptive partitioning that divides space based on data distribution.

**Use for:**
- Highly clustered data (cities, coastlines)
- Creating balanced partition sizes
- When H3/quadkey creates uneven partitions

```bash
# CLI
gpio add kdtree input.parquet output.parquet

# Python
gpio.read('input.parquet').add_kdtree().write('output.parquet')
```

### Summary: Cell Columns

| Column | Cell Shape | Resolution Range | Best For |
|--------|------------|------------------|----------|
| **H3** | Hexagon | 0-15 | Aggregations, joins, uniform coverage |
| **Quadkey** | Square | 0-23 | Web mapping, tile workflows |
| **KD-tree** | Varies | 1-20 | Clustered data, balanced partitions |

**Important:** Adding these columns does NOT improve query performance. Use Hilbert sorting for that.

## Spatial Partitioning (Splitting into Files)

Partitioning splits data into multiple files based on spatial location.

### When to Partition vs Sort

| Approach | File Structure | Best For |
|----------|---------------|----------|
| **Sorting only** | Single file | Most queries, datasets under 10GB |
| **Partitioning** | Many files | Huge datasets (10GB+), known query patterns |

### Partitioning Trade-offs

**Advantages:**
- Query engines can skip entire files
- Parallel reads from multiple files
- Easier to update/delete specific regions

**Disadvantages:**
- More files to manage
- Overhead for very small partitions
- May need to read multiple files for cross-region queries

### Partitioning Commands

```bash
# Partition by H3 cells
gpio partition h3 input.parquet output_dir/ --resolution 6

# Partition by quadkey
gpio partition quadkey input.parquet output_dir/ --partition-resolution 4

# Partition by country
gpio partition admin input.parquet output_dir/ --column country_code
```

### Python API

```python
import geoparquet_io as gpio

# Partition by H3
gpio.read('input.parquet') \
    .add_h3(resolution=9) \
    .partition_by_h3('output/', resolution=6)

# Partition by quadkey
gpio.read('input.parquet') \
    .add_quadkey(resolution=12) \
    .partition_by_quadkey('output/', partition_resolution=4)
```

## Recommended Optimization Pipeline

### For Most Datasets

```bash
# CLI: Add bbox and sort
gpio add bbox input.parquet | gpio sort hilbert - optimized.parquet

# Python
gpio.read('input.parquet') \
    .add_bbox() \
    .sort_hilbert() \
    .write('optimized.parquet')
```

### For Very Large Datasets (10GB+)

```bash
# CLI: Add bbox, sort, then partition
gpio add bbox input.parquet | \
    gpio sort hilbert - | \
    gpio partition h3 --resolution 5 - output_dir/

# Python
gpio.read('input.parquet') \
    .add_bbox() \
    .add_h3(resolution=9) \
    .sort_hilbert() \
    .partition_by_h3('output/', resolution=5)
```

### For Analysis Workloads

```bash
# Add H3 for aggregation, quadkey for mapping
gpio add bbox input.parquet | \
    gpio add h3 --resolution 9 - | \
    gpio add quadkey --resolution 12 - | \
    gpio sort hilbert - enriched.parquet
```

## Quick Reference

| Component | Purpose | Required for Performance? |
|-----------|---------|---------------------------|
| **Bbox column** | Enable row group filtering | **Yes** (with sorting) |
| **Hilbert sorting** | Cluster nearby features | **Yes** (makes bbox useful) |
| **H3/Quadkey columns** | Analysis and joins | No (for analysis only) |
| **Partitioning** | Split into files | No (for very large datasets) |

## Verifying Optimization

Check if your file is optimized:

```bash
gpio check all myfile.parquet
```

This verifies:
- Bbox column exists with proper metadata
- Data is spatially ordered
- Compression and row group sizes are appropriate

## See Also

- [Best Practices](best-practices.md) - Optimization techniques
- [Sort Command](../cli/sort.md) - Hilbert and quadkey sorting
- [Add Command](../cli/add.md) - Adding bbox, H3, quadkey columns
- [Partition Command](../cli/partition.md) - Partitioning strategies
