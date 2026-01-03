# Quick Start

Get started with geoparquet-io in 5 minutes.

## Installation

```bash
uv pip install geoparquet-io
```

See the [Installation Guide](installation.md) for more options.

## Basic Workflow

### 1. Convert to GeoParquet (Optional)

If you're starting with Shapefile, GeoJSON, GeoPackage, or CSV/TSV, convert to optimized GeoParquet:

```bash
gpio convert input.shp output.parquet
```

This automatically applies all best practices:
- ZSTD compression
- 100k row groups
- Bbox column with metadata
- Hilbert spatial ordering
- GeoParquet 1.1.0 metadata

Skip Hilbert ordering for faster conversion of large files:

```bash
gpio convert large.gpkg output.parquet --skip-hilbert
```

### 2. Inspect Your File

Take a look at what's in your GeoParquet file:

```bash
gpio inspect myfile.parquet
```

This shows you:

- File size and row count
- Coordinate reference system (CRS)
- Bounding box
- Column schema with types

Add `--head 10` to preview the first 10 rows, or `--stats` for column statistics.

### 3. Check Quality

Validate your file against GeoParquet best practices:

```bash
gpio check all myfile.parquet
```

This checks:

- Spatial ordering
- Compression settings
- Bbox metadata structure
- Row group optimization

### 4. Optimize Existing Files

If you already have GeoParquet files, enhance them with spatial indices.

Add a bounding box column for faster spatial queries:

```bash
gpio add bbox input.parquet output.parquet
```

Sort data using a Hilbert curve for better spatial locality:

```bash
gpio sort hilbert input.parquet sorted.parquet
```

### 5. Add Spatial Indices

Enhance your data with additional spatial indexing:

```bash
# Add H3 hexagonal cell IDs (resolution 9 ≈ 105m² cells)
gpio add h3 input.parquet output_h3.parquet --resolution 9

# Add KD-tree partition IDs (auto-selects optimal partition count)
gpio add kdtree input.parquet output_kdtree.parquet

# Add country codes via spatial join
gpio add admin-divisions buildings.parquet buildings_with_countries.parquet
```

### 6. Partition Large Datasets

Split large files into manageable partitions:

```bash
# Preview what partitions would be created
gpio partition admin buildings.parquet --preview

# Partition by country code
gpio partition admin buildings.parquet output_dir/

# Partition by H3 cells at resolution 7 (~5km² cells)
gpio partition h3 points.parquet output_dir/ --resolution 7

# Partition by KD-tree (auto-balanced spatial partitions)
gpio partition kdtree large_file.parquet output_dir/
```

### 7. Chain Commands with Pipes

gpio commands can be chained together using Unix pipes. This eliminates intermediate files and provides significant performance improvements:

```bash
# Extract, add bbox, and sort in one pipeline
gpio extract --limit 10000 input.parquet | gpio add bbox - | gpio sort hilbert - output.parquet

# Spatial filter, add indices, then partition
gpio extract --bbox "-122.5,37.5,-122.0,38.0" input.parquet | \
  gpio add quadkey - | \
  gpio partition string --column quadkey --chars 4 - output_dir/

# Add multiple indices in sequence
gpio add bbox input.parquet | gpio add h3 --resolution 9 - | gpio add quadkey - output.parquet
```

Use `-` as input to read from stdin. Output is auto-detected when piped. See the [Piping Guide](../guide/piping.md) for details.

## Common Patterns

### Convert and Validate

```bash
# 1. Convert from Shapefile/GeoJSON/GeoPackage/CSV
gpio convert input.shp output.parquet

# 2. Verify it meets best practices
gpio check all output.parquet

# 3. Inspect the results
gpio inspect output.parquet
```

### Convert, Fix, and Upload

```bash
# 1. Convert to GeoParquet
gpio convert input.shp data.parquet

# 2. Validate and fix issues
gpio check all data.parquet --fix --fix-output data_fixed.parquet

# 3. Upload to cloud storage
gpio publish upload data_fixed.parquet s3://bucket/data.parquet --profile prod
```

### Quality Check → Optimize → Validate

For existing GeoParquet files:

```bash
# 1. Check current state
gpio check all input.parquet

# 2. Optimize (using pipes - no intermediate files)
gpio add bbox input.parquet | gpio sort hilbert - optimized.parquet

# 3. Verify improvements
gpio check all optimized.parquet
```

### Inspect → Enhance → Partition

```bash
# 1. Understand your data
gpio inspect buildings.parquet --stats

# 2. Add country codes
gpio add admin-divisions buildings.parquet buildings_enhanced.parquet

# 3. Split by country
gpio partition admin buildings_enhanced.parquet by_country/
```

### Preview Before Processing

Always use `--preview` to understand what will happen:

```bash
# Preview partitioning strategy
gpio partition string input.parquet --column region --preview

# Preview with analysis
gpio partition h3 input.parquet --resolution 8 --preview

# If satisfied, run without --preview
gpio partition h3 input.parquet output/ --resolution 8
```

## Using the Python API

The Python API provides the best performance by keeping data in memory:

```python
import geoparquet_io as gpio

# Read and inspect a file
table = gpio.read('data.parquet')
table.info()  # Print summary

# Transform and write
gpio.read('input.parquet') \
    .add_bbox() \
    .sort_hilbert() \
    .write('optimized.parquet')

# Chain multiple operations
gpio.read('input.parquet') \
    .extract(limit=10000) \
    .add_bbox() \
    .add_quadkey(resolution=12) \
    .sort_hilbert() \
    .write('output.parquet')

# Convert from other formats
gpio.convert('data.gpkg') \
    .add_bbox() \
    .sort_hilbert() \
    .write('output.parquet')

# Upload to cloud storage
gpio.read('data.parquet') \
    .add_bbox() \
    .upload('s3://bucket/data.parquet')
```

The Python API is up to 5x faster than CLI operations because data stays in memory. See the [Python API documentation](../api/python-api.md) for the full reference.

## Getting Help

Every command has detailed help:

```bash
# General help
gpio --help

# Command group help
gpio add --help
gpio partition --help

# Specific command help
gpio add bbox --help
gpio partition h3 --help
```

## Next Steps

Now that you know the basics, explore:

- [Python API Reference](../api/python-api.md) - Full Python API documentation
- [User Guide](../guide/inspect.md) - Detailed documentation for all features
- [CLI Reference](../cli/overview.md) - Complete command reference
- [Examples](../examples/basic.md) - Real-world usage patterns
- [Spatial Performance](../concepts/spatial-indices.md) - Understanding bbox, sorting, and partitioning
