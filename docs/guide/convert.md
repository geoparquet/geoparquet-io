# Converting to GeoParquet

The `convert` command transforms vector formats into optimized GeoParquet files with all best practices applied automatically.

## Basic Usage

```bash
gpio convert input.shp output.parquet
```

Automatically applies:
- ZSTD compression (level 15)
- 100,000 row groups
- Bbox column with proper metadata
- Hilbert spatial ordering
- GeoParquet 1.1.0 metadata

## Supported Input Formats

Auto-detected by file extension:

- **Shapefile** (.shp)
- **GeoJSON** (.geojson, .json)
- **GeoPackage** (.gpkg)
- **File Geodatabase** (.gdb)

Any format supported by DuckDB's spatial extension can be read.

## Options

### Skip Hilbert Ordering

For faster conversion when spatial ordering isn't critical:

```bash
gpio convert large.gpkg output.parquet --skip-hilbert
```

Trade-off: Faster conversion but less optimal for spatial queries.

### Custom Compression

Control compression type and level:

```bash
# GZIP compression
gpio convert input.shp output.parquet --compression GZIP --compression-level 6

# Uncompressed (not recommended)
gpio convert input.geojson output.parquet --compression UNCOMPRESSED
```

Available compression types:
- `ZSTD` (default, level 15) - Best compression + speed balance
- `GZIP` (level 1-9) - Wide compatibility
- `BROTLI` (level 1-11) - High compression
- `LZ4` - Fastest decompression
- `SNAPPY` - Fast compression
- `UNCOMPRESSED` - No compression

### Verbose Output

Track progress and see detailed information:

```bash
gpio convert input.gpkg output.parquet --verbose
```

Shows:
- Geometry column detection
- Dataset bounds calculation
- Bbox column creation
- Hilbert ordering progress
- File size and validation

## Examples

### Basic Shapefile Conversion

```bash
gpio convert buildings.shp buildings.parquet
```

Output:
```
Converting buildings.shp...
Done in 2.3s
Output: buildings.parquet (4.2 MB)
✓ Output passes GeoParquet validation
```

### Large Dataset Without Hilbert

```bash
gpio convert large_dataset.gpkg output.parquet --skip-hilbert
```

Skips Hilbert ordering for faster processing on large files.

### Custom Compression Settings

```bash
gpio convert roads.geojson roads.parquet \
  --compression ZSTD \
  --compression-level 22 \
  --verbose
```

Maximum ZSTD compression with progress tracking.

### Convert and Inspect

```bash
# Convert
gpio convert input.shp output.parquet

# Verify
gpio inspect output.parquet

# Validate
gpio check all output.parquet
```

## What Gets Applied

### 1. ZSTD Compression

Default level 15 provides excellent compression with fast decompression:

```bash
# Check compression after conversion
gpio inspect output.parquet
# Shows: Compression: ZSTD
```

### 2. 100k Row Groups

Optimized row group size balancing query performance and file size:

```bash
gpio check row-groups output.parquet
```

### 3. Bbox Column

Struct column with `{xmin, ymin, xmax, ymax}` for each feature:

```bash
gpio inspect output.parquet --head 1
# Shows bbox column in schema
```

Includes proper GeoParquet 1.1.0 covering metadata.

### 4. Hilbert Spatial Ordering

Sorts features by Hilbert curve for spatial locality:

```bash
gpio check spatial output.parquet
# Shows improved spatial ordering score
```

Skip with `--skip-hilbert` for faster conversion.

### 5. GeoParquet 1.1.0 Metadata

Proper GeoParquet metadata including:
- Version 1.1.0 specification
- CRS information
- Geometry encoding (WKB)
- Bbox covering metadata

```bash
gpio meta output.parquet --geoparquet
```

## When to Use Convert

### Use `convert` when:

- **Starting from scratch** - Converting shapefiles, GeoJSON, or GeoPackage to GeoParquet
- **Want all best practices** - Automatic optimization without manual steps
- **One-step workflow** - Convert + optimize in single command
- **Don't need customization** - Standard settings work for your use case

### Use individual commands when:

- **Already have GeoParquet** - Use `format`, `add`, `sort` for specific enhancements
- **Need fine control** - Custom row group sizes, specific bbox names, etc.
- **Incremental optimization** - Add features one at a time
- **Testing performance** - Compare different optimization strategies

## Comparison with Manual Workflow

### Using `convert` (one command):

```bash
gpio convert input.shp output.parquet
```

### Equivalent manual workflow (four commands):

```bash
# 1. Basic conversion with DuckDB
duckdb -c "COPY (SELECT * FROM ST_Read('input.shp')) TO 'temp.parquet'"

# 2. Add bbox column
gpio add bbox temp.parquet temp_bbox.parquet

# 3. Sort with Hilbert
gpio sort hilbert temp_bbox.parquet temp_sorted.parquet

# 4. Apply formatting
gpio format compression temp_sorted.parquet output.parquet \
  --compression ZSTD --compression-level 15
```

The `convert` command does all this in one step with optimized defaults.

## Output Validation

All converted files automatically pass GeoParquet validation:

```bash
gpio convert input.shp output.parquet
# Output shows: ✓ Output passes GeoParquet validation
```

Verify with:

```bash
gpio check all output.parquet
```

## Performance Tips

### For Large Files

Use `--skip-hilbert` to save time if spatial ordering isn't critical:

```bash
gpio convert large.gpkg output.parquet --skip-hilbert
```

Hilbert ordering requires reading the entire dataset and sorting, which can be slow for very large files.

### For Maximum Compression

Use highest ZSTD level:

```bash
gpio convert input.shp output.parquet \
  --compression ZSTD \
  --compression-level 22
```

Trade-off: Slower compression time, but smallest file size.

### For Fast Queries

Keep default Hilbert ordering:

```bash
gpio convert input.shp output.parquet
```

Spatial ordering improves query performance for spatial filters.

## See Also

- [CLI Reference: convert](../cli/convert.md)
- [add command](add.md) - Add indices to existing GeoParquet
- [sort command](sort.md) - Sort existing GeoParquet spatially
- [format command](format.md) - Apply formatting to existing files
- [check command](check.md) - Validate GeoParquet best practices
