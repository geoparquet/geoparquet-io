# Sorting Data

The `sort` command spatially reorders GeoParquet files for optimal performance.

## Hilbert Curve Ordering

```bash
gpio sort hilbert input.parquet output.parquet
```

Reorders rows using a [Hilbert space-filling curve](https://en.wikipedia.org/wiki/Hilbert_curve), which:

- Improves spatial locality
- Increases compression ratios
- Optimizes cloud-native access patterns
- Enhances query performance

## Options

```bash
# Specify geometry column
gpio sort hilbert input.parquet output.parquet -g geom

# Add bbox column if missing
gpio sort hilbert input.parquet output.parquet --add-bbox

# Custom compression
gpio sort hilbert input.parquet output.parquet --compression GZIP --compression-level 9

# Row group sizing
gpio sort hilbert input.parquet output.parquet --row-group-size-mb 256

# Verbose output
gpio sort hilbert input.parquet output.parquet --verbose
```

## Compression Options

Supported formats:

- `ZSTD` (default) - Best balance, level 1-22, default 15
- `GZIP` - Wide compatibility, level 1-9, default 6
- `BROTLI` - High compression, level 1-11, default 6
- `LZ4` - Fastest
- `SNAPPY` - Fast, good compression
- `UNCOMPRESSED` - No compression

## Row Group Sizing

Control row group sizes for optimal performance:

```bash
# Exact row count
gpio sort hilbert input.parquet output.parquet --row-group-size 100000

# Target size in MB/GB
gpio sort hilbert input.parquet output.parquet --row-group-size-mb 256MB
gpio sort hilbert input.parquet output.parquet --row-group-size-mb 1GB
```

## Output Format

The output file:

- Follows GeoParquet 1.1 spec
- Preserves CRS information
- Includes bbox covering metadata
- Uses optimal row group sizes

## See Also

- [CLI Reference: sort](../cli/sort.md)
- [check spatial](check.md#spatial-ordering)
- [add bbox](add.md#bounding-boxes)
