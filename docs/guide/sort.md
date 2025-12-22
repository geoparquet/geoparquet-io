# Sorting Data

The `sort` command reorders GeoParquet files for optimal performance and query efficiency.

## Sorting Methods

- **Hilbert curve** - Optimal spatial ordering using Hilbert space-filling curve
- **Column** - Sort by any column(s) for non-spatial ordering needs

## Hilbert Curve Ordering

```bash
gpio sort hilbert input.parquet output.parquet

# From HTTPS to S3
gpio sort hilbert https://example.com/data.parquet s3://bucket/sorted.parquet --profile prod
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

--8<-- "_includes/compression-options.md"

## Row Group Sizing

Control row group sizes for optimal performance:

```bash
# Exact row count
gpio sort hilbert input.parquet output.parquet --row-group-size 100000

# Target size in MB/GB
gpio sort hilbert input.parquet output.parquet --row-group-size-mb 256MB
gpio sort hilbert input.parquet output.parquet --row-group-size-mb 1GB
```

## Column Ordering

Sort by any column(s) for non-spatial ordering needs:

```bash
# Sort by a single column
gpio sort column input.parquet output.parquet name

# Sort by multiple columns (comma-separated)
gpio sort column input.parquet output.parquet country,city

# Sort in descending order
gpio sort column input.parquet output.parquet date --descending
```

Column sorting:

- Accepts one or more column names (comma-separated)
- Validates that columns exist before sorting
- Preserves all original columns and metadata
- Useful for time-series data or alphabetical ordering

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
