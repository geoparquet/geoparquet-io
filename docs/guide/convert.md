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
- **CSV/TSV** (.csv, .tsv, .txt) - See [CSV/TSV Support](#csvtsv-support) below

Any format supported by DuckDB's spatial extension can be read.

## Remote Files

Read from cloud storage or HTTPS:

```bash
# Convert remote file
gpio convert https://example.com/data.geojson local.parquet

# Convert from S3
gpio convert s3://bucket/input.parquet local-optimized.parquet
```

See [Remote Files Guide](remote-files.md) for authentication setup.

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

## CSV/TSV Support

Auto-detects geometry columns. WKT columns (wkt, geometry, geom) checked first, then lat/lon pairs (lat/lon, latitude/longitude).

```bash
# Auto-detect WKT or lat/lon
gpio convert points.csv points.parquet

# Explicit columns
gpio convert data.csv out.parquet --wkt-column geom_wkt
gpio convert data.csv out.parquet --lat-column lat --lon-column lng

# Custom delimiter
gpio convert data.txt out.parquet --delimiter "|"
```

### CRS and Validation

Default: WGS84 (EPSG:4326). Override with `--crs` for WKT data:

```bash
gpio convert projected.csv out.parquet --crs EPSG:3857
```

Validates lat/lon ranges (-90 to 90, -180 to 180). Warns on large coordinates suggesting projected CRS.

### Invalid Geometries

Fails on invalid WKT by default. Skip with `--skip-invalid`:

```bash
gpio convert messy.csv out.parquet --skip-invalid
```

Skips invalid rows, disables Hilbert ordering. Mixed geometry types supported.

### Delimiters

Auto-detects comma and tab. Override with `--delimiter` for semicolon, pipe, or any single character.

```bash
gpio convert data.csv out.parquet --delimiter ";"
```

## Performance

The convert command uses DuckDB's spatial extension - the fastest option for GeoParquet conversion, especially for large files.

**Benchmarks on representative datasets:**

| Dataset | Size | Features | DuckDB | PyOGRIO | ogr2ogr | Fiona |
|---------|------|----------|--------|---------|---------|-------|
| GAUL L2 Shapefile | 739 MB | 45k | **4.6s** | 5.9s | 4.1s | 187s |
| Argentina Roads | 1.1 GB | 3.5M | **30s** | 66s | 117s | 349s |

DuckDB also uses significantly less memory than alternatives (near-zero vs 600MB-2GB for GeoPandas).

To run your own benchmarks:

```bash
gpio benchmark input.geojson --iterations 3
```

See [`gpio benchmark`](../cli/benchmark.md) for details.

## See Also

- [CLI Reference: convert](../cli/convert.md)
- [benchmark command](../cli/benchmark.md) - Compare conversion performance
- [add command](add.md) - Add indices to existing GeoParquet
- [sort command](sort.md) - Sort existing GeoParquet spatially
- [check command](check.md) - Validate and fix GeoParquet files
