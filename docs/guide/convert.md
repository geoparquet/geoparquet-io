# Converting Between Formats

The `convert` command transforms between GeoParquet and other vector formats with automatic format detection and optimization.

!!! note "CLI vs Python Behavior"
    The CLI `gpio convert` applies Hilbert sorting by default for optimal spatial queries.
    The Python `gpio.convert()` does NOT sort by default - chain `.sort_hilbert()` explicitly if needed.

## Basic Usage

=== "CLI"

    ```bash
    gpio convert input.shp output.parquet
    ```

    Automatically applies:

    - ZSTD compression (level 15)
    - 100,000 row groups
    - Bbox column with proper metadata
    - Hilbert spatial ordering
    - GeoParquet 1.1.0 metadata

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Convert with Hilbert sorting (recommended)
    gpio.convert('input.shp').sort_hilbert().write('output.parquet')

    # Or without sorting (faster but less optimal for spatial queries)
    gpio.convert('input.shp').write('output.parquet')
    ```

## Supported Formats

### Input Formats (to GeoParquet)

Auto-detected by file extension:

- **Shapefile** (.shp)
- **GeoJSON** (.geojson, .json)
- **GeoPackage** (.gpkg)
- **FlatGeobuf** (.fgb)
- **File Geodatabase** (.gdb)
- **CSV/TSV** (.csv, .tsv, .txt) - See [CSV/TSV Support](#csvtsv-support) below

Any format supported by DuckDB's spatial extension (50+ formats) can be read.

### Output Formats (from GeoParquet)

Auto-detected from output file extension:

- **GeoParquet** (.parquet) - Optimized cloud-native format
- **GeoPackage** (.gpkg) - SQLite-based OGC standard
- **FlatGeobuf** (.fgb) - Cloud-native streaming format
- **CSV** (.csv) - Tabular with WKT geometry
- **Shapefile** (.shp) - Legacy ESRI format
- **GeoJSON** (.geojson, .json) - Web-friendly JSON format

## Converting FROM GeoParquet

Convert GeoParquet to other formats with automatic format detection:

=== "CLI Auto-Detection"

    ```bash
    # Auto-detects format from extension
    gpio convert data.parquet output.gpkg      # → GeoPackage
    gpio convert data.parquet output.fgb       # → FlatGeobuf
    gpio convert data.parquet output.csv       # → CSV with WKT
    gpio convert data.parquet output.shp       # → Shapefile
    gpio convert data.parquet output.geojson   # → GeoJSON
    ```

=== "CLI Explicit Format"

    ```bash
    # Use explicit subcommand
    gpio convert geopackage data.parquet output.gpkg
    gpio convert flatgeobuf data.parquet output.fgb
    gpio convert csv data.parquet output.csv
    gpio convert shapefile data.parquet output.shp
    gpio convert geojson data.parquet output.geojson
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Load and convert
    table = gpio.read('data.parquet')

    # Auto-detects from extension
    table.write('output.gpkg')      # → GeoPackage
    table.write('output.fgb')       # → FlatGeobuf
    table.write('output.csv')       # → CSV with WKT
    table.write('output.shp')       # → Shapefile
    table.write('output.geojson')   # → GeoJSON

    # Or use explicit format
    table.write('output.dat', format='csv')
    ```

### Format-Specific Options

**GeoPackage:**

```bash
# Custom layer name
gpio convert data.parquet output.gpkg --layer-name buildings

# Overwrite existing
gpio convert data.parquet output.gpkg --overwrite
```

**Shapefile:**

```bash
# Custom encoding (default: UTF-8)
gpio convert data.parquet output.shp --encoding ISO-8859-1

# Overwrite existing
gpio convert data.parquet output.shp --overwrite
```

!!! warning "Shapefile Limitations"
    - Column names truncated to 10 characters
    - File size limit of 2GB
    - Limited data type support
    - Creates multiple files (.shp, .shx, .dbf, .prj)
    - Consider using GeoPackage or FlatGeobuf instead

**CSV:**

```bash
# Include WKT geometry (default)
gpio convert data.parquet output.csv

# Exclude geometry
gpio convert data.parquet output.csv --no-wkt

# Exclude bbox column
gpio convert data.parquet output.csv --no-bbox
```

**GeoJSON:**

```bash
# Custom precision (default: 7)
gpio convert data.parquet output.geojson --precision 5

# Include bbox for each feature
gpio convert data.parquet output.geojson --write-bbox

# Use specific field as feature ID
gpio convert data.parquet output.geojson --id-field osm_id

# Pretty-print JSON
gpio convert data.parquet output.geojson --pretty
```

### Cloud Output Support

All formats support cloud destinations via upload:

=== "CLI"

    ```bash
    # Write local then upload
    gpio convert data.parquet local.gpkg
    gpio upload local.gpkg s3://bucket/output.gpkg
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Write locally first
    table = gpio.read('data.parquet')
    table.write('local.gpkg')

    # Upload to cloud
    gpio.upload('local.gpkg', 's3://bucket/output.gpkg')
    ```

## Remote Files

Read from cloud storage or HTTPS:

```bash
# Convert remote file
gpio convert https://example.com/data.geojson local.parquet

# Convert from S3
gpio convert s3://bucket/input.parquet local-optimized.parquet

# Convert remote to local format
gpio convert s3://bucket/data.parquet local.gpkg
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

=== "CLI"

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

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.convert('buildings.shp').sort_hilbert().write('buildings.parquet')
    ```

### Large Dataset Without Hilbert

=== "CLI"

    ```bash
    gpio convert large_dataset.gpkg output.parquet --skip-hilbert
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Python doesn't sort by default, so just skip sort_hilbert()
    gpio.convert('large_dataset.gpkg').write('output.parquet')
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
