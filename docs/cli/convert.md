# convert Command

The convert command group handles format conversions. By default, converts to GeoParquet. Use subcommands for other conversions.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `geoparquet` | Convert vector formats to optimized GeoParquet (default) |
| `geopackage` | Convert GeoParquet to GeoPackage (.gpkg) |
| `flatgeobuf` | Convert GeoParquet to FlatGeobuf (.fgb) |
| `csv` | Convert GeoParquet to CSV with optional WKT geometry |
| `shapefile` | Convert GeoParquet to Shapefile (.shp) |
| `geojson` | Convert GeoParquet to GeoJSON (streaming or file) |
| `reproject` | Reproject a GeoParquet file to a different CRS |

## Quick Reference

```bash
gpio convert --help
gpio convert geoparquet --help
gpio convert geopackage --help
gpio convert flatgeobuf --help
gpio convert csv --help
gpio convert shapefile --help
gpio convert geojson --help
gpio convert reproject --help
```

## To GeoParquet (default)

For detailed usage, see the [Convert to GeoParquet Guide](../guide/convert.md).

```bash
# Convert Shapefile to GeoParquet
gpio convert input.shp output.parquet

# Explicit subcommand
gpio convert geoparquet input.gpkg output.parquet
```

## To GeoJSON

For detailed usage, see the [GeoJSON Conversion Guide](../guide/geojson.md).

```bash
# Stream to stdout (for tippecanoe)
gpio convert geojson data.parquet | tippecanoe -P -o tiles.pmtiles

# Write to file
gpio convert geojson data.parquet output.geojson
```

### geojson Options

| Option | Default | Description |
|--------|---------|-------------|
| `--no-rs` | false | Disable RFC 8142 record separators |
| `--precision N` | 7 | Coordinate decimal precision |
| `--write-bbox` | false | Include bbox property for features |
| `--id-field COL` | none | Use column as feature id |
| `--description TEXT` | none | Add description to FeatureCollection |
| `--feature-collection` | false | Output FeatureCollection instead of GeoJSONSeq |
| `--pretty` | false | Pretty-print with indentation |
| `--keep-crs` | false | Keep original CRS instead of reprojecting to WGS84 |
| `--no-repair-geometry` | false | Preserve invalid geometry instead of repairing it |
| `--verbose` | false | Show debug output |
| `--aws-profile NAME` | none | AWS profile for S3 |

## To GeoPackage

Convert GeoParquet to GeoPackage format. GeoPackage is an OGC standard based on SQLite with spatial indexing support.

```bash
# Convert to GeoPackage
gpio convert geopackage data.parquet output.gpkg

# With custom layer name
gpio convert geopackage data.parquet output.gpkg --layer-name buildings

# Auto-detection (no subcommand needed)
gpio convert data.parquet output.gpkg
```

### geopackage Options

| Option | Default | Description |
|--------|---------|-------------|
| `--layer-name TEXT` | features | Layer name in GeoPackage |
| `--overwrite` | false | Overwrite existing file |
| `--verbose` | false | Show debug output |
| `--aws-profile NAME` | none | AWS profile for S3 |

## To FlatGeobuf

Convert GeoParquet to FlatGeobuf format. FlatGeobuf is a cloud-native format with built-in spatial indexing designed for efficient streaming and HTTP range requests.

```bash
# Convert to FlatGeobuf
gpio convert flatgeobuf data.parquet output.fgb

# Auto-detection (no subcommand needed)
gpio convert data.parquet output.fgb
```

### flatgeobuf Options

| Option | Default | Description |
|--------|---------|-------------|
| `--verbose` | false | Show debug output |
| `--aws-profile NAME` | none | AWS profile for S3 |

## To CSV

Convert GeoParquet to CSV format with optional WKT geometry column. Complex types (STRUCT, LIST, MAP) are JSON-encoded.

```bash
# Convert to CSV with WKT geometry
gpio convert csv data.parquet output.csv

# Export only attributes (no geometry)
gpio convert csv data.parquet output.csv --no-wkt

# Exclude bbox column
gpio convert csv data.parquet output.csv --no-bbox

# Auto-detection (no subcommand needed)
gpio convert data.parquet output.csv
```

### csv Options

| Option | Default | Description |
|--------|---------|-------------|
| `--no-wkt` | false | Exclude WKT geometry column |
| `--no-bbox` | false | Exclude bbox column if present |
| `--verbose` | false | Show debug output |
| `--aws-profile NAME` | none | AWS profile for S3 |

## To Shapefile

Convert GeoParquet to Shapefile format.

!!! warning "Shapefile Limitations"
    - Column names truncated to 10 characters
    - File size limit of 2GB
    - Limited data type support
    - Creates multiple files (.shp, .shx, .dbf, .prj)

    Consider using GeoPackage or FlatGeobuf for modern workflows.

```bash
# Convert to Shapefile
gpio convert shapefile data.parquet output.shp

# With custom encoding
gpio convert shapefile data.parquet output.shp --encoding Latin1

# Auto-detection (no subcommand needed)
gpio convert data.parquet output.shp
```

### shapefile Options

| Option | Default | Description |
|--------|---------|-------------|
| `--encoding TEXT` | UTF-8 | Character encoding for attribute data |
| `--overwrite` | false | Overwrite existing file |
| `--verbose` | false | Show debug output |
| `--aws-profile NAME` | none | AWS profile for S3 |

## Reproject

Reproject a GeoParquet file to a different CRS.

```bash
gpio convert reproject input.parquet output.parquet --dst-crs EPSG:32610
```

### Unknown vs default CRS

The GeoParquet spec distinguishes two states for a geometry column's `crs`:

- **omitted** → defaults to OGC:CRS84 (lon/lat WGS84)
- **`null`** → CRS is *unknown*

A file whose CRS is unknown (`crs: null`, an empty `crs` value, or a `srid:0`
Parquet type) has no source CRS to reproject from, so reproject refuses it
unless you say what the coordinates are. If you know the real CRS, name it with
`--src-crs`:

```bash
gpio convert reproject unknown_crs.parquet out.parquet --src-crs EPSG:5070
```

If the coordinates are really lon/lat WGS84, use `--assume-crs84` to treat them as
OGC:CRS84 and rewrite the file so the `crs` key is omitted (no coordinates are
changed):

```bash
gpio convert reproject unknown_crs.parquet fixed.parquet --assume-crs84
```

!!! note "Reprojecting to the default CRS omits the `crs` key"
    Because EPSG:4326 and OGC:CRS84 are equivalent in GeoParquet (the spec fixes
    lon/lat axis order), reprojecting to the default — `--dst-crs EPSG:4326`, the
    default — writes the output with the `crs` key **omitted** rather than an
    explicit CRS object. This is the spec-correct way to signal the default.

See `gpio convert reproject --help` for all options.

## S3-Compatible Storage

```bash
gpio --s3-endpoint data.source.coop \
  convert geoparquet s3://bucket/geojson/data.geojson output.parquet
```
