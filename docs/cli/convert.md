# convert Command

The convert command group handles format conversions. By default, converts to GeoParquet. Use subcommands for other conversions.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `geoparquet` | Convert vector formats to optimized GeoParquet (default) |
| `reproject` | Reproject a GeoParquet file to a different CRS |
| `geojson` | Convert GeoParquet to GeoJSON (streaming or file) |

## Quick Reference

```bash
gpio convert --help
gpio convert geoparquet --help
gpio convert reproject --help
gpio convert geojson --help
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
| `--lco KEY=VALUE` | none | GDAL layer creation option (repeatable) |
| `--verbose` | false | Show debug output |
| `--profile NAME` | none | AWS profile for S3 |

## Reproject

Reproject a GeoParquet file to a different CRS.

```bash
gpio convert reproject input.parquet output.parquet --dst-crs EPSG:32610
```

See `gpio convert reproject --help` for all options.
