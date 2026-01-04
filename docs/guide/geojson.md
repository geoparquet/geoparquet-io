# GeoJSON Conversion

gpio can convert GeoParquet files to GeoJSON format, with a focus on streaming output for vector tile generation workflows.

## Overview

The `gpio convert geojson` command supports two modes:

1. **Streaming Mode** (default): Outputs newline-delimited GeoJSON (GeoJSONSeq) to stdout, designed for piping to tools like tippecanoe
2. **File Mode**: Writes a standard GeoJSON FeatureCollection to a file

## Streaming to tippecanoe for PMTiles

The primary use case is generating PMTiles or MBTiles from GeoParquet data by piping to [tippecanoe](https://github.com/felt/tippecanoe):

```bash
# Basic PMTiles generation
gpio convert geojson buildings.parquet | tippecanoe -P -o buildings.pmtiles

# With layer name
gpio convert geojson roads.parquet | tippecanoe -P -l roads -o roads.pmtiles

# Generate MBTiles instead
gpio convert geojson data.parquet | tippecanoe -P -o tiles.mbtiles
```

### Why Streaming Works with tippecanoe

The streaming output includes RFC 8142 record separators by default. These special characters (`\x1e`) enable tippecanoe's **parallel mode** (`-P` flag), which significantly speeds up tile generation by allowing tippecanoe to process features in parallel.

```bash
# The -P flag tells tippecanoe to read in parallel mode
gpio convert geojson data.parquet | tippecanoe -P -o output.pmtiles
```

If you're piping to a tool that doesn't support RFC 8142, disable the separators:

```bash
gpio convert geojson data.parquet --no-rs | some-other-tool
```

## Common Workflows

### Filter Before Converting

Use `gpio extract` to filter data before conversion to reduce output size:

```bash
# Filter by bounding box
gpio extract data.parquet --bbox "-122.5,37.5,-122,38" | \
  gpio convert geojson - | \
  tippecanoe -P -o sf.pmtiles

# Filter by column values
gpio extract data.parquet --where "population > 10000" | \
  gpio convert geojson - | \
  tippecanoe -P -o cities.pmtiles

# Limit rows for testing
gpio extract data.parquet --limit 1000 | \
  gpio convert geojson - | \
  tippecanoe -P -o sample.pmtiles
```

### Select Specific Columns

Reduce output size by selecting only needed columns:

```bash
gpio extract data.parquet --include-cols name,type,population | \
  gpio convert geojson - | \
  tippecanoe -P -o output.pmtiles
```

### Transform Before Converting

Apply spatial operations before conversion:

```bash
# Add bbox and sort, then convert
gpio add bbox data.parquet | \
  gpio sort hilbert - | \
  gpio convert geojson - | \
  tippecanoe -P -o output.pmtiles

# Reproject before converting
gpio convert reproject data.parquet - --dst-crs EPSG:4326 | \
  gpio convert geojson - | \
  tippecanoe -P -o output.pmtiles
```

## Writing to File

To write a standard GeoJSON FeatureCollection, specify an output file:

```bash
# Write to GeoJSON file
gpio convert geojson data.parquet output.geojson

# With options
gpio convert geojson data.parquet output.geojson --precision 5 --write-bbox
```

File output uses DuckDB's GDAL integration to produce properly formatted GeoJSON with RFC 7946 compliance.

## Options Reference

| Option | Default | Description |
|--------|---------|-------------|
| `--no-rs` | false | Disable RFC 8142 record separators (streaming only) |
| `--precision N` | 7 | Coordinate decimal precision (RFC 7946 recommends 7) |
| `--write-bbox` | false | Include bbox property for each feature |
| `--id-field COLUMN` | none | Use this column as the GeoJSON feature `id` |
| `--description TEXT` | none | Add a description to the FeatureCollection |
| `--feature-collection` | false | Output a FeatureCollection instead of GeoJSONSeq (streaming only) |
| `--pretty` | false | Pretty-print the JSON output with indentation |
| `--lco KEY=VALUE` | none | GDAL layer creation option (may be repeated) |
| `--verbose` | false | Show debug output |
| `--profile NAME` | none | AWS profile for S3 files |

### Coordinate Precision

The `--precision` option controls decimal places for coordinates. Lower precision reduces output size but decreases accuracy:

| Precision | Accuracy | Use Case |
|-----------|----------|----------|
| 7 (default) | ~1cm | High accuracy, RFC 7946 default |
| 6 | ~10cm | Most mapping applications |
| 5 | ~1m | City-level visualization |
| 4 | ~10m | Regional maps |

```bash
# Reduce precision for smaller output
gpio convert geojson data.parquet --precision 5 | tippecanoe -P -o output.pmtiles
```

### Feature ID Field

Use `--id-field` to specify which column should become the GeoJSON feature `id`:

```bash
gpio convert geojson buildings.parquet --id-field osm_id | tippecanoe -P -o output.pmtiles
```

This is useful for feature state in map rendering or for joining data.

### Bounding Box

Include per-feature bounding boxes with `--write-bbox`:

```bash
gpio convert geojson data.parquet output.geojson --write-bbox
```

### Description

Add a description to the FeatureCollection:

```bash
gpio convert geojson data.parquet output.geojson --description "My dataset"
```

### Pretty Print

For human-readable output with indentation:

```bash
gpio convert geojson data.parquet output.geojson --pretty
```

### FeatureCollection Mode (Streaming)

By default, streaming outputs newline-delimited GeoJSONSeq. To output a complete FeatureCollection instead:

```bash
gpio convert geojson data.parquet --feature-collection > output.geojson
```

### Advanced GDAL Options

For advanced use cases, pass GDAL layer creation options directly with `--lco`:

```bash
# Disable writing the layer name
gpio convert geojson data.parquet out.geojson --lco WRITE_NAME=NO

# Multiple options
gpio convert geojson data.parquet out.geojson --lco WRITE_NAME=NO --lco SIGNIFICANT_FIGURES=10
```

See the [GDAL GeoJSON driver documentation](https://gdal.org/drivers/vector/geojson.html#layer-creation-options) for all available options.

Note: Using `--lco` with the same option as a dedicated flag (e.g., `--lco COORDINATE_PRECISION=5` with `--precision 7`) will raise an error.

## Performance Tips

1. **Filter first**: Use `gpio extract` to reduce row count before conversion
2. **Select columns**: Only include columns needed for visualization
3. **Lower precision**: Use `--precision 5` or `--precision 6` for smaller output
4. **Pipeline processing**: Chain commands to avoid intermediate files

### Large File Example

```bash
# Efficient pipeline for large files
gpio extract large.parquet \
  --bbox "-122.5,37.5,-122,38" \
  --include-cols name,type,height | \
  gpio convert geojson - --precision 5 | \
  tippecanoe -P -z14 -o sf_buildings.pmtiles
```

## Remote Files

Read from S3, GCS, or Azure:

```bash
# From S3 with profile
gpio convert geojson s3://bucket/data.parquet --profile my-aws | tippecanoe -P -o output.pmtiles

# From public URL
gpio convert geojson https://example.com/data.parquet | tippecanoe -P -o output.pmtiles
```

## See Also

- [tippecanoe documentation](https://github.com/felt/tippecanoe)
- [PMTiles specification](https://github.com/protomaps/PMTiles)
- [Command Piping](piping.md) - More on gpio piping
- [Extract Guide](extract.md) - Filtering before conversion
- [Convert Guide](convert.md) - Other conversion options
