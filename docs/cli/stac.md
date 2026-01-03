# stac Command

For detailed usage and examples, see the [STAC User Guide](../guide/stac.md).

## Quick Reference

```bash
gpio publish stac --help
```

This will show all available options and examples.

## Command Overview

Generate STAC Item or Collection from GeoParquet file(s).

**Single file** → STAC Item JSON
**Partitioned directory** → STAC Collection + Items

Automatically detects PMTiles overview files and includes them as assets.

## Examples

```bash
# Single file
gpio publish stac input.parquet output.json --bucket s3://my-bucket/roads/

# Partitioned dataset
gpio publish stac partitions/ stac-output/ --bucket s3://my-bucket/roads/

# With public URL mapping
gpio publish stac data.parquet output.json \
  --bucket s3://my-bucket/roads/ \
  --public-url https://data.example.com/roads/

# Overwrite existing STAC files
gpio publish stac partitions/ stac-output/ \
  --bucket s3://my-bucket/roads/ \
  --overwrite
```
