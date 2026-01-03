# Viewing File Metadata

The `inspect --meta` command provides comprehensive metadata inspection for GeoParquet files. Use it to understand file structure, schema details, row group organization, and geospatial metadata.

## Basic Usage

```bash
gpio inspect --meta myfile.parquet
```

This displays all three metadata sections: Parquet file metadata, GeoParquet metadata, and Parquet geospatial metadata.

## When to Use meta vs inspect

| Use Case | Command |
|----------|---------|
| Quick file overview | `gpio inspect` |
| Preview actual data | `gpio inspect --head 10` |
| Check file is valid | `gpio inspect` |
| Deep dive into metadata | `gpio inspect --meta` |
| Debug row group issues | `gpio inspect --meta --row-groups 10` |
| Check GeoParquet compliance | `gpio inspect --meta --geoparquet` |
| Scripting/automation | `gpio inspect --meta --json` |

**Rule of thumb**: Start with `inspect` for a brief look. Add `--meta` when you need detailed structural information.

## Metadata Sections

### Parquet File Metadata

Shows the internal structure of the Parquet file:

```bash
gpio inspect --meta data.parquet --parquet
```

Output includes:
- **Schema**: Column names, types, and nullability
- **Row Groups**: How data is partitioned within the file
- **Compression**: Codec and sizes for each column
- **Statistics**: Min/max values, null counts per row group

This is useful for:
- Understanding data types and schema
- Diagnosing performance issues (row group sizes)
- Verifying compression is applied correctly

### GeoParquet Metadata

Shows GeoParquet-specific metadata from the 'geo' key:

```bash
gpio inspect --meta data.parquet --geoparquet
```

Output includes:
- **Primary Column**: Which column contains the main geometry
- **Geometry Types**: Point, LineString, Polygon, etc.
- **CRS**: Coordinate Reference System in PROJJSON format
- **Covering**: Bounding box metadata for spatial filtering
- **Version**: GeoParquet specification version

This is useful for:
- Verifying GeoParquet compliance
- Checking CRS information
- Confirming bbox covering metadata exists

### Parquet Geospatial Metadata

Shows geospatial metadata from the Parquet format specification (separate from GeoParquet):

```bash
gpio inspect --meta data.parquet --parquet-geo
```

Output includes:
- **GEOMETRY/GEOGRAPHY logical types**: Native Parquet geospatial types
- **Per-column bounding boxes**: Spatial extent of each row group
- **Encoding information**: How geometry is stored

This is useful for:
- Files using Parquet's native geospatial types
- Understanding row-group level spatial bounds
- Debugging spatial filtering behavior

## Row Group Analysis

By default, only the first row group is shown. To see more:

```bash
# Show first 5 row groups
gpio inspect --meta data.parquet --row-groups 5

# Show all row groups (for smaller files)
gpio inspect --meta data.parquet --row-groups 100
```

Row group analysis helps you:
- Check row group sizes are optimal (aim for 50-100MB)
- Verify data is evenly distributed
- Understand how spatial ordering affects row groups

## JSON Output for Scripting

Get machine-readable output:

```bash
# Full metadata as JSON
gpio inspect --meta data.parquet --json

# Specific section as JSON
gpio inspect --meta data.parquet --geoparquet --json

# Parse with jq
gpio inspect --meta data.parquet --json | jq '.geoparquet.primary_column'
```

## Remote Files

Works with all remote file types:

```bash
# HTTPS
gpio inspect --meta https://data.example.com/file.parquet

# S3
gpio inspect --meta s3://bucket/data.parquet --profile my-aws

# Google Cloud Storage
gpio inspect --meta gs://bucket/data.parquet
```

## Common Patterns

### Check GeoParquet Compliance

```bash
# View GeoParquet metadata
gpio inspect --meta data.parquet --geoparquet

# Look for:
# - version: Should be "1.0.0", "1.1.0", or similar
# - primary_column: Should be set
# - columns.<name>.covering: Should exist for bbox filtering
```

### Diagnose Query Performance

```bash
# Check row group sizes
gpio inspect --meta data.parquet --parquet --row-groups 10

# Look for:
# - Row groups of similar size (balanced distribution)
# - Row groups between 50-100MB compressed
# - Statistics present for filtering columns
```

### Verify Bbox Metadata

```bash
# Check bbox covering exists
gpio inspect --meta data.parquet --geoparquet --json | jq '.columns.geometry.covering'

# Should show something like:
# {
#   "bbox": {
#     "xmin": ["bbox", "xmin"],
#     "ymin": ["bbox", "ymin"],
#     "xmax": ["bbox", "xmax"],
#     "ymax": ["bbox", "ymax"]
#   }
# }
```

### Compare Files

```bash
# Compare schemas
diff <(gpio inspect --meta file1.parquet --parquet --json | jq '.schema') \
     <(gpio inspect --meta file2.parquet --parquet --json | jq '.schema')
```

## See Also

- [CLI Reference: inspect --meta](../cli/inspect.md) - Complete option reference
- [Inspecting Files](inspect.md) - Quick file overview and data preview
- [Checking Best Practices](check.md) - Validate GeoParquet files
