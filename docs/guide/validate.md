# Validating GeoParquet Files

The `check spec` command checks GeoParquet files against the official specifications, supporting GeoParquet 1.0, 1.1, 2.0, and Parquet native geospatial types.

## Basic Validation

```bash
gpio check spec myfile.parquet
```

The command auto-detects the file type and runs appropriate checks:

```text
GeoParquet Validation Report
================================

Detected: 1.0.0

Core Metadata:
  ✓ file includes a "geo" metadata key
  ✓ metadata is a valid JSON object
  ✓ metadata includes a "version" string: 1.0.0
  ✓ metadata includes a "primary_column" string: geometry
  ✓ metadata includes a "columns" object
  ✓ column metadata includes primary_column "geometry"

Column Validation:
  ✓ column "geometry" has valid encoding: WKB
  ✓ column "geometry" has valid geometry_types: ['Polygon', 'MultiPolygon']
  ...

Summary: 18 passed, 0 warnings, 0 failed
```

## Supported File Types

The validate command handles four types of files:

### GeoParquet 1.0/1.1

Standard GeoParquet files with `geo` metadata key containing version, primary_column, and column definitions.

```bash
gpio check spec geoparquet_v1.parquet
```

### GeoParquet 2.0

GeoParquet 2.0 files that use Parquet native GEOMETRY/GEOGRAPHY types alongside `geo` metadata.

```bash
gpio check spec geoparquet_v2.parquet
```

Additional checks verify:

- Native Parquet geo types are used
- CRS is inline in Parquet schema (if non-default)
- Metadata matches schema definitions

### Parquet-Geo-Only Files

Files with Parquet native geospatial types but no GeoParquet metadata. These are valid but may have limited tool compatibility.

```bash
gpio check spec parquet_geo_only.parquet
```

Output includes recommendations:

```text
Detected: parquet-geo-only

Parquet Geo (No Metadata):
  ✓ column "geometry" uses Parquet GEOMETRY logical type
  ✓ no CRS specified (defaults to OGC:CRS84)
  ⚠ CRS format may not be widely recognized by geospatial tools
      Use 'gpio convert --geoparquet-version 2.0' to add standardized metadata.
```

## Validation Categories

### Core Metadata Checks

Validates the `geo` metadata key structure:

- `geo` key exists in file metadata
- Metadata is valid JSON object
- `version` string present
- `primary_column` defined
- `columns` object present
- Primary column exists in columns

### Column Validation

For each geometry column:

- Valid `encoding` (WKB)
- Valid `geometry_types` list
- Valid `crs` (null or PROJJSON)
- Valid `orientation` if present
- Valid `edges` if present
- Valid `bbox` format if present
- Valid `epoch` if present

### Parquet Schema Checks

- Geometry columns not grouped
- Geometry uses BYTE_ARRAY type
- Geometry not repeated

### Data Validation

Optional checks that read actual geometry data:

- All geometries match declared encoding
- All geometry types in declared list
- All geometries within declared bbox

## Options

### Skip Data Validation

For faster validation, skip reading actual geometry data:

```bash
gpio check spec myfile.parquet --skip-data-validation
```

### Sample Size

Control how many rows are checked for data validation:

```bash
# Check first 500 rows (default: 1000)
gpio check spec myfile.parquet --sample-size 500

# Check all rows
gpio check spec myfile.parquet --sample-size 0
```

### Target Version

Validate against a specific version instead of auto-detecting:

```bash
gpio check spec myfile.parquet --geoparquet-version 1.1
```

### JSON Output

Get machine-readable results:

```bash
gpio check spec myfile.parquet --json
```

Output:

```json
{
  "file_path": "myfile.parquet",
  "detected_version": "1.0.0",
  "target_version": null,
  "is_valid": true,
  "summary": {
    "passed": 18,
    "warnings": 0,
    "failed": 0
  },
  "checks": [
    {
      "name": "geo_key_exists",
      "status": "passed",
      "message": "file includes a \"geo\" metadata key",
      "category": "core_metadata",
      "details": null
    },
    ...
  ]
}
```

## Exit Codes

The command returns different exit codes for scripting:

| Code | Meaning |
|------|---------|
| 0 | All checks passed |
| 1 | One or more checks failed |
| 2 | Warnings only (no failures) |

```bash
# Use in scripts
gpio check spec myfile.parquet && echo "Valid!"

# Check exit code
gpio check spec myfile.parquet
if [ $? -eq 0 ]; then
  echo "Valid GeoParquet file"
elif [ $? -eq 2 ]; then
  echo "Valid with warnings"
else
  echo "Invalid file"
fi
```

## GeoParquet 1.1 Checks

For files declaring version 1.1.0 or higher, additional checks run:

### Covering (Bbox Column)

If a `covering` is defined:

- Covering is a valid object
- Bbox paths include xmin/ymin/xmax/ymax
- Bbox column exists at schema root
- Bbox column is a struct with required fields
- Bbox fields are FLOAT or DOUBLE

### File Extension

```text
⚠ file extension is ".geoparquet" (recommend ".parquet")
```

GeoParquet 1.1 recommends `.parquet` extension.

## GeoParquet 2.0 Checks

For version 2.0 files, additional checks verify:

- Native Parquet GEOMETRY/GEOGRAPHY types are used
- Non-default CRS is inline in Parquet schema
- CRS in metadata matches schema
- Edges in metadata match algorithm in GEOGRAPHY type
- Bbox column not present (not recommended for 2.0)

## Parquet Native Geo Type Checks

For files using Parquet native geospatial types:

- GEOMETRY or GEOGRAPHY logical type present
- CRS format valid (srid:XXXX or inline PROJJSON)
- GEOGRAPHY edges algorithm valid
- GEOGRAPHY coordinates within bounds ([-180,180] x [-90,90])

## Remote Files

Validate files directly from S3, GCS, or HTTPS:

```bash
# S3 with AWS profile
gpio check spec s3://bucket/file.parquet --profile my-aws

# Public HTTPS
gpio check spec https://example.com/data.parquet
```

## Comparison with check Command

| Feature | `validate` | `check` |
|---------|-----------|---------|
| Purpose | Spec compliance | Best practices |
| Focus | Metadata validity | Performance optimization |
| Checks | Required fields, types | Spatial ordering, compression |
| Fix option | No | Yes (`--fix`) |

Use `validate` to verify spec compliance, use `check` to optimize for performance.

## Troubleshooting

### Common Validation Failures

#### "file must include a 'geo' metadata key"

The file doesn't have GeoParquet metadata. It may be:
- A plain Parquet file with geometry stored as WKB bytes
- A Parquet file with native geo types but no metadata (parquet-geo-only)

**Fix:** Use `gpio convert` to add GeoParquet metadata:

```bash
gpio convert input.parquet output.parquet --geoparquet-version 1.1
```

#### "coordinates outside valid range for CRS"

Geometry coordinates don't match the declared CRS bounds. Common causes:
- Geographic coordinates (lat/lon) in a file declared as projected CRS
- Projected coordinates in a file declared as geographic CRS
- Incorrect CRS assignment when the file was created

**Fix:** Use `gpio convert reproject` to transform coordinates:

```bash
# Transform coordinates to a new CRS
gpio convert reproject input.parquet output.parquet --dst-crs EPSG:4326

# Or inspect the file to understand the current state
gpio inspect input.parquet --verbose
```

#### "found undeclared geometry types"

The data contains geometry types not listed in `geometry_types` metadata. For example, the file declares `["Polygon"]` but contains `MultiPolygon` geometries.

**Fix:** Use `gpio convert` to regenerate metadata with correct types:

```bash
gpio convert input.parquet output.parquet
```

#### "CRS format may not be widely recognized"

For parquet-geo-only files, the CRS may be stored in a format that some tools don't understand.

**Fix:** Add GeoParquet 2.0 metadata for better compatibility:

```bash
gpio convert input.parquet output.parquet --geoparquet-version 2.0
```

#### "GeoParquet 2.0 requires native Parquet GEOMETRY/GEOGRAPHY type"

Validating against 2.0 but the file uses WKB bytes instead of native types.

**Fix:** Convert to GeoParquet 2.0 format:

```bash
gpio convert input.parquet output.parquet --geoparquet-version 2.0
```

### Warnings vs Failures

- **Failures (✗)**: The file doesn't comply with the specification
- **Warnings (⚠)**: The file is valid but may have compatibility issues
- **Skipped (○)**: Check wasn't applicable (e.g., no bbox to validate)

Files with only warnings are still valid GeoParquet files but may benefit from optimization.

### Version Mismatch

If you specify `--geoparquet-version` and the file is a different version, validation fails immediately:

```bash
gpio check spec v1_file.parquet --geoparquet-version 2.0
# Fails: "file is 1.0.0, not 2.0"
```

**Fix:** Either omit `--geoparquet-version` to validate against the detected version, or convert the file first:

```bash
# Validate as-is
gpio check spec v1_file.parquet

# Or convert then validate
gpio convert v1_file.parquet v2_file.parquet --geoparquet-version 2.0
gpio check spec v2_file.parquet --geoparquet-version 2.0
```

## See Also

- [CLI Reference: check spec](../cli/check.md)
- [check command](check.md) - Best practices validation
- [inspect command](inspect.md) - View file metadata
