# Selecting Fields

The `select` command creates a new GeoParquet file with only a subset of the original columns, reducing file size and improving query performance.

## Basic Usage

```bash
# Select specific fields
gpio select input.parquet output.parquet --fields "id,name,category"

# Exclude specific fields (keep all others)
gpio select input.parquet output.parquet --fields "temp_data,debug_info" --exclude
```

## When to Use

- **Reduce file size** by removing unnecessary columns before sharing or processing
- **Improve performance** by only including columns needed for analysis
- **Privacy/security** by removing sensitive fields before distribution
- **Simplify datasets** by keeping only relevant attributes

## Field Selection Modes

### Include Mode (Default)

Select only the specified fields:

```bash
gpio select buildings.parquet output.parquet --fields "id,height,geometry"
```

Output contains only `id`, `height`, and `geometry` columns.

### Exclude Mode

Keep all fields EXCEPT the specified ones:

```bash
gpio select buildings.parquet output.parquet --fields "internal_id,debug_flag" --exclude
```

Output contains all columns except `internal_id` and `debug_flag`.

## Geometry Column Handling

The geometry column is automatically included to maintain a valid GeoParquet file:

```bash
# These produce the same result - geometry is auto-included
gpio select data.parquet output.parquet --fields "name"
gpio select data.parquet output.parquet --fields "name,geometry"
```

To explicitly exclude the geometry column (creates non-GeoParquet output):

```bash
gpio select data.parquet output.parquet --fields "geometry" --exclude
# Warning: Output will not be a valid GeoParquet file
```

## Handling Special Field Names

### Fields with Spaces

Surround with double quotes:

```bash
gpio select data.parquet output.parquet --fields '"field with space",normal_field'
```

### Fields with Commas

Use quoted field names:

```bash
gpio select data.parquet output.parquet --fields '"address, city",zipcode'
```

### Fields with Double Quotes

Escape with backslash:

```bash
gpio select data.parquet output.parquet --fields '"field with \" quote"'
```

### Complex Example

```bash
gpio select data.parquet output.parquet \
  --fields 'regular_field,"field with space","field, with comma","field with \" quote"'
```

## Missing Fields

### Default Behavior (Error)

By default, specifying a non-existent field causes an error:

```bash
gpio select data.parquet output.parquet --fields "id,nonexistent"
# Error: Field 'nonexistent' not found in input file
```

### Ignore Missing Fields

Use `--ignore-missing-fields` to continue with a warning:

```bash
gpio select data.parquet output.parquet --fields "id,nonexistent" --ignore-missing-fields
# Warning: Field 'nonexistent' not found in input, skipping
# Creates output with just 'id' and 'geometry'
```

This is useful when:

- Processing multiple files with slightly different schemas
- Working with optional fields that may not exist in all files

## Remote Files

Works with cloud storage:

```bash
# S3 input and output
gpio select s3://bucket/input.parquet s3://bucket/output.parquet \
  --fields "id,name" --profile my-aws

# HTTP input to local
gpio select https://example.com/data.parquet local.parquet \
  --fields "name,geometry"
```

See [Remote Files Guide](remote-files.md) for authentication setup.

## Compression Options

--8<-- "_includes/compression-options.md"

```bash
# Custom compression
gpio select input.parquet output.parquet \
  --fields "id,name" \
  --compression GZIP \
  --compression-level 9

# Uncompressed for debugging
gpio select input.parquet output.parquet \
  --fields "id,name" \
  --compression UNCOMPRESSED
```

## Row Group Sizing

```bash
# Exact row count per group
gpio select input.parquet output.parquet \
  --fields "id,name" \
  --row-group-size 100000

# Target size in MB
gpio select input.parquet output.parquet \
  --fields "id,name" \
  --row-group-size-mb 256MB
```

## Examples

### Extract Minimal Dataset

```bash
# Keep only essential fields for visualization
gpio select buildings.parquet buildings_viz.parquet \
  --fields "name,height,geometry"
```

### Remove Debug/Temp Columns

```bash
# Remove internal fields before publishing
gpio select processed.parquet public.parquet \
  --fields "temp_id,processing_status,internal_notes" \
  --exclude
```

### Prepare for Spatial Join

```bash
# Keep only fields needed for join
gpio select poi.parquet poi_minimal.parquet \
  --fields "id,category"  # geometry auto-included
```

### Schema Normalization

When processing files with varying schemas:

```bash
# Extract common fields, ignoring missing ones
gpio select file1.parquet normalized1.parquet \
  --fields "id,name,type,date" \
  --ignore-missing-fields

gpio select file2.parquet normalized2.parquet \
  --fields "id,name,type,date" \
  --ignore-missing-fields
```

### View Available Fields First

```bash
# Inspect to see what fields exist
gpio inspect data.parquet

# Then select the ones you need
gpio select data.parquet output.parquet --fields "id,name,category"
```

## Verbose Output

Track progress with `--verbose`:

```bash
gpio select input.parquet output.parquet --fields "id,name" --verbose
```

Shows:

- Available fields in input
- Parsed field list
- Final output fields
- Processing progress
- Compression and metadata info

## See Also

- [CLI Reference: select](../cli/select.md)
- [inspect command](inspect.md) - View available fields
- [convert command](convert.md) - Convert and optimize files
