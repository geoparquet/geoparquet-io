# select Command

For detailed usage and examples, see the [Select User Guide](../guide/select.md).

## Quick Reference

```bash
gpio select INPUT_PARQUET OUTPUT_PARQUET --fields FIELDS [OPTIONS]
```

## Synopsis

```bash
gpio select input.parquet output.parquet --fields "id,name,category"
gpio select input.parquet output.parquet --fields "temp,debug" --exclude
gpio select input.parquet output.parquet --fields "id,maybe" --ignore-missing-fields
```

## Options

| Option | Description |
|--------|-------------|
| `--fields TEXT` | Required. Comma-separated list of fields to select (or exclude) |
| `--exclude` | Exclude specified fields instead of selecting them |
| `--ignore-missing-fields` | Warn instead of error for non-existent fields |
| `-v, --verbose` | Print verbose output |
| `--compression` | Compression type: ZSTD (default), GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED |
| `--compression-level` | Compression level (format-dependent) |
| `--row-group-size` | Exact number of rows per row group |
| `--row-group-size-mb` | Target row group size (e.g., '256MB', '1GB') |
| `--profile` | AWS profile name for S3 operations |

## Field Name Quoting

For field names containing special characters:

| Character | Syntax |
|-----------|--------|
| Space | `"field with space"` |
| Comma | `"field, with comma"` |
| Double quote | `"field with \" quote"` |

## Examples

```bash
# Select specific fields
gpio select data.parquet output.parquet --fields "id,name"

# Exclude fields
gpio select data.parquet output.parquet --fields "temp,debug" --exclude

# Handle missing fields gracefully
gpio select data.parquet output.parquet --fields "id,optional" --ignore-missing-fields

# With compression options
gpio select data.parquet output.parquet --fields "id,name" --compression GZIP

# Remote files
gpio select s3://bucket/in.parquet s3://bucket/out.parquet --fields "id" --profile aws
```

## Full Help

```bash
gpio select --help
```
