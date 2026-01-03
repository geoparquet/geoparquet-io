# check spec Command

For detailed usage and examples, see the [Check Spec Guide](../guide/validate.md).

## Quick Reference

```bash
gpio check spec --help
```

This will show all available options.

## Basic Usage

```bash
# Validate a GeoParquet file
gpio check spec myfile.parquet

# JSON output for machine processing
gpio check spec myfile.parquet --json

# Skip data validation for faster checks
gpio check spec myfile.parquet --skip-data-validation

# Validate against specific version
gpio check spec myfile.parquet --geoparquet-version 1.1
```

## Options

| Option | Description |
|--------|-------------|
| `--geoparquet-version` | Target version to validate against (auto-detected if not specified) |
| `--json` | Output results as JSON |
| `--skip-data-validation` | Skip geometry data validation checks |
| `--sample-size` | Number of rows to sample for data validation (default: 1000, 0 for all) |
| `--verbose` | Show detailed output |
| `--profile` | AWS profile for S3 files |

## Exit Codes

- `0` - All checks passed
- `1` - One or more checks failed
- `2` - Warnings only (no failures)
