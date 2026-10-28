# CLI Overview

The `gpio` command provides a comprehensive CLI for GeoParquet file operations.

## Command Structure

```
gpio [OPTIONS] COMMAND [ARGS]...
```

## Available Commands

### Core Commands

- **[convert](convert.md)** - Convert vector formats to optimized GeoParquet
- **[inspect](inspect.md)** - Examine file metadata and preview data
- **[extract](extract.md)** - Filter and subset GeoParquet files
- **[check](check.md)** - Validate files and fix issues automatically
- **[sort](sort.md)** - Spatially sort using Hilbert curves
- **[add](../guide/add.md)** - Enhance files with spatial indices
- **[partition](../guide/partition.md)** - Split files into optimized partitions
- **[benchmark](benchmark.md)** - Compare conversion performance

### publish

Upload files to cloud storage and generate STAC metadata for datasets. Use `gpio publish upload` for cloud storage uploads and `gpio publish stac` for STAC metadata generation.

## Global Options

```bash
--version        # Show version number
--timestamps     # Show timestamps in output messages
--s3-endpoint    # Custom S3-compatible endpoint (MinIO, R2, source.coop)
--s3-region      # S3 region for custom endpoints
--s3-no-ssl      # Disable SSL (use HTTP instead of HTTPS)
--aws-profile    # AWS profile name for S3 operations
--help           # Show help message
```

S3 options apply to all commands. See [Remote Files](../guide/remote-files.md) for details.

## Getting Help

Every command has detailed help:

```bash
# General help
gpio --help

# Command group help
gpio add --help
gpio partition --help
gpio check --help

# Specific command help
gpio add bbox --help
gpio partition h3 --help
gpio check spatial --help
gpio convert reproject --help
```

## Legacy Alias

The `gt` command is available as an alias for backwards compatibility:

```bash
gt inspect myfile.parquet  # Same as: gpio inspect myfile.parquet
```

## Common Patterns

### File Operations

Most commands follow this pattern:

```bash
gpio COMMAND INPUT OUTPUT [OPTIONS]
```

Examples:

```bash
gpio add bbox input.parquet output.parquet
gpio sort hilbert input.parquet sorted.parquet
```

### In-Place Operations

Some commands modify files in place:

```bash
gpio add bbox-metadata myfile.parquet
gpio check all myfile.parquet --fix
```

### Analysis Commands

Analysis commands take a single input:

```bash
gpio inspect myfile.parquet
gpio check all myfile.parquet
gpio check spec myfile.parquet
```

### Partition Commands

Partition commands output to directories:

```bash
gpio partition h3 input.parquet output_dir/
```

### Command Piping

Chain commands with Unix pipes for efficient multi-step workflows:

```bash
gpio add bbox input.parquet | gpio sort hilbert - output.parquet
```

Use `-` as input to read from stdin. See the [Piping Guide](../guide/piping.md) for details.

## Common Options

Many commands share these options:

--8<-- "_includes/common-cli-options.md"

```bash
--force        # Override warnings
```

## Exit Codes

- `0` - Success
- `1` - Error (with error message printed)
- `2` - Invalid usage (incorrect arguments)

`gpio check` refines `0` and `1` for its findings: a failed check exits `1`, a
warning exits `0` unless `--strict` is passed. See
[Exit Codes](../guide/check.md#exit-codes).

## Next Steps

Explore individual command references for detailed options and examples.
