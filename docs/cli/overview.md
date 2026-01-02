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
- **[check](check.md)** - Validate files and fix issues automatically
- **[validate](validate.md)** - Validate against GeoParquet specification
- **[sort](sort.md)** - Spatially sort using Hilbert curves
- **[add](add.md)** - Enhance files with spatial indices
- **[partition](partition.md)** - Split files into optimized partitions
- **[upload](upload.md)** - Upload files to cloud storage (S3, GCS, Azure)
- **[stac](stac.md)** - Generate STAC metadata for datasets
- **[benchmark](benchmark.md)** - Compare conversion performance

## Global Options

```bash
--version    # Show version number
--help       # Show help message
```

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
gpio publish stac --help
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

## Next Steps

Explore individual command references for detailed options and examples.
