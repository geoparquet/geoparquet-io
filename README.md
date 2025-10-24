# geoparquet-io

[![Tests](https://github.com/cholmes/geoparquet-io/actions/workflows/tests.yml/badge.svg)](https://github.com/cholmes/geoparquet-io/actions/workflows/tests.yml)
[![Python Version](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://github.com/cholmes/geoparquet-io)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/cholmes/geoparquet-io/blob/main/LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

Fast I/O and transformation tools for GeoParquet files using PyArrow and DuckDB.

## Features

- 🚀 **Fast**: Built on PyArrow and DuckDB for high-performance operations
- 📦 **Comprehensive**: Sort, partition, enhance, and validate GeoParquet files
- 🗺️ **Spatial Indexing**: Add bbox, H3 hexagonal cells, KD-tree partitions, and admin divisions
- 🎯 **Best Practices**: Automatic optimization following GeoParquet 1.1 spec
- 🔧 **Flexible**: CLI and Python API for any workflow
- ✅ **Tested**: Extensive test suite across Python 3.9-3.13 and all platforms

## Installation

### From PyPI (Coming Soon)

```bash
pip install geoparquet-io
```

### From Source

```bash
git clone https://github.com/cholmes/geoparquet-io.git
cd geoparquet-io
pip install -e .
```

### With uv (Recommended for Development)

```bash
git clone https://github.com/cholmes/geoparquet-io.git
cd geoparquet-io
uv sync --all-extras
```

For full development set up see the [getting started](CONTRIBUTING.md#getting-started) instructions.

### Requirements

- Python 3.9 or higher
- PyArrow 12.0.0+
- DuckDB 1.1.3+

## Quick Start

```bash
# Install
pip install geoparquet-io

# Inspect file structure and metadata
gpio inspect myfile.parquet

# Check file quality and best practices
gpio check all myfile.parquet

# Add bounding box column for faster queries
gpio add bbox input.parquet output.parquet

# Add H3 hexagonal cell IDs for spatial indexing
gpio add h3 input.parquet output.parquet --resolution 9

# Add KD-tree partition IDs for balanced spatial partitioning (auto mode)
gpio add kdtree input.parquet output.parquet

# Add country codes via spatial join
gpio add admin-divisions input.parquet output.parquet

# Sort using Hilbert curve for spatial locality
gpio sort hilbert input.parquet output_sorted.parquet

# Partition into separate files by country
gpio partition admin buildings.parquet output_dir/

# Get help
gpio --help
```

## Usage

The `geoparquet-io` package provides a command-line interface through the `gpio` command. Here are the available commands:

```
$ gpio --help
Usage: gpio [OPTIONS] COMMAND [ARGS]...

  Fast I/O and transformation tools for GeoParquet files.

Options:
  --help  Show this message and exit.

Commands:
  add        Commands for enhancing GeoParquet files in various ways.
  check      Commands for checking GeoParquet files for best practices.
  format     Commands for formatting GeoParquet files.
  inspect    Inspect a GeoParquet file and show metadata summary.
  partition  Commands for partitioning GeoParquet files.
  sort       Commands for sorting GeoParquet files.
```

> **Note:** The legacy `gt` command is still available as an alias for backwards compatibility.

### sort

The `sort` commands provide spatial sorting options for GeoParquet files. Currently supports Hilbert curve ordering using DuckDB's `ST_Hilbert` function. Preserves CRS information and outputs files following [GeoParquet 1.1 best practices](https://github.com/opengeospatial/geoparquet/pull/254/files) with proper bbox covering metadata.

```
$ gpio sort hilbert --help
Usage: gpio sort hilbert [OPTIONS] INPUT_PARQUET OUTPUT_PARQUET

  Reorder a GeoParquet file using Hilbert curve ordering.

  Takes an input GeoParquet file and creates a new file with rows ordered by
  their position along a Hilbert space-filling curve.

  By default, applies optimal formatting (ZSTD compression, optimized row
  groups, bbox metadata) while preserving the CRS.

Options:
  -g, --geometry-column TEXT  Name of the geometry column (default: geometry)
  -v, --verbose               Print verbose output
  --help                      Show this message and exit.
```

### add

The `add` commands aim to enhance GeoParquet files in various ways, typically adding more columns or metadata.

#### add bbox

Add a bounding box struct column to a GeoParquet file. This improves spatial query performance by providing precomputed bounding boxes for each feature, and automatically adds proper bbox covering metadata.

```
$ gpio add bbox --help
Usage: gpio add bbox [OPTIONS] INPUT_PARQUET OUTPUT_PARQUET

  Add a bbox struct column to a GeoParquet file.

  Creates a new column with bounding box coordinates (xmin, ymin, xmax, ymax)
  for each geometry feature. The bbox column improves spatial query
  performance and adds proper bbox covering metadata to the GeoParquet file.

Options:
  --bbox-name TEXT  Name for the bbox column (default: bbox)
  --verbose         Print additional information.
  --help            Show this message and exit.
```

Example usage:
```bash
# Add bbox column with default name 'bbox'
gpio add bbox input.parquet output.parquet

# Add bbox column with custom name
gpio add bbox input.parquet output.parquet --bbox-name bounds
```

#### add h3

Add H3 hexagonal cell IDs to a GeoParquet file based on geometry centroids. This provides hierarchical spatial indexing for aggregation and analysis, and automatically adds proper H3 covering metadata.

```
$ gpio add h3 --help
Usage: gpio add h3 [OPTIONS] INPUT_PARQUET OUTPUT_PARQUET

  Add an H3 cell ID column to a GeoParquet file.

  Computes H3 hexagonal cell IDs based on geometry centroids. H3 is a
  hierarchical hexagonal geospatial indexing system that provides consistent
  cell sizes and shapes across the globe.

  The cell ID is stored as a VARCHAR (string) for maximum portability across
  tools. Resolution determines cell size - higher values mean smaller cells
  with more precision.

Options:
  --h3-name TEXT        Name for the H3 column (default: h3_cell)
  --resolution INTEGER  H3 resolution level (0-15). Res 7: ~5km², Res 9:
                        ~105m², Res 11: ~2m², Res 13: ~0.04m². Default: 9
  --verbose             Print additional information.
  --help                Show this message and exit.
```

Example usage:
```bash
# Add H3 column with default resolution 9
gpio add h3 input.parquet output.parquet

# Add H3 column with custom resolution
gpio add h3 input.parquet output.parquet --resolution 13

# Add H3 column with custom name
gpio add h3 input.parquet output.parquet --h3-name h3_index
```

#### add kdtree

Add KD-tree partition IDs to a GeoParquet file. KD-tree creates balanced spatial partitions by recursively splitting on alternating X/Y dimensions at medians. The partition ID is stored as a binary string (e.g., "01011001").

By default, **auto-selects** partitions targeting ~120k rows each using **approximate mode** (O(n) with 100k sample) for fast computation. Use `--partitions N` for explicit control or `--exact` for deterministic results.

Example usage:
```bash
# Auto-select partitions targeting 120k rows each (default)
gpio add kdtree input.parquet output.parquet

# Use explicit partition count
gpio add kdtree input.parquet output.parquet --partitions 32

# Use exact computation (slower but deterministic)
gpio add kdtree input.parquet output.parquet --partitions 32 --exact

# Track progress with verbose mode
gpio add kdtree input.parquet output.parquet --verbose
```

**Note**: Partitions must be power of 2 (2, 4, 8, 16, ...).

#### add admin-divisions

Add ISO codes for countries based on spatial intersection, following the [administrative division extension](https://github.com/fiboa/administrative-division-extension) in [fiboa](https://github.com/fiboa).

By default, uses a curated countries dataset from [source.coop](https://data.source.coop/cholmes/admin-boundaries/countries.parquet), automatically filtered to your data's extent. You can also provide a custom countries file.

```
$ gpio add admin-divisions --help
Usage: gpio add admin-divisions [OPTIONS] INPUT_PARQUET OUTPUT_PARQUET

  Add country ISO codes to a GeoParquet file based on spatial intersection.

  If --countries-file is not provided, will use the default countries file
  from https://data.source.coop/cholmes/admin-boundaries/countries.parquet
  and filter to only the subset that overlaps with the input data (may take
  longer).

  Output is written as GeoParquet 1.1 with proper bbox covering metadata.

Options:
  --countries-file TEXT  Path or URL to countries parquet file. If not
                         provided, uses default from source.coop
  --add-bbox            Automatically add bbox column and metadata if missing.
  --compression [...]   Compression type for output file (default: ZSTD)
  --dry-run             Print SQL commands without executing
  --verbose             Print additional information.
  --help                Show this message and exit.
```

Example usage:
```bash
# Use default countries file (automatic)
gpio add admin-divisions buildings.parquet buildings_with_countries.parquet

# Use a custom countries file
gpio add admin-divisions buildings.parquet buildings_with_countries.parquet \
  --countries-file my_countries.parquet

# Preview the SQL without executing
gpio add admin-divisions buildings.parquet output.parquet --dry-run
```

### partition

The `partition` commands provide different options to partition GeoParquet files into separate files based on column values.

**Smart Analysis**: All partition commands automatically analyze your partitioning strategy before execution, calculating statistics and providing recommendations. Use `--preview` for dry-run analysis without creating files. Use `--force` to override warnings, or `--skip-analysis` for performance.

#### partition string

Partition a GeoParquet file by string column values. You can partition by full column values or by a prefix (first N characters). This is useful for splitting large datasets by categories, codes, regions, etc.

```
$ gpio partition string --help
Usage: gpio partition string [OPTIONS] INPUT_PARQUET [OUTPUT_FOLDER]

  Partition a GeoParquet file by string column values.

  Creates separate GeoParquet files based on distinct values in the specified
  column. When --chars is provided, partitions by the first N characters of
  the column values.

  Use --preview to see what partitions would be created without actually
  creating files.

Options:
  --column TEXT            Column name to partition by (required)  [required]
  --chars INTEGER          Number of characters to use as prefix for
                           partitioning
  --hive                   Use Hive-style partitioning in output folder
                           structure
  --overwrite              Overwrite existing partition files
  --preview                Preview partitions without creating files
  --preview-limit INTEGER  Number of partitions to show in preview (default:
                           15)
  --verbose                Print additional information
  --help                   Show this message and exit.
```

Example usage:
```bash
# Analyze and preview partition strategy (dry-run)
gpio partition string input.parquet --column MGRS --chars 1 --preview

# Partition by full column values
gpio partition string input.parquet output/ --column category

# Partition by first 2 characters of MGRS codes
gpio partition string input.parquet output/ --column mgrs_code --chars 2

# Use Hive-style partitioning with prefix
gpio partition string input.parquet output/ --column region --chars 1 --hive
```

#### partition h3

Partition a GeoParquet file by H3 hexagonal cells at a specified resolution. Automatically adds H3 column if it doesn't exist.

By default, the H3 column is **excluded** from the output files (since it's redundant with the partition path), except when using Hive-style partitioning where it's included. Use `--keep-h3-column` to explicitly keep the column in all cases.

```
$ gpio partition h3 --help
Usage: gpio partition h3 [OPTIONS] INPUT_PARQUET [OUTPUT_FOLDER]

  Partition a GeoParquet file by H3 cells at specified resolution.

  Creates separate GeoParquet files based on H3 cell prefixes at the
  specified resolution. If the H3 column doesn't exist, it will be
  automatically added before partitioning.

  By default, the H3 column is excluded from output files (since it's
  redundant with the partition path) unless using Hive-style partitioning.
  Use --keep-h3-column to explicitly keep the column in all cases.

  Use --preview to see what partitions would be created without actually
  creating files.

Options:
  --h3-name TEXT           Name of H3 column to partition by (default:
                           h3_cell)
  --resolution INTEGER     H3 resolution for partitioning (0-15, default: 9)
  --hive                   Use Hive-style partitioning in output folder
                           structure
  --overwrite              Overwrite existing partition files
  --preview                Preview partitions without creating files
  --preview-limit INTEGER  Number of partitions to show in preview (default:
                           15)
  --keep-h3-column         Keep the H3 column in output files (default:
                           excluded for non-Hive, included for Hive)
  --verbose                Print additional information
  --help                   Show this message and exit.
```

Example usage:
```bash
# Analyze and preview H3 partition strategy at resolution 7 (dry-run)
gpio partition h3 input.parquet --resolution 7 --preview

# Partition by H3 cells at resolution 9 (analyzes first, H3 column excluded from output)
gpio partition h3 input.parquet output/

# Force partitioning despite analysis warnings
gpio partition h3 input.parquet output/ --force

# Partition with H3 column kept in output files
gpio partition h3 input.parquet output/ --keep-h3-column

# Partition with custom resolution and Hive-style (H3 column included by default)
gpio partition h3 input.parquet output/ --resolution 8 --hive

# Use custom H3 column name
gpio partition h3 input.parquet output/ --h3-name my_h3
```

#### partition kdtree

Partition a GeoParquet file by KD-tree cells. Automatically adds KD-tree column if it doesn't exist. Creates balanced spatial partitions by recursively splitting on alternating X/Y dimensions at medians.

By default, **auto-selects** partitions targeting ~120k rows each using **approximate mode** (O(n) with 100k sample). The KD-tree column is **excluded** from output files (redundant with partition path), except when using Hive-style partitioning.

Example usage:
```bash
# Auto-partition targeting 120k rows each (default)
gpio partition kdtree input.parquet output/

# Preview with auto-selected partitions
gpio partition kdtree input.parquet --preview

# Use explicit partition count
gpio partition kdtree input.parquet output/ --partitions 32

# Use exact computation for deterministic results
gpio partition kdtree input.parquet output/ --partitions 32 --exact

# Use Hive-style partitioning with progress tracking
gpio partition kdtree input.parquet output/ --hive --verbose
```

**Note**: Partitions must be power of 2 (2, 4, 8, 16, ...).

#### partition admin

Split a GeoParquet file into separate files by country code (or any administrative column). By default, partitions by the `admin:country_code` column, but you can specify a different column.

```
$ gpio partition admin --help
Usage: gpio partition admin [OPTIONS] INPUT_PARQUET [OUTPUT_FOLDER]

  Split a GeoParquet file into separate files by country code.

  By default, partitions by the 'admin:country_code' column, but you can
  specify a different column using the --column option.

  Use --preview to see what partitions would be created without actually
  creating files.

Options:
  --column TEXT            Column name to partition by (default:
                           admin:country_code)
  --hive                   Use Hive-style partitioning in output folder
                           structure.
  --verbose                Print additional information.
  --overwrite              Overwrite existing country files.
  --preview                Preview partitions without creating files.
  --preview-limit INTEGER  Number of partitions to show in preview (default:
                           15)
  --help                   Show this message and exit.
```

Example usage:
```bash
# Preview country partitions
gpio partition admin input.parquet --preview

# Partition by country code (default column)
gpio partition admin input.parquet output/

# Partition by a custom admin column
gpio partition admin input.parquet output/ --column iso_code

# Use Hive-style partitioning
gpio partition admin input.parquet output/ --hive
```

### inspect

The `inspect` command provides quick, human-readable summaries of GeoParquet files for development and data workflows. It's useful for "gut checks" without launching external tools like DuckDB, pandas, or QGIS.

```bash
# Quick metadata inspection (instant)
gpio inspect data.parquet

# Preview first 10 rows
gpio inspect data.parquet --head 10

# Preview last 5 rows
gpio inspect data.parquet --tail 5

# Show column statistics (nulls, min/max, unique estimates)
gpio inspect data.parquet --stats

# JSON output for scripting
gpio inspect data.parquet --json

# Combined: preview with statistics
gpio inspect data.parquet --head 5 --stats
```

**Default output** (metadata only - instant):
- Filename and file size
- Row count and row group count
- CRS and bounding box
- Column schema with types (geometry column highlighted)

**Optional flags:**
- `--head N` - Show first N rows (default: 10 when used without number)
- `--tail N` - Show last N rows (default: 10 when used without number)
- `--stats` - Show column statistics (nulls, min/max, unique count estimates)
- `--json` - Output as JSON for scripting/piping

When using `--json`, the output includes preview data when `--head` or `--tail` is specified, and statistics when `--stats` is specified, making it ideal for automated validation pipelines.

### check

The `check` commands aim to provide different options to check GeoParquet files for
adherence to [developing best practices](https://github.com/opengeospatial/geoparquet/pull/254/files).

```
$ gpio check --help
Usage: gpio check [OPTIONS] COMMAND [ARGS]...

  Commands for checking GeoParquet files for best practices.

Options:
  --help  Show this message and exit.

Commands:
  all          Run all checks on a GeoParquet file.
  bbox         Check GeoParquet metadata version and bbox structure.
  compression  Check compression settings for geometry column.
  row-group    Check row group optimization.
  spatial      Check if a GeoParquet file is spatially ordered.
```

### format

The `format` command is still in development. It aims to enable formatting of GeoParquet
according to best practices, either all at once or by individual command, in sync with
the 'check'. So you could easily run check and then format. Right now it just has the
ability to add bbox metadata.
