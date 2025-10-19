# geoparquet-io

Fast I/O and transformation tools for GeoParquet files using PyArrow and DuckDB.

## Installation

For now, just clone the repo and run `pip install -e .` from the root directory.

Coming soon: `pip install geoparquet-io`

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
  partition  Commands for partitioning GeoParquet files.
  sort       Commands for sorting GeoParquet files.
```

> **Note:** The legacy `gt` command is still available as an alias for backwards compatibility.

### sort

The `sort` commands aims to provide different options to do spatial sorting of GeoParquet
files. Right now it just does Hilbert sorting, using DuckDB's `ST_Hilbert` function. It
preserves CRS information, so if you're working with projected data then it can be nicer
than using DuckDB directly. It outputs data according to recommended [GeoParquet
best practices](https://github.com/opengeospatial/geoparquet/pull/254/files) (except it doesn't yet right the bbox covering metadata).

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

#### add admin-divisions

Add ISO codes for countries based on spatial intersection, following the [administrative division extension](https://github.com/fiboa/administrative-division-extension) in [fiboa](https://github.com/fiboa).

```
$ gpio add admin-divisions --help
Usage: gpio add admin-divisions [OPTIONS] INPUT_PARQUET COUNTRIES_PARQUET
                              OUTPUT_PARQUET

  Add country ISO codes to a GeoParquet file based on spatial intersection.

Options:
  --verbose  Print additional information.
  --help     Show this message and exit.
```

The `COUNTRIES_PARQUET` file that works will be available soon on [source cooperative](https://source.coop/cholmes/admin-boundaries). (Or you can easily make your own - it's just the Overture division, filtered by country, written out in GeoParquet). Future
versions will aim to make this more automatic, and also enable different country file
definnitions.

### partition

The `partition` commands provide different options to partition GeoParquet files into separate files based on column values.

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
# Preview partitions by first character of MGRS codes
gpio partition string input.parquet --column MGRS --chars 1 --preview

# Partition by full column values
gpio partition string input.parquet output/ --column category

# Partition by first 2 characters of MGRS codes
gpio partition string input.parquet output/ --column mgrs_code --chars 2

# Use Hive-style partitioning with prefix
gpio partition string input.parquet output/ --column region --chars 1 --hive
```

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
