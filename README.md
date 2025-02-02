# geoparquet-tools

A collection of tools for GeoParquet, using PyArrow and DuckDB.

## Installation

For now, just clone the repo and run `pip install -e .` from the root directory.

## Usage

The `geoparquet-tools` package provides a command-line interface through the `gt` command. Here are the available commands:

```bash
$ gt --help
Usage: gt [OPTIONS] COMMAND [ARGS]...

  GeoParquet Tools CLI for working with GeoParquet files.

Options:
  --help  Show this message and exit.

Commands:
  add        Commands for enhancing GeoParquet files in various ways.
  check      Commands for checking GeoParquet files for best practices.
  format     Commands for formatting GeoParquet files.
  partition  Commands for partitioning GeoParquet files.
  sort       Commands for sorting GeoParquet files.
```

### sort

The `sort` commands aims to provide different options to do spatial sorting of GeoParquet
files. Right now it just does Hilbert sorting, using DuckDB's `ST_Hilbert` function. It
preserves CRS information, so if you're working with projected data then it can be nicer
than using DuckDB directly. It outputs data according to recommended [GeoParquet
best practices](https://github.com/opengeospatial/geoparquet/pull/254/files) (except it doesn't yet right the bbox covering metadata).

```bash
$ gt sort hilbert --help
Usage: gt sort hilbert [OPTIONS] INPUT_PARQUET OUTPUT_PARQUET

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

The `add` commands aim to enhance GeoParquet files in various ways, typically adding
more columns. Right now it just has a `country-codes` command, which adds a column with
ISO codes for countries based on spatial intersection, following the [administrative division extension](https://github.com/fiboa/administrative-division-extension) in [fiboa](https://github.com/fiboa). (adding bbox column is also on the todo list).

```
$ gt add admin-divisions --help
Usage: gt add admin-divisions [OPTIONS] INPUT_PARQUET COUNTRIES_PARQUET
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

The `partition` commands aim to provide different options to partition GeoParquet
files. Right now it just has an `admin` command, which splits a GeoParquet file into
separate files by country code.

```
$ gt partition admin --help
Usage: gt partition admin [OPTIONS] INPUT_PARQUET OUTPUT_FOLDER

  Split a GeoParquet file into separate files by country code.

Options:
  --verbose    Print additional information.
  --overwrite  Overwrite existing country files.
  --help       Show this message and exit.
```

### check

The `check` commands aim to provide different options to check GeoParquet files for
adherence to [developing best practices](https://github.com/opengeospatial/geoparquet/pull/254/files). 

```
$ gt check --help
Usage: gt check [OPTIONS] COMMAND [ARGS]...

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


## TODO's

First todo is to convert all of these into proper github issues ;) 

 - get hive partitioning working for country stuff
 - better handling of country file - download it automatically, test other country files, make a bit more generic. Param to set the country iso column (get rid of the table that guesses it)
 - spatial partitioning by kd-tree and s2
 - add tests
 - add better docs
 - call for country splitting to be all in one.
 - option for further partitioning within admin boundaries (like s2 / kd-tree when the files are above a threshold)
 - admin level 2 splitting
 - ticket to admin extension in fiboa about putting source boundary file.
 - tool to print country code stats - number of records in each country
 - better decompisition of checks - core call on its own, layer on the warning info
 - better handling of really small files - if only one row group don't print red on the size, and don't fail on spatial order analysis.

