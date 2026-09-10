# sort Command

For detailed usage and examples, see the [Sort User Guide](../guide/sort.md).

## Quick Reference

```bash
gpio sort --help
```

This will show all available subcommands and options.

## Subcommands

### hilbert

Sort by Hilbert space-filling curve for optimal spatial ordering:

```bash
gpio sort hilbert input.parquet output.parquet [OPTIONS]
```

Options:

| Option | Default | Description |
|--------|---------|-------------|
| `-g, --geometry-column` | auto-detect | Geometry column name |
| `--add-bbox` | - | Add bbox column if missing |
| `--compression` | ZSTD | Compression codec (ZSTD, SNAPPY, GZIP, etc.) |
| `--compression-level` | - | Compression level |
| `--row-group-size` | 49,152 | Row count per group, snapped up to a whole 2,048-row writer vector (10k-50k recommended for spatial pushdown; 50,000 snaps down to 49,152 to stay in that band) |
| `--row-group-size-mb` | - | Target group size in MB/GB |
| `--geoparquet-version` | 1.1 | Output version: `1.1`, `2.0`, or `parquet-geo-only` |
| `--overwrite` | - | Overwrite existing output file |
| `--verbose` | - | Verbose output |
| `--show-sql` | - | Show generated SQL |

### str

Pack rows with Sort-Tile-Recursive ordering for compact row-group bounding boxes:

```bash
gpio sort str input.parquet output.parquet --row-group-size 49152 [OPTIONS]
```

STR sorts geometry bounding-box centers into X strips, sorts each strip on Y,
and alternates the Y direction between strips.

`--row-group-size` does double duty: it is the writer's row-group target, and
it selects how many X strips STR builds, as
`ceil(sqrt(num_rows / row-group-size))`. That is a coarse control - nearby
values often produce an identical ordering. STR does not pack rows into
row-group-sized tiles, but both uses take the same value - snapped to a whole
2,048-row writer vector - so every strip is a whole number of row groups
whatever you pass. Left unset, both take the sort default of 49,152 rows.

The `str` subcommand supports the same geometry, bbox, compression, row-group,
GeoParquet version, overwrite, verbosity, and SQL-display options as `hilbert`.

### quadkey

Sort by quadkey for spatial locality:

```bash
gpio sort quadkey input.parquet output.parquet [OPTIONS]
```

### column

Sort by any column(s):

```bash
gpio sort column input.parquet output.parquet COLUMNS [OPTIONS]
```

Arguments:
- `COLUMNS` - Comma-separated column names to sort by

Options:
- `--descending` - Sort in descending order
- `--compression` - Compression codec
- `--geoparquet-version` - Output GeoParquet version
- `--overwrite` - Overwrite existing output
