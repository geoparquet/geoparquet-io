# benchmark Command

Benchmark GeoParquet conversion performance across different methods.

## Quick Reference

```bash
gpio benchmark INPUT_FILE [OPTIONS]
```

## Description

Tests available conversion methods (DuckDB, GeoPandas with Fiona/PyOGRIO, GDAL ogr2ogr) on an input geospatial file and reports time and memory usage.

## Available Converters

| Converter | Description | Install |
|-----------|-------------|---------|
| `duckdb` | DuckDB spatial extension | Always available |
| `geopandas_fiona` | GeoPandas with Fiona engine | `geopandas`, `fiona` |
| `geopandas_pyogrio` | GeoPandas with PyOGRIO engine | `geopandas`, `pyogrio` |
| `gdal_ogr2ogr` | GDAL ogr2ogr CLI | System GDAL installation |

Install all optional converters:

```bash
uv pip install geoparquet-io[benchmark]
# or: pip install geoparquet-io[benchmark]
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--iterations N` | 3 | Number of iterations per converter |
| `--converters LIST` | all available | Comma-separated list of converters to run |
| `--output-json PATH` | - | Save results to JSON file |
| `--keep-output DIR` | temp (cleaned up) | Directory to save converted files |
| `--warmup/--no-warmup` | enabled | Run warmup iteration before timing |
| `--format table\|json` | table | Output format |
| `--quiet` | - | Suppress progress output |

## Examples

### Basic Benchmark

```bash
gpio benchmark input.geojson
```

Runs all available converters with 3 iterations each.

### Specific Converters

```bash
gpio benchmark input.shp --converters duckdb,geopandas_pyogrio --iterations 5
```

### Save Results

```bash
gpio benchmark input.gpkg --output-json results.json --keep-output ./output
```

Saves JSON results and keeps the converted Parquet files.

### JSON Output

```bash
gpio benchmark input.geojson --format json
```

## Output

### Table Format (default)

```
======================================================================
BENCHMARK RESULTS
======================================================================

File: ARG.geojson
  Format: .geojson
  Features: 3,486,802
  Size: 1120.15 MB
  Geometry: LINESTRING

Converter                 Time (s)           Memory (MB)
-------------------------------------------------------------
DuckDB                    29.751 +/- 0.443   0.0 +/- 0.0
GeoPandas (PyOGRIO)       59.957 +/- 1.078   1196.7 +/- 0.0

Fastest: DuckDB (29.751s)
```

### JSON Format

Includes environment info, file metadata, raw results per iteration, and aggregated statistics.

## Interpreting Results

- **Time**: Mean elapsed seconds +/- standard deviation
- **Memory**: Peak memory usage in MB (Python tracemalloc for in-process, psutil for external)
- DuckDB shows 0 MB because it manages memory outside Python's allocator

## See Also

- [Convert Guide - Performance](../guide/convert.md#performance) - Summary benchmark results
