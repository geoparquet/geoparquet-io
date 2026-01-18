# gpio-pmtiles

PMTiles generation plugin for [geoparquet-io](https://github.com/geoparquet/geoparquet-io).

## Installation

```bash
pip install gpio-pmtiles
```

**Note:** This plugin requires [tippecanoe](https://github.com/felt/tippecanoe) to be installed and available in your PATH.

### Installing tippecanoe

**macOS:**
```bash
brew install tippecanoe
```

**Ubuntu/Debian:**
```bash
sudo apt install tippecanoe
```

**From source:**
See the [tippecanoe installation guide](https://github.com/felt/tippecanoe#installation).

## Usage

After installation, the `gpio pmtiles` command group becomes available:

```bash
# Basic conversion
gpio pmtiles create buildings.parquet buildings.pmtiles

# With layer name
gpio pmtiles create roads.parquet roads.pmtiles --layer roads

# With zoom levels
gpio pmtiles create data.parquet tiles.pmtiles --max-zoom 14

# With filtering
gpio pmtiles create data.parquet tiles.pmtiles \
  --bbox "-122.5,37.5,-122.0,38.0" \
  --where "population > 10000"

# With column selection
gpio pmtiles create data.parquet tiles.pmtiles \
  --include-cols name,type,height
```

## How It Works

This plugin wraps GPIO's streaming GeoJSON output and pipes it to tippecanoe for efficient PMTiles generation. Under the hood, it's equivalent to:

```bash
gpio convert geojson input.parquet | tippecanoe -P -o output.pmtiles
```

But with integrated filtering, smart defaults, and better error handling.

## Options

- `--layer` / `-l`: Layer name in the PMTiles file
- `--min-zoom`: Minimum zoom level
- `--max-zoom`: Maximum zoom level
- `--bbox`: Bounding box filter (minx,miny,maxx,maxy)
- `--where`: SQL WHERE clause for row filtering
- `--include-cols`: Comma-separated list of columns to include
- `--precision`: Coordinate decimal precision (default: 6)
- `--verbose` / `-v`: Enable verbose output
- `--profile`: AWS profile for S3 files

## See Also

- [GPIO Documentation](https://geoparquet.io/)
- [Tippecanoe Documentation](https://github.com/felt/tippecanoe)
- [PMTiles Specification](https://github.com/protomaps/PMTiles)
