# gpio-pmtiles

PMTiles generation plugin for [geoparquet-io](https://github.com/geoparquet/geoparquet-io).

## Installation

```bash
pipx inject geoparquet-io gpio-pmtiles  # If using pipx for CLI
pip install gpio-pmtiles                # If using pip for library
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

### Command Line

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

# With CRS override (if metadata is incorrect)
gpio pmtiles create data.parquet tiles.pmtiles \
  --src-crs EPSG:3857
```

### Python API

The plugin also provides a Python function for programmatic use:

```python
from gpio_pmtiles import create_pmtiles_from_geoparquet

# Basic usage
create_pmtiles_from_geoparquet(
    input_path="buildings.parquet",
    output_path="buildings.pmtiles"
)

# With filtering and options
create_pmtiles_from_geoparquet(
    input_path="data.parquet",
    output_path="filtered.pmtiles",
    bbox="-122.5,37.5,-122.0,38.0",
    where="population > 10000",
    include_cols="name,population,area",
    layer="cities",
    max_zoom=14,
    verbose=True
)

# With CRS override for incorrect metadata
create_pmtiles_from_geoparquet(
    input_path="projected_data.parquet",
    output_path="tiles.pmtiles",
    src_crs="EPSG:3857",  # Reproject from Web Mercator to WGS84
    verbose=True
)
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
- `--max-zoom`: Maximum zoom level (auto-detected if not specified)
- `--bbox`: Bounding box filter (minx,miny,maxx,maxy)
- `--where`: SQL WHERE clause for row filtering
- `--include-cols`: Comma-separated list of columns to include
- `--precision`: Coordinate decimal precision (default: 6 for ~10cm accuracy)
- `--src-crs`: Override source CRS if metadata is incorrect (e.g., EPSG:3857, EPSG:32719)
- `--verbose` / `-v`: Enable verbose output
- `--profile`: AWS profile for S3 files

## Troubleshooting

### "Can't guess maxzoom (-zg) without at least two distinct feature locations"

This error occurs when tippecanoe sees geometries with invalid coordinates. Common cause: **incorrect CRS metadata**.

**Symptoms:**
- Error message about guessing maxzoom
- Your data has very large coordinate values (e.g., -8237642, -27826656)
- Metadata claims WGS84 but coordinates are clearly in meters

**Solution:**
Use `--src-crs` to specify the actual CRS:

```bash
# Check your data's coordinate range
gpio inspect input.parquet

# If coordinates are in millions, likely a projected CRS
gpio pmtiles create input.parquet output.pmtiles --src-crs EPSG:3857
```

Common projected CRS values:
- `EPSG:3857`: Web Mercator (Google Maps, OpenStreetMap)
- `EPSG:32619-32660`: UTM zones (Northern hemisphere)
- `EPSG:32701-32760`: UTM zones (Southern hemisphere)

### Checking Your Data's CRS

```bash
# View metadata to see claimed CRS
gpio inspect input.parquet --meta

# View actual coordinate values
gpio convert geojson input.parquet | head -5
```

If the bbox shows coordinates outside -180 to 180 (longitude) or -90 to 90 (latitude), your data is NOT in WGS84.

## See Also

- [GPIO Documentation](https://geoparquet.io/)
- [Tippecanoe Documentation](https://github.com/felt/tippecanoe)
- [PMTiles Specification](https://github.com/protomaps/PMTiles)
- [EPSG.io CRS Reference](https://epsg.io/)
