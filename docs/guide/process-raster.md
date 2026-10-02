# Raster to Vector

`gpio process polygonize` and `gpio process contour` turn rasters into
GeoParquet, backed by [contourrs](https://github.com/isaaccorley/contourrs)
— a Rust raster-tracing library whose Arrow output flows straight into
gpio's write pipeline:

- **polygonize** traces a categorical raster (land cover, segmentation
  masks, classification output) into one polygon feature per contiguous
  class region;
- **contour** extracts filled contour bands from a continuous raster
  (elevation, temperature), one polygon per band with `min`/`max` break
  attributes.

The implementation lives in
[`core/process/raster/`](https://github.com/geoparquet/geoparquet-io/tree/main/geoparquet_io/core/process/raster)
([`reader.py`](https://github.com/geoparquet/geoparquet-io/blob/main/geoparquet_io/core/process/raster/reader.py),
[`polygonize.py`](https://github.com/geoparquet/geoparquet-io/blob/main/geoparquet_io/core/process/raster/polygonize.py),
[`contour.py`](https://github.com/geoparquet/geoparquet-io/blob/main/geoparquet_io/core/process/raster/contour.py),
[`output.py`](https://github.com/geoparquet/geoparquet-io/blob/main/geoparquet_io/core/process/raster/output.py)),
with the lazy dependency shims in
[`core/optional_deps.py`](https://github.com/geoparquet/geoparquet-io/blob/main/geoparquet_io/core/optional_deps.py).

## Installation

Raster support is an optional extra (gpio's first and only raster surface):

<!-- doctest: skip="install command; never run by the docs harness" -->
```bash
pip install 'geoparquet-io[raster]'
# or, for a uv tool install:
uv tool install geoparquet-io --with contourrs --with rasterio
```

!!! warning "Python 3.12+"
    contourrs ships wheels for **CPython 3.12–3.14** only. On Python
    3.10/3.11 the extra installs rasterio but skips contourrs, and the
    commands explain the floor at runtime. contourrs is Apache-2.0;
    rasterio reads the GeoTIFF input and supplies the transform, nodata and
    CRS, which land in the output's `geo` metadata as PROJJSON.

## Polygonize

=== "CLI"

    <!-- doctest: skip="requires the optional raster extra (contourrs, Python 3.12+)" -->
    ```bash
    # Every class becomes polygons; nodata is honored automatically
    gpio process polygonize landcover.tif landcover.parquet

    # Keep only classes 1 and 3, name the attribute column
    gpio process polygonize mask.tif buildings.parquet \
        --values 1,3 --value-column class_id
    ```

=== "Python"

    <!-- doctest: skip="requires the optional raster extra (contourrs, Python 3.12+)" -->
    ```python
    import geoparquet_io as gpio
    from geoparquet_io.api import ops

    # Table constructor, chainable
    gpio.Table.polygonize('landcover.tif').write('landcover.parquet')

    # Pure function returning a PyArrow table
    table = ops.polygonize('mask.tif', values=[1.0], value_column='class_id')
    ```

| Option | Default | Meaning |
|--------|---------|---------|
| `--band` | 1 | Raster band to trace |
| `--values` | all | Comma-separated class values to keep |
| `--value-column` | `value` | Name of the class attribute column |
| `--nodata` | file's tag | Nodata value to exclude. An explicit value *replaces* the tag: pixels equal to a wrong tag become data again |
| `--no-mask` | off | Ignore the raster's mask/nodata entirely |

The traced polygons come out with counterclockwise exterior rings, as the
GeoParquet spec recommends, and round-trip exactly: rasterizing the output
reproduces the input array pixel for pixel.

## Contour

=== "CLI"

    <!-- doctest: skip="requires the optional raster extra (contourrs, Python 3.12+)" -->
    ```bash
    # Bands every 100 units, spanning the raster's value range
    gpio process contour dem.tif contours.parquet --interval 100

    # Explicit break values
    gpio process contour dem.tif contours.parquet --levels 0,250,500,1000
    ```

=== "Python"

    <!-- doctest: skip="requires the optional raster extra (contourrs, Python 3.12+)" -->
    ```python
    import geoparquet_io as gpio
    from geoparquet_io.api import ops

    gpio.Table.contour('dem.tif', interval=100).write('contours.parquet')

    table = ops.contour('dem.tif', levels=[0.0, 250.0, 500.0, 1000.0])
    ```

| Option | Default | Meaning |
|--------|---------|---------|
| `--interval N` | — | Breaks every N units from `--base` (spans the band's range) |
| `--levels a,b,c` | — | Explicit break values (mutually exclusive with `--interval`) |
| `--base` | 0.0 | Offset for `--interval` |
| `--band` | 1 | Raster band to contour |
| `--nodata` | file's tag | Nodata value to exclude. An explicit value *replaces* the tag |
| `--no-mask` | off | Ignore the raster's mask/nodata entirely |
| `--min-column` / `--max-column` | `min` / `max` | Names for each band's break attributes |

Each output feature is one band polygon attributed with its `[min, max)`
break values; bands never overlap and together tile the valid data area.
Contours are interpolated between grid *nodes* (marching squares), so a
band footprint reaches the outermost pixel centers, not the outer pixel
edges.

## Output metadata

Both commands write through gpio's normal pipeline: the raster's CRS
becomes the `geo` block's PROJJSON `crs`, `geometry_types` and `bbox` are
computed from the actual output, and row groups, compression and
GeoParquet version behave exactly as in every other gpio write (all the
usual `--compression`, `--row-group-size`, `--version` options apply).
