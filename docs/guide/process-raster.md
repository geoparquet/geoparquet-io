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

    # Skip repairing self-touching rings: much faster, still reports the count
    gpio process polygonize landcover.tif landcover.parquet --no-repair-geometry
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

    # Skip repairing self-touching rings (see below)
    table = ops.polygonize('landcover.tif', repair_geometry=False)
    ```

| Option | Default | Meaning |
|--------|---------|---------|
| `--band` | 1 | Raster band to trace |
| `--values` | all | Comma-separated class values to keep |
| `--value-column` | `value` | Name of the class attribute column |
| `--nodata` | file's tag | Nodata value to exclude. An explicit value *replaces* the tag: pixels equal to a wrong tag become data again |
| `--no-mask` | off | Ignore the raster's mask/nodata entirely |
| `--repair-geometry` / `--no-repair-geometry` | on | Repair self-touching rings with `ST_MakeValid` (see below) |

The traced polygons come out with counterclockwise exterior rings, as the
GeoParquet spec recommends, and round-trip exactly: rasterizing the output
reproduces the input array pixel for pixel.

### Self-touching rings

Where a class region pinches to a single shared pixel corner, the tracer closes
it as one ring that revisits that vertex. That is valid WKB but not OGC-valid,
and `rasterio.features.shapes` and `gdal_polygonize` emit the enclosed part as
an interior ring instead. On the WorldCover crops measured in #1301 it affected
1.7-2.9% of the output: 29, 1,354 and 5,776 polygons at 1024², 4096² and 8192².

gpio repairs them by default, like every other command that writes geometry.
The repair preserves what the polygons are *for*: row count, class column,
per-class counts, and area to within float noise (worst per-class relative
difference 1.5e-14 over 217k polygons), and every polygon stays a `Polygon` —
the self-touch becomes its own interior ring, which is exactly what GDAL emits.

The repair is the expensive half, because the self-touching polygons are the
large sprawling regions: on that 8192² output, 5,776 invalid polygons hold 7.5
million vertices, and repairing them takes about 36 s against roughly 5 s for
the polygonization itself. Finding them is cheap (under a second).

So if you only need class areas or a visualization, pass
`--no-repair-geometry` (`repair_geometry=False` from Python, shown in the
examples above): it still reports the invalid count and writes the rings as
traced. Use the repair — the default — for anything that overlays, joins or
dissolves the polygons, where an invalid ring is a correctness problem rather
than a label.

Contour output needs none of this: it measured 100% valid on every test DEM.

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
