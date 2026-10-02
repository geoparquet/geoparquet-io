# Simplifying Geometries

`gpio process simplify` reduces geometry vertex counts with
[coarsen](https://github.com/isaaccorley/coarsen) — a multithreaded Rust
implementation of GEOS's simplification that produces **byte-identical output
to GEOS 3.13.1** at a 9–14× speedup. Plain mode is a drop-in
`shapely.simplify`; `--coverage` mode is `shapely.coverage_simplify`: shared
edges between adjacent polygons stay shared, so a simplified admin or parcel
coverage stays gap-free and overlap-free.

The implementation lives in
[`core/process/simplify.py`](https://github.com/geoparquet/geoparquet-io/blob/main/geoparquet_io/core/process/simplify.py).

## Installation

Simplification is an optional extra (the core install stays lean):

<!-- doctest: skip="install command; never run by the docs harness" -->
```bash
pip install 'geoparquet-io[simplify]'
# or, for a uv tool install:
uv tool install geoparquet-io --with coarsen
```

coarsen ships prebuilt wheels for Python 3.10+ on Linux, macOS and Windows.
It is LGPL-2.1 licensed and used as an unmodified dependency; without it,
`gpio process simplify` explains exactly this install step and everything
else in gpio works as before.

## Basic Usage

=== "CLI"

    <!-- doctest: skip="requires the optional simplify extra (coarsen)" -->
    ```bash
    # Tolerance is in the data's CRS units (here: 10 meters)
    gpio process simplify parcels.parquet simplified.parquet --tolerance 10

    # A polygonal coverage: keep shared edges shared, no gaps introduced
    gpio process simplify admin.parquet simplified.parquet \
        --tolerance 0.001 --coverage
    ```

=== "Python"

    <!-- doctest: skip="requires the optional simplify extra (coarsen)" -->
    ```python
    import geoparquet_io as gpio
    from geoparquet_io.api import ops

    # Table API, chainable
    gpio.read('parcels.parquet').simplify(10).write('simplified.parquet')

    # Pure function over a PyArrow table
    table = ops.simplify(my_arrow_table, tolerance=10, coverage=True)
    ```

## Options

| Option | Default | Meaning |
|--------|---------|---------|
| `--tolerance` | required | Douglas-Peucker tolerance, in CRS units |
| `--coverage` | off | Coverage mode: shared edges preserved across the whole file |
| `--preserve-topology` | on | Keep each geometry valid (plain mode only) |
| `--simplify-boundary` | on | Also simplify the coverage's outer boundary (`--coverage` only) |
| `--threads N` | auto | coarsen worker threads |
| `--geometry-column` | auto | Defaults to the file's primary geometry column |
| `--drop-empty` | off | Drop rows whose geometry is empty after simplification |
| `--simplify-crs` | — | Project to this CRS for the simplification (tolerance in its units), then back; `auto-utm` picks the UTM zone from the data |

Native **GeoParquet 2.0 inputs** are supported: the geometry arrives as a
geoarrow WKB extension column, is simplified on its raw WKB, and the write
preserves the 2.0 declaration (native geometry types and stats) — via the
in-memory path, since the streaming writer covers plain-WKB 1.x outputs.

## What happens to the metadata

Simplification changes the geometry, so the stats that describe it are
recomputed rather than carried through stale:

- the `geo` block's per-column `geometry_types` and `bbox` are recomputed
  from the simplified data by the write funnel;
- a declared **bbox covering column** is recomputed from the simplified
  geometries (float32 covering values are rounded *outward*, so the stored
  box always contains its geometry);
- CRS, edges and everything else carry through unchanged.

Geometries that collapse to empty at the given tolerance are kept (and
counted in a warning) by default; `--drop-empty` removes those rows instead
— empty geometries otherwise become NaN covering values and zero-area
features downstream. Null geometries are kept either way.

## Scaling to planet-sized files

Plain mode **streams**: batches are read, simplified and written one row
group at a time, so peak memory is bounded by a single row group no matter
how large the file — a 40 GB, 100M+-row GeoParquet simplifies within a
laptop's RAM. (The streaming writer covers plain-WKB 1.x outputs; a 2.0
native output or a `--row-group-size-mb` byte target takes the in-memory
path.)

`--coverage` is different in kind: preserving shared edges requires seeing
**every geometry in one pass** — splitting a coverage into chunks measurably
introduces slivers of gap and overlap along the seam, small enough to slip
past downstream repair thresholds. So coverage mode holds the whole
geometry column in memory, and for global-scale coverages the right recipe
is to partition on a boundary where parcels do not share edges and
coverage-simplify each part:

<!-- doctest: skip="requires the optional simplify extra (coarsen)" -->
```bash
gpio partition admin world_parcels.parquet parts/   # or h3 / kdtree / string
for f in parts/*.parquet; do
    gpio process simplify "$f" "simplified/$(basename "$f")"         --tolerance 5 --coverage
done
```

!!! note "Tolerance units"
    `--tolerance` is in the data's CRS units, and global data in EPSG:4326
    means *degrees*. For a metre tolerance pass `--simplify-crs`: the
    geometries are projected to that CRS (or to the data's own UTM zone
    with `auto-utm`), simplified there, and projected back — one pass, only
    the geometry round-trips, attributes and the file's CRS stay untouched.

<!-- doctest: skip="requires the optional simplify extra (coarsen)" -->
```bash
# 5 meter tolerance on lon/lat data, per-UTM-zone partitioned
gpio process simplify zone48.parquet out.parquet --tolerance 5 --simplify-crs auto-utm
```
