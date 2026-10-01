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
counted in a warning) rather than dropped.

!!! note "Memory in coverage mode"
    `--coverage` has to see every geometry at once to preserve shared edges,
    so the whole geometry column is held in memory. Plain mode processes the
    file chunk by chunk.
