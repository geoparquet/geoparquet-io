# Overview Levels

`gpio process overview` derives **coarser aggregate levels** from an existing `gpio process aggregate` output. Rolling up reads the small aggregate file — never the raw source — so building or re-tuning overviews takes seconds even when the original aggregation scanned billions of features.

**When to use it:** a fine aggregate (say A5 resolution 10) looks great zoomed in but cannot be rendered zoomed out — at low zooms every cell lands in a handful of tiles and the tiler has to drop features. Overviews give you a resolution ladder (`cells_r4.parquet`, `cells_r7.parquet`, …) so each zoom range can be served by a level that fits the tile budget. Feed the ladder to [`gpio pmtiles pyramid`](geojson.md#pmtiles-pyramids) to bake everything into one archive.

---

## Basic Usage

The input's scheme (`a5_cell` / `h3_cell` / `admin_code`) and base level are detected automatically:

=== "CLI"

    <!-- doctest: skip="needs cells.parquet, which the harness does not seed" -->
    ```bash
    # Auto-select levels against a 500 KB tile budget
    gpio process overview cells.parquet

    # Explicit grid resolutions
    gpio process overview cells.parquet --levels 4,7

    # Admin ladder: region-level input rolls up to country
    gpio process overview by_region.parquet --levels country

    # Tighter tile budget
    gpio process overview cells.parquet --max-tile-kb 300
    ```

=== "Python"

    <!-- doctest: skip="needs cells.parquet, which the harness does not seed" -->
    ```python
    from geoparquet_io.api import ops

    # File-based: writes one sibling per level, returns [(level, path), ...]
    ops.create_overviews('cells.parquet', levels=[4, 7])

    # In-memory: roll a single level with the Table API
    import geoparquet_io as gpio
    coarse = gpio.read('cells.parquet').overview(4)
    coarse.write('cells_r4.parquet')
    ```

Outputs are written next to the input (override with `--output-dir`):

| Input | Level | Output |
|-------|-------|--------|
| `cells.parquet` (a5/h3) | 7 | `cells_r7.parquet` |
| `by_region.parquet` (admin) | country | `by_region_country.parquet` |

## Rollup Semantics

Cells roll up by **true hierarchy** — `a5_cell_to_parent` / `h3_cell_to_parent` for grids, the ISO country prefix of `admin_code` (`US-CA` → `US`) for admin — and parent geometry is regenerated from the parent cell id (grids) or cached Overture country polygons (admin).

A coarser parent cell straddles the antimeridian more readily than its children, so grid rollups go through the same seam handling as `gpio process aggregate`: crossing cells are cut into `MultiPolygon`s at ±180 and pole cells are closed through a seam. See [Cells at the antimeridian and the poles](process-aggregate.md#cells-at-the-antimeridian-and-the-poles).

| Column | Rollup | Exactness |
|--------|--------|-----------|
| `count` | sum | exact |
| `sum_*` | sum | exact |
| `min_*` / `max_*` | min / max | exact |
| `count_*` breakdowns (incl. `count_other`) | sum | exact |
| `avg_*` | count-weighted mean over children with a value | exact **when the metric had no NULLs** |

!!! note "The `avg_*` caveat"
    A child cell's `avg_x` was computed over its non-NULL values, but the rollup weights it by the cell's full `count` (children whose `avg_x` is entirely NULL are excluded). When some features had NULL `x`, the weighting is approximate. With no NULLs, the count-weighted mean is exactly the mean over all original features.

Columns that don't match a rollup role are dropped with a warning. The `unassigned` bucket (features with no assignable cell/region) flows through with a NULL geometry.

## Auto Level Selection

Without `--levels`, gpio picks levels against the `--max-tile-kb` budget (default 500 KB, tippecanoe's cap):

1. **Bytes per cell** are estimated from the attribute schema and output geometry (override with `--bytes-per-cell`).
2. For each candidate level and zoom, parent-cell centroids are assigned to WebMercator tiles in DuckDB and the **worst tile's** cell count gives the estimated tile size. Because the probe uses your actual cells, regional datasets work without special casing.
3. Walking up from z0, the finest level that fits each zoom is chosen until the base level fits; consecutive picks merge into bands. The coarsest band always extends to z0 even if a single z0 tile overflows.

The selected bands drive which levels are materialized; the same selection powers `gpio pmtiles pyramid` zoom bands. If the base level already fits the budget at every zoom — for grid and admin inputs alike — no overview is built. (A geometry-less admin aggregate cannot be probed and falls back to building `country`.)

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--levels` | auto | Comma-separated levels to build (grid resolutions, or `country`) |
| `--max-tile-kb` | 500 | Tile-size budget driving auto selection |
| `--bytes-per-cell` | estimated | Override the compressed bytes-per-cell estimate |
| `--cell-column` | auto | Cell id column when detection fails |
| `--scheme` | auto | Bucketing scheme (`a5`/`h3`/`admin`) when inference is ambiguous, e.g. H3 ids stored as integers |
| `--output-dir` | input's directory | Where to write overview files |
| `--force` | off | Overwrite existing overview output files |

Compression (`--compression`, `--compression-level`), `--geoparquet-version`, `--verbose`, and `--show-sql` behave as elsewhere in gpio.

## See Also

- [Aggregating Data](process-aggregate.md) — create the base aggregate
- [GeoJSON & PMTiles](geojson.md) — bake levels into a single PMTiles pyramid
