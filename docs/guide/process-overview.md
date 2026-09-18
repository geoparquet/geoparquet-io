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

### Stating the bands instead

`gpio pmtiles pyramid` also takes `--bands`, which replaces the budget and the
probe with an explicit plan of `level:minzoom` pairs:

=== "CLI"

    <!-- doctest: skip="needs cells.parquet, which the harness does not seed" -->
    ```bash
    gpio pmtiles pyramid cells.parquet out.pmtiles --bands 5:0,8:6,10:9
    ```

=== "Python"

    <!-- doctest: skip="needs cells.parquet, which the harness does not seed" -->
    ```python
    from geoparquet_io.api import ops

    ops.create_pmtiles_pyramid('cells.parquet', 'out.pmtiles', bands='5:0,8:6,10:9')
    ```

Each entry says which zoom a level starts at; ends follow from the next entry
and the last band is open-ended, so the bands always cover every zoom. The
first must start at z0, zooms must strictly increase, a level may appear only
once, and grid levels must get finer as the zoom rises. A plan written from the
detailed end (`8:11,7:10,...,2:0`) is refused with the coarsest-first spelling
to use instead.

Because there is no probe left to feed, `--max-tile-kb` no longer applies, and
`--levels` or `--bytes-per-cell` alongside `--bands` is an error rather than a
silently dropped option.

Reach for this when several archives are browsed as one surface. Auto
selection judges each file alone, so two archives with different data land
their handovers on different zooms — and a viewer switching between them sees
cells change size at a zoom where nothing about the map should have changed.
Auto selection remains the better default for a single archive: it measures
the data instead of guessing, and `--bands` will happily produce a 20 MB tile
if that is what you asked for.

## One levelled file (`--overview-out`)

By default each level is written as its own sibling (`cells_r7.parquet`, …),
and a tiler then has to pick a zoom band per level. `--overview-out` also
assembles the ladder into **one** GeoParquet in tylertoo's
[overviews format](https://github.com/geoparquet/tylertoo/blob/main/context/OVERVIEWS_SPEC.md):
levels are additional *rows* tagged by an INT32 `level` column, coarse to
fine, each ending on a row-group boundary, with a `geo:overviews` footer
recording each level's row-group range and ground sample distance (GSD, in
metres). The sibling files are still written — under `--output-dir`, or beside
the input.

`tylertoo export-pmtiles` tiles that file in one pass, reading the levels as
written — no per-level tippecanoe run and no hand-pinned zoom bands. The
rollup lives in gpio because a coarse level of a count layer is a
**re-aggregation**, not a generalization: a tiler's coarse zoom would show
some of the fine cells and drop the rest, not their sum.

=== "CLI"

    <!-- doctest: setup="gpio process aggregate h3 buildings.parquet cells.parquet --resolution 9" -->
    ```bash
    gpio process overview cells.parquet --levels 8,7,6 --overview-out pyramid.parquet
    ```

    <!-- doctest: skip="tylertoo is not a gpio dependency" -->
    ```bash
    tylertoo export-pmtiles pyramid.parquet out.pmtiles
    ```

=== "Python"

    <!-- doctest: setup="gpio process aggregate h3 buildings.parquet cells.parquet --resolution 9" -->
    ```python
    from geoparquet_io.api import ops

    ops.create_overview_file('cells.parquet', 'pyramid.parquet', levels='8,7,6')
    ```

### Sizing the levels (`--cell-detail`, `--gsd`)

Each level's GSD is measured from the level itself: the median width of its
cells in metres (flat 111,320 m per degree, as the format prescribes), divided
by `--cell-detail` (default `4`, so a cell spans four GSD units at the zoom
that serves it). That is scheme-agnostic — H3, A5 and admin regions all have a
measurable cell width — and shrinks as levels refine. A larger `--cell-detail`
gives every level a smaller GSD, so tylertoo serves each level at a finer
zoom; a smaller one serves them coarser:

<!-- doctest: setup="gpio process aggregate h3 buildings.parquet cells.parquet --resolution 9" -->
```bash
gpio process overview cells.parquet --overview-out pyramid.parquet --levels 8,7 --cell-detail 8
```

For full control, `--gsd` takes absolute metres, coarse to fine, strictly
decreasing — one per built level **plus the base** — and overrides the
measurement:

<!-- doctest: setup="gpio process aggregate h3 buildings.parquet cells.parquet --resolution 9" -->
```bash
gpio process overview cells.parquet --overview-out pyramid.parquet \
    --levels 7,8 --gsd 4000,1500,600
```

The footer's `zoom` field is written only when the levels round to distinct
zooms; tylertoo derives zooms from `gsd` otherwise.

!!! note "The finest level is the aggregate, not the source features"

    The format's canonical level normally reproduces the source exactly. Here
    the finest level *is* an aggregate — the base cells — so that is what the
    file declares as canonical, in `duplicating` mode: every level is a
    complete rendering and a reader picks exactly one. Not yet written: the
    1.1 bbox covering the format requires (§4.4) and a spatial order within a
    level (§5.1); `tylertoo validate` reports the former.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--levels` | auto | Comma-separated levels to build (grid resolutions, or `country`) |
| `--max-tile-kb` | 500 | Tile-size budget driving auto selection |
| `--bands` (pyramid only) | auto | Explicit `level:minzoom` bands, e.g. `5:0,8:6,10:9`; skips the probe, so `--max-tile-kb` no longer applies and `--levels`/`--bytes-per-cell` are rejected |
| `--bytes-per-cell` | estimated | Override the compressed bytes-per-cell estimate |
| `--cell-column` | auto | Cell id column when detection fails |
| `--scheme` | auto | Bucketing scheme (`a5`/`h3`/`admin`) when inference is ambiguous, e.g. H3 ids stored as integers |
| `--output-dir` | input's directory | Where to write overview files |
| `--force` | off | Overwrite existing overview output files |
| `--overview-out` | off | Also assemble the ladder into one levelled GeoParquet at this path |
| `--cell-detail` | 4 | Cell width in GSD units at the level serving it (with `--overview-out`) |
| `--gsd` | measured | Explicit GSDs in metres, coarse to fine, one per built level plus the base (with `--overview-out`) |

Compression (`--compression`, `--compression-level`), `--geoparquet-version`, `--verbose`, and `--show-sql` behave as elsewhere in gpio.

## See Also

- [Aggregating Data](process-aggregate.md) — create the base aggregate
- [GeoJSON & PMTiles](geojson.md) — bake levels into a single PMTiles pyramid
