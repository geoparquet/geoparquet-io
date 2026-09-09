# Aggregating Data

The `process aggregate` commands reduce large GeoParquet datasets into compact summary files — one row per spatial bucket — suitable for low-zoom visualization, dashboards, and reporting.

**When to use it:** You have millions of features (fields, buildings, parcels) and want to display them at country or grid-cell scale without transmitting all the raw geometry. Aggregation gives you a small file where every cell or region carries a `count`, optional numeric rollups (`sum`, `avg`, `min`, `max`), and optional per-category breakdowns.

Three bucketing schemes are available:

| Command | Buckets | Bucket id column |
|---------|---------|-----------------|
| `gpio process aggregate a5` | Equal-area A5 grid cells | `a5_cell` (UBIGINT) |
| `gpio process aggregate h3` | H3 hexagonal grid cells | `h3_cell` (string) |
| `gpio process aggregate admin` | Administrative regions (Overture Maps) | `admin_code` + `admin_name` |

Every output row carries the bucket id regardless of the `--out-geometry` setting, so a `--out-geometry none` table can be re-joined to geometry later.

---

## Choosing What to Aggregate

Each output cell or region can carry three kinds of statistics. Pick the ones that answer the question you want the map to show — you can combine all three in one run.

| Column | Question it answers | Reach for it when |
|--------|---------------------|-------------------|
| `count` *(always present)* | *How much data is here?* | Show density — where features are concentrated vs. sparse. |
| `--metric func:column` | *What's the total or typical value here?* | Shade cells by a numeric attribute (area, population, price). |
| `--breakdown column` | *What's the mix of categories here?* | Toggle/filter classes (crop types) or animate over time (year). |

### Which metric function to use

`--metric` takes comma-separated `func:column` pairs. A bare column name is shorthand for `sum`.

| Function | Per-cell result | Use when the quantity is… | Example |
|----------|-----------------|---------------------------|---------|
| `sum` | Total across features in the cell | **additive** — area, population, revenue | `sum:area_ha` → total hectares of fields in the cell |
| `avg` | Mean across features | a **typical value/intensity**, independent of how many features | `avg:yield` → average yield per field |
| `min` / `max` | Smallest / largest value | an **extreme or range** — oldest, newest, cheapest, peak | `max:price`, `min:built_year` |

Rules of thumb:

- **`sum` scales with `count`** — dense cells naturally get large sums. Use it for "total stuff here," and keep `count` alongside to interpret it.
- **`avg` does *not* scale with count** — it isolates size/intensity, so a cell with 5 large fields is comparable to one with 5 000 small fields. Best for "typical value" maps.
- **`min`/`max`** surface outliers; pair `min:year` with `max:year` to show the span of values in a cell.
- **Don't request `count` via `--metric`** — every output row already includes a
  `count` column automatically (`COUNT(*)` per bucket). For per-category counts see
  [Which breakdown to use](#which-breakdown-to-use).
- The column must already exist and be numeric. To aggregate a **geometry-derived** value like area, compute it first with `gpio add geometry-metrics` (writes `metrics:area` in m²), then `--metric "sum:metrics:area"`.
- **NoData sentinels skew every metric.** Real-world numeric columns frequently
  encode "no value" as a sentinel like `-999` or `-9999` rather than SQL `NULL`
  (e.g. GlobalBuildingAtlas heights use `-999`). Fed to `--metric` directly, the
  sentinel drags `avg`/`sum` toward garbage and `min` reports the sentinel
  itself. Pass `--metric-nodata "-999"` (comma-separate multiple sentinels) to
  map them to `NULL` before aggregation — `sum`/`avg`/`min`/`max` then ignore
  those values while `count` still counts every feature. `nan` is accepted for
  NaN-encoded nodata, and sentinels are compared at the metric column's own
  precision, so the classic float32 nodata `-3.4028235e+38` matches `REAL`
  (float32) columns correctly. Sentinels apply only to numeric metric columns.

=== "CLI"

    <!-- doctest: skip="aggregates 'height', a column the sample data does not have" -->
    ```bash
    gpio process aggregate a5 buildings.parquet cells.parquet --auto \
        --metric "avg:height,max:height" --metric-nodata "-999"
    ```

=== "Python"

    <!-- doctest: skip="aggregates 'height', a column the sample data does not have" -->
    ```python
    import geoparquet_io as gpio

    result = gpio.read('buildings.parquet').aggregate_a5(
        resolution=10,
        metric="avg:height,max:height",
        metric_nodata="-999",
    )
    result.write('cells.parquet')
    ```

### Which breakdown to use

`--breakdown` pivots one categorical (or time-bucket) column into a `count_<value>` column per value — the building block for **filterable** or **animated** maps:

- A class column (`crop_type`) → `count_wheat`, `count_corn`, … that a frontend can toggle on/off.
- A year column (`harvest_year`) → `count_2021`, `count_2022`, … driving a time slider.

Values beyond `--breakdown-limit` (default 20, ordered by frequency) collapse into `count_other`, so a high-cardinality column won't explode the schema.

---

## A5 Grid Aggregation

Aggregate into A5 equal-area hexagonal grid cells. A5 cells at lower resolutions cover large areas (useful for global overviews) while higher resolutions approach parcel level.

### Resolution

Specify a resolution explicitly or let gpio choose one automatically:

=== "CLI"

    ```bash
    # Explicit resolution
    gpio process aggregate a5 fields.parquet cells.parquet --resolution 8

    # Auto-select resolution (~10 000 features per cell, up to 500 000 cells)
    gpio process aggregate a5 fields.parquet cells.parquet --auto

    # Auto with custom target
    gpio process aggregate a5 fields.parquet cells.parquet \
        --auto --target-per-cell 5000 --max-cells 200000
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Explicit resolution
    result = gpio.read('fields.parquet').aggregate_a5(resolution=8)

    # Write the result
    result.write('cells.parquet')
    ```

### Metric Rollups

Aggregate numeric columns with `--metric` (`func:column`, comma-separated; bare column = `sum`). See [Which metric function to use](#which-metric-function-to-use) for when to pick `sum` vs. `avg` vs. `min`/`max`.

=== "CLI"

    <!-- doctest: skip="aggregates 'area_ha', a column the sample data does not have" -->
    ```bash
    # Sum one column
    gpio process aggregate a5 fields.parquet cells.parquet \
        --resolution 8 --metric "sum:area_ha"

    # Multiple rollups (bare column = sum)
    gpio process aggregate a5 fields.parquet cells.parquet \
        --resolution 8 --metric "area_ha,avg:yield,max:price"
    ```

=== "Python"

    <!-- doctest: skip="aggregates 'area_ha', a column the sample data does not have" -->
    ```python
    import geoparquet_io as gpio

    result = gpio.read('fields.parquet').aggregate_a5(
        resolution=8,
        metric="area_ha,avg:yield,max:price",
    )
    result.write('cells.parquet')
    ```

### Category Breakdowns

Use `--breakdown` to pivot a categorical column into per-category count columns (`count_<value>`). Values beyond the limit are rolled into `count_other`.

=== "CLI"

    <!-- doctest: skip="breaks down by 'crop_type', a column the sample data does not have" -->
    ```bash
    # Breakdown by crop type (up to 20 categories, default)
    gpio process aggregate a5 fields.parquet cells.parquet \
        --resolution 8 --breakdown crop_type

    # Limit to top 10 categories
    gpio process aggregate a5 fields.parquet cells.parquet \
        --resolution 8 --breakdown crop_type --breakdown-limit 10
    ```

=== "Python"

    <!-- doctest: skip="aggregates 'area_ha', a column the sample data does not have" -->
    ```python
    import geoparquet_io as gpio

    result = gpio.read('fields.parquet').aggregate_a5(
        resolution=8,
        metric="sum:area_ha",
        breakdown="crop_type",
        breakdown_limit=10,
    )
    result.write('cells.parquet')
    ```

### Row Filtering

Use `--where` to aggregate only a subset of rows — a year, a category, a confidence threshold — without a separate pre-filter/rewrite pass. The clause is standard DuckDB SQL and applies to the source scan, so counts, metrics, breakdowns, and `--auto` resolution sizing all reflect only the filtered rows. All three subcommands (`a5`, `h3`, `admin`) support it, with the same semantics as [`gpio extract --where`](extract.md).

=== "CLI"

    <!-- doctest: skip="filters on 'determination:datetime', a column the sample data does not have" -->
    ```bash
    # Only 2025 rows (column names with special characters need
    # double quotes in SQL; shell escaping varies)
    gpio process aggregate a5 fields.parquet cells.parquet --resolution 6 \
        --where "year(\"determination:datetime\") = 2025"

    # Category / attribute filters
    gpio process aggregate h3 fields.parquet cells.parquet --resolution 8 \
        --where "\"crop:name\" = 'wheat'"
    gpio process aggregate admin fields.parquet by_country.parquet --level country \
        --where "confidence >= 50"

    # Hive partition columns are filterable too
    gpio process aggregate h3 "fields/**/*.parquet" cells.parquet --resolution 8 \
        --where "year = 2025"
    ```

=== "Python"

    <!-- doctest: skip="filters on 'determination:datetime', a column the sample data does not have" -->
    ```python
    import geoparquet_io as gpio

    result = gpio.read('fields.parquet').aggregate_a5(
        resolution=6,
        where="year(\"determination:datetime\") = 2025",
    )
    result.write('cells.parquet')
    ```

On a hive-partitioned input (`year=2025/...`), the partition columns are part of the scan, so `--where "year = 2025"` filters on them and DuckDB prunes the partitions it does not need. Partition columns never appear in the aggregated output.

The clause must be a single filtering expression: a `;` statement separator outside a quoted string is rejected, as are keywords that modify data (`DROP`, `DELETE`, `INSERT`, ...).
### Bucketing Point

Grid keying reduces every feature to a single point (by default the geometry
centroid) just to pick a cell — and with the default cell-polygon output, the
input geometry is read *only* to compute that point. For large polygon datasets
where geometry dominates file size, `--bucket-point bbox` derives the point from
a [bbox covering column](add.md) instead and **skips reading the geometry column
entirely**, cutting the scan to a small fraction:

| Value | Keying point | Reads geometry column? |
|-------|--------------|------------------------|
| `geometry` *(default)* | `ST_Centroid(geometry)` | yes |
| `bbox` | Center of the bbox covering column | **no** |
| `<column>` | An existing point column | **no** |

The bbox column is auto-detected from the file's GeoParquet `covering.bbox`
metadata when present, falling back to naming conventions (`bbox`, `*_bbox`,
`bounds`, `extent` structs with `xmin`/`ymin`/`xmax`/`ymax`); pass
`--bbox-column NAME` for uncovered, nonconventional names. Bbox center differs from the true
centroid only for L-shaped/very elongated features — negligible at aggregation
resolutions, where keying by centroid is already an approximation.

=== "CLI"

    <!-- doctest: skip="aggregates 'height', a column the sample data does not have" -->
    ```bash
    # 225 GB of building polygons, but only the bbox + height columns are read
    gpio process aggregate a5 buildings.parquet cells.parquet --auto \
        --bucket-point bbox --metric "avg:height"

    # Nonstandard bbox column name (not referenced by covering metadata and
    # not matching the naming conventions, so it can't be auto-detected)
    gpio process aggregate a5 buildings.parquet cells.parquet --resolution 8 \
        --bucket-point bbox --bbox-column my_box

    # Key from an existing point column
    gpio process aggregate h3 parcels.parquet cells.parquet --resolution 8 \
        --bucket-point anchor_point
    ```

=== "Python"

    <!-- doctest: skip="aggregates 'height', a column the sample data does not have" -->
    ```python
    import geoparquet_io as gpio

    result = gpio.read('buildings.parquet').aggregate_a5(
        resolution=10,
        metric="avg:height",
        bucket_point="bbox",
    )
    result.write('cells.parquet')
    ```

!!! note "`--auto` still probes the geometry column"
    Auto-resolution sizing samples geometry centroids over a bounded sample
    (≤500k rows), so `--auto` reads a small slice of the geometry column even
    with `--bucket-point bbox`. Pass `--resolution` explicitly to avoid touching
    geometry altogether.

### Output Geometry

Control what geometry each output row carries with `--out-geometry`:

| Value | Output |
|-------|--------|
| `polygon` | A5 cell polygon (default, valid GeoParquet) |
| `centroid` | Cell centroid point (valid GeoParquet) |
| `both` | Both polygon and centroid (centroid as WKB column) |
| `none` | No geometry — plain Parquet table |

=== "CLI"

    ```bash
    # Default: A5 polygon (valid GeoParquet)
    gpio process aggregate a5 fields.parquet cells.parquet --resolution 8

    # Centroid only
    gpio process aggregate a5 fields.parquet cells.parquet \
        --resolution 8 --out-geometry centroid

    # No geometry — plain Parquet, re-join to geometry later
    gpio process aggregate a5 fields.parquet cells_stats.parquet \
        --resolution 8 --out-geometry none
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Centroid geometry
    result = gpio.read('fields.parquet').aggregate_a5(
        resolution=8,
        out_geometry="centroid",
    )
    result.write('cells.parquet')

    # No geometry (plain Parquet)
    result = gpio.read('fields.parquet').aggregate_a5(
        resolution=8,
        out_geometry="none",
    )
    result.write('cells_stats.parquet')
    ```

### Re-joining Geometry Later

When `--out-geometry none` is used, the output is a plain (non-geo) Parquet file. The `a5_cell` bucket id lets you re-join to any A5 geometry source:

=== "CLI"

    <!-- doctest: skip="aggregates 'area_ha', a column the sample data does not have" -->
    ```bash
    # Step 1: aggregate without geometry
    gpio process aggregate a5 fields.parquet cells_stats.parquet \
        --resolution 8 --metric "sum:area_ha" --out-geometry none

    # Step 2: inspect the result (non-geo Parquet)
    gpio inspect summary cells_stats.parquet
    # a5_cell, count, sum_area_ha columns are present
    ```

=== "Python"

    <!-- doctest: skip="aggregates 'area_ha', a column the sample data does not have" -->
    ```python
    import geoparquet_io as gpio

    stats = gpio.read('fields.parquet').aggregate_a5(
        resolution=8, metric="sum:area_ha", out_geometry="none"
    )
    # stats is a plain (non-geo) Table; use .to_arrow() for DuckDB joins
    arrow = stats.to_arrow()
    ```

### Complete A5 Example

Combining all three kinds of statistics. The output cell carries `count`, `sum_area_ha`, `avg_yield`, and a `count_<crop>` column per crop type — so a map can **size** each cell by `count`, **shade** it by `sum_area_ha` (total farmed area) or `avg_yield` (typical productivity), and **filter** by crop.

=== "CLI"

    <!-- doctest: skip="aggregates 'area_ha', a column the sample data does not have" -->
    ```bash
    gpio process aggregate a5 fields.parquet cells.parquet \
        --auto \
        --metric "sum:area_ha,avg:yield" \
        --breakdown crop_type \
        --breakdown-limit 15 \
        --out-geometry polygon
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio
    ```

    <!-- doctest: skip="aggregates 'area_ha', a column the sample data does not have" -->
    ```python
    gpio.read('fields.parquet') \
        .aggregate_a5(
            resolution=8,
            metric="sum:area_ha,avg:yield",
            breakdown="crop_type",
            breakdown_limit=15,
        ) \
        .write('cells.parquet')
    ```

---

## H3 Grid Aggregation

H3 aggregation works exactly like A5 — the same `--metric`, `--breakdown`,
`--breakdown-limit`, `--out-geometry`, `--resolution`, and `--auto` options apply.
The differences are that H3 uses Uber's hexagonal grid, its resolution range is
**0–15** (A5 is 0–30), and the bucket id column is `h3_cell` (a string, e.g.
`861fa80e7ffffff`).

=== "CLI"

    <!-- doctest: skip="aggregates 'area_ha', a column the sample data does not have" -->
    ```bash
    gpio process aggregate h3 fields.parquet cells.parquet \
        --resolution 8 \
        --metric "sum:area_ha" \
        --breakdown crop_type
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio
    ```

    <!-- doctest: skip="aggregates 'area_ha', a column the sample data does not have" -->
    ```python
    gpio.read('fields.parquet') \
        .aggregate_h3(
            resolution=8,
            metric="sum:area_ha",
            breakdown="crop_type",
        ) \
        .write('cells.parquet')
    ```

---

## Admin Region Aggregation

Aggregate into administrative regions (countries or sub-national regions) using Overture Maps boundary data. A cache of the admin dataset is downloaded on first use.

### Level

=== "CLI"

    <!-- doctest: network -->
    ```bash
    # Country level (default)
    gpio process aggregate admin fields.parquet by_country.parquet

    # Sub-national regions
    gpio process aggregate admin fields.parquet by_region.parquet --level region
    ```

=== "Python"

    <!-- doctest: network -->
    ```python
    import geoparquet_io as gpio

    # Country level
    result = gpio.read('fields.parquet').aggregate_admin(level="country")
    result.write('by_country.parquet')

    # Region level
    result = gpio.read('fields.parquet').aggregate_admin(level="region")
    result.write('by_region.parquet')
    ```

The bucket id columns in admin output are `admin_code` (ISO country/region code) and `admin_name`. Features that fall outside all known admin regions are placed in an `unassigned` bucket.

!!! note "Known limitation"
    `admin_name` currently equals the ISO code (same as `admin_code`). A separate human-readable name column is not yet available from the per-level Overture cache.

### Metric Rollups and Breakdown

The same `--metric` and `--breakdown` options are available for admin aggregation:

=== "CLI"

    <!-- doctest: skip="aggregates 'area_ha', a column the sample data does not have" -->
    ```bash
    gpio process aggregate admin fields.parquet by_country.parquet \
        --level country \
        --metric "sum:area_ha,avg:yield" \
        --breakdown crop_type
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio
    ```

    <!-- doctest: skip="aggregates 'area_ha', a column the sample data does not have" -->
    ```python
    gpio.read('fields.parquet') \
        .aggregate_admin(
            level="country",
            metric="sum:area_ha,avg:yield",
            breakdown="crop_type",
        ) \
        .write('by_country.parquet')
    ```

### Output Geometry for Admin

The same `--out-geometry polygon|centroid|both|none` options apply. With `none`, the output is a plain Parquet table that can be joined to country/region geometry from any source using `admin_code`.

=== "CLI"

    <!-- doctest: skip="aggregates 'area_ha', a column the sample data does not have" -->
    ```bash
    # Stats only — no geometry
    gpio process aggregate admin fields.parquet country_stats.parquet \
        --level country --metric "sum:area_ha" --out-geometry none
    ```

=== "Python"

    <!-- doctest: skip="aggregates 'area_ha', a column the sample data does not have" -->
    ```python
    import geoparquet_io as gpio

    stats = gpio.read('fields.parquet').aggregate_admin(
        level="country", metric="sum:area_ha", out_geometry="none"
    )
    stats.write('country_stats.parquet')
    ```

---

## Output Schema

Regardless of the chosen bucket scheme, every output file contains:

| Column | Type | Description |
|--------|------|-------------|
| `a5_cell` or `admin_code` | UBIGINT / VARCHAR | Bucket identifier |
| `admin_name` | VARCHAR | Human-readable name (admin only; currently equals `admin_code`) |
| `count` | BIGINT | Number of input features in the bucket |
| `sum_<col>`, `avg_<col>`, etc. | DOUBLE | Numeric rollups from `--metric` |
| `count_<value>` | BIGINT | Per-category counts from `--breakdown` |
| `count_other` | BIGINT | Count for categories beyond `--breakdown-limit` |
| `geometry` | WKB | Bucket polygon or centroid (absent when `--out-geometry none`) |

### Cells at the antimeridian and the poles

A grid cell is a region of the sphere, and a few of them cannot be drawn as a
single flat polygon. Those are written the way [RFC 7946
3.1.9](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.9) prescribes:

- a cell straddling the antimeridian is **cut at ±180 into a `MultiPolygon`**,
  with one part either side of the line, rather than a ring that stretches
  across the whole map;
- a cell holding a pole is closed through an explicit seam that runs up to
  ±90, so it covers the polar cap instead of self-intersecting;
- every coordinate written stays inside `[-180, 180]`, which is what
  `gpio check spec` requires of a geographic CRS.

So `geometry_types` in the output's GeoParquet metadata reads
`["Polygon", "MultiPolygon"]` (or just `["MultiPolygon"]`) for any dataset that
reaches the antimeridian, and consumers must expect both. A dataset that sits
astride the line — rather than spanning the globe — also gets the RFC 7946 5.2
wrap-around `bbox`, with `xmin > xmax`.

`--out-geometry centroid` and `both` take the centroid from the grid library's
own cell-centre function, which reports lon/lat in range, so it always falls
inside the polygon on the same row.

---

## Common Options

Both commands support the standard output options:

```text
--where "confidence >= 50"  # DuckDB WHERE clause filtering input rows
--metric-nodata "-999"    # NoData sentinel(s) mapped to NULL in --metric columns
--compression SNAPPY      # Output compression (default: ZSTD)
--geoparquet-version 1.1  # GeoParquet spec version
--show-sql                # Print the generated DuckDB SQL
--verbose                 # Detailed progress output
```

## See Also

- [Overview Levels](process-overview.md) — derive coarser levels from an aggregate output
- [PMTiles Pyramids](geojson.md#pmtiles-pyramids) — bake all levels into one zoom-banded archive
- [Adding Spatial Indices](add.md) — add A5/H3 columns before aggregating
- [Partitioning Files](partition.md) — split large files by spatial index or admin region
- [Python API Reference](../api/python-api.md#aggregation) — full API documentation
