# Adding Spatial Indices & Metadata

The `add` commands enhance GeoParquet files with spatial indices, geometry metrics, and administrative metadata.

!!! note "The output keeps the input's GeoParquet version and CRS"
    Every `add` command rewrites the whole file to append its column, and with
    no `--geoparquet-version` that rewrite preserves what the input was: a 1.1
    file stays 1.1, a 2.0 file stays 2.0, and a file that carries a native
    Parquet `GEOMETRY` type with no `geo` key is written as native 2.0. Until
    [#993](https://github.com/geoparquet/geoparquet-io/issues/993) that last
    case was re-encoded as 1.1 WKB, which stripped the logical type **and the
    CRS stored inside it** — an `EPSG:5070` file came back out declaring
    nothing, which every reader resolves as `OGC:CRS84`. Pass
    `--geoparquet-version` to ask for something other than the input's version;
    the CRS is preserved either way, including when you ask for 1.1, which has
    nowhere but the `geo` block to record it.

    It is the same rewrite behind `gpio sort quadkey` and
    `gpio partition quadkey/h3/s2/a5/kdtree`, which add their index column to a
    scratch file first, so those commands preserve the version and the CRS now
    too.

    Two shapes are not fully covered yet, both pinned by failing-on-purpose
    tests: a native `GEOGRAPHY` column comes back as a planar `GEOMETRY` while
    the metadata still declares spherical edges
    ([#999](https://github.com/geoparquet/geoparquet-io/issues/999)), and a
    *secondary* geometry column keeps its own Parquet type but is left out of
    `geo.columns` and loses its CRS
    ([#1000](https://github.com/geoparquet/geoparquet-io/issues/1000)).

!!! note "Coordinate Reference Systems"
    The grid-index commands (`h3`, `s2`, `a5`, `quadkey`) and `admin-divisions` are
    **CRS-aware**. If your input declares a projected CRS (e.g. EPSG:5070 or
    EPSG:28992), the geometry centroid is reprojected to OGC:CRS84 (lon/lat
    degrees) before the cell is computed, and the input is reprojected to the
    admin boundaries' CRS before the spatial join — so you no longer need to run
    `gpio convert reproject` first. Inputs without a declared CRS are treated as
    OGC:CRS84 per the GeoParquet spec.

## Bounding Boxes

Add precomputed bounding boxes for faster spatial queries:

=== "CLI"

    ```bash
    gpio add bbox input.parquet output.parquet
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Works with remote files
    gpio --aws-profile prod add bbox s3://bucket/input.parquet s3://bucket/output.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').add_bbox().write('output.parquet')

    # Custom column name
    gpio.read('input.parquet').add_bbox(column_name='bounds').write('output.parquet')
    ```

Creates a struct column with `{xmin, ymin, xmax, ymax}` for each feature, plus the `covering` metadata that points at it. Because gpio computes the column from the geometry in the same statement, it can declare the covering. For a column gpio did not compute, the only name it will vouch for is a struct column called exactly `bbox` — the universal convention every 1.0-era writer emitted before `covering` existed, which any write carries forward with a covering. Any other name (`bounds`, `extent`, `tile_bbox`) is left undeclared, since a covering asserts a relationship gpio cannot verify from a column name; `gpio add bbox-metadata` is where you assert it deliberately. The `covering` key is not part of the GeoParquet 2.0 specification text — it was introduced in 1.1 and removed in 2.0 in favour of the native statistics. 2.0 readers must tolerate unknown fields, so a covering stays legal to carry, and [geoparquet#302](https://github.com/opengeospatial/geoparquet/pull/302) *proposes* reinstating it as an option (still open at time of writing). The motivation is real either way: native statistics prune whole row groups, while a bbox column's page index also prunes pages within one.

### Existing Bbox Detection

The command automatically checks for existing bbox columns:

- **If bbox exists with metadata**: Nothing is recomputed, and the input is copied to the output file unchanged
- **If bbox exists without metadata**: Same copy, plus a suggestion to use `gpio add bbox-metadata`
- **If `--bbox-name` asks for a different name**: That column is computed and added alongside the existing one, with a warning that the file will have two bbox columns
- **If a write option is asked for explicitly** (`--geoparquet-version`, `--compression`, `--compression-level`, `--row-group-size`): the column is recomputed instead of copied, because a verbatim copy reproduces the input's settings and cannot honour the requested ones
- **Use `--force`**: Replace existing bbox column with a freshly computed one

The copy matters for pipelines: `gpio add bbox in.parquet out.parquet` always leaves a file at `out.parquet` with a bbox column, whether or not one had to be computed, so the next step in a script has something to read ([#728](https://github.com/geoparquet/geoparquet-io/issues/728)). Every copy is reported on the console, so a recomputed bbox is never confused with a copied one. `--dry-run` says which of the two it would be. When no output file is given there is nothing to write, and the command only reports.

The same holds for a stream: `... | gpio add bbox - out.parquet` on an input that already has a bbox column passes the data through untouched and declares no covering for a column gpio did not compute.

That copy is a remote write like any other, so it goes through the object store gpio is configured to use: `--s3-endpoint`, `--s3-region`, `--s3-no-ssl` and `--aws-profile` apply to it exactly as they do to a recomputed output, and a copy to MinIO no longer ends up pointed at AWS ([#810](https://github.com/geoparquet/geoparquet-io/issues/810)). Inputs may be local paths or `s3://`, `gs://` and `https://` URLs — an `https://` input is fetched with a plain streaming GET of the URL exactly as given, so a presigned URL keeps its signature. Outputs may be local paths or `s3://`, `gs://` and `az://` URLs. `az://` is a *write destination only*: reading from Azure is not supported, and an `az://` input is refused up front with a message saying so, rather than failing deep in the metadata probe. An Azure output URL is written account-first, `az://<account>/<container>/<path>`, with the credential taken from `AZURE_STORAGE_ACCOUNT_KEY` or `AZURE_STORAGE_SAS_TOKEN` ([#864](https://github.com/geoparquet/geoparquet-io/issues/864)); the `abfs://`, `abfss://` and `azure://` spellings order those parts differently and are refused rather than mis-read. Any scheme the copy cannot serve is refused by name before anything is read, rather than failing partway through.

The copy resolves S3 credentials the same way `gpio publish upload` does: through the full AWS chain — environment keys, `--aws-profile`, `~/.aws/credentials`, and the SSO, assume-role and `credential_process` configurations botocore supports ([#865](https://github.com/geoparquet/geoparquet-io/issues/865)) — so a session that can read the input can also write the copy. Credentials are resolved once at the start of the write; a session token that expires mid-upload is not refreshed.

<!-- doctest: menu -->
```bash
# Already has a bbox: copies input to output, computing nothing
gpio add bbox input.parquet output.parquet

# Force replace existing bbox
gpio add bbox input.parquet output.parquet --force
```

**Options:**

<!-- doctest: menu -->
```bash
# Custom column name
gpio add bbox input.parquet output.parquet --bbox-name bounds

# Force replace existing bbox
gpio add bbox input.parquet output.parquet --force

# With compression settings
gpio add bbox input.parquet output.parquet --compression ZSTD --compression-level 15

# Dry run (preview SQL)
gpio add bbox input.parquet output.parquet --dry-run
```

### Add Bbox Metadata Only

If your file already has a bbox column but lacks covering metadata (e.g., from external tools):

=== "CLI"

    ```bash
    gpio add bbox-metadata myfile.parquet
    ```

    This modifies the file in-place to add only the metadata, without creating a new file.

    !!! note "Requires GeoParquet 1.1+"

        The `covering` key was introduced in GeoParquet 1.1, so this command fails on a
        file declaring 1.0 rather than writing metadata that version cannot carry.
        Convert it first: `gpio convert geoparquet in.parquet out.parquet --geoparquet-version 1.1`.

    !!! note "Requires existing GeoParquet metadata"

        The command adds a `covering` key to metadata that already describes the
        geometry column. On plain Parquet with no `geo` key it fails rather than
        inventing a `geo` block with no `encoding` and no `geometry_types`. Make it
        GeoParquet first: `gpio convert geoparquet in.parquet out.parquet`.
        `Table.add_bbox_metadata()` and `ops.add_bbox_metadata()` refuse the same
        input with the same error. A file with no bbox column also fails, rather
        than printing the error and exiting `0`.

=== "Python"

    <!-- doctest: skip="needs file_with_bbox.parquet, which the harness does not seed" -->
    ```python
    import geoparquet_io as gpio

    # Add bbox column, then add covering metadata
    table = gpio.read('input.parquet')
    table_with_bbox = table.add_bbox().add_bbox_metadata()
    table_with_bbox.write('output.parquet')

    # Or just add metadata if bbox column already exists
    table = gpio.read('file_with_bbox.parquet')
    table.add_bbox_metadata().write('output.parquet')
    ```

## H3 Hexagonal Cells

!!! note "Input CRS"
    The grid commands (`h3`, `s2`, `a5`, `quadkey`) key on lon/lat. Input in a
    non-CRS84 CRS is reprojected to lon/lat automatically before cell assignment,
    so projected data (e.g. national grids in metres) keys correctly with no
    manual reprojection. Input already in OGC:CRS84 / EPSG:4326 is untouched.

Add [H3](https://h3geo.org/) hexagonal cell IDs based on geometry centroids:

=== "CLI"

    ```bash
    gpio add h3 input.parquet output.parquet --resolution 9
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # From HTTPS to S3
    gpio add h3 https://example.com/data.parquet s3://bucket/indexed.parquet --resolution 9
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').add_h3(resolution=9).write('output.parquet')

    # Custom column name
    gpio.read('input.parquet').add_h3(column_name='h3_index', resolution=13).write('output.parquet')
    ```

**Resolution guide:**

--8<-- "_includes/h3-resolutions.md"

**Options:**

<!-- doctest: menu -->
```bash
# Custom column name
gpio add h3 input.parquet output.parquet --h3-name h3_index

# Different resolution
gpio add h3 input.parquet output.parquet --resolution 13

# With row group sizing
gpio add h3 input.parquet output.parquet --row-group-size-mb 256MB
```

## S2 Spherical Cells

Add [S2](https://s2geometry.io/) spherical cell IDs based on geometry centroids.
S2 cells are computed by the [`geography` DuckDB community extension](https://community-extensions.duckdb.org/extensions/geography.html),
which gpio installs on first use:

=== "CLI"

    ```bash
    gpio add s2 input.parquet output.parquet --level 13
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # From HTTPS to S3
    gpio add s2 https://example.com/data.parquet s3://bucket/indexed.parquet --level 13
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').add_s2(level=13).write('output.parquet')

    # Custom column name
    gpio.read('input.parquet').add_s2(column_name='s2_index', level=18).write('output.parquet')
    ```

S2 uses Google's Spherical Geometry library which divides the Earth's surface into a hierarchy of cells using quadtree subdivision. Unlike H3's hexagonal grid, S2 cells are variable quads that provide hierarchical spatial indexing.

**Level guide:**

--8<-- "_includes/s2-levels.md"

**Options:**

<!-- doctest: menu -->
```bash
# Custom column name
gpio add s2 input.parquet output.parquet --s2-name s2_index

# Different level
gpio add s2 input.parquet output.parquet --level 18

# With row group sizing
gpio add s2 input.parquet output.parquet --row-group-size-mb 256MB
```

### Technical Details

S2 cell IDs are computed using DuckDB's geography extension:

```sql
s2_cell_token(
    s2_cell_parent(
        s2_cellfromlonlat(
            ST_X(ST_Centroid(geometry)),
            ST_Y(ST_Centroid(geometry))
        ),
        level
    )
)
```

- **s2_cellfromlonlat**: Converts lon/lat to S2 cell at maximum precision (level 30)
- **s2_cell_parent**: Gets parent cell at desired level
- **s2_cell_token**: Converts to hex token string for portability

Cell IDs are stored as hex strings (e.g., `"89c25901"`) rather than integers for
maximum portability across systems.

## A5 Cells

Add [A5](https://a5geo.org/) spatial cell IDs based on geometry centroids.

=== "CLI"

    ```bash
    gpio add a5 input.parquet output.parquet --resolution 15
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # From HTTPS to S3
    gpio add a5 https://example.com/data.parquet s3://bucket/indexed.parquet --resolution 15
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').add_a5(resolution=15).write('output.parquet')

    # Custom column name
    gpio.read('input.parquet').add_a5(column_name='a5_index', resolution=12).write('output.parquet')
    ```

**Options:**

<!-- doctest: menu -->
```bash
# Custom column name
gpio add a5 input.parquet output.parquet --a5-name a5_index

# Different resolution
gpio add a5 input.parquet output.parquet --resolution 12

# With row group sizing
gpio add a5 input.parquet output.parquet --row-group-size-mb 256MB
```

## KD-Tree Partitions

Add balanced spatial partition IDs using KD-tree:

=== "CLI"

    <!-- doctest: menu -->
    ```bash
    # Auto-select partitions (default: ~120k rows each)
    gpio add kdtree input.parquet output.parquet

    # Explicit partition count (must be power of 2)
    gpio add kdtree input.parquet output.parquet --partitions 32

    # Exact mode (deterministic but slower)
    gpio add kdtree input.parquet output.parquet --partitions 16 --exact
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Auto mode: size the tree from the row count, like the bare CLI call
    gpio.read('input.parquet').add_kdtree(auto=True).write('output.parquet')

    # A different target, still auto
    gpio.read('input.parquet').add_kdtree(
        auto=True, target_rows=50000
    ).write('output.parquet')

    # Custom column name and an explicit iteration count
    gpio.read('input.parquet').add_kdtree(
        column_name='partition_id',
        iterations=5  # 2^5 = 32 partitions
    ).write('output.parquet')
    ```

!!! warning "Breaking change for the Python API: the implicit `iterations=9` is gone"
    `Table.add_kdtree()` and `ops.add_kdtree()` used to fall back to
    `iterations=9` — 512 partitions whatever the input — where the bare CLI call
    sizes the tree from the row count. Such a call now raises
    `InvalidParameterError`. Pass `auto=True` to get what the CLI gives you, or
    pass `iterations=9` to keep the output you had.

**Auto mode** (default):
- Targets ~120k rows per partition
- Uses approximate computation (O(n))
- Fast on large datasets

**Explicit mode**:
- Specify partition count (2, 4, 8, 16, 32, ...)
- Control granularity

**Exact vs Approximate**:
- Approximate: O(n), samples 100k points
- Exact: O(n × log₂(partitions)), deterministic

**Options:**

<!-- doctest: menu -->
```bash
# Custom target rows per partition
gpio add kdtree input.parquet output.parquet --auto 200000

# Custom sample size for approximate mode
gpio add kdtree input.parquet output.parquet --approx 200000

# Track progress
gpio add kdtree input.parquet output.parquet --verbose
```

## Geometry Metrics

Add geodesic area and perimeter measurements to each feature:

=== "CLI"

    <!-- doctest: menu -->
    ```bash
    gpio add geometry-metrics input.parquet output.parquet

    # Preview SQL without executing
    gpio add geometry-metrics input.parquet output.parquet --dry-run

    # Without Vecorel metadata
    gpio add geometry-metrics input.parquet output.parquet --no-vecorel
    ```

=== "Python"

    <!-- doctest: skip="the lines are alternatives that write the same output file" -->
    ```python
    import geoparquet_io as gpio
    from geoparquet_io.core.add.geometry_metrics import add_geometry_metrics

    # Add area and perimeter columns
    add_geometry_metrics('input.parquet', 'output.parquet')

    # Without Vecorel schema metadata
    add_geometry_metrics('input.parquet', 'output.parquet', vecorel=False)
    ```

Calculates two columns using WGS84 spheroid-based calculations:

- **`metrics:area`** — geodesic area in square meters (m²)
- **`metrics:perimeter`** — geodesic perimeter in meters (m)

By default, the output follows the [Vecorel geometry-metrics extension](https://vecorel.org/geometry-metrics-extension/v0.1.0/schema.yaml) specification. This adds `collection` metadata to the Parquet file referencing the schema URL, and ensures required columns (`id`, `geometry`) are present and non-nullable. Use `--no-vecorel` to skip the metadata and just add the raw metric columns.

**Options:**

<!-- doctest: menu -->
```bash
# With compression settings
gpio add geometry-metrics input.parquet output.parquet --compression ZSTD --compression-level 15

# With row group sizing
gpio add geometry-metrics input.parquet output.parquet --row-group-size-mb 256MB

# Show generated SQL
gpio add geometry-metrics input.parquet output.parquet --show-sql
```

!!! note "Input CRS"
    The spheroid calculations assume WGS84 (EPSG:4326) input. If your data uses a different CRS, reproject first with `gpio convert reproject`.

## Administrative Divisions

Add administrative division columns via spatial join with remote boundaries datasets:

### How It Works

Performs spatial intersection between your data and remote admin boundaries to add admin division columns. Uses efficient spatial extent filtering to query only relevant boundaries from remote datasets.

Input in a non-CRS84 CRS is reprojected to the boundaries' CRS (OGC:CRS84) before the intersection, so projected data joins correctly instead of erroring on a CRS mismatch. (For a non-CRS84 input the bbox pre-filter is skipped, since the stored bbox is in the source CRS — the extent filter still bounds the query.)

For native-geometry inputs (GeoParquet 2.0 and GeoParquet 1.1 with geoarrow encoding), the spatial join relies on native Parquet column statistics to skip irrelevant data, but this is not quite working yet, so expect those to be a bit slower until [#462](https://github.com/geoparquet/geoparquet-io/issues/462) is implemented. For non-native inputs (GeoParquet 1.x that carry a `bbox` column), a cheap bbox-overlap test is applied before `ST_Intersects` to prune candidates, and using it will be 5-10x faster.

The join is a streaming `LEFT JOIN` so it scales to very large inputs (hundreds of millions of features) with bounded memory. A feature is attributed to the admin polygon(s) it intersects; because the per-level caches are non-overlapping, this is normally exactly one polygon per level. A feature straddling overlapping polygons at a border is emitted once per match. De-duplicating such border overlaps is intentionally left to a future dedicated operation rather than folded into this join, as including it greatly degraded performance.

### Quick Start

=== "CLI"

    <!-- doctest: network -->
    ```bash
    # Add all GAUL levels (continent, country, department)
    gpio add admin-divisions input.parquet output.parquet --dataset gaul

    # Preview SQL before execution
    gpio add admin-divisions input.parquet output.parquet --dataset gaul --dry-run
    ```

=== "Python"

    <!-- doctest: network -->
    ```python
    import geoparquet_io as gpio

    # Add all GAUL levels (continent, country, department) -- the default,
    # exactly like the CLI with no --dataset/--levels
    table = gpio.read('input.parquet')
    enriched = table.add_admin_divisions()
    enriched.write('output.parquet')

    # Or name the levels explicitly
    enriched = table.add_admin_divisions(dataset='gaul', levels=['country'])
    enriched.write('output.parquet')
    ```

### Multi-Level Admin Divisions

Add multiple hierarchical administrative levels:

=== "CLI"

    <!-- doctest: network -->
    ```bash
    # Add all GAUL levels (adds admin:continent, admin:country, admin:department)
    gpio add admin-divisions buildings.parquet output.parquet --dataset gaul

    # Add specific levels only
    gpio add admin-divisions buildings.parquet output.parquet --dataset gaul \
      --levels continent,country

    # Use Overture Maps dataset
    gpio add admin-divisions buildings.parquet output.parquet --dataset overture \
      --levels country,region
    ```

=== "Python"

    <!-- doctest: network -->
    ```python
    import geoparquet_io as gpio

    # Add multiple levels
    table = gpio.read('buildings.parquet')
    enriched = table.add_admin_divisions(
        dataset='gaul',
        levels=['continent', 'country', 'department']
    )
    enriched.write('output.parquet')

    # Overture dataset, pinning the pre-1.4 column prefix
    enriched = table.add_admin_divisions(
        dataset='overture',
        levels=['country', 'region'],
        prefix='overture'
    )
    ```

### Vecorel-Compliant Output

Use `--vecorel` for output that follows the [Vecorel administrative division extension](https://vecorel.org/administrative-division-extension/v0.1.0/schema.yaml):

=== "CLI"

    <!-- doctest: network -->
    ```bash
    # Vecorel-compliant admin columns (uses Overture dataset automatically)
    gpio add admin-divisions input.parquet output.parquet --vecorel

    # Equivalent to:
    gpio add admin-divisions input.parquet output.parquet \
      --dataset overture --levels country,region --prefix admin
    ```

=== "Python"

    <!-- doctest: network -->
    ```python
    import geoparquet_io as gpio

    table = gpio.read('input.parquet')
    enriched = table.add_admin_divisions(
        dataset='overture',
        levels=['country', 'region'],
        vecorel=True
    )
    enriched.write('output.parquet')
    ```

The `--vecorel` flag:

- Forces the **Overture** dataset (overrides `--dataset`)
- Sets levels to **country,region** (overrides `--levels`)
- Adds `admin:country_code` (ISO 3166-1 alpha-2) and `admin:subdivision_code` (ISO 3166-2)
- Writes `collection` metadata with the Vecorel schema URL
- Ensures `id` and `geometry` columns are present and non-nullable

### Datasets

--8<-- "_includes/admin-datasets.md"

!!! note "Overture: per-level cache and simplified boundaries"
    The Overture dataset uses a **per-level cache**: country and region
    boundaries are downloaded and cached as separate files rather than one
    combined file.

    The country level covers **dependent territories** as well as sovereign
    states, each attributed to its own ISO 3166-1 alpha-2 code — `GF` for
    French Guiana, `PR` for Puerto Rico, `RE` for Réunion, `GL`, `HK`, `NC`
    and the rest, plus `BQ` and `SJ` for the Caribbean and Arctic islands
    Overture files under placeholder codes. They are not folded into their
    sovereign's code.

    Overture boundaries are simplified with `ST_SimplifyPreserveTopology` at a
    tolerance of `0.0001` degrees (roughly 11 m near the equator, ~7 m at
    50°N). Country and region layers are simplified **independently**, so
    shared borders are not perfectly coincident. As a result, **near-border
    attribution is approximate**: a feature sitting in a border zone may be
    attributed to the neighboring region. This is a deliberate
    accuracy-for-memory tradeoff. If you need exact boundaries near borders,
    use the `gaul` dataset instead.

### Caching

Admin datasets (GAUL, Overture) are automatically cached locally on first use:

- **First run**: Downloads and caches the full dataset (~5-50MB depending on dataset)
- **Subsequent runs**: Uses cached version (instant startup)
- **Cache location**: `~/.geoparquet-io/cache/admin/`
- **Warning**: Shown if cache is older than 6 months

**Cache management options:**

=== "CLI"

    <!-- doctest: network -->
    ```bash
    # Skip cache and use remote directly
    gpio add admin-divisions input.parquet output.parquet --dataset gaul --no-cache

    # Clear all cached datasets (prompts for confirmation)
    gpio add admin-divisions input.parquet output.parquet --dataset gaul --clear-cache
    ```

=== "Python"

    <!-- doctest: network -->
    ```python
    import geoparquet_io as gpio
    from geoparquet_io.core.admin_datasets import get_cache_dir, clear_cache

    # Default: uses cached datasets automatically
    gpio.read('input.parquet').add_admin_divisions(
        dataset='overture',
        levels=['country', 'region']
    ).write('output.parquet')

    # Check cache location (~/.geoparquet-io/cache/admin/)
    cache_dir = get_cache_dir()
    print(f"Cache location: {cache_dir}")

    # Clear all cached admin datasets
    result = clear_cache(confirm=True)
    print(f"Cleared {result['files_deleted']} files, {result['bytes_freed'] / 1024 / 1024:.2f} MB freed")
    ```

!!! tip "When to clear cache"
    Clear your cache when you need fresh admin boundary data, such as after a
    new Overture Maps release. The cache files are named with version numbers
    (e.g., `gaul-2024-12-19.parquet`, `overture-2025-10-22.0.parquet`).

## Common Options

All `add` commands support:

--8<-- "_includes/common-cli-options.md"

```text
--add-bbox         # Auto-add bbox if missing (some commands)
```

## See Also

- [CLI Reference](../cli/overview.md) - Full command reference
- [partition command](partition.md)
- [sort command](sort.md)
