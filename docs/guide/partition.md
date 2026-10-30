# Partitioning Files

The `partition` commands split GeoParquet files into separate files based on column values or spatial indices.

**Smart Analysis**: All partition commands automatically analyze your strategy before execution, providing statistics and recommendations.

## Auto-Resolution Mode

All spatial partitioning commands (H3, S2, A5, Quadkey) support **automatic resolution calculation** using the `--auto` flag, and the Python API takes the same `auto=True`. This eliminates the need to manually specify resolution levels by calculating the optimal value based on your data.

Neither front end guesses for you: pass a resolution, or ask for `auto`. A call with neither raises rather than silently partitioning at some arbitrary default.

!!! warning "Breaking change for the Python API: the implicit resolutions are gone"
    The `Table.partition_by_*` methods used to fall back to a hardcoded
    resolution the CLI never applied, so the same file partitioned differently
    depending on which front end you used. Such a call now raises
    `InvalidParameterError`. Pass `auto=True` to get what the CLI gives you, or
    pass the old value explicitly to keep the output you had:
    `partition_by_h3(resolution=9)`,
    `partition_by_quadkey(resolution=13, partition_resolution=6)`,
    `partition_by_s2(level=13)`, `partition_by_a5(resolution=15)`,
    `partition_by_kdtree(iterations=9)`. The same applies to `add_kdtree()`,
    which pinned `iterations=9` too.

    One workflow is exempt: `partition_by_kdtree()` on a table that already
    carries the `kdtree_cell` column (say from `add_kdtree()`) needs no sizing
    parameter — the existing cells drive the partition, exactly as they did
    before this change.

### How It Works

Auto-resolution analyzes your dataset and calculates the optimal spatial index resolution to achieve your target partition size:

1. Counts total rows in your input file
2. Calculates how many partitions are needed to achieve `--target-rows` per partition
3. Selects the resolution that produces approximately that many partitions
4. Respects `--max-partitions` as an upper bound

### Common Options

| Option | Python | Default | Description |
|--------|--------|---------|-------------|
| `--auto` | `auto=True` | off | Enable auto-resolution calculation |
| `--target-rows` | `target_rows=` | 100,000 | Target rows per partition |
| `--max-partitions` | `max_partitions=` | 10,000 | Maximum partitions to create |

### Quick Examples

=== "CLI"

    ```bash
    # H3 with ~100K rows per partition (default)
    gpio partition h3 input.parquet output/ --auto

    # Quadkey with partition limit
    gpio partition quadkey input.parquet output/ --auto --max-partitions 1000

    # A5 with preview
    gpio partition a5 input.parquet --auto --preview
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # H3 with ~100K rows per partition (default)
    gpio.read('input.parquet').partition_by_h3('output/', auto=True)

    # Quadkey with partition limit
    gpio.read('input.parquet').partition_by_quadkey(
        'output/', auto=True, max_partitions=1000
    )

    # A5 targeting smaller partitions
    gpio.read('input.parquet').partition_by_a5('output/', auto=True, target_rows=50000)
    ```

### How auto-resolution is chosen

Auto-resolution is **extent-aware**: gpio probes a bounded sample of your data
and counts how many cells are actually non-empty at each candidate resolution,
then picks the resolution whose non-empty cell count is closest to your target
partition count (`total rows ÷ --target-rows`, capped by `--max-partitions`).

This sizes partitions to where your data actually is. A globally-uniform
formula (e.g. `cells = 6 × 4^resolution` for A5) assumes the data is spread
evenly across the whole planet, which badly under-resolves regional or national
datasets — collapsing them into a handful of giant partitions. If the sample
probe can't run (for example, the file has no geometry column), gpio falls back
to the uniform-coverage estimate.

> **Note:** cell assignment is CRS-aware — a projected input (e.g. EPSG:5070) is
> reprojected to OGC:CRS84 before the grid cell is computed, and `partition admin`
> reprojects the input to the admin boundaries' CRS before the spatial join. The
> auto-resolution probe still samples the input's own coordinates, so its
> estimate is most accurate for lon/lat data.

## By String Column

Partition by string column values or prefixes:

=== "CLI"

    <!-- doctest: skip="partitions on 'region', a column the sample data does not have" -->
    ```bash
    # Preview partitions
    gpio partition string input.parquet --column region --preview

    # Partition by full column values
    gpio partition string input.parquet output/ --column category

    # Partition by first 2 characters
    gpio partition string input.parquet output/ --column mgrs_code --chars 2

    # Hive-style partitioning
    gpio partition string input.parquet output/ --column region --hive
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # To cloud storage
    gpio --aws-profile prod partition string s3://bucket/input.parquet s3://bucket/output/ --column region
    ```

=== "Python"

    <!-- doctest: skip="partitions on 'category', a column the sample data does not have" -->
    ```python
    import geoparquet_io as gpio

    # Partition by full column values
    gpio.read('input.parquet').partition_by_string('output/', column='category')

    # Partition by first 2 characters
    gpio.read('input.parquet').partition_by_string(
        'output/',
        column='mgrs_code',
        chars=2
    )

    # Hive-style with options
    gpio.read('input.parquet').partition_by_string(
        'output/',
        column='region',
        hive=True,
        overwrite=True
    )
    ```

## By H3 Cells

Partition by H3 hexagonal cells:

=== "CLI"

    <!-- doctest: menu -->
    ```bash
    # Auto-calculate optimal resolution for ~100K rows per partition
    gpio partition h3 input.parquet output/ --auto

    # Auto with custom target partition size
    gpio partition h3 input.parquet output/ --auto --target-rows 50000

    # Preview at resolution 7 (~5km² cells)
    gpio partition h3 input.parquet --resolution 7 --preview
    ```

    <!-- doctest: skip="the 766-row sample is too small to partition meaningfully" -->
    ```bash
    # Partition at specific resolution 9
    gpio partition h3 input.parquet output/ --resolution 9

    # Keep H3 column in output files
    gpio partition h3 input.parquet output/ --resolution 9 --keep-h3-column

    # Hive-style (H3 column included by default)
    gpio partition h3 input.parquet output/ --resolution 8 --hive
    ```

=== "Python"

    <!-- doctest: skip="the 766-row sample is too small to partition meaningfully" -->
    ```python
    import geoparquet_io as gpio

    # Partition by H3 (flat files by default, like the CLI; pass hive=True for key=value/)
    # A resolution is required -- or ask gpio to size one with auto=True.
    gpio.read('input.parquet').partition_by_h3('output/', resolution=7)

    # Let gpio pick the resolution from the data
    gpio.read('input.parquet').partition_by_h3('output/', auto=True)

    # With options
    gpio.read('input.parquet').partition_by_h3(
        'output/',
        resolution=8,
        compression='ZSTD',
        overwrite=True
    )
    ```

**Column behavior:**

- Non-Hive: H3 column excluded by default (redundant with path)
- Hive: H3 column included by default
- Use `--keep-h3-column` to explicitly keep

If H3 column doesn't exist, it's automatically added.

### Auto-Resolution for H3

Use `--auto` to let gpio calculate the optimal H3 resolution:

=== "CLI"

    ```bash
    # Auto-select resolution for ~100k rows per partition (default)
    gpio partition h3 input.parquet output/ --auto

    # Target 50k rows per partition
    gpio partition h3 input.parquet output/ --auto --target-rows 50000

    # Limit maximum partitions created
    gpio partition h3 input.parquet output/ --auto --max-partitions 5000

    # Preview auto-selected partitions
    gpio partition h3 input.parquet --auto --preview
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Auto-select resolution for ~100k rows per partition (default)
    gpio.read('input.parquet').partition_by_h3('output/', auto=True)

    # Target 50k rows per partition
    gpio.read('input.parquet').partition_by_h3('output/', auto=True, target_rows=50000)

    # Limit maximum partitions created
    gpio.read('input.parquet').partition_by_h3('output/', auto=True, max_partitions=5000)
    ```

Auto-resolution probes your data's actual extent (see [How auto-resolution is chosen](#how-auto-resolution-is-chosen)) to pick the H3 resolution that targets your specified rows per partition, while respecting the `--max-partitions` constraint.

## By S2 Cells

Partition by S2 spherical cells. S2 partitioning uses the `geography` DuckDB
community extension, which gpio installs on first use; see
[S2 Spherical Cells](add.md#s2-spherical-cells) for how the cells are computed.

=== "CLI"

    <!-- doctest: menu -->
    ```bash
    # Auto-calculate optimal level for ~100K rows per partition
    gpio partition s2 input.parquet output/ --auto

    # Auto with custom target partition size
    gpio partition s2 input.parquet output/ --auto --target-rows 500000

    # Preview at level 10 (~78 km² cells)
    gpio partition s2 input.parquet --level 10 --preview

    # Hive-style (S2 column included by default)
    gpio partition s2 input.parquet output/ --auto --hive
    ```

    <!-- doctest: skip="the 766-row sample is too small to partition meaningfully" -->
    ```bash
    # Partition at specific level 13 (~1.2km² cells)
    gpio partition s2 input.parquet output/ --level 13

    # Keep S2 column in output files
    gpio partition s2 input.parquet output/ --level 12 --keep-s2-column
    ```

=== "Python"

    <!-- doctest: skip="the 766-row sample is too small to partition meaningfully" -->
    ```python
    import geoparquet_io as gpio

    # Partition by S2 (flat files by default, like the CLI; pass hive=True for key=value/)
    # A level is required -- or ask gpio to size one with auto=True.
    gpio.read('input.parquet').partition_by_s2('output/', level=10)

    # Let gpio pick the level from the data
    gpio.read('input.parquet').partition_by_s2('output/', auto=True)

    # With options
    gpio.read('input.parquet').partition_by_s2(
        'output/',
        level=10,
        compression='ZSTD',
        overwrite=True
    )
    ```

**Column behavior:**

- Non-Hive: S2 column excluded by default (redundant with path)
- Hive: S2 column included by default
- Use `--keep-s2-column` to explicitly keep

If S2 column doesn't exist, it's automatically added.

### Auto-Resolution for S2

Use `--auto` to let gpio calculate the optimal S2 level:

=== "CLI"

    ```bash
    # Auto-select level for ~100k rows per partition (default)
    gpio partition s2 input.parquet output/ --auto

    # Target 50k rows per partition
    gpio partition s2 input.parquet output/ --auto --target-rows 50000

    # Limit maximum partitions created
    gpio partition s2 input.parquet output/ --auto --max-partitions 5000

    # Preview auto-selected partitions
    gpio partition s2 input.parquet --auto --preview
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Auto-select level for ~100k rows per partition (default)
    gpio.read('input.parquet').partition_by_s2('output/', auto=True)

    # Target 50k rows per partition
    gpio.read('input.parquet').partition_by_s2('output/', auto=True, target_rows=50000)

    # Limit maximum partitions created
    gpio.read('input.parquet').partition_by_s2('output/', auto=True, max_partitions=5000)
    ```

Auto-resolution probes your data's actual extent (see [How auto-resolution is chosen](#how-auto-resolution-is-chosen)) to pick the S2 level that targets your specified rows per partition, while respecting the `--max-partitions` constraint.

## By A5 Cells

Partition by A5 spatial cells:

=== "CLI"

    <!-- doctest: menu -->
    ```bash
    # Auto-calculate optimal resolution for ~100K rows per partition
    gpio partition a5 input.parquet output/ --auto

    # Auto with custom target partition size
    gpio partition a5 input.parquet output/ --auto --target-rows 500000

    # Preview at resolution 10 (~41km² cells)
    gpio partition a5 input.parquet --resolution 10 --preview

    # Hive-style (A5 column included by default)
    gpio partition a5 input.parquet output/ --auto --hive
    ```

    <!-- doctest: skip="the 766-row sample is too small to partition meaningfully" -->
    ```bash
    # Partition at specific resolution 15
    gpio partition a5 input.parquet output/ --resolution 15

    # Keep A5 column in output files
    gpio partition a5 input.parquet output/ --resolution 12 --keep-a5-column
    ```

=== "Python"

    <!-- doctest: skip="the 766-row sample is too small to partition meaningfully" -->
    ```python
    import geoparquet_io as gpio

    # Partition by A5 (flat files by default, like the CLI; pass hive=True for key=value/)
    # A resolution is required -- or ask gpio to size one with auto=True.
    gpio.read('input.parquet').partition_by_a5('output/', resolution=12)

    # Let gpio pick the resolution from the data
    gpio.read('input.parquet').partition_by_a5('output/', auto=True)
    ```

**Column behavior:**

- Non-Hive: A5 column excluded by default (redundant with path)
- Hive: A5 column included by default
- Use `--keep-a5-column` to explicitly keep

If A5 column doesn't exist, it's automatically added.

### Auto-Resolution for A5

Use `--auto` to let gpio calculate the optimal A5 resolution:

=== "CLI"

    ```bash
    # Auto-select resolution for ~100k rows per partition (default)
    gpio partition a5 input.parquet output/ --auto

    # Target 50k rows per partition
    gpio partition a5 input.parquet output/ --auto --target-rows 50000

    # Limit maximum partitions created
    gpio partition a5 input.parquet output/ --auto --max-partitions 5000

    # Preview auto-selected partitions
    gpio partition a5 input.parquet --auto --preview
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Auto-select resolution for ~100k rows per partition (default)
    gpio.read('input.parquet').partition_by_a5('output/', auto=True)

    # Target 50k rows per partition
    gpio.read('input.parquet').partition_by_a5('output/', auto=True, target_rows=50000)

    # Limit maximum partitions created
    gpio.read('input.parquet').partition_by_a5('output/', auto=True, max_partitions=5000)
    ```

Auto-resolution probes your data's actual extent (see [How auto-resolution is chosen](#how-auto-resolution-is-chosen)) to pick the A5 resolution that targets your specified rows per partition, while respecting the `--max-partitions` constraint.

## By Quadkey Cells

Partition by Bing Maps quadkey tiles:

=== "CLI"

    <!-- doctest: menu -->
    ```bash
    # Auto-calculate optimal resolution for ~100K rows per partition
    gpio partition quadkey input.parquet output/ --auto

    # Auto with custom target partition size
    gpio partition quadkey input.parquet output/ --auto --target-rows 500000

    # Preview with auto-resolution
    gpio partition quadkey input.parquet --auto --preview

    # Hive-style (quadkey column included by default)
    gpio partition quadkey input.parquet output/ --auto --hive
    ```

    <!-- doctest: skip="the 766-row sample is too small to partition meaningfully" -->
    ```bash
    # Partition at specific resolutions (column at 13, partition at 9)
    gpio partition quadkey input.parquet output/ --resolution 13 --partition-resolution 9

    # Keep quadkey column in output files
    gpio partition quadkey input.parquet output/ --resolution 13 --partition-resolution 9 --keep-quadkey-column
    ```

=== "Python"

    <!-- doctest: skip="the 766-row sample is too small to partition meaningfully" -->
    ```python
    import geoparquet_io as gpio

    # Both resolutions are required -- or ask gpio to size them with auto=True.
    gpio.read('input.parquet').partition_by_quadkey(
        'output/', resolution=13, partition_resolution=8
    )

    # Let gpio pick both from the data
    gpio.read('input.parquet').partition_by_quadkey('output/', auto=True)

    # With options
    gpio.read('input.parquet').partition_by_quadkey(
        'output/',
        resolution=13,
        partition_resolution=10,
        compression='ZSTD',
        overwrite=True
    )
    ```

**Column behavior:**

- Non-Hive: Quadkey column excluded by default (redundant with path)
- Hive: Quadkey column included by default
- Use `--keep-quadkey-column` to explicitly keep

The quadkey column is created at `--resolution` (for full precision) but partitions are created using the first `--partition-resolution` characters, allowing coarser partitioning while retaining full precision in the column.

### Auto-Resolution for Quadkey

Use `--auto` to let gpio calculate the optimal quadkey zoom level:

=== "CLI"

    ```bash
    # Auto-select zoom level for ~100k rows per partition (default)
    gpio partition quadkey input.parquet output/ --auto

    # Target 50k rows per partition
    gpio partition quadkey input.parquet output/ --auto --target-rows 50000

    # Limit maximum partitions created
    gpio partition quadkey input.parquet output/ --auto --max-partitions 5000

    # Preview auto-selected partitions
    gpio partition quadkey input.parquet --auto --preview
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Auto-select zoom level for ~100k rows per partition (default)
    gpio.read('input.parquet').partition_by_quadkey('output/', auto=True)

    # Target 50k rows per partition
    gpio.read('input.parquet').partition_by_quadkey(
        'output/', auto=True, target_rows=50000
    )

    # Limit maximum partitions created
    gpio.read('input.parquet').partition_by_quadkey(
        'output/', auto=True, max_partitions=5000
    )
    ```

Auto-resolution probes your data's actual extent (see [How auto-resolution is chosen](#how-auto-resolution-is-chosen)) to pick the quadkey zoom level that targets your specified rows per partition, while respecting the `--max-partitions` constraint.

## By KD-Tree

Partition by balanced spatial partitions:

=== "CLI"

    ```bash
    # Preview auto-selected partitions
    gpio partition kdtree input.parquet --preview
    ```

    ```bash
    # Auto-partition (default: ~120k rows each; small inputs get few partitions)
    gpio partition kdtree input.parquet output/ --force

    # Hive-style with progress tracking
    gpio partition kdtree input.parquet output/ --hive --verbose --force
    ```

    <!-- doctest: skip="766 rows cannot fill 32 or 16 explicit partitions" -->
    ```bash
    # Explicit partition count (must be power of 2)
    gpio partition kdtree input.parquet output/ --partitions 32

    # Exact computation (deterministic)
    gpio partition kdtree input.parquet output/ --partitions 16 --exact
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Auto mode: size the tree from the row count, like the bare CLI call
    gpio.read('input.parquet').partition_by_kdtree('output/', auto=True, overwrite=True)

    # A different target, still auto
    gpio.read('input.parquet').partition_by_kdtree(
        'output/', auto=True, target_rows=50000, overwrite=True
    )
    ```

    <!-- doctest: skip="766 rows cannot fill 64 or 32 explicit partitions" -->
    ```python
    import geoparquet_io as gpio

    # 64 partitions (2^6)
    gpio.read('input.parquet').partition_by_kdtree('output/', iterations=6)

    # With options
    gpio.read('input.parquet').partition_by_kdtree(
        'output/',
        iterations=5,  # 32 partitions
        hive=True,
        overwrite=True
    )
    ```

    !!! note "CLI vs Python API"
        The Python API uses `iterations` which creates 2^iterations partitions (power-of-two semantics).
        The CLI uses `--partitions N` to specify an absolute count directly. For example:

        - Python `iterations=6` → 64 partitions (2^6)
        - CLI `--partitions 64` → 64 partitions

        Auto mode is spelled `auto=True` with `target_rows=` on the Python side
        and `--auto N` on the CLI; both mean "size the tree so partitions hold
        about N rows", and both default to 120,000.

!!! warning "Breaking change for the Python API: the implicit `iterations=9` is gone"
    `Table.partition_by_kdtree()` and `ops.partition_by_kdtree()` used to fall
    back to `iterations=9` when they had to build the tree themselves — 512
    partitions whatever the input — where the bare CLI call sizes the tree from
    the row count. Such a call now raises `InvalidParameterError`. Pass
    `auto=True` to get what the CLI gives you, or pass `iterations=9` to keep
    the output you had.

    A table that already carries the `kdtree_cell` column (say from
    `add_kdtree()`) is unaffected: it needs no sizing parameter, and never fell
    back to 512 partitions — the existing cells drive the partition, before and
    after this change.

**Column behavior:**
- Similar to H3: excluded by default, included for Hive
- Use `--keep-kdtree-column` to explicitly keep

If KD-tree column doesn't exist, it's automatically added.

## By Admin Boundaries

Split by administrative boundaries via spatial join with remote datasets:

### How It Works

This command performs **two operations**:

1. **Spatial Join**: Queries remote admin boundaries using spatial extent filtering, then spatially joins them with your data
2. **Partition**: Splits the enriched data by administrative levels

### Quick Start

=== "CLI"

    <!-- doctest: network -->
    ```bash
    # Preview GAUL partitions by continent
    gpio partition admin input.parquet --dataset gaul --levels continent --preview

    # Partition by continent
    gpio partition admin input.parquet output/ --dataset gaul --levels continent

    # Hive-style partitioning
    gpio partition admin input.parquet output/ --dataset gaul --levels continent --hive
    ```

=== "Python"

    <!-- doctest: network -->
    ```python
    import geoparquet_io as gpio

    # Partition by country using GAUL dataset
    gpio.read('input.parquet').partition_by_admin(
        'output/',
        dataset='gaul',
        levels=['country']
    )

    # Hive-style partitioning
    gpio.read('input.parquet').partition_by_admin(
        'output/',
        dataset='gaul',
        levels=['country'],
        hive=True
    )
    ```

### Multi-Level Hierarchical Partitioning

Partition by multiple administrative levels:

=== "CLI"

    <!-- doctest: network -->
    ```bash
    # Hierarchical: continent → country
    gpio partition admin input.parquet output/ --dataset gaul --levels continent,country

    # All GAUL levels: continent → country → department
    gpio partition admin input.parquet output/ --dataset gaul --levels continent,country,department

    # Hive-style multi-level (creates continent=Africa/country=Kenya/department=Accra/)
    gpio partition admin input.parquet output/ --dataset gaul \
        --levels continent,country,department --hive

    # Overture Maps by country and region
    gpio partition admin input.parquet output/ --dataset overture --levels country,region
    ```

=== "Python"

    <!-- doctest: network -->
    ```python
    import geoparquet_io as gpio

    # Multi-level hierarchical
    gpio.read('input.parquet').partition_by_admin(
        'output/',
        dataset='gaul',
        levels=['continent', 'country', 'department'],
        hive=True
    )

    # Using Overture Maps dataset
    gpio.read('input.parquet').partition_by_admin(
        'output/',
        dataset='overture',
        levels=['country', 'region']
    )
    ```

### Vecorel-compliant partitions

Use `--vecorel` to write [Vecorel](https://vecorel.org/)-compliant admin columns
into each partition. This forces the Overture dataset with `country,region`
levels, names the columns `admin:country_code` and `admin:subdivision_code`,
marks `admin:country_code` non-nullable, and writes the Vecorel collection
schema metadata. Unlike the default mode, the admin columns are kept in the
output files (not just encoded in the folder names).

=== "CLI"

    <!-- doctest: network -->
    ```bash
    # Vecorel partitions (forces Overture country,region)
    gpio partition admin input.parquet output/ --vecorel

    # Combine with Hive-style layout
    gpio partition admin input.parquet output/ --vecorel --hive
    ```

=== "Python"

    <!-- doctest: network -->
    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').partition_by_admin(
        'output/',
        vecorel=True
    )
    ```

### Datasets

--8<-- "_includes/admin-datasets.md"

## Common Options

All partition commands support:

--8<-- "_includes/common-cli-options.md"

```text
--preview-limit 15     # Number of partitions to show (default: 15)
--force                # Override analysis warnings
--skip-analysis        # Skip analysis (performance-sensitive cases)
--prefix PREFIX        # Custom filename prefix (e.g., 'fields' → fields_USA.parquet)
```

## Output Structures

### Standard Partitioning

```
output/
├── partition_value_1.parquet
├── partition_value_2.parquet
└── partition_value_3.parquet
```

### Hive-Style Partitioning

```
output/
├── column=value1/
│   └── data.parquet
├── column=value2/
│   └── data.parquet
└── column=value3/
    └── data.parquet
```

### Custom Filename Prefix

Add `--prefix NAME` to prepend a custom prefix to partition filenames:

<!-- doctest: network -->
```bash
# Standard: fields_USA.parquet, fields_Kenya.parquet
gpio partition admin input.parquet output/ --dataset gaul --levels country --prefix fields

# Hive: country=USA/fields_USA.parquet, country=Kenya/fields_Kenya.parquet
gpio partition admin input.parquet output/ --dataset gaul --levels country --prefix fields --hive
```

## Partition Analysis

Before creating files, analysis shows:

- Total partition count
- Rows per partition (min/max/avg/median)
- Distribution statistics
- Recommendations and warnings

**Warnings trigger for:**
- Very uneven distributions
- Too many small partitions
- Single-row partitions

Use `--force` to override warnings or `--skip-analysis` for performance.

## Preview Workflow

### With Auto-Resolution

```bash
# 1. Preview with auto-resolution
gpio partition h3 large.parquet --auto --preview

# 2. Adjust target rows if needed
gpio partition h3 large.parquet --auto --target-rows 50000 --preview

# 3. Execute when satisfied
gpio partition h3 large.parquet output/ --auto --target-rows 50000
```

### With Manual Resolution

<!-- doctest: menu -->
```bash
# 1. Preview to understand partitioning
gpio partition h3 large.parquet --resolution 7 --preview

# 2. Adjust resolution if needed
gpio partition h3 large.parquet --resolution 8 --preview
```

<!-- doctest: skip="the 766-row sample is too small to partition meaningfully" -->
```bash
# 3. Execute when satisfied
gpio partition h3 large.parquet output/ --resolution 8
```

## Sub-Partitioning Large Files

After partitioning by admin boundaries or string columns, some files may still be too large. Use `--min-size` with directory input to sub-partition only the oversized files:

<!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
```bash
# Sub-partition files >100MB with H3
gpio partition h3 by_country/ --min-size 100MB --resolution 7 --in-place
```

See [Sub-Partitioning Large Files](sub-partitioning.md) for details.

## Function-Style API

Every partition subcommand also has an `ops` function, for callers holding a plain
PyArrow table rather than the fluent `Table` wrapper. Each takes the table plus an
output directory and accepts the same keywords as the CLI command it mirrors --
including `auto`, `target_rows` and `max_partitions` on the four spatial-index
schemes:

| CLI command | `ops` function |
|-------------|----------------|
| `gpio partition h3` | `ops.partition_by_h3(table, output_dir, resolution=..., auto=...)` |
| `gpio partition a5` | `ops.partition_by_a5(table, output_dir, resolution=..., auto=...)` |
| `gpio partition s2` | `ops.partition_by_s2(table, output_dir, level=..., auto=...)` |
| `gpio partition quadkey` | `ops.partition_by_quadkey(table, output_dir, resolution=..., partition_resolution=..., auto=...)` |
| `gpio partition kdtree` | `ops.partition_by_kdtree(table, output_dir, iterations=..., auto=...)` |
| `gpio partition string` | `ops.partition_by_string(table, output_dir, column, chars=...)` |
| `gpio partition admin` | `ops.partition_by_admin(table, output_dir, dataset=..., levels=...)` |

Every one of them returns the same dict -- `{'output_dir': str, 'file_count': int, 'hive': bool}` -- as does the matching `Table.partition_by_*` method.

<!-- doctest: skip="the 766-row sample is too small to partition meaningfully, has no 'region' column for partition_by_string, and partition_by_admin would download a boundaries dataset" -->
```python
import pyarrow.parquet as pq
from geoparquet_io.api import ops

table = pq.read_table('input.parquet')

# An explicit resolution, or auto=True -- neither front end guesses one for you
stats = ops.partition_by_h3(table, 'output/', resolution=7)
stats = ops.partition_by_a5(table, 'output/', auto=True, target_rows=50000)

# Non-spatial schemes return the same dict
stats = ops.partition_by_string(table, 'output/', column='region', hive=True)
stats = ops.partition_by_admin(table, 'output/', levels=['country'])

print(f"Created {stats['file_count']} files")
```

## See Also

- [CLI Reference](../cli/overview.md) - Full command reference
- [add command](add.md) - Add spatial indices before partitioning
