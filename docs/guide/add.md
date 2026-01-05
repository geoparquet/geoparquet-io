# Adding Spatial Indices

The `add` commands enhance GeoParquet files with spatial indices and metadata.

## Bounding Boxes

Add precomputed bounding boxes for faster spatial queries:

=== "CLI"

    ```bash
    gpio add bbox input.parquet output.parquet

    # Works with remote files
    gpio add bbox s3://bucket/input.parquet s3://bucket/output.parquet --profile prod
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').add_bbox().write('output.parquet')

    # Custom column name
    gpio.read('input.parquet').add_bbox(column_name='bounds').write('output.parquet')
    ```

Creates a struct column with `{xmin, ymin, xmax, ymax}` for each feature. Bbox covering metadata is automatically added to comply with GeoParquet 1.1 spec.

### Existing Bbox Detection

The command automatically checks for existing bbox columns:

- **If bbox exists with metadata**: Informs you and exits successfully (no action needed)
- **If bbox exists without metadata**: Suggests using `gpio add bbox-metadata` instead
- **Use `--force`**: Replace existing bbox column with a freshly computed one

```bash
# Check and skip if bbox already exists
gpio add bbox input.parquet output.parquet

# Force replace existing bbox
gpio add bbox input.parquet output.parquet --force
```

**Options:**

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

```bash
gpio add bbox-metadata myfile.parquet
```

This modifies the file in-place to add only the metadata, without creating a new file.

## H3 Hexagonal Cells

Add [H3](https://h3geo.org/) hexagonal cell IDs based on geometry centroids:

=== "CLI"

    ```bash
    gpio add h3 input.parquet output.parquet --resolution 9

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

```bash
# Custom column name
gpio add h3 input.parquet output.parquet --h3-name h3_index

# Different resolution
gpio add h3 input.parquet output.parquet --resolution 13

# With row group sizing
gpio add h3 input.parquet output.parquet --row-group-size-mb 256MB
```

## KD-Tree Partitions

Add balanced spatial partition IDs using KD-tree:

=== "CLI"

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

    # Add kdtree column with default settings (9 iterations = 512 partitions)
    gpio.read('input.parquet').add_kdtree().write('output.parquet')

    # Custom column name and iterations
    gpio.read('input.parquet').add_kdtree(
        column_name='partition_id',
        iterations=5  # 2^5 = 32 partitions
    ).write('output.parquet')
    ```

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

```bash
# Custom target rows per partition
gpio add kdtree input.parquet output.parquet --auto 200000

# Custom sample size for approximate mode
gpio add kdtree input.parquet output.parquet --approx 200000

# Track progress
gpio add kdtree input.parquet output.parquet --verbose
```

## Administrative Divisions

!!! info "CLI Only"
    Administrative divisions is currently only available via the CLI. See [issue #151](https://github.com/cholmes/geoparquet-io/issues/151) for Python API roadmap.

Add administrative division columns via spatial join with remote boundaries datasets:

### How It Works

Performs spatial intersection between your data and remote admin boundaries to add admin division columns. Uses efficient spatial extent filtering to query only relevant boundaries from remote datasets.

### Quick Start

```bash
# Add all GAUL levels (continent, country, department)
gpio add admin-divisions input.parquet output.parquet --dataset gaul

# Preview SQL before execution
gpio add admin-divisions input.parquet output.parquet --dataset gaul --dry-run
```

### Multi-Level Admin Divisions

Add multiple hierarchical administrative levels:

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

### Datasets

--8<-- "_includes/admin-datasets.md"

## Common Options

All `add` commands support:

--8<-- "_includes/common-cli-options.md"

```bash
--add-bbox         # Auto-add bbox if missing (some commands)
```

## See Also

- [CLI Reference: add](../cli/add.md)
- [partition command](partition.md)
- [sort command](sort.md)
