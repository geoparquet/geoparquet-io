# Adding Spatial Indices

The `add` commands enhance GeoParquet files with spatial indices and metadata.

## Bounding Boxes

Add precomputed bounding boxes for faster spatial queries:

```bash
gpio add bbox input.parquet output.parquet
```

Creates a struct column with `{xmin, ymin, xmax, ymax}` for each feature and adds proper bbox covering metadata.

**Options:**

```bash
# Custom column name
gpio add bbox input.parquet output.parquet --bbox-name bounds

# With compression settings
gpio add bbox input.parquet output.parquet --compression ZSTD --compression-level 15

# Dry run (preview SQL)
gpio add bbox input.parquet output.parquet --dry-run
```

## H3 Hexagonal Cells

Add [H3](https://h3geo.org/) hexagonal cell IDs based on geometry centroids:

```bash
gpio add h3 input.parquet output.parquet --resolution 9
```

**Resolution guide:**

- Resolution 7: ~5 km² cells
- Resolution 9: ~105 m² cells (default)
- Resolution 11: ~2 m² cells
- Resolution 13: ~0.04 m² cells

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

```bash
# Auto-select partitions (default: ~120k rows each)
gpio add kdtree input.parquet output.parquet

# Explicit partition count (must be power of 2)
gpio add kdtree input.parquet output.parquet --partitions 32

# Exact mode (deterministic but slower)
gpio add kdtree input.parquet output.parquet --partitions 16 --exact
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

Add country ISO codes via spatial join:

```bash
# Use default countries dataset
gpio add admin-divisions buildings.parquet output.parquet

# Use custom countries file
gpio add admin-divisions buildings.parquet output.parquet \
  --countries-file my_countries.parquet

# Preview SQL
gpio add admin-divisions buildings.parquet output.parquet --dry-run
```

Uses the [administrative division extension](https://github.com/fiboa/administrative-division-extension) from [fiboa](https://github.com/fiboa).

Default countries source: [source.coop admin boundaries](https://data.source.coop/cholmes/admin-boundaries/countries.parquet)

## Common Options

All `add` commands support:

```bash
# Compression settings
--compression [ZSTD|GZIP|BROTLI|LZ4|SNAPPY|UNCOMPRESSED]
--compression-level [1-22]

# Row group sizing
--row-group-size [exact row count]
--row-group-size-mb [target size like '256MB' or '1GB']

# Workflow options
--dry-run          # Preview SQL without executing
--verbose          # Detailed output
--add-bbox         # Auto-add bbox if missing (some commands)
```

## See Also

- [CLI Reference: add](../cli/add.md)
- [partition command](partition.md)
- [sort command](sort.md)
