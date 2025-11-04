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

Two remote admin boundary datasets are supported:

| Dataset | Standard | Columns Added | Description |
|---------|----------|---------------|-------------|
| `gaul` (default) | GAUL naming + ISO 3166-1 alpha-3 | `admin:continent`, `admin:country`, `admin:department` | FAO Global Administrative Unit Layers (GAUL) L2 - worldwide coverage with standardized naming |
| `overture` | **Vecorel compliant** (ISO 3166-1/2) | `admin:country_code`, `admin:subdivision_code` | Overture Maps Divisions with ISO 3166 codes (219 countries, 3,544 regions) - [docs](https://docs.overturemaps.org/guides/divisions/) |

### Vecorel Compliance (Overture Dataset Only)

The `overture` dataset follows the [Vecorel administrative division extension](https://vecorel.org/administrative-division-extension/v0.1.0/schema.yaml) specification with standardized ISO codes:

- **`admin:country_code`** (REQUIRED): ISO 3166-1 alpha-2 country code (e.g., "US", "AR", "DE")
- **`admin:subdivision_code`**: ISO 3166-2 subdivision code WITHOUT country prefix (e.g., "CA" not "US-CA")

The tool automatically transforms Overture's native region codes (e.g., "US-CA") to strip the country prefix for Vecorel compliance.

**Note:** The GAUL dataset uses FAO's standardized naming system but is NOT Vecorel compliant:
- Has ISO 3166-1 alpha-3 codes (e.g., "TZA"), but Vecorel requires alpha-2 (e.g., "TZ")
- Uses GAUL's standardized naming for subnational units, not ISO 3166-2 codes
- Columns: `admin:continent` (continent name), `admin:country` (GAUL country name), `admin:department` (GAUL L2 name)

### Notes

- **Overture dataset**: Vecorel compliant with ISO 3166-1 alpha-2 and ISO 3166-2 codes
- **GAUL dataset**: FAO standardized naming system - [source.coop GAUL L2](https://data.source.coop/nlebovits/gaul-l2-admin/)
- Performs spatial intersection to assign admin divisions based on geometry
- Requires internet connection to access remote datasets
- Uses spatial extent filtering and bbox columns for optimization

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
