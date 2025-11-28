# Partitioning Files

The `partition` commands split GeoParquet files into separate files based on column values or spatial indices.

**Smart Analysis**: All partition commands automatically analyze your strategy before execution, providing statistics and recommendations.

## By String Column

Partition by string column values or prefixes:

```bash
# Preview partitions
gpio partition string input.parquet --column region --preview

# Partition by full column values
gpio partition string input.parquet output/ --column category

# Partition by first 2 characters
gpio partition string input.parquet output/ --column mgrs_code --chars 2

# Hive-style partitioning
gpio partition string input.parquet output/ --column region --hive

# To cloud storage
gpio partition string s3://bucket/input.parquet s3://bucket/output/ --column region --profile prod
```

## By H3 Cells

Partition by H3 hexagonal cells:

```bash
# Preview at resolution 7 (~5km² cells)
gpio partition h3 input.parquet --resolution 7 --preview

# Partition at default resolution 9
gpio partition h3 input.parquet output/

# Keep H3 column in output files
gpio partition h3 input.parquet output/ --keep-h3-column

# Hive-style (H3 column included by default)
gpio partition h3 input.parquet output/ --resolution 8 --hive
```

**Column behavior:**
- Non-Hive: H3 column excluded by default (redundant with path)
- Hive: H3 column included by default
- Use `--keep-h3-column` to explicitly keep

If H3 column doesn't exist, it's automatically added.

## By KD-Tree

Partition by balanced spatial partitions:

```bash
# Auto-partition (default: ~120k rows each)
gpio partition kdtree input.parquet output/

# Preview auto-selected partitions
gpio partition kdtree input.parquet --preview

# Explicit partition count (must be power of 2)
gpio partition kdtree input.parquet output/ --partitions 32

# Exact computation (deterministic)
gpio partition kdtree input.parquet output/ --partitions 16 --exact

# Hive-style with progress tracking
gpio partition kdtree input.parquet output/ --hive --verbose
```

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

```bash
# Preview GAUL partitions by continent
gpio partition admin input.parquet --dataset gaul --levels continent --preview

# Partition by continent
gpio partition admin input.parquet output/ --dataset gaul --levels continent

# Hive-style partitioning
gpio partition admin input.parquet output/ --dataset gaul --levels continent --hive
```

### Multi-Level Hierarchical Partitioning

Partition by multiple administrative levels:

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

### Datasets

--8<-- "_includes/admin-datasets.md"

## Common Options

All partition commands support:

--8<-- "_includes/common-cli-options.md"

```bash
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

```bash
# 1. Preview to understand partitioning
gpio partition h3 large.parquet --resolution 7 --preview

# 2. Adjust resolution if needed
gpio partition h3 large.parquet --resolution 8 --preview

# 3. Execute when satisfied
gpio partition h3 large.parquet output/ --resolution 8
```

## See Also

- [CLI Reference: partition](../cli/partition.md)
- [add command](add.md) - Add spatial indices before partitioning
