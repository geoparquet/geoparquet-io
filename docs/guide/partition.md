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

Split by country code or other administrative column:

```bash
# Preview partitions
gpio partition admin input.parquet --preview

# Partition by default column (admin:country_code)
gpio partition admin input.parquet output/

# Custom admin column
gpio partition admin input.parquet output/ --column iso_code

# Hive-style
gpio partition admin input.parquet output/ --hive
```

## Common Options

All partition commands support:

```bash
--preview              # Analyze and preview without creating files
--preview-limit 15     # Number of partitions to show (default: 15)
--hive                 # Use Hive-style partitioning (column=value/)
--overwrite            # Overwrite existing partition files
--force                # Override analysis warnings
--skip-analysis        # Skip analysis (performance-sensitive cases)
--verbose              # Detailed output
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
