# Core Functions Reference

Detailed API reference for core functions. For full parameter documentation, see the docstrings in the source code.

## Common Utilities (`geoparquet_io.core.common`)

- `safe_file_url(file_path, verbose)` - Handle both local and remote files
- `get_parquet_metadata(parquet_file, verbose)` - Get Parquet file metadata
- `find_primary_geometry_column(parquet_file, verbose)` - Find primary geometry column
- `add_computed_column(...)` - Add a computed column using SQL expression
- `write_parquet_with_metadata(...)` - Write parquet with proper compression and metadata
- `get_dataset_bounds(parquet_file, geometry_column, verbose)` - Calculate dataset bounding box

## Adding Columns

### `add_bbox_column(input_parquet, output_parquet, bbox_name, ...)`

Add a bounding box struct column.

**Source**: `geoparquet_io.core.add_bbox_column`

### `add_h3_column(input_parquet, output_parquet, h3_column_name, h3_resolution, ...)`

Add H3 hexagonal cell IDs.

**Source**: `geoparquet_io.core.add_h3_column`

### `add_kdtree_column(input_parquet, output_parquet, kdtree_column_name, iterations, ...)`

Add KD-tree partition IDs.

**Source**: `geoparquet_io.core.add_kdtree_column`

### `add_country_codes(input_parquet, countries_file, output_parquet, ...)`

Add country ISO codes via spatial join.

**Source**: `geoparquet_io.core.add_country_codes`

## Spatial Operations

### `hilbert_order(input_parquet, output_parquet, geometry_column, ...)`

Sort by Hilbert space-filling curve.

**Source**: `geoparquet_io.core.hilbert_order`

### `add_bbox_metadata(parquet_file, verbose)`

Update bbox covering metadata.

**Source**: `geoparquet_io.core.add_bbox_metadata`

## Partitioning

### `partition_by_string(input_parquet, output_folder, column, chars, ...)`

Partition by string column values or prefixes.

**Source**: `geoparquet_io.core.partition_by_string`

### `partition_by_h3(input_parquet, output_folder, h3_column_name, resolution, ...)`

Partition by H3 hexagonal cells.

**Source**: `geoparquet_io.core.partition_by_h3`

### `partition_by_kdtree(input_parquet, output_folder, kdtree_column_name, iterations, ...)`

Partition by KD-tree cells.

**Source**: `geoparquet_io.core.partition_by_kdtree`

### `split_by_country(input_parquet, output_folder, column, ...)`

Split by country code or admin column.

**Source**: `geoparquet_io.core.split_by_country`

## Validation

### `check_all(parquet_file, verbose)`

Run all validation checks.

**Source**: `geoparquet_io.core.check_parquet_structure`

### `check_spatial_order(parquet_file, random_sample_size, limit_rows, verbose)`

Check if data is spatially ordered.

**Source**: `geoparquet_io.core.check_spatial_order`

## Source Code

For complete parameter details and implementation, see the [source code on GitHub](https://github.com/cholmes/geoparquet-io/tree/main/geoparquet_io/core).
