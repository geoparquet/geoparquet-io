# Streaming DuckDB Write with Footer Rewrite

## Problem

The current write flow loads entire datasets into memory as Arrow tables before writing:

```
DuckDB query → fetch_arrow_table() → apply_metadata() → pq.write_table()
                    ↑
            Everything in memory
```

This causes memory pressure and performance issues for large datasets.

## Solution

Use DuckDB's native COPY TO for streaming writes, then rewrite just the Parquet footer to add GeoParquet metadata:

```
DuckDB query → COPY TO parquet → rewrite_footer_with_geo_metadata()
                    ↑                         ↑
            Streaming to disk         Only footer in memory (~KB)
```

## Key Design Decisions

### Metadata Preservation

Preserve metadata from input file by default. Only recompute when operations change bounds/types:

| Operation | Bbox | Geometry Types |
|-----------|------|----------------|
| extract (columns only) | Preserve | Preserve |
| extract (bbox filter) | Recalculate | Preserve |
| extract (rows filter) | Recalculate | Preserve |
| add bbox/h3/etc | Preserve | Preserve |
| sort hilbert | Preserve | Preserve |
| reproject | Recalculate | Preserve |
| partition | Recalculate (per partition) | Preserve |
| convert (from other format) | Calculate | Calculate |

### Metadata Computation via SQL

When recalculation is needed, compute before COPY TO:

```sql
-- Bbox
SELECT
  MIN(ST_XMin(geometry)) as xmin,
  MIN(ST_YMin(geometry)) as ymin,
  MAX(ST_XMax(geometry)) as xmax,
  MAX(ST_YMax(geometry)) as ymax
FROM (query)

-- Geometry types
SELECT DISTINCT ST_GeometryType(geometry) as geom_type
FROM (query)
```

### Footer Rewrite with fastparquet

Use `fastparquet.update_file_custom_metadata()` to add geo metadata without rewriting data:

```python
from fastparquet import update_file_custom_metadata

update_file_custom_metadata(
    path="output.parquet",
    custom_metadata={"geo": json.dumps(geo_metadata)}
)
```

**Limitation:** Local files only. Remote outputs require temp file + upload.

### Version-Specific Behavior

| Version | Footer Rewrite Needed | Bbox in Metadata |
|---------|----------------------|------------------|
| 1.0 / 1.1 | Yes | Yes |
| 2.0 | Yes | No (native stats) |
| parquet-geo-only (default CRS) | Could skip | No |
| parquet-geo-only (non-default CRS) | Yes (for CRS) | No |

## Implementation

### New Function: `write_geoparquet_via_duckdb()`

Location: `core/common.py`

```python
def write_geoparquet_via_duckdb(
    con: duckdb.DuckDBPyConnection,
    query: str,
    output_path: str,
    original_metadata: dict,
    geoparquet_version: str,
    geometry_column: str = "geometry",
    preserve_bbox: bool = True,
    preserve_geometry_types: bool = True,
    input_crs: dict = None,
    compression: str = "zstd",
    ...
):
    # 1. Prepare geo metadata (preserve or compute)
    geo_meta = _prepare_geo_metadata(
        original_metadata,
        preserve_bbox,
        preserve_geometry_types
    )

    # 2. If recalculation needed, run metadata query
    if not preserve_bbox or not preserve_geometry_types:
        computed = _compute_metadata_via_sql(con, query, geometry_column)
        geo_meta.update(computed)

    # 3. Handle remote output
    if is_remote_url(output_path):
        final_path = tempfile.mktemp(suffix=".parquet")
        upload_after = True
    else:
        final_path = output_path
        upload_after = False

    # 4. DuckDB COPY TO (streaming, low memory)
    copy_query = f"COPY ({query}) TO '{final_path}' (FORMAT PARQUET, COMPRESSION {compression})"
    con.execute(copy_query)

    # 5. Rewrite footer with geo metadata
    update_file_custom_metadata(
        path=final_path,
        custom_metadata={"geo": json.dumps(geo_meta)}
    )

    # 6. Upload if remote
    if upload_after:
        upload_file(final_path, output_path)
        os.unlink(final_path)
```

### Integration Point

Modify `write_parquet_with_metadata()` to support both paths:

```python
def write_parquet_with_metadata(
    con, query, output_path, original_metadata,
    geometry_column, geoparquet_version,
    use_streaming=False,  # New flag
    preserve_bbox=True,
    preserve_geometry_types=True,
    ...
):
    if use_streaming:
        return write_geoparquet_via_duckdb(...)
    else:
        return write_geoparquet_via_arrow(...)
```

### Command Integration

| Command | use_streaming | preserve_bbox | preserve_geometry_types |
|---------|--------------|---------------|------------------------|
| extract --columns | True | True | True |
| extract --bbox | True | False | True |
| extract --where | True | False | True |
| sort hilbert | True | True | True |
| add bbox | True | True | True |
| add h3 | True | True | True |
| reproject | True | False | True |
| convert | True | False | False |

## Error Handling

1. **COPY fails mid-write:** Delete partial file
2. **Footer rewrite fails:** Fall back to Arrow path or error with clear message
3. **Disk full:** Clean up partial file

**Atomicity:** Write to temp file, then rename to final path.

## New Dependency

Add `fastparquet` to project dependencies.

## Testing

### Unit Tests
- `test_footer_rewrite_adds_geo_metadata()` - Verify geo key added
- `test_preserve_bbox_from_input()` - Bbox passed through unchanged
- `test_recalculate_bbox_after_filter()` - Bbox updated after spatial filter
- `test_streaming_matches_arrow_output()` - Compare outputs of both paths

### Integration Tests
- Large file (>available RAM) successfully writes via streaming
- Remote output path works via temp file + upload
- All GeoParquet versions produce valid output

### Test Data
- Large file: `/Users/cholmes/geodata/parquet-test-data/japan.parquet`

### Performance Benchmarks
- Memory high-water mark: streaming vs Arrow
- Wall-clock time comparison

## Files to Modify

1. `core/common.py` - Add `write_geoparquet_via_duckdb()` and helper functions
2. `core/extract.py` - Add streaming flag based on operation
3. `core/convert.py` - Add streaming flag
4. `core/hilbert_order.py` - Add streaming flag
5. `core/add_*.py` - Add streaming flags
6. `pyproject.toml` - Add fastparquet dependency
