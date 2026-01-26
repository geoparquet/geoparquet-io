# Streaming Execution Research

## Problem Statement

The current eager Table API loads entire datasets into memory, making it unsuitable for large files. A 1.6GB file (13M rows) requires 5.6GB of peak memory for a simple `add_bbox() -> sort_hilbert() -> write()` chain.

**Goal**: Enable processing of datasets larger than available RAM while maintaining reasonable performance.

## Current State

### Eager API (main branch)
- Loads entire file into PyArrow Table
- Each operation materializes full result
- Fast but memory-intensive
- No memory control options

### Lazy API (feat/lazy-execution branch)
- Builds DuckDB SQL queries, executes at terminal operation
- Memory control via `write_memory` parameter
- Issues discovered during testing:
  - `sort_hilbert()` was broken (fixed: ST_Extent not an aggregate function)
  - Slower than eager for most operations
  - Memory limits not fully respected (~1.1GB minimum regardless of setting)

## Benchmark Results

### Test Files
| File | Rows | Size |
|------|------|------|
| chips_austria.parquet | 6,686 | 0.2 MB |
| si.parquet | 830,856 | 175.8 MB |
| 2017612633061982208.parquet | 13,076,358 | 1.6 GB |

### Full Dataset 3-Op Chain: `add_bbox() -> sort_hilbert() -> write()`

| Method | Time | Peak Memory | Notes |
|--------|------|-------------|-------|
| Eager | 32s | 5.6 GB | Fast, memory-hungry |
| Lazy (512MB limit) | 124s | 1.4 GB | Slow, moderate memory |
| Lazy (128MB limit) | 131s | 1.1 GB | Slow, lower memory |
| DuckDB direct (512MB) | 14s | 1.7 GB | Fast, moderate memory |
| DuckDB direct (256MB) | 31s | 1.2 GB | Moderate, lower memory |
| **Streaming prototype** | **41.5s** | **1.6 GB** | Balanced |

### Streaming add_bbox Only (no sort)

| Method | Time | Peak Memory |
|--------|------|-------------|
| Eager | ~25s | 5.6 GB |
| **Streaming** | **20.5s** | **973 MB** |

**Key finding**: Streaming is faster AND uses 5.8x less memory for operations without global sorting.

### Operations with LIMIT (large file)

| Limit | Eager | Lazy | Winner |
|-------|-------|------|--------|
| 1,000 | 1.90s | 0.56s | Lazy 3.4x faster |
| 10,000 | 0.92s | 0.53s | Lazy 1.7x faster |
| 100,000 | 1.20s | 0.72s | Lazy 1.7x faster |

**Key finding**: LIMIT operations benefit from pushdown in lazy/streaming approaches.

## Proposed Solution: Arrow Streaming

Replace the lazy DuckDB SQL approach with Arrow RecordBatch streaming integrated into the main Table API.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Streaming Pipeline                       │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌───────┐ │
│  │ Parquet  │───▶│  Batch   │───▶│  Batch   │───▶│ Write │ │
│  │  Reader  │    │ Transform│    │ Transform│    │ Batch │ │
│  └──────────┘    └──────────┘    └──────────┘    └───────┘ │
│       │                                              │       │
│       │         iter_batches()                       │       │
│       └──────────────────────────────────────────────┘       │
│                    100K rows at a time                       │
└─────────────────────────────────────────────────────────────┘
```

### Operation Categories

**1. Streamable (single-pass, no global state)**
- `add_bbox()` - compute per-row bbox
- `extract(columns=...)` - column selection
- `extract(where=...)` - row filtering
- `extract(limit=N)` - limit with early termination

**2. Two-pass (requires global stats first)**
- `sort_hilbert()` - needs global extent for BOX_2D
- `sort_column()` - needs full sort (external sort via DuckDB)

**3. Implementation approach for two-pass**
```
Pass 1: Stream batches, compute global stats (extent)
        Memory: O(1) - just accumulating min/max

Pass 2: Stream batches, add hilbert index column
        Write to temp file with __hilbert_idx
        Memory: O(batch_size)

Pass 3: DuckDB external sort with memory limit
        COPY (SELECT * EXCLUDE(__hilbert_idx)
              FROM temp ORDER BY __hilbert_idx) TO output
        Memory: O(sort_memory setting)
```

### API Design

```python
class Table:
    def write(
        self,
        path: str,
        *,
        streaming: bool = False,
        batch_size: int = 100_000,
        sort_memory: str = "512MB"
    ):
        """Write table to parquet.

        Args:
            streaming: Use streaming writes for lower memory usage.
                      Recommended for files larger than available RAM.
            batch_size: Rows per batch when streaming (default 100K)
            sort_memory: DuckDB memory limit for sort operations
        """

# Usage
gpio.read("large.parquet") \
    .add_bbox() \
    .sort_hilbert() \
    .write("output.parquet", streaming=True, sort_memory="512MB")
```

### Alternative: Auto-detect streaming

```python
def write(self, path: str, *, memory_limit: str = None):
    """Auto-select streaming vs eager based on memory_limit.

    If memory_limit is set, use streaming approach.
    Otherwise, use eager (current behavior).
    """
```

## Implementation Considerations

### 1. Geometry Column Handling

When reading Arrow batches directly (not via DuckDB's read_parquet), geometry columns come through as BLOB, not GEOMETRY. Must use `ST_GeomFromWKB()`:

```sql
-- From Arrow batch (geometry is BLOB)
SELECT ST_XMin(ST_GeomFromWKB(geometry)) as bbox_xmin FROM batch

-- From read_parquet (geometry is GEOMETRY)
SELECT ST_XMin(geometry) as bbox_xmin FROM read_parquet('file.parquet')
```

### 2. GeoParquet Metadata

Streaming writes must preserve GeoParquet metadata:
- Copy geo metadata from input schema
- Update bbox in metadata after processing
- Handle geometry column encoding

### 3. Minimum Memory for Sorting

DuckDB external sort has a floor:
- 256MB: Works but slower
- 128MB: Fails with OOM
- 512MB: Good balance of speed and memory

For 13M rows, ~1GB peak memory is the practical minimum when sorting.

### 4. Progress Reporting

Streaming enables progress reporting:
```python
for i, batch in enumerate(pf.iter_batches(batch_size=100_000)):
    progress = (i * batch_size) / total_rows
    # Report progress
```

### 5. Error Handling

Need to handle:
- Partial writes (cleanup on failure)
- DuckDB connection management per batch
- Temp file cleanup

## Files Modified

The streaming approach would primarily modify:

1. `geoparquet_io/api/table.py`
   - Add `streaming` parameter to `write()`
   - Add `_write_streaming()` method
   - Add streaming variants for transform methods

2. `geoparquet_io/core/common.py`
   - Add `StreamingWriter` class
   - Add batch processing utilities

3. `geoparquet_io/core/streaming.py` (new)
   - Streaming implementations of operations
   - Two-pass processing for sort operations

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Slower than eager for small files | Default to eager, streaming opt-in |
| Temp file disk space for sorting | Document requirement, allow custom temp dir |
| DuckDB connection overhead per batch | Reuse connection where possible |
| GeoParquet metadata corruption | Comprehensive tests for metadata preservation |

## Recommendation

Implement Arrow streaming as an opt-in feature in the main Table API:

1. **Phase 1**: Add `streaming=True` parameter to `write()`
   - Implement streaming for `add_bbox()` first (no global state)
   - Test extensively with large files

2. **Phase 2**: Add two-pass streaming for `sort_hilbert()`
   - Compute extent in pass 1
   - Add hilbert index + external sort in passes 2-3

3. **Phase 3**: Consider deprecating lazy API
   - Streaming approach is simpler and performs better
   - Lazy API has architectural issues (memory limits not respected)

## Appendix: Prototype Code

### Streaming add_bbox

```python
def streaming_add_bbox(input_path: str, output_path: str,
                       geom_col: str = 'geometry',
                       batch_size: int = 100_000):
    pf = pq.ParquetFile(input_path)
    writer = None

    for batch in pf.iter_batches(batch_size=batch_size):
        batch_table = pa.Table.from_batches([batch])

        con = duckdb.connect()
        con.execute('INSTALL spatial; LOAD spatial;')
        con.register('batch', batch_table)

        result = con.execute(f'''
            SELECT *,
                ST_XMin(ST_GeomFromWKB({geom_col})) as bbox_xmin,
                ST_YMin(ST_GeomFromWKB({geom_col})) as bbox_ymin,
                ST_XMax(ST_GeomFromWKB({geom_col})) as bbox_xmax,
                ST_YMax(ST_GeomFromWKB({geom_col})) as bbox_ymax
            FROM batch
        ''').fetch_arrow_table()

        con.close()

        if writer is None:
            writer = pq.ParquetWriter(output_path, result.schema)
        writer.write_table(result)
        del batch_table, result

    if writer:
        writer.close()
```

### Two-pass Hilbert Sort

```python
def streaming_add_bbox_hilbert(input_path: str, output_path: str,
                                geom_col: str = 'geometry',
                                batch_size: int = 100_000,
                                sort_memory: str = '512MB'):
    pf = pq.ParquetFile(input_path)

    # Pass 1: Compute global extent
    xmin, ymin = float('inf'), float('inf')
    xmax, ymax = float('-inf'), float('-inf')

    for batch in pf.iter_batches(batch_size=batch_size):
        batch_table = pa.Table.from_batches([batch])
        con = duckdb.connect()
        con.execute('INSTALL spatial; LOAD spatial;')
        con.register('batch', batch_table)

        result = con.execute(f'''
            SELECT MIN(ST_XMin(ST_GeomFromWKB({geom_col}))),
                   MIN(ST_YMin(ST_GeomFromWKB({geom_col}))),
                   MAX(ST_XMax(ST_GeomFromWKB({geom_col}))),
                   MAX(ST_YMax(ST_GeomFromWKB({geom_col})))
            FROM batch
        ''').fetchone()
        con.close()

        xmin, ymin = min(xmin, result[0]), min(ymin, result[1])
        xmax, ymax = max(xmax, result[2]), max(ymax, result[3])

    # Pass 2: Add bbox + hilbert index to temp file
    temp_path = Path(tempfile.gettempdir()) / f'hilbert_{uuid.uuid4()}.parquet'
    writer = None

    for batch in pf.iter_batches(batch_size=batch_size):
        batch_table = pa.Table.from_batches([batch])
        con = duckdb.connect()
        con.execute('INSTALL spatial; LOAD spatial;')
        con.register('batch', batch_table)

        result = con.execute(f'''
            SELECT *,
                ST_XMin(ST_GeomFromWKB({geom_col})) as bbox_xmin,
                ST_YMin(ST_GeomFromWKB({geom_col})) as bbox_ymin,
                ST_XMax(ST_GeomFromWKB({geom_col})) as bbox_xmax,
                ST_YMax(ST_GeomFromWKB({geom_col})) as bbox_ymax,
                ST_Hilbert(ST_GeomFromWKB({geom_col}),
                    {{'min_x': {xmin}, 'min_y': {ymin},
                      'max_x': {xmax}, 'max_y': {ymax}}}::BOX_2D
                ) as __hilbert_idx
            FROM batch
        ''').fetch_arrow_table()
        con.close()

        if writer is None:
            writer = pq.ParquetWriter(str(temp_path), result.schema)
        writer.write_table(result)

    writer.close()

    # Pass 3: Sort with memory limit
    con = duckdb.connect()
    con.execute('INSTALL spatial; LOAD spatial;')
    con.execute(f'SET memory_limit = "{sort_memory}";')
    con.execute(f'''
        COPY (
            SELECT * EXCLUDE(__hilbert_idx)
            FROM read_parquet('{temp_path}')
            ORDER BY __hilbert_idx
        ) TO '{output_path}' (FORMAT PARQUET)
    ''')
    con.close()

    temp_path.unlink()
```
