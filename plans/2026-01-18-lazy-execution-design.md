# Lazy Execution API Design

**Date:** 2026-01-18
**Status:** In Progress
**Branch:** `feat/lazy-execution`

---

## Enhancement Summary

**Deepened on:** 2025-01-25
**Research agents used:** DuckDB docs, PyArrow streaming, GeoParquet 2.0 spec, Polars LazyFrame patterns, Python reviewer, Performance oracle, Architecture strategist, Simplicity reviewer, Pattern recognition

### Key Improvements from Research

1. **Use DuckDB Relation API** - Use `DuckDBPyRelation` instead of raw SQL strings for simpler query building
2. **Immutable LazyTable** - Return new instances from transforms to avoid mutation bugs
3. **Resource management** - Add context manager support and connection ownership tracking
4. **GeoParquet 2.0 alignment** - Support new `edges`, `epoch`, and spherical interpolation fields
5. **Simplify API** - Cut `from_table()`, `eager=True`, `geoarrow=False` parameters for v1

### Critical Findings

- **Polars pattern**: Use `.explain()` for query plan visibility (matches Polars exactly)
- **DuckDB streaming**: Use `rel.arrow(batch_size)` for efficient batch consumption
- **GeoParquet 2.0**: CRS defaults to OGC:CRS84, axis order always (x, y), PROJJSON required
- **Transform functions > classes**: SQL transforms can be simple functions, not a class hierarchy

---

## Problem

The current fluent API materializes full Arrow tables between operations, losing DuckDB's native spill-to-disk capability for large datasets.

## Foundation (Complete)

The streaming write infrastructure is already implemented on this branch:

- **Write Strategies**: `ArrowStreamingStrategy`, `DuckDBKVStrategy`, `ArrowMemoryStrategy`, `DiskRewriteStrategy`
- **CLI Options**: `--write-strategy` and `--write-memory` flags
- **Container-aware memory**: Automatic cgroup detection for Docker/Kubernetes
- **Documentation**: `docs/guide/write-strategies.md`
- **Tests**: `tests/test_write_strategies.py`

This plan focuses on the remaining work: the **LazyTable API** that defers execution until terminal operations.

## Goals

1. Enable processing of datasets larger than available memory
2. Preserve the fluent API ergonomics
3. Support handoff from user DuckDB workflows
4. Retain GeoArrow compatibility for interop with other tools
5. Provide clear documentation for DuckDB → gpio workflows

## Non-Goals

- Distributed processing (Spark-style)
- GPU acceleration
- Breaking changes to the existing API (deprecation path instead)

---

## Design Overview

### Core Concept: LazyTable

Introduce a `LazyTable` class that wraps a DuckDB query plan instead of a materialized Arrow table. Operations build up SQL transformations; execution happens only when explicitly requested.

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Entry Point    │────▶│   LazyTable     │────▶│   Execution     │
│                 │     │  (query plan)   │     │                 │
│ - read()        │     │                 │     │ - write()       │
│ - convert()     │     │ - add_bbox()    │     │ - collect()     │
│ - from_table()  │     │ - sort_hilbert()│     │ - to_arrow()    │
│ - from_relation │     │ - extract()     │     │                 │
│ - from_arrow()  │     │ - reproject()   │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Entry Points

| Function | Input | Use Case |
|----------|-------|----------|
| `gpio.read(path)` | File path | Read GeoParquet file |
| `gpio.convert(path)` | File path | Convert from GeoJSON, GPKG, etc. |
| `gpio.from_table(name, con)` | Table name + DuckDB connection | Hand off DuckDB table |
| `gpio.from_relation(rel)` | DuckDB Relation | Hand off any DuckDB query |
| `gpio.from_arrow(table)` | Arrow/GeoArrow table | Interop with other libraries |

### Execution Points

| Method | Behavior |
|--------|----------|
| `.write(path)` | Execute query, stream to file |
| `.collect()` | Execute query, return Arrow table (materializes!) |
| `.to_arrow()` | Alias for `.collect()` |
| `.head(n)` | Execute with LIMIT, return Arrow table |
| `.count()` | Execute COUNT(*), return int |

---

## Detailed Design

### LazyTable Class

#### Research Insights

**Best Practices (from Python reviewer + Polars patterns):**
- Return new instances from transform methods (immutable pattern) to avoid mutation bugs
- Add `__enter__`/`__exit__` for context manager support
- Track connection ownership with `_owns_connection: bool`
- Add `.sql` property for query inspection (matches Polars `.explain()`)
- Use Protocol for `SQLTransform` interface for testability

**Resource Management Pattern:**
```python
class LazyTable:
    _owns_connection: bool = False  # True if we created the connection

    def close(self) -> None:
        if self._owns_connection and self._connection:
            self._connection.close()

    def __enter__(self) -> LazyTable:
        return self

    def __exit__(self, *args) -> None:
        self.close()
```

**DuckDB Relation API Alternative:**
Instead of raw SQL strings, use `DuckDBPyRelation` for simpler query building:
```python
# Relation-based (simpler)
rel = con.sql("SELECT * FROM read_parquet('file.parquet')")
rel = rel.filter("value > 100")
rel = rel.project("id, geometry, value")

# vs SQL string wrapping (current design)
sql = f"SELECT id, geometry, value FROM ({inner_sql}) WHERE value > 100"
```

**Recommended `LazyTable.__init__` signature:**
```python
def __init__(
    self,
    source: str | duckdb.DuckDBPyRelation,
    connection: duckdb.DuckDBPyConnection | None = None,
    geometry_column: str = "geometry",
    crs: CRSMetadata | None = None,  # Use TypedDict, not raw dict
):
```

---

```python
class LazyTable:
    """
    Lazy wrapper around a DuckDB query plan.

    Operations build SQL transformations; execution is deferred
    until write(), collect(), or other terminal operations.
    """

    def __init__(
        self,
        source: str | duckdb.DuckDBPyRelation,
        connection: duckdb.DuckDBPyConnection | None = None,
        geometry_column: str = "geometry",
        crs: dict | None = None,
    ):
        self._source = source  # SQL expression or relation
        self._connection = connection or self._create_connection()
        self._geometry_column = geometry_column
        self._crs = crs
        self._transformations: list[SQLTransform] = []

    def add_bbox(self, column_name: str = "bbox") -> LazyTable:
        """Add bbox computation to the query plan."""
        self._transformations.append(AddBboxTransform(column_name))
        return self

    def sort_hilbert(self) -> LazyTable:
        """Add Hilbert ordering to the query plan."""
        self._transformations.append(HilbertSortTransform())
        return self

    def extract(
        self,
        columns: list[str] | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        where: str | None = None,
        limit: int | None = None,
    ) -> LazyTable:
        """Add filtering to the query plan."""
        self._transformations.append(ExtractTransform(columns, bbox, where, limit))
        return self

    def write(
        self,
        path: str,
        compression: str = "ZSTD",
        **kwargs,
    ) -> Path:
        """Execute the query plan and write to file."""
        sql = self._build_sql()
        # Use DuckDB's COPY TO for streaming write
        self._connection.execute(f"""
            COPY ({sql}) TO '{path}' (FORMAT PARQUET, COMPRESSION {compression})
        """)
        # Apply GeoParquet metadata post-write
        self._apply_geoparquet_metadata(path)
        return Path(path)

    def collect(self) -> pa.Table:
        """Execute and materialize as Arrow table."""
        sql = self._build_sql()
        return self._connection.execute(sql).fetch_arrow_table()

    def _build_sql(self) -> str:
        """Compile transformations into final SQL."""
        sql = self._source_sql()
        for transform in self._transformations:
            sql = transform.wrap(sql, self._geometry_column)
        return sql
```

### SQL Transforms

Each operation becomes a SQL transformation that wraps the previous query:

```python
class AddBboxTransform:
    def __init__(self, column_name: str):
        self.column_name = column_name

    def wrap(self, inner_sql: str, geom_col: str) -> str:
        return f"""
            SELECT *,
                {{
                    'xmin': ST_XMin("{geom_col}"),
                    'ymin': ST_YMin("{geom_col}"),
                    'xmax': ST_XMax("{geom_col}"),
                    'ymax': ST_YMax("{geom_col}")
                }} AS {self.column_name}
            FROM ({inner_sql})
        """

class HilbertSortTransform:
    def wrap(self, inner_sql: str, geom_col: str) -> str:
        # Note: Hilbert requires bounds, computed via subquery
        return f"""
            WITH bounds AS (
                SELECT
                    MIN(ST_XMin("{geom_col}")) as xmin,
                    MIN(ST_YMin("{geom_col}")) as ymin,
                    MAX(ST_XMax("{geom_col}")) as xmax,
                    MAX(ST_YMax("{geom_col}")) as ymax
                FROM ({inner_sql})
            ),
            data AS ({inner_sql})
            SELECT data.*
            FROM data, bounds
            ORDER BY ST_Hilbert(
                "{geom_col}",
                ST_Extent(ST_MakeEnvelope(bounds.xmin, bounds.ymin, bounds.xmax, bounds.ymax))
            )
        """

class ExtractTransform:
    def __init__(self, columns, bbox, where, limit):
        self.columns = columns
        self.bbox = bbox
        self.where = where
        self.limit = limit

    def wrap(self, inner_sql: str, geom_col: str) -> str:
        select = ", ".join(f'"{c}"' for c in self.columns) if self.columns else "*"

        conditions = []
        if self.bbox:
            xmin, ymin, xmax, ymax = self.bbox
            conditions.append(
                f'ST_Intersects("{geom_col}", ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))'
            )
        if self.where:
            conditions.append(f"({self.where})")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        limit_clause = f"LIMIT {self.limit}" if self.limit else ""

        return f"""
            SELECT {select}
            FROM ({inner_sql})
            {where_clause}
            {limit_clause}
        """
```

### Entry Point Implementations

```python
def read(path: str, **kwargs) -> LazyTable:
    """Read a GeoParquet file lazily."""
    # Detect geometry column and CRS from file metadata
    geom_col, crs = _read_geoparquet_metadata(path)
    source_sql = f"SELECT * FROM read_parquet('{path}')"
    return LazyTable(source_sql, geometry_column=geom_col, crs=crs)

def convert(path: str, **kwargs) -> LazyTable:
    """Convert from other formats lazily."""
    if _is_csv_file(path):
        source_sql = _build_csv_read_sql(path, **kwargs)
    else:
        source_sql = f"SELECT *, ST_AsWKB(geometry) AS geometry FROM ST_Read('{path}')"
    return LazyTable(source_sql)

def from_table(name: str, con: duckdb.DuckDBPyConnection) -> LazyTable:
    """Create LazyTable from a DuckDB table."""
    geom_col = _detect_geometry_column(con, name)
    return LazyTable(f'SELECT * FROM "{name}"', connection=con, geometry_column=geom_col)

def from_relation(rel: duckdb.DuckDBPyRelation) -> LazyTable:
    """Create LazyTable from a DuckDB relation."""
    # Relations can be used directly as subqueries
    return LazyTable(rel)

def from_arrow(
    table: pa.Table,
    geometry_column: str | None = None,
) -> LazyTable:
    """Create LazyTable from Arrow/GeoArrow table."""
    # Register the Arrow table in DuckDB
    con = _create_connection()
    con.register("__arrow_input", table)
    geom_col = geometry_column or _detect_geometry_from_arrow(table)
    crs = _extract_crs_from_arrow(table, geom_col)
    return LazyTable("SELECT * FROM __arrow_input", connection=con, geometry_column=geom_col, crs=crs)
```

---

## GeoArrow Interoperability

### Research Insights: GeoParquet 2.0 Specification

**Key Changes in GeoParquet 2.0** (from https://github.com/opengeospatial/geoparquet/tree/2.0-first-draft):

1. **Built on Parquet's native geospatial types** - GeoParquet 2.0 provides guidance for Parquet's GEOMETRY/GEOGRAPHY logical types (added in Parquet 2.11, March 2025)

2. **CRS Handling:**
   - Default CRS is **OGC:CRS84** (WGS84 in longitude-latitude order) when `crs` field is absent
   - CRS must be expressed in **PROJJSON** format
   - Coordinate axis order in WKB is **always (x, y)** regardless of CRS definition
   - Setting `crs: null` explicitly indicates undefined/unknown CRS

3. **New Metadata Fields:**
   - `edges`: "planar" (default) or "spherical" for edge interpolation
   - `algorithm`: spherical interpolation method (Vincenty, Thomas, Andoyer, Karney)
   - `epoch`: coordinate epoch for dynamic CRS (decimal year format)

4. **Geometry Types:** Enhanced `geometry_types` with Z/M/ZM suffixes for 3D and measured geometries

5. **Bbox Format:**
   - Geographic: `[southwest_lon, southwest_lat, northeast_lon, northeast_lat]`
   - Non-geographic: `[xmin, ymin, xmax, ymax]` (2D) or 6 values (3D)

**Implementation Implications:**
- `LazyTable._crs` should store PROJJSON dict, not EPSG code string
- Add `edges` and `epoch` to metadata tracking for 2.0 output
- CRS round-trip tests must verify PROJJSON preservation

---

### Input: Accepting GeoArrow Tables

When a user passes an Arrow table with GeoArrow extension types:

1. Detect the geometry column by checking for `geoarrow.*` extension types
2. Extract CRS from the extension type metadata
3. Register the table in DuckDB (DuckDB handles GeoArrow types natively since v0.10)

```python
def from_arrow(table: pa.Table, geometry_column: str | None = None) -> LazyTable:
    """
    Create LazyTable from Arrow/GeoArrow table.

    Accepts:
    - Plain Arrow with WKB geometry column
    - GeoArrow with extension types (geoarrow.wkb, geoarrow.point, etc.)

    GeoArrow CRS metadata is preserved and applied to output.
    """
    con = _create_connection()
    con.register("__arrow_input", table)

    # Detect geometry column
    geom_col = geometry_column
    if not geom_col:
        geom_col = _detect_geoarrow_column(table) or _detect_wkb_column(table)

    # Extract CRS from GeoArrow extension type
    crs = None
    if geom_col and _is_geoarrow_type(table.schema.field(geom_col).type):
        crs = _extract_crs_from_geoarrow(table, geom_col)

    return LazyTable(
        "SELECT * FROM __arrow_input",
        connection=con,
        geometry_column=geom_col,
        crs=crs,
    )
```

### Output: Producing GeoArrow

The `.collect()` method returns GeoArrow by default (the modern standard):

```python
def collect(self, geoarrow: bool = True) -> pa.Table:
    """
    Execute and return Arrow table.

    Args:
        geoarrow: If True (default), apply GeoArrow extension types to geometry column.
                  Set to False for plain Arrow with WKB binary if needed.
    """
    table = self._connection.execute(self._build_sql()).fetch_arrow_table()

    if geoarrow:
        table = _apply_geoarrow_extension(table, self._geometry_column, self._crs)

    return table
```

This ensures `.collect()` produces proper GeoArrow tables that work seamlessly with GeoPandas, Lonboard, and other GeoArrow-aware libraries.

---

## Migration Path

### Release Strategy

| Version | Changes |
|---------|---------|
| **v0.10.0** | `LazyTable` is the default. `read()`, `convert()` return `LazyTable`. New entry points (`from_table`, `from_relation`, `from_arrow`). `eager=True` parameter available for backwards compatibility. |
| **v1.0-beta.1** | Remove eager `Table` from public API. `eager=True` parameter removed. Deprecation warnings become errors. |
| **v1.0.0** | Stable release with lazy-only API. |

### v0.10.0 Behavior

```python
# New default - returns LazyTable
table = gpio.read("input.parquet")
table = gpio.convert("input.geojson")

# Backwards compat - returns eager Table (with deprecation warning)
table = gpio.read("input.parquet", eager=True)

# New entry points - return LazyTable
table = gpio.from_table("processed", con)
table = gpio.from_relation(rel)
table = gpio.from_arrow(arrow_table)
```

### Breaking Changes in v0.10.0

Users who relied on immediate `.table` property access will need to add `.collect()`:

```python
# Before (v0.9.x)
arrow_table = gpio.read("input.parquet").table

# After (v0.10.0)
arrow_table = gpio.read("input.parquet").collect()

# Or use eager mode during transition
arrow_table = gpio.read("input.parquet", eager=True).table
```

---

## Operations Support Matrix

| Operation | Lazy Support | Notes |
|-----------|--------------|-------|
| `add_bbox()` | Yes | Simple SQL transform |
| `add_quadkey()` | Yes | SQL with Hilbert/tile math |
| `add_h3()` | Partial | Requires H3 extension or UDF |
| `sort_hilbert()` | Yes | Requires bounds subquery (2-pass) |
| `sort_quadkey()` | Yes | SQL ORDER BY |
| `sort_column()` | Yes | SQL ORDER BY |
| `extract()` | Yes | SQL WHERE/SELECT/LIMIT |
| `reproject()` | Yes | SQL with ST_Transform |
| `partition_*()` | Deferred | Write to multiple files - special handling |
| `check*()` | N/A | Inspection only, not transformation |
| `validate()` | N/A | Inspection only |

### Partitioning Strategy

Partition operations write multiple files. For lazy execution:

```python
def partition_by_quadkey(self, output_dir: str, **kwargs) -> dict:
    """Partition execution - always terminal."""
    sql = self._build_sql()

    # Use DuckDB's partitioned COPY
    self._connection.execute(f"""
        COPY ({sql}) TO '{output_dir}' (
            FORMAT PARQUET,
            PARTITION_BY (quadkey_prefix),
            OVERWRITE_OR_IGNORE
        )
    """)

    return {"output_dir": output_dir, "file_count": ...}
```

---

## Tutorial: DuckDB to gpio Workflow

### Basic Handoff

```python
import duckdb
import geoparquet_io as gpio

# Connect and load extensions
con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial")

# Do your DuckDB work
con.execute("""
    CREATE TABLE processed AS
    SELECT
        b.id,
        b.name,
        b.geometry,
        c.population
    FROM read_parquet('buildings.parquet') b
    JOIN read_parquet('census.parquet') c
        ON ST_Within(b.geometry, c.geometry)
    WHERE b.area_sqm > 100
""")

# Hand off to gpio for finalization
gpio.from_table("processed", con) \
    .add_bbox() \
    .sort_hilbert() \
    .write("output.parquet")
```

### Complex Query Handoff

```python
# Build a complex query as a relation
rel = con.sql("""
    WITH buildings_with_stats AS (
        SELECT
            *,
            ST_Area(geometry) as area,
            ST_Perimeter(geometry) as perimeter
        FROM read_parquet('buildings.parquet')
    ),
    filtered AS (
        SELECT *
        FROM buildings_with_stats
        WHERE area > 50 AND area / perimeter > 0.1
    )
    SELECT * FROM filtered
""")

# Pass relation directly - stays lazy
gpio.from_relation(rel) \
    .sort_hilbert() \
    .write("output.parquet", compression="ZSTD")
```

### Interop with GeoPandas/GeoArrow

```python
import geopandas as gpd
import geoparquet_io as gpio

# Load data in GeoPandas
gdf = gpd.read_file("input.geojson")

# Do GeoPandas operations
gdf = gdf[gdf.population > 10000]
gdf["density"] = gdf.population / gdf.area

# Convert to Arrow with GeoArrow extension types
arrow_table = gdf.to_arrow()  # or pa.Table.from_pandas(gdf)

# Hand off to gpio - preserves CRS from GeoArrow
gpio.from_arrow(arrow_table) \
    .sort_hilbert() \
    .write("output.parquet")
```

### Streaming Large Conversions

```python
# Convert 50GB GeoJSON - streams through DuckDB, never fully in memory
gpio.convert("massive.geojson") \
    .add_bbox() \
    .sort_hilbert() \
    .write("output.parquet")

# With filtering to reduce output size
gpio.convert("massive.geojson") \
    .extract(bbox=(-122.5, 37.5, -122.0, 38.0)) \
    .sort_hilbert() \
    .write("sf_subset.parquet")
```

---

## Implementation Plan

**Test file:** `/Users/cholmes/geodata/parquet-test-data/japan.parquet`

### Research Insights: Simplification Recommendations

**From simplicity reviewer - cut for v1:**
- `from_table()` entry point (use `from_relation(con.sql("SELECT * FROM name"))`)
- `eager=True` parameter (users can call `.collect()` for eager behavior)
- `geoarrow=False` parameter (always output GeoArrow from `.collect()`)
- `.to_arrow()` alias (duplicate of `.collect()`)
- `.head()` shortcut (use `.extract(limit=n).collect()`)
- Transform class hierarchy (use simple functions instead)

**Minimal v1 API:**
```python
# Entry points (4, not 5)
gpio.read(path) -> LazyTable
gpio.convert(path) -> LazyTable
gpio.from_relation(rel) -> LazyTable
gpio.from_arrow(table) -> LazyTable

# Transforms (5 essential)
.add_bbox() -> LazyTable
.extract(columns, bbox, where, limit) -> LazyTable
.sort_hilbert() -> LazyTable
.reproject(target_crs) -> LazyTable
.sort_column(col) -> LazyTable

# Execution (2)
.write(path) -> Path
.collect() -> pa.Table  # Always GeoArrow
```

**Deferred to v1.1:**
- `.count()`, `.head()` convenience methods
- `.add_quadkey()`, `.add_h3()` transforms
- Partition operations (already work on eager Table)

### Research Insights: Performance Recommendations

**From performance oracle:**

1. **Hilbert Sort Optimization** - Extract bounds from file metadata when available to avoid 2-pass:
   ```python
   def sort_hilbert(self) -> LazyTable:
       bounds = self._try_extract_bounds_from_metadata()
       if bounds:
           # Single-pass with known bounds
           self._transformations.append(HilbertSortTransform(bounds=bounds))
       else:
           # Fall back to 2-pass with CTE
           self._transformations.append(HilbertSortTransform())
   ```

2. **Dynamic Batch Sizing** - Adjust based on geometry complexity:
   ```python
   # Point geometries: 100K-500K rows/batch
   # MultiPolygon: 10K-50K rows/batch
   # Target: 64-128 MB per batch
   ```

3. **Query Plan Visibility** - Add `.explain()` for debugging:
   ```python
   def explain(self) -> str:
       return self._connection.execute(f"EXPLAIN {self.sql}").fetchone()[0]
   ```

---

### Step 1: Core LazyTable Infrastructure
- [ ] Create `LazyTable` class in `api/lazy.py`
- [ ] Implement `_build_sql()` query compilation
- [ ] Implement `write()` using write strategies (already implemented)
- [ ] Implement `collect()` for Arrow/GeoArrow materialization
- [ ] Add connection management and cleanup
- [ ] Track CRS through all operations

### Step 2: Entry Points
- [ ] `from_relation(rel)` - DuckDB relation handoff
- [ ] `from_table(name, con)` - DuckDB table handoff
- [ ] `from_arrow(table)` - Arrow/GeoArrow handoff
- [ ] Update `read()` to return `LazyTable` by default
- [ ] Update `convert()` to return `LazyTable` by default
- [ ] Add `eager=True` parameter for backwards compatibility

### Step 3: SQL Transforms
- [ ] `AddBboxTransform`
- [ ] `ExtractTransform` (columns, bbox, where, limit)
- [ ] `HilbertSortTransform`
- [ ] `QuadkeySortTransform`
- [ ] `ColumnSortTransform`
- [ ] `ReprojectTransform`
- [ ] `AddQuadkeyTransform`
- [ ] `AddH3Transform` (if DuckDB H3 extension available)

### Step 4: GeoParquet Metadata
- [ ] Preserve CRS through all transforms
- [ ] Apply geo metadata on write (via streaming writer)
- [ ] Handle GeoArrow extension types on input
- [ ] Support v1.0, v1.1, v2.0 output

### Step 5: Performance Testing
- [ ] Create `tests/test_lazy_performance.py`
- [ ] Benchmark: Eager Table API vs LazyTable API
- [ ] Test chained operations: `convert().add_bbox().sort_hilbert().write()`
- [ ] Measure memory for DuckDB handoff scenarios
- [ ] Verify no regression from current write strategy benchmarks

```python
@pytest.mark.slow
class TestLazyTablePerformance:
    """Performance benchmarks for LazyTable vs eager Table."""

    TEST_FILE = "/Users/cholmes/geodata/parquet-test-data/japan.parquet"

    def test_chained_operations_memory(self):
        """Verify lazy chained ops use less memory than eager."""
        # Eager: each step materializes
        def eager_chain():
            table = gpio.read(self.TEST_FILE, eager=True)
            table = table.add_bbox()
            table = table.sort_hilbert()
            table.write(output)

        # Lazy: single execution at end
        def lazy_chain():
            gpio.read(self.TEST_FILE) \
                .add_bbox() \
                .sort_hilbert() \
                .write(output)

        eager_memory = measure_peak_memory(eager_chain)
        lazy_memory = measure_peak_memory(lazy_chain)

        assert lazy_memory < eager_memory * 0.5

    def test_duckdb_handoff_memory(self):
        """Verify from_table() doesn't spike memory."""
        con = duckdb.connect()
        con.execute(f"CREATE TABLE test AS SELECT * FROM read_parquet('{self.TEST_FILE}')")

        def handoff_and_write():
            gpio.from_table("test", con).sort_hilbert().write(output)

        memory = measure_peak_memory(handoff_and_write)
        file_size_mb = Path(self.TEST_FILE).stat().st_size / (1024 * 1024)

        # Memory should stay well below 2x file size
        assert memory < file_size_mb * 2
```

### Step 6: Documentation
- [ ] API reference for LazyTable
- [ ] Tutorial: DuckDB → gpio workflow
- [ ] Tutorial: GeoPandas/GeoArrow → gpio workflow
- [ ] Migration guide from eager Table
- [ ] Update existing examples

### Step 7: Testing
- [ ] Unit tests for each SQL transform
- [ ] Integration tests for full pipelines
- [ ] Memory tests with japan.parquet
- [ ] GeoArrow round-trip tests

### Step 8: CRS Preservation Tests (Critical)

CRS must be preserved through all operations. Add explicit tests for:

- [ ] **Round-trip tests:** Input CRS matches output CRS for each entry point
  - `read()` → operations → `write()` → verify CRS in output file
  - `read()` → operations → `collect()` → verify CRS in GeoArrow extension
  - `from_arrow(geoarrow_table)` → operations → `collect()` → verify CRS preserved
  - `from_table()` with explicit CRS → `write()` → verify CRS in output

- [ ] **Transform-specific CRS tests:**
  - `add_bbox()` preserves CRS
  - `add_quadkey()` preserves CRS
  - `sort_hilbert()` preserves CRS
  - `sort_quadkey()` preserves CRS
  - `extract()` preserves CRS
  - `reproject()` correctly updates CRS to target

- [ ] **Reproject chain tests:**
  - Single reproject: EPSG:4326 → EPSG:32610 → verify output is EPSG:32610
  - Double reproject: EPSG:4326 → EPSG:32610 → EPSG:3857 → verify output is EPSG:3857
  - Reproject back: EPSG:4326 → EPSG:32610 → EPSG:4326 → verify coordinates and CRS

- [ ] **Edge case tests:**
  - Source with no CRS → verify warning and sensible default (WGS84)
  - Source with obscure CRS (PROJJSON) → verify preserved exactly
  - `from_table()` without CRS parameter → verify warning
  - `from_relation()` without CRS parameter → verify warning

- [ ] **GeoArrow CRS round-trip:**
  - GeoArrow input with CRS → `collect()` → verify CRS in output extension type
  - GeoArrow input with CRS → `write()` → `read()` → `collect()` → verify CRS matches original

---

## Open Questions

1. **H3 support:** H3 operations require the h3 extension or Python UDFs. May need hybrid approach. **Recommendation:** Defer to v1.1, not essential for core lazy execution.

2. **Error context:** Lazy execution means errors happen at write time. **Recommendation:** Store operation context (transform name, position) for better error traces:
   ```python
   class TransformError(Exception):
       def __init__(self, message: str, transform_name: str, position: int):
           super().__init__(f"Error in transform #{position} ({transform_name}): {message}")
   ```

## Research Findings (New Resolved Decisions)

1. **Connection ownership** ✅ - Track with `_owns_connection: bool`. gpio only closes connections it created. User-provided connections are never closed.

2. **Query plan visibility** ✅ - Add `.sql` property and `.explain()` method (matches Polars pattern):
   ```python
   @property
   def sql(self) -> str:
       return self._build_sql()

   def explain(self) -> str:
       return self._connection.execute(f"EXPLAIN {self.sql}").fetchone()[0]
   ```

3. **Immutability pattern** ✅ - Transform methods return new `LazyTable` instances to avoid mutation bugs:
   ```python
   def add_bbox(self, column_name: str = "bbox") -> LazyTable:
       return self._copy_with_transform(AddBboxTransform(column_name))
   ```

4. **Transform implementation** ✅ - Use functions instead of class hierarchy for simplicity:
   ```python
   _TRANSFORMS = {
       "add_bbox": lambda sql, gcol, **kw: f"SELECT *, ... FROM ({sql})",
       "extract": lambda sql, gcol, **kw: f"SELECT ... FROM ({sql}) WHERE ...",
   }
   ```

5. **GeoParquet 2.0 CRS handling** ✅ - Default to OGC:CRS84, store as PROJJSON dict, always use (x, y) axis order.

## Resolved Decisions

1. **Metadata preservation:** ✅ Solved by write strategies. `DuckDBKVStrategy` uses `KV_METADATA` option to embed geo metadata directly. `ArrowStreamingStrategy` adds metadata to schema before streaming. No post-processing needed.

2. **CRS for DuckDB handoff:** `from_table()` and `from_relation()` accept optional `crs` parameter. If not provided, default to WGS84 (EPSG:4326) with a warning. Rationale: most web/geo data is WGS84, and failing silently is worse than a warning.

   ```python
   gpio.from_table("processed", con)                    # Warns, assumes WGS84
   gpio.from_table("processed", con, crs="EPSG:32610")  # Explicit, no warning
   ```

3. **GeoArrow output default:** `.collect()` returns GeoArrow with extension types by default. Use `collect(geoarrow=False)` for plain WKB if needed.

4. **CRS preservation:** CRS is tracked in `LazyTable._crs` and applied to output. Every transform must preserve CRS except `reproject()` which updates it.

---

## Alternatives Considered

### Temp File Checkpointing
Write to temp parquet files between operations. Simpler to implement but has I/O overhead and no cross-operation optimization.

### Two-Tier API
Separate `Table` (eager) and `LazyTable` classes with different entry points. More explicit but doubles the API surface and maintenance burden.

### Polars Integration
Use Polars LazyFrame instead of DuckDB relations. Polars has good lazy execution but weaker spatial support than DuckDB.

---

## References

**DuckDB Documentation:**
- Relational API: https://duckdb.org/docs/stable/clients/python/relational_api
- Python Reference: https://duckdb.org/docs/stable/clients/python/reference
- Spatial Extension: https://duckdb.org/docs/extensions/spatial
- Arrow Export: https://duckdb.org/docs/stable/guides/python/export_arrow

**GeoParquet Specification:**
- GeoParquet 2.0 Draft: https://github.com/opengeospatial/geoparquet/tree/2.0-first-draft/format-specs
- PROJJSON Specification: https://proj.org/en/stable/specifications/projjson.html

**Polars Lazy Execution Patterns:**
- LazyFrame API: https://docs.pola.rs/api/python/stable/reference/lazyframe/
- Lazy vs Eager: https://docs.pola.rs/user-guide/concepts/lazy-api/

**PyArrow:**
- ParquetWriter: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html
- RecordBatchReader: https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatchReader.html
