# Streaming DuckDB Write Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a streaming write path that uses DuckDB COPY TO + footer rewrite for low-memory GeoParquet output.

**Architecture:** New `write_geoparquet_via_duckdb()` function that streams data to disk using DuckDB's COPY TO command, then uses fastparquet to add GeoParquet metadata to the footer. Metadata is preserved from input by default, only recomputed when operations change bounds/types.

**Tech Stack:** DuckDB (COPY TO), fastparquet (`update_file_custom_metadata`), existing GeoParquet metadata utilities

---

## Task 1: Add fastparquet Dependency

**Files:**
- Modify: `pyproject.toml:28-47`

**Step 1: Add fastparquet to dependencies**

Edit `pyproject.toml` to add fastparquet after the existing dependencies:

```toml
dependencies = [
    # ... existing deps ...
    "mercantile>=1.2.0",  # Required for quadkey generation
    "httpx>=0.25.0",  # Required for ArcGIS REST API requests
    "click-plugins>=1.1.1",  # Required for plugin system
    "fastparquet>=2024.2.0",  # Required for footer metadata rewrite
]
```

**Step 2: Install updated dependencies**

Run: `pip install -e ".[dev]"`
Expected: Successfully installed fastparquet

**Step 3: Verify import works**

Run: `python -c "from fastparquet import update_file_custom_metadata; print('OK')"`
Expected: `OK`

**Step 4: Commit**

```bash
git add pyproject.toml
git commit -m "Add fastparquet dependency for footer metadata rewrite"
```

---

## Task 2: Create Footer Rewrite Helper Function

**Files:**
- Modify: `geoparquet_io/core/common.py`
- Test: `tests/test_streaming_write.py` (new file)

**Step 1: Write failing test for footer rewrite**

Create `tests/test_streaming_write.py`:

```python
"""Tests for streaming DuckDB write with footer rewrite."""

import json
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest


class TestFooterRewrite:
    """Tests for footer metadata rewrite functionality."""

    @pytest.fixture
    def sample_parquet(self):
        """Create a sample parquet file without geo metadata."""
        import duckdb
        import pyarrow as pa

        tmp_path = Path(tempfile.gettempdir()) / f"test_footer_{id(self)}.parquet"

        # Create simple parquet via DuckDB (no geo metadata)
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute(f"""
            COPY (
                SELECT
                    1 as id,
                    ST_AsWKB(ST_Point(-122.4, 37.8)) as geometry
            ) TO '{tmp_path}' (FORMAT PARQUET)
        """)
        con.close()

        yield str(tmp_path)

        if tmp_path.exists():
            tmp_path.unlink()

    def test_rewrite_footer_adds_geo_metadata(self, sample_parquet):
        """Test that rewrite_footer_with_geo_metadata adds geo key."""
        from geoparquet_io.core.common import rewrite_footer_with_geo_metadata

        geo_meta = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "bbox": [-122.4, 37.8, -122.4, 37.8],
                }
            },
        }

        rewrite_footer_with_geo_metadata(sample_parquet, geo_meta)

        # Verify metadata was added
        pf = pq.ParquetFile(sample_parquet)
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata

        parsed = json.loads(metadata[b"geo"].decode("utf-8"))
        assert parsed["version"] == "1.1.0"
        assert parsed["primary_column"] == "geometry"
        assert "geometry" in parsed["columns"]
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_streaming_write.py::TestFooterRewrite::test_rewrite_footer_adds_geo_metadata -v`
Expected: FAIL with "cannot import name 'rewrite_footer_with_geo_metadata'"

**Step 3: Implement rewrite_footer_with_geo_metadata**

Add to `geoparquet_io/core/common.py` after the imports section (around line 50):

```python
def rewrite_footer_with_geo_metadata(file_path: str, geo_meta: dict) -> None:
    """
    Rewrite parquet file footer to add GeoParquet metadata.

    Uses fastparquet to update only the footer section without rewriting data.
    This is efficient for large files as it only modifies the last few KB.

    Args:
        file_path: Path to local parquet file
        geo_meta: GeoParquet metadata dict to add

    Raises:
        ValueError: If file_path is a remote URL (not supported)
    """
    from fastparquet import update_file_custom_metadata

    if is_remote_url(file_path):
        raise ValueError(
            f"Footer rewrite only works on local files, got: {file_path}"
        )

    update_file_custom_metadata(
        path=file_path,
        custom_metadata={"geo": json.dumps(geo_meta)},
    )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_streaming_write.py::TestFooterRewrite::test_rewrite_footer_adds_geo_metadata -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/core/common.py tests/test_streaming_write.py
git commit -m "Add rewrite_footer_with_geo_metadata helper function"
```

---

## Task 3: Create SQL Metadata Computation Functions

**Files:**
- Modify: `geoparquet_io/core/common.py`
- Test: `tests/test_streaming_write.py`

**Step 1: Write failing test for bbox computation via SQL**

Add to `tests/test_streaming_write.py`:

```python
class TestSQLMetadataComputation:
    """Tests for computing metadata via SQL queries."""

    def test_compute_bbox_via_sql(self):
        """Test bbox computation using DuckDB SQL."""
        import duckdb
        from geoparquet_io.core.common import compute_bbox_via_sql

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")

        query = """
            SELECT ST_Point(-122.4, 37.8) as geometry
            UNION ALL
            SELECT ST_Point(-122.0, 38.0) as geometry
        """

        bbox = compute_bbox_via_sql(con, query, "geometry")

        assert bbox is not None
        assert len(bbox) == 4
        assert bbox[0] == pytest.approx(-122.4, rel=1e-6)  # xmin
        assert bbox[1] == pytest.approx(37.8, rel=1e-6)   # ymin
        assert bbox[2] == pytest.approx(-122.0, rel=1e-6)  # xmax
        assert bbox[3] == pytest.approx(38.0, rel=1e-6)   # ymax

        con.close()

    def test_compute_geometry_types_via_sql(self):
        """Test geometry type computation using DuckDB SQL."""
        import duckdb
        from geoparquet_io.core.common import compute_geometry_types_via_sql

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")

        query = """
            SELECT ST_Point(-122.4, 37.8) as geometry
            UNION ALL
            SELECT ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') as geometry
        """

        types = compute_geometry_types_via_sql(con, query, "geometry")

        assert "Point" in types
        assert "Polygon" in types
        assert len(types) == 2

        con.close()
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_streaming_write.py::TestSQLMetadataComputation -v`
Expected: FAIL with "cannot import name 'compute_bbox_via_sql'"

**Step 3: Implement SQL metadata computation functions**

Add to `geoparquet_io/core/common.py` after `rewrite_footer_with_geo_metadata`:

```python
def compute_bbox_via_sql(
    con,
    query: str,
    geometry_column: str,
) -> list[float] | None:
    """
    Compute bounding box from query using DuckDB spatial functions.

    Args:
        con: DuckDB connection with spatial extension loaded
        query: SQL query containing geometry column
        geometry_column: Name of geometry column

    Returns:
        [xmin, ymin, xmax, ymax] or None if query returns no rows
    """
    bbox_query = f"""
        SELECT
            MIN(ST_XMin("{geometry_column}")) as xmin,
            MIN(ST_YMin("{geometry_column}")) as ymin,
            MAX(ST_XMax("{geometry_column}")) as xmax,
            MAX(ST_YMax("{geometry_column}")) as ymax
        FROM ({query})
    """
    result = con.execute(bbox_query).fetchone()

    if result and all(v is not None for v in result):
        return list(result)
    return None


def compute_geometry_types_via_sql(
    con,
    query: str,
    geometry_column: str,
) -> list[str]:
    """
    Compute distinct geometry types from query using DuckDB.

    Args:
        con: DuckDB connection with spatial extension loaded
        query: SQL query containing geometry column
        geometry_column: Name of geometry column

    Returns:
        List of geometry type names (e.g., ["Point", "Polygon"])
    """
    types_query = f"""
        SELECT DISTINCT ST_GeometryType("{geometry_column}") as geom_type
        FROM ({query})
        WHERE "{geometry_column}" IS NOT NULL
    """
    results = con.execute(types_query).fetchall()

    # DuckDB returns types like "POINT", "POLYGON" - convert to GeoParquet format
    type_map = {
        "POINT": "Point",
        "LINESTRING": "LineString",
        "POLYGON": "Polygon",
        "MULTIPOINT": "MultiPoint",
        "MULTILINESTRING": "MultiLineString",
        "MULTIPOLYGON": "MultiPolygon",
        "GEOMETRYCOLLECTION": "GeometryCollection",
    }

    types = []
    for (geom_type,) in results:
        if geom_type:
            normalized = type_map.get(geom_type.upper(), geom_type)
            types.append(normalized)

    return sorted(set(types))
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_streaming_write.py::TestSQLMetadataComputation -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/core/common.py tests/test_streaming_write.py
git commit -m "Add SQL-based metadata computation functions"
```

---

## Task 4: Create Metadata Preparation Function

**Files:**
- Modify: `geoparquet_io/core/common.py`
- Test: `tests/test_streaming_write.py`

**Step 1: Write failing test for metadata preparation**

Add to `tests/test_streaming_write.py`:

```python
class TestMetadataPreparation:
    """Tests for preparing geo metadata from input."""

    def test_prepare_geo_metadata_preserves_from_input(self):
        """Test that metadata is preserved from input when flags are True."""
        from geoparquet_io.core.common import prepare_geo_metadata_for_streaming

        original_metadata = {
            b"geo": json.dumps({
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "geometry_types": ["Point", "Polygon"],
                        "bbox": [-180, -90, 180, 90],
                    }
                },
            }).encode("utf-8")
        }

        result = prepare_geo_metadata_for_streaming(
            original_metadata=original_metadata,
            geometry_column="geometry",
            geoparquet_version="1.1",
            preserve_bbox=True,
            preserve_geometry_types=True,
            input_crs=None,
        )

        assert result["version"] == "1.1.0"
        assert result["columns"]["geometry"]["bbox"] == [-180, -90, 180, 90]
        assert result["columns"]["geometry"]["geometry_types"] == ["Point", "Polygon"]

    def test_prepare_geo_metadata_clears_bbox_when_not_preserved(self):
        """Test that bbox is cleared when preserve_bbox=False."""
        from geoparquet_io.core.common import prepare_geo_metadata_for_streaming

        original_metadata = {
            b"geo": json.dumps({
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "geometry_types": ["Point"],
                        "bbox": [-180, -90, 180, 90],
                    }
                },
            }).encode("utf-8")
        }

        result = prepare_geo_metadata_for_streaming(
            original_metadata=original_metadata,
            geometry_column="geometry",
            geoparquet_version="1.1",
            preserve_bbox=False,
            preserve_geometry_types=True,
            input_crs=None,
        )

        # bbox should not be present (needs recomputation)
        assert "bbox" not in result["columns"]["geometry"]
        # geometry_types should still be preserved
        assert result["columns"]["geometry"]["geometry_types"] == ["Point"]

    def test_prepare_geo_metadata_adds_crs(self):
        """Test that CRS is added when provided."""
        from geoparquet_io.core.common import prepare_geo_metadata_for_streaming

        input_crs = {
            "type": "GeographicCRS",
            "name": "NAD83",
            "id": {"authority": "EPSG", "code": 4269},
        }

        result = prepare_geo_metadata_for_streaming(
            original_metadata=None,
            geometry_column="geometry",
            geoparquet_version="1.1",
            preserve_bbox=True,
            preserve_geometry_types=True,
            input_crs=input_crs,
        )

        assert result["columns"]["geometry"]["crs"] == input_crs
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_streaming_write.py::TestMetadataPreparation -v`
Expected: FAIL with "cannot import name 'prepare_geo_metadata_for_streaming'"

**Step 3: Implement prepare_geo_metadata_for_streaming**

Add to `geoparquet_io/core/common.py`:

```python
def prepare_geo_metadata_for_streaming(
    original_metadata: dict | None,
    geometry_column: str,
    geoparquet_version: str,
    preserve_bbox: bool,
    preserve_geometry_types: bool,
    input_crs: dict | None,
) -> dict:
    """
    Prepare GeoParquet metadata for streaming write.

    Extracts metadata from input and prepares it for the output file.
    When preserve flags are False, the corresponding fields are removed
    so they can be recomputed via SQL.

    Args:
        original_metadata: Metadata dict from input file
        geometry_column: Name of geometry column
        geoparquet_version: Target GeoParquet version (1.0, 1.1, 2.0)
        preserve_bbox: Whether to preserve bbox from input
        preserve_geometry_types: Whether to preserve geometry_types from input
        input_crs: CRS dict to add to output (optional)

    Returns:
        Prepared geo metadata dict
    """
    # Get version config
    version_config = GEOPARQUET_VERSIONS.get(geoparquet_version, GEOPARQUET_VERSIONS["1.1"])
    metadata_version = version_config.get("metadata_version", "1.1.0")

    # Parse existing geo metadata or create new
    geo_meta = _parse_existing_geo_metadata(original_metadata)
    geo_meta = _initialize_geo_metadata(geo_meta, geometry_column, version=metadata_version)

    # Ensure encoding is set
    if "encoding" not in geo_meta["columns"][geometry_column]:
        geo_meta["columns"][geometry_column]["encoding"] = "WKB"

    col_meta = geo_meta["columns"][geometry_column]

    # Handle bbox preservation
    if not preserve_bbox and "bbox" in col_meta:
        del col_meta["bbox"]

    # Handle geometry_types preservation
    if not preserve_geometry_types and "geometry_types" in col_meta:
        del col_meta["geometry_types"]

    # Add CRS if provided and not default
    if input_crs and not is_default_crs(input_crs):
        col_meta["crs"] = input_crs

    return geo_meta
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_streaming_write.py::TestMetadataPreparation -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/core/common.py tests/test_streaming_write.py
git commit -m "Add prepare_geo_metadata_for_streaming function"
```

---

## Task 5: Implement write_geoparquet_via_duckdb

**Files:**
- Modify: `geoparquet_io/core/common.py`
- Test: `tests/test_streaming_write.py`

**Step 1: Write failing test for streaming write**

Add to `tests/test_streaming_write.py`:

```python
class TestStreamingWrite:
    """Tests for write_geoparquet_via_duckdb function."""

    @pytest.fixture
    def output_file(self):
        """Create temp output path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_streaming_out_{id(self)}.parquet"
        yield str(tmp_path)
        if tmp_path.exists():
            tmp_path.unlink()

    @pytest.fixture
    def sample_input(self):
        """Create sample input parquet with geo metadata."""
        import duckdb
        import pyarrow as pa
        import pyarrow.parquet as pq

        tmp_path = Path(tempfile.gettempdir()) / f"test_streaming_in_{id(self)}.parquet"

        # Create via DuckDB
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        table = con.execute("""
            SELECT
                1 as id,
                ST_AsWKB(ST_Point(-122.4, 37.8)) as geometry
            UNION ALL
            SELECT
                2 as id,
                ST_AsWKB(ST_Point(-122.0, 38.0)) as geometry
        """).fetch_arrow_table()
        con.close()

        # Add geo metadata
        geo_meta = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "bbox": [-122.4, 37.8, -122.0, 38.0],
                }
            },
        }
        new_meta = {b"geo": json.dumps(geo_meta).encode("utf-8")}
        table = table.replace_schema_metadata(new_meta)
        pq.write_table(table, str(tmp_path))

        yield str(tmp_path)

        if tmp_path.exists():
            tmp_path.unlink()

    def test_streaming_write_basic(self, sample_input, output_file):
        """Test basic streaming write preserves data and metadata."""
        import duckdb
        from geoparquet_io.core.common import (
            get_parquet_metadata,
            write_geoparquet_via_duckdb,
        )

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")

        original_metadata = get_parquet_metadata(sample_input)
        query = f"SELECT * FROM read_parquet('{sample_input}')"

        write_geoparquet_via_duckdb(
            con=con,
            query=query,
            output_path=output_file,
            geometry_column="geometry",
            original_metadata=original_metadata,
            geoparquet_version="1.1",
            preserve_bbox=True,
            preserve_geometry_types=True,
        )
        con.close()

        # Verify output
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows == 2

        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata

        geo = json.loads(metadata[b"geo"].decode("utf-8"))
        assert geo["version"] == "1.1.0"
        assert geo["columns"]["geometry"]["bbox"] == [-122.4, 37.8, -122.0, 38.0]

    def test_streaming_write_recalculates_bbox(self, sample_input, output_file):
        """Test that bbox is recalculated when preserve_bbox=False."""
        import duckdb
        from geoparquet_io.core.common import (
            get_parquet_metadata,
            write_geoparquet_via_duckdb,
        )

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")

        original_metadata = get_parquet_metadata(sample_input)
        # Filter to just one point
        query = f"SELECT * FROM read_parquet('{sample_input}') WHERE id = 1"

        write_geoparquet_via_duckdb(
            con=con,
            query=query,
            output_path=output_file,
            geometry_column="geometry",
            original_metadata=original_metadata,
            geoparquet_version="1.1",
            preserve_bbox=False,  # Force recalculation
            preserve_geometry_types=True,
        )
        con.close()

        # Verify bbox was recalculated (should be smaller, just one point)
        pf = pq.ParquetFile(output_file)
        geo = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))
        bbox = geo["columns"]["geometry"]["bbox"]

        # Should be point bbox, not original [-122.4, 37.8, -122.0, 38.0]
        assert bbox[0] == pytest.approx(-122.4, rel=1e-6)
        assert bbox[2] == pytest.approx(-122.4, rel=1e-6)  # xmax == xmin for single point
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_streaming_write.py::TestStreamingWrite -v`
Expected: FAIL with "cannot import name 'write_geoparquet_via_duckdb'"

**Step 3: Implement write_geoparquet_via_duckdb**

Add to `geoparquet_io/core/common.py`:

```python
def write_geoparquet_via_duckdb(
    con,
    query: str,
    output_path: str,
    geometry_column: str,
    original_metadata: dict | None = None,
    geoparquet_version: str = "1.1",
    preserve_bbox: bool = True,
    preserve_geometry_types: bool = True,
    input_crs: dict | None = None,
    compression: str = "zstd",
    compression_level: int = 15,
    verbose: bool = False,
) -> None:
    """
    Write GeoParquet using DuckDB COPY TO + footer rewrite.

    This streaming approach is memory-efficient for large datasets:
    1. Prepares geo metadata (preserving from input or marking for recompute)
    2. Runs SQL metadata queries if recalculation needed
    3. Uses DuckDB COPY TO for streaming write
    4. Rewrites footer to add GeoParquet metadata

    Args:
        con: DuckDB connection with spatial extension loaded
        query: SQL SELECT query to execute
        output_path: Path to output file (local only for now)
        geometry_column: Name of geometry column
        original_metadata: Metadata dict from input file
        geoparquet_version: Target version (1.0, 1.1, 2.0, parquet-geo-only)
        preserve_bbox: Whether to preserve bbox from input
        preserve_geometry_types: Whether to preserve geometry_types from input
        input_crs: CRS dict to add to output
        compression: Compression codec (zstd, gzip, snappy, lz4, none)
        compression_level: Compression level
        verbose: Whether to print verbose output
    """
    from geoparquet_io.core.logging_config import debug, success

    # Handle remote output via temp file
    if is_remote_url(output_path):
        import tempfile
        local_path = tempfile.mktemp(suffix=".parquet")
        upload_after = True
    else:
        local_path = output_path
        upload_after = False

    try:
        # 1. Prepare geo metadata
        geo_meta = prepare_geo_metadata_for_streaming(
            original_metadata=original_metadata,
            geometry_column=geometry_column,
            geoparquet_version=geoparquet_version,
            preserve_bbox=preserve_bbox,
            preserve_geometry_types=preserve_geometry_types,
            input_crs=input_crs,
        )

        # 2. Compute missing metadata via SQL if needed
        col_meta = geo_meta["columns"][geometry_column]

        if not preserve_bbox or "bbox" not in col_meta:
            if verbose:
                debug("Computing bbox via SQL...")
            bbox = compute_bbox_via_sql(con, query, geometry_column)
            if bbox:
                col_meta["bbox"] = bbox

        if not preserve_geometry_types or "geometry_types" not in col_meta:
            if verbose:
                debug("Computing geometry types via SQL...")
            types = compute_geometry_types_via_sql(con, query, geometry_column)
            col_meta["geometry_types"] = types

        # 3. Wrap query with WKB conversion
        final_query = _wrap_query_with_wkb_conversion(query, geometry_column)

        # 4. DuckDB COPY TO (streaming)
        # Map compression to DuckDB format
        compression_map = {
            "zstd": "ZSTD",
            "gzip": "GZIP",
            "snappy": "SNAPPY",
            "lz4": "LZ4",
            "none": "UNCOMPRESSED",
            "uncompressed": "UNCOMPRESSED",
        }
        duckdb_compression = compression_map.get(compression.lower(), "ZSTD")

        # Escape path for SQL
        escaped_path = local_path.replace("'", "''")

        copy_query = f"""
            COPY ({final_query})
            TO '{escaped_path}'
            (FORMAT PARQUET, COMPRESSION {duckdb_compression})
        """

        if verbose:
            debug(f"Writing via DuckDB COPY TO with {duckdb_compression} compression...")

        con.execute(copy_query)

        # 5. Rewrite footer with geo metadata
        if verbose:
            debug("Rewriting footer with GeoParquet metadata...")

        rewrite_footer_with_geo_metadata(local_path, geo_meta)

        if verbose:
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(local_path)
            success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")

        # 6. Upload if remote
        if upload_after:
            from geoparquet_io.core.common import upload_if_remote
            upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

    finally:
        # Clean up temp file if used
        if upload_after and Path(local_path).exists():
            Path(local_path).unlink()
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_streaming_write.py::TestStreamingWrite -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/core/common.py tests/test_streaming_write.py
git commit -m "Add write_geoparquet_via_duckdb streaming write function"
```

---

## Task 6: Integrate with write_parquet_with_metadata

**Files:**
- Modify: `geoparquet_io/core/common.py:2608-2670`
- Test: `tests/test_streaming_write.py`

**Step 1: Write test for integration**

Add to `tests/test_streaming_write.py`:

```python
class TestWriteIntegration:
    """Tests for write_parquet_with_metadata streaming integration."""

    @pytest.fixture
    def output_file(self):
        tmp_path = Path(tempfile.gettempdir()) / f"test_integration_{id(self)}.parquet"
        yield str(tmp_path)
        if tmp_path.exists():
            tmp_path.unlink()

    def test_use_streaming_flag_routes_correctly(self, output_file):
        """Test that use_streaming=True uses DuckDB path."""
        import duckdb
        from geoparquet_io.core.common import write_parquet_with_metadata

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")

        query = "SELECT 1 as id, ST_Point(-122.4, 37.8) as geometry"

        write_parquet_with_metadata(
            con=con,
            query=query,
            output_file=output_file,
            use_streaming=True,
            verbose=False,
        )
        con.close()

        # Verify output has geo metadata
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows == 1
        assert b"geo" in pf.schema_arrow.metadata
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_streaming_write.py::TestWriteIntegration -v`
Expected: FAIL (use_streaming parameter not recognized)

**Step 3: Modify write_parquet_with_metadata to support streaming**

Update `geoparquet_io/core/common.py` function `write_parquet_with_metadata`:

```python
def write_parquet_with_metadata(
    con,
    query,
    output_file,
    original_metadata=None,
    compression="ZSTD",
    compression_level=15,
    row_group_size_mb=None,
    row_group_rows=None,
    custom_metadata=None,
    verbose=False,
    show_sql=False,
    profile=None,
    geoparquet_version=None,
    input_crs=None,
    use_streaming=False,
    preserve_bbox=True,
    preserve_geometry_types=True,
):
    """
    Write a parquet file with proper compression and metadata handling.

    Supports two write paths:
    - Arrow path (default): Fetches as Arrow table, applies metadata in memory
    - Streaming path (use_streaming=True): Uses DuckDB COPY TO + footer rewrite

    The streaming path is more memory-efficient for large datasets.

    Args:
        con: DuckDB connection
        query: SQL query to execute
        output_file: Path to output file (local path or remote URL)
        original_metadata: Original metadata from source file
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB (Arrow path only)
        row_group_rows: Exact number of rows per row group (Arrow path only)
        custom_metadata: Optional dict with custom metadata (Arrow path only)
        verbose: Whether to print verbose output
        show_sql: Whether to print SQL statements before execution
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        input_crs: PROJJSON dict with CRS from input file
        use_streaming: Use streaming DuckDB COPY TO path (default: False)
        preserve_bbox: Preserve bbox from input (streaming path only)
        preserve_geometry_types: Preserve geometry_types from input (streaming path only)

    Returns:
        None
    """
    if use_streaming:
        # Detect geometry column for streaming path
        geometry_column = _detect_geometry_from_query(con, query, original_metadata, verbose)

        write_geoparquet_via_duckdb(
            con=con,
            query=query,
            output_path=output_file,
            geometry_column=geometry_column,
            original_metadata=original_metadata,
            geoparquet_version=geoparquet_version or "1.1",
            preserve_bbox=preserve_bbox,
            preserve_geometry_types=preserve_geometry_types,
            input_crs=input_crs,
            compression=compression,
            compression_level=compression_level,
            verbose=verbose,
        )
    else:
        # Delegate to the Arrow-based implementation
        write_geoparquet_via_arrow(
            con=con,
            query=query,
            output_file=output_file,
            geometry_column=None,  # Auto-detect
            original_metadata=original_metadata,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            custom_metadata=custom_metadata,
            verbose=verbose,
            show_sql=show_sql,
            profile=profile,
            geoparquet_version=geoparquet_version,
            input_crs=input_crs,
        )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_streaming_write.py::TestWriteIntegration -v`
Expected: PASS

**Step 5: Run full test suite to ensure no regressions**

Run: `pytest -n auto -m "not slow and not network" -q`
Expected: All tests pass

**Step 6: Commit**

```bash
git add geoparquet_io/core/common.py tests/test_streaming_write.py
git commit -m "Integrate streaming write path into write_parquet_with_metadata"
```

---

## Task 7: Add Comparison Test (Arrow vs Streaming Output)

**Files:**
- Test: `tests/test_streaming_write.py`

**Step 1: Write comparison test**

Add to `tests/test_streaming_write.py`:

```python
class TestOutputComparison:
    """Tests comparing Arrow and streaming output."""

    def test_streaming_matches_arrow_output(self):
        """Verify streaming and Arrow paths produce equivalent output."""
        import duckdb
        from geoparquet_io.core.common import write_parquet_with_metadata

        arrow_path = Path(tempfile.gettempdir()) / f"arrow_out_{id(self)}.parquet"
        streaming_path = Path(tempfile.gettempdir()) / f"streaming_out_{id(self)}.parquet"

        try:
            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial")

            query = """
                SELECT 1 as id, ST_Point(-122.4, 37.8) as geometry
                UNION ALL
                SELECT 2 as id, ST_Point(-122.0, 38.0) as geometry
            """

            # Write via Arrow path
            write_parquet_with_metadata(
                con=con,
                query=query,
                output_file=str(arrow_path),
                use_streaming=False,
                geoparquet_version="1.1",
            )

            # Write via streaming path
            write_parquet_with_metadata(
                con=con,
                query=query,
                output_file=str(streaming_path),
                use_streaming=True,
                geoparquet_version="1.1",
                preserve_bbox=False,  # Compute fresh for fair comparison
                preserve_geometry_types=False,
            )
            con.close()

            # Compare outputs
            arrow_pf = pq.ParquetFile(str(arrow_path))
            streaming_pf = pq.ParquetFile(str(streaming_path))

            # Same row count
            assert arrow_pf.metadata.num_rows == streaming_pf.metadata.num_rows

            # Both have geo metadata
            arrow_geo = json.loads(arrow_pf.schema_arrow.metadata[b"geo"].decode("utf-8"))
            streaming_geo = json.loads(streaming_pf.schema_arrow.metadata[b"geo"].decode("utf-8"))

            # Same version
            assert arrow_geo["version"] == streaming_geo["version"]

            # Same geometry types
            assert (
                arrow_geo["columns"]["geometry"]["geometry_types"]
                == streaming_geo["columns"]["geometry"]["geometry_types"]
            )

            # Bbox values should be very close
            arrow_bbox = arrow_geo["columns"]["geometry"]["bbox"]
            streaming_bbox = streaming_geo["columns"]["geometry"]["bbox"]
            for i in range(4):
                assert arrow_bbox[i] == pytest.approx(streaming_bbox[i], rel=1e-6)

        finally:
            if arrow_path.exists():
                arrow_path.unlink()
            if streaming_path.exists():
                streaming_path.unlink()
```

**Step 2: Run test**

Run: `pytest tests/test_streaming_write.py::TestOutputComparison -v`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/test_streaming_write.py
git commit -m "Add test comparing Arrow and streaming output equivalence"
```

---

## Task 8: Add Large File Integration Test

**Files:**
- Test: `tests/test_streaming_write.py`

**Step 1: Write large file test (marked as slow)**

Add to `tests/test_streaming_write.py`:

```python
@pytest.mark.slow
class TestLargeFileStreaming:
    """Integration tests with large files."""

    def test_japan_file_streaming_write(self):
        """Test streaming write with real large file."""
        import duckdb
        from geoparquet_io.core.common import (
            get_parquet_metadata,
            write_parquet_with_metadata,
        )

        input_file = "/Users/cholmes/geodata/parquet-test-data/japan.parquet"

        # Skip if test file doesn't exist
        if not Path(input_file).exists():
            pytest.skip(f"Test file not found: {input_file}")

        output_path = Path(tempfile.gettempdir()) / f"japan_streaming_{id(self)}.parquet"

        try:
            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial")

            original_metadata = get_parquet_metadata(input_file)
            query = f"SELECT * FROM read_parquet('{input_file}')"

            # Write via streaming path
            write_parquet_with_metadata(
                con=con,
                query=query,
                output_file=str(output_path),
                original_metadata=original_metadata,
                use_streaming=True,
                preserve_bbox=True,
                preserve_geometry_types=True,
                verbose=True,
            )
            con.close()

            # Verify output
            assert output_path.exists()
            pf = pq.ParquetFile(str(output_path))
            assert pf.metadata.num_rows > 0
            assert b"geo" in pf.schema_arrow.metadata

        finally:
            if output_path.exists():
                output_path.unlink()
```

**Step 2: Run test**

Run: `pytest tests/test_streaming_write.py::TestLargeFileStreaming -v -m slow`
Expected: PASS (if test file exists)

**Step 3: Commit**

```bash
git add tests/test_streaming_write.py
git commit -m "Add large file integration test for streaming write"
```

---

## Task 9: Update Documentation

**Files:**
- Create: `docs/guide/streaming-write.md`
- Modify: `docs/api/python-api.md`

**Step 1: Create streaming write guide**

Create `docs/guide/streaming-write.md`:

```markdown
# Streaming Write Mode

For large datasets that don't fit in memory, gpio supports a streaming write mode that uses DuckDB's native COPY TO command with footer metadata rewriting.

## When to Use Streaming Mode

Use streaming mode when:
- Processing files larger than available RAM
- Memory efficiency is critical
- You don't need custom row group sizing

## Python API

```python
import geoparquet_io as gpio
from geoparquet_io.core.common import write_parquet_with_metadata, get_duckdb_connection

# Get connection and metadata
con = get_duckdb_connection(load_spatial=True)
original_metadata = gpio.get_parquet_metadata("input.parquet")

# Write with streaming mode
write_parquet_with_metadata(
    con=con,
    query="SELECT * FROM read_parquet('input.parquet')",
    output_file="output.parquet",
    original_metadata=original_metadata,
    use_streaming=True,
    preserve_bbox=True,  # Keep bbox from input
    preserve_geometry_types=True,  # Keep geometry types from input
)
```

## Metadata Preservation

By default, streaming mode preserves metadata from the input file:

| Flag | Default | Effect |
|------|---------|--------|
| `preserve_bbox` | True | Keep bbox from input |
| `preserve_geometry_types` | True | Keep geometry types from input |

Set to `False` to force recalculation (e.g., after spatial filtering).

## Limitations

- Local files only (remote outputs use temp file + upload)
- No custom row group sizing (uses DuckDB defaults)
- No custom metadata injection (use Arrow path for that)
```

**Step 2: Run docs build to verify**

Run: `cd docs && mkdocs build --strict` (if mkdocs is installed)
Expected: No errors

**Step 3: Commit**

```bash
git add docs/guide/streaming-write.md
git commit -m "Add streaming write documentation"
```

---

## Task 10: Final Verification and Cleanup

**Step 1: Run full test suite**

Run: `pytest -n auto -m "not slow and not network" -q`
Expected: All tests pass with coverage >= 67%

**Step 2: Run linting**

Run: `pre-commit run --all-files`
Expected: All checks pass

**Step 3: Check complexity**

Run: `xenon --max-absolute=A --max-modules=A --max-average=A geoparquet_io/core/common.py`
Expected: Pass (or note any new complexity to address)

**Step 4: Create summary commit**

```bash
git log --oneline -10
```

Review commits are clean and logical.

---

## Summary

This plan implements:
1. **fastparquet dependency** for footer manipulation
2. **rewrite_footer_with_geo_metadata()** - footer rewrite helper
3. **compute_bbox_via_sql()** / **compute_geometry_types_via_sql()** - SQL metadata computation
4. **prepare_geo_metadata_for_streaming()** - metadata preparation with preservation flags
5. **write_geoparquet_via_duckdb()** - main streaming write function
6. **write_parquet_with_metadata() integration** - `use_streaming` flag
7. **Comprehensive tests** including large file and output comparison
8. **Documentation** for the new feature
