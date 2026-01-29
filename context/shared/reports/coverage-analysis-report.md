# Test Coverage Analysis Report
**Generated:** 2026-01-18
**Overall Coverage:** 72.78% (below 75% threshold)
**Test Suite:** Fast tests only (excluding slow and network tests)

---

## Executive Summary

The project is **2.22 percentage points below the 75% threshold**, with 3,400 missing lines out of 12,493 total statements. The primary coverage gaps are in:

1. **Core utility functions** (common.py) - 74.0% coverage
2. **API check results** (api/check.py) - 40.0% coverage
3. **Cloud upload functionality** (core/upload.py) - 61.8% coverage
4. **Metadata management** (core/metadata_utils.py) - 58.4% coverage

**Good news:** The Python API is relatively well-tested (75-84% coverage for most modules), with clear gaps in specific methods that can be addressed systematically.

---

## Coverage by Module Category

### CRITICAL - Below 75% Threshold

| Module | Coverage | Missing Lines | Status |
|--------|----------|---------------|--------|
| **api/check.py** | 40.0% | 36 | 🔴 Critical - heavily used API |
| **core/metadata_utils.py** | 58.4% | 225 | 🔴 Critical - metadata operations |
| **core/upload.py** | 61.8% | 109 | 🔴 High - cloud integration |
| **core/reproject.py** | 65.8% | 67 | 🔴 High - CRS transformation |
| **core/streaming.py** | 72.4% | 56 | 🟡 Medium - large files |
| **core/common.py** | 74.0% | 325 | 🔴 Critical - used everywhere |
| **core/partition_by_kdtree.py** | 74.4% | 20 | 🟡 Low impact - small gap |

### Acceptable Coverage (75-85%)

| Module | Coverage | Missing Lines |
|--------|----------|---------------|
| core/extract.py | 75.1% | 106 |
| core/validate.py | 75.9% | 227 |
| core/format_writers.py | 76.3% | 36 |
| core/add_kdtree_column.py | 77.4% | 40 |
| core/partition_by_h3.py | 77.5% | 16 |
| core/partition_reader.py | 79.0% | 16 |
| **api/table.py** | 79.6% | 94 |
| core/geojson_stream.py | 81.4% | 36 |
| core/convert.py | 82.1% | 79 |
| cli/main.py | 82.3% | 262 |
| core/check_parquet_structure.py | 82.6% | 33 |
| core/inspect_utils.py | 83.1% | 114 |
| **api/ops.py** | 84.3% | 14 |

### Enterprise/External Services (Lower Priority)

These modules require external services and are lower priority for v1.0-beta.1:

| Module | Coverage | Description |
|--------|----------|-------------|
| core/extract_bigquery.py | 30.1% | BigQuery extraction |
| core/add_country_codes.py | 14.0% | Country code enrichment |
| core/partition_admin_hierarchical.py | 13.4% | Admin hierarchical partitioning |
| api/stac.py | 13.2% | STAC catalog integration |
| core/arcgis.py | 54.8% | ArcGIS feature services |

---

## Python API Coverage Analysis

### Table Class Methods (30/40 tested = 75.0%)

**UNTESTED methods (10):**
- ❌ `add_admin_divisions` - Admin divisions enrichment
- ❌ `from_bigquery` - BigQuery integration
- ❌ `geometry_column` - Property to get geometry column name
- ❌ `num_rows` - Property to get row count
- ❌ `partition_by_admin` - Admin boundary partitioning
- ❌ `partition_by_h3` - H3 hexagon partitioning
- ❌ `partition_by_kdtree` - K-D tree partitioning
- ❌ `partition_by_quadkey` - Quadkey partitioning
- ❌ `partition_by_string` - String-based partitioning
- ❌ `table` - Property to access underlying Arrow table

**Well-tested methods (30):**
- ✅ Core operations: `read`, `write`, `extract`, `convert`
- ✅ Spatial indexing: `add_bbox`, `add_quadkey`, `add_h3`, `add_kdtree`
- ✅ Sorting: `sort_hilbert`, `sort_column`, `sort_quadkey`
- ✅ Inspection: `head`, `tail`, `info`, `stats`, `metadata`
- ✅ Properties: `crs`, `bounds`, `schema`, `geoparquet_version`, `column_names`
- ✅ Checks: `check`, `check_bbox`, `check_compression`, `check_row_groups`, `check_spatial`, `validate`
- ✅ Transformations: `reproject`, `to_arrow`, `to_geojson`
- ✅ Cloud: `upload`

### ops Module Functions (13/16 tested = 81.2%)

**UNTESTED functions (3):**
- ❌ `convert_to_geojson` - GeoJSON conversion via ops
- ❌ `from_arcgis` - ArcGIS feature service integration
- ❌ `read_bigquery` - BigQuery reader

**Well-tested functions (13):**
- ✅ All spatial indexing functions
- ✅ All sorting functions
- ✅ Most conversion functions (geopackage, flatgeobuf, csv, shapefile)
- ✅ Core operations (extract, reproject)

---

## Critical Untested Functions by Module

### 🔴 api/check.py (40.0% coverage)
**Impact:** CRITICAL - Used by all check commands and Python API

Untested functionality:
- `warnings` property (13 lines) - Returns list of warning messages
- `recommendations` property (9 lines) - Returns list of recommendations
- `failures` property (6 lines) - Returns list of failure messages
- `passed` property (4 lines) - Returns list of passed checks
- `__repr__` method (4 lines) - String representation

**Why critical:** Users rely on check results for validation workflows. Properties for accessing results are untested.

### 🔴 core/common.py (74.0% coverage)
**Impact:** CRITICAL - Shared utilities used throughout codebase

Top untested functions:
- `apply_crs_to_parquet` (33 lines) - Apply CRS to parquet file
- `_strip_geoarrow_to_plain_wkb` (20 lines) - Convert GeoArrow to WKB
- `add_crs_to_geoparquet_metadata` (19 lines) - Add CRS to metadata
- `calculate_file_bounds` (18 lines) - Calculate spatial bounds
- `write_geoparquet_table` (17 lines) - Write table with metadata
- `get_duckdb_connection_for_s3` (15 lines) - S3-enabled DuckDB connection
- `detect_crs_from_spatial_file` (11 lines) - Auto-detect CRS
- `remote_write_context` (8 lines) - Context manager for remote writes
- `upload_if_remote` (7 lines) - Upload helper

**Why critical:** These are foundational utilities. Missing tests mean edge cases aren't covered.

### 🔴 core/upload.py (61.8% coverage)
**Impact:** HIGH - Cloud storage integration

Top untested functions:
- `_check_azure_credentials` (17 lines) - Validate Azure credentials
- `_check_s3_credentials` (15 lines) - Validate S3 credentials
- `_upload_directory_sync` (15 lines) - Directory upload sync
- `_upload_one_file` (14 lines) - Single file upload
- `_check_gcs_credentials` (12 lines) - Validate GCS credentials
- `_upload_file_sync` (8 lines) - File upload helper
- `_print_upload_summary` (6 lines) - Upload summary output

**Why critical:** Users need reliable cloud uploads. Credential validation and error handling untested.

### 🔴 core/reproject.py (65.8% coverage)
**Impact:** HIGH - CRS transformation

Top untested functions:
- `_reproject_streaming` (36 lines) - Streaming reprojection for large files
- `reproject_impl` (12 lines) - Core reprojection logic
- `_detect_crs_from_table` (6 lines) - Auto-detect source CRS
- `reproject` (5 lines) - Main entry point
- Error handling for invalid CRS

**Why critical:** CRS transformation is complex and error-prone. Missing tests for streaming and edge cases.

### 🟡 core/streaming.py (72.4% coverage)
**Impact:** MEDIUM - Large file processing

Untested areas:
- Error conditions during streaming
- Multi-partition streaming workflows
- Memory limit handling
- Cleanup on failure

**Why important:** Streaming is critical for large datasets, but edge cases are untested.

### 🔴 api/table.py (79.6% coverage - above threshold but gaps)
**Impact:** HIGH - Primary user-facing API

Untested methods with missing lines:
- `_write_format` (21 lines) - Format conversion helper
- `stats` (17 lines) - Statistical summary (partially tested)
- `_with_temp_io_files` (11 lines) - Temp file management
- `from_bigquery` (7 lines) - BigQuery integration
- `add_bbox_metadata` (7 lines) - Bbox metadata helper (partially tested)
- All partition methods (2 lines each) - Simple wrappers but untested

---

## Suggested Test Additions for v1.0-beta.1

### Priority 1: Critical API Gaps (Blocking 75% threshold)

**api/check.py** - Add 5 tests (~30 minutes):
```python
def test_check_result_warnings():
    # Test warnings property returns list
def test_check_result_recommendations():
    # Test recommendations property returns list
def test_check_result_failures():
    # Test failures property returns list
def test_check_result_passed():
    # Test passed property returns list
def test_check_result_repr():
    # Test string representation
```
**Impact:** +15% to api/check.py → 55% coverage

**api/table.py** - Add 10 tests (~1 hour):
```python
# Properties
def test_table_num_rows():
def test_table_geometry_column():
def test_table_table_property():

# Partitioning methods
def test_partition_by_admin():
def test_partition_by_h3():
def test_partition_by_kdtree():
def test_partition_by_quadkey():
def test_partition_by_string():

# Admin enrichment
def test_add_admin_divisions():

# Stats edge cases
def test_stats_with_null_values():
```
**Impact:** +10% to api/table.py → 89% coverage

**api/ops.py** - Add 3 tests (~20 minutes):
```python
def test_convert_to_geojson():
def test_from_arcgis():  # May require mocking
def test_read_bigquery():  # May require mocking
```
**Impact:** +10% to api/ops.py → 94% coverage

### Priority 2: Core Utilities (Critical for stability)

**core/common.py** - Add 15 tests (~2 hours):
```python
# CRS operations
def test_apply_crs_to_parquet():
def test_add_crs_to_geoparquet_metadata():
def test_detect_crs_from_spatial_file():

# Metadata operations
def test_write_geoparquet_table():
def test_calculate_file_bounds():
def test_parse_geo_metadata_edge_cases():

# S3 operations
def test_get_duckdb_connection_for_s3():
def test_remote_write_context():
def test_upload_if_remote():

# Data conversion
def test_strip_geoarrow_to_plain_wkb():

# Connection management
def test_get_duckdb_connection_with_extensions():
def test_get_duckdb_connection_error_handling():

# Compression validation
def test_validate_compression_edge_cases():

# Bounds calculation
def test_calculate_row_group_size():
def test_estimate_row_size():
```
**Impact:** +15% to core/common.py → 89% coverage

### Priority 3: Cloud and Transform Operations

**core/upload.py** - Add 8-10 tests (~1.5 hours, requires mocking):
```python
# Credential validation
def test_check_s3_credentials_valid():
def test_check_s3_credentials_missing():
def test_check_azure_credentials_valid():
def test_check_gcs_credentials_valid():

# Upload operations
def test_upload_one_file():
def test_upload_file_sync():
def test_upload_directory_sync():

# Error handling
def test_upload_with_invalid_credentials():
def test_upload_summary_output():
```
**Impact:** +20% to core/upload.py → 82% coverage

**core/reproject.py** - Add 6-8 tests (~1 hour):
```python
# Streaming operations
def test_reproject_streaming():
def test_reproject_streaming_large_file():

# CRS detection
def test_detect_crs_from_table():
def test_detect_geometry_column_from_table():

# Edge cases
def test_reproject_invalid_crs():
def test_reproject_missing_geometry():
def test_reproject_impl_coordinates():
```
**Impact:** +20% to core/reproject.py → 86% coverage

**core/streaming.py** - Add 6-8 tests (~1 hour):
```python
# Error conditions
def test_streaming_with_memory_limit():
def test_streaming_partition_failure():
def test_streaming_cleanup_on_error():

# Multi-partition
def test_streaming_multiple_partitions():
def test_streaming_partition_coordination():

# Edge cases
def test_streaming_empty_partitions():
```
**Impact:** +15% to core/streaming.py → 87% coverage

---

## Summary of Recommended Work

### Quick Wins (2-3 hours)
1. **api/check.py** - 5 tests → Raises from 40% to 55%
2. **api/table.py** - 10 tests → Raises from 79.6% to 89%
3. **api/ops.py** - 3 tests → Raises from 84.3% to 94%

**Result:** API fully tested, ~18 tests added

### Core Utilities (3-4 hours)
4. **core/common.py** - 15 tests → Raises from 74% to 89%

**Result:** Crosses 75% threshold, foundational utilities tested

### Extended Coverage (4-5 hours)
5. **core/upload.py** - 10 tests → Raises from 61.8% to 82%
6. **core/reproject.py** - 8 tests → Raises from 65.8% to 86%
7. **core/streaming.py** - 8 tests → Raises from 72.4% to 87%

**Result:** All critical modules above 80%

### Total Estimate
- **~50-60 new tests**
- **~10-12 hours of work**
- **Expected outcome:** Overall coverage from 72.8% → 82-85%

---

## What NOT to Test for v1.0-beta.1

**External service integrations** (low priority unless needed):
- BigQuery extraction (30% coverage) - Requires BigQuery access
- ArcGIS feature services (55% coverage) - Requires ArcGIS server
- STAC catalog (13% coverage) - Requires STAC server
- Country code enrichment (14% coverage) - Complex admin data
- Admin hierarchical partitioning (13% coverage) - Complex admin data

**Rationale:** These modules require external services or complex test data. Focus on core functionality that users rely on daily.

---

## Testability Assessment

### Easy to Test (No external dependencies)
- ✅ api/check.py - Pure Python objects
- ✅ api/table.py properties - Simple getters
- ✅ core/common.py utility functions - Pure functions
- ✅ core/streaming.py - Can use small fixtures

### Moderate Difficulty (Requires mocking)
- 🟡 core/upload.py - Mock cloud storage clients
- 🟡 core/reproject.py - Need coordinate transformation fixtures
- 🟡 api/ops conversion functions - May need temp files

### Difficult (Requires external services)
- 🔴 BigQuery functions - Need BigQuery instance or mock
- 🔴 ArcGIS functions - Need feature service or mock
- 🔴 STAC functions - Need STAC catalog

---

## Conclusion

The project is close to the 75% threshold with clear, actionable gaps. The highest ROI work is:

1. **Complete api/check.py testing** (critical, heavily used, easy to test)
2. **Add missing Table/ops API tests** (user-facing, easy to test)
3. **Fill core/common.py gaps** (used everywhere, moderate effort)
4. **Add upload/reproject tests** (important features, require mocking)

Following the Priority 1-2 recommendations will push overall coverage above 75% and ensure core API stability for v1.0-beta.1.
