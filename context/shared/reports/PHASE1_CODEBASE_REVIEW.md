# Phase 1: GPIO Codebase Surface Area Mapping

**Date**: 2026-01-18
**Purpose**: Pre-cleanup comprehensive review for v1.0-beta.1
**Scope**: Complete mapping of CLI commands, Python API, and architecture

---

## Executive Summary

GPIO is a mature GeoParquet processing library with:
- **64 functions** defined across CLI/core modules
- **44 core modules** (~2200 lines in cli/main.py, ~1400 in core/common.py)
- **77 utility functions** in core/common.py alone
- **Both CLI and Python API** with partial parity
- **Well-structured** with clear CLI/core separation

### Architecture Quality
- ✅ Clean CLI/core separation (thin wrappers in CLI, logic in core)
- ✅ Extensive reusable decorators (compression, row groups, partitioning)
- ✅ Comprehensive utility library (core/common.py)
- ⚠️ ~2200 line cli/main.py suggests potential for modularization
- ⚠️ Python API incomplete compared to CLI surface area

---

## Python API Surface Area

### Entry Points (`geoparquet_io/__init__.py`)

**Top-level exports**:
```python
read()              # Read GeoParquet → Table
read_partition()    # Read Hive-partitioned data → Table
read_bigquery()     # BigQuery → Table (via ops module)
convert()           # File conversion → Table
extract_arcgis()    # ArcGIS Feature Service → Table
Table              # Fluent API class
pipe()             # Pipeline composition helper
ops                # Functional API module
CheckResult        # Validation results
generate_stac()    # STAC metadata generation
validate_stac()    # STAC validation
```

### Fluent API (`api/table.py` - Table class)

**Properties** (read-only):
- `table` - Underlying PyArrow Table
- `geometry_column` - Geometry column name
- `num_rows` - Row count
- `column_names` - List of column names
- `crs` - Coordinate Reference System
- `bounds` - Bounding box (xmin, ymin, xmax, ymax)
- `schema` - PyArrow Schema
- `geoparquet_version` - GeoParquet version string

**Transformation methods** (return new Table):
```python
# Column operations
.add_bbox(column_name="bbox")
.add_quadkey(column_name="quadkey", resolution=13, use_centroid=False)
.add_h3(column_name="h3_cell", resolution=9)
.add_kdtree(column_name="kdtree_cell", iterations=9, sample_size=100000)
.add_bbox_metadata(bbox_column="bbox")
.add_admin_divisions(dataset="overture", levels=None)

# Sorting operations
.sort_hilbert()
.sort_column(column_name, descending=False)
.sort_quadkey(column_name="quadkey", resolution=13, use_centroid=False, remove_column=False)

# Filtering/extraction
.extract(columns=None, exclude_columns=None, bbox=None, where=None, limit=None)
.head(n=10)
.tail(n=10)

# Reprojection
.reproject(target_crs="EPSG:4326", source_crs=None)
```

**Output methods**:
```python
# Write operations
.write(path, format=None, compression="ZSTD", compression_level=None, ...)
.upload(destination, compression="ZSTD", s3_endpoint=None, ...)
.to_arrow()
.to_geojson(output_path=None, precision=7, write_bbox=False, id_field=None)

# Partitioning operations (return dict with stats)
.partition_by_quadkey(output_dir, resolution=13, partition_resolution=6, ...)
.partition_by_h3(output_dir, resolution=9, compression="ZSTD", ...)
.partition_by_string(output_dir, column, chars=None, hive=True, ...)
.partition_by_kdtree(output_dir, iterations=9, hive=True, ...)
.partition_by_admin(output_dir, dataset="gaul", levels=None, ...)
```

**Inspection methods**:
```python
.info(verbose=True)                        # Summary or dict
.stats()                                   # Column statistics
.metadata(include_parquet_metadata=False)  # GeoParquet metadata
```

**Validation methods** (return CheckResult):
```python
.check()                                   # All best-practice checks
.check_spatial(sample_size=100, limit_rows=100000)
.check_compression()
.check_bbox()
.check_row_groups()
.validate(version=None)                    # GeoParquet spec compliance
```

**Class methods**:
```python
Table.from_bigquery(table_id, project=None, credentials_file=None, ...)
```

### Functional API (`api/ops.py`)

**All functions accept/return `pa.Table`**:

```python
# Transformations
add_bbox(table, column_name="bbox", geometry_column=None)
add_quadkey(table, column_name="quadkey", resolution=13, use_centroid=False, geometry_column=None)
add_h3(table, column_name="h3_cell", resolution=9, geometry_column=None)
add_kdtree(table, column_name="kdtree_cell", iterations=9, sample_size=100000, geometry_column=None)

sort_hilbert(table, geometry_column=None)
sort_column(table, column, descending=False)
sort_quadkey(table, column_name="quadkey", resolution=13, use_centroid=False, remove_column=False)

extract(table, columns=None, exclude_columns=None, bbox=None, where=None, limit=None, geometry_column=None)

reproject(table, target_crs="EPSG:4326", source_crs=None, geometry_column=None)

# Data sources
read_bigquery(table_id, project=None, credentials_file=None, where=None, bbox=None, ...)
from_arcgis(service_url, token=None, where="1=1", bbox=None, ...)

# Format conversions
convert_to_geojson(table, output_path=None, rs=True, precision=7, write_bbox=False, id_field=None)
convert_to_geopackage(table, output_path, overwrite=False, layer_name="features")
convert_to_flatgeobuf(table, output_path)
convert_to_csv(table, output_path, include_wkt=True, include_bbox=True)
convert_to_shapefile(table, output_path, overwrite=False, encoding="UTF-8")
```

---

## CLI Surface Area

### Top-Level Commands

```
gpio [--version] [--timestamps] [--help]
  ├── convert     - Convert between formats and CRS
  ├── extract     - Extract data from files/services
  ├── inspect     - Inspect metadata, preview, stats
  ├── add         - Enhance GeoParquet files
  ├── sort        - Sort GeoParquet files
  ├── partition   - Partition GeoParquet files
  ├── check       - Check best practices
  ├── publish     - Publish data (STAC, upload)
  └── benchmark   - Performance benchmarking
```

### Convert Group (7 subcommands)

**Auto-detects format from extension**, or use explicit subcommands:

```bash
gpio convert [INPUT] [OUTPUT]  # Auto-detect from extension
gpio convert geoparquet [INPUT] [OUTPUT]  # Force GeoParquet
gpio convert geopackage [INPUT] [OUTPUT]
gpio convert flatgeobuf [INPUT] [OUTPUT]
gpio convert csv [INPUT] [OUTPUT]
gpio convert shapefile [INPUT] [OUTPUT]
gpio convert geojson [INPUT] [OUTPUT] [--precision=7] [--write-bbox] [--id-field=...]
gpio convert reproject [INPUT] [OUTPUT] -d/--destination-crs EPSG:XXXX [--source-crs=...]
```

**Common options**:
- `--compression`, `--compression-level`
- `--row-group-size`, `--row-group-size-mb`
- `--geoparquet-version` (1.0, 1.1, 2.0, parquet-geo-only)
- `--profile` (AWS profile for S3)
- `--verbose`, `--dry-run`

### Extract Group (3 subcommands)

```bash
gpio extract geoparquet [INPUT] [OUTPUT]
    [--columns=...] [--exclude-columns=...]
    [--bbox=minx,miny,maxx,maxy]
    [--where='SQL clause']
    [--limit=N]
    [--allow-schema-diff] [--hive-input]

gpio extract arcgis [URL] [OUTPUT]
    [--token=...] [--token-file=...] [--username=...] [--password=...]
    [--where='...'] [--bbox=...] [--limit=...]
    [--include-cols=...] [--exclude-cols=...]

gpio extract bigquery [TABLE_ID] [OUTPUT]
    [--project=...] [--credentials-file=...]
    [--where='...'] [--bbox=...]
    [--bbox-mode=auto|server|local] [--bbox-threshold=N]
    [--limit=...] [--columns=...] [--exclude-columns=...]
```

### Inspect Group (5 subcommands)

```bash
gpio inspect [FILE]                  # Default: summary
gpio inspect summary [FILE]          # Quick metadata
gpio inspect meta [FILE] [--geo] [--json]
gpio inspect head [FILE] [N=10]
gpio inspect tail [FILE] [N=10]
gpio inspect stats [FILE]
```

### Add Group (6 subcommands)

```bash
gpio add bbox [INPUT] [OUTPUT] [--column-name=bbox]
gpio add bbox-metadata [INPUT] [OUTPUT] [--bbox-column=bbox]
gpio add quadkey [INPUT] [OUTPUT] [--resolution=13] [--use-centroid]
gpio add h3 [INPUT] [OUTPUT] [--resolution=9]
gpio add kdtree [INPUT] [OUTPUT] [--iterations=9] [--sample-size=100000]
gpio add admin-divisions [INPUT] [OUTPUT]
    [--dataset=overture|gaul|URL]
    [--levels=country,admin1,admin2]
```

### Sort Group (3 subcommands)

```bash
gpio sort hilbert [INPUT] [OUTPUT]
gpio sort column [INPUT] [OUTPUT] [--columns=col1,col2] [--descending]
gpio sort quadkey [INPUT] [OUTPUT]
    [--resolution=13] [--use-centroid]
    [--column-name=quadkey] [--remove-column]
```

### Partition Group (5 subcommands)

```bash
gpio partition quadkey [INPUT] [OUTPUT_DIR]
    [--resolution=13] [--partition-resolution=6]
    [--hive] [--overwrite]
    [--preview] [--preview-limit=15]
    [--force] [--skip-analysis]

gpio partition h3 [INPUT] [OUTPUT_DIR]
    [--resolution=9]
    [--hive] [--overwrite]

gpio partition kdtree [INPUT] [OUTPUT_DIR]
    [--iterations=9]
    [--hive] [--overwrite]

gpio partition string [INPUT] [OUTPUT_DIR]
    [--column=COLUMN]
    [--chars=N]
    [--hive] [--overwrite]

gpio partition admin [INPUT] [OUTPUT_DIR]
    [--dataset=gaul|overture|URL]
    [--levels=country,admin1,admin2]
    [--hive] [--overwrite]
```

**Common partition options**:
- `--preview` - Dry-run analysis
- `--preview-limit=15` - Show first N partitions
- `--force` - Override warnings
- `--skip-analysis` - Skip validation
- `--prefix=...` - Custom filename prefix
- `--compression`, `--compression-level`

### Check Group (6 subcommands)

```bash
gpio check [FILE]                    # Default: all checks
gpio check all [FILE] [--fix]
gpio check bbox [FILE] [--fix]
gpio check compression [FILE] [--fix]
gpio check row-group [FILE] [--fix]
gpio check spatial [FILE]
    [--random-sample-size=100]
    [--limit-rows=100000]
gpio check spec [FILE]
    [--version=1.0|1.1|2.0]
    [--validate-data]
    [--sample-size=1000]
gpio check stac [STAC_JSON]
```

**Common check options**:
- `--fix` - Apply automatic fixes
- `--all-files` - Check all files in partition
- `--sample-files=N` - Check first N files

### Publish Group (2 subcommands)

```bash
gpio publish stac [INPUT] [OUTPUT_JSON]
    [--type=item|collection]
    [--title=...] [--description=...]
    [--license=...] [--id=...]

gpio publish upload [SOURCE] [DESTINATION]
    [--profile=...] [--s3-endpoint=...] [--s3-region=...]
    [--s3-use-ssl] [--chunk-concurrency=12]
```

---

## Reusable CLI Decorators (`cli/decorators.py`)

**Parameter decorators**:
- `@verbose_option` - Adds `--verbose/-v`
- `@dry_run_option` - Adds `--dry-run`
- `@show_sql_option` - Adds `--show-sql`
- `@overwrite_option` - Adds `--overwrite`
- `@any_extension_option` - Allows non-.parquet extensions
- `@profile_option` - AWS profile for S3
- `@bbox_option` - Auto-add bbox if missing
- `@prefix_option` - Partition filename prefix
- `@geoparquet_version_option` - Version selection

**Grouped decorators**:
- `@compression_options` - `--compression`, `--compression-level`
- `@row_group_options` - `--row-group-size`, `--row-group-size-mb`
- `@output_format_options` - Combines compression + row groups
- `@partition_options` - All partition flags (preview, force, hive, etc.)
- `@partition_input_options` - `--allow-schema-diff`, `--hive-input`
- `@check_partition_options` - `--all-files`, `--sample-files`

**Custom command classes**:
- `GlobAwareCommand` - Detects shell-expanded globs, provides helpful errors
- `SingleFileCommand` - For commands that don't support globs/partitions

---

## Core Architecture

### Core Modules (44 files)

**Key patterns**:
- `add_*.py` - Column addition logic (bbox, quadkey, h3, kdtree, admin)
- `partition_*.py` - Partitioning implementations (quadkey, h3, kdtree, string, admin)
- `check_*.py` - Validation logic (parquet structure, spatial order, fixes)
- `extract*.py` - Data extraction (geoparquet, bigquery, arcgis)
- `format_writers.py` - Output format conversions (GeoPackage, Shapefile, CSV, GeoJSON, FlatGeobuf)

**Core utilities** (`core/common.py` - 77 functions):

```python
# DuckDB connections
get_duckdb_connection(load_spatial=True, load_httpfs=None, use_s3_auth=False)
get_duckdb_connection_for_s3(...)

# File/URL handling
is_remote_url(path)
is_s3_url(path), is_azure_url(path), is_gcs_url(path)
has_glob_pattern(path)
is_partition_path(path)
safe_file_url(file_path, verbose=False)
needs_httpfs(path)

# Cloud storage
setup_aws_profile_if_needed(profile, *paths)
upload_if_remote(local_path, remote_path, ...)
remote_write_context(output_path, is_directory=False, verbose=False)

# Metadata operations
get_parquet_metadata(parquet_file, verbose=False)
parse_geo_metadata(metadata, verbose=False)
find_primary_geometry_column(parquet_file, verbose=False)
calculate_file_bounds(file_path, geom_column=None, verbose=False)
check_bbox_structure(parquet_file, verbose=False)

# Writing/validation
write_geoparquet_table(table, output_file, geometry_column=None, ...)
write_parquet_with_metadata(table, output_file, metadata_updates=None, ...)
validate_compression_settings(compression, compression_level)
validate_output_path(output_path, verbose=False)
validate_parquet_extension(output_file, any_extension=False)

# ... and ~50 more utility functions
```

---

## API/CLI Parity Analysis

### Complete Parity ✅

These operations exist in both CLI and Python API with equivalent functionality:

**Data Sources**:
- ✅ Read GeoParquet files
- ✅ Read partitioned data
- ✅ BigQuery extraction
- ✅ ArcGIS Feature Service extraction
- ✅ Format conversion (GeoJSON, Shapefile, CSV, GPKG, FlatGeobuf)

**Transformations**:
- ✅ Add bbox column
- ✅ Add quadkey column
- ✅ Add H3 column
- ✅ Add KD-tree column
- ✅ Add bbox metadata
- ✅ Add admin divisions
- ✅ Hilbert sorting
- ✅ Column sorting
- ✅ Quadkey sorting
- ✅ Extract/filter (columns, bbox, where, limit)
- ✅ Reprojection

**Partitioning**:
- ✅ Partition by quadkey
- ✅ Partition by H3
- ✅ Partition by KD-tree
- ✅ Partition by string column
- ✅ Partition by admin boundaries

**Validation**:
- ✅ Check all
- ✅ Check spatial order
- ✅ Check compression
- ✅ Check bbox
- ✅ Check row groups
- ✅ Validate GeoParquet spec
- ✅ Validate STAC

**Publishing**:
- ✅ Upload to cloud storage
- ✅ Generate STAC metadata

**Inspection**:
- ✅ Show metadata (via `.info()`, `.metadata()`)
- ✅ Show statistics (via `.stats()`)
- ✅ Head/tail (via `.head()`, `.tail()`)

### CLI-Only Features ⚠️

These exist in CLI but not Python API:

1. **`--fix` flag for checks** - Auto-fix detected issues
   - CLI: `gpio check all data.parquet --fix`
   - API: No equivalent (checks return CheckResult, but no auto-fix)

2. **Partition preview/analysis** - Dry-run partition strategy
   - CLI: `gpio partition quadkey data.parquet output/ --preview`
   - API: No preview mode (partition methods execute immediately)

3. **`--show-sql` flag** - Display executed SQL
   - CLI: `gpio extract data.parquet out.parquet --show-sql`
   - API: No SQL logging option

4. **GeoJSON streaming mode** - Output to stdout with RS separators
   - CLI: `gpio convert geojson data.parquet` (pipes to stdout)
   - API: `.to_geojson()` requires output_path or writes to stdout (but no streaming control)

5. **Benchmark command** - Performance testing
   - CLI: `gpio benchmark ...`
   - API: No equivalent

### Python API-Only Features ✅

These exist in Python API but not CLI:

1. **Method chaining** - Fluent composition
   ```python
   gpio.read('data.parquet').add_bbox().sort_hilbert().write('out.parquet')
   ```
   CLI requires multiple commands

2. **In-memory operations** - Work with Arrow tables directly
   ```python
   table = pq.read_table('data.parquet')
   table = ops.add_bbox(table)
   table = ops.sort_hilbert(table)
   ```
   CLI always reads/writes files

3. **Inspection without file I/O** - Properties on Table object
   ```python
   table.num_rows
   table.crs
   table.bounds
   ```
   CLI requires running `gpio inspect` commands

4. **Pipeline composition** - `pipe()` helper
   ```python
   from geoparquet_io import pipe
   table = pipe(
       gpio.read('data.parquet'),
       lambda t: t.add_bbox(),
       lambda t: t.sort_hilbert(),
   )
   ```

---

## Inconsistencies & Issues

### Naming Inconsistencies

1. **Quadkey column name parameter**:
   - CLI add: `--column-name` (flag)
   - CLI sort: `--column-name` (flag)
   - Python add: `column_name` (param)
   - Python sort: `column_name` (param)
   - ✅ Consistent within each interface

2. **Resolution parameters**:
   - Quadkey: `--resolution` (CLI) / `resolution=` (Python)
   - H3: `--resolution` (CLI) / `resolution=` (Python)
   - ✅ Consistent

3. **Compression options**:
   - CLI: `--compression`, `--compression-level`
   - Python: `compression=`, `compression_level=`
   - ✅ Consistent

4. **BigQuery bbox filtering**:
   - CLI: `--bbox-mode`, `--bbox-threshold`
   - Python: `bbox_mode=`, `bbox_threshold=`
   - ✅ Consistent

### Parameter Type Mismatches

1. **Column lists**:
   - CLI extract: `--columns=col1,col2,col3` (comma-separated string)
   - Python extract: `columns=['col1', 'col2', 'col3']` (Python list)
   - ✅ Appropriate for each interface

2. **Bbox input**:
   - CLI: `--bbox=-122.5,37.5,-122.0,38.0` (comma-separated string)
   - Python: `bbox=(-122.5, 37.5, -122.0, 38.0)` (tuple)
   - ✅ Appropriate for each interface

### Missing Documentation

From CLAUDE.md requirement: "Every pull request must include documentation updates"

Areas needing documentation review:
1. Some Python API methods lack examples in docstrings
2. CLI help text generally good, but some advanced options lack examples
3. Error messages could be more helpful (especially for cloud storage auth)

---

## Code Quality Observations

### Strengths ✅

1. **Excellent separation of concerns**:
   - CLI is thin wrapper over core logic
   - Core functions are reusable
   - Clean architecture enables both CLI and Python API

2. **Comprehensive test coverage**:
   - Project maintains 75% coverage threshold
   - Test markers (`@pytest.mark.slow`, `@pytest.mark.network`)
   - Windows compatibility handled

3. **Extensive utilities**:
   - 77 functions in core/common.py
   - Handles remote files, cloud storage, metadata
   - Robust error handling with helpful hints

4. **Reusable decorators**:
   - DRY principle applied well
   - Consistent option names across commands
   - Grouped decorators for related options

5. **Logging system**:
   - Centralized logging (core/logging_config.py)
   - No `click.echo()` in core modules (enforced by pre-commit)
   - Proper verbose mode support

### Areas for Improvement ⚠️

1. **File size**:
   - cli/main.py: ~2200 lines
   - core/common.py: ~1400 lines
   - Xenon complexity checks needed

2. **Python API completeness**:
   - Missing `--fix` equivalent for checks
   - Missing partition preview/dry-run
   - Missing SQL logging option
   - Missing benchmark functionality

3. **Potential refactoring**:
   - cli/main.py could be split into submodule files
   - Some core modules have overlapping concerns
   - Partition implementations share common patterns

4. **Documentation gaps**:
   - Not all Python API methods have both CLI/Python examples
   - Some advanced features underdocumented
   - Migration guides for deprecated commands?

5. **Error handling**:
   - Cloud storage auth errors could be more actionable
   - Some DuckDB errors bubble up raw
   - File locking issues (Windows) partially handled

---

## Recommendations for v1.0-beta.1

### High Priority

1. **API Parity**:
   - Add `CheckResult.fix()` method to enable auto-fixes from Python
   - Add `preview=True` parameter to partition methods
   - Add `show_sql=True` parameter to operations that use DuckDB
   - Consider adding `benchmark()` function to API

2. **Documentation**:
   - Audit all Python API docstrings for CLI/Python example tabs
   - Document migration path for deprecated commands
   - Add troubleshooting guide for cloud storage authentication

3. **Code organization**:
   - Run Xenon complexity check on cli/main.py and core/common.py
   - Consider splitting cli/main.py into command group modules
   - Extract common partition logic into partition_common.py (already exists, verify usage)

4. **Naming consistency** (Issue #120):
   - Review all `--column` vs `--column-name` flags
   - Ensure `--prefix` behavior is consistent across partition commands
   - Document any intentional differences

### Medium Priority

5. **Testing**:
   - Verify 75% coverage maintained
   - Add integration tests for Python API parity with CLI
   - Test cloud storage operations (S3, GCS, Azure) if not already covered

6. **Error messages**:
   - Improve cloud storage auth error messages
   - Add hints for common DuckDB errors
   - Better guidance when glob patterns are misused

7. **Performance**:
   - Profile large file operations
   - Identify bottlenecks in partition operations
   - Consider streaming improvements

### Low Priority

8. **API design**:
   - Consider `Table.benchmark()` method for performance testing
   - Evaluate if `CheckResult` needs expansion
   - Review property vs. method design on Table class

9. **CLI UX**:
   - Ensure all commands have useful `--help` text
   - Consider progressive disclosure (basic vs advanced options)
   - Improve error messages for missing dependencies

10. **Code cleanup**:
    - Remove deprecated command implementations after migration period
    - Consolidate duplicate logic across partition implementations
    - Review type hints for completeness

---

## Next Steps

**For Phase 2 (Identify Inconsistencies)**:

1. Run full test suite to establish baseline
2. Check Xenon complexity on all files
3. Generate coverage report and identify gaps
4. Grep for TODO/FIXME comments in codebase
5. Review open GitHub issues for patterns
6. Compare CLI help text with actual parameter names
7. Verify all decorators are used consistently

**For Phase 3 (Cleanup Checklist)**:

Based on Phase 2 findings, create concrete tasks:
- Individual parameter renames
- Documentation additions
- Test additions
- Refactoring targets
- Deprecation removals

---

## Appendix: Full Command Tree

```
gpio
├── convert
│   ├── geoparquet (default)
│   ├── geopackage
│   ├── flatgeobuf
│   ├── csv
│   ├── shapefile
│   ├── geojson
│   └── reproject
├── extract
│   ├── geoparquet (default)
│   ├── arcgis
│   └── bigquery
├── inspect
│   ├── summary (default)
│   ├── meta
│   ├── head
│   ├── tail
│   └── stats
├── add
│   ├── bbox
│   ├── bbox-metadata
│   ├── quadkey
│   ├── h3
│   ├── kdtree
│   └── admin-divisions
├── sort
│   ├── hilbert
│   ├── column
│   └── quadkey
├── partition
│   ├── quadkey
│   ├── h3
│   ├── kdtree
│   ├── string
│   └── admin
├── check
│   ├── all (default)
│   ├── bbox
│   ├── compression
│   ├── row-group
│   ├── spatial
│   ├── spec
│   └── stac
├── publish
│   ├── stac
│   └── upload
└── benchmark
```

**Total**: 8 groups, 37 subcommands (including defaults)

---

**Document Version**: 1.0
**Generated**: 2026-01-18
**Status**: Complete - Ready for Phase 2
