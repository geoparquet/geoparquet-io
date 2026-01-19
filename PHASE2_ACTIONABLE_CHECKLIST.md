# Phase 2: Actionable Cleanup Checklist for v1.0-beta.1

**Date**: 2026-01-18
**Purpose**: Concrete tasks from systematic codebase analysis
**Status**: Ready for implementation (Phase 3)

---

## Executive Summary

**Good News**: The codebase is in excellent shape with minimal technical debt.

**Key Findings**:
- ✅ Only **2 TODO comments** (both low priority future enhancements)
- ⚠️ **1 critical complexity issue** (Grade E function requiring immediate attention)
- ⚠️ **6 high-priority GitHub issues** for v1.0-beta.1
- ⚠️ **Test coverage at 72.78%** (2.22 points below 75% threshold)
- ⚠️ **8 CLI parameter inconsistencies** to address

**Bottom Line**: This is refinement work, not rescue work. Focus on consistency and polish.

---

## Priority Legend

- 🔴 **P0 - CRITICAL**: Must fix before v1.0-beta.1 (blocking release)
- 🟠 **P1 - HIGH**: Should fix before v1.0-beta.1 (important for quality)
- 🟡 **P2 - MEDIUM**: Nice to have for v1.0-beta.1 (enhances polish)
- 🟢 **P3 - LOW**: Can defer to v1.0.1+ (future improvements)

---

## P0 - CRITICAL (Must Fix Before Release)

### 🔴 C1. Refactor `cli/main.py:2033` - `inspect_legacy()` Function

**Severity**: Grade E complexity (critical)
**File**: `/home/nissim/Documents/dev/geoparquet-io/geoparquet_io/cli/main.py`
**Line**: 2033
**Effort**: 4-6 hours

**Problem**: Single function with extreme complexity handling all legacy inspection logic.

**Action**:
1. Extract each inspection type into separate helper functions:
   - `_inspect_summary()`
   - `_inspect_metadata()`
   - `_inspect_stats()`
   - `_inspect_head_tail()`
2. Move core logic to `core/inspect_utils.py` (already has formatting functions)
3. Keep CLI wrapper thin (parameter parsing only)
4. Run `uv run xenon geoparquet_io/cli/main.py` to verify Grade B or better

**Success Criteria**: Function reduced from Grade E to Grade B, logic moved to core module

---

### 🔴 C2. Fix Test Coverage Below 75% Threshold

**Current**: 72.78% (2.22 points below threshold)
**Target**: 75%+ (project requirement)
**Effort**: 8-10 hours

**Critical Modules** (<75% coverage):

| Module | Coverage | Priority | Effort |
|--------|----------|----------|--------|
| `api/check.py` | 40.0% | 🔴 CRITICAL | 2h - Add 5 property tests |
| `core/common.py` | 74.0% | 🔴 CRITICAL | 3h - Add 15 utility tests |
| `api/table.py` | 79.6%* | 🟠 HIGH | 2h - Add 10 method tests |
| `api/ops.py` | 84.3%* | 🟠 HIGH | 1h - Add 3 function tests |

*Above threshold but has gaps in partition methods

**Quick Win Tests** (Priority 1 - 5 hours total):

```python
# api/check.py - Add property tests
def test_check_result_warnings():
    result = CheckResult({...})
    assert result.warnings() == [...]

def test_check_result_recommendations():
    ...

def test_check_result_failures():
    ...

def test_check_result_passed():
    ...

def test_check_result_repr():
    ...
```

```python
# api/table.py - Add partition method tests
def test_partition_by_quadkey():
    table = gpio.read('test.parquet')
    result = table.partition_by_quadkey('output/', resolution=12)
    assert result['file_count'] > 0

# Similar for: partition_by_h3, partition_by_string, partition_by_kdtree, partition_by_admin
```

```python
# api/ops.py - Add missing function tests
def test_convert_to_geojson():
    ...

def test_from_arcgis():
    ...

def test_read_bigquery():
    ...
```

**Success Criteria**: Coverage reaches 75%+ on next `uv run pytest --cov=geoparquet_io`

---

### 🔴 C3. Fix GitHub Issue #120 - CLI Command Consistency

**Impact**: High - Core UX issue affecting all CLI users
**Effort**: 6-8 hours

**Sub-tasks**:

1. **Consolidate metadata flags** (2h)
   - Current: `--meta`, `--geo`, `--geoparquet`, `--parquet`, `--parquet-geo` (5 flags)
   - Target: `--meta-type` with choices `[geo, parquet, parquet-geo, all]`
   - Files: `cli/main.py` inspect commands
   - Add deprecation warnings for old flags

2. **Standardize argument names** (2h)
   - Current: `input_file`, `input_parquet`, `parquet_file`, `input` (inconsistent)
   - Target: `input_file` for files, `input_dir` for directories
   - Files: All CLI command definitions
   - Search/replace with careful testing

3. **Replace inline options with decorators** (2h)
   - Current: Check commands use inline `@click.option("--verbose", ...)`
   - Target: Use `@verbose_option` decorator
   - Files: `cli/main.py` check command group
   - Verify help text consistency

4. **Add tests for new parameter names** (2h)
   - Ensure backward compatibility via deprecation
   - Test that old flags still work with warnings
   - Test new flags work correctly

**Success Criteria**: All commands use consistent naming, decorators applied, tests pass

---

### 🔴 C4. Remove Deprecated Commands (GitHub #154)

**Impact**: Medium - API cleanup before 1.0
**Effort**: 2-3 hours (Quick Win!)

**Commands to Remove**:
1. `gpio reproject` → Use `gpio convert reproject`
2. `gpio meta` → Use `gpio inspect meta`
3. `gpio stac` → Use `gpio publish stac`
4. `gpio upload` → Use `gpio publish upload`
5. `gpio validate` → Use `gpio check spec`

**Action**:
1. Remove command definitions from `cli/main.py`
2. Remove deprecation tests (commands will no longer exist)
3. Update any internal references to use new commands
4. Update CHANGELOG.md with migration guide
5. Verify all tests still pass

**Files**:
- `/home/nissim/Documents/dev/geoparquet-io/geoparquet_io/cli/main.py`
- `/home/nissim/Documents/dev/geoparquet-io/tests/test_cli.py`

**Success Criteria**: Commands removed, tests pass, no breaking changes to new command structure

---

## P1 - HIGH PRIORITY (Should Fix)

### 🟠 H1. Improve Error Handling (GitHub #140)

**Impact**: Medium-High - Professional error UX
**Effort**: 3-4 hours (Quick Win!)

**Problems**:
1. NoneType errors when `output_file` is None
2. Full stack traces for invalid file types (.gpkg instead of .parquet)
3. Unhelpful messages for common user errors

**Action**:

```python
# Add input validation before processing
def validate_output_file(output_file, expected_ext='.parquet'):
    if output_file is None:
        raise click.UsageError(
            "Missing output file path. Usage: gpio <command> input.parquet output.parquet"
        )

    if not output_file.endswith(expected_ext):
        raise click.UsageError(
            f"Output file must have {expected_ext} extension. Got: {output_file}\n"
            f"For other formats, use: gpio convert {Path(output_file).stem}{expected_ext} output{Path(output_file).suffix}"
        )
```

**Files to Update**:
- `core/common.py` - Add validation helpers
- All CLI commands - Use validation before processing
- Add tests for error messages

**Success Criteria**: User-friendly error messages, no stack traces for common mistakes

---

### 🟠 H2. Refactor Grade D Functions

**Impact**: Code maintainability
**Effort**: 12-16 hours total

**High-Priority Functions** (9 total):

| File | Function | Grade | Action | Effort |
|------|----------|-------|--------|--------|
| `core/extract.py:673` | `extract_table` | D | Split validation/filter/output | 2h |
| `core/extract.py:1060` | `_extract_impl` | D | Separate streaming/non-streaming | 2h |
| `core/convert.py:929` | `convert_to_geoparquet` | D | Extract per-format converters | 2h |
| `core/inspect_utils.py:794` | `format_terminal_output` | D | Extract section formatters | 2h |
| `core/inspect_utils.py:1381` | `format_markdown_output` | D | Extract section formatters | 2h |
| `core/add_quadkey_column.py:170` | `add_quadkey_table` | D | Extract validation/compute/write | 2h |
| `core/duckdb_metadata.py:224` | `parse_geometry_logical_type` | D | Use dict dispatch | 1h |
| `core/metadata_utils.py:76` | `parse_geometry_type_from_schema` | D | Extract per-type parsers | 2h |
| `core/metadata_utils.py:882` | `format_geoparquet_metadata` | D | Extract section formatters | 1h |

**Approach** (per function):
1. Run `uv run xenon <file>` to identify complexity hotspots
2. Extract helper functions for each branch/section
3. Use dict dispatch instead of if-elif chains where applicable
4. Verify tests still pass after refactoring
5. Re-run Xenon to confirm Grade C or better

**Success Criteria**: All Grade D functions reduced to Grade C or better

---

### 🟠 H3. Clean Up --profile Flag Usage (GitHub #150)

**Impact**: Medium - API simplification
**Effort**: 3-4 hours

**Problem**: `--profile` flag appears on too many commands where it's not needed.

**Action**:
1. Audit all commands with `@profile_option` decorator
2. Keep only on commands that actually interact with S3:
   - `convert` (when input/output is S3)
   - `extract` (when reading from S3)
   - `upload` (explicit S3 operation)
3. Remove from commands that never touch S3:
   - `inspect`
   - `check`
   - `add` (unless input/output is remote)
   - `sort` (unless input/output is remote)

**Optional Enhancement**: Rename to `--aws-profile` for clarity

**Files**:
- `cli/decorators.py` - Update `@profile_option` decorator
- `cli/main.py` - Remove decorator from non-S3 commands
- Tests - Verify S3 operations still work

**Success Criteria**: Only S3-related commands have `--profile` flag

---

### 🟠 H4. Investigate Large Dataset Issue (GitHub #169)

**Impact**: High - Blocking real-world use cases
**Effort**: 6-8 hours (investigation + fix)

**Problem**: 45GB GPKG conversion fails with 15GB RAM due to Arrow buffer limits.

**Investigation Steps**:
1. Reproduce issue with large test file
2. Profile memory usage during conversion
3. Identify where full table is loaded into memory
4. Determine if streaming is possible

**Potential Solutions**:
1. **Streaming/chunked processing** - Best solution, requires architecture changes
2. **Direct DuckDB COPY** - Add flag to bypass Arrow for large files
3. **Memory limit detection** - Warn users before attempting large conversions
4. **Documentation** - Clearly document memory limitations

**Action**:
1. Add test with large file (if feasible)
2. Implement streaming or direct copy option
3. Add memory limit warnings
4. Document workarounds in troubleshooting guide

**Success Criteria**: Large files (>10GB) can be converted, or clear documentation of limits

---

## P2 - MEDIUM PRIORITY (Nice to Have)

### 🟡 M1. Split `core/common.py` into Specialized Modules

**Impact**: Code organization
**Effort**: 8-10 hours

**Current**: 3,441 lines with 77 functions (14 are Grade C complexity)

**Proposed Structure**:

```
core/
├── common.py (keep only truly shared utilities)
├── crs_utils.py (CRS detection, parsing, validation)
├── remote_io.py (S3, GCS, Azure, HTTP handling)
├── geoparquet_writer.py (Writing utilities)
├── path_utils.py (Path/URL utilities)
└── validators.py (Input validation helpers)
```

**Migration Plan**:
1. Create new modules
2. Move functions to appropriate modules
3. Update imports across codebase
4. Keep backward-compatible imports in `common.py`
5. Update tests to import from new locations
6. Run full test suite

**Success Criteria**: `common.py` reduced to <1000 lines, logical organization

---

### 🟡 M2. Add CLI Examples to Partition Methods in Python API Docs

**Impact**: Documentation completeness
**Effort**: 2-3 hours

**Files**: `docs/api/python-api.md`

**Action**: Add CLI counterparts to all partition method examples

```markdown
### `partition_by_h3()`

=== "CLI"
    ```bash
    gpio partition h3 input.parquet output/ --resolution 9
    ```

=== "Python"
    ```python
    import geoparquet_io as gpio

    table = gpio.read('input.parquet')
    result = table.partition_by_h3('output/', resolution=9)
    print(f"Created {result['file_count']} files")
    ```
```

**Methods to Document** (5 total):
- `partition_by_quadkey()`
- `partition_by_h3()`
- `partition_by_string()`
- `partition_by_kdtree()`
- `partition_by_admin()`

**Success Criteria**: All partition methods have both CLI and Python examples

---

### 🟡 M3. Standardize CRS Parameter Names

**Impact**: API consistency
**Effort**: 4-5 hours

**Problem**: Mix of abbreviated and explicit names

**Current**:
- `--src-crs`, `--dst-crs` (abbreviated)
- `--crs` (ambiguous)

**Target**:
- `--source-crs`, `--target-crs` (explicit, clear)

**Migration**:
1. Add new parameter names
2. Keep old names with deprecation warnings
3. Update documentation
4. Plan removal for v1.1.0

**Files**:
- `cli/main.py` - reproject command
- `core/reproject.py` - core logic
- Documentation

**Success Criteria**: Explicit CRS parameter names, deprecation warnings for old names

---

### 🟡 M4. Fix Duplicate Column Names in `inspect` (GitHub #115)

**Impact**: Display bug
**Effort**: 2-3 hours (Quick Win!)

**Problem**: Nested structures show duplicate column names in output

**Action**:
1. Identify root cause in `core/inspect_utils.py`
2. Fix column listing logic for nested types
3. Add test for nested structure inspection
4. Verify fix doesn't break other output formats

**Success Criteria**: No duplicate column names in `gpio inspect` output

---

## P3 - LOW PRIORITY (Future Work)

### 🟢 L1. Add GeoPandas Interoperability (GitHub #141)

**Impact**: Python API enhancement
**Effort**: 6-8 hours

**Feature Request**: `table.to_geopandas()` and `Table.from_geopandas()`

**Implementation**:
1. Add soft dependency on `geoarrow-pyarrow`
2. Implement conversion methods
3. Add tests (if geopandas installed)
4. Document in Python API guide

**Defer Reason**: Not critical for 1.0, can add in 1.1

---

### 🟢 L2. Performance Optimization (GitHub #129)

**Impact**: Performance regression (25% slower 0.4 → 0.7)
**Effort**: Ongoing investigation

**Defer Reason**: Functional correctness > speed for 1.0. Optimize in 1.x series.

---

### 🟢 L3. STAC Remote File Support

**Context**: Intentional limitation, documented in code
**Action**: Only implement if users request it
**Defer Reason**: No user demand yet

---

## Summary Statistics

### Tasks by Priority

| Priority | Count | Total Effort |
|----------|-------|--------------|
| 🔴 P0 - Critical | 4 tasks | 20-27 hours |
| 🟠 P1 - High | 4 tasks | 30-40 hours |
| 🟡 P2 - Medium | 4 tasks | 16-21 hours |
| 🟢 P3 - Low | 3 tasks | 15-20+ hours |
| **TOTAL** | **15 tasks** | **81-108 hours** |

### Recommended Approach for v1.0-beta.1

**Sprint 1** (1 week - 20-27 hours):
- C1: Refactor `inspect_legacy()` (4-6h)
- C2: Fix test coverage (8-10h)
- C4: Remove deprecated commands (2-3h)
- H1: Improve error handling (3-4h)
- M4: Fix inspect duplicate columns (2-3h)

**Sprint 2** (1 week - 18-23 hours):
- C3: Fix CLI consistency (#120) (6-8h)
- H3: Clean up --profile flag (3-4h)
- H4: Investigate large dataset issue (6-8h)
- M2: Add partition CLI examples (2-3h)

**Sprint 3** (1 week - 12-16 hours):
- H2: Refactor Grade D functions (focus on top 3-4)

**Post-1.0-beta.1**:
- M1: Split common.py (can defer)
- M3: Standardize CRS params (can defer)
- L1-L3: Low priority enhancements

---

## Success Metrics

**v1.0-beta.1 Ready When**:
- ✅ No Grade E complexity functions
- ✅ Test coverage ≥75%
- ✅ All deprecated commands removed
- ✅ GitHub #120, #140, #150, #154 closed
- ✅ User-friendly error messages (no stack traces)
- ✅ Consistent CLI parameter naming

**Bonus Goals** (if time permits):
- ✅ All Grade D functions reduced to Grade C
- ✅ GitHub #169 resolved or documented
- ✅ Documentation 100% complete (CLI + Python examples)

---

## Files Requiring Most Attention

**Top 10 by Lines + Complexity**:

1. `/home/nissim/Documents/dev/geoparquet-io/geoparquet_io/cli/main.py` (4,972 lines, Grade E function)
2. `/home/nissim/Documents/dev/geoparquet-io/geoparquet_io/core/common.py` (3,441 lines, 14 Grade C functions)
3. `/home/nissim/Documents/dev/geoparquet-io/geoparquet_io/core/inspect_utils.py` (1,552 lines, 2 Grade D functions)
4. `/home/nissim/Documents/dev/geoparquet-io/geoparquet_io/core/extract.py` (1,212 lines, 2 Grade D functions)
5. `/home/nissim/Documents/dev/geoparquet-io/geoparquet_io/core/convert.py` (1,138 lines, 1 Grade D function)
6. `/home/nissim/Documents/dev/geoparquet-io/geoparquet_io/core/metadata_utils.py` (1,077 lines, 2 Grade D functions)
7. `/home/nissim/Documents/dev/geoparquet-io/geoparquet_io/core/duckdb_metadata.py` (907 lines, 1 Grade D function)
8. `/home/nissim/Documents/dev/geoparquet-io/geoparquet_io/core/add_quadkey_column.py` (598 lines, 1 Grade D function)
9. `/home/nissim/Documents/dev/geoparquet-io/geoparquet_io/api/table.py` (2,227 lines, test coverage gaps)
10. `/home/nissim/Documents/dev/geoparquet-io/geoparquet_io/api/check.py` (40% coverage - critical gap)

---

## Next Steps

1. **Review this checklist** with Chris and prioritize
2. **Create GitHub issues** for each P0/P1 task
3. **Assign to sprint** (recommend 2-3 week timeline for P0/P1)
4. **Start with quick wins**: C4 (remove deprecated), H1 (error handling), M4 (inspect fix)
5. **Tackle critical items**: C1 (inspect refactor), C2 (test coverage), C3 (CLI consistency)

---

**Document Version**: 1.0
**Generated**: 2026-01-18
**Status**: Ready for Phase 3 (Implementation)
**Previous Phase**: [PHASE1_CODEBASE_REVIEW.md](./PHASE1_CODEBASE_REVIEW.md)
