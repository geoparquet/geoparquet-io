# PR2: Refactor inspect_legacy() - Complexity Reduction Plan

**Branch**: `cleanup/pr2-api-design`
**Target**: Reduce `inspect_legacy()` from Grade E to Grade C
**Estimated Effort**: 4-6 hours
**Deepened**: 2026-01-19

## Enhancement Summary

**Research completed:** Code simplicity review, existing code analysis
**Key insight:** This is a **deprecated legacy command** - simplicity over reusability
**Sections enhanced:** Research Phase, Implementation Phase, Verification Phase

### Critical Findings

1. **YAGNI Warning:** Original plan over-engineers for a deprecated command
2. **Simpler approach:** Guard clauses + minimal extraction (not full module refactor)
3. **Keep CLI concerns in CLI:** Formatting, validation, deprecation warnings stay put
4. **Only 2 functions to core:** Data operations only, not formatters

---

## Overview

The `inspect_legacy()` function in `cli/main.py` has Grade E complexity (186 lines, 13 parameters). This PR will refactor it using guard clauses and minimal extraction to reduce complexity while avoiding over-engineering.

### Current State Analysis

**Location:** `geoparquet_io/cli/main.py:1979-2165` (186 lines)

**Function Structure:**
```python
def inspect_legacy(
    parquet_file, head, tail, stats, meta,
    meta_parquet, meta_geoparquet, meta_parquet_geo,
    meta_row_groups, json_output, markdown_output,
    check_all_files, profile
):
```

**Execution Paths:**
1. **Meta mode** (lines 2043-2067): Display metadata with various sub-options
2. **Check-all mode** (lines 2082-2110): Aggregate partition validation
3. **Default mode** (lines 2112-2161): Standard file inspection

**Complexity Drivers:**
- 13 parameters (boolean flags)
- Nested validation logic (mutually exclusive options)
- Three execution branches with different concerns
- Output formatting conditionals (JSON/markdown/terminal)
- Existing helper call: `_handle_meta_display()` (already extracted)

---

## Research Phase

### 1. Understand Current Structure

**Complexity Metrics (via Xenon):**
- Grade: E (highest complexity)
- Lines: 186
- Cyclomatic Complexity: High due to nested conditionals
- Parameters: 13 (above recommended 5-7)

**Key Sections:**
- Lines 2018-2027: Deprecation warnings (10 lines)
- Lines 2028-2040: Validation logic (13 lines)
- Lines 2043-2067: Meta mode handling (25 lines)
- Lines 2069-2110: Check-all mode (42 lines)
- Lines 2112-2161: Default inspection (50 lines)

### 2. Identify Dependencies

**Existing Utilities in `core/inspect_utils.py`:**
- `extract_file_info()` - File metadata extraction
- `extract_geo_info()` - GeoParquet metadata
- `get_column_statistics()` - Statistical summaries
- `get_preview_data()` - Head/tail preview
- `format_terminal_output()` - Terminal display
- `format_json_output()` - JSON serialization
- `format_markdown_output()` - Markdown formatting
- `extract_partition_summary()` - Partition aggregation
- `format_partition_*_output()` - Partition formatters

**Already Extracted:**
- `_handle_meta_display()` helper exists in CLI (line 2057 call)

**Testing:**
- 65 tests in `tests/test_inspect.py`
- Tests use CliRunner for integration testing
- Coverage target: 75%+

### 3. Design Extraction Strategy

### Research Insights

**Best Practices - Simplicity First:**

Based on code-simplicity review, the original extraction strategy is **over-engineered** for a deprecated command. Key insights:

1. **Don't create new modules for deprecated code**
   - Original plan: Create multiple helpers in `core/inspect_utils.py`
   - **Better:** Add 2 functions to existing `core/common.py` or keep in CLI

2. **CLI-specific logic stays in CLI**
   - Formatting (JSON/markdown/terminal) is CLI concern - 10-15 lines inline
   - Deprecation warnings - stay in CLI
   - Validation - stay in CLI (Click-specific)

3. **Only extract data operations**
   - Metadata extraction (if not already available)
   - Partition validation (if not already available)
   - Everything else inline

4. **Guard clauses > helper functions**
   - Early returns flatten nesting more effectively
   - Reduces complexity without abstraction overhead

**Pattern Analysis:**

Common refactoring patterns that apply:
- **Guard Clause Pattern:** Replace nested if-else with early returns
- **Extract Method (minimal):** Only for data operations, not formatters
- **Inline Helpers:** Small (<10 lines) helpers as nested functions

Anti-patterns to avoid:
- **Over-extraction:** Creating helpers for 5-line blocks
- **Premature generalization:** Building reusable components for deprecated code
- **Dataclass ceremony:** Wrapping 13 parameters in objects

**Complexity Reduction Techniques:**

1. **Guard clauses reduce nesting:**
```python
# Before (nested)
if meta:
    if head or tail:
        raise UsageError()
    if stats:
        raise UsageError()
    # ... more nesting
    _handle_meta()

# After (flat)
if meta and (head or tail):
    raise UsageError("--meta cannot be used with --head or --tail")
if meta and stats:
    raise UsageError("--meta cannot be used with --stats")
if meta:
    return _handle_meta()  # Early exit
```

2. **Inline small helpers:**
```python
# Don't extract 5-line formatters
# Keep them as nested functions or inline
def _show_deprecation_warnings():
    if head: warn("--head deprecated...")
    if tail: warn("--tail deprecated...")
```

3. **Consolidate validation:**
```python
# Group related validations
_validate_mutually_exclusive_options(meta, head, tail, stats, check_all)
```

---

## Implementation Phase

### Revised Minimal Approach

**Goal:** Grade E → Grade C with minimal code churn (not architectural rewrite)

### 1. Extract Helper Functions (Minimal)

**In CLI (`cli/main.py`) - inline helpers:**

```python
def _show_deprecation_warnings(head, tail, stats, meta):
    """Show deprecation warnings. Stays in CLI - it's CLI-specific."""
    if head is not None:
        warn("--head flag is deprecated. Use: gpio inspect head <file> [count]")
    if tail is not None:
        warn("--tail flag is deprecated. Use: gpio inspect tail <file> [count]")
    if stats:
        warn("--stats flag is deprecated. Use: gpio inspect stats <file>")
    if meta:
        warn("--meta flag is deprecated. Use: gpio inspect meta <file>")

def _validate_mutually_exclusive(meta, head, tail, stats, check_all,
                                  json_output, markdown_output):
    """Validate mutually exclusive options. Stays in CLI - Click-specific."""
    if head is not None and tail is not None:
        raise click.UsageError("--head and --tail are mutually exclusive")
    if json_output and markdown_output:
        raise click.UsageError("--json and --markdown are mutually exclusive")

    # Meta mode exclusions
    if meta:
        if head or tail:
            raise click.UsageError("--meta cannot be used with --head or --tail")
        if stats:
            raise click.UsageError("--meta cannot be used with --stats")
        if markdown_output:
            raise click.UsageError("--meta does not support --markdown")
        if check_all:
            raise click.UsageError("--meta cannot be used with --check-all")

    # Check-all exclusions
    if check_all:
        if head or tail:
            raise click.UsageError("--check-all cannot be used with --head or --tail")
        if stats:
            raise click.UsageError("--check-all cannot be used with --stats")
```

**NO new functions in `core/` - everything needed already exists:**
- `_handle_meta_display()` already exists
- All data operations (`extract_file_info`, `extract_geo_info`, etc.) exist
- All formatters exist

### 2. Refactor CLI Function with Guard Clauses

**Simplified structure:**

```python
def inspect_legacy(parquet_file, head, tail, stats, meta,
                   meta_parquet, meta_geoparquet, meta_parquet_geo,
                   meta_row_groups, json_output, markdown_output,
                   check_all_files, profile):
    """[DEPRECATED] Legacy inspect command."""
    # Import at top
    from geoparquet_io.core.common import setup_aws_profile_if_needed, validate_profile_for_urls
    from geoparquet_io.core.duckdb_metadata import get_usable_columns
    from geoparquet_io.core.inspect_utils import (
        extract_partition_summary, format_partition_json_output,
        format_partition_markdown_output, format_partition_terminal_output,
    )
    from geoparquet_io.core.logging_config import warn
    from geoparquet_io.core.partition_reader import get_partition_info

    # Inline helpers (nested functions)
    def _show_deprecation_warnings():
        # 10 lines of warnings
        pass

    def _validate_options():
        # 20 lines of validation - consolidated
        pass

    # Main flow - flat with early returns
    _show_deprecation_warnings()
    _validate_options()

    # Guard clause: Meta mode
    if meta:
        validate_profile_for_urls(profile, parquet_file)
        setup_aws_profile_if_needed(profile, parquet_file)
        try:
            _handle_meta_display(parquet_file, meta_parquet, meta_geoparquet,
                                 meta_parquet_geo, meta_row_groups, json_output)
        except Exception as e:
            raise click.ClickException(str(e)) from e
        return  # Early exit

    # Setup AWS for remaining modes
    validate_profile_for_urls(profile, parquet_file)
    setup_aws_profile_if_needed(profile, parquet_file)

    try:
        partition_info = get_partition_info(parquet_file, verbose=False)

        # Guard clause: Check-all mode
        if partition_info["is_partition"] and check_all_files:
            _handle_check_all_mode(partition_info, json_output, markdown_output)
            return  # Early exit

        # Default mode - existing logic (no changes needed)
        _handle_default_mode(parquet_file, partition_info, head, tail, stats,
                             json_output, markdown_output)
    except Exception as e:
        raise click.ClickException(str(e)) from e
```

### 3. Key Differences from Original Plan

| Original Plan | Revised Plan | Rationale |
|--------------|--------------|-----------|
| Create 4+ helpers in `core/inspect_utils.py` | 0 new functions in core | All data operations exist |
| Extract formatters to core | Keep formatters inline | CLI-specific, 10-15 lines total |
| New module structure | Nested functions in CLI | Simpler, deprecated command |
| Dataclass for params | Keep Click params | No abstraction overhead |
| Extract 5+ functions | 2-3 inline helpers | Minimal extraction |

### 4. Maintain Behavior

**Critical:**
- No functional changes
- All 65 existing tests must pass
- Output must be byte-identical (JSON/markdown/terminal)
- Deprecation warnings still shown

**Testing Strategy:**
- Run full test suite: `uv run pytest tests/test_inspect.py -v`
- Manual smoke tests with all flag combinations
- Compare output before/after for identity

---

## Verification Phase

### 1. Complexity Check

```bash
# Before refactoring - document baseline
uv run xenon geoparquet_io/cli/main.py --max-absolute=E | grep inspect_legacy

# After refactoring - must be Grade C or better
uv run xenon geoparquet_io/cli/main.py --max-absolute=C

# If Grade C check fails, we have a problem
```

**Expected improvement:**
- Before: Grade E (>15 cyclomatic complexity)
- After: Grade C (6-10 cyclomatic complexity)
- Method: Guard clauses reduce nesting, inline helpers reduce line count

### 2. Test Verification

```bash
# Run all inspect tests
uv run pytest tests/test_inspect.py -v

# Check coverage - must maintain 75%+
uv run pytest tests/test_inspect.py --cov=geoparquet_io.cli.main --cov=geoparquet_io.core.inspect_utils --cov-report=term-missing

# Fast tests only (development)
uv run pytest tests/test_inspect.py -n auto -m "not slow and not network" -v
```

**Pass criteria:**
- All 65 tests pass
- No new test failures
- Coverage maintains or improves

### 3. Integration Testing

```bash
# Prepare test data
cd tests/data

# Test all modes and output formats
uv run gpio inspect test.parquet  # Default
uv run gpio inspect test.parquet --meta  # Meta mode
uv run gpio inspect test.parquet --meta --json  # Meta JSON
uv run gpio inspect test.parquet --head 5  # Head preview
uv run gpio inspect test.parquet --tail 5  # Tail preview
uv run gpio inspect test.parquet --stats  # Statistics
uv run gpio inspect test.parquet --json  # JSON output
uv run gpio inspect test.parquet --markdown  # Markdown output

# Test partition handling
uv run gpio inspect partitioned/ --check-all  # Partition mode
uv run gpio inspect partitioned/ --check-all --json

# Verify deprecation warnings appear
uv run gpio inspect test.parquet --head 5 2>&1 | grep deprecated
```

**Pass criteria:**
- Output identical to pre-refactor (use `diff` on JSON output)
- Deprecation warnings still shown
- No crashes or errors
- Performance unchanged (no measurable slowdown)

### 4. Performance Validation

```bash
# Benchmark before refactoring
time uv run gpio inspect large-file.parquet --stats

# Benchmark after refactoring
time uv run gpio inspect large-file.parquet --stats

# Should be within 5% (function call overhead is negligible)
```

---

## Quality Checks

### Pre-Commit Checklist

- [ ] All tests pass: `uv run pytest tests/test_inspect.py -v`
- [ ] No Grade E functions: `uv run xenon geoparquet_io/cli/main.py --max-absolute=D`
- [ ] inspect_legacy is Grade C: Verify in xenon output
- [ ] Ruff linting: `uv run ruff check .`
- [ ] Ruff formatting: `uv run ruff format .`
- [ ] No behavior changes: Manual verification
- [ ] Complexity >= Grade A check: `uv run xenon geoparquet_io/cli/main.py --max-absolute=A` (aspirational)

### Code Review Checklist

- [ ] Functions have clear, single responsibilities
- [ ] Naming is descriptive: `_show_deprecation_warnings`, `_validate_options`
- [ ] No duplicate code
- [ ] CLI logic stays in CLI (validation, formatting, deprecation)
- [ ] No over-abstraction (this is deprecated code)
- [ ] Inline helpers are small (<15 lines)
- [ ] Guard clauses used effectively
- [ ] Early returns flatten control flow

### Simplicity Check

**Questions to ask during review:**

1. Can this be simpler? (Always yes for first draft - iterate)
2. Is this abstraction necessary? (If deprecated, probably no)
3. Would inline code be clearer? (For <10 line blocks, yes)
4. Does this add reusability? (Don't care - it's deprecated)

---

## Documentation Updates

### CHANGELOG.md

Add under `## [Unreleased]`:

```markdown
### Internal

- Refactored `inspect_legacy()` function to reduce complexity (Grade E → Grade C)
- Improved code maintainability with guard clauses and minimal extraction
- No functional changes or API modifications
```

### Code Documentation

**Docstrings for inline helpers:**

```python
def _show_deprecation_warnings(head, tail, stats, meta):
    """Show deprecation warnings for legacy flags.

    This function is part of the deprecated inspect_legacy command.
    It will be removed in v2.0.
    """
    pass

def _validate_mutually_exclusive(...):
    """Validate mutually exclusive option combinations.

    Raises:
        click.UsageError: If invalid option combination detected
    """
    pass
```

**No new module documentation** - we're not creating new modules.

---

## Files to Modify

- `geoparquet_io/cli/main.py` - Refactor `inspect_legacy()` (primary change)
- `tests/test_inspect.py` - Verify tests still pass (no modifications expected)
- `CHANGELOG.md` - Document internal refactoring

**Files NOT modified** (unlike original plan):
- `geoparquet_io/core/inspect_utils.py` - No new functions
- `geoparquet_io/core/common.py` - No changes needed

---

## Success Criteria

- [ ] No Grade E functions in `cli/main.py`
- [ ] `inspect_legacy()` is Grade C or better
- [ ] All existing 65 tests pass
- [ ] No behavior changes (output identical)
- [ ] Code is more maintainable (less nesting)
- [ ] Inline helpers are simple and clear
- [ ] LOC reduction: ~20-30 lines saved via consolidation
- [ ] Complexity improvement documented with Xenon metrics

---

## Risks and Mitigations

**Risk**: Breaking existing functionality during refactor
**Mitigation**: Run tests after every extraction, compare output byte-for-byte

**Risk**: Over-abstracting and making code harder to follow
**Mitigation**: Use inline helpers (nested functions), avoid creating new modules

**Risk**: Missing edge cases in refactored code
**Mitigation**: Preserve all conditional logic exactly, no changes to logic flow

**Risk**: Making code "too clever" with guard clauses
**Mitigation**: Keep guard clauses simple (one condition per early return)

**Risk**: Performance regression from function calls
**Mitigation**: Benchmark before/after, inline helpers have zero overhead

---

## Implementation Steps (Detailed)

### Step 1: Document Current Complexity

```bash
# Save baseline
uv run xenon geoparquet_io/cli/main.py --max-absolute=E > complexity-before.txt
wc -l geoparquet_io/cli/main.py >> complexity-before.txt
```

### Step 2: Extract Inline Helpers

1. Create `_show_deprecation_warnings()` as nested function
2. Create `_validate_mutually_exclusive()` as nested function
3. Extract check-all logic to `_handle_check_all_mode()` nested function
4. Extract default logic to `_handle_default_mode()` nested function

### Step 3: Apply Guard Clauses

1. Replace nested if-elif with early returns
2. Flatten meta mode handling
3. Flatten check-all handling
4. Keep default mode as-is (already clean)

### Step 4: Test After Each Change

```bash
uv run pytest tests/test_inspect.py::test_inspect_default -v
uv run pytest tests/test_inspect.py::test_inspect_head -v
# ... run relevant tests after each helper extraction
```

### Step 5: Verify Complexity Improvement

```bash
uv run xenon geoparquet_io/cli/main.py --max-absolute=C
# Must pass
```

### Step 6: Full Test Suite

```bash
uv run pytest tests/test_inspect.py -v
# All 65 tests must pass
```

### Step 7: Manual Smoke Tests

Test all execution paths manually to verify behavior.

---

## Performance Considerations

### Research Insights

**Function Call Overhead:**
- Python function calls: ~100-200ns on modern hardware
- For a CLI tool, this is negligible compared to I/O (file reads, DuckDB queries)
- The refactored code will have 2-3 additional function calls vs. inline
- **Impact:** <0.1% performance change (unmeasurable)

**Optimization Opportunities:**
- None identified - existing code is already efficient
- All expensive operations (file I/O, DuckDB queries) are in `core/` functions
- No duplicate calls introduced by refactoring

**Recommendation:** Don't optimize, just refactor for clarity

---

## Bottom Line

**Original Plan:** Architecturally sound but over-engineered for deprecated code
**Revised Plan:** Minimal extraction + guard clauses = same complexity reduction, half the churn

**LOC Impact:**
- Original: ~20 lines saved, but adds new module/abstractions
- Revised: ~30 lines saved via consolidation, no new files

**Maintenance Impact:**
- Original: Creates reusable components (not needed for deprecated code)
- Revised: Makes current code clearer without abstraction overhead

**Complexity Target:**
- Both approaches: E → C
- Revised uses fewer abstractions to achieve same result

**Recommendation:** Follow revised minimal approach unless there's a specific need for component reusability (there isn't - this command is deprecated).
