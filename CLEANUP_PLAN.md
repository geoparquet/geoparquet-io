# GPIO v1.0-beta.1 Cleanup Implementation Plan

**Purpose**: Systematic cleanup for v1.0-beta.1 release
**Executor**: Claude Code
**Reviewer**: Nissim
**Status**: Ready to execute

---

## Rules of Engagement

### Workflow Per PR

1. **Read this plan and PROGRESS.md** to identify current PR's tasks
2. **Create feature branch** from `main`: `cleanup/<pr-id>-<short-description>`
3. **Plan the changes** - identify all files affected
4. **Implement all tasks in this PR** including:
   - Code changes
   - Test updates/additions (run `pytest` to verify)
   - Python API updates (if applicable)
   - Documentation updates (if applicable)
   - CHANGELOG.md entry under `## [Unreleased]`
5. **Run quality checks**:
   - `uv run pytest` - all tests pass
   - `uv run pytest --cov=geoparquet_io` - coverage meets threshold
   - `uv run ruff check .` - no lint errors
   - `uv run ruff format .` - code formatted
6. **Commit with clear message** following project conventions
7. **Push branch and open PR**:
   - Title: `[Cleanup] <Description covering all tasks>`
   - Body: Summary of changes for all tasks, link to GitHub issues if applicable
   - Self-review: Check diff, note any concerns
8. **Update PROGRESS.md**: Mark PR as "🔍 PR Open - Awaiting Review"
9. **STOP AND WAIT** for Nissim to review and merge

### After Merge

1. Pull latest `main`
2. Update PROGRESS.md: Mark PR as "✅ Complete"
3. Proceed to next PR

### PR Requirements

Every PR must:
- [ ] Pass all tests (`uv run pytest`)
- [ ] Maintain or improve coverage (target: 75%+)
- [ ] Pass linting (`uv run ruff check .`)
- [ ] Be formatted (`uv run ruff format .`)
- [ ] Include CHANGELOG.md entry
- [ ] Update docs if behavior changes
- [ ] Update Python API if CLI changes (and vice versa)

### Commit Message Format

```
<verb> <what changed>

- Detail 1
- Detail 2
```

Keep it brief. No emoji. No Claude Code footer.

---

## PR Execution Order

### PR 1: Quick Wins - Command Removal + Column Duplication Fix
**Branch**: `cleanup/pr1-quick-wins`
**GitHub Issues**: #154, #115
**Estimated Effort**: 4-6 hours
**Tasks**: C4 (remove deprecated commands) + M4 (inspect duplicate columns)

#### Task C4: Remove Deprecated Commands (#154)

**Commands to Remove**:
1. `gpio reproject` → Users should use `gpio convert reproject`
2. `gpio meta` → Users should use `gpio inspect meta`
3. `gpio stac` → Users should use `gpio publish stac`
4. `gpio upload` → Users should use `gpio publish upload`
5. `gpio validate` → Users should use `gpio check spec`

**Files to Modify**:
- `geoparquet_io/cli/main.py` - Remove command definitions
- `tests/test_cli.py` - Remove/update deprecation tests
- `CHANGELOG.md` - Document removal with migration guide

**Acceptance Criteria**:
- [ ] All 5 deprecated commands removed from CLI
- [ ] No references to removed commands in codebase
- [ ] Tests pass
- [ ] CHANGELOG documents migration path (note: breaking changes are fine pre-v1.0)

#### Task M4: Fix Duplicate Column Names in Inspect (#115)

**Problem**: Nested structures show duplicate column names in `gpio inspect` output

**Approach**:
1. Identify root cause in `core/inspect_utils.py`
2. Fix column listing logic for nested types
3. Add test for nested structure inspection

**Acceptance Criteria**:
- [ ] No duplicate column names in inspect output
- [ ] Test added for nested structures
- [ ] No regression in other inspect functionality

---

### PR 2: Critical Complexity Fix
**Branch**: `cleanup/pr2-inspect-refactor`
**GitHub Issue**: None (internal quality)
**Estimated Effort**: 4-6 hours
**Tasks**: C1 (refactor inspect_legacy)

#### Task C1: Refactor inspect_legacy() Function

**Problem**: `cli/main.py:inspect_legacy()` is Grade E complexity

**Research First** (do this before implementing):
```bash
# Find the function
grep -n "def inspect_legacy" geoparquet_io/cli/main.py

# Understand its structure
uv run xenon geoparquet_io/cli/main.py --max-absolute=E

# Count branches/lines
```

**Approach** (after understanding the code):
1. Identify the function and document its branches
2. Extract helper functions:
   - `_inspect_summary()`
   - `_inspect_metadata()`
   - `_inspect_stats()`
   - `_inspect_head_tail()`
3. Move core logic to `core/inspect_utils.py` if not already there
4. Keep CLI function as thin dispatcher

**Files to Modify**:
- `geoparquet_io/cli/main.py` - Refactor function
- `geoparquet_io/core/inspect_utils.py` - Add extracted logic (if needed)
- Tests - Ensure existing tests still pass

**Verification**:
```bash
uv run xenon geoparquet_io/cli/main.py --max-absolute=D
```

**Acceptance Criteria**:
- [ ] No Grade E functions remain
- [ ] Function reduced to Grade C or better
- [ ] All existing tests pass
- [ ] No behavior changes (pure refactor)

---

### PR 3: Test Coverage Fix
**Branch**: `cleanup/pr3-test-coverage`
**GitHub Issue**: None (internal quality)
**Estimated Effort**: 8-10 hours
**Tasks**: C2 (improve test coverage to 75%+)

#### Task C2: Fix Test Coverage to 75%+

**Current**: 72.78%
**Target**: 75%+

**Research First**:
```bash
# Generate detailed coverage report
uv run pytest --cov=geoparquet_io --cov-report=term-missing

# Identify specific uncovered lines per module
uv run pytest --cov=geoparquet_io --cov-report=html
# Open htmlcov/index.html
```

**Priority Modules** (from existing analysis):
1. `api/check.py` (40% → 70%+) - Add property tests for CheckResult
2. `core/common.py` (74% → 76%+) - Add utility function tests
3. `api/table.py` - Add partition method tests
4. `api/ops.py` - Add conversion function tests

**Test Files to Create/Modify**:
- `tests/test_api_check.py` - New or expand
- `tests/test_common.py` - Expand
- `tests/test_api.py` - Expand

**Approach**:
1. Run coverage report to identify specific gaps
2. Document uncovered lines/functions per module
3. Write tests for uncovered lines, prioritizing:
   - Public API methods
   - Error handling paths
   - Edge cases
4. Re-run coverage after each module
5. Focus on meaningful tests, not just line coverage

**Acceptance Criteria**:
- [ ] Overall coverage ≥75%
- [ ] `api/check.py` coverage ≥60%
- [ ] All new tests pass
- [ ] No flaky tests introduced
- [ ] Tests validate behavior, not just execute code

---

### PR 4: CLI Consistency
**Branch**: `cleanup/pr4-cli-consistency`
**GitHub Issue**: #120
**Estimated Effort**: 6-8 hours
**Tasks**: C3 (fix CLI command consistency)

#### Task C3: Fix CLI Command Consistency (#120)

**Sub-tasks**:

**4a. Consolidate metadata flags**
- Current: `--meta`, `--geo`, `--geoparquet`, `--parquet`, `--parquet-geo`
- Target: Simplify to consistent pattern (research best approach first)
- Files: `cli/main.py` inspect commands
- Note: Breaking changes are fine pre-v1.0

**4b. Standardize argument names**
- Current: `input_file`, `input_parquet`, `parquet_file`, `input` (inconsistent)
- Target: `input_file` for files, `input_dir` for directories
- Files: All CLI command definitions

**4c. Replace inline options with decorators**
- Current: Check commands use inline `@click.option("--verbose", ...)`
- Target: Use `@verbose_option` decorator from `cli/decorators.py`
- Files: `cli/main.py` check command group
- Verification: Compare `--help` output before/after to ensure no behavior change

**4d. Update tests and docs**
- Ensure all renamed parameters work
- Update help text if needed

**Acceptance Criteria**:
- [ ] Consistent argument naming across all commands
- [ ] Decorators used instead of inline options where appropriate
- [ ] All tests pass with new parameter names
- [ ] Help text is clear and consistent
- [ ] No unintended behavior changes from decorator replacement

---

### PR 5: Error Handling + Profile Cleanup
**Branch**: `cleanup/pr5-error-handling-profile`
**GitHub Issues**: #140, #150
**Estimated Effort**: 6-8 hours
**Tasks**: H1 (improve error handling) + H3 (clean up --profile flag)

#### Task H1: Improve Error Handling (#140)

**Problem**: Stack traces shown for common user errors (e.g., wrong file type)

**Scope** (to prevent scope creep):
Focus on top 3 commands by usage:
1. `gpio convert`
2. `gpio extract`
3. `gpio inspect`

**Research First**:
```bash
# Test current error UX
gpio convert test.gpkg test.parquet
gpio extract test.parquet
# Document what users see
```

**Approach**:
1. Add input validation helpers to `core/common.py`
2. Catch common errors and raise `click.UsageError` with helpful messages
3. Add `--debug` flag for full stack traces (if not exists)

**Common Errors to Handle**:
- Missing output file
- Wrong file extension
- File not found
- Invalid format for command

**Acceptance Criteria**:
- [ ] No stack traces for common user mistakes in top 3 commands
- [ ] Helpful error messages with suggestions
- [ ] `--debug` flag available for troubleshooting
- [ ] Tests for error conditions

#### Task H3: Clean Up --profile Flag Usage (#150)

**Problem**: `--profile` appears on commands where it's not needed

**Approach**:
1. Identify which commands actually need `--profile` (S3 operations only)
2. Remove from commands that don't need it
3. Document in CHANGELOG (breaking changes are fine pre-v1.0)

**Commands that SHOULD have --profile**:
- `gpio publish upload`
- `gpio convert` (when writing to S3)
- `gpio extract` (when reading from S3)

**Research First**:
```bash
# Find all uses of @profile_option
grep -n "@profile_option" geoparquet_io/cli/main.py
```

**Acceptance Criteria**:
- [ ] `--profile` only on commands that use S3
- [ ] CHANGELOG documents the breaking change
- [ ] Tests updated

---

### PR 6: Grade D Refactoring (Top 3 Functions)
**Branch**: `cleanup/pr6-grade-d-refactor`
**GitHub Issue**: None (internal quality)
**Estimated Effort**: 6-8 hours
**Tasks**: H2 (refactor Grade D functions - subset)

#### Task H2: Refactor Grade D Functions (Top 3 Only)

**Scope**: Focus on highest-impact Grade D functions only (from existing analysis):
1. `core/extract.py:673` - `extract_table`
2. `core/convert.py:929` - `convert_to_geoparquet`
3. `core/inspect_utils.py:794` - `format_terminal_output`

**Research First Per Function**:
```bash
# Check current complexity
uv run xenon geoparquet_io/core/extract.py --max-absolute=D

# Understand the function structure
grep -A 50 "def extract_table" geoparquet_io/core/extract.py
```

**Approach Per Function**:
1. Document current complexity metrics (lines, branches, nesting)
2. Identify extraction targets based on actual code structure
3. Extract helper functions for each logical section
4. Use dict dispatch where applicable (if-elif chains)
5. Verify tests still pass after refactoring
6. Re-run Xenon to confirm Grade C or better

**General Patterns**:
- Extract validation logic
- Extract per-branch logic into separate functions
- Use early returns (guard clauses)
- Use dict dispatch for format/type selection

**Acceptance Criteria**:
- [ ] Target 3 functions reduced to Grade C or better
- [ ] No behavior changes (pure refactor)
- [ ] All tests pass
- [ ] Each function documented with clear purpose

---

## Optional PR 7: Documentation Audit (If Time Permits)
**Branch**: `cleanup/pr7-docs-audit`
**Estimated Effort**: 3-4 hours
**Tasks**: Verify docs reflect all changes

**Scope**:
- [ ] CLI reference up to date (removed commands documented)
- [ ] Python API docs match actual API
- [ ] Examples work with new command structure
- [ ] Migration guide complete for breaking changes
- [ ] All new features have both CLI and Python examples

---

## PROGRESS.md Template

Create this file at start:

```markdown
# GPIO Cleanup Progress

**Started**: [DATE]
**Target**: v1.0-beta.1

## Status

| PR | Tasks | Status | PR Link | Notes |
|----|-------|--------|---------|-------|
| PR1 | C4 + M4: Quick wins (remove commands, fix dupes) | ⏳ Not started | - | Issues #154, #115 |
| PR2 | C1: Refactor inspect_legacy | ⏳ Not started | - | Grade E → C |
| PR3 | C2: Test coverage 75%+ | ⏳ Not started | - | 72.78% → 75%+ |
| PR4 | C3: CLI consistency | ⏳ Not started | - | Issue #120 |
| PR5 | H1 + H3: Error handling + profile cleanup | ⏳ Not started | - | Issues #140, #150 |
| PR6 | H2: Grade D refactoring (top 3) | ⏳ Not started | - | extract, convert, inspect |
| PR7 | Docs audit (optional) | ⏳ Not started | - | If time permits |

## Status Legend
- ⏳ Not started
- 🔄 In progress
- 🔍 PR Open - Awaiting Review
- ✅ Complete
- ⏸️ Blocked
- ❌ Skipped

## Log

### [DATE]
- Started cleanup process
- Current PR: PR1
```

---

## Final Checklist (Before v1.0-beta.1)

After all PRs complete:

- [ ] All PRs merged
- [ ] Test coverage ≥75%
- [ ] No Grade E complexity functions
- [ ] Grade D complexity reduced (at least top 3 functions)
- [ ] CHANGELOG.md complete
- [ ] Documentation reflects all changes
- [ ] Migration guide complete for breaking changes
- [ ] Tag release: `v1.0.0-beta.1`

**Note**: Planning files (PROGRESS.md, CLEANUP_PLAN.md, PHASE*.md) will be manually deleted by Nissim after release.

---

## Reference

**Key Files**:
- CLI: `geoparquet_io/cli/main.py`
- Decorators: `geoparquet_io/cli/decorators.py`
- Common utils: `geoparquet_io/core/common.py`
- Python API: `geoparquet_io/api/table.py`, `geoparquet_io/api/ops.py`
- Tests: `tests/`

**Commands**:
```bash
# Run tests
uv run pytest

# Check coverage with details
uv run pytest --cov=geoparquet_io --cov-report=term-missing
uv run pytest --cov=geoparquet_io --cov-report=html

# Check complexity
uv run xenon geoparquet_io/ --max-absolute=D
uv run xenon geoparquet_io/cli/main.py --max-absolute=E

# Lint and format
uv run ruff check .
uv run ruff format .

# Find all uses of a pattern
grep -rn "pattern" geoparquet_io/
```

**GitHub Issues**:
- #120 - CLI consistency
- #140 - Error handling
- #150 - --profile cleanup
- #154 - Remove deprecated commands
- #115 - Inspect duplicate columns

---

## Success Criteria

**v1.0-beta.1 is ready when**:
- ✅ No Grade E complexity functions
- ✅ Test coverage ≥75%
- ✅ All deprecated commands removed
- ✅ GitHub #120, #140, #150, #154, #115 closed
- ✅ User-friendly error messages (no stack traces for common errors)
- ✅ Consistent CLI parameter naming
- ✅ `--profile` flag only on S3-related commands

**Bonus goals**:
- ✅ Top 3 Grade D functions reduced to Grade C
- ✅ Documentation 100% accurate and up-to-date

---

**Notes**:
- Breaking changes are acceptable pre-v1.0
- PRs should be reviewable but not tiny (3-7 PRs total is the target)
- Each PR should be mergeable independently where possible
- STOP AND WAIT for review after each PR before proceeding
- Focus on quality over speed - this is cleanup, not a race
