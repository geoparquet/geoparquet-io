# Claude Code Instructions for geoparquet-io

This file contains project-specific instructions for Claude Code when working in this repository.

## Project Overview

geoparquet-io (gpio) is a Python CLI tool for fast I/O and transformation of GeoParquet files. It uses Click for CLI, PyArrow and DuckDB for data processing, and follows modern Python packaging standards.

**Entry point**: `gpio` command defined in `geoparquet_io/cli/main.py`

---

## Planning and Research Before Coding

**Always research before implementing.** This is the most important guideline.

### Before Writing Any Code

1. **Understand the request fully** - Ask clarifying questions if scope is ambiguous
2. **Search for existing patterns** - Check if similar functionality exists
3. **Identify affected files** - Map out what needs to change
4. **Check for utilities** - Review `core/common.py` and `cli/decorators.py` for reusable code
5. **Understand test requirements** - Look at existing tests for the area you're modifying

### Research Commands

```bash
# Find files by pattern
ls geoparquet_io/core/
ls geoparquet_io/cli/

# Search for function usage
grep -r "function_name" geoparquet_io/

# Check how similar features are implemented
grep -r "pattern_to_find" --include="*.py"

# Run tests in specific area
pytest tests/test_<area>.py -v
```

### Questions to Answer Before Coding

1. Does this feature already exist partially?
2. What existing utilities can I reuse?
3. How do similar features handle errors?
4. What's the test coverage expectation?
5. Are there edge cases mentioned in similar code?

---

## Codebase Architecture

Understanding the structure prevents architectural mistakes.

### Directory Layout

```
geoparquet_io/
├── __init__.py          # Exports cli entry point
├── cli.py               # Legacy entry (imports from cli/)
├── cli/
│   ├── __init__.py      # Exports cli group
│   ├── main.py          # All CLI commands defined here (~2200 lines)
│   ├── decorators.py    # Reusable Click option decorators
│   └── fix_helpers.py   # Helpers for check --fix functionality
└── core/
    ├── common.py        # Shared utilities (~1400 lines) - CHECK THIS FIRST
    ├── extract.py       # Extract command logic
    ├── convert.py       # Convert command logic
    ├── hilbert_order.py # Hilbert sorting logic
    ├── partition_*.py   # Partitioning implementations
    ├── add_*.py         # Column addition logic
    ├── check_*.py       # Check/validation logic
    └── ...
```

### Key Patterns

**1. CLI/Core Separation**
CLI commands are thin wrappers. Business logic lives in `core/`.

```python
# In cli/main.py - CLI wrapper
@cli.command()
@click.argument("input_file")
@verbose_option  # From decorators.py
def mycommand(input_file, verbose):
    """Command docstring."""
    mycommand_impl(input_file, verbose)  # Core function

# In core/mymodule.py - Business logic
def mycommand_impl(input_file, verbose):
    # Actual implementation here
```

**2. Shared Decorators**
Common options are defined once in `cli/decorators.py`:
- `@verbose_option` - Adds `--verbose/-v`
- `@dry_run_option` - Adds `--dry-run`
- `@compression_options` - Adds `--compression` and `--compression-level`
- `@output_format_options` - Compression + row group options
- `@partition_options` - All partition-related flags
- `@profile_option` - AWS profile for S3

**3. Common Utilities** (`core/common.py`)
Before writing new utility code, check if these exist:
- `get_duckdb_connection()` - Creates configured DuckDB connection
- `is_remote_url()`, `is_s3_url()` - URL detection
- `safe_file_url()` - Validates and encodes file paths
- `write_parquet_with_metadata()` - Writes parquet with proper metadata
- `add_computed_column()` - Generic column addition helper
- `check_bbox_structure()` - Checks bbox column presence
- `find_primary_geometry_column()` - Gets geometry column name
- `validate_compression_settings()` - Validates compression params
- `remote_write_context()` - Context manager for remote output

**4. Error Handling**
Use Click's error handling for user-facing errors:
```python
from click import ClickException, BadParameter

# For general errors
raise ClickException("Human readable error message")

# For parameter validation
raise BadParameter("Invalid value for --option")
```

---

## Key Dependencies and Usage

### DuckDB (SQL engine)
- Primary data processing engine
- Spatial extension for geometry operations
- httpfs extension for remote files

```python
from geoparquet_io.core.common import get_duckdb_connection, needs_httpfs

con = get_duckdb_connection(
    load_spatial=True,
    load_httpfs=needs_httpfs(file_path)
)
con.execute("SELECT * FROM read_parquet('file.parquet')")
```

### PyArrow (Parquet I/O)
- Reading/writing parquet with metadata control
- Schema inspection

```python
import pyarrow.parquet as pq

pf = pq.ParquetFile(file_path)
schema = pf.schema_arrow
metadata = schema.metadata  # Dict with b"geo" key for GeoParquet
```

### Click (CLI framework)
- All CLI commands use Click decorators
- Command groups: `cli`, `check`, `add`, `sort`, `partition`
- **IMPORTANT: Do NOT use `click.echo()` for output** - use the logger instead (see Logging section below)

---

## Logging

**CRITICAL: Never use `click.echo()` in `core/` modules. Always use the logging helpers.**

`click.echo()` is allowed in `cli/` for direct CLI output, but `core/` modules must use the logger for testability and library compatibility.

This project uses a centralized logging system in `core/logging_config.py` that provides colored CLI output while maintaining compatibility with library usage and testing.

### Import and Usage
```python
from geoparquet_io.core.logging_config import success, warn, error, info, debug, progress

success("Operation completed")  # Green - for completed operations
warn("Something to note")       # Yellow - for warnings
error("Something went wrong")   # Red - for errors
info("Informational message")   # Cyan - for tips/context
debug("Debug details")          # Only shown when verbose=True
progress("Processing...")       # Plain text - for status updates
```

### Why Not click.echo()?

1. **Testability**: Logger output is captured by pytest; click.echo requires special handling
2. **Library usage**: When gpio is used as a library, users can configure logging handlers
3. **Consistency**: Single source of truth for all output formatting
4. **Verbosity control**: Debug messages are automatically hidden unless `--verbose` is passed

### Setting Up Verbose Mode in Core Functions
```python
from geoparquet_io.core.logging_config import configure_verbose, debug

def my_function(input_file: str, verbose: bool = False):
    configure_verbose(verbose)  # Call at start of function
    debug("This only shows with --verbose")
```

### Pre-Commit Enforcement
A pre-commit hook enforces this rule. If you add `click.echo()` in `core/`, the commit will fail.

### fsspec (File system abstraction)
- Handles local and remote file access uniformly
```python
import fsspec
with fsspec.open(file_path, "rb") as f:
    # Works for local, S3, HTTP, etc.
```

---

## Git Commit Messages

Keep commit messages brief and focused:

- **Maximum 1-2 lines** - Single sentence preferred
- Use imperative mood: "Add feature" not "Added feature"
- Start with a verb: Add, Fix, Update, Remove, Refactor, Improve
- No period at the end, no emoji
- Focus on *what* changed, not *how*

**Good examples:**
```
Add spatial filtering to gpio extract command
Fix bbox validation for antimeridian-crossing geometries
Remove deprecated --format flag from convert command
```

**Bad examples:**
```
Updated the extract.py file to add new functionality for filtering rows and columns with various options including bbox support
```

Do NOT include the standard Claude Code footer. Keep commits minimal.

---

## Pull Requests

Follow the template in `.github/pull_request_template.md` exactly:

### PR Title
- Action-oriented: `<Verb> <what>` - e.g., "Add `gpio inspect --meta` option"
- Used in changelogs and release notes

### PR Body Structure
1. **Description**: 1-3 sentences on what and why
2. **Technical Details**: Implementation notes for reviewers
3. **Related Issue(s)**: Link with `#<number>`
4. **Checklist**: Verify formatting and tests

---

## Code Formatting with Ruff

This project uses Ruff for linting and formatting.

### Before Any Commit
```bash
pre-commit run --all-files
```

### Ruff Configuration (`pyproject.toml`)
- **Line length**: 100 characters
- **Target Python**: 3.10+
- **Enabled rules**: E, W, F, I, B, C4, UP
- Use double quotes, f-strings, comprehensions
- Imports: stdlib, third-party, first-party (`geoparquet_io`)

### Auto-fix
```bash
ruff check --fix .
ruff format .
```

---

## Code Complexity with Xenon

**Aim for grade 'A' complexity on all new code.**

### Complexity Grades
- **A**: Simple, easy to understand (TARGET)
- **B**: Acceptable, low complexity
- **C-F**: Needs refactoring

### Check Complexity
```bash
# Strict check - aim for this
xenon --max-absolute=A --max-modules=A --max-average=A geoparquet_io/

# Current pre-commit threshold (minimum acceptable)
xenon --max-absolute=E --max-modules=D --max-average=C geoparquet_io/
```

### Reducing Complexity

**1. Extract helper functions**
```python
# BAD: Long function with many branches
def process_file(file, options):
    if options.a:
        # 20 lines of code
    elif options.b:
        # 20 lines of code
    # ...

# GOOD: Extracted helpers
def process_file(file, options):
    if options.a:
        return _process_option_a(file)
    elif options.b:
        return _process_option_b(file)

def _process_option_a(file):
    # 20 lines of code

def _process_option_b(file):
    # 20 lines of code
```

**2. Early returns (guard clauses)**
```python
# BAD: Nested conditions
def validate(data):
    if data:
        if data.valid:
            if data.complete:
                return process(data)
    return None

# GOOD: Early returns
def validate(data):
    if not data:
        return None
    if not data.valid:
        return None
    if not data.complete:
        return None
    return process(data)
```

**3. Use data structures instead of branching**
```python
# BAD: Long if-elif chain
if format == "json":
    return json_handler()
elif format == "csv":
    return csv_handler()
elif format == "parquet":
    return parquet_handler()

# GOOD: Dictionary dispatch
handlers = {
    "json": json_handler,
    "csv": csv_handler,
    "parquet": parquet_handler,
}
return handlers[format]()
```

**4. Single responsibility per function**
- Each function does one thing
- If you need "and" to describe it, split it
- Max 30-40 lines per function

---

## Code Reuse and Refactoring

### Before Writing New Code

1. **Search `core/common.py`** - Most utilities are here
2. **Search `cli/decorators.py`** - Reusable Click options
3. **Search for similar patterns**: `grep -r "pattern" geoparquet_io/`
4. **Check if helper exists in the same module**

### DRY Signals (When to Extract)

- Same code appears in 2+ places → extract to helper
- Same parameters grouped together → consider dataclass
- Same error handling pattern → extract to context manager
- Same Click options → extract to decorator in `decorators.py`

### When Refactoring

1. **Ensure tests exist first** - Never refactor untested code
2. **Make incremental changes** - One extraction at a time
3. **Run tests after each change**
4. **Preserve behavior** - Refactoring ≠ feature changes

### Where to Put Extracted Code

| Code Type | Location |
|-----------|----------|
| CLI option decorators | `cli/decorators.py` |
| File/URL utilities | `core/common.py` |
| DuckDB/Parquet utilities | `core/common.py` |
| Domain-specific logic | Same module or new `core/*.py` |
| Test utilities | `tests/conftest.py` |

---

## Testing Guidelines

### Test Coverage Requirements

**CRITICAL: All new code must have tests. This is mandatory, not optional.**

- **Overall project threshold**: 75% minimum (enforced by pytest)
- **New code requirement**: 80% or higher coverage for new features/changes
- **Every commit with code changes must include corresponding tests**

When writing new code:
1. Write tests alongside implementation, not as an afterthought
2. Test both happy paths and error cases
3. Run `pytest` before committing to verify coverage threshold is met

```bash
# Check coverage for a specific file
pytest --cov=geoparquet_io/core/mymodule --cov-report=term-missing tests/test_mymodule.py

# Check overall coverage
pytest --cov=geoparquet_io --cov-report=term-missing
```

If coverage falls below 75%, pytest will fail. Fix this by adding missing tests before committing.

### Test Structure
```
tests/
├── conftest.py          # Shared fixtures
├── data/                # Test parquet files
├── test_<module>.py     # Tests mirror source structure
```

### Running Tests
```bash
# All tests
pytest

# Skip slow tests
pytest -m "not slow"

# Skip network tests
pytest -m "not network"

# Specific module
pytest tests/test_extract.py -v

# Single test
pytest tests/test_extract.py::TestParseBbox::test_valid_bbox -v
```

### Test Patterns Used

**1. Class-based organization**
```python
class TestParseBbox:
    """Tests for parse_bbox function."""

    def test_valid_bbox(self):
        result = parse_bbox("-122.5,37.5,-122.0,38.0")
        assert result == (-122.5, 37.5, -122.0, 38.0)
```

**2. Fixtures for temp files**
```python
@pytest.fixture
def output_file(self):
    tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.parquet"
    yield str(tmp_path)
    if tmp_path.exists():
        tmp_path.unlink()
```

**3. Markers for conditional tests**
```python
@pytest.mark.slow
class TestRemoteFiles:
    """Tests requiring network access."""
```

**4. CLI testing with CliRunner**
```python
from click.testing import CliRunner
from geoparquet_io.cli.main import extract

runner = CliRunner()
result = runner.invoke(extract, [input_file, output_file])
assert result.exit_code == 0
```

### Windows Compatibility
- Use `uuid.uuid4()` in temp filenames to avoid locking
- Retry cleanup with delays for file handle release
- Use context managers for DuckDB connections

---

## Debugging and Troubleshooting

### Common Issues

**1. DuckDB file locking (Windows)**
```python
# Always close connections explicitly
con = get_duckdb_connection()
try:
    # ... operations
finally:
    con.close()
```

**2. Remote file authentication**
```python
# Check if profile is set correctly
from geoparquet_io.core.common import setup_aws_profile_if_needed
setup_aws_profile_if_needed(profile, file_path)
```

**3. GeoParquet metadata issues**
```python
# Debug metadata structure
import json
metadata = pf.schema_arrow.metadata
if b"geo" in metadata:
    geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
    print(json.dumps(geo_meta, indent=2))
```

### Debug Commands

```bash
# Check file structure
gpio inspect file.parquet --verbose

# Check metadata
gpio inspect --meta file.parquet --json

# Dry-run to see SQL
gpio extract input.parquet output.parquet --dry-run

# Show SQL during execution
gpio extract input.parquet output.parquet --show-sql --verbose
```

### Adding Debug Output
```python
from geoparquet_io.core.logging_config import debug

debug(f"variable = {variable}")  # Only shown when verbose=True
```

---

## Pre-Commit Workflow Summary

Before every commit:
```bash
# 1. Run all checks
pre-commit run --all-files

# 2. Fix any issues
ruff check --fix .
ruff format .

# 3. Verify complexity (aim for A)
xenon --max-absolute=A --max-modules=A --max-average=A geoparquet_io/

# 4. Run tests
pytest

# 5. If tests pass, commit
git add .
git commit -m "Brief description of change"
```

---

## Quick Reference

### Adding a New CLI Command

1. Define core logic in `core/newcommand.py`
   - Create a `*_table()` function that accepts/returns `pa.Table` for Python API use
   - Keep file I/O separate from the core transformation logic
2. Add CLI wrapper in `cli/main.py`
3. Use existing decorators from `cli/decorators.py`
4. **Add Python API support** (see below)
5. Add tests in `tests/test_newcommand.py` and `tests/test_api.py`
6. Check complexity: `xenon --max-absolute=A geoparquet_io/core/newcommand.py`

### Adding Python API for New Commands

**CRITICAL: Every new CLI command must have a corresponding Python API.**

The Python API is equally important as the CLI. Users expect to be able to use gpio programmatically.

1. **Add a method to `Table` class** in `api/table.py`:
   ```python
   def new_operation(self, param: str = "default") -> Table:
       """Brief description."""
       from geoparquet_io.core.newcommand import new_operation_table
       result = new_operation_table(self._table, param=param)
       return Table(result, self._geometry_column)
   ```

2. **Add a function to `ops` module** in `api/ops.py`:
   ```python
   def new_operation(table: pa.Table, param: str = "default") -> pa.Table:
       """Brief description for functional API users."""
       from geoparquet_io.core.newcommand import new_operation_table
       return new_operation_table(table, param=param)
   ```

3. **Export if needed** from `api/__init__.py` and top-level `__init__.py`

4. **Add tests** in `tests/test_api.py` for both Table method and ops function

5. **Update documentation** in `docs/api/python-api.md`

The Python API should mirror the CLI functionality but work with in-memory Arrow tables for better performance and composability.

### Adding a CLI Option

1. Check if decorator exists in `cli/decorators.py`
2. If common, add decorator there
3. If command-specific, add inline

### Modifying Core Logic

1. Read existing tests first
2. Understand the function's contract
3. Make changes
4. Run tests: `pytest tests/test_<module>.py -v`
5. Check complexity

### Working with Remote Files

1. Use `is_remote_url()` to detect
2. Use `needs_httpfs()` for DuckDB
3. Use `remote_write_context()` for writes
4. Use `setup_aws_profile_if_needed()` for S3
