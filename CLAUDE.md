# Claude Code Instructions for geoparquet-io

This file contains project-specific instructions for Claude Code when working in this repository.

## Project Overview

geoparquet-io (gpio) is a Python CLI tool for fast I/O and transformation of GeoParquet files. It uses Click for CLI, PyArrow and DuckDB for data processing, and follows modern Python packaging standards.

**Entry point**: `gpio` command defined in `geoparquet_io/cli/main.py`

---

## Documentation Structure

### context/ Directory
Contains ephemeral planning docs and durable reference documentation:

- **context/shared/plans/** - Active feature plans and implementation strategies
- **context/shared/documentation/** - Durable docs on specific topics/features for AI developers
- **context/shared/reports/** - Analysis reports and architectural assessments
- **context/shared/research/** - Auto-generated research from feature exploration

**Important**: When starting work on any feature, check `context/README.md` for available documentation and read relevant docs before proceeding.

---

## Before Writing Code: Research First

**Always research before implementing.** Before any code changes:

1. **Understand the request** - Ask clarifying questions if ambiguous
2. **Search for patterns** - Check if similar functionality exists (`grep -r "pattern"`)
3. **Check utilities** - Review `core/common.py` and `cli/decorators.py` first
4. **Identify affected files** - Map out what needs to change
5. **Review existing tests** - Look at tests for the area you're modifying
6. **Plan documentation** - Identify docs needing updates

**Key questions:** Does this exist partially? What utilities can I reuse? How do similar features handle errors? What's the test coverage expectation?

---

## Test-Driven Development (Required)

**Always use TDD when implementing features** unless explicitly told otherwise:

1. **Write failing tests first** - Define expected behavior before implementation
2. **Run tests to confirm they fail** - Ensure tests actually test the new functionality
3. **Implement minimal code** - Write just enough to make tests pass
4. **Refactor if needed** - Clean up while keeping tests green
5. **Add edge cases** - Expand test coverage for error conditions

This approach ensures correctness, prevents regressions, and documents expected behavior.

---

## Architecture & Key Files

```
geoparquet_io/
├── cli/
│   ├── main.py          # All CLI commands (~2200 lines)
│   ├── decorators.py    # Reusable Click options - CHECK FIRST
│   └── fix_helpers.py   # Check --fix helpers
└── core/
    ├── common.py        # Shared utilities (~1400 lines) - CHECK FIRST
    ├── <command>.py     # Command implementations (extract, convert, etc.)
    └── logging_config.py # Logging system
```

### Key Patterns

1. **CLI/Core Separation**: CLI commands are thin wrappers; business logic in `core/`
2. **Common Utilities**: Always check `core/common.py` before writing new utilities
3. **Shared Decorators**: Use existing decorators from `cli/decorators.py`
4. **Error Handling**: Use `ClickException` for user-facing errors

### Critical Rules

- **Never use `click.echo()` in `core/` modules** - Use logging helpers instead
- **Every CLI command needs a Python API** - Add to `api/table.py` and `api/ops.py`
- **All documentation needs CLI + Python examples** - Use tabbed format

---

## Dependencies Quick Reference

```python
# DuckDB with extensions
from geoparquet_io.core.common import get_duckdb_connection, needs_httpfs
con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(file_path))

# Logging (not click.echo!)
from geoparquet_io.core.logging_config import success, warn, error, info, debug, progress

# Remote files
from geoparquet_io.core.common import is_remote_url, remote_write_context, setup_aws_profile_if_needed
```

---

## Testing with uv

```bash
# Fast tests only (recommended for development)
uv run pytest -n auto -m "not slow and not network"

# Specific test
uv run pytest tests/test_extract.py::TestParseBbox::test_valid_bbox -v

# With coverage
uv run pytest --cov=geoparquet_io --cov-report=term-missing
```

**Test markers:**
- `@pytest.mark.slow` - Tests >5s, conversions, reprojection
- `@pytest.mark.network` - Requires network access
- **Coverage requirement**: 75% minimum (enforced), 80% for new code

---

## Git Workflow

### Commits
- **One line, imperative mood**: "Add feature" not "Added feature"
- Start with verb: Add, Fix, Update, Remove, Refactor
- No emoji, no period, no Claude footer

### Pull Requests
- Update relevant guide in `docs/guide/`
- Update `docs/api/python-api.md` if API changed
- Include both CLI and Python examples
- Follow PR template

---

## Code Quality

```bash
# Before committing (all handled by pre-commit)
pre-commit run --all-files

# Or manually
uv run ruff check --fix .
uv run ruff format .
uv run xenon --max-absolute=A geoparquet_io/  # Aim for A grade
```

**Complexity reduction:**
- Extract helper functions
- Use early returns (guard clauses)
- Dictionary dispatch over long if-elif
- Max 30-40 lines per function

---

## Quick Checklist for New Features

1. [ ] Core logic in `core/<feature>.py` with `*_table()` function
2. [ ] CLI wrapper in `cli/main.py` using decorators
3. [ ] Python API in `api/table.py` and `api/ops.py`
4. [ ] Tests in `tests/test_<feature>.py` and `tests/test_api.py`
5. [ ] Documentation in `docs/guide/<feature>.md` with CLI/Python tabs
6. [ ] Complexity grade A (`xenon --max-absolute=A`)
7. [ ] Coverage >80% for new code

---

## Debugging

```bash
# Inspect file structure
gpio inspect file.parquet --verbose

# Check metadata
gpio inspect --meta file.parquet --json

# Dry-run with SQL
gpio extract input.parquet output.parquet --dry-run --show-sql
```

For Windows: Always close DuckDB connections explicitly, use UUID in temp filenames.

---

## Claude Hooks & Permissions

### Automatic Command Approvals
The project uses smart command auto-approval patterns. Commands are automatically approved when they follow safe patterns with common wrappers.

**Safe wrapper patterns** (automatically stripped and approved):
- `uv run <command>` - Package manager execution
- `timeout <seconds> <command>` - Time-limited execution
- `.venv/bin/<command>` - Virtual environment commands
- `nice <command>` - Priority adjustment
- Environment variables: `ENV_VAR=value <command>`

**Safe core commands** (auto-approved after wrapper stripping):
- **Testing**: `pytest`, `pre-commit`, `ruff`, `xenon`
- **Git**: All git operations including `add`, `commit`, `push`
- **GitHub**: `gh pr`, `gh issue`, `gh api`
- **Build tools**: `make`, `cargo`, `npm`, `yarn`, `pip`, `uv`
- **Read-only**: `ls`, `cat`, `grep`, `find`, `head`, `tail`
- **Project CLI**: `gpio` (all subcommands)

**Example auto-approvals**:
```bash
uv run pytest -n auto                    # ✅ Auto-approved
timeout 60 uv run pytest tests/          # ✅ Auto-approved
.venv/bin/gpio convert input.parquet     # ✅ Auto-approved
SKIP=xenon pre-commit run --all-files    # ✅ Auto-approved
```

Commands with dangerous patterns (command substitution `$(...)`, backticks) are always rejected for safety.

### Custom Permission Overrides
For commands not covered by patterns, add to `.claude/settings.local.json`:
```json
{
  "permissions": {
    "allow": [
      "Bash(custom-command:*)",
      "WebFetch(domain:example.com)"
    ]
  }
}
```

### Session Hooks
Create `.claude/hooks/` for automated session behavior:
- **pre-session-hook.md**: Instructions Claude reads at session start
- Enforces documentation checks, context loading, etc.

This maintains consistency across conversations and prevents reinventing already-solved problems.
