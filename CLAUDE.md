# Claude Code Instructions for geoparquet-io

## Project Overview

geoparquet-io (`gpio`) is a Python CLI for GeoParquet I/O. Entry point: `geoparquet_io/cli/main.py`

---

## Package Management

**uv only.** See `pyproject.toml` for dependencies.
```bash
uv sync --all-extras        # Install
uv run pytest               # Run commands
uv tool install geoparquet-io  # Global install
```

---

## Before Writing Code

1. Search for existing patterns (`grep -r "pattern"`)
2. Check `core/common.py` and `cli/decorators.py` first
3. Review tests for the area you're modifying

---

## Test-Driven Development (MANDATORY)

**WRITE TESTS FIRST.** Unless user says "skip tests":
1. Write failing test → 2. Implement → 3. Verify pass → 4. Add edge cases

---

## Architecture

```
geoparquet_io/
├── cli/main.py        # CLI commands (thin wrappers)
├── cli/decorators.py  # Reusable Click options
├── core/              # Business logic (52 modules)
│   └── common.py      # Shared utilities - CHECK FIRST
└── api/               # Python API (table.py, ops.py)
```

**Enforced rules** (see `.pre-commit-config.yaml`):
- `no-click-echo`: Use logger in `core/`, not `click.echo()`
- `duckdb-antipatterns`: Blocks `.fetch_arrow_table()`, `.to_arrow_table()`, `TRY_CAST.*GEOMETRY`
- `import-linter`: Core cannot import Click; API cannot import CLI
- `check-api-for-cli`: Reminds to add Python API for new CLI commands

<!-- freshness: last-verified: 2026-04-03, maps-to: geoparquet_io/cli/main.py -->
<!-- BEGIN GENERATED: cli-commands -->
### CLI Command Groups

| Command Group | Subcommands | Description |
|---------------|-------------|-------------|
| `gpio add` | a5, admin-divisions, bbox, bbox-metadata, geometry-metrics, h3, kdtree, quadkey, s2 | Commands for enhancing GeoParquet files in various ways |
| `gpio benchmark` | compare, explain, report, suite | Benchmark GeoParquet performance |
| `gpio check` | all, bbox, compression, optimization, row-group, spatial, spec, stac | Check GeoParquet files for best practices |
| `gpio convert` | csv, flatgeobuf, geojson, geopackage, geoparquet, reproject, shapefile | Convert between formats and coordinate systems |
| `gpio extract` | arcgis, bigquery, carto, geoparquet, wfs | Extract data from files and services to GeoParquet |
| `gpio inspect` | head, layers, meta, stats, summary, tail | Inspect GeoParquet files and show metadata, previews, or statistics |
| `gpio partition` | a5, admin, h3, kdtree, quadkey, s2, string | Commands for partitioning GeoParquet files |
| `gpio pmtiles` | create, pyramid | PMTiles generation commands |
| `gpio process` | aggregate, overview | Transform or reduce GeoParquet data (aggregate, overview, |
| `gpio publish` | stac, upload | Commands for publishing GeoParquet data (STAC metadata, cloud uploads) |
| `gpio skills` |  | List and access LLM skills for gpio |
| `gpio sort` | column, hilbert, quadkey, str | Commands for sorting GeoParquet files |
<!-- END GENERATED: cli-commands -->

<!-- BEGIN GENERATED: core-modules -->
### Core Modules

| Module | Purpose | Lines |
|--------|---------|-------|
| `common.py` |  | 4086 |
| `validate.py` | GeoParquet file validation against specification r... | 2854 |
| `inspect_utils.py` | Utilities for inspecting GeoParquet files. | 1608 |
| `convert.py` |  | 1395 |
| `duckdb_metadata.py` | DuckDB-based Parquet metadata extraction. | 1322 |
| `arcgis.py` | ArcGIS Feature Service to GeoParquet conversion. | 1226 |
| `extract.py` | Extract columns and rows from GeoParquet files. | 1225 |
| `metadata_utils.py` | Utilities for extracting and formatting GeoParquet... | 1197 |
| `wfs.py` | WFS (Web Feature Service) to GeoParquet conversion... | 1193 |
| `extract_bigquery.py` |  | 1044 |
| `partition_common.py` |  | 908 |
| `admin_datasets.py` |  | 735 |
| `partition_admin_hierarchical.py` |  | 698 |
| `upload.py` | Upload GeoParquet files to cloud object storage. | 675 |
| ... | *39 more modules* | |
<!-- END GENERATED: core-modules -->

<!-- freshness: last-verified: 2026-03-20, maps-to: geoparquet_io/core/common.py, geoparquet_io/cli/decorators.py -->
### Key Patterns

1. **CLI/Core Separation**: CLI commands are thin wrappers; business logic in `core/`
2. **Common Utilities**: Always check `core/common.py` before writing new utilities
3. **Shared Decorators**: Use existing decorators from `cli/decorators.py`
4. **Error Handling**: Use `ClickException` for user-facing errors

### Critical Rules

- **Never use `click.echo()` in `core/` modules** - Use logging helpers instead
- **Every CLI command needs a Python API** - Add to `api/table.py` (methods) and `api/ops.py` (functions)
- **All documentation needs CLI + Python examples** - Use tabbed format

---

<!-- freshness: last-verified: 2026-03-20, maps-to: geoparquet_io/core/common.py -->
## Key Imports

```python
from geoparquet_io.core.common import get_duckdb_connection, needs_httpfs
from geoparquet_io.core.logging_config import success, warn, error, info, debug
from pathlib import Path  # Prefer over os.path
```

### DuckDB 1.5 Patterns

**Enforced by `duckdb-antipatterns` pre-commit hook.** Violations fail the build.

| Old (crashes) | Correct |
|---------------|---------|
| `.fetch_arrow_table()` | `.arrow().read_all()` |
| `.to_arrow_table()` | `.arrow().read_all()` |
| `TRY_CAST(x AS GEOMETRY)` | `TRY(ST_GeomFromText(x))` |
| `f'"{col}"'` / `col.replace('"', '""')` | `quote_identifier(col)` |
| `WHERE path = '{value}'` | `_escape_sql_string(value)` |
| `FROM '{path}'` | `FROM {sql_path(path)}` |

Never hand-roll SQL escaping. `quote_identifier()` is for **identifiers**
(column/table names — doubles embedded `"`); `_escape_sql_string()` is for SQL
**string literals** (doubles embedded `'`); `sql_path()` is for a **file path**
and returns the quotes too, so a call site cannot forget the escape. All three
live in `core/duckdb_utils.py` and take a RAW value — escaping is not
idempotent, so escape exactly once. Column names arrive from a file's own
`geo.primary_column` and from `--column`/`--bbox-name`, so this is an injection
surface, not a style nit.

A path is escaped **either** by `safe_file_url()` (which also resolves remote
URLs and checks existence; callers quote its bare result) **or** by `sql_path()`
— never both. Handing an already-escaped path to a helper that escapes its own
argument, or to a filesystem API, turns `o'brien` into `o''brien` and the file
"disappears" (#718). A function takes a RAW path or a SQL-ready literal, never
ambiguously both. `scripts/check_sql_path_literals.py` ratchets new
`FROM '{path}'` sites out of the tree (`--update` regenerates its baseline).

Additional patterns (not yet enforced):
- `ST_Transform(..., always_xy := true)` → `SET geometry_always_xy = true` at session level
- `apply_crs_to_parquet()` removed → use `_wrap_query_with_crs()`

---

<!-- freshness: last-verified: 2026-04-03, maps-to: pyproject.toml -->
## Testing

Config in `pyproject.toml [tool.pytest.ini_options]`.

```bash
uv run pytest -n auto -m "not slow and not network and not meta"  # Fast tests (no coverage)
uv run pytest -m meta                                             # Repo tooling checks
uv run pytest --cov=geoparquet_io --cov-report=term-missing --cov-fail-under=0  # opt into coverage
```

`--cov-fail-under=0` is needed on partial runs: `[tool.coverage.report].fail_under`
re-arms the 80% floor whenever you opt into `--cov`, and a subset never clears it.

Local runs are uninstrumented: `addopts` carries no `--cov`, so a single-file run
is fast and a partial run can't fail a whole-suite gate. The 80% floor (a trailing ratchet: measured full-fast-suite coverage minus two points) and the 90%
diff-cover gate on changed lines are enforced in CI (the ubuntu/3.11 job in
`.github/workflows/tests.yml`), which passes the coverage flags explicitly.

The `meta` lane (codespell, commitizen, doc-sync, mutmut, mypy,
validate-claude-md, security tool checks) is excluded from the fast suite and
runs in the slow/nightly job instead. Pre-commit covers most of it locally, but
not all: commitizen is a `commit-msg`-stage hook and mutmut has no hook, so
`uv run pytest -m meta` is the only local way to check those two.

<!-- BEGIN GENERATED: test-markers -->
### Test Markers

| Marker | Description |
|--------|-------------|
| `@pytest.mark.slow` | marks tests as slow (deselect with '-m "not slow"') |
| `@pytest.mark.network` | marks tests requiring network access (deselect with '-m "not network"') |
| `@pytest.mark.integration` | marks end-to-end integration tests; runs in the fast suite unless also marked slow/network (see tests/e2e/test_integration_lane.py) |
| `@pytest.mark.corpus` | tests against the official geoparquet-testing corpus (requires git submodule) |
| `@pytest.mark.meta` | repo tooling checks, excluded from the fast suite |
| `@pytest.mark.docs_example` | a fenced example block executed out of docs/guide/*.md |
<!-- END GENERATED: test-markers -->

Run the docs lane as `uv run pytest docs/guide -n 4 -m "not network"`. The bare
form also collects the 29 `network` blocks, which each download hundreds of MB
of administrative boundaries from a live service — they belong to the network CI
lane, and locally they time out and read as a flaky docs lane.

---

## Code Quality

**All handled by pre-commit.** See `.pre-commit-config.yaml` for full list.

| Stage | Hooks |
|-------|-------|
| commit | ruff, codespell, no-click-echo, duckdb-antipatterns, doc-sync, menard-check |
| pre-push | xenon (complexity), import-linter, deptry, vulture |

Complexity guidance: guard clauses, dictionary dispatch, max 30-40 lines/function.

---

## Git Workflow

**Commits**: Enforced by commitizen hook. Format: `type(scope): message`
**PRs**: Update `docs/guide/` and `docs/api/python-api.md` if API changed.

### The PR title is the changelog entry

**Enforced by `.github/workflows/pr-title.yml`** (`cz check` on the title, a
required check). A squash merge uses the PR title as the commit message, and
`scripts/release_notes.py` groups the changelog by the type in that title.

`type(scope): summary`, `!` after the scope for a breaking change. The type
decides the section, so pick it for where the entry should land:

| Type | Changelog section |
|------|-------------------|
| `feat` | Added |
| `fix` | Fixed |
| `perf`, `refactor`, `revert` | Changed |
| `docs` | Documentation |
| `test`, `ci`, `chore`, `style`, `build` | Internal — *counted, not listed* |
| `build(deps...)`, or any bot bump | Dependencies — *counted, not listed* |
| anything with `!` | **Breaking** |

Write the summary as the line a user reads in the changelog, not as a note to
the reviewer: `fix(convert): keep CSV rows whose geometry is NULL`, not
`fix: address review feedback`. A breaking change needs `!` in the *title* — a
squash merge drops the body, so `BREAKING CHANGE:` written there is lost.

A title that cannot be fixed after the fact is not a blocker: rewrite it for the
changelog in `scripts/release_title_overrides.json`, which leaves the PR alone.

---

## Releases

Use the `release` skill (`.claude/skills/release/`). Do not hand-write a
changelog section and do not run `cz bump` on its own.

`CHANGELOG.md` sections are generated from the merged pull requests by
`scripts/release_notes.py`, which asks GitHub for its own release-note list and
groups it into Keep a Changelog sections by conventional-commit type:

```bash
uv run python scripts/release_notes.py 1.4.0 --previous v1.3.0          # preview
uv run python scripts/release_notes.py 1.4.0 --previous v1.3.0 --write  # apply
```

One line per PR — `- <title> by @<author> in #<number>` — in this section order:
Breaking, Added, Changed, Fixed, Documentation, then New Contributors and the Full
Changelog link.

**Internal and dependency work is counted, not listed.** Anything typed `test`,
`ci`, `chore`, `style` or `build` — and every bot bump — is left out, under a
one-line note giving the counts. The compare link and the GitHub release page
still carry them. This is why the type in a PR title matters: it decides whether
the change is published or only counted.

Above them go two to four paragraphs of highlights, written by hand, and the last
one **must name every first-time contributor**, say what they contributed, give
their pull-request count when it is more than one, and thank them. `--contributors` prints each one with every PR they wrote, which is
more than the `### New Contributors` list shows:

```bash
uv run python scripts/release_notes.py 1.4.0 --previous v1.3.0 --contributors
```

`scripts/release_title_overrides.json` rewrites a non-conforming PR title for the
changelog only. Add an entry whenever the generator leaves something in
`### Uncategorized`; that heading must be gone before a release ships.

`update_changelog_on_bump` is off, so `cz bump` writes versions only and leaves
the reviewed section alone. The skill stops for human review before anything is
bumped or tagged.

---

## New Feature Checklist

1. [ ] Core logic in `core/<feature>.py`
2. [ ] CLI wrapper in `cli/main.py`
3. [ ] Python API in `api/table.py` and `api/ops.py`
4. [ ] Tests in `tests/`
5. [ ] Docs in `docs/guide/`

---

## Claude Hooks

**Permissions**: See `.claude/settings.local.json`
**Global hooks**: See `~/.claude/CLAUDE.md` (approve-variants.py, rtk-rewrite.sh)

Dangerous patterns (command substitution `$(...)`, backticks) always rejected.

---

## Debugging

```bash
gpio inspect summary file.parquet --verbose
gpio inspect meta file.parquet --json
gpio extract input.parquet output.parquet --dry-run --show-sql
```
