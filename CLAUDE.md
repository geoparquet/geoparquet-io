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

## Test-Driven Development (MANDATORY)

**YOU MUST USE TDD. NO EXCEPTIONS.** Unless the user explicitly says "skip tests":

1. **WRITE TESTS FIRST** - Before ANY implementation code
2. **RUN TESTS** - Verify they fail with `uv run pytest`
3. **IMPLEMENT** - Minimal code to pass tests
4. **RUN TESTS AGAIN** - Verify they pass
5. **ADD EDGE CASES** - Test error conditions

**VIOLATING TDD IS UNACCEPTABLE.** Every feature needs tests FIRST.

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

### PreToolUse Hooks
The project includes command modification hooks in `.claude/settings.local.json`:

```json
"hooks": {
  "PreToolUse": [
    {
      "matcher": "Bash",
      "hooks": [{
        "type": "command",
        "command": "python .claude/hooks/ensure-uv-run.py"
      }]
    }
  ]
}
```

**ensure-uv-run.py**: Automatically prefixes Python commands with `uv run`:
- `pytest` → `uv run pytest`
- `ruff check` → `uv run ruff check`
- `gpio` → `uv run gpio`

This ensures commands always use the correct virtual environment without manual intervention.

### Session Hooks
- **pre-session-hook.md**: Instructions Claude reads at session start
- Enforces documentation checks, context loading, etc.

This maintains consistency across conversations and prevents reinventing already-solved problems.

## Directives d'utilisation des outils MCP

Utilisez les outils Distill MCP pour des opérations économes en tokens :

### Règle 1 : Lecture intelligente de fichiers

Lors de la lecture de fichiers source pour **exploration ou compréhension** :

```
mcp__distill__smart_file_read filePath="path/to/file.ts"
```

**Quand utiliser Read natif à la place :**
- Avant d'éditer un fichier (Edit nécessite Read d'abord)
- Fichiers de configuration : `.json`, `.yaml`, `.toml`, `.md`, `.env`

### Règle 2 : Compresser les sorties volumineuses

Après les commandes Bash qui produisent une sortie volumineuse (>500 caractères) :

```
mcp__distill__auto_optimize content="<collez la sortie volumineuse>"
```

### Règle 3 : SDK d'exécution de code pour les opérations complexes

Pour les opérations multi-étapes, utilisez `code_execute` au lieu de plusieurs appels d'outils (**98% d'économie de tokens**) :

```
mcp__distill__code_execute code="<code typescript>"
```

**API du SDK (`ctx`) :**

*Compression :*
- `ctx.compress.auto(content, hint?)` - Détection auto et compression
- `ctx.compress.logs(logs)` - Résumer les logs
- `ctx.compress.diff(diff)` - Compresser les git diff
- `ctx.compress.semantic(content, ratio?)` - Compression TF-IDF

*Code :*
- `ctx.code.parse(content, lang)` - Parser en structure AST
- `ctx.code.extract(content, lang, {type, name})` - Extraire un élément
- `ctx.code.skeleton(content, lang)` - Obtenir les signatures uniquement

*Fichiers :*
- `ctx.files.read(path)` - Lire le contenu d'un fichier
- `ctx.files.exists(path)` - Vérifier si un fichier existe
- `ctx.files.glob(pattern)` - Trouver des fichiers par pattern

*Git :*
- `ctx.git.diff(ref?)` - Obtenir le diff git
- `ctx.git.log(limit?)` - Historique des commits
- `ctx.git.status()` - Statut du repo
- `ctx.git.branch()` - Info sur les branches
- `ctx.git.blame(file, line?)` - Git blame d'un fichier

*Recherche :*
- `ctx.search.grep(pattern, glob?)` - Rechercher un pattern dans les fichiers
- `ctx.search.symbols(query, glob?)` - Rechercher des symboles (fonctions, classes)
- `ctx.search.files(pattern)` - Rechercher des fichiers par pattern
- `ctx.search.references(symbol, glob?)` - Trouver les références d'un symbole

*Analyse :*
- `ctx.analyze.dependencies(file)` - Analyser les imports/exports
- `ctx.analyze.callGraph(fn, file, depth?)` - Construire le graphe d'appels
- `ctx.analyze.exports(file)` - Obtenir les exports d'un fichier
- `ctx.analyze.structure(dir?, depth?)` - Structure du répertoire avec analyse

*Utilitaires :*
- `ctx.utils.countTokens(text)` - Compter les tokens
- `ctx.utils.detectType(content)` - Détecter le type de contenu
- `ctx.utils.detectLanguage(path)` - Détecter le langage depuis le chemin

**Exemples :**

```typescript
// Obtenir les squelettes de tous les fichiers TypeScript
const files = ctx.files.glob("src/**/*.ts").slice(0, 5);
return files.map(f => ({
  file: f,
  skeleton: ctx.code.skeleton(ctx.files.read(f), "typescript")
}));

// Compresser et analyser les logs
const logs = ctx.files.read("server.log");
return ctx.compress.logs(logs);

// Extraire une fonction spécifique
const content = ctx.files.read("src/api.ts");
return ctx.code.extract(content, "typescript", { type: "function", name: "handleRequest" });
```

### Référence rapide

| Action | Utiliser |
|--------|----------|
| Lire du code pour exploration | `mcp__distill__smart_file_read filePath="file.ts"` |
| Obtenir une fonction/classe | `mcp__distill__smart_file_read filePath="file.ts" target={"type":"function","name":"myFunc"}` |
| Compresser les erreurs de build | `mcp__distill__auto_optimize content="..."` |
| Résumer les logs | `mcp__distill__summarize_logs logs="..."` |
| Opérations multi-étapes | `mcp__distill__code_execute code="return ctx.files.glob('src/**/*.ts')"` |
| Avant d'éditer | Utiliser l'outil natif `Read` |

<!-- END DISTILL -->
