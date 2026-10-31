"""Tests for CLAUDE.md validation script.

Tests for scripts/validate_claude_md.py which validates that CLAUDE.md
stays in sync with the actual codebase.
"""

import ast
import subprocess
import textwrap
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Repo tooling checks: this suite validates a maintenance script rather than
# library behaviour, so it runs in the meta lane instead of the fast suite.
pytestmark = pytest.mark.meta

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "validate_claude_md.py"


def _run_validator(*extra_args: str) -> subprocess.CompletedProcess:
    """Run the validation script as a subprocess and return the CompletedProcess.

    Only the happy path and one end-to-end failure still go through a
    subprocess -- enough to prove the script is executable and wires argv,
    stdout and exit codes together. Every other case calls ``main()`` in
    process, which is the same code path without the ~1s `uv run` spawn.
    """
    return subprocess.run(
        ["uv", "run", "python", str(SCRIPT_PATH), *extra_args],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


class TestValidateClaudeMdIntegration:
    """End-to-end tests that run the full validation flow.

    Two of these spawn the script as a subprocess (happy path and one
    detected-error path); the rest drive the same ``main()`` entry point
    in process to avoid a `uv run` spawn per test.
    """

    def test_script_exists(self):
        """Verify validation script exists."""
        assert SCRIPT_PATH.exists(), f"Script not found at {SCRIPT_PATH}"

    def test_script_runs_successfully(self):
        """Verify script runs and passes on current CLAUDE.md."""
        result = _run_validator()
        assert result.returncode == 0, (
            f"Validation failed (rc={result.returncode}):\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

    def test_script_accepts_custom_path(self, tmp_path, capsys):
        """Verify the validator accepts a custom CLAUDE.md path via --claude-md."""
        from scripts.validate_claude_md import main

        fake = tmp_path / "CLAUDE.md"
        fake.write_text(
            "## Project Overview\n"
            "## Architecture & Key Files\n"
            "## Testing with uv\n"
            "## Git Workflow\n"
        )
        rc = main(["--claude-md", str(fake)])
        out = capsys.readouterr().out
        # Exit code 2 means "CLAUDE.md not found", so anything else proves the
        # custom path was located and read.
        assert rc in (0, 1), f"custom --claude-md path was not read:\n{out}"

    def test_script_accepts_project_root(self, tmp_path, capsys):
        """Verify the validator accepts a --project-root argument."""
        from scripts.validate_claude_md import main

        fake_root = tmp_path / "project"
        fake_root.mkdir()
        claude = fake_root / "CLAUDE.md"
        claude.write_text("## Project Overview\n## Architecture\n## Testing\n## Git Workflow\n")
        rc = main(["--claude-md", str(claude), "--project-root", str(fake_root)])
        out = capsys.readouterr().out
        assert rc in (0, 1), f"--project-root was not accepted:\n{out}"

    def test_missing_claude_md(self, tmp_path, capsys):
        """Validator returns exit code 2 when CLAUDE.md is missing."""
        from scripts.validate_claude_md import main

        missing = tmp_path / "nonexistent.md"
        rc = main(["--claude-md", str(missing)])
        assert rc == 2
        assert "not found" in capsys.readouterr().out

    def test_catches_invalid_command(self, tmp_path):
        """Detects references to non-existent CLI commands."""
        fake = tmp_path / "CLAUDE.md"
        fake.write_text(
            "## Project Overview\n"
            "## Architecture & Key Files\n"
            "## Testing with uv\n"
            "## Git Workflow\n"
            "\nRun `gpio nonexistent-command` to do stuff.\n"
        )
        result = _run_validator("--claude-md", str(fake))
        assert result.returncode == 1
        assert "nonexistent-command" in result.stdout

    def test_catches_invalid_path(self, tmp_path, capsys):
        """Detects references to non-existent file paths."""
        from scripts.validate_claude_md import main

        fake = tmp_path / "CLAUDE.md"
        fake.write_text(
            "## Project Overview\n"
            "## Architecture & Key Files\n"
            "## Testing with uv\n"
            "## Git Workflow\n"
            "\nCheck `geoparquet_io/core/does_not_exist.py` for details.\n"
        )
        rc = main(["--claude-md", str(fake)])
        assert rc == 1
        assert "does_not_exist" in capsys.readouterr().out

    def test_catches_invalid_marker(self, tmp_path, capsys):
        """Detects references to undefined pytest markers."""
        from scripts.validate_claude_md import main

        fake = tmp_path / "CLAUDE.md"
        fake.write_text(
            "## Project Overview\n"
            "## Architecture & Key Files\n"
            "## Testing with uv\n"
            "## Git Workflow\n"
            "\nUse `@pytest.mark.nonexistent` for special tests.\n"
        )
        rc = main(["--claude-md", str(fake)])
        assert rc == 1
        assert "nonexistent" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Unit tests: CLI command extraction
# ---------------------------------------------------------------------------


class TestCliCommandValidation:
    """Unit tests for CLI command extraction and validation."""

    def test_extracts_gpio_commands_from_backticks(self):
        """Extracts gpio command groups from backticked references."""
        from scripts.validate_claude_md import extract_gpio_commands

        content = (
            "Run `gpio add bbox` to add bounding boxes.\n"
            "Use `gpio convert crs` for conversion.\n"
            "Try `gpio inspect meta` for metadata.\n"
        )
        commands = extract_gpio_commands(content)
        assert "add" in commands
        assert "convert" in commands
        assert "inspect" in commands

    def test_extracts_commands_from_code_blocks(self):
        """Extracts gpio commands from fenced code blocks."""
        from scripts.validate_claude_md import extract_gpio_commands

        content = "```bash\ngpio add           # Enhance files\ngpio convert       # Convert\n```\n"
        commands = extract_gpio_commands(content)
        assert "add" in commands
        assert "convert" in commands

    def test_filters_parquet_file_arguments(self):
        """Does not treat file arguments as sub-commands."""
        from scripts.validate_claude_md import extract_gpio_commands

        content = (
            "```bash\ngpio inspect file.parquet\ngpio extract input.parquet output.parquet\n```\n"
        )
        commands = extract_gpio_commands(content)
        assert "file.parquet" not in commands
        assert "input.parquet" not in commands
        assert "output.parquet" not in commands
        assert "inspect" in commands
        assert "extract" in commands

    def test_empty_content_returns_empty_set(self):
        """Returns empty set for content with no gpio commands."""
        from scripts.validate_claude_md import extract_gpio_commands

        assert extract_gpio_commands("") == set()
        assert extract_gpio_commands("No commands here.") == set()

    def test_gets_actual_cli_commands(self):
        """Introspects the real CLI to get command names."""
        from scripts.validate_claude_md import get_actual_commands

        commands = get_actual_commands()
        assert "add" in commands
        assert "convert" in commands
        assert "inspect" in commands
        assert "extract" in commands
        assert "sort" in commands
        assert "check" in commands
        assert "partition" in commands
        assert "publish" in commands
        assert "benchmark" in commands

    def test_validate_cli_commands_passes_for_valid(self):
        """No errors when all referenced commands exist."""
        from scripts.validate_claude_md import validate_cli_commands

        content = "```bash\ngpio add\ngpio convert\ngpio inspect\n```\n"
        errors = validate_cli_commands(content)
        assert errors == []

    def test_validate_cli_commands_fails_for_invalid(self):
        """Reports error for a non-existent command."""
        from scripts.validate_claude_md import validate_cli_commands

        content = "Run `gpio frobnicate` to do stuff.\n"
        errors = validate_cli_commands(content)
        assert len(errors) == 1
        assert "frobnicate" in errors[0]


# ---------------------------------------------------------------------------
# Unit tests: file path extraction
# ---------------------------------------------------------------------------


class TestFilePathValidation:
    """Unit tests for file path extraction and validation."""

    def test_extracts_python_file_paths(self):
        """Extracts file paths that look like Python files."""
        from scripts.validate_claude_md import extract_file_paths

        content = "Check `core/common.py` and `cli/main.py`."
        paths = extract_file_paths(content)
        assert any("core/common.py" in p for p in paths)
        assert any("cli/main.py" in p for p in paths)

    def test_extracts_qualified_paths(self):
        """Extracts fully-qualified paths within geoparquet_io."""
        from scripts.validate_claude_md import extract_file_paths

        content = "Entry point in `geoparquet_io/cli/main.py`."
        paths = extract_file_paths(content)
        assert "geoparquet_io/cli/main.py" in paths

    def test_extracts_markdown_paths(self):
        """Extracts .md file paths."""
        from scripts.validate_claude_md import extract_file_paths

        content = "See `docs/guide/sorting.md` for details."
        paths = extract_file_paths(content)
        assert "docs/guide/sorting.md" in paths

    def test_skips_generic_patterns(self):
        """Does not treat wildcard patterns like `add_*.py` as concrete paths."""
        from scripts.validate_claude_md import extract_file_paths

        content = "Files matching `add_*.py` exist in core."
        paths = extract_file_paths(content)
        assert not any("add_*.py" in p for p in paths)

    def test_skips_template_paths(self):
        """Does not treat template paths like `core/<feature>.py` as concrete paths."""
        from scripts.validate_claude_md import extract_file_paths

        content = "Core logic in `core/<feature>.py` with `*_table()` function."
        paths = extract_file_paths(content)
        assert not any("<feature>" in p for p in paths)

    def test_ignores_non_project_paths(self):
        """Does not extract paths that lack a known project prefix."""
        from scripts.validate_claude_md import extract_file_paths

        content = "See `random/other.py` for details."
        paths = extract_file_paths(content)
        assert len(paths) == 0

    def test_validate_file_paths_passes_for_valid(self):
        """No errors for paths that exist."""
        from scripts.validate_claude_md import validate_file_paths

        content = "See `geoparquet_io/cli/main.py` for details."
        errors = validate_file_paths(content, PROJECT_ROOT)
        assert errors == []

    def test_validate_file_paths_resolves_short_paths(self):
        """Resolves short paths like `core/common.py` under geoparquet_io/."""
        from scripts.validate_claude_md import validate_file_paths

        content = "Check `core/common.py` for utilities."
        errors = validate_file_paths(content, PROJECT_ROOT)
        assert errors == []

    def test_validate_file_paths_fails_for_invalid(self):
        """Reports error for non-existent file paths."""
        from scripts.validate_claude_md import validate_file_paths

        content = "See `geoparquet_io/core/does_not_exist.py` for details."
        errors = validate_file_paths(content, PROJECT_ROOT)
        assert len(errors) == 1
        assert "does_not_exist" in errors[0]


# ---------------------------------------------------------------------------
# Unit tests: test marker extraction
# ---------------------------------------------------------------------------


class TestTestMarkerValidation:
    """Unit tests for pytest marker extraction and validation."""

    def test_extracts_markers_from_decorator_syntax(self):
        """Extracts marker names from @pytest.mark.X references."""
        from scripts.validate_claude_md import extract_test_markers

        content = "Use `@pytest.mark.slow` for slow tests."
        markers = extract_test_markers(content)
        assert "slow" in markers

    def test_extracts_markers_from_m_flag(self):
        """Extracts marker names from -m flag references."""
        from scripts.validate_claude_md import extract_test_markers

        content = '`-m "not slow and not network"`'
        markers = extract_test_markers(content)
        assert "slow" in markers
        assert "network" in markers

    def test_extracts_markers_from_single_quoted_m_flag(self):
        """Extracts marker names from single-quoted -m flag references."""
        from scripts.validate_claude_md import extract_test_markers

        content = """`-m 'not slow and not network'`"""
        markers = extract_test_markers(content)
        assert "slow" in markers
        assert "network" in markers

    def test_excludes_builtin_pytest_markers(self):
        """Built-in markers like parametrize, skipif are excluded."""
        from scripts.validate_claude_md import extract_test_markers

        content = (
            "Use `@pytest.mark.parametrize` for parameterized tests.\n"
            "Use `@pytest.mark.skipif` for conditional skips.\n"
            "Use `@pytest.mark.xfail` for expected failures.\n"
            "Use `@pytest.mark.slow` for slow tests.\n"
        )
        markers = extract_test_markers(content)
        assert "parametrize" not in markers
        assert "skipif" not in markers
        assert "xfail" not in markers
        assert "slow" in markers

    def test_validate_markers_passes_for_valid(self):
        """No errors when all referenced markers are defined."""
        from scripts.validate_claude_md import validate_test_markers

        content = "Use `@pytest.mark.slow` and `@pytest.mark.network`."
        errors = validate_test_markers(content, PROJECT_ROOT / "pyproject.toml")
        assert errors == []

    def test_validate_markers_fails_for_undefined(self):
        """Reports error for undefined markers."""
        from scripts.validate_claude_md import validate_test_markers

        content = "Use `@pytest.mark.flaky` for flaky tests."
        errors = validate_test_markers(content, PROJECT_ROOT / "pyproject.toml")
        assert len(errors) == 1
        assert "flaky" in errors[0]

    def test_validate_markers_missing_pyproject(self, tmp_path):
        """Reports error when pyproject.toml is missing."""
        from scripts.validate_claude_md import validate_test_markers

        content = "Use `@pytest.mark.slow` for slow tests."
        errors = validate_test_markers(content, tmp_path / "nonexistent.toml")
        assert len(errors) == 1
        assert "pyproject.toml" in errors[0]


# ---------------------------------------------------------------------------
# Unit tests: import validation
# ---------------------------------------------------------------------------


class TestImportValidation:
    """Unit tests for import example validation."""

    def test_extracts_imports(self):
        """Extracts import statements from backticked code blocks."""
        from scripts.validate_claude_md import extract_imports

        content = "```python\nfrom geoparquet_io.core.common import get_duckdb_connection\n```\n"
        imports = extract_imports(content)
        assert len(imports) >= 1
        assert any(
            imp[0] == "geoparquet_io.core.common" and "get_duckdb_connection" in imp[1]
            for imp in imports
        )

    def test_extracts_multi_name_imports(self):
        """Extracts imports with multiple names."""
        from scripts.validate_claude_md import extract_imports

        content = "from geoparquet_io.core.common import foo, bar, baz\n"
        imports = extract_imports(content)
        assert len(imports) == 1
        assert imports[0][0] == "geoparquet_io.core.common"
        assert imports[0][1] == ["foo", "bar", "baz"]

    def test_extracts_parenthesized_multiline_imports(self):
        """Extracts multi-line parenthesized imports correctly."""
        from scripts.validate_claude_md import extract_imports

        content = textwrap.dedent("""\
            from geoparquet_io.core.common import (
                get_duckdb_connection,
                needs_httpfs,
            )
        """)
        imports = extract_imports(content)
        assert len(imports) >= 1
        mod, names = imports[0]
        assert mod == "geoparquet_io.core.common"
        assert "get_duckdb_connection" in names
        assert "needs_httpfs" in names

    def test_handles_as_alias_in_imports(self):
        """Extracts original name from 'import X as Y' patterns."""
        from scripts.validate_claude_md import extract_imports

        content = "from geoparquet_io.core.common import get_duckdb_connection as gdc\n"
        imports = extract_imports(content)
        assert len(imports) == 1
        assert "get_duckdb_connection" in imports[0][1]
        # Should NOT have the full "get_duckdb_connection as gdc" as a name
        assert not any("as" in name for name in imports[0][1])

    def test_validate_imports_passes_for_valid(self):
        """No errors when imports reference real modules and names."""
        from scripts.validate_claude_md import validate_imports

        content = "```python\nfrom geoparquet_io.core.common import get_duckdb_connection\n```\n"
        errors = validate_imports(content, PROJECT_ROOT)
        assert errors == []

    def test_validate_imports_detects_missing_module(self):
        """Reports error when module does not exist."""
        from scripts.validate_claude_md import validate_imports

        content = "```python\nfrom geoparquet_io.core.nonexistent_module import foo\n```\n"
        errors = validate_imports(content, PROJECT_ROOT)
        assert len(errors) >= 1
        assert "nonexistent_module" in errors[0]

    def test_validate_imports_detects_missing_name(self):
        """Reports error when imported name does not exist in the module."""
        from scripts.validate_claude_md import validate_imports

        content = "```python\nfrom geoparquet_io.core.common import nonexistent_function\n```\n"
        errors = validate_imports(content, PROJECT_ROOT)
        assert len(errors) >= 1
        assert "nonexistent_function" in errors[0]

    def test_validate_imports_handles_package_init(self):
        """Validates imports from package __init__.py files."""
        from scripts.validate_claude_md import validate_imports

        content = "from geoparquet_io.api import Table\n"
        errors = validate_imports(content, PROJECT_ROOT)
        # Should not report module as missing (api/ is a package with __init__.py)
        assert not any("does not exist" in e and "api" in e for e in errors)


# ---------------------------------------------------------------------------
# Unit tests: _parse_import_names
# ---------------------------------------------------------------------------


class TestParseImportNames:
    """Unit tests for the _parse_import_names helper."""

    def test_simple_names(self):
        """Parses a simple comma-separated list."""
        from scripts.validate_claude_md import _parse_import_names

        assert _parse_import_names("foo, bar, baz") == ["foo", "bar", "baz"]

    def test_strips_whitespace(self):
        """Strips leading/trailing whitespace from names."""
        from scripts.validate_claude_md import _parse_import_names

        assert _parse_import_names("  foo ,  bar  ") == ["foo", "bar"]

    def test_handles_as_alias(self):
        """Extracts original name from alias."""
        from scripts.validate_claude_md import _parse_import_names

        assert _parse_import_names("foo as f, bar as b") == ["foo", "bar"]

    def test_filters_comments(self):
        """Filters out comment lines."""
        from scripts.validate_claude_md import _parse_import_names

        assert _parse_import_names("foo, # comment") == ["foo"]

    def test_filters_empty_and_parens(self):
        """Filters out empty strings and parentheses."""
        from scripts.validate_claude_md import _parse_import_names

        assert _parse_import_names("(, foo, ), ") == ["foo"]

    def test_multiline_content(self):
        """Handles content that comes from parenthesized imports."""
        from scripts.validate_claude_md import _parse_import_names

        content = "\n    get_duckdb_connection,\n    needs_httpfs,\n"
        names = _parse_import_names(content)
        assert names == ["get_duckdb_connection", "needs_httpfs"]


# ---------------------------------------------------------------------------
# Unit tests: _get_module_exports / _collect_names
# ---------------------------------------------------------------------------


class TestGetModuleExports:
    """Unit tests for the _get_module_exports helper."""

    def test_finds_functions(self):
        """Finds top-level function definitions."""
        from scripts.validate_claude_md import _get_module_exports

        tree = ast.parse("def foo(): pass\ndef bar(): pass\n")
        exports = _get_module_exports(tree)
        assert exports == {"foo", "bar"}

    def test_finds_classes(self):
        """Finds top-level class definitions."""
        from scripts.validate_claude_md import _get_module_exports

        tree = ast.parse("class Foo: pass\n")
        exports = _get_module_exports(tree)
        assert "Foo" in exports

    def test_finds_assignments(self):
        """Finds top-level variable assignments."""
        from scripts.validate_claude_md import _get_module_exports

        tree = ast.parse("X = 1\nY: int = 2\n")
        exports = _get_module_exports(tree)
        assert "X" in exports
        assert "Y" in exports

    def test_finds_imports(self):
        """Finds top-level import re-exports."""
        from scripts.validate_claude_md import _get_module_exports

        tree = ast.parse("from os.path import join\nimport sys\n")
        exports = _get_module_exports(tree)
        assert "join" in exports
        assert "sys" in exports

    def test_finds_names_in_try_except(self):
        """Finds names defined inside try/except blocks."""
        from scripts.validate_claude_md import _get_module_exports

        code = textwrap.dedent("""\
            try:
                import json
            except ImportError:
                import ast as syntax_tree

            def foo():
                pass
        """)
        tree = ast.parse(code)
        exports = _get_module_exports(tree)
        assert "json" in exports
        assert "foo" in exports

    def test_finds_names_in_if_blocks(self):
        """Finds names defined inside if blocks."""
        from scripts.validate_claude_md import _get_module_exports

        code = textwrap.dedent("""\
            import sys
            if sys.platform == "linux":
                import json
            else:
                import ast as syntax_tree
        """)
        tree = ast.parse(code)
        exports = _get_module_exports(tree)
        assert "json" in exports
        assert "sys" in exports

    def test_finds_aliased_imports(self):
        """Handles 'import X as Y' by returning the alias name."""
        from scripts.validate_claude_md import _get_module_exports

        tree = ast.parse("import numpy as np\n")
        exports = _get_module_exports(tree)
        assert "np" in exports
        assert "numpy" not in exports


# ---------------------------------------------------------------------------
# Unit tests: required sections
# ---------------------------------------------------------------------------


class TestRequiredSections:
    """Unit tests for required section validation."""

    def test_passes_with_all_sections(self):
        """No errors when all required sections are present."""
        from scripts.validate_claude_md import validate_required_sections

        content = (
            "## Project Overview\n"
            "Some text.\n"
            "## Architecture & Key Files\n"
            "More text.\n"
            "## Testing with uv\n"
            "Test info.\n"
            "## Git Workflow\n"
            "Git info.\n"
        )
        errors = validate_required_sections(content)
        assert errors == []

    def test_reports_missing_section(self):
        """Reports error for missing required sections."""
        from scripts.validate_claude_md import validate_required_sections

        content = "## Some Other Section\nContent.\n"
        errors = validate_required_sections(content)
        assert len(errors) >= 1

    def test_reports_specific_missing_sections(self):
        """Each missing section gets its own error."""
        from scripts.validate_claude_md import validate_required_sections

        content = "## Project Overview\n"
        errors = validate_required_sections(content)
        # Should report Architecture, Testing, Git Workflow as missing
        assert len(errors) == 3
        missing_keywords = [e.split("'")[1] for e in errors]
        assert "Architecture" in missing_keywords
        assert "Testing" in missing_keywords
        assert "Git Workflow" in missing_keywords

    def test_matches_partial_section_names(self):
        """Section matching is substring-based."""
        from scripts.validate_claude_md import validate_required_sections

        content = (
            "## Project Overview\n"
            "## Architecture & Key Files\n"
            "## Testing with uv\n"
            "## Git Workflow\n"
        )
        errors = validate_required_sections(content)
        assert errors == []

    def test_matches_h3_headings(self):
        """Matches ### headings too, not just ##."""
        from scripts.validate_claude_md import validate_required_sections

        content = "### Project Overview\n### Architecture\n### Testing\n### Git Workflow\n"
        errors = validate_required_sections(content)
        assert errors == []

    def test_case_insensitive_matching(self):
        """Section matching is case-insensitive."""
        from scripts.validate_claude_md import validate_required_sections

        content = "## project overview\n## ARCHITECTURE\n## Testing\n## git workflow\n"
        errors = validate_required_sections(content)
        assert errors == []


# ---------------------------------------------------------------------------
# Unit tests: main() function
# ---------------------------------------------------------------------------


class TestMainFunction:
    """Unit tests for the main() entry point."""

    def test_main_with_missing_file(self, tmp_path):
        """Returns exit code 2 for missing CLAUDE.md."""
        from scripts.validate_claude_md import main

        missing = tmp_path / "nonexistent.md"
        rc = main(["--claude-md", str(missing)])
        assert rc == 2

    def test_main_with_valid_minimal_file(self, tmp_path):
        """Returns exit code 0 or 1 with a minimal valid file."""
        from scripts.validate_claude_md import main

        fake = tmp_path / "CLAUDE.md"
        fake.write_text("## Project Overview\n## Architecture\n## Testing\n## Git Workflow\n")
        rc = main(["--claude-md", str(fake)])
        assert rc in (0, 1)

    def test_main_with_custom_project_root(self, tmp_path):
        """Accepts --project-root argument."""
        from scripts.validate_claude_md import main

        fake = tmp_path / "CLAUDE.md"
        fake.write_text("## Project Overview\n## Architecture\n## Testing\n## Git Workflow\n")
        rc = main(["--claude-md", str(fake), "--project-root", str(tmp_path)])
        assert rc in (0, 1)

    def test_main_catches_validator_exceptions(self, tmp_path, monkeypatch):
        """Validator exceptions are caught and reported as errors."""
        from scripts import validate_claude_md
        from scripts.validate_claude_md import main

        fake = tmp_path / "CLAUDE.md"
        fake.write_text("## Project Overview\n## Architecture\n## Testing\n## Git Workflow\n")

        def _raise_error(content):
            msg = "boom"
            raise RuntimeError(msg)

        monkeypatch.setattr(validate_claude_md, "validate_required_sections", _raise_error)
        rc = main(["--claude-md", str(fake), "--project-root", str(tmp_path)])
        # Should not crash -- should return 1 (errors found)
        assert rc == 1


# ---------------------------------------------------------------------------
# Unit tests: _BUILTIN_PYTEST_MARKERS
# ---------------------------------------------------------------------------


class TestBuiltinMarkers:
    """Verify the built-in markers list is reasonable."""

    def test_builtin_markers_is_frozenset(self):
        """The built-in markers constant is a frozenset (immutable)."""
        from scripts.validate_claude_md import _BUILTIN_PYTEST_MARKERS

        assert isinstance(_BUILTIN_PYTEST_MARKERS, frozenset)

    @pytest.mark.parametrize(
        "marker",
        [
            "parametrize",
            "skip",
            "skipif",
            "xfail",
            "usefixtures",
            "filterwarnings",
            "timeout",
        ],
    )
    def test_expected_builtins_present(self, marker):
        """Each expected built-in marker is in the set."""
        from scripts.validate_claude_md import _BUILTIN_PYTEST_MARKERS

        assert marker in _BUILTIN_PYTEST_MARKERS
