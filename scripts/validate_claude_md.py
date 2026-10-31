#!/usr/bin/env python3
"""Validate CLAUDE.md against the actual codebase.

This script catches drift between documentation and code by validating:
1. CLI command references exist
2. File path references exist
3. Test markers match pyproject.toml
4. Import examples are valid
5. Required sections exist

Exit codes:
    0: All validations passed
    1: Validation errors found
    2: Script error (file not found, etc.)
"""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from pathlib import Path

# ---------------------------------------------------------------------------
# Extraction helpers (pure functions operating on text)
# ---------------------------------------------------------------------------


def extract_gpio_commands(content: str) -> set[str]:
    """Extract top-level gpio sub-command names mentioned in *content*.

    We look for patterns like:
      - ``gpio <command>`` inside backticks
      - ``gpio <command>`` at the start of a line inside fenced code blocks

    Returns a set of first-level sub-command names (e.g. ``{"add", "convert"}``).
    """
    commands: set[str] = set()

    # Pattern 1: backtick-wrapped ``gpio <word>``
    for m in re.finditer(r"`gpio\s+(\w[\w-]*)(?:\s|`)", content):
        commands.add(m.group(1))

    # Pattern 2: bare ``gpio <word>`` at start of line (common in code blocks)
    for m in re.finditer(r"^gpio\s+(\w[\w-]*)", content, re.MULTILINE):
        commands.add(m.group(1))

    # Filter out things that look like file arguments rather than sub-commands
    commands.discard("file.parquet")
    commands.discard("input.parquet")
    commands.discard("output.parquet")

    return commands


def extract_file_paths(content: str) -> set[str]:
    """Extract file-path references from backticks in *content*.

    We match paths that:
      - Start with a known project directory prefix (``geoparquet_io/``,
        ``core/``, ``cli/``, ``api/``, ``tests/``, ``docs/``, ``scripts/``)
      - End with ``.py`` or ``.md``
      - Do NOT contain wildcard (``*``) or template (``<``, ``>``) characters
    """
    pattern = (
        r"`("
        r"(?:geoparquet_io/|core/|cli/|api/|tests/|docs/|scripts/)"
        r"[a-zA-Z0-9_/.-]+"
        r"\.(?:py|md)"
        r")`"
    )
    paths: set[str] = set()
    for m in re.finditer(pattern, content):
        p = m.group(1)
        # Skip wildcard or template patterns
        if "*" in p or "<" in p or ">" in p:
            continue
        paths.add(p)
    return paths


_BUILTIN_PYTEST_MARKERS = frozenset(
    {
        "parametrize",
        "skip",
        "skipif",
        "xfail",
        "usefixtures",
        "filterwarnings",
        "timeout",
    }
)


def extract_test_markers(content: str) -> set[str]:
    """Extract pytest marker names referenced in *content*.

    We look for:
      - ``@pytest.mark.<name>``
      - ``-m "not <name> and not <name2>"``  (words after ``not``)
      - ``-m '<name> or <name2>'``

    Built-in pytest markers (``parametrize``, ``skip``, ``xfail``, etc.)
    are excluded since they do not need to be declared in ``pyproject.toml``.
    """
    markers: set[str] = set()

    # Pattern 1: @pytest.mark.<name>
    for m in re.finditer(r"@pytest\.mark\.(\w+)", content):
        markers.add(m.group(1))

    # Pattern 2: -m "..." or -m '...' flag contents
    for m in re.finditer(r"""-m\s+["']([^"']+)["']""", content):
        expr = m.group(1)
        # Extract bare words that aren't boolean operators
        for word in re.findall(r"\b(\w+)\b", expr):
            if word not in ("not", "and", "or"):
                markers.add(word)

    # Remove built-in pytest markers that don't need pyproject.toml definitions
    markers -= _BUILTIN_PYTEST_MARKERS

    return markers


def extract_imports(content: str) -> list[tuple[str, list[str]]]:
    """Extract ``from X import Y, Z`` statements from *content*.

    Only matches imports from ``geoparquet_io.*`` modules.  Handles both
    single-line imports and multi-line parenthesized imports, as well as
    ``as`` aliases.

    Returns a list of ``(module_path, [name1, name2, ...])`` tuples.
    """
    imports: list[tuple[str, list[str]]] = []

    # Pattern 1: multi-line parenthesized imports
    #   from geoparquet_io.foo import (
    #       bar,
    #       baz,
    #   )
    paren_pattern = r"from\s+(geoparquet_io\.\S+)\s+import\s+\(([^)]+)\)"
    for m in re.finditer(paren_pattern, content, re.DOTALL):
        module = m.group(1)
        names = _parse_import_names(m.group(2))
        if names:
            imports.append((module, names))

    # Pattern 2: single-line imports
    #   from geoparquet_io.foo import bar, baz
    single_pattern = r"from\s+(geoparquet_io\.\S+)\s+import\s+(?!\()([^\n]+)"
    for m in re.finditer(single_pattern, content):
        module = m.group(1)
        names = _parse_import_names(m.group(2))
        if names:
            imports.append((module, names))

    return imports


def _parse_import_names(names_str: str) -> list[str]:
    """Parse import names from a comma-separated string.

    Handles ``as`` aliases by extracting only the original name, and
    filters out empty strings, comments, and parentheses.
    """
    names: list[str] = []
    for part in names_str.split(","):
        part = part.strip().rstrip(",")
        if not part or part.startswith("#") or part in ("(", ")"):
            continue
        # Handle ``name as alias`` -- validate the original name
        if " as " in part:
            part = part.split(" as ")[0].strip()
        if part:
            names.append(part)
    return names


# ---------------------------------------------------------------------------
# CLI introspection
# ---------------------------------------------------------------------------


def get_actual_commands() -> set[str]:
    """Return the set of top-level sub-command names registered on the CLI.

    Uses Click introspection on the real ``cli`` group object.
    """
    import click

    from geoparquet_io.cli.main import cli

    commands: set[str] = set()
    if isinstance(cli, click.Group):
        for name in cli.commands:
            commands.add(name)
    return commands


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------


def validate_cli_commands(content: str) -> list[str]:
    """Validate that gpio commands mentioned in CLAUDE.md exist."""
    errors: list[str] = []
    referenced = extract_gpio_commands(content)
    actual = get_actual_commands()

    for cmd in sorted(referenced):
        if cmd not in actual:
            errors.append(
                f"CLI command 'gpio {cmd}' referenced in CLAUDE.md "
                f"but not found in CLI (available: {', '.join(sorted(actual))})"
            )
    return errors


def validate_file_paths(content: str, project_root: Path) -> list[str]:
    """Validate that file paths in backticks exist on disk."""
    errors: list[str] = []
    paths = extract_file_paths(content)

    for p in sorted(paths):
        # Try the path as-is first (may already be relative to project root)
        candidate = project_root / p
        if candidate.exists():
            continue

        # If the path starts with a sub-package dir (core/, cli/, api/, etc.),
        # also try under geoparquet_io/
        prefixed = project_root / "geoparquet_io" / p
        if prefixed.exists():
            continue

        errors.append(f"File path '{p}' referenced in CLAUDE.md does not exist")

    return errors


def validate_test_markers(content: str, pyproject_path: Path) -> list[str]:
    """Validate test markers match pyproject.toml definitions."""
    errors: list[str] = []

    if not pyproject_path.exists():
        errors.append(f"pyproject.toml not found at {pyproject_path}")
        return errors

    with open(pyproject_path, "rb") as f:
        pyproject = tomllib.load(f)

    # Extract defined marker names from pyproject.toml
    markers_raw = (
        pyproject.get("tool", {}).get("pytest", {}).get("ini_options", {}).get("markers", [])
    )
    defined_markers: set[str] = set()
    for marker_line in markers_raw:
        # Format: "name: description"
        name = marker_line.split(":")[0].strip()
        if name:
            defined_markers.add(name)

    referenced = extract_test_markers(content)

    for marker in sorted(referenced):
        if marker not in defined_markers:
            errors.append(
                f"Test marker '@pytest.mark.{marker}' referenced in CLAUDE.md "
                f"but not defined in pyproject.toml markers "
                f"(defined: {', '.join(sorted(defined_markers))})"
            )

    return errors


def validate_imports(content: str, project_root: Path) -> list[str]:
    """Validate import examples are valid (module exists, names exported)."""
    errors: list[str] = []
    imports = extract_imports(content)

    for module_dotpath, names in imports:
        # Convert dotted module path to filesystem path
        rel_path = module_dotpath.replace(".", "/") + ".py"
        module_file = project_root / rel_path

        if not module_file.exists():
            # Maybe it's a package (__init__.py)
            pkg_init = project_root / module_dotpath.replace(".", "/") / "__init__.py"
            if not pkg_init.exists():
                errors.append(
                    f"Import module '{module_dotpath}' referenced in CLAUDE.md "
                    f"does not exist (looked for {rel_path})"
                )
                continue
            module_file = pkg_init

        # Parse the module AST to find exported names
        try:
            source = module_file.read_text(encoding="utf-8")
            tree = ast.parse(source)
        except (SyntaxError, OSError) as exc:
            errors.append(f"Could not parse {module_file}: {exc}")
            continue

        exports = _get_module_exports(tree)

        for name in names:
            if name not in exports:
                errors.append(
                    f"Name '{name}' imported from '{module_dotpath}' in CLAUDE.md "
                    f"does not exist in the module"
                )

    return errors


def _get_module_exports(tree: ast.Module) -> set[str]:
    """Return the set of names defined at module level in an AST.

    Walks into ``if``/``try``/``with`` blocks at module level to catch
    names defined conditionally (e.g. ``try: import X except: ...``).
    """
    exports: set[str] = set()
    _collect_names(tree, exports)
    return exports


def _collect_names(node: ast.AST, exports: set[str]) -> None:
    """Recursively collect names from an AST node.

    Descends into compound statements (``if``, ``try``, ``with``) to find
    names that are defined conditionally at module level.
    """
    for child in ast.iter_child_nodes(node):
        if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
            exports.add(child.name)
        elif isinstance(child, ast.ClassDef):
            exports.add(child.name)
        elif isinstance(child, ast.Assign):
            for target in child.targets:
                if isinstance(target, ast.Name):
                    exports.add(target.id)
        elif isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
            exports.add(child.target.id)
        elif isinstance(child, ast.ImportFrom):
            if child.names:
                for alias in child.names:
                    name = alias.asname if alias.asname else alias.name
                    exports.add(name)
        elif isinstance(child, ast.Import):
            for alias in child.names:
                name = alias.asname if alias.asname else alias.name
                exports.add(name)
        elif isinstance(child, ast.If | ast.Try | ast.With):
            # Recurse into compound statements to find conditional definitions
            _collect_names(child, exports)
        elif hasattr(ast, "TryStar") and isinstance(child, ast.TryStar):
            _collect_names(child, exports)


def validate_required_sections(content: str) -> list[str]:
    """Validate required sections exist.

    Checks that the CLAUDE.md contains headings (``##`` or ``###``) whose
    text includes each of the required keywords.
    """
    errors: list[str] = []

    required_keywords = [
        "Project Overview",
        "Architecture",
        "Testing",
        "Git Workflow",
    ]

    # Extract all headings (## or ### level)
    headings = re.findall(r"^#{2,3}\s+(.+)$", content, re.MULTILINE)

    for keyword in required_keywords:
        found = any(keyword.lower() in h.lower() for h in headings)
        if not found:
            errors.append(f"Required section '{keyword}' not found in CLAUDE.md headings")

    return errors


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Run all validations and report results."""
    import argparse

    parser = argparse.ArgumentParser(description="Validate CLAUDE.md against the codebase")
    parser.add_argument(
        "--claude-md",
        type=Path,
        default=None,
        help="Path to CLAUDE.md (default: auto-detect from project root)",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=None,
        help="Path to the project root (default: parent of scripts/)",
    )

    args = parser.parse_args(argv)

    # Resolve project root
    if args.project_root:
        project_root = args.project_root.resolve()
    else:
        project_root = Path(__file__).resolve().parent.parent

    # Resolve CLAUDE.md path
    if args.claude_md:
        claude_md = args.claude_md.resolve()
    else:
        claude_md = project_root / "CLAUDE.md"

    if not claude_md.exists():
        print(f"ERROR: CLAUDE.md not found at {claude_md}")
        return 2

    content = claude_md.read_text(encoding="utf-8")
    all_errors: list[str] = []

    # Run each validator
    validators = [
        ("CLI commands", lambda: validate_cli_commands(content)),
        ("File paths", lambda: validate_file_paths(content, project_root)),
        (
            "Test markers",
            lambda: validate_test_markers(content, project_root / "pyproject.toml"),
        ),
        ("Imports", lambda: validate_imports(content, project_root)),
        ("Required sections", lambda: validate_required_sections(content)),
    ]

    for name, validator in validators:
        try:
            errors = validator()
        except Exception as exc:
            errors = [f"Validator '{name}' raised an exception: {exc}"]
        if errors:
            all_errors.extend(errors)

    if all_errors:
        print(f"CLAUDE.md validation failed with {len(all_errors)} error(s):\n")
        for error in all_errors:
            print(f"  - {error}")
        return 1

    print("CLAUDE.md validation passed!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
