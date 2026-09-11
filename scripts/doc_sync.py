#!/usr/bin/env python3
"""Sync generated documentation sections from code introspection.

This script auto-generates documentation sections in CLAUDE.md and skill (geoparquet.md)
to prevent drift between code and documentation.

CLAUDE.md sections:
- CLI command table from Click introspection
- Test markers from pyproject.toml

skill (geoparquet.md) sections:
- Command reference table
- Inspection commands
- Check commands
- Compression options

Also syncs:
- docs/CHANGELOG.md from root CHANGELOG.md (MkDocs can't follow symlinks)

Usage:
    python scripts/doc_sync.py              # Show what would change
    python scripts/doc_sync.py --update     # Update all docs
    python scripts/doc_sync.py --check      # Exit 1 if outdated (CI mode)
    python scripts/doc_sync.py --claude     # Only CLAUDE.md
    python scripts/doc_sync.py --skill      # Only skill (geoparquet.md)
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import click

from geoparquet_io.core.parquet_writer import DEFAULT_ROW_GROUP_ROWS

if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomllib  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[no-redef]


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------


def update_section(content: str, section_name: str, new_content: str) -> str:
    """Replace a generated section in the content.

    Args:
        content: Full file content.
        section_name: Name used in BEGIN/END GENERATED markers.
        new_content: New section content including markers.

    Returns:
        Updated content with section replaced, or unchanged if section not found.
    """
    pattern = (
        rf"<!-- BEGIN GENERATED: {re.escape(section_name)} -->"
        r".*?"
        rf"<!-- END GENERATED: {re.escape(section_name)} -->"
    )

    if re.search(pattern, content, re.DOTALL):
        return re.sub(pattern, new_content, content, count=1, flags=re.DOTALL)
    else:
        return content


def get_cli_commands() -> dict[str, dict]:
    """Introspect Click CLI to get all commands and their structure."""
    from geoparquet_io.cli.main import cli

    result = {}
    for name, cmd in sorted(cli.commands.items()):
        if isinstance(cmd, click.Group):
            subcommands = sorted(cmd.commands.keys())
            result[name] = {
                "subcommands": subcommands,
                "help": cmd.help or "",
            }
        else:
            result[name] = {
                "subcommands": [],
                "help": cmd.help or "",
            }
    return result


# ---------------------------------------------------------------------------
# CLAUDE.md generators
# ---------------------------------------------------------------------------


def generate_cli_section() -> str:
    """Generate the CLI commands markdown section for CLAUDE.md."""
    commands = get_cli_commands()

    lines = [
        "<!-- BEGIN GENERATED: cli-commands -->",
        "### CLI Command Groups",
        "",
        "| Command Group | Subcommands | Description |",
        "|---------------|-------------|-------------|",
    ]

    for name, info in sorted(commands.items()):
        subcommands = ", ".join(info["subcommands"]) if info["subcommands"] else ""
        help_text = info["help"].strip()
        # Normalize whitespace before extracting first sentence
        normalized_help = re.sub(r"\s+", " ", help_text).strip()
        desc = normalized_help.split(".", 1)[0].strip() if normalized_help else ""
        lines.append(f"| `gpio {name}` | {subcommands} | {desc} |")

    lines.append("<!-- END GENERATED: cli-commands -->")
    return "\n".join(lines)


def get_test_markers(pyproject_path: Path) -> list[dict]:
    """Parse test markers from pyproject.toml."""
    with open(pyproject_path, "rb") as f:
        config = tomllib.load(f)

    markers_raw = config.get("tool", {}).get("pytest", {}).get("ini_options", {}).get("markers", [])
    markers = []
    for marker in markers_raw:
        if ":" in marker:
            name, desc = marker.split(":", 1)
            markers.append({"name": name.strip(), "description": desc.strip()})
    return markers


def generate_markers_section(pyproject_path: Path) -> str:
    """Generate the test markers markdown section."""
    markers = get_test_markers(pyproject_path)

    lines = [
        "<!-- BEGIN GENERATED: test-markers -->",
        "### Test Markers",
        "",
        "| Marker | Description |",
        "|--------|-------------|",
    ]

    for marker in markers:
        lines.append(f"| `@pytest.mark.{marker['name']}` | {marker['description']} |")

    lines.append("<!-- END GENERATED: test-markers -->")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# skill (geoparquet.md) generators
# ---------------------------------------------------------------------------


def truncate_text(text: str, max_len: int = 60) -> str:
    """Truncate text at word boundary, normalizing whitespace first."""
    # Normalize: collapse all whitespace (including newlines) to single spaces
    normalized = re.sub(r"\s+", " ", text).strip()
    if len(normalized) <= max_len:
        return normalized
    # Find last space before max_len
    truncated = normalized[:max_len].rsplit(" ", 1)[0]
    if not truncated:  # No space found, hard cut
        truncated = normalized[: max_len - 3]
    return truncated + "..."


def generate_skill_commands_table() -> str:
    """Generate the CLI commands table for skill (geoparquet.md)."""
    commands = get_cli_commands()

    lines = [
        "<!-- BEGIN GENERATED: skill-commands -->",
        "### Command Reference",
        "",
        "| Command | Subcommands | Description |",
        "|---------|-------------|-------------|",
    ]

    for name, info in sorted(commands.items()):
        subs = ", ".join(info["subcommands"]) if info["subcommands"] else ""
        desc = truncate_text(info["help"], 60)
        lines.append(f"| `gpio {name}` | {subs} | {desc} |")

    lines.append("<!-- END GENERATED: skill-commands -->")
    return "\n".join(lines)


def generate_compression_options() -> str:
    """Generate compression options section from actual CLI defaults."""
    from geoparquet_io.cli.main import cli

    compression = "zstd"
    compression_level = 15

    if "convert" in cli.commands:
        convert_group = cli.commands["convert"]
        if isinstance(convert_group, click.Group) and "geoparquet" in convert_group.commands:
            geoparquet_cmd = convert_group.commands["geoparquet"]
            for param in geoparquet_cmd.params:
                if param.name == "compression":
                    val = param.default
                    if hasattr(val, "value"):
                        compression = str(val.value).lower()
                    elif val is not None and "Sentinel" not in str(type(val)):
                        compression = str(val).lower()
                elif param.name == "compression_level":
                    val = param.default
                    if val is not None and "Sentinel" not in str(type(val)):
                        compression_level = val

    lines = [
        "<!-- BEGIN GENERATED: compression-options -->",
        "### Compression Options",
        "",
        "Most write commands accept these options:",
        "",
        "| Option | Values | Default |",
        "|--------|--------|---------|",
        f"| `--compression` | zstd, snappy, gzip, lz4, brotli, uncompressed | {compression} |",
        f"| `--compression-level` | 1-22 (for zstd) | {compression_level} |",
        f"| `--row-group-size` | Number of rows per group | {DEFAULT_ROW_GROUP_ROWS:,} |",
        "<!-- END GENERATED: compression-options -->",
    ]
    return "\n".join(lines)


def generate_inspection_commands() -> str:
    """Generate inspection commands section."""
    from geoparquet_io.cli.main import cli

    lines = [
        "<!-- BEGIN GENERATED: inspect-commands -->",
        "### Inspection Commands",
        "",
        "```bash",
    ]

    if "inspect" in cli.commands:
        inspect_group = cli.commands["inspect"]
        if isinstance(inspect_group, click.Group):
            for subname, subcmd in sorted(inspect_group.commands.items()):
                help_line = (subcmd.help or "").split("\n")[0].strip()[:40].rstrip()
                lines.append(f"gpio inspect {subname} <file>  # {help_line}")

    lines.extend(
        [
            "```",
            "<!-- END GENERATED: inspect-commands -->",
        ]
    )
    return "\n".join(lines)


def generate_check_commands() -> str:
    """Generate check commands section."""
    from geoparquet_io.cli.main import cli

    lines = [
        "<!-- BEGIN GENERATED: check-commands -->",
        "### Validation Commands",
        "",
        "```bash",
        "# Run all checks",
        "gpio check all <file>",
        "",
        "# Individual checks",
    ]

    if "check" in cli.commands:
        check_group = cli.commands["check"]
        if isinstance(check_group, click.Group):
            for subname in sorted(check_group.commands.keys()):
                if subname != "all":
                    lines.append(f"gpio check {subname} <file>")

    lines.extend(
        [
            "",
            "# Auto-fix issues",
            "gpio check all <file> --fix --fix-output <fixed_file>",
            "```",
            "<!-- END GENERATED: check-commands -->",
        ]
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def sync_claude_md(project_root: Path, update: bool, check: bool) -> tuple[bool, list[str]]:
    """Sync CLAUDE.md generated sections.

    Returns:
        Tuple of (changed, messages).
    """
    claude_md = project_root / "CLAUDE.md"
    pyproject = project_root / "pyproject.toml"

    if not claude_md.exists():
        return False, ["ERROR: CLAUDE.md not found"]

    if not pyproject.exists():
        return False, ["ERROR: pyproject.toml not found"]

    original = claude_md.read_text(encoding="utf-8")
    updated = original

    sections = {
        "cli-commands": generate_cli_section(),
        "test-markers": generate_markers_section(pyproject),
    }

    for name, content in sections.items():
        updated = update_section(updated, name, content)

    if updated == original:
        return False, ["CLAUDE.md: up to date"]

    messages = []
    for name in sections:
        if f"<!-- BEGIN GENERATED: {name} -->" not in original:
            messages.append(f"CLAUDE.md: would ADD {name}")
        else:
            pattern = (
                rf"<!-- BEGIN GENERATED: {re.escape(name)} -->"
                r".*?"
                rf"<!-- END GENERATED: {re.escape(name)} -->"
            )
            old_match = re.search(pattern, original, re.DOTALL)
            new_match = re.search(pattern, updated, re.DOTALL)
            if old_match and new_match and old_match.group() != new_match.group():
                messages.append(f"CLAUDE.md: would UPDATE {name}")

    if update:
        claude_md.write_text(updated, encoding="utf-8")
        messages = ["CLAUDE.md: updated"]

    return True, messages


def sync_skill_md(project_root: Path, update: bool, check: bool) -> tuple[bool, list[str]]:
    """Sync skill generated sections.

    Returns:
        Tuple of (changed, messages).
    """
    skill_md = project_root / "geoparquet_io" / "skills" / "geoparquet.md"

    if not skill_md.exists():
        return False, [f"ERROR: {skill_md} not found"]

    original = skill_md.read_text(encoding="utf-8")
    updated = original

    sections = {
        "skill-commands": generate_skill_commands_table(),
        "compression-options": generate_compression_options(),
        "inspect-commands": generate_inspection_commands(),
        "check-commands": generate_check_commands(),
    }

    for name, content in sections.items():
        updated = update_section(updated, name, content)

    if updated == original:
        return False, ["skill (geoparquet.md): up to date"]

    messages = []
    for name in sections:
        if f"<!-- BEGIN GENERATED: {name} -->" not in original:
            messages.append(f"skill (geoparquet.md): would ADD {name}")
        else:
            pattern = (
                rf"<!-- BEGIN GENERATED: {re.escape(name)} -->"
                r".*?"
                rf"<!-- END GENERATED: {re.escape(name)} -->"
            )
            old_match = re.search(pattern, original, re.DOTALL)
            new_match = re.search(pattern, updated, re.DOTALL)
            if old_match and new_match and old_match.group() != new_match.group():
                messages.append(f"skill (geoparquet.md): would UPDATE {name}")

    if update:
        skill_md.write_text(updated, encoding="utf-8")
        messages = ["skill (geoparquet.md): updated"]

    return True, messages


def sync_contributing_md(project_root: Path, update: bool, check: bool) -> tuple[bool, list[str]]:
    """Sync contributing.md generated sections.

    Returns:
        Tuple of (changed, messages).
    """
    contrib_md = project_root / "docs" / "contributing.md"
    pyproject = project_root / "pyproject.toml"

    if not contrib_md.exists():
        return False, ["ERROR: docs/contributing.md not found"]

    if not pyproject.exists():
        return False, ["ERROR: pyproject.toml not found"]

    original = contrib_md.read_text(encoding="utf-8")
    updated = original

    sections = {
        "test-markers": generate_markers_section(pyproject),
    }

    for name, content in sections.items():
        updated = update_section(updated, name, content)

    if updated == original:
        return False, ["contributing.md: up to date"]

    messages = []
    for name in sections:
        if f"<!-- BEGIN GENERATED: {name} -->" not in original:
            messages.append(f"contributing.md: would ADD {name}")
        else:
            pattern = (
                rf"<!-- BEGIN GENERATED: {re.escape(name)} -->"
                r".*?"
                rf"<!-- END GENERATED: {re.escape(name)} -->"
            )
            old_match = re.search(pattern, original, re.DOTALL)
            new_match = re.search(pattern, updated, re.DOTALL)
            if old_match and new_match and old_match.group() != new_match.group():
                messages.append(f"contributing.md: would UPDATE {name}")

    if update:
        contrib_md.write_text(updated, encoding="utf-8")
        messages = ["contributing.md: updated"]

    return True, messages


def sync_changelog(project_root: Path, update: bool, check: bool) -> tuple[bool, list[str]]:
    """Sync CHANGELOG.md from root to docs/.

    The root CHANGELOG.md is the source of truth. docs/CHANGELOG.md is a copy
    for MkDocs (which doesn't follow symlinks outside docs/).

    Returns:
        Tuple of (changed, messages).
    """
    root_changelog = project_root / "CHANGELOG.md"
    docs_changelog = project_root / "docs" / "CHANGELOG.md"

    if not root_changelog.exists():
        return False, ["ERROR: CHANGELOG.md not found"]

    root_content = root_changelog.read_text(encoding="utf-8")

    if docs_changelog.exists():
        docs_content = docs_changelog.read_text(encoding="utf-8")
        if root_content == docs_content:
            return False, ["docs/CHANGELOG.md: up to date"]
    else:
        docs_content = ""

    if update:
        docs_changelog.write_text(root_content, encoding="utf-8")
        return True, ["docs/CHANGELOG.md: synced from root"]

    return True, ["docs/CHANGELOG.md: out of sync with root CHANGELOG.md"]


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Sync generated documentation sections from code")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--update", action="store_true", help="Update docs in place")
    group.add_argument(
        "--check", action="store_true", help="Check if docs are current (exit 1 if not)"
    )

    target = parser.add_mutually_exclusive_group()
    target.add_argument("--claude", action="store_true", help="Only sync CLAUDE.md")
    target.add_argument("--skill", action="store_true", help="Only sync skill (geoparquet.md)")

    args = parser.parse_args()

    project_root = Path(__file__).parent.parent
    all_messages = []
    any_changed = False

    # Sync CLAUDE.md
    if not args.skill:
        changed, messages = sync_claude_md(project_root, args.update, args.check)
        any_changed = any_changed or changed
        all_messages.extend(messages)

    # Sync skill (geoparquet.md)
    if not args.claude:
        changed, messages = sync_skill_md(project_root, args.update, args.check)
        any_changed = any_changed or changed
        all_messages.extend(messages)

    # Sync contributing.md
    if not args.claude and not args.skill:
        changed, messages = sync_contributing_md(project_root, args.update, args.check)
        any_changed = any_changed or changed
        all_messages.extend(messages)

    # Sync CHANGELOG.md (root → docs/)
    if not args.claude and not args.skill:
        changed, messages = sync_changelog(project_root, args.update, args.check)
        any_changed = any_changed or changed
        all_messages.extend(messages)

    # Output
    for msg in all_messages:
        print(msg)

    if args.check and any_changed:
        print("\nDocs are out of date. Run with --update to fix.")
        return 1

    if not args.update and any_changed:
        print("\nRun with --update to apply changes.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
