#!/usr/bin/env python3
"""
PreToolUse hook that ensures pytest, ruff, and other Python tools use 'uv run'.
Automatically prefixes commands with 'uv run' when appropriate.
"""

import json
import sys

# Read the tool input from stdin
data = json.load(sys.stdin)

# Get the command from the tool input
tool_input = data.get("tool_input", {})
cmd = tool_input.get("command", "")

# Commands that should use 'uv run'
UV_COMMANDS = [
    "pytest",
    "ruff",
    "xenon",
    "pre-commit",
    "gpio",
    "python -m pytest",
    "python -m",
    "mypy",
    "black",
    "isort",
    "flake8",
    "pylint",
    "coverage",
    "tox",
    "pip",
    "vulture",
    "radon",
]

# Check if command needs 'uv run' prefix
needs_uv = False
for uv_cmd in UV_COMMANDS:
    if cmd.startswith(uv_cmd) and not cmd.startswith("uv run"):
        needs_uv = True
        break

# If it needs uv run, modify the command
if needs_uv:
    tool_input["command"] = f"uv run {cmd}"
    data["tool_input"] = tool_input

    # Output the modified command
    json.dump({"action": "allow", "modified_input": tool_input}, sys.stdout)
    sys.exit(0)

# Otherwise, allow the command as-is
json.dump({"action": "allow"}, sys.stdout)
