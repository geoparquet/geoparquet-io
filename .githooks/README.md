# Git Hooks for geoparquet-io

This directory contains optional git hooks to improve the development workflow.

## Available Hooks

### pre-push
Runs fast tests before pushing to ensure basic functionality works. Excludes slow and network tests for speed.

### commit-msg
Enforces imperative mood in commit messages (e.g., "Add feature" not "Added feature").

### post-checkout
Automatically runs `uv sync` when switching branches if pyproject.toml has changed.

## Installation

To use these hooks, configure git to use this directory:

```bash
git config core.hooksPath .githooks
```

To disable temporarily:

```bash
git config --unset core.hooksPath
```

To bypass a hook for a single operation:

```bash
git push --no-verify  # Skip pre-push
git commit --no-verify  # Skip commit-msg
```

## Note

These hooks are optional and complementary to the pre-commit hooks defined in `.pre-commit-config.yaml`. The pre-commit hooks handle code formatting and linting, while these git hooks handle workflow tasks like testing and dependency syncing.
