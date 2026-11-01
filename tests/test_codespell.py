"""Tests for codespell configuration."""

from __future__ import annotations

import subprocess
import tomllib
from pathlib import Path

import pytest

# Repo tooling checks: these shell out to codespell and duplicate what the CI
# lint job already runs via pre-commit, so they live in the meta lane instead
# of the fast suite.
pytestmark = pytest.mark.meta

# Resolve project root relative to this test file, so tests work
# regardless of the working directory pytest is invoked from.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"


class TestCodespell:
    """Test that codespell is configured and passing."""

    def test_codespell_command_exists(self):
        """Verify codespell command is available."""
        result = subprocess.run(
            ["uv", "run", "codespell", "--version"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

    def test_codespell_passes(self):
        """Verify no typos in codebase."""
        result = subprocess.run(
            ["uv", "run", "codespell"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        assert result.returncode == 0, f"Typos found:\n{result.stdout}\n{result.stderr}"

    def test_codespell_config_in_pyproject(self):
        """Verify codespell configuration exists in pyproject.toml."""
        with open(PYPROJECT_PATH, "rb") as f:
            config = tomllib.load(f)

        assert "codespell" in config.get("tool", {}), "codespell config missing from pyproject.toml"

    def test_ignore_list_includes_geospatial_terms(self):
        """Verify common geospatial terms are in ignore list."""
        with open(PYPROJECT_PATH, "rb") as f:
            config = tomllib.load(f)

        ignore_list = config.get("tool", {}).get("codespell", {}).get("ignore-words-list", "")
        # These abbreviations are commonly flagged as typos but are valid geospatial terms
        for term in ("crs", "nd"):
            assert term in ignore_list, f"{term} should be in codespell ignore list"

    def test_codespell_skips_binary_and_generated_files(self):
        """Verify codespell skips files that would produce false positives."""
        with open(PYPROJECT_PATH, "rb") as f:
            config = tomllib.load(f)

        skip = config.get("tool", {}).get("codespell", {}).get("skip", "")
        # These patterns should be skipped to avoid false positives
        for pattern in ("*.parquet", "*.lock", ".git"):
            assert pattern in skip, f"{pattern} should be in codespell skip list"
