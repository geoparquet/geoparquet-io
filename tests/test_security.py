"""Tests for security scanning configuration."""

import subprocess
import tomllib
from pathlib import Path

import pytest

# Timeout for security tool subprocess calls (seconds).
_SUBPROCESS_TIMEOUT = 120

# Root of the project, resolved relative to this test file so the tests
# work regardless of pytest's working directory.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_PYPROJECT = _PROJECT_ROOT / "pyproject.toml"

# Maximum number of bandit skips allowed.  A generous upper bound that
# prevents the skip list from growing unchecked (which would silently
# defeat the purpose of running bandit at all).
_MAX_BANDIT_SKIPS = 15


def _load_pyproject() -> dict:
    """Load and return the parsed pyproject.toml."""
    with open(_PYPROJECT, "rb") as f:
        return tomllib.load(f)


@pytest.mark.meta
class TestSecurityToolAvailability:
    """Verify that security tools are installed and runnable.

    Tooling checks only: they spawn `uv run` subprocesses and are covered for
    real by the dedicated `security` job, which actually runs bandit and
    pip-audit. Marked `meta` so they stay out of the fast suite. The bandit
    configuration tests below are plain pyproject.toml parsing and stay fast.
    """

    def test_bandit_command_exists(self):
        """Verify bandit is installed and reports its version."""
        result = subprocess.run(
            ["uv", "run", "bandit", "--version"],
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT,
            cwd=_PROJECT_ROOT,
        )
        assert result.returncode == 0, f"bandit --version failed:\n{result.stderr}"

    def test_pip_audit_command_exists(self):
        """Verify pip-audit is installed and reports its version."""
        result = subprocess.run(
            ["uv", "run", "pip-audit", "--version"],
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT,
            cwd=_PROJECT_ROOT,
        )
        assert result.returncode == 0, f"pip-audit --version failed:\n{result.stderr}"


# NOTE: The live dependency audit ("does pip-audit pass?") intentionally does
# NOT live here as a pytest test. It runs in the dedicated `security` job in
# .github/workflows/tests.yml, which blocks PRs on new vulnerabilities and is
# non-blocking on nightly schedules. The self-expiring CVE-ignore list lives in
# .pip-audit-ignores at the repo root (parsed by scripts/pip_audit_ignores.py)
# and is shared with security-audit.yml. A duplicate audit used to live here
# and its ignore list drifted out of sync with the workflow's (PYSEC-2026-196),
# silently breaking `main` on every push. Keep the audit in exactly one place —
# the workflow — and the ignore list in exactly one file.


class TestBanditConfiguration:
    """Validate bandit configuration in pyproject.toml."""

    def test_bandit_config_exists(self):
        """Verify [tool.bandit] section is present."""
        config = _load_pyproject()
        assert "bandit" in config.get("tool", {}), "Missing [tool.bandit] section in pyproject.toml"

    def test_exclude_dirs_includes_tests(self):
        """Verify bandit excludes the tests directory."""
        config = _load_pyproject()
        exclude_dirs = config["tool"]["bandit"].get("exclude_dirs", [])
        assert "tests" in exclude_dirs, "tests dir not excluded from bandit scan"

    def test_exclude_dirs_includes_venv(self):
        """Verify bandit excludes the .venv directory."""
        config = _load_pyproject()
        exclude_dirs = config["tool"]["bandit"].get("exclude_dirs", [])
        assert ".venv" in exclude_dirs, ".venv dir not excluded from bandit scan"

    def test_skips_list_is_bounded(self):
        """Guard against unbounded growth of the bandit skip list.

        Each skip weakens the security scan, so we enforce an upper limit.
        If you legitimately need to raise the cap, update _MAX_BANDIT_SKIPS
        and add a comment explaining why.
        """
        config = _load_pyproject()
        skips = config["tool"]["bandit"].get("skips", [])
        assert len(skips) <= _MAX_BANDIT_SKIPS, (
            f"Bandit skip list has {len(skips)} entries (max {_MAX_BANDIT_SKIPS}). "
            f"Review whether all skips are still necessary before raising the limit."
        )

    def test_every_skip_has_valid_id_format(self):
        """Verify each skip is a valid bandit test ID (e.g., B101, B608)."""
        config = _load_pyproject()
        skips = config["tool"]["bandit"].get("skips", [])
        for skip in skips:
            assert skip.startswith("B") and skip[1:].isdigit(), (
                f"Invalid bandit skip ID: {skip!r}. Expected format: B<digits>"
            )


class TestDevDependencies:
    """Verify security tools are declared as dev dependencies."""

    def test_bandit_in_dev_deps(self):
        """Verify bandit is listed in [project.optional-dependencies.dev]."""
        config = _load_pyproject()
        dev_deps = config.get("project", {}).get("optional-dependencies", {}).get("dev", [])
        dep_names = [
            d.split(">=")[0].split("==")[0].split("<")[0].strip().lower() for d in dev_deps
        ]
        assert "bandit" in dep_names, "bandit not found in dev extras"

    def test_pip_audit_in_dev_deps(self):
        """Verify pip-audit is listed in [project.optional-dependencies.dev]."""
        config = _load_pyproject()
        dev_deps = config.get("project", {}).get("optional-dependencies", {}).get("dev", [])
        dep_names = [
            d.split(">=")[0].split("==")[0].split("<")[0].strip().lower() for d in dev_deps
        ]
        assert "pip-audit" in dep_names, "pip-audit not found in dev extras"
