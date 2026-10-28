"""Tests for mypy type checking configuration."""

import importlib
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]

# Repo tooling checks: these shell out to mypy and duplicate the CI lint job,
# so they run in the meta lane instead of the fast suite. The full package
# type-check keeps its own `slow` mark on top of this.
pytestmark = pytest.mark.meta

# Timeout in seconds for subprocess calls
_SUBPROCESS_TIMEOUT = 120

# Root of the repository (resolve relative to this test file)
_REPO_ROOT = Path(__file__).resolve().parent.parent
_PYPROJECT = _REPO_ROOT / "pyproject.toml"


def _load_pyproject() -> dict:
    """Load and return the parsed pyproject.toml."""
    with open(_PYPROJECT, "rb") as f:
        return tomllib.load(f)


class TestMypyConfiguration:
    """Test that mypy is configured and passing."""

    def test_mypy_command_exists(self):
        """Verify mypy command is available."""
        result = subprocess.run(
            [sys.executable, "-m", "mypy", "--version"],
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT,
        )
        assert result.returncode == 0, f"mypy --version failed:\n{result.stdout}\n{result.stderr}"
        assert "mypy" in result.stdout

    @pytest.mark.slow
    def test_mypy_passes_on_package(self):
        """Verify mypy passes on the main package."""
        result = subprocess.run(
            [sys.executable, "-m", "mypy", "geoparquet_io/"],
            capture_output=True,
            text=True,
            cwd=str(_REPO_ROOT),
            timeout=_SUBPROCESS_TIMEOUT,
        )
        assert result.returncode == 0, f"mypy failed:\n{result.stdout}\n{result.stderr}"

    def test_mypy_config_in_pyproject(self):
        """Verify mypy configuration exists in pyproject.toml."""
        config = _load_pyproject()
        assert "mypy" in config.get("tool", {}), "mypy config missing from pyproject.toml"

    def test_mypy_config_has_required_settings(self):
        """Verify mypy config has the expected base settings."""
        config = _load_pyproject()
        mypy_config = config["tool"]["mypy"]
        assert mypy_config.get("python_version") == "3.10"
        assert mypy_config.get("ignore_missing_imports") is True

    def test_mypy_overrides_reference_existing_modules(self):
        """Verify every module listed in mypy overrides actually exists."""
        config = _load_pyproject()
        overrides = config["tool"]["mypy"].get("overrides", [])

        missing = []
        for override in overrides:
            modules = override.get("module", [])
            if isinstance(modules, str):
                modules = [modules]
            for module_name in modules:
                # Check if the module can be found as a file on disk
                parts = module_name.split(".")
                # Try as a .py file
                py_path = _REPO_ROOT / Path(*parts).with_suffix(".py")
                # Try as a package (__init__.py)
                pkg_path = _REPO_ROOT / Path(*parts) / "__init__.py"
                if py_path.exists() or pkg_path.exists():
                    continue
                # A third-party override (e.g. numpy's stub skip) is valid
                # when the module resolves in the environment; the guard
                # still catches typos and entries gone stale.
                root = module_name.removesuffix(".*").split(".")[0]
                if importlib.util.find_spec(root) is not None:
                    continue
                missing.append(module_name)

        assert not missing, f"mypy overrides reference non-existent modules: {missing}"

    def test_tomllib_import_compatible(self):
        """Verify tomllib (or tomli fallback) is importable on this Python."""
        if sys.version_info >= (3, 11):
            assert importlib.util.find_spec("tomllib") is not None
        else:
            # On Python 3.10, tomli must be installed as backport
            assert importlib.util.find_spec("tomli") is not None, (
                "tomli backport is required for Python <3.11"
            )
