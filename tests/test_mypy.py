"""Tests for mypy type checking configuration."""

import ast
import importlib
import importlib.util
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

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
        assert mypy_config.get("python_version") == "3.11"
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
        """Python 3.11+ supplies TOML parsing in the standard library."""
        assert importlib.util.find_spec("tomllib") is not None


# A consumer's module, type-checked outside the repo and against the INSTALLED
# package: this is what the py.typed marker promises. `good.py` must come back
# clean, and `bad.py` must come back with the one error it earns -- a run that
# reported nothing at all would pass just as happily if the marker were gone
# and every name resolved to Any.
_CONSUMER_GOOD = '''\
"""Only the published API, fully annotated."""

import geoparquet_io as gpio
from geoparquet_io import Table, WriteOptions


def tidy(src: str, dst: str) -> str:
    table: Table = gpio.read(src)
    rows: int = table.num_rows
    names: list[str] = table.column_names
    table.add_bbox().sort_hilbert().write(dst, WriteOptions(compression="zstd"))
    verdict: str = gpio.check_all(dst).verdict()
    return f"{rows} rows, {len(names)} columns, {verdict}"
'''

_CONSUMER_BAD = """\
import geoparquet_io as gpio

wrong: str = gpio.read("in.parquet").num_rows
"""

# The documented promise (docs/api/python-api.md, "Type Checking"): every path
# argument on the published surface takes a `pathlib.Path` as happily as a
# `str`. Runtime always accepted one; a bare `str` annotation made a type
# checker reject it, so this consumer is the claim's test.
_CONSUMER_PATHS = '''\
"""Every published path argument, handed a pathlib.Path."""

from pathlib import Path

import geoparquet_io as gpio
from geoparquet_io import Table, WriteOptions
from geoparquet_io.api import ops


def run(src: Path, dst: Path, out_dir: Path) -> None:
    table: Table = gpio.read(src)
    table.write(dst, WriteOptions(compression="zstd"))
    gpio.check_all(dst)

    ops.compression_stats(src)
    ops.explain_analyze(src)
    ops.get_row_group_geo_stats(src)
    ops.preview_bbox_filter(src, (0.0, 0.0, 1.0, 1.0))
    ops.create_overviews(src, output_dir=out_dir)
    ops.create_overview_file(src, dst, output_dir=out_dir)
    ops.create_pmtiles(src, dst, temporary_directory=out_dir)
    ops.create_pmtiles_pyramid(src, dst, features_source=src, temporary_directory=out_dir)
    ops.polygonize(src)
    ops.contour(src)
    ops.from_wfs_layers("https://example.com/wfs", ["layer"], out_dir)
    ops.read_bigquery("project.dataset.table", credentials_file=src)

    arrow = table.to_arrow()
    ops.convert_to_geojson(arrow, dst)
    ops.convert_to_geopackage(arrow, dst)
    ops.convert_to_flatgeobuf(arrow, dst)
    ops.convert_to_csv(arrow, dst)
    ops.convert_to_shapefile(arrow, dst)
'''


def test_the_installed_package_ships_usable_types(tmp_path):
    """An outside caller type-checks against gpio: py.typed plus real annotations.

    Run from a directory of its own, with its own mypy config, so nothing of
    the repo's -- not the per-module overrides, not `ignore_missing_imports` --
    is in play: only what a `pip install geoparquet-io` leaves behind.
    """
    (tmp_path / "mypy.ini").write_text("[mypy]\npython_version = 3.11\n")
    (tmp_path / "good.py").write_text(_CONSUMER_GOOD)
    (tmp_path / "bad.py").write_text(_CONSUMER_BAD)

    result = subprocess.run(
        [sys.executable, "-m", "mypy", "--no-incremental", "good.py", "bad.py"],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
        timeout=_SUBPROCESS_TIMEOUT,
    )
    output = result.stdout + result.stderr

    assert "py.typed" not in output, f"the installed package has no py.typed marker:\n{output}"
    reported = [line for line in output.splitlines() if line.startswith("good.py:")]
    assert not reported, f"typed, published usage should check clean:\n{output}"
    assert any("bad.py:" in line and "[assignment]" in line for line in output.splitlines()), (
        f"a wrong type against the public API should be caught:\n{output}"
    )


def test_every_published_path_argument_accepts_pathlib_path(tmp_path):
    """A consumer handing `pathlib.Path` to the published API checks clean.

    Same outside-the-repo setup as the usability test: the installed package
    and a bare mypy config, so only what ships is in play.
    """
    (tmp_path / "mypy.ini").write_text("[mypy]\npython_version = 3.11\n")
    (tmp_path / "paths.py").write_text(_CONSUMER_PATHS)

    result = subprocess.run(
        [sys.executable, "-m", "mypy", "--no-incremental", "paths.py"],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
        timeout=_SUBPROCESS_TIMEOUT,
    )
    output = result.stdout + result.stderr

    reported = [line for line in output.splitlines() if line.startswith("paths.py:")]
    assert not reported, f"a Path should be as good as a str on every path argument:\n{output}"


def test_the_lazy_root_declares_every_export_to_a_type_checker():
    """``geoparquet_io/__init__.py``'s TYPE_CHECKING block covers ``_LAZY_EXPORTS``.

    PEP 562 laziness hides the public names from a type checker, which resolves
    whatever ``__getattr__`` serves to ``Any``. The TYPE_CHECKING imports are
    what py.typed actually delivers, so a name added to ``_LAZY_EXPORTS`` and
    not to that block would ship untyped.
    """
    import geoparquet_io

    source = (_REPO_ROOT / "geoparquet_io" / "__init__.py").read_text()
    tree = ast.parse(source)
    declared = {
        alias.asname or alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.If) and ast.unparse(node.test) == "TYPE_CHECKING"
        for stmt in node.body
        if isinstance(stmt, ast.ImportFrom)
        for alias in stmt.names
    }

    lazy = set(geoparquet_io._LAZY_EXPORTS)
    assert not lazy - declared, (
        "these lazy exports are invisible to a type checker; add them to the "
        f"TYPE_CHECKING block in geoparquet_io/__init__.py: {sorted(lazy - declared)}"
    )
    assert not declared - lazy, (
        "the TYPE_CHECKING block declares names the lazy root does not serve: "
        f"{sorted(declared - lazy)}"
    )
