"""The embeddable core surface imports without pulling in click or duckdb (#1307).

QGIS plugins vendor these modules as source files. QGIS ships neither click nor
DuckDB, so the vendored set has to *import* cleanly with both absent — the
plugin needs to get far enough to tell the user to install DuckDB. The strong
form (importing with the packages genuinely uninstalled) runs in CI's bare-venv
job (``.github/workflows/tests.yml``); these tests prove the equivalent local
fact in a fresh interpreter where both packages *are* installed: importing the
embeddable surface never puts ``click`` or ``duckdb`` into ``sys.modules``.

Every check runs in a subprocess because the pytest process imported everything
long ago; ``sys.executable -c`` is the only honest fresh interpreter.
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

#: The declared, semver-tracked embeddable surface (#1307). Order matters only
#: for readability. docs/guide/embedding.md names exactly this set; the
#: doc-sync test below keeps the two lists from drifting apart.
EMBEDDABLE_MODULES = (
    "geoparquet_io.core.bbox_structure",
    "geoparquet_io.core.duckdb_metadata",
    "geoparquet_io.core.extract",
    "geoparquet_io.core.format_writers",
    "geoparquet_io.core.geo_metadata",
    "geoparquet_io.core.remote",
)

FORBIDDEN_AT_IMPORT = ("click", "duckdb")


def _run_python(code: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
    )


def _imported_forbidden(imports: str) -> dict[str, bool]:
    """Which forbidden packages land in ``sys.modules`` after *imports* run."""
    code = (
        "import json, sys\n"
        f"{imports}\n"
        f"print(json.dumps({{name: name in sys.modules for name in {FORBIDDEN_AT_IMPORT!r}}}))"
    )
    result = _run_python(code)
    assert result.returncode == 0, f"import failed:\n{result.stderr}"
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_import_geoparquet_io_pulls_neither_click_nor_duckdb():
    """``import geoparquet_io`` is side-effect-light: no CLI, no DuckDB."""
    landed = _imported_forbidden("import geoparquet_io")
    offenders = [name for name, present in landed.items() if present]
    assert not offenders, (
        f"`import geoparquet_io` imported {offenders}; the package root must "
        "stay lazy (PEP 562) so vendored copies import without them (#1307)"
    )


@pytest.mark.parametrize("module", EMBEDDABLE_MODULES)
def test_embeddable_module_import_defers_duckdb_and_click(module):
    """Importing an embeddable module never imports duckdb or click."""
    landed = _imported_forbidden(f"import {module}")
    offenders = [name for name, present in landed.items() if present]
    assert not offenders, (
        f"`import {module}` imported {offenders}; the embeddable surface "
        "defers duckdb to call time and never touches click (#1307)"
    )


def test_from_geoparquet_io_import_cli_still_yields_the_click_group():
    """The lazy root keeps the old surface: ``cli`` is the Click Group."""
    code = (
        "import click\n"
        "from geoparquet_io import cli\n"
        "assert isinstance(cli, click.Group), type(cli)\n"
        "import geoparquet_io\n"
        "assert isinstance(geoparquet_io.cli, click.Group), type(geoparquet_io.cli)\n"
        "print('ok')"
    )
    result = _run_python(code)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().splitlines()[-1] == "ok"


def test_every_public_name_still_resolves_lazily():
    """Each name in ``__all__`` resolves through the lazy ``__getattr__``."""
    code = (
        "import geoparquet_io\n"
        "missing = [n for n in geoparquet_io.__all__ "
        "if getattr(geoparquet_io, n, None) is None]\n"
        "assert not missing, missing\n"
        "assert set(geoparquet_io.__all__) <= set(dir(geoparquet_io))\n"
        "print('ok')"
    )
    result = _run_python(code)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().splitlines()[-1] == "ok"


def test_unknown_attribute_still_raises_attribute_error():
    """PEP 562 laziness must not swallow typos into ImportError or None."""
    code = (
        "import geoparquet_io\n"
        "try:\n"
        "    geoparquet_io.not_a_real_name\n"
        "except AttributeError as e:\n"
        "    assert 'not_a_real_name' in str(e), e\n"
        "    print('ok')\n"
        "else:\n"
        "    raise SystemExit('expected AttributeError')"
    )
    result = _run_python(code)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().splitlines()[-1] == "ok"


def test_docs_page_names_every_embeddable_module():
    """docs/guide/embedding.md and this test declare the same surface."""
    from pathlib import Path

    page = Path(__file__).resolve().parent.parent / "docs" / "guide" / "embedding.md"
    assert page.is_file(), "docs/guide/embedding.md documents the embeddable surface (#1307)"
    text = page.read_text(encoding="utf-8")
    missing = [m for m in EMBEDDABLE_MODULES if m.rsplit(".", 1)[1] not in text]
    assert not missing, f"docs/guide/embedding.md does not name: {missing}"


def test_bare_venv_ci_job_covers_every_embeddable_module():
    """The tests.yml bare-venv job imports exactly this surface."""
    from pathlib import Path

    workflow = (
        Path(__file__).resolve().parent.parent / ".github" / "workflows" / "tests.yml"
    ).read_text(encoding="utf-8")
    missing = [m for m in EMBEDDABLE_MODULES if m not in workflow]
    assert not missing, f"bare-venv CI job does not import: {missing}"
