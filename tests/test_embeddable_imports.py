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


def test_the_cli_export_survives_a_submodule_import_first():
    """``cli`` is the Group in EVERY import order, not just the fresh one.

    Importing any ``geoparquet_io.cli.*`` module makes the import machinery
    bind the subpackage as the root's ``cli`` attribute, after which a plain
    PEP 562 ``__getattr__`` never fires for the name again. The eager root
    always ended with the Group winning; the lazy root must too.
    """
    code = (
        "import click\n"
        "import geoparquet_io.cli.main\n"
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
    """docs/guide/embedding.md and this test declare the same surface.

    Set equality against the table's ``geoparquet_io/core/<name>.py`` cells,
    both directions: a module guarded here but missing from the docs fails,
    and so does a module the docs promise but nothing guards (prose mentions
    like "extraction" cannot satisfy either side).
    """
    import re
    from pathlib import Path

    page = Path(__file__).resolve().parent.parent / "docs" / "guide" / "embedding.md"
    assert page.is_file(), "docs/guide/embedding.md documents the embeddable surface (#1307)"
    text = page.read_text(encoding="utf-8")
    documented = {
        f"geoparquet_io.core.{name}" for name in re.findall(r"`geoparquet_io/core/(\w+)\.py`", text)
    }
    assert documented == set(EMBEDDABLE_MODULES), (
        f"docs table vs EMBEDDABLE_MODULES: docs-only {documented - set(EMBEDDABLE_MODULES)}, "
        f"guard-only {set(EMBEDDABLE_MODULES) - documented}"
    )


def test_bare_venv_ci_job_covers_every_embeddable_module():
    """The tests.yml bare-venv job imports exactly this surface."""
    from pathlib import Path

    workflow = (
        Path(__file__).resolve().parent.parent / ".github" / "workflows" / "tests.yml"
    ).read_text(encoding="utf-8")
    missing = [m for m in EMBEDDABLE_MODULES if m not in workflow]
    assert not missing, f"bare-venv CI job does not import: {missing}"


class TestDuckdbLessDegradation:
    """Helpers importable from the embeddable surface degrade without duckdb.

    ``monkeypatch.setitem(sys.modules, "duckdb", None)`` makes ``import
    duckdb`` raise ImportError, the same signal a vendoring host without the
    package produces.
    """

    def test_is_interrupt_still_sees_a_keyboard_interrupt(self, monkeypatch):
        from geoparquet_io.core.duckdb_utils import is_interrupt

        monkeypatch.setitem(sys.modules, "duckdb", None)
        assert is_interrupt(KeyboardInterrupt())

    def test_is_interrupt_answers_false_not_modulenotfounderror(self, monkeypatch):
        from geoparquet_io.core.duckdb_utils import is_interrupt

        monkeypatch.setitem(sys.modules, "duckdb", None)
        assert not is_interrupt(ValueError("boom"))

    def test_where_validation_falls_back_to_the_regex_blocklist(self, monkeypatch):
        from geoparquet_io.core.duckdb_utils import validate_where_clause
        from geoparquet_io.core.exceptions import ValidationError

        monkeypatch.setitem(sys.modules, "duckdb", None)
        with pytest.raises(ValidationError, match="dangerous"):
            validate_where_clause("1=1; DROP TABLE t")

    def test_where_validation_accepts_a_plain_clause(self, monkeypatch):
        """The regex fallback is conservative (it would flag 'Grant County');
        a clause with no blocklisted word must still pass without duckdb."""
        from geoparquet_io.core.duckdb_utils import validate_where_clause

        monkeypatch.setitem(sys.modules, "duckdb", None)
        validate_where_clause("population > 1000 AND name = 'Oslo'")


class TestLazyRootInProcess:
    """In-process twins of the subprocess assertions, for the coverage gate."""

    def test_cli_property_wins_over_the_machinery_binding(self):
        import click

        import geoparquet_io
        import geoparquet_io.cli.main  # noqa: F401  (binds the subpackage attribute)

        assert isinstance(geoparquet_io.cli, click.Group)
        # The import machinery assigns the subpackage through the setter; the
        # property keeps answering with the Group.
        geoparquet_io.cli = "ignored"
        assert isinstance(geoparquet_io.cli, click.Group)

    def test_api_and_exports_resolve_through_the_hook(self):
        import geoparquet_io

        for name in ("api", "GeoParquetError"):
            if name in vars(geoparquet_io):
                delattr(geoparquet_io, name)
            assert getattr(geoparquet_io, name) is not None

    def test_an_unknown_attribute_raises(self):
        import geoparquet_io

        with pytest.raises(AttributeError, match="no attribute"):
            _ = geoparquet_io.not_a_real_name

    def test_input_file_errors_tuple_is_cached(self):
        from geoparquet_io.core import duckdb_utils

        vars(duckdb_utils).pop("INPUT_FILE_DUCKDB_ERRORS", None)
        first = duckdb_utils.INPUT_FILE_DUCKDB_ERRORS
        assert duckdb_utils.INPUT_FILE_DUCKDB_ERRORS is first
        assert duckdb_utils.is_input_file_duckdb_error(first[0]("boom"))
