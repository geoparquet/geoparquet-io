"""Self-tests for the ``duckdb-antipatterns`` hook's hand-rolled-quoting arm.

The rule bans ``f'"{col}"'`` and ``col.replace('"', '""')`` in favour of
:func:`duckdb_utils.quote_identifier` / :func:`duckdb_utils._escape_sql_string`,
because a column name can arrive from a file's own ``geo.primary_column`` or
from ``--column`` / ``--geography-column`` -- an injection surface, not a nit.

Why these tests exist (#946). When #939 unanchored the ``\\"{`` arm, the newly
visible sites in ``core/extract_bigquery.py`` were silenced with a **file-scoped**
exclusion in ``.pre-commit-config.yaml``::

    | grep -v '^geoparquet_io/core/extract_bigquery.py:' \\

That switched the whole rule off over an entire module, not just the three
BigQuery-dialect lines the rationale covered, and nothing in the suite noticed.
#944 fixed the sites and dropped the exclusion; these tests keep it dropped:

* the spelling tests run their hostile source **from that exact path**, so any
  path-scoped exclusion for it turns straight back into a failure, and
* :class:`TestManualQuoteRuleHasNoPathExemptions` reads the config and asserts
  that ``core/duckdb_utils.py`` -- which implements the two helpers and so
  cannot use them -- is the only path the hook exempts at all.

The sibling arm that bans bare ``duckdb.connect(`` is self-tested in
``tests/test_duckdb_connection_factory_bypass.py``.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).parent.parent

# The one module the hook is allowed to exempt: quote_identifier() and
# _escape_sql_string() live there, so their own implementations necessarily
# spell out the quoting the rule forbids everywhere else.
FACTORY_MODULE = "geoparquet_io/core/duckdb_utils.py"

# The module #939 exempted wholesale. Every rejection test below plants its
# source here, so re-adding an exclusion for it fails these tests.
FORMERLY_EXEMPT_MODULE = "extract_bigquery.py"


def _bash_can_run_scripts() -> bool:
    """Whether ``bash`` on this platform can actually execute a script.

    ``shutil.which("bash")`` is not enough on Windows runners: there ``bash``
    resolves to WSL's ``bash.exe``, which exits non-zero with an "install a
    distribution" notice when no WSL distro is present -- which would pass
    every rejection test below for entirely the wrong reason.
    """
    try:
        probe = subprocess.run(["bash", "-c", "exit 0"], capture_output=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return False
    return probe.returncode == 0


_NEEDS_BASH = pytest.mark.skipif(
    not _bash_can_run_scripts(),
    reason="pre-commit hook scripts are POSIX shell; no usable bash on this platform",
)


def _hook_script() -> str:
    """The duckdb-antipatterns script from .pre-commit-config.yaml, exactly as
    pre-commit invokes it."""
    config = yaml.safe_load((REPO_ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    for repo in config["repos"]:
        for hook in repo.get("hooks", []):
            if hook["id"] == "duckdb-antipatterns":
                return str(hook["args"][-1])
    raise AssertionError("duckdb-antipatterns hook not found in .pre-commit-config.yaml")


# Each spelling the rule has to catch. The escaped-quote one is #936's shape:
# it sits mid-f-string, which is why #939 had to unanchor the pattern.
MANUAL_QUOTE_SPELLINGS = {
    "f_string_quotes": "def build(col):\n    return f'\"{col}\"'\n",
    "escaped_mid_string": (
        'def build(col, wkt):\n    return f"ST_Intersects(\\"{col}\\", \'{wkt}\')"\n'
    ),
    "replace_doubling": "def build(col):\n    return '\"' + col.replace('\"', '\"\"') + '\"'\n",
}


@_NEEDS_BASH
class TestHookRejectsManualQuoting:
    """The rule must fire on every spelling, in every module -- including the
    one #939 exempted by path."""

    def _run_hook(self, cwd: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", "-c", _hook_script()],
            cwd=cwd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

    def _workspace(self, tmp_path: Path, files: dict[str, str]) -> Path:
        """An isolated ``geoparquet_io/`` tree, so these tests never mutate the
        real repo tree -- the suite runs under pytest-xdist."""
        core_dir = tmp_path / "geoparquet_io" / "core"
        core_dir.mkdir(parents=True)
        for name, content in files.items():
            (core_dir / name).write_text(content, encoding="utf-8")
        return tmp_path

    @pytest.mark.parametrize("spelling", sorted(MANUAL_QUOTE_SPELLINGS))
    def test_rejected_in_the_formerly_exempt_module(self, tmp_path, spelling):
        """#946: hand-rolled quoting in core/extract_bigquery.py must fail the
        hook. Under #939's file-scoped ``grep -v`` it passed."""
        workspace = self._workspace(
            tmp_path, {FORMERLY_EXEMPT_MODULE: MANUAL_QUOTE_SPELLINGS[spelling]}
        )

        result = self._run_hook(workspace)

        assert result.returncode != 0, result.stdout + result.stderr
        output = result.stdout + result.stderr
        assert "Do not hand-roll SQL identifier quoting" in output
        assert FORMERLY_EXEMPT_MODULE in output

    @pytest.mark.parametrize("spelling", sorted(MANUAL_QUOTE_SPELLINGS))
    def test_rejected_in_an_ordinary_module(self, tmp_path, spelling):
        """Control for the test above: the same source in a module that was
        never exempt fails too, so a pass there would be a real regression and
        not an artefact of the fixture."""
        workspace = self._workspace(tmp_path, {"some_module.py": MANUAL_QUOTE_SPELLINGS[spelling]})

        result = self._run_hook(workspace)

        assert result.returncode != 0, result.stdout + result.stderr

    def test_a_clean_module_passes(self, tmp_path):
        """The rejections above must come from the manual quoting, not from
        anything else in the fixture tree."""
        workspace = self._workspace(
            tmp_path,
            {FORMERLY_EXEMPT_MODULE: "def build(col):\n    return quote_identifier(col)\n"},
        )

        result = self._run_hook(workspace)

        assert result.returncode == 0, result.stdout + result.stderr

    def test_the_factory_module_stays_exempt(self, tmp_path):
        """quote_identifier()'s own body is the quoting, so core/duckdb_utils.py
        cannot satisfy the rule and is exempt by design."""
        workspace = self._workspace(
            tmp_path, {"duckdb_utils.py": MANUAL_QUOTE_SPELLINGS["f_string_quotes"]}
        )

        result = self._run_hook(workspace)

        assert result.returncode == 0, result.stdout + result.stderr

    def test_a_nested_duckdb_utils_is_not_exempt(self, tmp_path):
        """The exemption names one path, not any file called duckdb_utils.py."""
        nested = tmp_path / "geoparquet_io" / "core" / "partition"
        workspace = self._workspace(tmp_path, {})
        nested.mkdir(parents=True)
        (nested / "duckdb_utils.py").write_text(
            MANUAL_QUOTE_SPELLINGS["f_string_quotes"], encoding="utf-8"
        )

        result = self._run_hook(workspace)

        assert result.returncode != 0, result.stdout + result.stderr

    def test_the_allow_manual_quote_escape_hatch_is_honoured(self, tmp_path):
        """A deliberate exception stays possible -- per line, and visible in the
        diff that introduces it, which is what a file-scoped exclusion is not."""
        workspace = self._workspace(
            tmp_path,
            {
                FORMERLY_EXEMPT_MODULE: (
                    "def build(col):\n    return f'\"{col}\"'  # allow-manual-quote (#932)\n"
                )
            },
        )

        result = self._run_hook(workspace)

        assert result.returncode == 0, result.stdout + result.stderr


class TestManualQuoteRuleHasNoPathExemptions:
    """#946's structural half: a whole-file exclusion must not come back.

    The spelling tests above catch an exclusion for *this* module; this one
    catches an exclusion for any module, in either shape the hook script uses
    (``grep -v '^<path>:'`` and ``awk -F: '$1 != "<path>"'``).
    """

    # grep -v '^geoparquet_io/...:'  and  awk -F: '$1 != "geoparquet_io/..."'
    _EXEMPT_PATH_PATTERNS = (
        re.compile(r"""grep\s+-v\s+'\^(geoparquet_io/[^:']+)"""),
        re.compile(r"""\$1\s*!=\s*"(geoparquet_io/[^"]+)"""),
    )

    def test_duckdb_utils_is_the_only_exempt_path(self):
        script = _hook_script()

        exempted = {
            match.group(1).rstrip(":")
            for pattern in self._EXEMPT_PATH_PATTERNS
            for match in pattern.finditer(script)
        }

        assert exempted == {FACTORY_MODULE}, (
            "duckdb-antipatterns exempts a module by path. Only "
            f"{FACTORY_MODULE} may be exempt (it implements the helpers the "
            "rule points at). A file-scoped exclusion switches the whole rule "
            "off over that module -- #939 did exactly that to "
            f"core/{FORMERLY_EXEMPT_MODULE} and it went unnoticed until #946. "
            "Use a trailing '# allow-manual-quote' on the one line instead. "
            f"Found: {sorted(exempted)}"
        )

    def test_the_real_extract_bigquery_module_needs_no_exemption(self):
        """The reason the exclusion could go: the module quotes properly now --
        quote_identifier() for its DuckDB-dialect sites and
        _quote_bigquery_identifier() for its GoogleSQL ones, where ``"..."``
        is a string literal rather than an identifier."""
        source = (REPO_ROOT / "geoparquet_io" / "core" / FORMERLY_EXEMPT_MODULE).read_text(
            encoding="utf-8"
        )

        assert "quote_identifier(geom_col)" in source
        assert "_quote_bigquery_identifier(geom_col)" in source
        assert "allow-manual-quote" not in source


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
