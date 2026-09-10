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
* :class:`TestManualQuoteRuleHasNoPathExemptions` reads the config and pins the
  arm's pipeline stage by stage, so any added filter fails whatever spelling it
  uses -- and the one path it may name is ``core/duckdb_utils.py``, which
  implements the two helpers and so cannot use them.

The stage list is deliberately a whitelist rather than a list of exclusion
spellings to recognise. A blacklist is only as good as its list, and a
whole-module exemption has many shapes: ``grep -v`` without the ``^``, a
double-quoted path, an ``awk`` regex match, a ``--exclude=`` on the search, or
the path held in a shell variable. Pinning the stages catches all of them,
including shapes nobody has thought of.

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


def _hook_entry() -> dict:
    """The duckdb-antipatterns hook's own YAML entry."""
    config = yaml.safe_load((REPO_ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    for repo in config["repos"]:
        for hook in repo.get("hooks", []):
            if hook["id"] == "duckdb-antipatterns":
                return hook
    raise AssertionError("duckdb-antipatterns hook not found in .pre-commit-config.yaml")


def _hook_script() -> str:
    """The duckdb-antipatterns script from .pre-commit-config.yaml, exactly as
    pre-commit invokes it."""
    return str(_hook_entry()["args"][-1])


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

    The spelling tests above catch an exclusion for *this* module, whatever
    shape it takes, because ``extract_bigquery.py`` is their fixture. This one
    has to catch an exclusion for **any** module, and it does that by pinning
    the arm's pipeline rather than by recognising exclusion spellings.

    The difference matters. A blacklist of known spellings is only ever as good
    as the list: ``grep -v '...'`` without the ``^``, a double-quoted path, an
    ``awk`` regex match instead of ``!=``, a ``--exclude=`` on the ``grep -r``,
    or the path in a shell variable are all whole-module exemptions, and none of
    them looks like the two shapes the arm happens to use today. Asserting the
    stage list instead inverts that: any new filter fails, in any spelling,
    including ones nobody has thought of.

    Scoping to the arm matters too. The script has six arms and two of the
    others legitimately exempt ``duckdb_utils.py`` as well, so a check that
    scanned the whole script would stay green on their matches while this arm
    was reworded out from under it.
    """

    #: Every stage of the manual-quote pipeline, in order, normalised for
    #: whitespace. `grep -rnE` finds the banned spellings; the exemption for the
    #: module that *implements* the helpers; the per-line escape hatch; a filter
    #: dropping commented-out lines; and `grep .` to set the exit status.
    _EXPECTED_STAGES = (
        "grep -rnE",
        "grep -v '^geoparquet_io/core/duckdb_utils.py:'",
        "grep -v 'allow-manual-quote'",
        "awk -F: '$3 !~ /^[[:space:]]*#/'",
        "grep .",
    )

    @staticmethod
    def _manual_quote_stages() -> list[str]:
        """The pipeline stages of the manual-quote arm, and only that arm."""
        script = _hook_script()
        marker = "Do not hand-roll SQL identifier quoting."
        assert marker in script, (
            "The manual-quote arm's error message has changed, so this test can "
            "no longer find the arm it is meant to pin. Update the marker rather "
            "than deleting the test."
        )
        # The arm is the `if <pipeline>; then` immediately above its message.
        head = script[: script.index(marker)]
        condition = head[head.rindex("if ") :]
        condition = condition[: condition.index("; then")]
        condition = condition[len("if ") :]
        # Undo the shell's backslash-newline line continuations.
        condition = condition.replace("\\\n", " ")
        # Split on pipeline pipes only. The grep pattern itself contains `|`
        # alternations, but those are never surrounded by whitespace.
        return [" ".join(stage.split()) for stage in re.split(r"\s\|\s", condition)]

    def test_the_manual_quote_arm_has_exactly_the_expected_stages(self):
        stages = self._manual_quote_stages()

        assert len(stages) == len(self._EXPECTED_STAGES), (
            "The manual-quote arm gained or lost a pipeline stage. A new stage "
            "is how a whole-module exemption gets in -- that is what #939 did to "
            f"core/{FORMERLY_EXEMPT_MODULE}, unnoticed until #946. Silence one "
            "line with a trailing '# allow-manual-quote' instead.\n"
            f"Expected {len(self._EXPECTED_STAGES)} stages, found {len(stages)}:\n"
            + "\n".join(f"  {stage}" for stage in stages)
        )
        for found, expected in zip(stages, self._EXPECTED_STAGES, strict=True):
            assert found.startswith(expected), (
                f"Manual-quote pipeline stage changed.\n  expected: {expected}\n  found:    {found}"
            )

    def test_the_only_path_the_arm_exempts_is_the_factory_module(self):
        """Read as a path question rather than a stage-list one, for the message."""
        # Stage 0 is the search itself, which names `geoparquet_io/` as its
        # root; only the filter stages after it can exempt anything.
        exempted = [
            stage
            for stage in self._manual_quote_stages()[1:]
            if ".py" in stage and FACTORY_MODULE not in stage
        ]

        assert not exempted, (
            "The manual-quote arm names a module other than "
            f"{FACTORY_MODULE}, which is the only path that may ever be exempt "
            "(it implements the helpers the rule points at). A file-scoped "
            "exclusion switches the whole rule off over that module.\n"
            + "\n".join(f"  {stage}" for stage in exempted)
        )

    def test_the_hook_is_not_narrowed_by_a_files_or_exclude_key(self):
        """`files:`/`exclude:` in the YAML would silence the hook above the script."""
        hook = _hook_entry()

        narrowing = {key: hook[key] for key in ("files", "exclude") if key in hook}

        assert not narrowing, (
            "The duckdb-antipatterns hook declares a files/exclude key. The "
            "script scans geoparquet_io/ itself (pass_filenames: false), so "
            "these do not narrow what is scanned -- but a 'files' pattern that "
            "matches nothing turns the whole hook into a skip. Leave both unset "
            f"so the hook always runs. Found: {narrowing}"
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
