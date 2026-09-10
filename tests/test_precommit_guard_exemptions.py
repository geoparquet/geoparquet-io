"""Self-tests for the ``no-click-echo`` and ``forbid-bespoke-schema-reconciliation``
guards, whose exemptions used to be unanchored (#970).

Both filtered their own output with a bare substring match::

    grep -rn "click\\.echo" geoparquet_io/core/ --include="*.py" | grep -v "logging_config.py"
    grep -rnE 'def _(align|unify|reconcile)[a-zA-Z_]*schema' ... | grep -v "common.py"

``grep -v`` sees the whole ``path:line:text`` line, not just the path field, so
each exempted strictly more than the one module it named:

1. **A comment silenced the guard.** ``def _align_schema(a, b):  # mirrors
   common.py`` was not flagged, and neither was ``click.echo("hi")  # migrate to
   logging_config.py later``. Anyone could switch a guard off, in the same line
   the guard exists to catch, by naming the exempt module in a comment.
2. **``common.py`` matched four modules.** As a substring of the path it also
   covered ``core/partition/common.py``, ``core/process/aggregate/common.py``
   and ``core/process/aggregate/grid_common.py`` -- three real modules silently
   outside the guard.

Both now use the anchored form #964 settled on for the ``duckdb-antipatterns``
manual-quote arm, ``grep -v '^<path>:'``, which pins the match to the path field
and so fixes both consequences in one change.

The structural tests pin each guard's pipeline stage by stage rather than
recognising known bad spellings, for the reason spelled out in
``tests/test_duckdb_antipatterns_hook.py``: a blacklist is only as good as its
list, and a whole-module exemption has many shapes (``grep -v`` without the
``^``, a double-quoted path, an ``awk`` regex match, an ``--exclude=`` on the
search, the path in a shell variable). Pinning the stages fails all of them.

Neither guard has any match in the tree today -- anchoring newly exempted
nothing and newly flagged nothing -- so these tests are the only thing standing
between the guards and a hole nobody would notice. That is what happened to
``duckdb-antipatterns`` in #946.
"""

from __future__ import annotations

import re

import pytest

from tests._precommit_hook import (
    REPO_ROOT,
    core_tree,
    hook_entry,
    hook_script,
    needs_bash,
    run_hook_script,
)

CLICK_ECHO_HOOK = "no-click-echo"
SCHEMA_HOOK = "forbid-bespoke-schema-reconciliation"

#: The one module each guard may exempt, as a full path from the repo root.
CLICK_ECHO_EXEMPT = "geoparquet_io/core/logging_config.py"
SCHEMA_EXEMPT = "geoparquet_io/core/common.py"


class _GuardCase:
    """One guard, plus source that should and should not trip it."""

    def __init__(self, hook_id, exempt_path, violation, comment_bypass, clean):
        self.hook_id = hook_id
        self.exempt_path = exempt_path
        #: Source the guard must flag.
        self.violation = violation
        #: The same violation with the exempt module named in a trailing
        #: comment -- reproduction 1, which used to pass.
        self.comment_bypass = comment_bypass
        #: Source the guard must not flag, so a failure means the violation.
        self.clean = clean

    @property
    def basename(self) -> str:
        return self.exempt_path.rsplit("/", 1)[-1]

    def __repr__(self) -> str:  # pragma: no cover - pytest ids only
        return self.hook_id


CLICK_ECHO = _GuardCase(
    hook_id=CLICK_ECHO_HOOK,
    exempt_path=CLICK_ECHO_EXEMPT,
    violation='import click\n\nclick.echo("hello")\n',
    comment_bypass='import click\n\nclick.echo("hello")  # port to logging_config.py later\n',
    clean="from geoparquet_io.core.logging_config import info\n\ninfo('hello')\n",
)

SCHEMA = _GuardCase(
    hook_id=SCHEMA_HOOK,
    exempt_path=SCHEMA_EXEMPT,
    violation="def _align_schema(a, b):\n    return a\n",
    comment_bypass="def _align_schema(a, b):  # mirrors common.py\n    return a\n",
    clean="from geoparquet_io.core.common import _compute_unified_schema\n",
)

GUARDS = [CLICK_ECHO, SCHEMA]


def _run(guard: _GuardCase, tmp_path, files):
    return run_hook_script(hook_script(guard.hook_id), core_tree(tmp_path, files))


@needs_bash
@pytest.mark.parametrize("guard", GUARDS, ids=lambda g: g.hook_id)
class TestGuardsFlagWhatTheyClaimTo:
    def test_a_violation_in_an_ordinary_module_is_flagged(self, guard, tmp_path):
        """The control: without this, every test below could pass because the
        guard never fires at all."""
        result = _run(guard, tmp_path, {"some_module.py": guard.violation})

        assert result.returncode != 0, result.stdout + result.stderr

    def test_a_clean_module_passes(self, guard, tmp_path):
        """And the other control: the rejections are the violation, not the
        fixture tree."""
        result = _run(guard, tmp_path, {"some_module.py": guard.clean})

        assert result.returncode == 0, result.stdout + result.stderr

    def test_the_exempt_module_is_still_exempt(self, guard, tmp_path):
        """Anchoring narrows the exemption; it does not remove it. Each guard
        points at the module that implements the thing it wants used, and that
        module necessarily spells out what the rule forbids everywhere else."""
        result = _run(guard, tmp_path, {guard.basename: guard.violation})

        assert result.returncode == 0, result.stdout + result.stderr

    def test_a_comment_naming_the_exempt_module_does_not_silence_it(self, guard, tmp_path):
        """#970 reproduction 1. Under the unanchored ``grep -v`` this passed:
        the filter matched the comment text on the ``path:line:text`` line."""
        result = _run(guard, tmp_path, {"some_module.py": guard.comment_bypass})

        assert result.returncode != 0, (
            f"{guard.hook_id} was silenced by a comment naming {guard.basename}. "
            "The exemption must be anchored to the path field "
            f"(grep -v '^{guard.exempt_path}:'), not matched anywhere in the line."
        )

    def test_a_nested_module_of_the_same_name_is_not_exempt(self, guard, tmp_path):
        """#970 reproduction 2. The exemption names one path, not every file
        with that basename: ``grep -v "common.py"`` also covered
        core/partition/common.py, core/process/aggregate/common.py and
        core/process/aggregate/grid_common.py."""
        result = _run(guard, tmp_path, {f"partition/{guard.basename}": guard.violation})

        assert result.returncode != 0, (
            f"{guard.hook_id} exempts any file named {guard.basename}, not just "
            f"{guard.exempt_path}."
        )

    def test_a_module_whose_name_merely_ends_with_the_exempt_one_is_not_exempt(
        self, guard, tmp_path
    ):
        """The ``grid_common.py`` shape: a substring match on the path catches
        neighbours nobody meant to exempt."""
        result = _run(guard, tmp_path, {f"grid_{guard.basename}": guard.violation})

        assert result.returncode != 0, (
            f"{guard.hook_id} exempts grid_{guard.basename}, which is not {guard.exempt_path}."
        )


class TestGuardExemptionsStayAnchored:
    """The structural half: pin each guard's pipeline so a reworded or added
    filter fails whatever spelling it uses."""

    #: Every stage of each guard's pipeline, in order. Stage 0 is the search;
    #: everything after it can only ever narrow what is reported.
    EXPECTED_STAGES = {
        CLICK_ECHO_HOOK: (
            'grep -rn "click\\.echo" geoparquet_io/core/ --include="*.py"',
            f"grep -v '^{CLICK_ECHO_EXEMPT}:'",
        ),
        SCHEMA_HOOK: (
            "grep -rnE 'def _(align|unify|reconcile)[a-zA-Z_]*schema' "
            'geoparquet_io/core/ --include="*.py"',
            f"grep -v '^{SCHEMA_EXEMPT}:'",
        ),
    }

    EXEMPT_PATHS = {CLICK_ECHO_HOOK: CLICK_ECHO_EXEMPT, SCHEMA_HOOK: SCHEMA_EXEMPT}

    #: ``if`` where a shell statement can begin. A bare ``"if "`` substring
    #: count -- which is what this used to do -- also matches ``elif``, a
    #: comment mentioning "if ", and a grep pattern that happens to contain it:
    #: spelling-sensitivity inside a module whose whole thesis is not to depend
    #: on spellings. It failed loudly rather than silently, but that is a reason
    #: to fix it, not to keep it.
    _IF_STATEMENT = re.compile(r"(?m)^[ \t]*if[ \t]")

    @classmethod
    def _pipeline(cls, hook_id: str) -> list[str]:
        """The stages of the guard's single ``if <pipeline>; then`` condition."""
        script = hook_script(hook_id)
        statements = list(cls._IF_STATEMENT.finditer(script))
        assert len(statements) == 1, (
            f"The {hook_id} script grew a second `if`, so this test can no "
            "longer identify the pipeline it pins. Update the test rather than "
            f"deleting it. Found {len(statements)}."
        )
        start = statements[0].end()
        condition = script[start : script.index("; then", start)]
        condition = condition.replace("\\\n", " ")
        # Split on pipeline pipes only: the grep patterns contain `|`
        # alternations, but those are never surrounded by whitespace.
        return [" ".join(stage.split()) for stage in condition.split(" | ")]

    @pytest.mark.parametrize("hook_id", sorted(EXPECTED_STAGES))
    def test_the_pipeline_has_exactly_the_expected_stages(self, hook_id):
        stages = self._pipeline(hook_id)
        expected = self.EXPECTED_STAGES[hook_id]

        assert len(stages) == len(expected), (
            f"The {hook_id} guard gained or lost a pipeline stage. A new filter "
            "stage is how a whole-module exemption gets in -- that is what the "
            "unanchored `grep -v` did to three modules, unnoticed until #970.\n"
            f"Expected {len(expected)} stages, found {len(stages)}:\n"
            + "\n".join(f"  {stage}" for stage in stages)
        )
        for found, want in zip(stages, expected, strict=True):
            assert found == want, (
                f"{hook_id} stage changed.\n  expected: {want}\n  found:    {found}"
            )

    @pytest.mark.parametrize("hook_id", sorted(EXPECTED_STAGES))
    def test_every_filter_that_names_a_module_anchors_it_to_the_path_field(self, hook_id):
        """Stated as the property #970 is about, so a failure names it.

        An exemption spelled ``grep -v "common.py"`` matches the whole
        ``path:line:text`` line: a comment mentioning the module switches the
        guard off, and every sibling path containing that substring is exempt
        too. ``'^<path>:'`` matches the path field alone.
        """
        exempt = self.EXEMPT_PATHS[hook_id]
        unanchored = [
            stage
            for stage in self._pipeline(hook_id)[1:]
            if ".py" in stage and f"'^{exempt}:'" not in stage
        ]

        assert not unanchored, (
            f"The {hook_id} guard names a module in a filter that is not "
            f"anchored to the path field. Use grep -v '^{exempt}:' -- and only "
            f"for {exempt}, which is the one module the guard may exempt.\n"
            + "\n".join(f"  {stage}" for stage in unanchored)
        )

    @pytest.mark.parametrize("hook_id", sorted(EXPECTED_STAGES))
    def test_the_hook_is_not_narrowed_by_a_files_or_exclude_key(self, hook_id):
        """A ``files:``/``exclude:`` key would silence the guard above its
        script. Both scan ``geoparquet_io/core/`` themselves
        (``pass_filenames: false``), so a ``files`` pattern that matches nothing
        turns the whole hook into a skip rather than narrowing it."""
        hook = hook_entry(hook_id)

        narrowing = {key: hook[key] for key in ("files", "exclude") if key in hook}

        assert not narrowing, f"{hook_id} declares a files/exclude key: {narrowing}"


class TestTheAnchoredGuardsStillHaveNothingToReport:
    """Why anchoring was safe to land on its own.

    #970 asked whether the three newly un-exempted modules contain matches. They
    do not, and neither does anything else in ``core/``: both guards report zero
    hits before and after. The only occurrences of ``click.echo`` anywhere in
    ``core/`` are prose in ``logging_config.py``'s own docstrings, which is why
    that module keeps its (now path-scoped) exemption.
    """

    @needs_bash
    @pytest.mark.parametrize("guard", GUARDS, ids=lambda g: g.hook_id)
    def test_the_real_tree_passes(self, guard):
        result = run_hook_script(hook_script(guard.hook_id), REPO_ROOT)

        assert result.returncode == 0, result.stdout + result.stderr


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
