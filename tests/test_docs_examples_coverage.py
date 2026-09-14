"""Prove no skipped fence is hiding commands that would pass.

The cheap meta-tests check that every block is *reachable*. This one checks
something they cannot: that opting a fence out is not quietly throwing away
coverage. A fence that mixes four working commands with one that needs a column
the sample data lacks reads like a single justified skip, and costs four
commands' worth of testing — the reviewer's measurement found 18 such fences
holding 39 working commands.

So: run each statement of every skipped, locally-runnable bash fence in its own
seeded directory. If one exits 0, the fence should have been split (or marked
``menu``), not skipped wholesale.

This executes real commands, so it lives in the slow lane. The cheap structural
guards stay in ``test_docs_examples_meta.py`` where they gate every PR.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from tests.docs_examples.collector import BASH, split_statements
from tests.docs_examples.parser import iter_fences
from tests.docs_examples.seeder import seed_workdir

GUIDE_DIR = Path(__file__).resolve().parent.parent / "docs" / "guide"
DOCS_ROOT = GUIDE_DIR.parent
GUIDE_PAGES = sorted(GUIDE_DIR.glob("*.md"))

#: Fences this check cannot judge locally: they need the network, credentials, a
#: plugin, or a tool that may not be installed. Their commands cannot be run
#: here, so "would it pass?" is unanswerable and the skip stands on its own.
#: Clipboard tools (pbcopy) and installer commands (uv tool, pipx, pip, sudo,
#: brew, apt) are unjudgeable too — and the installers MUST stay here: this
#: audit executes candidate statements for real, so judging a skipped
#: ``uv tool uninstall`` fence would uninstall the developer's global gpio.
UNJUDGEABLE = re.compile(
    r"s3://|gs://|az://|r2://|https?://|bigquery|carto|arcgis|wfs|source\.coop"
    r"|docker|xclip|tippecanoe|fiboa|myplugin|pyproject|Set-Clipboard|uv (pip|add|init)"
    r"|pbcopy|uv tool|pipx|pip (install|uninstall)|sudo|brew|apt(-get)? ",
    re.I,
)

PER_COMMAND_TIMEOUT = 120


def statement_groups(source: str) -> list[list[str]]:
    """Split a fence into statements, the way the ``menu`` directive would.

    Delegates to the collector's :func:`split_statements` so this audit and the
    ``menu`` directive cannot disagree about what "one command" is. Comment-only
    groups are already dropped there: a trailing ``# Error: ...`` is prose about
    the command above it, and bash would exit 0 on it, which would look exactly
    like a passing command.
    """
    return [statement.split("\n") for statement in split_statements(source)]


def _env() -> dict[str, str]:
    env = dict(os.environ)
    env["PATH"] = str(Path(sys.executable).parent) + os.pathsep + env.get("PATH", "")
    env["NO_COLOR"] = "1"
    env["COLUMNS"] = "100"
    return env


def _passes(statement: str, setups: tuple[str, ...]) -> bool:
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        seed_workdir(work)
        try:
            for command in setups:
                subprocess.run(
                    [BASH, "-c", f"set -euo pipefail\n{command}"],
                    cwd=work,
                    env=_env(),
                    capture_output=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=PER_COMMAND_TIMEOUT,
                    check=False,
                )
            script = work / "statement.sh"
            script.write_text(f"set -euo pipefail\n{statement}\n", encoding="utf-8")
            done = subprocess.run(
                [BASH, str(script)],
                cwd=work,
                env=_env(),
                capture_output=True,
                encoding="utf-8",
                errors="replace",
                timeout=PER_COMMAND_TIMEOUT,
                check=False,
            )
        except subprocess.TimeoutExpired:
            # A hung setup or statement cannot be judged; err on the side of
            # "does not pass" so the fence's skip stands.
            return False
        return done.returncode == 0


def _candidate_fences(page: Path):
    for block in iter_fences(page, DOCS_ROOT):
        directives = block.directives
        if block.lang != "bash" or not directives.skip:
            continue
        if directives.demonstrates_error or UNJUDGEABLE.search(block.source):
            continue
        groups = statement_groups(block.source)
        # Single-command fences count too. They are empirically clean today, but
        # excluding them would leave an unguarded path for exactly the regression
        # this test exists to catch: a lone command that quietly starts passing
        # while the fence still claims it cannot run.
        if groups:
            yield block, groups


@pytest.mark.slow
@pytest.mark.parametrize("page", GUIDE_PAGES, ids=lambda p: p.name)
def test_no_skipped_fence_hides_a_working_command(page: Path):
    """A skipped fence must not contain a command that passes on its own."""
    if BASH is None:
        pytest.skip("no usable bash (the Windows WSL stub is not a shell)")
    offenders = []
    for block, groups in _candidate_fences(page):
        passing = [
            "\n".join(g).strip() for g in groups if _passes("\n".join(g), block.directives.setup)
        ]
        if passing:
            offenders.append(
                f"{page.name}:{block.line} — {len(passing)} of {len(groups)} commands pass\n"
                f"      reason given: {block.directives.skip_reason!r}\n"
                + "".join(f"      passes: {p.splitlines()[-1][:88]}\n" for p in passing[:3])
            )
    assert not offenders, (
        "Skipped fence(s) hiding commands that work:\n"
        + "".join(offenders)
        + "\nSplit the fence so the working commands run (use `menu` when they are\n"
        "alternatives writing the same output), and leave the skip on the rest.\n"
        "If the fence deliberately shows a command failing, add `demonstrates-error`."
    )
