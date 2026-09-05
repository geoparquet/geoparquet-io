"""Pytest glue: turn each fenced block in ``docs/guide/*.md`` into a test item.

Test ids are ``guide/sort.md:L14[bash]`` and the reported location is the doc
file and the fence's line, so a failure points straight at the example that
broke rather than at harness code.

**What this harness does and does not prove.** An example passes when it exits
zero. Nothing here inspects what it wrote, so a command that succeeds while
doing nothing useful still counts as green — ``gpio add bbox in.parquet
out.parquet`` on a file that already has a bbox prints a notice, writes no
output file, and exits 0. That is a real gap, not an oversight: catching it
needs expected-output assertions (an ``expects="..."`` directive, or a
post-condition per block), which is deliberately left for a follow-up. Read a
green docs lane as "every documented command still parses, runs, and does not
crash", not as "every documented command does what the prose claims".
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from tests.docs_examples.parser import Block, iter_blocks, strip_prompts
from tests.docs_examples.seeder import seed_workdir

#: Pages whose blocks run in the fast suite. Everything else lands in the slow
#: lane. Deliberately small: the fast subset is a smoke test that the core
#: commands in the docs still work, not a second full run.
FAST_PAGES = frozenset({"sort.md", "piping.md", "check.md"})

#: Wall-clock ceiling for a single example. Generous — it exists to stop a
#: prompting or hanging command from wedging CI, not to police runtimes.
BLOCK_TIMEOUT_SECONDS = 120

#: Ceiling for a ``network``-marked example, which has to finish a real download
#: before it can run anything. The largest of these is the GAUL boundary
#: dataset at ~724 MB: at a pessimistic-but-plausible 2 MB/s that is six
#: minutes, so 120 s could never have been enough and the block could only ever
#: time out (#894). 600 s clears that fetch with headroom while still capping a
#: genuinely wedged command. It applies only to blocks that opted into the
#: network lane; every other block keeps the 120 s default, so a hanging local
#: example still fails fast.
NETWORK_BLOCK_TIMEOUT_SECONDS = 600


def find_bash() -> str | None:
    """Locate a bash that can actually run scripts, or ``None``.

    On Windows, ``bash`` on ``PATH`` usually resolves to the System32 WSL
    stub, which is not a shell: without a WSL distribution installed it prints
    an install hint and exits 1 (in UTF-16, for good measure). Git Bash is the
    real thing and ships with the Git for Windows install every contributor
    and CI runner already has, so prefer it by its well-known location.
    """
    if sys.platform != "win32":
        return shutil.which("bash")
    for env_var in ("ProgramFiles", "ProgramFiles(x86)"):
        candidate = Path(os.environ.get(env_var, "")) / "Git" / "bin" / "bash.exe"
        if candidate.is_file():
            return str(candidate)
    found = shutil.which("bash")
    if found and "system32" not in found.lower():
        return found
    return None


#: Resolved once at import: every bash example in a run uses the same shell.
BASH = find_bash()


class DocExampleFile(pytest.File):
    """A guide page. Collects one item per executable fenced block."""

    def collect(self):
        docs_root = Path(str(self.path)).parent.parent
        for block in iter_blocks(Path(str(self.path)), docs_root):
            yield DocExampleItem.from_parent(self, name=block.test_id, block=block)


class DocExampleItem(pytest.Item):
    """One ``bash`` or ``python`` example from a guide page."""

    def __init__(self, *, block: Block, **kwargs):
        super().__init__(**kwargs)
        self.block = block
        self._apply_marks()

    def _apply_marks(self) -> None:
        directives = self.block.directives
        self.add_marker(pytest.mark.docs_example)
        if directives.network:
            self.add_marker(pytest.mark.network)
        if _is_slow(self.block):
            self.add_marker(pytest.mark.slow)
        if directives.skip:
            reason = directives.skip_reason or "marked skip by a doctest directive"
            self.add_marker(pytest.mark.skip(reason=reason))
        if directives.needs_tippecanoe and shutil.which("tippecanoe") is None:
            self.add_marker(pytest.mark.skip(reason="tippecanoe not installed"))
        if directives.needs_ogr and shutil.which("ogr2ogr") is None:
            self.add_marker(pytest.mark.skip(reason="ogr2ogr not installed"))
        if self.block.lang == "bash" and BASH is None:
            self.add_marker(
                pytest.mark.skip(reason="no usable bash (the Windows WSL stub is not a shell)")
            )

    def runtest(self) -> None:
        source = strip_prompts(self.block.source)
        if self.block.directives.prelude:
            source = self.block.directives.prelude + "\n" + source
        if self.block.directives.menu:
            refusal = menu_refusal_reason(self.block.lang, source, self.block.directives)
            if refusal:
                pytest.fail(refusal)
            for statement in split_statements(source):
                self._run_in_fresh_dir(statement)
            return
        self._run_in_fresh_dir(source)

    def _run_in_fresh_dir(self, source: str) -> None:
        workdir = seed_workdir(self._tmp_base() / "work")
        for command in self.block.directives.setup:
            self._run("bash", command, workdir, phase="setup")
        self._run(self.block.lang, source, workdir, phase="block")

    def _tmp_base(self) -> Path:
        slug = self.block.test_id.replace("/", "_").replace(":", "_").replace("[", "_")
        # _tmp_path_factory is private pytest API; fall back to tempfile so a
        # pytest bump that renames it degrades to unmanaged temp dirs instead of
        # an AttributeError on every docs example.
        factory = getattr(self.config, "_tmp_path_factory", None)
        if factory is not None:
            return factory.mktemp(slug.rstrip("]")[:30], numbered=True)
        return Path(tempfile.mkdtemp(prefix=slug.rstrip("]")[:30]))

    def _run(self, lang: str, source: str, workdir: Path, *, phase: str) -> None:
        script = workdir / (".doctest_block.sh" if lang == "bash" else ".doctest_block.py")
        script.write_text(
            source if lang == "python" else f"set -euo pipefail\n{source}\n",
            encoding="utf-8",
        )
        argv = [BASH, str(script)] if lang == "bash" else [sys.executable, str(script)]
        try:
            completed = subprocess.run(
                argv,
                cwd=workdir,
                env=_subprocess_env(),
                capture_output=True,
                encoding="utf-8",
                errors="replace",
                timeout=block_timeout(self.block),
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise DocExampleFailure(
                self.block, phase, source, -1, "", f"timed out after {exc.timeout}s"
            ) from None
        if completed.returncode != 0:
            raise DocExampleFailure(
                self.block, phase, source, completed.returncode, completed.stdout, completed.stderr
            )

    def repr_failure(self, excinfo, style=None):  # noqa: ARG002
        if isinstance(excinfo.value, DocExampleFailure):
            return str(excinfo.value)
        return super().repr_failure(excinfo)

    def reportinfo(self):
        # The 0-based line pytest wants; points at the opening fence.
        return self.path, self.block.line - 1, self.block.test_id


class DocExampleFailure(Exception):
    """A documentation example that exited non-zero.

    Formats itself as the doc location, the source that ran, and the output,
    because the useful debugging material is the example, not a Python traceback
    through the harness.
    """

    def __init__(
        self, block: Block, phase: str, source: str, returncode: int, stdout: str, stderr: str
    ):
        super().__init__(block.test_id)
        self.block, self.phase = block, phase
        self.source, self.returncode = source, returncode
        self.stdout, self.stderr = stdout, stderr

    def __str__(self) -> str:
        where = f"{self.block.path}:{self.block.line}"
        header = f"{self.block.lang} example failed (exit {self.returncode}) at {where}"
        if self.phase == "setup":
            header = f"doctest setup= command failed (exit {self.returncode}) for {where}"
        parts = [header, "", _indent(self.source, "  | ")]
        if self.stdout.strip():
            parts += ["", "--- stdout ---", _tail(self.stdout)]
        if self.stderr.strip():
            parts += ["", "--- stderr ---", _tail(self.stderr)]
        parts += ["", *self._advice()]
        return "\n".join(parts)

    def _advice(self) -> list[str]:
        """Closing lines: what the reader should actually do about this failure.

        A network block failing is usually not a documentation error — it is a
        real download that did not happen — so say that instead of pointing at
        the doc, which has sent more than one reader hunting for a broken
        example that was fine (#894).
        """
        if not self.block.directives.network:
            return [
                "Fix the example, or mark it in the doc with an HTML comment on the",
                'line above the fence, e.g. <!-- doctest: skip="needs credentials" -->.',
            ]
        return [
            "This block is marked <!-- doctest: network -->: it needs the internet",
            "and a real download (the boundary datasets run to hundreds of MB), so",
            "it runs only in the network lane and only against live third-party",
            "services. A failure here usually means the download was slow, blocked",
            "or unavailable — not that the documented command is wrong. Check the",
            "network before editing the doc, and note that the whole docs lane is",
            'meant to be run as -m "docs_example and not network".',
        ]


#: Shell syntax that makes "one line, one independent command" untrue. A menu
#: block containing any of it is refused rather than mis-split.
_NOT_A_MENU = re.compile(r"^\s*(for|while|if|case|function)\b|<<|\$\(|`|^\s*\w+=", re.M)


def split_statements(source: str) -> list[str]:
    """Split a menu block into its individual commands.

    Deliberately simple: a statement starts at a non-indented, non-comment line
    and continues while lines end in ``\\``, ``|``, ``&&`` or ``||``. Comments
    are carried along with the command they introduce so a failure still reads
    like the doc. Anything more shell-like than that is rejected by
    :func:`check_menu_is_splittable` instead of being guessed at.
    """
    statements: list[str] = []
    pending: list[str] = []
    continuing = False
    for line in source.split("\n"):
        stripped = line.strip()
        if not stripped and not continuing:
            continue
        starts_new = not continuing and not line[:1].isspace()
        if starts_new and any(not ln.strip().startswith("#") for ln in pending):
            statements.append("\n".join(pending))
            pending = []
        pending.append(line)
        continuing = stripped.endswith(("\\", "|", "&&", "||"))
    if pending and any(not ln.strip().startswith("#") for ln in pending):
        statements.append("\n".join(pending))
    return statements


def menu_refusal_reason(lang: str, source: str, directives) -> str | None:
    """Why this block may not be treated as a menu, or ``None`` if it may.

    Split out of ``runtest`` so the refusals are testable without building a
    pytest item around them.
    """
    if not directives.menu:
        return None
    if lang != "bash":
        return "the menu directive only applies to bash blocks"
    if directives.prelude:
        # prelude exists for Python tabs continuing an earlier session; menu is
        # bash-only. Combined, the prelude line would be split off as its own
        # "alternative" and run alone, which is never what the author meant.
        return "menu and prelude cannot be combined (menu is bash, prelude is Python)"
    problem = check_menu_is_splittable(source)
    return f"menu directive refused: {problem}" if problem else None


def check_menu_is_splittable(source: str) -> str | None:
    """Return why ``source`` may not be treated as a menu, or ``None``."""
    match = _NOT_A_MENU.search(source)
    if match:
        return (
            f"contains shell syntax that is not a standalone command "
            f"({match.group(0).strip()!r}); the lines are a script, not a menu"
        )
    return None


def block_timeout(block: Block) -> int:
    """Seconds this block gets before it is killed.

    A module-level function rather than a method so the choice is testable
    without building a pytest item around a fence.
    """
    if block.directives.network:
        return NETWORK_BLOCK_TIMEOUT_SECONDS
    return BLOCK_TIMEOUT_SECONDS


def _is_slow(block: Block) -> bool:
    """Fast lane membership. Directives win over the page default."""
    if block.directives.fast:
        return False
    if block.directives.slow:
        return True
    return block.path.name not in FAST_PAGES


def _subprocess_env() -> dict[str, str]:
    """Environment for an example: the project's venv first on ``PATH``."""
    env = dict(os.environ)
    venv_bin = str(Path(sys.executable).parent)
    env["PATH"] = venv_bin + os.pathsep + env.get("PATH", "")
    env["NO_COLOR"] = "1"
    env["COLUMNS"] = "100"
    # Examples must never inherit the harness's own coverage instrumentation.
    env.pop("COV_CORE_SOURCE", None)
    return env


def _indent(text: str, prefix: str) -> str:
    return "\n".join(prefix + line for line in text.rstrip("\n").split("\n"))


def _tail(text: str, limit: int = 40) -> str:
    lines = text.rstrip("\n").split("\n")
    if len(lines) <= limit:
        return _indent(text, "  ")
    return _indent(
        "\n".join([f"... {len(lines) - limit} earlier lines ...", *lines[-limit:]]), "  "
    )
