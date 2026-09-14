"""Find fenced code blocks in the guides and read their doctest directives.

Kept free of pytest imports so the meta-test and the collector can share it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

# Languages the harness knows how to execute.
EXECUTABLE_LANGUAGES = ("bash", "python")

# Languages that are documentation, not instructions: nothing to execute, and
# nothing to explain. Anything outside these two sets is a typo or an alias
# (``sh``, ``py``, ``console``) that would silently escape the harness, so the
# meta-test rejects it rather than guessing.
INERT_LANGUAGES = ("", "text", "json", "yaml", "toml", "sql", "js", "powershell")

#: ``<!-- doctest: skip="reason" -->`` on the line above a fence.
DIRECTIVE_COMMENT = re.compile(r"<!--\s*doctest:\s*(?P<body>.*?)\s*-->")

#: One directive: a keyword, optionally ``="quoted value"``. Separated by commas.
_DIRECTIVE_ITEM = re.compile(
    r"""(?P<keyword>[a-z][a-z0-9-]*)      # skip, network, setup, ...
        (?:\s*=\s*"(?P<value>[^"]*)")?    # optional ="value"
        \s*(?:,\s*|$)""",
    re.X,
)

#: A fence, with its own indentation captured so indented (tabbed-block) fences
#: are matched against a closing fence at the same indentation.
_FENCE = re.compile(
    r"^(?P<indent>[ \t]*)```(?P<lang>[A-Za-z0-9_+-]*)[^\n]*\n"
    r"(?P<body>.*?)"
    r"^(?P=indent)```[ \t]*$",
    re.M | re.S,
)

DIRECTIVE_KEYWORDS = {
    # keyword: takes a value?
    "skip": "optional",  # skip="reason"; the reason is shown in the skip report
    "network": "no",  # needs the internet -> pytest.mark.network
    "slow": "no",  # force into the slow lane
    "fast": "no",  # force into the fast lane, whatever page it lives on
    "needs-tippecanoe": "no",  # skipif tippecanoe is not installed
    "needs-ogr": "no",  # skipif ogr2ogr is not installed
    "setup": "required",  # setup="shell command"; may be repeated
    "prelude": "required",  # prelude="source"; prepended to the block before it
    # runs. For the tabbed guides, where a later Python tab continues the
    # session the first one started: the doc stays uncluttered and the block
    # still executes, with the precondition stated in the source.
    "demonstrates-error": "no",  # the fence deliberately shows a command failing,
    # so the commands around it passing is the point, not wasted coverage. Lets a
    # block out of the no-passing-commands-in-a-skipped-fence check below.
    "skip-on": "required",  # skip-on="win32: reason"; sys.platform prefix, then
    # why. For a command the shell on that platform cannot deliver intact --
    # Git Bash's MSYS runtime expands a quoted glob before the program sees it,
    # so `gpio check spatial "part/*.parquet"` arrives as N positional paths.
    # Unlike `skip`, the block still runs everywhere else, so it is not hiding
    # a working command and the no-passing-commands check leaves it alone.
    "menu": "no",  # the lines are alternatives, not a script: run each from a
    # freshly seeded directory. Without it, a fence listing four ways to call
    # one command fails on the second, because the first already wrote the
    # output file and gpio (rightly) refuses to clobber it.
}


class DirectiveSyntaxError(ValueError):
    """A ``<!-- doctest: ... -->`` comment that cannot be understood.

    Raised at collection time so a malformed directive fails loudly instead of
    quietly leaving a block unguarded.
    """


@dataclass(frozen=True)
class Directives:
    """The parsed content of a block's ``doctest`` comment."""

    skip: bool = False
    skip_reason: str = ""
    #: ``(sys.platform, reason)`` pairs; the block is skipped on those platforms
    #: only, and runs normally on every other one.
    skip_on: tuple[tuple[str, str], ...] = ()
    network: bool = False
    slow: bool = False
    fast: bool = False
    needs_tippecanoe: bool = False
    needs_ogr: bool = False
    menu: bool = False
    demonstrates_error: bool = False
    setup: tuple[str, ...] = ()
    prelude: str = ""

    #: True when the block carries any directive at all, i.e. someone made a
    #: deliberate statement about it. The meta-test uses this.
    @property
    def present(self) -> bool:
        return bool(
            self.skip
            or self.skip_on
            or self.network
            or self.slow
            or self.fast
            or self.needs_tippecanoe
            or self.needs_ogr
            or self.menu
            or self.demonstrates_error
            or self.setup
            or self.prelude
        )

    @property
    def runs(self) -> bool:
        """True when the block is actually executed in some lane.

        ``skip_on`` does not make a block stop running: it runs on every
        platform but the named one, which is the whole difference between it
        and ``skip``.
        """
        return not self.skip

    def skipped_here(self, platform: str) -> str:
        """The reason this block is skipped on ``platform``, or ``""``."""
        for name, reason in self.skip_on:
            if name == platform:
                return reason
        return ""


@dataclass(frozen=True)
class Block:
    """One fenced code block in a guide page."""

    path: Path
    #: Path relative to ``docs/``, e.g. ``guide/sort.md`` — the test id prefix.
    relpath: str
    lang: str
    #: 1-based line of the opening fence.
    line: int
    source: str
    directives: Directives = field(default_factory=Directives)
    #: 1-based line of the closing fence. Used by the fence-parity meta-test,
    #: which checks that every ``` marker in a page belongs to a block the
    #: parser found — a stray marker silently swallows the next real block.
    end_line: int = 0

    @property
    def test_id(self) -> str:
        return f"{self.relpath}:L{self.line}[{self.lang}]"

    @property
    def executable(self) -> bool:
        return self.lang in EXECUTABLE_LANGUAGES


def parse_directives(text: str) -> Directives:
    """Parse the body of a ``doctest`` comment into a :class:`Directives`."""
    values: dict[str, object] = {"setup": [], "skip_on": []}
    position = 0
    while position < len(text):
        match = _DIRECTIVE_ITEM.match(text, position)
        if not match:
            raise DirectiveSyntaxError(f"cannot parse directive at {text[position:]!r}")
        keyword, value = match.group("keyword"), match.group("value")
        arity = DIRECTIVE_KEYWORDS.get(keyword)
        if arity is None:
            known = ", ".join(sorted(DIRECTIVE_KEYWORDS))
            raise DirectiveSyntaxError(f"unknown directive {keyword!r} (known: {known})")
        if arity == "required" and value is None:
            raise DirectiveSyntaxError(f'directive {keyword!r} requires a "value"')
        if arity == "no" and value is not None:
            raise DirectiveSyntaxError(f"directive {keyword!r} takes no value")
        if keyword == "skip-on":
            platform, separator, reason = (value or "").partition(":")
            platform, reason = platform.strip(), reason.strip()
            if not platform or not separator or not reason:
                raise DirectiveSyntaxError(
                    f'skip-on takes "<sys.platform>: <why>", e.g. '
                    f'skip-on="win32: the shell expands the quoted glob"; got {value!r}'
                )
            values["skip_on"].append((platform, reason))  # type: ignore[union-attr]
        elif keyword == "prelude":
            values["prelude"] = value
        elif keyword == "setup":
            values["setup"].append(value)  # type: ignore[union-attr]
        elif keyword == "skip":
            values["skip"] = True
            values["skip_reason"] = value or ""
        else:
            values[keyword.replace("-", "_")] = True
        position = match.end()
    return Directives(
        skip=bool(values.get("skip")),
        skip_reason=str(values.get("skip_reason", "")),
        skip_on=tuple(values["skip_on"]),  # type: ignore[arg-type]
        network=bool(values.get("network")),
        slow=bool(values.get("slow")),
        fast=bool(values.get("fast")),
        needs_tippecanoe=bool(values.get("needs_tippecanoe")),
        needs_ogr=bool(values.get("needs_ogr")),
        menu=bool(values.get("menu")),
        demonstrates_error=bool(values.get("demonstrates_error")),
        setup=tuple(values["setup"]),  # type: ignore[arg-type]
        prelude=str(values.get("prelude", "")),
    )


def _directives_above(text: str, fence_start: int) -> Directives:
    """Read the ``doctest`` comment immediately preceding a fence.

    Only blank lines may sit between the comment and the fence, so a directive
    always visibly belongs to the block below it. Indentation is ignored, which
    lets a directive inside a tabbed section line up with its fence.
    """
    preceding = text[:fence_start].rstrip("\n")
    for line in reversed(preceding.split("\n")):
        stripped = line.strip()
        if not stripped:
            continue
        match = DIRECTIVE_COMMENT.search(stripped)
        return parse_directives(match.group("body")) if match else Directives()
    return Directives()


def _dedent(body: str, indent: str) -> str:
    """Remove a tabbed block's fence indentation from every line."""
    if not indent:
        return body
    lines = []
    for line in body.split("\n"):
        lines.append(line[len(indent) :] if line.startswith(indent) else line.lstrip())
    return "\n".join(lines)


def iter_fences(path: Path, docs_root: Path | None = None):
    """Yield every fenced block in ``path``, whatever its language."""
    docs_root = docs_root or path.parent.parent
    text = path.read_text(encoding="utf-8")
    try:
        relpath = path.relative_to(docs_root).as_posix()
    except ValueError:
        relpath = path.name
    for match in _FENCE.finditer(text):
        yield Block(
            path=path,
            relpath=relpath,
            lang=match.group("lang"),
            line=text.count("\n", 0, match.start()) + 1,
            source=_dedent(match.group("body"), match.group("indent")),
            directives=_directives_above(text, match.start()),
            # match.end() sits at the end of the closing fence line.
            end_line=text.count("\n", 0, match.end()) + 1,
        )


def iter_blocks(path: Path, docs_root: Path | None = None):
    """Yield only the blocks the harness knows how to execute."""
    for block in iter_fences(path, docs_root):
        if block.executable:
            yield block


def strip_prompts(source: str) -> str:
    """Drop leading ``$ `` shell prompts so prompted examples still run."""
    lines = source.split("\n")
    if not any(line.lstrip().startswith("$ ") for line in lines):
        return source
    return "\n".join(re.sub(r"^(\s*)\$ ", r"\1", line) for line in lines)
