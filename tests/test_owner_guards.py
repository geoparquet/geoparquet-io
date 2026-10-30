"""Owner guards G1, G2 and G4 of #1367, their exemption counts, and their self-tests.

#1367 is "one owner per concept": a question such as "how is a GeoParquet file
written", "how much memory may DuckDB use" or "is this CRS geographic" has one
module that answers it, and every other module asks that one. The copies that
answered it a second time are where most open defects came from. These checks
make a *new* copy fail the build:

* **G1, banned calls outside the owner.** Two layers. ruff ``TID251``
  (``[tool.ruff.lint.flake8-tidy-imports.banned-api]`` in pyproject.toml) bans
  the imports -- ``pyarrow.parquet.write_table``/``ParquetWriter``, ``psutil``,
  ``os.sysconf``, ``requests``, ``httpx``. The AST walk below covers what an
  import ban cannot express: a ``json.loads`` of the ``geo`` key, a ``SET
  memory_limit`` literal, a hand-rolled SQL escape, a cgroup path, a
  ``duckdb.__version__`` gate, the shapes of a private HTTP retry policy, and a
  bare ``json.dumps`` report in ``cli/``.
* **G2, owner-only definitions.** ``scripts/check_owner_defs.py``, run by the
  ``forbid-bespoke-schema-reconciliation`` hook; this module pins its count.
* **G4, dead code.** vulture at ``min_confidence = 60`` with
  ``scripts/vulture_whitelist.py`` as the baseline; this module pins its size.

(G5, the ``common.py`` guard, lives in ``tests/test_common_split_guards.py``.)

Every exemption, for every check, is in ``scripts/owner_exemptions.toml`` with a
reason. ``EXPECTED_COUNTS`` pins how many each check has, so a number can only
move with a diff that shows it, and every entry must still match something, so
an exemption cannot outlive the copy it excused. When a track deletes a copy it
deletes the entry and lowers the number in the same commit.

The self-tests at the bottom (G12) plant each banned pattern in a synthetic tree
and assert the check fails on it: a guard that cannot fail protects nothing
(#946, #970).
"""

from __future__ import annotations

import ast
import functools
import json
import re
import subprocess
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

from tests._precommit_hook import core_tree, hook_entry

REPO = Path(__file__).resolve().parents[1]
EXEMPTIONS_FILE = REPO / "scripts" / "owner_exemptions.toml"
CHECK_OWNER_DEFS = REPO / "scripts" / "check_owner_defs.py"

#: The pinned number of exemption entries per check (G1 per arm). Lower it with
#: the commit that deletes a copy; raising it needs a reason in review.
EXPECTED_COUNTS = {
    "G1.tid251": 19,
    "G1.geo_parse": 14,
    "G1.set_memory_limit": 1,
    "G1.sql_escape": 1,
    "G1.cgroup": 0,
    "G1.duckdb_version": 2,
    "G1.retry_loop": 3,
    "G1.retry_constants": 4,
    "G1.retry_param_defaults": 28,
    "G1.retry_status_sets": 4,
    "G1.retry_after": 2,
    "G1.retry_wrappers": 4,
    "G1.json_dumps": 2,
    "G2.exemptions": 38,
    "G4.whitelist": 146,
}


def load_exemptions(path: Path = EXEMPTIONS_FILE) -> dict:
    with path.open("rb") as fh:
        return tomllib.load(fh)


# ---------------------------------------------------------------------------
# Shared matching: a hit is excused by exactly one entry, and every entry must
# excuse exactly ``count`` hits (default 1), so a second copy in an already
# exempted function or file still fails.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Hit:
    path: str  # POSIX path from the scanned repo root
    lineno: int
    name: str  # the enclosing def's dotted name, "<module>", or the banned API
    line: str  # the source line, for line_pattern entries


def _entry_matches(entry: dict, hit: Hit) -> bool:
    if entry["path"] != hit.path:
        return False
    if "name" in entry and entry["name"] != hit.name:
        return False
    if "line_pattern" in entry and not re.search(entry["line_pattern"], hit.line):
        return False
    return True


def reconcile(hits: list[Hit], entries: list[dict]) -> tuple[list[Hit], list[str]]:
    """The unexcused hits, and a description of each entry whose count is off."""
    unexcused = [h for h in hits if not any(_entry_matches(e, h) for e in entries)]
    miscounted = []
    for entry in entries:
        matched = sum(_entry_matches(entry, h) for h in hits)
        if matched != entry.get("count", 1):
            miscounted.append(
                f"{entry['path']} {entry.get('name', '')} {entry.get('line_pattern', '')}: "
                f"entry says count={entry.get('count', 1)}, matches {matched}"
            )
    return unexcused, miscounted


def _format(hits: list[Hit]) -> str:
    return "\n  ".join(f"{h.path}:{h.lineno} [{h.name}] {h.line.strip()}" for h in hits)


# ---------------------------------------------------------------------------
# G1, AST layer
# ---------------------------------------------------------------------------


def _owns(path: str, owners: tuple[str, ...]) -> bool:
    return any(fnmatchcase(path, pattern) for pattern in owners)


def _docstring_nodes(tree: ast.Module) -> set[int]:
    """ids of string constants that are bare statements: docstrings and prose."""
    return {
        id(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    }


def _scopes(tree: ast.Module) -> dict[int, str]:
    """id(node) -> dotted name of the innermost enclosing def or class."""
    names: dict[int, str] = {}

    def visit(node: ast.AST, prefix: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                inner = f"{prefix}.{child.name}" if prefix else child.name
                names[id(child)] = inner
                visit(child, inner)
            else:
                names[id(child)] = prefix or "<module>"
                visit(child, prefix)

    visit(tree, "")
    return names


@dataclass
class _Module:
    path: str
    tree: ast.Module
    lines: list[str]
    docstrings: set[int]
    scope: dict[int, str]

    def hit(self, node: ast.AST, name: str | None = None) -> Hit:
        lineno = getattr(node, "lineno", 1)
        return Hit(self.path, lineno, name or self.scope[id(node)], self.lines[lineno - 1])

    def strings(self) -> Iterator[ast.Constant]:
        """String constants in code (f-string parts included), never prose."""
        for node in ast.walk(self.tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and id(node) not in self.docstrings
            ):
                yield node


def _is_call(node: ast.AST, module: str, attr: str) -> bool:
    """``module.attr(...)``."""
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == attr
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == module
    )


def _identifier_tokens(node: ast.AST) -> Iterator[str]:
    for sub in ast.walk(node):
        if isinstance(sub, ast.Name):
            yield from sub.id.lower().split("_")
        elif isinstance(sub, ast.Attribute):
            yield from sub.attr.lower().split("_")


def _mentions_geo(node: ast.AST) -> bool:
    for sub in ast.walk(node):
        if isinstance(sub, ast.Constant) and sub.value in ("geo", b"geo"):
            return True
    return "geo" in set(_identifier_tokens(node))


def _find_geo_parse(m: _Module) -> Iterator[Hit]:
    """``json.loads`` of the ``geo`` key outside ``core/geo_metadata.py``.

    Would have caught #982 and #913 (a bad-JSON or bad-UTF-8 ``geo`` block
    that one reader warned about and another crashed on) and their regrowth in
    #1120 and #1169: each new reader parsed the key itself instead of calling
    the owner's parse-and-sanitize.
    """
    for node in ast.walk(m.tree):
        if _is_call(node, "json", "loads") and any(_mentions_geo(a) for a in node.args):
            yield m.hit(node)


def _find_set_memory_limit(m: _Module) -> Iterator[Hit]:
    """A ``SET memory_limit`` literal outside memory_limits / duckdb_utils.

    Would have caught the BigQuery extractor's own ``SET memory_limit``, a
    second ceiling beside the one policy of #1156, #1174 and #1179.
    """
    for node in m.strings():
        if re.search(r"(?i)\bSET\s+memory_limit\b", node.value):
            yield m.hit(node)


def _find_sql_escape(m: _Module) -> Iterator[Hit]:
    """``.replace("'", "''")`` / ``.replace('"', '""')`` outside duckdb_utils.

    Would have caught the hand-rolled escaper of #1101: ``quote_identifier``,
    ``_escape_sql_string`` and ``sql_path`` are the only escapes, because
    escaping is not idempotent and a second copy escapes twice (#718).
    """
    escapes = {("'", "''"), ('"', '""')}
    for node in ast.walk(m.tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "replace"
            and len(node.args) == 2
            and all(isinstance(a, ast.Constant) for a in node.args)
            and (node.args[0].value, node.args[1].value) in escapes
        ):
            yield m.hit(node)


def _find_cgroup(m: _Module) -> Iterator[Hit]:
    """A ``/sys/fs/cgroup`` path outside ``core/memory_limits.py``.

    Would have caught a second ceiling detector (#1156, #1174): the cgroup cap
    is read in one place, so every connection agrees on the ceiling.
    """
    for node in m.strings():
        if "/sys/fs/cgroup" in node.value:
            yield m.hit(node)


def _find_duckdb_version(m: _Module) -> Iterator[Hit]:
    """``duckdb.__version__`` outside ``core/duckdb_utils.py``.

    Would have caught the three private DuckDB-version gates of #1357, #1358
    and #1360, each deciding "is this 2.0" its own way.
    """
    for node in ast.walk(m.tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr == "__version__"
            and isinstance(node.value, ast.Name)
            and node.value.id == "duckdb"
        ):
            yield m.hit(node)


# The HTTP retry policy (attempts, delay, timeout, which statuses to retry,
# Retry-After) is core/http_retry.py's. The arms below look for its shapes
# rather than its names: #1367's T4a audit showed a name-based guard passing a
# renamed constant, a tuple of statuses, a lowercase header and a ``cast()``
# around an owner call.

#: Owner entry points a caller may call but not wrap, partially apply, or hand
#: its own policy literals. Read from the owner too, so a new one counts.
_OWNER_HTTP_FUNCTIONS = {
    "make_request_with_retry",
    "run_with_retry",
    "get_shared_http_client",
    "reset_http_client",
    "parse_retry_after",
}
_HTTP_OWNER_PATH = "geoparquet_io/core/http_retry.py"
_HTTP_NAMES = {"httpx", "requests", "urllib", "urlopen", "aiohttp", "urllib3"}
_HTTP_VERBS = {"get", "post", "put", "patch", "delete", "head", "request", "send", "stream"}
_HTTP_RECEIVERS = {"client", "session", "http", "httpx", "requests", "conn", "connection"}


def _owner_module(repo: Path) -> ast.Module | None:
    for root in (repo, REPO):
        owner = root / _HTTP_OWNER_PATH
        if owner.is_file():
            return ast.parse(owner.read_text(encoding="utf-8"))
    return None


@functools.lru_cache(maxsize=8)
def _owner_surface(repo: Path) -> tuple[frozenset[str], frozenset[float]]:
    """The owner's public functions, and the values of its ``*TIMEOUT`` constants."""
    tree = _owner_module(repo)
    functions, timeouts = set(_OWNER_HTTP_FUNCTIONS), set()
    for node in tree.body if tree else []:
        if isinstance(node, ast.FunctionDef) and not node.name.startswith("_"):
            functions.add(node.name)
        elif (
            isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, (int, float))
            and any(isinstance(t, ast.Name) and t.id.endswith("TIMEOUT") for t in node.targets)
        ):
            timeouts.add(float(node.value.value))
    return frozenset(functions), frozenset(timeouts)


def _owner_imports(m: _Module) -> dict[str, str]:
    """Local name -> owner name, for every ``from ...http_retry import X [as Y]``."""
    return {
        alias.asname or alias.name: alias.name
        for node in ast.walk(m.tree)
        if isinstance(node, ast.ImportFrom) and (node.module or "").endswith("http_retry")
        for alias in node.names
    }


def _defers_to_owner(m: _Module, default: ast.AST) -> bool:
    """``None``, or a constant imported from the owner (``DEFAULT_TIMEOUT``,
    ``http_retry.DEFAULT_MAX_RETRIES``): the owner still decides."""
    if isinstance(default, ast.Constant) and default.value is None:
        return True
    if isinstance(default, ast.Name):
        return default.id in _owner_imports(m)
    if isinstance(default, ast.Attribute):
        return "retry" in set(_identifier_tokens(default.value))
    return False


def _is_sleep(node: ast.AST) -> bool:
    """``time.sleep(...)``, a bare ``sleep(...)`` (``from time import sleep``) or
    ``<event>.wait(...)``: every way a retry waits."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Attribute):
        if func.attr == "wait":
            return True
        return func.attr == "sleep" and isinstance(func.value, ast.Name) and func.value.id == "time"
    return isinstance(func, ast.Name) and func.id == "sleep"


def _call_name(call: ast.Call) -> str | None:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _does_http(scope: ast.AST) -> bool:
    """The code names an HTTP library, calls a verb on a client-like receiver,
    or hands a URL to a call."""
    for sub in ast.walk(scope):
        if isinstance(sub, ast.Name) and sub.id in _HTTP_NAMES:
            return True
        if isinstance(sub, ast.Attribute) and sub.attr in _HTTP_NAMES:
            return True
        if not isinstance(sub, ast.Call):
            continue
        func = sub.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr in _HTTP_VERBS
            and _HTTP_RECEIVERS & set(_identifier_tokens(func.value))
        ):
            return True
        if any("url" in tok for arg in sub.args for tok in _identifier_tokens(arg)):
            return True
    return False


def _find_retry_loop(m: _Module) -> Iterator[Hit]:
    """A retry outside http_retry: code that makes an HTTP request and waits
    (``time.sleep``, ``from time import sleep``, ``Event.wait``) in a loop, or
    in a function that calls itself again after waiting.

    That is a private retry policy: its own attempt count, backoff and status
    classification. Would have caught the WFS, ArcGIS and CARTO retry loops
    that #1367's T4a folded into ``core/http_retry.py``.
    """
    parents = {id(c): p for p in ast.walk(m.tree) for c in ast.iter_child_nodes(p)}
    flagged: set[int] = set()
    for node in ast.walk(m.tree):
        if not _is_sleep(node):
            continue
        up = parents.get(id(node))
        while up is not None and not isinstance(
            up, (ast.For, ast.While, ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
        ):
            up = parents.get(id(up))
        if up is None or id(up) in flagged or isinstance(up, ast.Lambda):
            continue
        if isinstance(up, (ast.FunctionDef, ast.AsyncFunctionDef)):
            recursive = any(
                isinstance(c, ast.Call) and _call_name(c) == up.name for c in ast.walk(up)
            )
            if not recursive:
                continue
        if _does_http(up):
            flagged.add(id(up))
            yield m.hit(up)


#: Module constants that are retry policy whatever their prefix:
#: ``MAX_RETRIES``, ``DEFAULT_RETRY_DELAY``, ``BACKOFF``, ``MAX_ATTEMPTS``.
_RETRY_CONSTANT = re.compile(
    r"^_?(?:[A-Z0-9]+_)*(?:RETRIES|RETRY|BACKOFF|ATTEMPTS)(?:_[A-Z0-9]+)*$"
)


def _number(node: ast.AST | None) -> float | None:
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        inner = _number(node.operand)
        return None if inner is None else (inner if isinstance(node.op, ast.UAdd) else -inner)
    if (
        isinstance(node, ast.Constant)
        and isinstance(node.value, (int, float))
        and not isinstance(node.value, bool)
    ):
        return float(node.value)
    return None


def _find_retry_constants(m: _Module, repo: Path) -> Iterator[Hit]:
    """A module-level retry constant outside ``core/http_retry.py``: any name
    shaped like ``*RETRIES*``, ``*RETRY*``, ``*BACKOFF*``, ``*ATTEMPTS*``, the
    exact ``DEFAULT_TIMEOUT``, or a ``*_TIMEOUT`` whose value equals an owner
    timeout default.

    The fragmentation that survived T4a (#1367): with the loops gone, each
    caller kept its own attempt count, delay and timeout. A ``*_TIMEOUT`` with
    its own value is a documented per-request budget of the caller (CARTO's
    120, WFS's 600) and is not flagged; one that restates the owner's 60 is a
    copy that drifts when the owner changes.
    """
    _, owner_timeouts = _owner_surface(repo)
    for node in m.tree.body:
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign):
            targets, value = [node.target], node.value
        else:
            continue
        for target in targets:
            if not isinstance(target, ast.Name):
                continue
            name = target.id
            policy_name = _RETRY_CONSTANT.match(name) and name != "MAX_RETRY_AFTER"
            if policy_name or name == "DEFAULT_TIMEOUT":
                yield m.hit(node, name)
            elif name.endswith("_TIMEOUT") and _number(value) in owner_timeouts:
                yield m.hit(node, name)


#: Parameter names that carry retry policy.
_RETRY_PARAMS = {
    "max_retries",
    "retries",
    "retry_delay",
    "attempts",
    "max_attempts",
    "backoff",
    "backoff_factor",
    "timeout",
}


def _find_retry_param_defaults(m: _Module) -> Iterator[Hit]:
    """A default for a retry-policy parameter (``max_retries``, ``retries``,
    ``retry_delay``, ``attempts``, ``backoff``, ``timeout`` ...) outside
    http_retry, whether a literal, ``+3`` or a name.

    The same T4a residue in signature form: a default in a caller's signature
    is a policy that silently overrides the owner's, and the API and CLI
    retyped the owner's 60-second timeout as literals (#1367 T4a audit,
    finding 3). A module-local name is as private as a literal. Allowed:
    ``None`` ("let the owner decide") and a constant imported from the owner.
    """
    for node in ast.walk(m.tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        args = node.args
        positional = [*args.posonlyargs, *args.args]
        pairs = list(
            zip(positional[len(positional) - len(args.defaults) :], args.defaults, strict=True)
        )
        pairs += [(a, d) for a, d in zip(args.kwonlyargs, args.kw_defaults, strict=True) if d]
        scope = m.scope[id(node)]
        for arg, default in pairs:
            if arg.arg in _RETRY_PARAMS and not _defers_to_owner(m, default):
                yield m.hit(arg, f"{scope}({arg.arg})")


def _is_retry_status(value: object) -> bool:
    return value == 429 or (isinstance(value, float) and 500 <= value <= 511)


def _status_like(node: ast.AST) -> bool:
    return bool({"status", "code"} & set(_identifier_tokens(node)))


def _find_retry_status_sets(m: _Module) -> Iterator[Hit]:
    """Which HTTP statuses are worth another attempt, decided outside http_retry.

    A set, tuple or list literal of statuses holding 429 or a 5xx (one element
    is enough: ``{429}``), and a comparison against one: ``s == 429``,
    ``status in (429, 503)`` (the tuple), ``500 <= s < 600``, ``code >= 500``.
    Comparisons with other 4xx codes (``== 401``) are error wording, not retry
    policy, and are not flagged.
    A private ``{408, 429}`` is a second classification (T4a residue, #1367).
    """
    for node in ast.walk(m.tree):
        if isinstance(node, (ast.Set, ast.Tuple, ast.List)) and node.elts:
            values = [_number(e) for e in node.elts]
            if all(v is not None and 100 <= v <= 599 for v in values) and any(
                _is_retry_status(v) for v in values
            ):
                yield m.hit(node)
        elif isinstance(node, ast.Compare):
            operands = [node.left, *node.comparators]
            numbers = {_number(o) for o in operands} - {None}
            if 429 in numbers:
                yield m.hit(node)
            elif 500 in numbers and numbers & {599, 600}:
                yield m.hit(node)
            elif any(500 <= n <= 599 for n in numbers) and any(map(_status_like, operands)):
                yield m.hit(node)


def _find_retry_after(m: _Module) -> Iterator[Hit]:
    """Reading the ``Retry-After`` header, in any case, outside ``core/http_retry.py``.

    The owner parses both RFC 9110 forms and caps the wait; a caller that
    fetches the header itself is one step from parsing it itself (T4a residue).
    httpx headers are case-insensitive, so ``"retry-after"`` is the same read.
    """
    for node in m.strings():
        if "retry-after" in node.value.lower():
            yield m.hit(node)


def _find_retry_wrappers(m: _Module, repo: Path) -> Iterator[Hit]:
    """A private layer over an owner entry point outside http_retry:

    * a function whose whole body is one statement calling an owner function,
      however it is dressed (``cast(...)``, ``bytes(...)``) and under any
      import alias -- the #1367 T4a audit's ``_get_shared_http_client``
      pass-through;
    * ``functools.partial(<owner function>, ...)``;
    * a call to an owner function that passes its own numeric policy
      (``run_with_retry(a, max_retries=5, retry_delay=2.0)``).
    """
    surface, _ = _owner_surface(repo)
    owner_functions = set(surface) | {
        local for local, name in _owner_imports(m).items() if name in surface
    }

    def calls_owner(node: ast.AST) -> bool:
        return any(
            isinstance(c, ast.Call) and _call_name(c) in owner_functions for c in ast.walk(node)
        )

    for node in ast.walk(m.tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            body = [
                s
                for s in node.body
                if not (isinstance(s, ast.Expr) and id(s.value) in m.docstrings)
            ]
            if (
                len(body) == 1
                and isinstance(body[0], (ast.Return, ast.Expr))
                and calls_owner(body[0])
            ):
                yield m.hit(node, m.scope[id(node)] + ".<wrapper>")
        elif isinstance(node, ast.Call) and _call_name(node) == "partial":
            target = node.args[0] if node.args else None
            if (isinstance(target, ast.Name) and target.id in owner_functions) or (
                isinstance(target, ast.Attribute) and target.attr in owner_functions
            ):
                yield m.hit(node)
        elif isinstance(node, ast.Call) and _call_name(node) in owner_functions:
            if any(
                kw.arg in _RETRY_PARAMS and _number(kw.value) is not None for kw in node.keywords
            ):
                yield m.hit(node)


def _find_json_dumps(m: _Module) -> Iterator[Hit]:
    """A bare ``json.dumps`` in ``cli/``: reports go through ``core/json_output.py``.

    ``dumps_json`` is strict JSON over values a bare ``json.dumps`` raises on
    (a DATE, a DECIMAL) or prints as the non-JSON ``NaN`` (#1226).
    """
    for node in ast.walk(m.tree):
        if _is_call(node, "json", "dumps"):
            yield m.hit(node)


@dataclass(frozen=True)
class Arm:
    finder: Callable[..., Iterator[Hit]]
    owners: tuple[str, ...]
    roots: tuple[str, ...] = ("geoparquet_io",)
    #: The finder also takes the scanned repo (to read the owner's surface).
    needs_repo: bool = False


_HTTP_OWNER = ("geoparquet_io/core/http_retry.py",)

#: G1's AST arms: what each looks for, which module owns it, where it looks.
#: The finder's docstring names the defect or regrowth it would have caught.
ARMS = {
    "geo_parse": Arm(_find_geo_parse, ("geoparquet_io/core/geo_metadata.py",)),
    "set_memory_limit": Arm(
        _find_set_memory_limit,
        ("geoparquet_io/core/memory_limits.py", "geoparquet_io/core/duckdb_utils.py"),
    ),
    "sql_escape": Arm(_find_sql_escape, ("geoparquet_io/core/duckdb_utils.py",)),
    "cgroup": Arm(_find_cgroup, ("geoparquet_io/core/memory_limits.py",)),
    "duckdb_version": Arm(_find_duckdb_version, ("geoparquet_io/core/duckdb_utils.py",)),
    "retry_loop": Arm(_find_retry_loop, _HTTP_OWNER),
    "retry_constants": Arm(_find_retry_constants, _HTTP_OWNER, needs_repo=True),
    "retry_param_defaults": Arm(_find_retry_param_defaults, _HTTP_OWNER),
    "retry_status_sets": Arm(_find_retry_status_sets, _HTTP_OWNER),
    "retry_after": Arm(_find_retry_after, _HTTP_OWNER),
    "retry_wrappers": Arm(_find_retry_wrappers, _HTTP_OWNER, needs_repo=True),
    # cli/ only: in core/, json.dumps mostly serializes the geo block and other
    # footer metadata, which is not a report (see the T0b notes in #1367).
    "json_dumps": Arm(
        _find_json_dumps, ("geoparquet_io/core/json_output.py",), ("geoparquet_io/cli",)
    ),
}


@functools.lru_cache(maxsize=8)
def _modules(repo: Path, roots: tuple[str, ...]) -> tuple[_Module, ...]:
    """Every module under *roots*, parsed once per tree (each arm walks them all)."""
    found = []
    for root in roots:
        for source in sorted((repo / root).rglob("*.py")):
            text = source.read_text(encoding="utf-8")
            tree = ast.parse(text)
            found.append(
                _Module(
                    source.relative_to(repo).as_posix(),
                    tree,
                    text.splitlines(),
                    _docstring_nodes(tree),
                    _scopes(tree),
                )
            )
    return tuple(found)


def arm_hits(repo: Path, arm_name: str) -> list[Hit]:
    """Every hit of one arm under *repo*, owners excluded."""
    arm = ARMS[arm_name]
    return [
        hit
        for module in _modules(repo, arm.roots)
        if not _owns(module.path, arm.owners)
        for hit in (arm.finder(module, repo) if arm.needs_repo else arm.finder(module))
    ]


@pytest.mark.parametrize("arm_name", sorted(ARMS))
def test_g1_no_unexempted_call_outside_its_owner(arm_name):
    entries = load_exemptions().get("G1", {}).get(arm_name, [])
    unexcused, miscounted = reconcile(arm_hits(REPO, arm_name), entries)

    assert not unexcused, (
        f"G1 {arm_name}: these do what {ARMS[arm_name].owners[0]} owns. Call the "
        f"owner instead.\n  {_format(unexcused)}\n{ARMS[arm_name].finder.__doc__}"
    )
    assert not miscounted, (
        f"G1 {arm_name}: exemptions in scripts/owner_exemptions.toml no longer match "
        "the tree. Delete an entry whose copy is gone, and lower EXPECTED_COUNTS:\n  "
        + "\n  ".join(miscounted)
    )


# ---------------------------------------------------------------------------
# G1, import layer (ruff TID251)
# ---------------------------------------------------------------------------

#: Who owns each banned API. Mirrors the owner lines of
#: [tool.ruff.lint.per-file-ignores]; the ruff run below ignores those so it can
#: tell an owner from an exempted copy API by API.
TID251_OWNERS = {
    "pyarrow.parquet.write_table": (
        "geoparquet_io/core/write_funnels.py",
        "geoparquet_io/core/parquet_writer.py",
        "geoparquet_io/core/parquet_footer.py",
        "geoparquet_io/core/write_strategies/*.py",
    ),
    "pyarrow.parquet.ParquetWriter": (
        "geoparquet_io/core/write_funnels.py",
        "geoparquet_io/core/parquet_writer.py",
        "geoparquet_io/core/parquet_footer.py",
        "geoparquet_io/core/write_strategies/*.py",
    ),
    "psutil": (
        "geoparquet_io/core/memory_limits.py",
        "geoparquet_io/core/benchmark.py",
        "geoparquet_io/core/benchmark_suite.py",
        "geoparquet_io/benchmarks/*.py",
    ),
    "os.sysconf": (
        "geoparquet_io/core/memory_limits.py",
        "geoparquet_io/core/benchmark.py",
        "geoparquet_io/core/benchmark_suite.py",
        "geoparquet_io/benchmarks/*.py",
    ),
    "requests": _HTTP_OWNER,
    "httpx": _HTTP_OWNER,
}

#: Paths outside the package, which G1 does not govern.
TID251_OUT_OF_SCOPE = {
    "tests/**",
    "scripts/**",
    "docs/**",
    "plugins/**",
    "examples/**",
    "conftest.py",
}

_BANNED = re.compile(r"^`(?P<api>[^`]+)` is banned")


def _pyproject() -> dict:
    with (REPO / "pyproject.toml").open("rb") as fh:
        return tomllib.load(fh)


def tid251_hits(repo: Path, target: Path, config: Path = REPO / "pyproject.toml") -> list[Hit]:
    """TID251 findings under *target*, with every per-file ignore and noqa off."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--config",
            str(config),
            "--config",
            "lint.per-file-ignores = {}",
            "--select",
            "TID251",
            "--ignore-noqa",
            "--no-cache",
            "--output-format",
            "json",
            str(target),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode in (0, 1), result.stderr
    hits = []
    for finding in json.loads(result.stdout):
        path = Path(finding["filename"]).resolve()
        repo = repo.resolve()
        api = _BANNED.match(finding["message"])
        assert api, finding["message"]
        row = finding["location"]["row"]
        lines = path.read_text(encoding="utf-8").splitlines()
        hits.append(Hit(path.relative_to(repo).as_posix(), row, api["api"], lines[row - 1]))
    return hits


def _tid251_non_owner_hits(repo: Path) -> list[Hit]:
    return [
        h
        for h in tid251_hits(repo, repo / "geoparquet_io")
        if not _owns(h.path, TID251_OWNERS[h.name])
    ]


def test_g1_banned_imports_are_owned_or_exempted():
    entries = load_exemptions()["G1"]["tid251"]
    unexcused, miscounted = reconcile(_tid251_non_owner_hits(REPO), entries)

    assert not unexcused, (
        "G1 (TID251): a banned API used outside its owner. Call the owner "
        f"(see the banned-api message in pyproject.toml).\n  {_format(unexcused)}"
    )
    assert not miscounted, "G1 tid251 exemptions out of date:\n  " + "\n  ".join(miscounted)


def test_g1_ruff_per_file_ignores_mirror_the_owners_and_exemptions():
    """pyproject.toml is where ruff reads the exemptions; the toml is where they
    are explained. Neither may hold a TID251 ignore the other does not."""
    ignores = _pyproject()["tool"]["ruff"]["lint"]["per-file-ignores"]
    declared = {path for path, codes in ignores.items() if "TID251" in codes}
    owners = {p for paths in TID251_OWNERS.values() for p in paths}
    # An exempted copy inside a file an owner glob already covers (a benchmark
    # module, which owns psutil but not writes) needs no line of its own.
    exempted = {
        e["path"]
        for e in load_exemptions()["G1"]["tid251"]
        if not any(_owns(e["path"], (owner,)) for owner in owners)
    }

    assert declared == owners | exempted | TID251_OUT_OF_SCOPE, (
        f"only in pyproject: {sorted(declared - owners - exempted - TID251_OUT_OF_SCOPE)}; "
        f"missing from pyproject: {sorted((owners | exempted | TID251_OUT_OF_SCOPE) - declared)}"
    )


def test_g1_tid251_is_selected_and_bans_each_api():
    lint = _pyproject()["tool"]["ruff"]["lint"]
    assert "TID" in lint["select"]
    assert set(lint["flake8-tidy-imports"]["banned-api"]) == set(TID251_OWNERS)


# ---------------------------------------------------------------------------
# G2: owner-only definitions (scripts/check_owner_defs.py)
# ---------------------------------------------------------------------------


def _run_owner_defs(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CHECK_OWNER_DEFS), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def test_g2_the_tree_passes_the_owner_defs_check():
    result = _run_owner_defs("--repo", str(REPO))
    assert result.returncode == 0, result.stdout + result.stderr


def test_g2_every_exemption_has_a_reason_and_owners_exist():
    table = load_exemptions()["G2"]
    pending = set(table.get("pending_owners", {}))
    for owners in table["owners"].values():
        for owner in [owners] if isinstance(owners, str) else owners:
            assert (REPO / owner).is_file() or owner in pending, (
                f"G2 owner {owner} does not exist and is not in [G2.pending_owners]"
            )
    for path in pending:
        assert not (REPO / path).exists(), f"{path} exists now; drop it from [G2.pending_owners]"


def test_g2_hook_runs_the_owner_defs_script():
    hook = hook_entry("forbid-bespoke-schema-reconciliation")
    assert "scripts/check_owner_defs.py" in " ".join([hook["entry"], *hook.get("args", [])])
    assert not {"files", "exclude"} & set(hook), "a files/exclude key would narrow the guard"


def test_g2_the_schema_rule_it_replaced_is_still_in_the_table():
    """The hook this grew out of banned ``def _(align|unify|reconcile)*schema``
    outside arrow_types.py (#1007); that rule must survive the rewrite."""
    owners = load_exemptions()["G2"]["owners"]
    for glob in ("_align*schema", "_unify*schema", "_reconcile*schema"):
        assert owners[glob] == ["geoparquet_io/core/arrow_types.py"]


# ---------------------------------------------------------------------------
# G4: dead code (vulture)
# ---------------------------------------------------------------------------

_WHITELIST_LINE = re.compile(r"^(?P<name>[\w.]+)\s+# unused .*\((?P<path>[^:()]+):\d+\)$")


def _whitelist_entries() -> list[tuple[str, str]]:
    whitelist = REPO / load_exemptions()["G4"]["whitelist"]
    return [
        (m["name"], m["path"])
        for line in whitelist.read_text(encoding="utf-8").splitlines()
        if (m := _WHITELIST_LINE.match(line))
    ]


def test_g4_vulture_runs_at_60_with_the_whitelist():
    vulture = _pyproject()["tool"]["vulture"]
    assert vulture["min_confidence"] == 60
    assert load_exemptions()["G4"]["whitelist"] in vulture["paths"]


def test_g4_every_whitelist_entry_still_names_something():
    """A whitelist line whose name is gone from its file is a stale exemption
    that a new finding of the same name would hide behind."""
    stale = [
        f"{path}: {name}"
        for name, path in _whitelist_entries()
        if not re.search(
            rf"\b{re.escape(name.rsplit('.', 1)[-1])}\b",
            (REPO / path).read_text(encoding="utf-8") if (REPO / path).exists() else "",
        )
    ]
    assert not stale, "delete these lines from the vulture whitelist:\n  " + "\n  ".join(stale)


# ---------------------------------------------------------------------------
# The counts, and the shape of the file
# ---------------------------------------------------------------------------


def _measured_counts() -> dict[str, int]:
    data = load_exemptions()
    counts = {f"G1.{arm}": len(data.get("G1", {}).get(arm, [])) for arm in ARMS}
    counts["G1.tid251"] = len(data["G1"]["tid251"])
    counts["G2.exemptions"] = len(data["G2"]["exemptions"])
    counts["G4.whitelist"] = len(_whitelist_entries())
    return counts


def test_exemption_counts_are_pinned():
    """Each number moves only with a diff to this test that shows it."""
    assert _measured_counts() == EXPECTED_COUNTS


def _all_entries(data: dict) -> Iterator[tuple[str, dict]]:
    for check, table in data.items():
        for key, value in table.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                for entry in value:
                    yield f"{check}.{key}", entry


def test_every_exemption_entry_names_a_path_and_a_reason():
    problems = []
    for where, entry in _all_entries(load_exemptions()):
        if not str(entry.get("reason", "")).strip():
            problems.append(f"{where} {entry.get('path')}: no reason")
        if not (REPO / entry.get("path", "")).is_file():
            problems.append(f"{where} {entry.get('path')}: no such file")
        unknown = set(entry) - {"path", "name", "line_pattern", "count", "reason"}
        if unknown:
            problems.append(f"{where} {entry.get('path')}: unknown keys {sorted(unknown)}")
    assert not problems, "\n".join(problems)


def test_g4_whitelist_has_a_reason():
    assert load_exemptions()["G4"]["reason"].strip()


# ---------------------------------------------------------------------------
# G12: each check fails on a planted violation
# ---------------------------------------------------------------------------

G1_PLANTS = {
    "geo_parse": 'import json\n\ndef read(md):\n    return json.loads(md[b"geo"])\n',
    "set_memory_limit": "def cap(con):\n    con.execute(\"SET memory_limit = '4GB'\")\n",
    "sql_escape": 'def esc(v):\n    return v.replace("\'", "\'\'")\n',
    "cgroup": 'CAP = "/sys/fs/cgroup/memory.max"\n',
    "duckdb_version": 'import duckdb\n\nNEW = duckdb.__version__.startswith("2")\n',
    "retry_loop": (
        "import time\nimport httpx\n\n"
        "def fetch(url):\n"
        "    for attempt in range(3):\n"
        "        try:\n"
        "            return httpx.get(url)\n"
        "        except httpx.HTTPError:\n"
        "            time.sleep(attempt)\n"
    ),
    "retry_constants": "DEFAULT_MAX_RETRIES = 5\n",
    "retry_param_defaults": "def fetch(url, max_retries=5):\n    return url\n",
    "retry_status_sets": "RETRYABLE = {429, 503}\n",
    "retry_after": 'def wait(resp):\n    return resp.headers.get("Retry-After")\n',
    "retry_wrappers": "def fetch(url):\n    return make_request_with_retry('GET', url)\n",
    "json_dumps": "import json\n\ndef show(d):\n    print(json.dumps(d))\n",
}

#: Near-misses each arm must not flag, so a planted hit is the pattern itself.
G1_CLEAN = {
    "geo_parse": "import json\n\ndef read(text):\n    return json.loads(text)\n",
    "set_memory_limit": '"""Never SET memory_limit here."""\n',
    "sql_escape": 'def esc(v):\n    return v.replace("a", "b")\n',
    "cgroup": '"""Reads nothing under /sys/fs/cgroup."""\n',
    "duckdb_version": "import duckdb\n\nV = duckdb.connect\n",
    "retry_loop": (
        "import time\n\ndef wait(path):\n    for i in range(3):\n"
        "        if path.exists():\n            return\n        time.sleep(i)\n"
    ),
    "retry_constants": "DEFAULT_BATCH_SIZE = 5\n",
    "retry_param_defaults": "def fetch(url, max_retries):\n    return url\n",
    "retry_status_sets": "SIZES = (1000, 500, 100)\nTILE = (512, 512)\n",
    "retry_after": '"""Honours Retry-After through http_retry."""\n',
    "retry_wrappers": "def f(a):\n    log(a)\n    return run_with_retry(a)\n",
    "json_dumps": "from geoparquet_io.core.json_output import dumps_json\n",
}


#: Every shape the #1367 T4a audit probe found a name-based retry guard
#: missing (scratchpad probe.py, audit-t4a-2.md), each with the arm that must
#: catch it.
RETRY_SHAPES = [
    ("retry_status_sets", "tuple", "RETRY = (429, 503)\n"),
    ("retry_status_sets", "list", "RETRY = [429, 503]\n"),
    ("retry_status_sets", "inline in", "def f(s):\n    return s in (429, 503)\n"),
    ("retry_status_sets", "range compare", "def f(s):\n    return 500 <= s < 600\n"),
    ("retry_status_sets", "== 429", "def f(s):\n    return s == 429\n"),
    ("retry_status_sets", "code >= 500", "def f(code):\n    return code >= 500\n"),
    ("retry_status_sets", "one-element set", "R = {429}\n"),
    ("retry_status_sets", "frozenset", "R = frozenset({429, 503})\n"),
    ("retry_after", "lowercase", "def g(r):\n    return r.headers.get('retry-after')\n"),
    ("retry_constants", "MAX_RETRIES", "MAX_RETRIES = 5\n"),
    ("retry_constants", "BACKOFF", "BACKOFF = 2.0\n"),
    ("retry_constants", "MAX_ATTEMPTS", "MAX_ATTEMPTS = 4\n"),
    ("retry_constants", "*_TIMEOUT at the owner default", "REQUEST_TIMEOUT = 60\n"),
    ("retry_constants", "annotated", "DEFAULT_RETRY_DELAY: float = 2.0\n"),
    ("retry_param_defaults", "name default", "_R = 5\n\ndef f(max_retries=_R):\n    pass\n"),
    ("retry_param_defaults", "retries=", "def f(retries=5):\n    pass\n"),
    ("retry_param_defaults", "attempts=", "def f(attempts=3):\n    pass\n"),
    ("retry_param_defaults", "backoff=", "def f(backoff=2.0):\n    pass\n"),
    ("retry_param_defaults", "timeout=", "def f(timeout=60.0):\n    pass\n"),
    ("retry_param_defaults", "unary", "def f(max_retries=+3):\n    pass\n"),
    ("retry_param_defaults", "keyword-only", "def f(*, retry_delay=1.5):\n    pass\n"),
    (
        "retry_wrappers",
        "call literal",
        "def f(a):\n    return run_with_retry(a, max_retries=5, retry_delay=2.0)\n",
    ),
    (
        "retry_wrappers",
        "cast wrapper",
        "def w(url, timeout):\n"
        "    return cast('dict', make_request_with_retry('GET', url, timeout=timeout))\n",
    ),
    (
        "retry_wrappers",
        "bytes wrapper",
        "def w(a, d):\n    return bytes(run_with_retry(a, retry_delay=d))\n",
    ),
    (
        "retry_wrappers",
        "client wrapper",
        "def w(timeout):\n    return get_shared_http_client(timeout=timeout)\n",
    ),
    (
        "retry_wrappers",
        "aliased wrapper",
        "from geoparquet_io.core.http_retry import get_shared_http_client as _base\n\n"
        "def w(timeout):\n    return _base(timeout=timeout)\n",
    ),
    (
        "retry_wrappers",
        "partial",
        "import functools\nw = functools.partial(run_with_retry, max_retries=5)\n",
    ),
    (
        "retry_loop",
        "session.get",
        "import time\ndef f(session):\n    for i in range(3):\n"
        "        session.get('u')\n        time.sleep(1)\n",
    ),
    (
        "retry_loop",
        "self.client.get",
        "import time\nclass C:\n    def f(self):\n        while True:\n"
        "            self.client.get('u')\n            time.sleep(1)\n",
    ),
    (
        "retry_loop",
        "urlopen",
        "import time, urllib.request\ndef f():\n    for i in range(3):\n"
        "        urllib.request.urlopen('u')\n        time.sleep(1)\n",
    ),
    (
        "retry_loop",
        "from time import sleep",
        "from time import sleep\ndef f(client):\n    for i in range(3):\n"
        "        client.get('u')\n        sleep(1)\n",
    ),
    (
        "retry_loop",
        "recursion",
        "import time\ndef f(client, n=3):\n    try:\n        return client.get('u')\n"
        "    except Exception:\n        time.sleep(1)\n        return f(client, n - 1)\n",
    ),
    (
        "retry_loop",
        "Event.wait",
        "def f(client, ev):\n    for i in range(3):\n        client.get('u')\n        ev.wait(1)\n",
    ),
]

#: Caller policy or unrelated code the retry arms must leave alone.
RETRY_NEAR_MISSES = [
    ("retry_constants", "a caller's own request budget", "CARTO_REQUEST_TIMEOUT = 120\n"),
    ("retry_constants", "a page timeout", "_PAGE_TIMEOUT = 600.0\n"),
    ("retry_constants", "60 under a non-timeout name", "DEFAULT_BATCH_SIZE = 60\n"),
    ("retry_param_defaults", "None defers", "def f(timeout=None, max_retries=None):\n    pass\n"),
    (
        "retry_param_defaults",
        "an owner constant",
        "from geoparquet_io.core.http_retry import DEFAULT_TIMEOUT\n\n"
        "def f(timeout=DEFAULT_TIMEOUT):\n    pass\n",
    ),
    ("retry_status_sets", "4xx wording", "def f(status):\n    return status == 401\n"),
    ("retry_status_sets", "a row bound", "def f(n):\n    return 10 <= n <= 500\n"),
    ("retry_status_sets", "a batch list", "B = [1000, 500, 100, 50]\n"),
    (
        "retry_wrappers",
        "a call with the caller's value",
        "def f(a, d):\n    x = 1\n    return run_with_retry(a, retry_delay=d)\n",
    ),
    (
        "retry_loop",
        "a file retry",
        "import time\ndef f(p):\n    for i in range(3):\n        p.unlink()\n        time.sleep(1)\n",
    ),
    (
        "retry_loop",
        "recursion without HTTP",
        "import time\ndef f(n):\n    time.sleep(1)\n    return f(n - 1)\n",
    ),
]


@pytest.mark.parametrize(
    ("arm_name", "label", "source"), RETRY_SHAPES, ids=[f"{a}-{lbl}" for a, lbl, _ in RETRY_SHAPES]
)
def test_g12_the_retry_arms_catch_every_shape_the_t4a_probe_found(
    tmp_path, arm_name, label, source
):
    repo = _plant(tmp_path, arm_name, source)
    assert arm_hits(repo, arm_name), f"{arm_name} missed the {label} shape:\n{source}"


@pytest.mark.parametrize(
    ("arm_name", "label", "source"),
    RETRY_NEAR_MISSES,
    ids=[f"{a}-{lbl}" for a, lbl, _ in RETRY_NEAR_MISSES],
)
def test_g12_the_retry_arms_leave_caller_policy_alone(tmp_path, arm_name, label, source):
    repo = _plant(tmp_path, arm_name, source)
    assert arm_hits(repo, arm_name) == [], f"{arm_name} flagged {label}"


def _plant(tmp_path: Path, arm_name: str, source: str) -> Path:
    root = ARMS[arm_name].roots[0]
    target = tmp_path / root / "some_module.py"
    target.parent.mkdir(parents=True)
    target.write_text(source, encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize("arm_name", sorted(ARMS))
def test_g12_each_g1_arm_flags_its_pattern(arm_name, tmp_path):
    repo = _plant(tmp_path, arm_name, G1_PLANTS[arm_name])
    unexcused, _ = reconcile(arm_hits(repo, arm_name), [])
    assert len(unexcused) == 1, unexcused


@pytest.mark.parametrize("arm_name", sorted(ARMS))
def test_g12_each_g1_arm_passes_its_near_miss(arm_name, tmp_path):
    repo = _plant(tmp_path, arm_name, G1_CLEAN[arm_name])
    assert arm_hits(repo, arm_name) == []


@pytest.mark.parametrize("arm_name", sorted(ARMS))
def test_g12_the_owner_is_exempt_and_only_the_owner(arm_name, tmp_path):
    """The owner may spell the pattern out; a same-named module elsewhere may not."""
    arm = ARMS[arm_name]
    owner = next(o for o in arm.owners if "*" not in o)
    if not owner.startswith(arm.roots[0] + "/"):
        pytest.skip(f"{owner} is outside the arm's scan roots {arm.roots}")
    nested = (Path(owner).parent / "sub" / Path(owner).name).as_posix()
    for rel in (owner, nested):
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(G1_PLANTS[arm_name], encoding="utf-8")

    assert [h.path for h in arm_hits(tmp_path, arm_name)] == [nested]


def test_g12_an_exemption_excuses_exactly_its_count(tmp_path):
    """A second copy inside an already exempted function still fails."""
    repo = _plant(tmp_path, "geo_parse", G1_PLANTS["geo_parse"] * 2)
    hits = arm_hits(repo, "geo_parse")
    entry = {"path": "geoparquet_io/some_module.py", "name": "read", "reason": "x"}

    assert len(hits) == 2
    assert reconcile(hits, [entry])[1], "an entry for one copy excused two"
    assert reconcile(hits, [dict(entry, count=2)]) == ([], [])
    assert reconcile(hits, [dict(entry, count=2), dict(entry, name="gone")])[1], (
        "a stale entry passed"
    )


def test_g12_tid251_flags_a_write_table_call(tmp_path):
    repo = core_tree(
        tmp_path,
        {
            "some_module.py": "import pyarrow.parquet as pq\n\ndef w(t):\n    pq.write_table(t, 'x')\n"
        },
    )
    hits = tid251_hits(repo, repo / "geoparquet_io")
    assert [h.name for h in hits] == ["pyarrow.parquet.write_table"]


@pytest.mark.parametrize(
    ("source", "api"),
    [
        ("import pyarrow.parquet as pq\nW = pq.ParquetWriter\n", "pyarrow.parquet.ParquetWriter"),
        ("import psutil\n", "psutil"),
        ("import os\nN = os.sysconf('SC_PAGE_SIZE')\n", "os.sysconf"),
        ("def f():\n    import httpx\n", "httpx"),
        ("import requests\n", "requests"),
    ],
)
def test_g12_tid251_flags_each_banned_api(tmp_path, source, api):
    repo = core_tree(tmp_path, {"some_module.py": source})
    assert api in {h.name for h in tid251_hits(repo, repo / "geoparquet_io")}


def test_g12_tid251_ignores_noqa(tmp_path):
    repo = core_tree(tmp_path, {"some_module.py": "import psutil  # noqa: TID251\n"})
    assert [h.name for h in tid251_hits(repo, repo / "geoparquet_io")] == ["psutil"]


def _g2_tree(tmp_path: Path, files: dict[str, str], exemptions: str = "") -> Path:
    repo = core_tree(tmp_path, files)
    (repo / "scripts").mkdir()
    owners = EXEMPTIONS_FILE.read_text(encoding="utf-8").split("[G2.owners]", 1)[1]
    owners = owners.split("\n[", 1)[0]
    (repo / "scripts" / "owner_exemptions.toml").write_text(
        f"[G2.owners]{owners}\n{exemptions}", encoding="utf-8"
    )
    return repo


def test_g12_g2_flags_a_second_is_geographic_crs(tmp_path):
    repo = _g2_tree(tmp_path, {"extract2.py": "def is_geographic_crs(crs):\n    return True\n"})
    result = _run_owner_defs("--repo", str(repo))
    assert result.returncode == 1, result.stdout
    assert "is_geographic_crs" in result.stdout


def test_g12_g2_flags_the_schema_rule_in_a_nested_module_but_not_the_owner(tmp_path):
    body = "def _align_schema(a, b):  # mirrors arrow_types.py\n    return a\n"
    repo = _g2_tree(tmp_path, {"arrow_types.py": body})
    assert _run_owner_defs("--repo", str(repo)).returncode == 0
    repo2 = _g2_tree(tmp_path / "two", {"partition/arrow_types.py": body})
    assert _run_owner_defs("--repo", str(repo2)).returncode == 1


def test_g12_g2_ignores_a_name_in_a_comment_or_string(tmp_path):
    source = '# def is_geographic_crs(x)\nDOC = "def parse_bbox(s): ..."\n'
    repo = _g2_tree(tmp_path, {"m.py": source})
    assert _run_owner_defs("--repo", str(repo)).returncode == 0


def test_g12_g2_exempts_a_listed_copy_and_fails_a_stale_entry(tmp_path):
    entry = (
        '[[G2.exemptions]]\npath = "geoparquet_io/core/m.py"\n'
        'name = "is_geographic_crs"\nreason = "copy"\n'
    )
    repo = _g2_tree(tmp_path, {"m.py": "def is_geographic_crs(c):\n    return 1\n"}, entry)
    assert _run_owner_defs("--repo", str(repo)).returncode == 0
    stale = _g2_tree(tmp_path / "two", {"m.py": "X = 1\n"}, entry)
    result = _run_owner_defs("--repo", str(stale))
    assert result.returncode == 1 and "stale" in result.stdout


def test_g12_vulture_at_the_configured_level_flags_an_unused_function(tmp_path):
    level = str(_pyproject()["tool"]["vulture"]["min_confidence"])
    dead = tmp_path / "dead.py"
    dead.write_text("def never_called():\n    return 1\n", encoding="utf-8")
    live = tmp_path / "live.py"
    live.write_text("def called():\n    return 1\n\n\nprint(called())\n", encoding="utf-8")

    def vulture(path: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "vulture", "--min-confidence", level, str(path)],
            capture_output=True,
            text=True,
            cwd=tmp_path,
        )

    flagged = vulture(dead)
    assert flagged.returncode != 0 and "never_called" in flagged.stdout
    assert vulture(live).returncode == 0
