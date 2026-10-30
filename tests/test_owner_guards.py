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
  ``owner-only-defs`` hook; this module pins its count.
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
from http import HTTPStatus
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
    "G1.tid251": 21,
    "G1.geo_parse": 17,
    "G1.set_memory_limit": 0,
    "G1.sql_escape": 1,
    "G1.cgroup": 0,
    "G1.duckdb_version": 0,
    "G1.retry_loop": 0,
    "G1.retry_constants": 0,
    "G1.retry_param_defaults": 1,
    "G1.retry_status_sets": 2,
    "G1.retry_after": 0,
    "G1.retry_wrappers": 0,
    "G1.json_dumps": 40,
    "G2.exemptions": 33,
    "G3.pairs": 27,
    "G3.baseline": 27,
    "G3.pylint": 58,
    "G13.options": 13,
    "G13.bbox_parsers": 3,
    "G4.whitelist": 184,
    "G4.tests_whitelist": 99,
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
    """The unexcused hits, and a description of each entry whose count is off.

    ``count = "*"`` (a per-file allowance, used only where a whole module is
    legitimately in the pattern's business) matches one hit or more."""
    unexcused = [h for h in hits if not any(_entry_matches(e, h) for e in entries)]
    miscounted = []
    for entry in entries:
        matched = sum(_entry_matches(entry, h) for h in hits)
        want = entry.get("count", 1)
        if matched != want and not (want == "*" and matched >= 1):
            miscounted.append(
                f"{entry['path']} {entry.get('name', '')} {entry.get('line_pattern', '')}: "
                f"entry says count={want}, matches {matched}"
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


_HTTP_STATUS = {member.name: member.value for member in HTTPStatus}


def _imports(tree: ast.Module) -> dict[str, str]:
    """Local name -> dotted origin, for every import in the module
    (``import json as j`` -> ``j: json``; ``from json import loads as l`` ->
    ``l: json.loads``)."""
    bound: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    bound[alias.asname] = alias.name
                else:
                    top = alias.name.split(".", 1)[0]
                    bound[top] = top
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            for alias in node.names:
                bound[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return bound


def _bindings(tree: ast.AST) -> dict[str, list[ast.AST]]:
    """Name -> every value assigned to it by ``NAME = value`` anywhere."""
    bound: dict[str, list[ast.AST]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    bound.setdefault(target.id, []).append(node.value)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value:
            bound.setdefault(node.target.id, []).append(node.value)
    return bound


@dataclass
class _Module:
    path: str
    tree: ast.Module
    lines: list[str]
    docstrings: set[int]
    scope: dict[int, str]

    def __post_init__(self) -> None:
        self.imports = _imports(self.tree)
        self.bindings = _bindings(self.tree)
        self.parents = {id(c): p for p in ast.walk(self.tree) for c in ast.iter_child_nodes(p)}

    def local_bindings(self, node: ast.AST) -> dict[str, list[ast.AST]]:
        """The assignments of the function enclosing *node* (the module's at
        top level): a name's earlier value, without another function's ``raw``."""
        up = self.parents.get(id(node))
        while up is not None and not isinstance(up, (ast.FunctionDef, ast.AsyncFunctionDef)):
            up = self.parents.get(id(up))
        if up is None:
            return self.bindings
        cache = self.__dict__.setdefault("_local", {})
        if id(up) not in cache:
            cache[id(up)] = _bindings(up)
        return cache[id(up)]

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

    def dotted(self, node: ast.AST) -> str | None:
        """``node``'s dotted origin through the module's imports:
        ``j.loads`` -> ``json.loads``, ``V`` -> ``duckdb.__version__``."""
        if isinstance(node, ast.Name):
            return self.imports.get(node.id, node.id)
        if isinstance(node, ast.Attribute):
            base = self.dotted(node.value)
            return f"{base}.{node.attr}" if base else None
        return None

    def calls(self, node: ast.AST, *targets: str) -> bool:
        return isinstance(node, ast.Call) and self.dotted(node.func) in targets

    def const(self, node: ast.AST | None, depth: int = 0) -> object:
        """A literal's value, following module names one binding deep at a time
        (``Q = "'"``; ``TOO_MANY = 429``), simple arithmetic and string
        concatenation, ``chr(39)``, and ``HTTPStatus`` members. ``None`` when
        it cannot be known."""
        if node is None or depth > 4:
            return None
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            inner = self.const(node.operand, depth + 1)
            if isinstance(inner, (int, float)) and not isinstance(inner, bool):
                return inner if isinstance(node.op, ast.UAdd) else -inner
            return None
        if isinstance(node, ast.Name):
            values = self.bindings.get(node.id, [])
            return self.const(values[0], depth + 1) if len(values) == 1 else None
        if isinstance(node, ast.Attribute):
            dotted = self.dotted(node) or ""
            if dotted.rsplit(".", 1)[0].endswith("HTTPStatus"):
                return _HTTP_STATUS.get(node.attr)
            return None
        if isinstance(node, ast.BinOp):
            left = self.const(node.left, depth + 1)
            right = self.const(node.right, depth + 1)
            try:
                if isinstance(node.op, ast.Add):
                    return left + right  # type: ignore[operator]
                if isinstance(node.op, ast.Mult):
                    return left * right  # type: ignore[operator]
            except TypeError:
                return None
            return None
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "chr":
            code = self.const(node.args[0], depth + 1) if node.args else None
            return chr(code) if isinstance(code, int) else None
        return None


def _identifier_tokens(node: ast.AST) -> Iterator[str]:
    for sub in ast.walk(node):
        if isinstance(sub, ast.Name):
            yield from sub.id.lower().split("_")
        elif isinstance(sub, ast.Attribute):
            yield from sub.attr.lower().split("_")


def _mentions_geo(node: ast.AST) -> bool:
    """The ``geo`` key by constant (``b"geo"``, or ``key = 'geo'`` in a footer
    query) or by an identifier token (``geo_bytes``)."""
    for sub in ast.walk(node):
        if isinstance(sub, ast.Constant) and sub.value in ("geo", b"geo"):
            return True
        if isinstance(sub, ast.Constant) and isinstance(sub.value, str) and "'geo'" in sub.value:
            return True
    return "geo" in set(_identifier_tokens(node))


def _geo_bound(m: _Module, node: ast.AST, depth: int = 2) -> bool:
    """*node* mentions the geo key, or names a variable its function assigned
    from an expression that does, up to *depth* assignments back
    (``raw = md[b"geo"]``; ``row = con.execute("... key = 'geo'")`` then
    ``text = row[0].decode()``)."""
    if _mentions_geo(node):
        return True
    if depth == 0:
        return False
    local = m.local_bindings(node)
    return any(
        isinstance(sub, ast.Name)
        and any(_geo_bound(m, v, depth - 1) for v in local.get(sub.id, []))
        for sub in ast.walk(node)
    )


def _find_geo_parse(m: _Module) -> Iterator[Hit]:
    """``json.loads`` of the ``geo`` key outside ``core/geo_metadata.py``,
    however ``loads`` is imported, and with the key fetched in the call or up to
    two assignments earlier.

    Would have caught #982 and #913 (a bad-JSON or bad-UTF-8 ``geo`` block
    that one reader warned about and another crashed on) and their regrowth in
    #1120 and #1169: each new reader parsed the key itself instead of calling
    the owner's parse-and-sanitize.
    """
    for node in ast.walk(m.tree):
        if m.calls(node, "json.loads") and any(_geo_bound(m, a) for a in node.args):
            yield m.hit(node)


_MEMORY_SETTING = re.compile(r"(?i)\b(?:SET|PRAGMA)\s+(?:GLOBAL\s+)?(?:memory_limit|max_memory)\b")
_MEMORY_KEYS = {"memory_limit", "max_memory"}


def _find_set_memory_limit(m: _Module) -> Iterator[Hit]:
    """Setting DuckDB's memory ceiling outside memory_limits / duckdb_utils:
    ``SET``/``PRAGMA memory_limit`` (or its alias ``max_memory``), or a
    ``{"memory_limit": ...}`` connection config.

    Would have caught the BigQuery extractor's own ``SET memory_limit``, a
    second ceiling beside the one policy of #1156, #1174 and #1179.
    """
    for node in m.strings():
        if _MEMORY_SETTING.search(node.value):
            yield m.hit(node)
    for node in ast.walk(m.tree):
        if (
            isinstance(node, ast.keyword)
            and node.arg == "config"
            and isinstance(node.value, ast.Dict)
            and any(m.const(k) in _MEMORY_KEYS for k in node.value.keys)
        ):
            yield m.hit(node.value)


def _find_sql_escape(m: _Module) -> Iterator[Hit]:
    """Doubling a quote by hand outside duckdb_utils: ``.replace("'", "''")``,
    ``str.replace(v, '"', '""')``, or the same with the quote in a name or
    built with ``chr(39)``.

    Would have caught the hand-rolled escaper of #1101: ``quote_identifier``,
    ``_escape_sql_string`` and ``sql_path`` are the only escapes, because
    escaping is not idempotent and a second copy escapes twice (#718).
    """
    escapes = {("'", "''"), ('"', '""')}
    for node in ast.walk(m.tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "replace"
            and len(node.args) >= 2
        ):
            continue
        old, new = (m.const(a) for a in node.args[-2:])
        if (old, new) in escapes:
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
    """Reading DuckDB's version outside ``core/duckdb_utils.py``:
    ``duckdb.__version__`` under any import alias, ``from duckdb import
    __version__``, or ``importlib.metadata.version("duckdb")``.

    Would have caught the three private DuckDB-version gates of #1357, #1358
    and #1360, each deciding "is this 2.0" its own way.
    """
    for node in ast.walk(m.tree):
        if isinstance(node, ast.ImportFrom) and node.module == "duckdb":
            if any(alias.name == "__version__" for alias in node.names):
                yield m.hit(node)
        elif isinstance(node, ast.Attribute) and node.attr == "__version__":
            if m.dotted(node) == "duckdb.__version__":
                yield m.hit(node)
        elif m.calls(node, "importlib.metadata.version", "importlib_metadata.version"):
            if node.args and m.const(node.args[0]) == "duckdb":
                yield m.hit(node)


# The HTTP retry policy (attempts, delay, timeout, which statuses to retry,
# Retry-After) is core/http_retry.py's. The arms below look for its shapes
# rather than its names: #1367's T4a audit showed a name-based guard passing a
# renamed constant, a tuple of statuses, a lowercase header and a ``cast()``
# around an owner call, and the T0b audit a helper that sleeps, a two-statement
# wrapper, ``num_retries=``, class constants, ``HTTPStatus`` and ``range(500, 600)``.

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
_HTTP_RECEIVERS = {"client", "session", "http", "httpx", "requests", "con", "conn", "connection"}
#: Calls that open a network client: the owner's, or a DuckDB connection whose
#: remote read (httpfs) a loop retries.
_CLIENT_FACTORIES = {"get_shared_http_client", "_get_shared_http_client", "get_duckdb_connection"}
#: Name tokens that make a parameter, keyword or constant retry policy:
#: ``max_retries``, ``num_retries``, ``max_tries``, ``attempts``, ``retry_delay``,
#: ``backoff_factor``.
_RETRY_TOKENS = {"retries", "retry", "tries", "attempts", "backoff"}


def _is_retry_policy_name(name: str) -> bool:
    tokens = set(name.lower().strip("_").split("_"))
    return bool(tokens & _RETRY_TOKENS) and "after" not in tokens


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
        local: origin.rsplit(".", 1)[1]
        for local, origin in m.imports.items()
        if origin.rsplit(".", 1)[0].endswith("http_retry")
    }


def _defers_to_owner(m: _Module, default: ast.AST) -> bool:
    """``None``, a bool switch, or a constant imported from the owner
    (``DEFAULT_TIMEOUT``, ``http_retry.DEFAULT_MAX_RETRIES``): the owner still decides."""
    if isinstance(default, ast.Constant) and (
        default.value is None or isinstance(default.value, bool)
    ):
        return True
    if isinstance(default, ast.Name):
        return default.id in _owner_imports(m)
    if isinstance(default, ast.Attribute):
        return "retry" in set(_identifier_tokens(default.value))
    return False


def _names_a_request_budget(arg: ast.arg, default: ast.AST) -> bool:
    """``timeout=CARTO_REQUEST_TIMEOUT``: a caller's own named, documented
    request budget is caller policy (the #1367 T4a audit's ruling). Where that
    constant is defined, ``retry_constants`` still flags it if its value merely
    restates the owner's default; a literal ``timeout=60`` is still flagged."""
    if arg.arg != "timeout":
        return False
    name = default.id if isinstance(default, ast.Name) else getattr(default, "attr", "")
    return name.endswith("_TIMEOUT") and name != "DEFAULT_TIMEOUT"


def _is_sleep(node: ast.AST) -> bool:
    """``time.sleep(...)`` (or any ``<x>.sleep``), a bare ``sleep(...)``
    (``from time import sleep``) or ``<event>.wait(...)``: every way a retry waits."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr in ("wait", "sleep")
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
    opens a client, or hands a URL to a call."""
    for sub in ast.walk(scope):
        if isinstance(sub, ast.Name) and sub.id in _HTTP_NAMES:
            return True
        if isinstance(sub, ast.Attribute) and sub.attr in _HTTP_NAMES:
            return True
        if not isinstance(sub, ast.Call):
            continue
        if _call_name(sub) in _CLIENT_FACTORIES:
            return True
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


def _helpers(m: _Module) -> tuple[set[str], set[str]]:
    """Module functions that wait, and module functions that make a request:
    a call to one stands for the wait or the request it hides."""
    waits, requests = set(), set()
    for node in ast.walk(m.tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if any(_is_sleep(c) for c in ast.walk(node)):
                waits.add(node.name)
            if _does_http(node) or _calls_a_verb(node):
                requests.add(node.name)
    return waits, requests


def _calls_a_verb(node: ast.AST) -> bool:
    """An HTTP verb (``.get``, ``.post`` ...) on any receiver with a string
    first argument: in code that also waits in a loop, ``c.get('u')`` is a
    request whatever ``c`` is called. Only used beside a wait, where
    ``dict.get`` in a sleeping loop is rare enough to be worth a look."""
    return any(
        isinstance(c, ast.Call)
        and isinstance(c.func, ast.Attribute)
        and c.func.attr in _HTTP_VERBS
        and c.args
        and isinstance(c.args[0], (ast.Constant, ast.JoinedStr, ast.Name))
        for c in ast.walk(node)
    )


def _find_retry_loop(m: _Module) -> Iterator[Hit]:
    """A retry outside http_retry: code that makes an HTTP request and waits,
    in a loop (the wait may be in an inner loop, and either half may sit in a
    same-module helper the loop calls) or in a function that calls itself again
    after waiting. Waits are ``time.sleep``, ``from time import sleep``, any
    ``<x>.sleep`` and ``Event.wait``.

    That is a private retry policy: its own attempt count, backoff and status
    classification. Would have caught the WFS, ArcGIS and CARTO retry loops
    that #1367's T4a folded into ``core/http_retry.py``.
    """
    waits, requests = _helpers(m)

    def waits_in(node: ast.AST) -> bool:
        return any(
            _is_sleep(c) or (isinstance(c, ast.Call) and _call_name(c) in waits)
            for c in ast.walk(node)
        )

    def requests_in(node: ast.AST) -> bool:
        return _does_http(node) or any(
            isinstance(c, ast.Call) and _call_name(c) in requests for c in ast.walk(node)
        )

    flagged: set[int] = set()
    for node in ast.walk(m.tree):  # breadth-first: an outer loop before its inner ones
        if isinstance(node, (ast.For, ast.While, ast.AsyncFor)):
            up = m.parents.get(id(node))
            while up is not None and id(up) not in flagged:
                up = m.parents.get(id(up))
            if up is None and waits_in(node) and requests_in(node):
                flagged.add(id(node))
                yield m.hit(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            recursive = any(
                isinstance(c, ast.Call) and _call_name(c) == node.name for c in ast.walk(node)
            )
            if recursive and waits_in(node) and requests_in(node):
                yield m.hit(node)


def _find_retry_constants(m: _Module, repo: Path) -> Iterator[Hit]:
    """A retry constant outside ``core/http_retry.py``, at module level, in a
    class body or in a function: an UPPER_CASE name carrying a retry token
    (``MAX_RETRIES``, ``_N_TRIES``, ``BACKOFF``, ``MAX_ATTEMPTS``), the exact
    ``DEFAULT_TIMEOUT``, or a ``*_TIMEOUT`` whose value equals an owner default.

    The fragmentation that survived T4a (#1367): with the loops gone, each
    caller kept its own attempt count, delay and timeout. A ``*_TIMEOUT`` with
    its own value is a documented per-request budget of the caller (CARTO's
    120, WFS's 600) and is not flagged; one that restates the owner's 60 is a
    copy that drifts when the owner changes.
    """
    _, owner_timeouts = _owner_surface(repo)
    for node in ast.walk(m.tree):
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign):
            targets, value = [node.target], node.value
        else:
            continue
        scope = m.scope[id(node)]
        for target in targets:
            if not isinstance(target, ast.Name) or not target.id.lstrip("_").isupper():
                continue
            name = target.id
            label = name if scope == "<module>" else f"{scope}.{name}"
            number = m.const(value)
            if _is_retry_policy_name(name) or name == "DEFAULT_TIMEOUT":
                yield m.hit(node, label)
            elif name.endswith("_TIMEOUT") and number in owner_timeouts:
                yield m.hit(node, label)


def _is_retry_param(name: str) -> bool:
    return _is_retry_policy_name(name) or name == "timeout"


def _find_retry_param_defaults(m: _Module) -> Iterator[Hit]:
    """A default for a retry-policy parameter outside http_retry: any name
    carrying a retry token (``max_retries``, ``num_retries``, ``max_tries``,
    ``attempts``, ``retry_delay``, ``backoff``), or ``timeout``; whether the
    default is a literal, ``+3`` or a local name.

    The same T4a residue in signature form: a default in a caller's signature
    is a policy that silently overrides the owner's, and the API and CLI
    retyped the owner's 60-second timeout as literals (#1367 T4a audit,
    finding 3). Allowed: ``None`` ("let the owner decide"), a bool switch, a
    constant imported from the owner, and a ``timeout=`` naming a caller's
    ``*_TIMEOUT`` budget.
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
            if _is_retry_param(arg.arg) and not (
                _defers_to_owner(m, default) or _names_a_request_budget(arg, default)
            ):
                yield m.hit(arg, f"{scope}({arg.arg})")


def _is_retry_status(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and (value in (408, 429) or 500 <= value <= 599)
    )


def _status_like(node: ast.AST) -> bool:
    return bool({"status", "code"} & set(_identifier_tokens(node)))


def _status_set(m: _Module, node: ast.AST) -> bool:
    """A set/tuple/list whose known members are all HTTP statuses, with a
    retryable one among them. A member that cannot be resolved does not clear
    it (``{429, OTHER}``); a lone 5xx above 504 in a tuple does not count
    (``(512, 512)`` is a tile size)."""
    known = [v for v in (m.const(e) for e in node.elts) if v is not None]
    if not known or not all(
        isinstance(v, int) and not isinstance(v, bool) and 100 <= v <= 599 for v in known
    ):
        return False
    retryable = {v for v in known if _is_retry_status(v)}
    if not retryable:
        return False
    named = any(
        isinstance(e, ast.Attribute)
        and (m.dotted(e) or "").rsplit(".", 1)[0].endswith("HTTPStatus")
        for e in node.elts
    )
    return named or len(set(known)) >= 2 or bool(retryable & {408, 429, 500, 501, 502, 503, 504})


def _find_retry_status_sets(m: _Module) -> Iterator[Hit]:
    """Which HTTP statuses are worth another attempt, decided outside http_retry.

    A set, tuple or list of statuses holding 408, 429 or a 5xx (one element
    is enough: ``{429}``; members may be names or ``HTTPStatus`` members);
    ``range(500, 600)``; and a comparison: ``s == 429`` (also via a name bound
    to 429), ``status in (429, 503)``, ``500 <= s < 600``, ``code >= 500``,
    ``status // 100 == 5``. Comparisons with other 4xx codes (``== 401``) are
    error wording, not retry policy, and are not flagged.
    A private ``{408, 429}`` is a second classification (T4a residue, #1367).
    """
    for node in ast.walk(m.tree):
        if isinstance(node, (ast.Set, ast.Tuple, ast.List)) and node.elts:
            if _status_set(m, node):
                yield m.hit(node)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id == "range" and len(node.args) >= 2:
                start, stop = (m.const(a) for a in node.args[:2])
                if (
                    isinstance(start, int)
                    and isinstance(stop, int)
                    and 400 <= start < stop <= 600
                    and any(_is_retry_status(v) for v in range(start, stop))
                ):
                    yield m.hit(node)
        elif isinstance(node, ast.Compare):
            operands = [node.left, *node.comparators]
            numbers = {v for v in (m.const(o) for o in operands) if isinstance(v, int)}
            floor_100 = any(
                isinstance(o, ast.BinOp)
                and isinstance(o.op, ast.FloorDiv)
                and m.const(o.right) == 100
                for o in operands
            )
            if numbers & {408, 429}:
                yield m.hit(node)
            elif 500 in numbers and numbers & {599, 600}:
                yield m.hit(node)
            elif floor_100 and 5 in numbers:
                yield m.hit(node)
            elif any(500 <= n <= 599 for n in numbers) and any(map(_status_like, operands)):
                yield m.hit(node)


def _find_retry_after(m: _Module) -> Iterator[Hit]:
    """Reading the ``Retry-After`` header, in any case, outside ``core/http_retry.py``,
    including a header name built from pieces (``H + "-After"``).

    The owner parses both RFC 9110 forms and caps the wait; a caller that
    fetches the header itself is one step from parsing it itself (T4a residue).
    httpx headers are case-insensitive, so ``"retry-after"`` is the same read.
    """
    inside: set[int] = set()
    for node in ast.walk(m.tree):
        if isinstance(node, ast.BinOp) and id(node) not in inside:
            value = m.const(node)
            if isinstance(value, str) and "retry-after" in value.lower():
                inside.update(id(sub) for sub in ast.walk(node))
                yield m.hit(node)
    for node in m.strings():
        if id(node) not in inside and "retry-after" in node.value.lower():
            yield m.hit(node)


def _find_retry_wrappers(m: _Module, repo: Path) -> Iterator[Hit]:
    """A private layer over an owner entry point outside http_retry:

    * a function of three statements or fewer (docstring aside) that calls an
      owner function, however it is dressed (``cast(...)``, ``bytes(...)``,
      ``r = ...; return r``) and under any import alias -- the #1367 T4a
      audit's ``_get_shared_http_client`` pass-through -- and a lambda that does;
    * ``functools.partial(<owner function>, ...)``;
    * a call that passes its own numeric retry policy under any retry-token
      keyword (``max_retries=5``, ``num_retries=5``, ``retry_delay=2.0``) to any
      callee, or a literal ``timeout=`` to an owner function.
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
                0 < len(body) <= 3
                and not any(
                    isinstance(s, (ast.For, ast.While, ast.If, ast.Try, ast.With)) for s in body
                )
                and any(calls_owner(s) for s in body)
            ):
                yield m.hit(node, m.scope[id(node)] + ".<wrapper>")
        elif isinstance(node, ast.Lambda) and calls_owner(node.body):
            yield m.hit(node)
        elif isinstance(node, ast.Call) and _call_name(node) == "partial":
            target = node.args[0] if node.args else None
            if (isinstance(target, ast.Name) and target.id in owner_functions) or (
                isinstance(target, ast.Attribute) and target.attr in owner_functions
            ):
                yield m.hit(node)
        elif isinstance(node, ast.Call):
            # Retry counts and delays as literals at any call; a literal timeout
            # only at an owner call (subprocess and DuckDB take their own).
            owner_call = _call_name(node) in owner_functions

            def policy(kw: ast.keyword, owner_call: bool = owner_call) -> bool:
                value = m.const(kw.value)
                numeric = isinstance(value, (int, float)) and not isinstance(value, bool)
                if kw.arg and _is_retry_policy_name(kw.arg):
                    return numeric
                # A named *_TIMEOUT budget is the caller's; a literal is a copy.
                literal = isinstance(kw.value, (ast.Constant, ast.UnaryOp))
                return owner_call and kw.arg == "timeout" and numeric and literal

            if any(policy(kw) for kw in node.keywords):
                yield m.hit(node)


def _find_json_dumps(m: _Module) -> Iterator[Hit]:
    """``json.dumps``/``json.dump`` (however imported) in ``cli/`` and ``core/``:
    reports go through ``core/json_output.py``.

    ``dumps_json`` is strict JSON over values a bare ``json.dumps`` raises on
    (a DATE, a DECIMAL) or prints as the non-JSON ``NaN`` (#1226). The modules
    that serialize the geo block and other footer metadata are allowed per
    file in the exemptions (``count = "*"``): that is metadata, not a report.
    """
    for node in ast.walk(m.tree):
        if m.calls(node, "json.dumps", "json.dump"):
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
    "json_dumps": Arm(
        _find_json_dumps,
        ("geoparquet_io/core/json_output.py",),
        ("geoparquet_io/cli", "geoparquet_io/core"),
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
    "urllib.request": _HTTP_OWNER,
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
    hook = hook_entry("owner-only-defs")
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

_WHITELIST_LINE = re.compile(
    r"^(?P<name>[\w.]+)\s+# unused [\w ]+ \((?P<path>[^:()]+):\d+\) -- (?P<tag>dynamic|dead): \S.*$"
)
_WHITELISTS = ("whitelist", "tests_whitelist")


def _whitelist_lines(key: str) -> list[str]:
    whitelist = REPO / load_exemptions()["G4"][key]
    return [
        line
        for line in whitelist.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith((" ", '"', "#")) and "# unused" in line
    ]


def _whitelist_entries(key: str = "whitelist") -> list[tuple[str, str]]:
    return [
        (m["name"], m["path"])
        for line in _whitelist_lines(key)
        if (m := _WHITELIST_LINE.match(line))
    ]


def test_g4_vulture_runs_at_60_with_the_whitelist():
    """The package run is package-only: with tests/ on the path, a name only a
    test called counted as used (T0b audit)."""
    vulture = _pyproject()["tool"]["vulture"]
    assert vulture["min_confidence"] == 60
    assert vulture["paths"] == ["geoparquet_io", load_exemptions()["G4"]["whitelist"]]


def test_g4_the_tests_run_has_its_own_hook_and_whitelist():
    hook = hook_entry("vulture-tests")
    entry = hook["entry"]
    assert "tests" in entry.split()
    assert load_exemptions()["G4"]["tests_whitelist"] in entry
    assert "--min-confidence 60" in entry
    assert hook["stages"] == ["pre-push"]


@pytest.mark.parametrize("key", _WHITELISTS)
def test_g4_every_whitelist_entry_is_tagged_dynamic_or_dead(key):
    untagged = [line for line in _whitelist_lines(key) if not _WHITELIST_LINE.match(line)]
    assert not untagged, (
        "tag each line '-- dynamic: <how it is used>' or '-- dead: <track> deletes':\n  "
        + "\n  ".join(untagged)
    )


def _ast_identifiers(path: Path) -> set[str]:
    """Every identifier the module's AST binds or uses: names, attributes,
    def/class names, parameters, import aliases and keyword arguments. Text in
    comments, strings and docstrings does not count."""
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Name):
            found.add(node.id)
        elif isinstance(node, ast.Attribute):
            found.add(node.attr)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            found.add(node.name)
        elif isinstance(node, ast.arg):
            found.add(node.arg)
        elif isinstance(node, ast.alias):
            found.add((node.asname or node.name).split(".")[0])
            found.add(node.name.rsplit(".", 1)[-1])
        elif isinstance(node, ast.keyword) and node.arg:
            found.add(node.arg)
    return found


def stale_whitelist_entries(repo: Path, entries: list[tuple[str, str]]) -> list[str]:
    """Entries whose name no longer occurs in its file's AST."""
    cache: dict[str, set[str]] = {}
    stale = []
    for name, path in entries:
        source = repo / path
        if path not in cache:
            cache[path] = _ast_identifiers(source) if source.exists() else set()
        if name.rsplit(".", 1)[-1] not in cache[path]:
            stale.append(f"{path}: {name}")
    return stale


@pytest.mark.parametrize("key", _WHITELISTS)
def test_g4_every_whitelist_entry_still_names_something(key):
    """A whitelist line whose name is gone from its file is a stale exemption
    that a new finding of the same name would hide behind. Checked against
    the AST, so a comment or string still holding the name does not keep it."""
    stale = stale_whitelist_entries(REPO, _whitelist_entries(key))
    assert not stale, "delete these lines from the vulture whitelist:\n  " + "\n  ".join(stale)


# ---------------------------------------------------------------------------
# G3: near-duplicate functions (scripts/check_duplicates.py; pylint R0801)
# ---------------------------------------------------------------------------

CHECK_DUPLICATES = REPO / "scripts" / "check_duplicates.py"
CHECK_PYLINT_DUPLICATES = REPO / "scripts" / "check_pylint_duplicates.py"


def _script(path: Path):
    """Import a scripts/ module by path (scripts/ is not a package)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[path.stem] = module
    spec.loader.exec_module(module)
    return module


def _g3_baseline() -> list[str]:
    dups = _script(CHECK_DUPLICATES)
    return dups.read_baseline(REPO / load_exemptions()["G3"]["baseline"])


@functools.lru_cache(maxsize=1)
def _g3_pairs_on_the_tree() -> frozenset[str]:
    dups = _script(CHECK_DUPLICATES)
    return frozenset(p.key for p in dups.scan(dups.collect(REPO)))


def test_g3_the_tree_has_no_pair_outside_the_baseline():
    """A near-duplicate of an existing function (>= 8 statements, >= 0.90
    after renaming identifiers): call the existing function instead."""
    new = sorted(_g3_pairs_on_the_tree() - set(_g3_baseline()))
    assert not new, "new near-duplicate pairs:\n  " + "\n  ".join(new)


def test_g3_the_baseline_has_no_gone_pair():
    """The baseline only falls: a pair whose copy is gone leaves it
    (``check_duplicates.py --update``)."""
    gone = sorted(set(_g3_baseline()) - _g3_pairs_on_the_tree())
    assert not gone, "run scripts/check_duplicates.py --update:\n  " + "\n  ".join(gone)


def test_g3_every_baseline_pair_has_a_reason_and_only_those():
    pairs = load_exemptions()["G3"]["pairs"]
    reasons = {e["pair"]: e["reason"] for e in pairs}
    assert len(reasons) == len(pairs), "a pair is listed twice under [[G3.pairs]]"
    assert set(reasons) == set(_g3_baseline())
    assert all(r.strip() for r in reasons.values())
    assert len(set(_g3_baseline())) == len(_g3_baseline()), "a baseline line is repeated"


def test_g3_every_pylint_entry_has_a_reason():
    entries = load_exemptions()["G3"]["pylint"]
    assert all(e["reason"].strip() and "|" in e["block"] for e in entries)
    assert len({e["block"] for e in entries}) == len(entries)


def test_g3_hooks_run_both_layers():
    dups = hook_entry("check-duplicates")
    assert dups["entry"] == "uv run python scripts/check_duplicates.py"
    assert dups["types"] == ["python"] and dups["pass_filenames"] is False
    assert "stages" not in dups  # commit stage
    pylint = hook_entry("pylint-duplicates")
    assert pylint["entry"] == "uv run python scripts/check_pylint_duplicates.py"
    assert pylint["stages"] == ["pre-push"]


def test_g3_pylint_runs_r0801_alone_at_8_lines():
    tool = _pyproject()["tool"]["pylint"]
    assert tool["main"] == {"disable": ["all"], "enable": ["R0801"]}
    assert tool["similarities"] == {
        "min-similarity-lines": 8,
        "ignore-comments": True,
        "ignore-docstrings": True,
        "ignore-imports": True,
    }
    assert any(
        dep.startswith("pylint") for dep in _pyproject()["project"]["optional-dependencies"]["dev"]
    )


def test_g3_the_lint_job_runs_the_pre_push_stage():
    """pylint-duplicates is a pre-push hook, so CI runs it only through this step."""
    workflow = (REPO / ".github" / "workflows" / "tests.yml").read_text(encoding="utf-8")
    assert "pre-commit run --all-files --hook-stage pre-push" in " ".join(workflow.split())


# ---------------------------------------------------------------------------
# The counts, and the shape of the file
# ---------------------------------------------------------------------------


def _measured_counts() -> dict[str, int]:
    data = load_exemptions()
    counts = {f"G1.{arm}": len(data.get("G1", {}).get(arm, [])) for arm in ARMS}
    counts["G1.tid251"] = len(data["G1"]["tid251"])
    counts["G2.exemptions"] = len(data["G2"]["exemptions"])
    counts["G3.pairs"] = len(data["G3"]["pairs"])
    counts["G3.baseline"] = len(_g3_baseline())
    counts["G3.pylint"] = sum(e.get("count", 1) for e in data["G3"]["pylint"])
    counts["G13.options"] = len(data["G13"]["options"])
    counts["G13.bbox_parsers"] = len(data["G13"]["bbox_parsers"])
    counts["G4.whitelist"] = len(_whitelist_entries())
    counts["G4.tests_whitelist"] = len(_whitelist_entries("tests_whitelist"))
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
        if where.startswith(("G3.", "G13.")):  # pairs/options, not paths: own tests
            continue
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
    "retry_wrappers": (
        "def f(a):\n    if not a:\n        return None\n    rows = run_with_retry(a)\n"
        "    log(rows)\n    return [r for r in rows if r]\n"
    ),
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
    (
        "retry_param_defaults",
        "a local DEFAULT_TIMEOUT",
        "DEFAULT_TIMEOUT = 30\n\ndef f(timeout=DEFAULT_TIMEOUT):\n    pass\n",
    ),
    ("retry_param_defaults", "a local retry name", "_N = 4\n\ndef f(retries=_N):\n    pass\n"),
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
    ("retry_status_sets", "408 with a 4xx", "R = {408, 400}\n"),
    (
        "retry_wrappers",
        "literal to any helper",
        "def f(a):\n    x = 1\n    return fetch(a, max_retries=5)\n",
    ),
    (
        "retry_loop",
        "asyncio.sleep",
        "import asyncio\nasync def f(client):\n    for i in range(3):\n"
        "        client.get('u')\n        await asyncio.sleep(1)\n",
    ),
    (
        "retry_loop",
        "a DuckDB remote read",
        "import time\ndef f(q):\n    for i in range(3):\n"
        "        con = get_duckdb_connection()\n        con.sql(q)\n        time.sleep(1)\n",
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
    (
        "retry_param_defaults",
        "a named request budget",
        "CARTO_REQUEST_TIMEOUT = 120\n\ndef f(timeout=CARTO_REQUEST_TIMEOUT):\n    pass\n",
    ),
    ("retry_status_sets", "4xx wording", "def f(status):\n    return status == 401\n"),
    ("retry_status_sets", "a row bound", "def f(n):\n    return 10 <= n <= 500\n"),
    ("retry_status_sets", "a batch list", "B = [1000, 500, 100, 50]\n"),
    (
        "retry_wrappers",
        "a call with the caller's value",
        "def f(a, d):\n    for x in a:\n        log(x)\n    rows = run_with_retry(a, retry_delay=d)\n"
        "    log(rows)\n    return rows\n",
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
        ("import urllib.request\n", "urllib.request"),
        ("from urllib.request import urlopen\n", "urllib.request"),
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
    text = EXEMPTIONS_FILE.read_text(encoding="utf-8")
    sections = [
        f"[{key}]\n" + text.split(f"\n[{key}]\n", 1)[1].split("\n[", 1)[0] + "\n"
        for key in ("G2.owners", "G2.known_copies")
    ]
    (repo / "scripts" / "owner_exemptions.toml").write_text(
        "\n".join(sections) + f"\n{exemptions}", encoding="utf-8"
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


def test_g12_g2_a_leading_underscore_does_not_leave_the_family(tmp_path):
    """``_is_geographic_crs`` (wfs, quadkey) is an ``is_geographic*`` copy."""
    repo = _g2_tree(tmp_path, {"wfs2.py": "def _is_geographic_crs(crs):\n    return True\n"})
    result = _run_owner_defs("--repo", str(repo))
    assert result.returncode == 1 and "_is_geographic_crs" in result.stdout, result.stdout


@pytest.mark.parametrize(
    "name",
    ["_detect_version_from_table", "_crs_matches", "free_column_name", "_validated_bbox"],
)
def test_g12_g2_flags_a_known_renamed_copy(tmp_path, name):
    repo = _g2_tree(tmp_path, {"m.py": f"def {name}(x):\n    return x\n"})
    result = _run_owner_defs("--repo", str(repo))
    assert result.returncode == 1 and name in result.stdout, result.stdout


def test_g12_g2_fails_an_entry_whose_count_is_off(tmp_path):
    """A second copy under an exempted name is not hidden by the first's entry."""
    entry = (
        '[[G2.exemptions]]\npath = "geoparquet_io/core/m.py"\n'
        'name = "_is_geographic_crs"\nreason = "copy"\n'
    )
    two = "def _is_geographic_crs(c):\n    return 1\n\n\ndef _is_geographic_crs(c):\n    return 2\n"
    repo = _g2_tree(tmp_path, {"m.py": two}, entry)
    result = _run_owner_defs("--repo", str(repo))
    assert result.returncode == 1 and "the tree has 2" in result.stdout, result.stdout
    counted = _g2_tree(
        tmp_path / "two", {"m.py": two}, entry.replace("reason", "count = 2\nreason")
    )
    assert _run_owner_defs("--repo", str(counted)).returncode == 0


@pytest.mark.parametrize(
    ("name", "flagged"),
    [
        ("_bbox_covering_metadata", True),
        ("_covering_for", True),
        ("build_bbox_covering", True),
        ("check_bbox_covering", False),
        ("_has_covering", False),
    ],
)
def test_g12_g2_the_covering_family_is_the_builders(tmp_path, name, flagged):
    repo = _g2_tree(tmp_path, {"m.py": f"def {name}(x):\n    return x\n"})
    assert (_run_owner_defs("--repo", str(repo)).returncode == 1) is flagged


# The T0b audit's probe (scratchpad audit-t0b/probe.py): 31 shapes the first
# version of the G1 arms missed, each now a self-test.
G1_AUDIT_SHAPES = [
    (
        "retry_loop",
        "sleep in a helper the loop calls",
        "import time\ndef _backoff(i):\n    time.sleep(i)\ndef f(client):\n    for i in range(3):\n        client.get('u')\n        _backoff(i)\n",
    ),
    (
        "retry_loop",
        "sleep deep inside for/with/try/except/if",
        "import time\ndef f(client, lock):\n    for i in range(3):\n        with lock:\n            try:\n                return client.get('u')\n            except Exception:\n                if i < 2:\n                    time.sleep(i)\n",
    ),
    (
        "retry_loop",
        "request in a helper, sleep in the loop",
        "import time\ndef _once(c):\n    return c.get('u')\ndef f(c):\n    for i in range(3):\n        r = _once(c)\n        if r: return r\n        time.sleep(1)\n",
    ),
    (
        "retry_loop",
        "sleep in an inner loop",
        "import time, httpx\ndef f(url):\n    while True:\n        r = httpx.get(url)\n        for _ in range(2):\n            time.sleep(1)\n",
    ),
    (
        "retry_status_sets",
        "HTTPStatus members",
        "from http import HTTPStatus\nR = {HTTPStatus.TOO_MANY_REQUESTS, HTTPStatus.SERVICE_UNAVAILABLE}\n",
    ),
    ("retry_status_sets", "range(500, 600)", "def f(s):\n    return s in range(500, 600)\n"),
    (
        "retry_status_sets",
        "status // 100 == 5",
        "def f(resp):\n    return resp.status_code // 100 == 5\n",
    ),
    (
        "retry_status_sets",
        "429 through a name",
        "TOO_MANY = 429\ndef f(s):\n    return s == TOO_MANY\n",
    ),
    ("retry_status_sets", "a set with a non-literal member", "R = {429, OTHER}\n"),
    ("retry_status_sets", "codes above 511", "R = (520, 599)\n"),
    ("retry_constants", "renamed _N_TRIES", "_N_TRIES = 5\n"),
    ("retry_constants", "class attribute", "class Client:\n    MAX_RETRIES = 5\n"),
    (
        "retry_constants",
        "function local",
        "def f():\n    MAX_RETRIES = 5\n    return MAX_RETRIES\n",
    ),
    ("retry_constants", "DEFAULT_TIMEOUT in a class", "class C:\n    DEFAULT_TIMEOUT = 60\n"),
    (
        "retry_after",
        "header built from two strings",
        "H = 'Retry'\ndef g(r):\n    return r.headers.get(H + '-After')\n",
    ),
    ("retry_param_defaults", "num_retries=5", "def f(num_retries=5):\n    pass\n"),
    ("retry_param_defaults", "max_tries=5", "def f(max_tries=5):\n    pass\n"),
    (
        "retry_wrappers",
        "two-statement wrapper",
        "def w(url):\n    r = make_request_with_retry('GET', url)\n    return r\n",
    ),
    (
        "retry_wrappers",
        "lambda wrapper",
        "w = lambda url: make_request_with_retry('GET', url, timeout=5)\n",
    ),
    (
        "retry_wrappers",
        "num_retries= literal",
        "def f(a):\n    g(a, num_retries=5)\n    h(a)\n    i(a)\n    j(a)\n",
    ),
    (
        "geo_parse",
        "key fetched one assignment back",
        "import json\ndef r(md):\n    raw = md[b'geo']\n    return json.loads(raw)\n",
    ),
    (
        "geo_parse",
        "key fetched two assignments back",
        "import json\ndef r(con):\n    row = con.execute(\"SELECT value WHERE key = 'geo'\").fetchone()\n    text = row[0].decode()\n    return json.loads(text)\n",
    ),
    (
        "geo_parse",
        "from json import loads",
        "from json import loads\ndef r(md):\n    return loads(md[b'geo'])\n",
    ),
    (
        "geo_parse",
        "method in a class",
        "import json\nclass A:\n    def r(self, md):\n        if md:\n            return json.loads(md.get(b'geo'))\n",
    ),
    ("sql_escape", "str.replace(v, ...)", 'def e(v):\n    return str.replace(v, "\'", "\'\'")\n'),
    ("sql_escape", "quote in a name", 'Q = "\'"\ndef e(v):\n    return v.replace(Q, Q * 2)\n'),
    ("sql_escape", "chr(39)", "def e(v):\n    return f\"'{v.replace(chr(39), chr(39)*2)}'\"\n"),
    (
        "duckdb_version",
        "from duckdb import __version__",
        "from duckdb import __version__ as V\nNEW = V.startswith('2')\n",
    ),
    ("duckdb_version", "import duckdb as ddb", "import duckdb as ddb\nNEW = ddb.__version__\n"),
    (
        "duckdb_version",
        "importlib.metadata",
        "from importlib.metadata import version\nNEW = version('duckdb')\n",
    ),
    (
        "set_memory_limit",
        "PRAGMA memory_limit",
        "def c(con):\n    con.execute(\"PRAGMA memory_limit='4GB'\")\n",
    ),
    (
        "set_memory_limit",
        "connect(config=)",
        "import duckdb\ncon = duckdb.connect(config={'memory_limit': '4GB'})\n",
    ),
    (
        "set_memory_limit",
        "SET max_memory",
        "def c(con):\n    con.execute(\"SET max_memory = '4GB'\")\n",
    ),
    (
        "json_dumps",
        "from json import dumps",
        "from json import dumps\ndef s(d):\n    print(dumps(d))\n",
    ),
    (
        "json_dumps",
        "json.dump to stdout",
        "import json, sys\ndef s(d):\n    json.dump(d, sys.stdout)\n",
    ),
]

#: Shapes close to the above that are not the pattern.
G1_AUDIT_NEAR_MISSES = [
    (
        "set_memory_limit",
        "memory_limit as a writer kwarg",
        "def kw(o):\n    return {'memory_limit': o.memory_limit, 'profile': o.profile}\n",
    ),
    (
        "retry_wrappers",
        "a named *_TIMEOUT budget to an owner call",
        "_PAGE_TIMEOUT = 600\ndef f(u):\n    c = get_shared_http_client(timeout=_PAGE_TIMEOUT)\n"
        "    for x in u:\n        log(x)\n    return c\n",
    ),
    (
        "geo_parse",
        "a variable of the same name in another function",
        "import json\ndef a(md):\n    raw = md[b'geo']\n    return raw\ndef b(f):\n    raw = f.read()\n    return json.loads(raw)\n",
    ),
    ("retry_status_sets", "a tile size", "SIZE = (512, 512)\n"),
    ("retry_param_defaults", "a bool switch", "def f(retry=False):\n    pass\n"),
    ("retry_constants", "a lowercase local", "def f():\n    retries = 3\n    return retries\n"),
]


@pytest.mark.parametrize(
    ("arm_name", "label", "source"),
    G1_AUDIT_SHAPES,
    ids=[f"{a}-{lbl}" for a, lbl, _ in G1_AUDIT_SHAPES],
)
def test_g12_the_g1_arms_catch_every_shape_the_t0b_audit_found(tmp_path, arm_name, label, source):
    repo = _plant(tmp_path, arm_name, source)
    assert arm_hits(repo, arm_name), f"{arm_name} missed the {label} shape:\n{source}"


@pytest.mark.parametrize(
    ("arm_name", "label", "source"),
    G1_AUDIT_NEAR_MISSES,
    ids=[f"{a}-{lbl}" for a, lbl, _ in G1_AUDIT_NEAR_MISSES],
)
def test_g12_the_g1_arms_pass_the_t0b_near_misses(tmp_path, arm_name, label, source):
    repo = _plant(tmp_path, arm_name, source)
    assert arm_hits(repo, arm_name) == [], f"{arm_name} flagged {label}"


def test_g12_a_star_count_allows_a_whole_module_but_not_none(tmp_path):
    entry = {"path": "geoparquet_io/m.py", "count": "*", "reason": "r"}
    hits = [Hit("geoparquet_io/m.py", n, "f", "x") for n in (1, 2, 3)]
    assert reconcile(hits, [entry]) == ([], [])
    assert reconcile([], [entry])[1], "a '*' entry matching nothing is stale"


def test_g12_the_whitelist_stale_check_reads_the_ast_not_the_text(tmp_path):
    module = tmp_path / "geoparquet_io" / "m.py"
    module.parent.mkdir()
    module.write_text("# old_helper was here\nNOTE = 'old_helper'\n\ndef kept():\n    pass\n")
    entries = [("old_helper", "geoparquet_io/m.py"), ("kept", "geoparquet_io/m.py")]
    assert stale_whitelist_entries(tmp_path, entries) == ["geoparquet_io/m.py: old_helper"]


_TEN_STATEMENTS = """
def {name}(rows, limit):
    total = 0
    seen = set()
    for row in rows:
        if row in seen:
            continue
        seen.add(row)
        total += row
        if total > limit:
            break
    result = [total, len(seen)]
    return result
"""


def _g3_tree(tmp_path: Path, files: dict[str, str]) -> Path:
    for rel, text in files.items():
        target = tmp_path / "geoparquet_io" / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    return tmp_path


def _run_check_duplicates(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CHECK_DUPLICATES), "--repo", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )


_G3_KEY = "geoparquet_io/a.py:first|geoparquet_io/b.py:second"


def test_g12_g3_flags_two_functions_that_differ_only_in_names(tmp_path):
    second = _TEN_STATEMENTS.format(name="second").replace("total", "acc").replace("seen", "done")
    repo = _g3_tree(tmp_path, {"a.py": _TEN_STATEMENTS.format(name="first"), "b.py": second})
    result = _run_check_duplicates(repo)
    assert result.returncode == 1, result.stdout
    assert _G3_KEY in result.stdout


def test_g12_g3_a_baselined_pair_passes(tmp_path):
    second = _TEN_STATEMENTS.format(name="second").replace("total", "acc")
    repo = _g3_tree(tmp_path, {"a.py": _TEN_STATEMENTS.format(name="first"), "b.py": second})
    baseline = tmp_path / "baseline.txt"
    baseline.write_text(f"# reason\n{_G3_KEY}\n", encoding="utf-8")
    assert _run_check_duplicates(repo, "--baseline", str(baseline)).returncode == 0


def test_g12_g3_a_gone_pair_warns_and_update_drops_it_but_never_adds(tmp_path):
    repo = _g3_tree(tmp_path, {"a.py": _TEN_STATEMENTS.format(name="first")})
    baseline = tmp_path / "baseline.txt"
    baseline.write_text(f"# keep me\n{_G3_KEY}\n", encoding="utf-8")
    warned = _run_check_duplicates(repo, "--baseline", str(baseline))
    assert warned.returncode == 0 and "warning: baseline pair gone" in warned.stdout
    assert _run_check_duplicates(repo, "--baseline", str(baseline), "--update").returncode == 0
    assert baseline.read_text(encoding="utf-8") == "# keep me\n"
    (repo / "geoparquet_io" / "b.py").write_text(
        _TEN_STATEMENTS.format(name="second"), encoding="utf-8"
    )
    added = _run_check_duplicates(repo, "--baseline", str(baseline), "--update")
    assert added.returncode == 1 and baseline.read_text(encoding="utf-8") == "# keep me\n"


def test_g12_g3_short_or_different_functions_are_not_pairs(tmp_path):
    short = "def first(a):\n    b = a\n    return b\n"
    other = "def second(xs):\n" + "".join(f"    print('step {i}', xs[{i}])\n" for i in range(10))
    repo = _g3_tree(
        tmp_path,
        {
            "a.py": short,
            "b.py": short.replace("first", "second"),
            "c.py": other,
            "d.py": _TEN_STATEMENTS.format(name="third"),
        },
    )
    assert _run_check_duplicates(repo).returncode == 0


_PYLINT_REPORT = """************* Module geoparquet_io.x
geoparquet_io/x.py:1:0: R0801: Similar lines in 2 files
==geoparquet_io.x:[1:12]
==geoparquet_io.y:[3:14]
    a = 1
"""


def _pylint_tree(tmp_path: Path, y_source: str) -> Path:
    pkg = tmp_path / "geoparquet_io"
    pkg.mkdir(exist_ok=True)
    (pkg / "x.py").write_text("def f():\n" + "    a = 1\n" * 14, encoding="utf-8")
    (pkg / "y.py").write_text(y_source, encoding="utf-8")
    return tmp_path


_Y_COPY_IN_G = "def g():\n" + "    a = 1\n" * 14
_Y_COPY_IN_H = "def g():\n    return 0\ndef h():\n" + "    a = 1\n" * 12


def _run_pylint_wrapper(
    tmp_path: Path, pinned: str, y_source: str = _Y_COPY_IN_G
) -> subprocess.CompletedProcess[str]:
    repo = _pylint_tree(tmp_path, y_source)
    report = tmp_path / "report.txt"
    report.write_text(_PYLINT_REPORT, encoding="utf-8")
    toml = tmp_path / "owner_exemptions.toml"
    toml.write_text(pinned, encoding="utf-8")
    return subprocess.run(
        [
            sys.executable,
            str(CHECK_PYLINT_DUPLICATES),
            "--repo",
            str(repo),
            "--report",
            str(report),
            "--exemptions",
            str(toml),
        ],
        capture_output=True,
        text=True,
        check=False,
    )


_PINNED_BLOCK = '[[G3.pylint]]\nblock = "x:f|y:g"\nreason = "r"\n'


def test_g12_g3_pylint_wrapper_fails_an_unpinned_pair_and_passes_a_pinned_one(tmp_path):
    assert _run_pylint_wrapper(tmp_path, "").returncode == 1
    assert _run_pylint_wrapper(tmp_path, _PINNED_BLOCK).returncode == 0
    stale = _PINNED_BLOCK + '[[G3.pylint]]\nblock = "p:a|q:b"\nreason = "r"\n'
    result = _run_pylint_wrapper(tmp_path, stale)
    assert result.returncode == 1 and "stale" in result.stdout


def test_g12_g3_pylint_wrapper_fails_a_copy_swapped_between_the_same_modules(tmp_path):
    """The pin names the functions, so trading the copy in y.g for one in y.h fails."""
    result = _run_pylint_wrapper(tmp_path, _PINNED_BLOCK, _Y_COPY_IN_H)
    assert result.returncode == 1, result.stdout
    assert "x:f|y:h" in result.stdout


_BODY = """
    total = 0
    seen = set()
    for row in rows:
        if row in seen:
            continue
        seen.add(row)
        total += row
        if total > limit:
            break
    return [total, len(seen)]
"""


def test_g12_g3_the_signature_does_not_shift_the_numbering(tmp_path):
    """Byte-identical bodies under signatures that differ by an annotation, a
    default and an extra leading parameter are one pair (the T0c audit's
    normaliser defect hid the stats twins this way)."""
    repo = _g3_tree(
        tmp_path,
        {
            "a.py": "def first(rows, limit):" + _BODY,
            "b.py": "def second(ctx, rows: list[int], limit: int = 5) -> list:" + _BODY,
        },
    )
    result = _run_check_duplicates(repo)
    assert result.returncode == 1 and _G3_KEY in result.stdout, result.stdout
    assert "(1.00)" in result.stdout


def test_g12_g3_update_refuses_to_write_on_a_failing_run(tmp_path):
    second = _TEN_STATEMENTS.format(name="second")
    repo = _g3_tree(tmp_path, {"a.py": _TEN_STATEMENTS.format(name="first"), "b.py": second})
    baseline = tmp_path / "baseline.txt"
    original = "geoparquet_io/a.py:gone|geoparquet_io/b.py:gone\n"
    baseline.write_text(original, encoding="utf-8")
    result = _run_check_duplicates(repo, "--baseline", str(baseline), "--update")
    assert result.returncode == 1 and "not updating" in result.stdout
    assert baseline.read_text(encoding="utf-8") == original
