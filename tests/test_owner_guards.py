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

from geoparquet_io.core.parquet_writer import DEFAULT_ROW_GROUP_ROWS
from tests._precommit_hook import core_tree, hook_entry

REPO = Path(__file__).resolve().parents[1]
EXEMPTIONS_FILE = REPO / "scripts" / "owner_exemptions.toml"
CHECK_OWNER_DEFS = REPO / "scripts" / "check_owner_defs.py"

#: The pinned number of exemption entries per check (G1 per arm). Lower it with
#: the commit that deletes a copy; raising it needs a reason in review.
EXPECTED_COUNTS = {
    "G1.tid251": 13,
    "G1.geo_parse": 0,
    "G1.set_memory_limit": 0,
    "G1.sql_escape": 0,
    "G1.sql_escape_hand_quote": 0,
    "G1.cgroup": 0,
    "G1.duckdb_version": 0,
    "G1.retry_loop": 0,
    "G1.retry_constants": 0,
    "G1.retry_param_defaults": 0,
    "G1.retry_status_sets": 2,
    "G1.retry_after": 0,
    "G1.retry_wrappers": 0,
    "G1.json_dumps": 34,
    "G1.parquet_metadata": 0,
    "G1.row_group_policy": 2,
    "G1.scheme_test": 0,
    "G1.overwrite_check": 0,
    "G1.output_version": 0,
    "G1.geometry_stats": 0,
    "G1.edges": 0,
    "G1.column_fold": 0,
    "G1.footer_patch": 0,
    "G1.geometry_identity": 9,
    "G2.exemptions": 9,
    "G3.pairs": 0,
    "G3.baseline": 0,
    "G3.pylint": 18,
    "G13.options": 11,
    "G13.bbox_parsers": 0,
    "G4.whitelist": 175,
    "G4.tests_whitelist": 95,
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


def _identifier_tokens(node: ast.AST, skip: frozenset[int] = frozenset()) -> Iterator[str]:
    for sub in ast.walk(node):
        if id(sub) in skip:
            continue
        if isinstance(sub, ast.Name):
            yield from sub.id.lower().split("_")
        elif isinstance(sub, ast.Attribute):
            yield from sub.attr.lower().split("_")


def _mentions_geo(node: ast.AST) -> bool:
    """The ``geo`` key by constant (``b"geo"``, or ``key = 'geo'`` in a footer
    query) or by an identifier token (``geo_bytes``).

    A callee's own name is not read: ``parse_duckdb_geo_type(t)`` is a call whose
    result is whatever that function returns, not the footer key. (Only this arm
    skips callees; the retry arms read a callee's name on purpose.)
    """
    for sub in ast.walk(node):
        if isinstance(sub, ast.Constant) and sub.value in ("geo", b"geo"):
            return True
        if isinstance(sub, ast.Constant) and isinstance(sub.value, str) and "'geo'" in sub.value:
            return True
    callees = frozenset(id(sub.func) for sub in ast.walk(node) if isinstance(sub, ast.Call))
    return "geo" in set(_identifier_tokens(node, callees))


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


def _find_sql_escape_hand_quote(m: _Module) -> Iterator[Hit]:
    """``_escape_sql_string`` reached from outside duckdb_utils: imported (under
    any alias) or read as ``duckdb_utils._escape_sql_string``.

    It is the bare escape under ``sql_literal`` and ``sql_path``; a caller of it
    has to supply the quotes by hand (``f"'{_escape_sql_string(v)}'"``, or the
    escaped value quoted lines later), and gets no NUL handling. Would have
    caught the ~45 hand-quoted literals that bypassed ``sql_literal`` (#1367
    T1e audit, finding 9).

    The same escape reached the other way round is flagged too: a subscript of
    a ``sql_path(...)`` or ``sql_literal(...)`` call -- ``sql_path(p)[1:-1]``,
    the literal with its quotes cut off so a template can put them back -- is
    a bare escape by another name, and with a NUL byte (``'a' || chr(0) ||
    'b'``) is not even one quoted span (#1367 T1d+T1e audit 2, finding 4).
    """
    name = "_escape_sql_string"
    for node in ast.walk(m.tree):
        if isinstance(node, ast.ImportFrom) and any(a.name == name for a in node.names):
            yield m.hit(node)
        elif isinstance(node, ast.Attribute) and node.attr == name:
            yield m.hit(node)
        elif isinstance(node, ast.Subscript) and _calls_a_literal_builder(node.value):
            yield m.hit(node)


#: The owners whose result is a whole SQL literal, never to be cut into.
_LITERAL_BUILDERS = frozenset({"sql_literal", "sql_path"})


def _calls_a_literal_builder(node: ast.AST) -> bool:
    """``sql_path(...)`` / ``duckdb_utils.sql_literal(...)``: the call itself."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
    return name in _LITERAL_BUILDERS


def _find_cgroup(m: _Module) -> Iterator[Hit]:
    """A ``/sys/fs/cgroup`` path outside ``core/memory_limits.py``.

    Would have caught a second ceiling detector (#1156, #1174): the cgroup cap
    is read in one place, so every connection agrees on the ceiling.
    """
    for node in m.strings():
        if "/sys/fs/cgroup" in node.value:
            yield m.hit(node)


_PARQUET_METADATA_CALL = re.compile(r"(?i)\bparquet_metadata\s*\(")


def _find_parquet_metadata(m: _Module) -> Iterator[Hit]:
    """A ``parquet_metadata(...)`` table-function call outside ``core/duckdb_metadata.py``.

    Would have caught the per-row-group readers of #1367 concept 17 (T1c audit):
    ``metadata_utils._get_num_rows_per_row_group`` and validate's ``geo_types``
    query each opened their own connection over ``parquet_metadata()`` beside
    the owner's ``_parquet_metadata_rows`` runner and its typed views.
    """
    for node in m.strings():
        if _PARQUET_METADATA_CALL.search(node.value):
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


#: The row-group count's owner: the default and every resolver live there.
_ROW_GROUP_OWNER = ("geoparquet_io/core/parquet_writer.py",)
_ROW_GROUP_NAME = re.compile(r"(?i)row_?group|batch_size")
_COPY_ROW_GROUP_LITERAL = re.compile(r"(?i)ROW_GROUP_SIZE[\s=]+\d+")
#: Row counts that have been some writer's default: gpio's, 50,000 and
#: 100,000 (two old gpio copies) and DuckDB's own 122,880.
_ROW_GROUP_DEFAULTS = (DEFAULT_ROW_GROUP_ROWS, 50_000, 100_000, 122_880)


def _row_group_default_literal(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Constant)
        and type(node.value) is int
        and node.value in _ROW_GROUP_DEFAULTS
    )


def _row_group_fallback(node: ast.AST) -> bool:
    """An int literal or a CONSTANT name: what ``rows or <this>`` falls back to."""
    if isinstance(node, ast.Constant):
        return type(node.value) is int
    name = node.id if isinstance(node, ast.Name) else getattr(node, "attr", "")
    return bool(name) and name.isupper()


def _names_row_groups(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return bool(_ROW_GROUP_NAME.search(node.id))
    if isinstance(node, ast.Attribute):
        return bool(_ROW_GROUP_NAME.search(node.attr))
    if isinstance(node, ast.Call):
        return _names_row_groups(node.func)
    return False


def _find_row_group_policy(m: _Module) -> Iterator[Hit]:
    """A row-group count decided outside ``parquet_writer`` (#981, #1030, #1215).

    The default typed as a literal; a literal ``ROW_GROUP_SIZE n`` in a COPY;
    ``row_group_size=<literal>``; a second default after ``or``
    (``rows or DEFAULT_BATCH_SIZE``, ``self.row_group_rows or 50000``); and a
    row-group-named constant bound to a past default. Each was a copy that
    wrote its own number: 100,000 in the pyarrow settings, ``DEFAULT_BATCH_SIZE``
    in the streaming strategy, the input's average in ``add bbox-metadata``.
    """
    strings = {id(node) for node in m.strings()}
    for node in ast.walk(m.tree):
        if _row_group_policy_node(node, strings):
            yield m.hit(node)


def _row_group_policy_node(node: ast.AST, strings: set[int]) -> bool:
    if isinstance(node, ast.Constant):
        if type(node.value) is int:
            return node.value == DEFAULT_ROW_GROUP_ROWS
        return id(node) in strings and bool(_COPY_ROW_GROUP_LITERAL.search(node.value))
    if isinstance(node, ast.keyword):
        return node.arg == "row_group_size" and isinstance(node.value, ast.Constant)
    if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
        first, *rest = node.values
        return _names_row_groups(first) and any(_row_group_fallback(r) for r in rest)
    if isinstance(node, (ast.Assign, ast.AnnAssign)):
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        return any(_names_row_groups(t) for t in targets) and _row_group_default_literal(node.value)
    return False


#: The output version's owners: the decision in ``parquet_writer``, the version
#: table (``GEOPARQUET_VERSIONS``) and the carried-block decoder in ``geo_metadata``.
_VERSION_OWNERS = ("geoparquet_io/core/parquet_writer.py", "geoparquet_io/core/geo_metadata.py")
#: Every spelling of a GeoParquet version a write can declare or be asked for.
_VERSION_SPELLINGS = frozenset(
    {"1.0", "1.1", "2.0", "1.1-geoarrow", "parquet-geo-only", "1.0.0", "1.1.0", "2.0.0"}
)
#: The write-path decoders of a carried ``geo`` block (T3a): a write reads its
#: input's version off one of these, and only the owner may.
_CARRIED_GEO_DECODERS = ("sanitized_carried_geo", "sanitized_carried_geo_of_file")


#: What an output version is called on a write path: the request, the answer.
_OUTPUT_VERSION_NAMES = frozenset({"geoparquet_version", "effective_version", "output_version"})
#: Keys that make a dict literal a ``geo`` block: composing one is T3e's arm
#: (``geo_metadata`` composes every block), not a version decision.
_GEO_BLOCK_KEYS = frozenset({"primary_column", "columns"})


def _names_an_output_version(node: ast.AST) -> bool:
    """``geoparquet_version``, ``self.effective_version``, or a ``resolve_*version*(...)`` call."""
    if isinstance(node, ast.Name):
        return node.id in _OUTPUT_VERSION_NAMES
    if isinstance(node, ast.Attribute):
        return node.attr in _OUTPUT_VERSION_NAMES
    if isinstance(node, ast.Call):
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
        return fnmatchcase(name.lstrip("_"), "resolve_*version*")
    return False


def _in_geo_block_literal(m: _Module, node: ast.AST) -> bool:
    up = m.parents.get(id(node))
    while up is not None:
        if isinstance(up, ast.Dict) and any(
            isinstance(k, ast.Constant) and k.value in _GEO_BLOCK_KEYS for k in up.keys
        ):
            return True
        up = m.parents.get(id(up))
    return False


def _version_fallback(node: ast.AST) -> bool:
    """A version spelled out, or ``DEFAULT_GEOPARQUET_VERSION``: a second default."""
    if isinstance(node, ast.Constant):
        return node.value in _VERSION_SPELLINGS
    name = node.id if isinstance(node, ast.Name) else getattr(node, "attr", "")
    return name == "DEFAULT_GEOPARQUET_VERSION"


def _is_version_key(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value == "version"


def _decodes_carried_geo(m: _Module, node: ast.AST) -> bool:
    """*node* is a write-path decoder call, or a name the enclosing function bound to one."""
    if isinstance(node, ast.Call):
        return (m.dotted(node.func) or "").rsplit(".", 1)[-1] in _CARRIED_GEO_DECODERS
    if isinstance(node, ast.Name):
        values = m.local_bindings(node).get(node.id, [])
        return any(_decodes_carried_geo(m, v) for v in values if not isinstance(v, ast.Name))
    return False


#: The spelled-out metadata versions a block declares.
_METADATA_VERSIONS = frozenset({"1.0.0", "1.1.0", "2.0.0"})
#: The writer's own version keys (``--geoparquet-version`` values). Not
#: ``parquet-geo-only``: bound to a geometry-free write it names "no ``geo``
#: key", which ``apply_output_kv_metadata`` owns, not a version.
_SHORT_VERSIONS = _VERSION_SPELLINGS - _METADATA_VERSIONS - {"parquet-geo-only"}
#: Prefixes a declared version is sniffed with.
_VERSION_PREFIXES = frozenset({"1.", "2.", "1.0", "1.1", "2.0"})
#: Names a decoded ``geo`` block goes by.
_GEO_BLOCK_NAME = re.compile(r"(?i)geo|meta|block")


def _names_a_version(node: ast.AST) -> bool:
    """A name or attribute with ``version`` in it, a ``resolve_*version*()`` call,
    or ``x.get("<...version...>")``."""
    if _names_an_output_version(node):
        return True
    if isinstance(node, ast.Name):
        return "version" in node.id.lower()
    if isinstance(node, ast.Attribute):
        return "version" in node.attr.lower()
    if _is_get_call(node):
        key = node.args[0]
        return isinstance(key, ast.Constant) and "version" in str(key.value).lower()
    return False


def _names_a_geo_block(node: ast.AST) -> bool:
    return isinstance(node, ast.Name) and bool(_GEO_BLOCK_NAME.search(node.id))


def _is_get_call(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and bool(node.args)
    )


def _is_short_version(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value in _SHORT_VERSIONS


def _find_output_version(m: _Module) -> Iterator[Hit]:
    """An output GeoParquet version decided outside ``parquet_writer`` (#1367 concept 1).

    ``resolve_output_geoparquet_version`` is the one decision: explicit request,
    then the input file, then the carried ``geo`` key, then the table's schema,
    then the default. Its copies took one of three shapes. A read of the
    carried block's ``version`` off a write-path decoder
    (``sanitized_carried_geo(md).get("version")`` -- ``add bbox``'s
    pass-through, which kept a 1.0 input at 1.0, and the table detector the
    strategies each called), whatever wraps it; and a second default after
    ``or`` (``resolve(...) or "1.1"`` in ``extract``,
    ``... or DEFAULT_GEOPARQUET_VERSION`` in ``sort hilbert`` and
    ``Table.write``) or in ``.get("version", "1.1.0")``.

    Reporting the *input's* declared version is not a write decision: ``gpio
    check`` reads it through ``read_raw_geo``/``raw_geo_of_file``, and ``gpio
    inspect`` or a write command naming it in a refusal or advice asks the
    owner's ``declared_geoparquet_version``. WFS and stac-geoparquet protocol versions
    pass too.
    A ``version`` inside a ``geo`` block literal (one with ``primary_column`` or
    ``columns``) is block composition, which T3e's arm owns.

    The first audit added the shapes it found passing at 0 (``version or
    "1.1"``, a ``["version"]`` read on a geo-named block, ``GEOPARQUET_VERSIONS
    .get``, a ``metadata_version`` fallback, a version literal bound to a
    ``*version*`` name or parameter default, a GeoParquet version compared with
    a literal); the second, the two shapes this PR deleted: a file-type ->
    version map (a dict literal, or an ``if file_type == ...: return "2.0"``
    chain -- ``check_fixes`` and ``validate`` each had one) and a
    ``geoparquet_version="1.1"`` keyword literal at a call.

    The comparison shape is a name heuristic and stays one: it counts only an
    operand whose name says both ``geo`` and ``version``, so WFS's and
    stac-geoparquet's ``version == "1.0.0"`` and ``gpio check``'s spec
    comparisons of the declared version pass. A copy that compares under a
    bare ``version`` name is not caught by this shape.
    """
    for node in ast.walk(m.tree):
        if _output_version_node(m, node) and not _in_geo_block_literal(m, node):
            yield m.hit(node)


def _output_version_node(m: _Module, node: ast.AST) -> bool:
    return any(shape(m, node) for shape in _OUTPUT_VERSION_SHAPES)


def _version_or_fallback(m: _Module, node: ast.AST) -> bool:
    """``geoparquet_version or "1.1"``, ``version or DEFAULT_GEOPARQUET_VERSION``,
    ``info.get("geo_version") or "1.0.0"``: a second default."""
    if not (isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or)):
        return False
    first, *rest = node.values
    return _names_a_version(first) and any(_version_fallback(r) for r in rest)


def _version_key_read(m: _Module, node: ast.AST) -> bool:
    """``block["version"]`` off a decoder or a geo-named block (a parameter
    included), and ``.get("version")`` off a write-path decoder."""
    if isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Load):
        return _is_version_key(node.slice) and (
            _decodes_carried_geo(m, node.value) or _names_a_geo_block(node.value)
        )
    if not (_is_get_call(node) and _is_version_key(node.args[0])):
        return False
    return _decodes_carried_geo(m, node.func.value)


def _version_get_default(m: _Module, node: ast.AST) -> bool:
    """``.get("version", "1.1.0")``, ``.get("metadata_version", "1.1.0")``,
    ``GEOPARQUET_VERSIONS.get(v, ...)``: a version an unknown key falls back to."""
    if not _is_get_call(node):
        return False
    receiver = node.func.value
    if (m.dotted(receiver) or "").rsplit(".", 1)[-1] == "GEOPARQUET_VERSIONS":
        return True
    key = node.args[0]
    keyed = isinstance(key, ast.Constant) and key.value in ("version", "metadata_version")
    return keyed and len(node.args) > 1 and _version_fallback(node.args[1])


def _version_literal_bound(m: _Module, node: ast.AST) -> bool:
    """A version spelled out as a decision: assigned to a version name, or the
    default of a ``*version*`` parameter."""
    if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        return any(_names_a_version(t) for t in targets) and _is_short_version(node.value)
    if isinstance(node, ast.arguments):
        positional = node.posonlyargs + node.args
        pairs = list(
            zip(positional[len(positional) - len(node.defaults) :], node.defaults, strict=True)
        )
        pairs += [
            (a, d) for a, d in zip(node.kwonlyargs, node.kw_defaults, strict=True) if d is not None
        ]
        return any("version" in a.arg.lower() and _is_short_version(d) for a, d in pairs)
    return False


def _version_literal_compared(m: _Module, node: ast.AST) -> bool:
    """``geo_version == "1.0.0"``, ``geo_version.startswith("1.")``: a GeoParquet
    version tested against a spelling, which is the owner's map
    (``_version_for_file_type``, ``writable_geoparquet_version``).

    Only an operand named for GeoParquet (``geo``/``geoparquet`` in the name)
    counts: a bare ``version`` there is WFS's or stac-geoparquet's protocol
    version, and ``gpio check``/``validate`` test the version an input
    declares against the spec under their own names.
    """
    if isinstance(node, ast.Compare):
        sides = (node.left, *node.comparators)
        literal = any(isinstance(x, ast.Constant) and x.value in _METADATA_VERSIONS for x in sides)
        return literal and any(_names_a_geo_version(x) for x in sides)
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "startswith"
        and node.args
    ):
        arg = node.args[0]
        prefix = isinstance(arg, ast.Constant) and arg.value in _VERSION_PREFIXES
        return prefix and _names_a_geo_version(node.func.value)
    return False


def _names_a_geo_version(node: ast.AST) -> bool:
    if isinstance(node, ast.BoolOp):  # (geo_version or "").startswith(...)
        return _names_a_geo_version(node.values[0])
    if _is_get_call(node) or isinstance(node, ast.Subscript):  # info["geo_version"] == ...
        key = node.args[0] if isinstance(node, ast.Call) else node.slice
        name = str(key.value) if isinstance(key, ast.Constant) else ""
    else:
        name = node.id if isinstance(node, ast.Name) else getattr(node, "attr", "")
    return "version" in name.lower() and "geo" in name.lower()


#: ``detect_geoparquet_file_type``'s answers for a GeoParquet input.
_FILE_TYPES = frozenset({"geoparquet_v1", "geoparquet_v2", "parquet_geo_only"})


def _is_version_value(node: ast.AST | None) -> bool:
    return isinstance(node, ast.Constant) and node.value in _VERSION_SPELLINGS


def _tests_a_file_type(node: ast.AST) -> bool:
    """A comparison against a file-type literal (``==``, or ``in`` a literal tuple)."""
    if not isinstance(node, ast.Compare):
        return isinstance(node, ast.BoolOp) and any(_tests_a_file_type(v) for v in node.values)
    for side in (node.left, *node.comparators):
        items = side.elts if isinstance(side, (ast.Tuple, ast.List, ast.Set)) else [side]
        if any(isinstance(i, ast.Constant) and i.value in _FILE_TYPES for i in items):
            return True
    return False


def _file_type_version_map(m: _Module, node: ast.AST) -> bool:
    """``{"geoparquet_v2": "2.0", ...}`` or ``if ft == "geoparquet_v2": return "2.0"``:
    the file-type -> version map, which is the owner's ``_version_for_file_type``."""
    if isinstance(node, ast.Dict):
        keyed = any(isinstance(k, ast.Constant) and k.value in _FILE_TYPES for k in node.keys)
        return keyed and any(_is_version_value(v) for v in node.values)
    if isinstance(node, ast.IfExp):
        return _tests_a_file_type(node.test) and (
            _is_version_value(node.body) or _is_version_value(node.orelse)
        )
    if not (isinstance(node, ast.If) and _tests_a_file_type(node.test)):
        return False
    for stmt in node.body:
        if isinstance(stmt, (ast.Return, ast.Assign, ast.AnnAssign)) and _is_version_value(
            stmt.value
        ):
            return True
    return False


def _version_keyword_literal(m: _Module, node: ast.AST) -> bool:
    """``write(..., geoparquet_version="1.1")``: a version decided at the call site."""
    if not isinstance(node, ast.Call):
        return False
    return any(
        kw.arg is not None and "geoparquet_version" in kw.arg and _is_short_version(kw.value)
        for kw in node.keywords
    )


_OUTPUT_VERSION_SHAPES = (
    _file_type_version_map,
    _version_keyword_literal,
    _version_or_fallback,
    _version_key_read,
    _version_get_default,
    _version_literal_bound,
    _version_literal_compared,
)


#: ``in`` and ``not in``.
_MEMBERSHIP_OPS = (ast.In, ast.NotIn)  # codespell:ignore

#: A scheme prefix spelled out as a literal: ``"s3://"``, ``"https://"``.
_SCHEME_PREFIX = re.compile(r"^[a-z][a-z0-9+.-]*://$")


def _remote_scheme_names(repo: Path) -> frozenset[str]:
    """The keys of the owner's ``REMOTE_SCHEMES`` table, read from its source."""
    owner = repo / "geoparquet_io" / "core" / "remote.py"
    if not owner.is_file():
        from geoparquet_io.core.remote import REMOTE_SCHEMES

        return frozenset(REMOTE_SCHEMES)
    for node in ast.walk(ast.parse(owner.read_text(encoding="utf-8"))):
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "REMOTE_SCHEMES" for t in node.targets)
            and isinstance(node.value, ast.Dict)
        ):
            return frozenset(k.value for k in node.value.keys if isinstance(k, ast.Constant))
    return frozenset()


def _compares_to_a_scheme_name(node: ast.Compare, names: frozenset[str]) -> bool:
    """``x == "s3"`` / ``"gs" != x``: an equality test against one scheme name."""
    if not any(isinstance(op, (ast.Eq, ast.NotEq)) for op in node.ops):
        return False
    return any(
        isinstance(side, ast.Constant) and isinstance(side.value, str) and side.value in names
        for side in (node.left, *node.comparators)
    )


def _find_scheme_test(m: _Module, repo: Path) -> Iterator[Hit]:
    """A URL-scheme test outside ``core/remote.py``: ``"://" in path``, a scheme
    prefix spelled out (``startswith("s3://")``, a prefix tuple), a
    tuple/list/set of two or more scheme names (``("s3", "gs", "az")``), a
    compare against one scheme name (``url_scheme(x) == "s3"``) or a dict keyed
    by scheme names (``{"s3": "s3", "gcs": "gs"}``).

    ``remote.REMOTE_SCHEMES`` is the one scheme table, and ``is_remote_url``,
    ``url_scheme`` and ``remote_family`` the tests over it. Would have caught
    the six-scheme ``_is_remote_url`` in ``add/bbox_metadata.py`` (no
    ``s3a``/``gcs``/``abfs``), the three-scheme ``startswith`` copies in
    ``stac_check`` and ``admin_datasets`` that called ``gs://`` local, and the
    ``"://" in`` test in ``process/simplify.py`` (#1367 T2e). Building a URL
    (``f"s3://{bucket}"``) and splitting one (``.partition("://")``) are not
    tests and pass. The single-name compare and the dict would have caught
    upload's per-spelling dispatch and ``copy_file``'s private spelling map
    that disagreed over ``s3a://``/``gcs://`` (PR #1387 review).
    """
    names = _remote_scheme_names(repo)
    for node in ast.walk(m.tree):
        if isinstance(node, ast.Compare):
            literal = isinstance(node.left, ast.Constant) and node.left.value == "://"
            if literal and any(isinstance(op, _MEMBERSHIP_OPS) for op in node.ops):
                yield m.hit(node)
            elif _compares_to_a_scheme_name(node, names):
                yield m.hit(node)
        elif isinstance(node, ast.Dict) and node.keys:
            keys = [k.value if isinstance(k, ast.Constant) else None for k in node.keys]
            if all(isinstance(k, str) and k in names for k in keys):
                yield m.hit(node)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            in_fstring = isinstance(m.parents.get(id(node)), ast.JoinedStr)
            if _SCHEME_PREFIX.match(node.value) and id(node) not in m.docstrings and not in_fstring:
                yield m.hit(node)
        elif isinstance(node, (ast.Tuple, ast.List, ast.Set)) and len(node.elts) >= 2:
            values = [e.value if isinstance(e, ast.Constant) else None for e in node.elts]
            if all(isinstance(v, str) and v in names for v in values):
                yield m.hit(node)


#: The names a command's overwrite switch goes by.
_OVERWRITE_SWITCHES = {"overwrite", "force"}
#: Calls that ask whether a path exists.
_EXISTENCE_CALLS = {"exists", "isfile", "is_file", "lexists"}


def _find_overwrite_check(m: _Module) -> Iterator[Hit]:
    """An overwrite check outside ``core/file_utils.py``: an ``if`` whose test
    asks whether a path exists *and* reads the overwrite switch
    (``if os.path.exists(out) and not force: raise ...``,
    ``if overwrite and out.exists(): out.unlink()``).

    ``file_utils.handle_output_overwrite`` is the one overwrite policy, with
    one refusal (``output_exists_error``). Would have caught the nine hand-rolled
    checks #1367 found -- pmtiles chunks and pyramid, process overview, ArcGIS,
    CARTO, WFS, the format writers, ``add stac-metadata`` and the STAC API --
    each with its own message, exception type and exit code (T2e).
    """

    def switch(node: ast.AST) -> bool:
        return any(
            (isinstance(n, ast.Name) and n.id in _OVERWRITE_SWITCHES)
            or (isinstance(n, ast.Attribute) and n.attr in _OVERWRITE_SWITCHES)
            for n in ast.walk(node)
        )

    def existence(node: ast.AST) -> bool:
        return any(
            isinstance(n, ast.Call)
            and isinstance(n.func, (ast.Attribute, ast.Name))
            and (n.func.attr if isinstance(n.func, ast.Attribute) else n.func.id)
            in _EXISTENCE_CALLS
            for n in ast.walk(node)
        )

    for node in ast.walk(m.tree):
        if isinstance(node, (ast.If, ast.IfExp)) and existence(node.test) and switch(node.test):
            yield m.hit(node)


#: The stats aggregates ``core/geo_metadata.py`` owns (#1367 concept 5). Each
#: string literal is read as a template, an f-string's ``{...}`` parts as ``{}``.
_STATS_AGGREGATE = re.compile(
    r"(?is)\bST_Extent_Agg\b"
    r"|\b(?:min|max)\s*\(\s*ST_[XYZ](?:min|max)?\s*\("
    r"|\b(?:min|max)\s*\(\s*(?:\{\}|\"?\w+\"?)\.\"?[xyz](?:min|max)\"?\s*\)"
    r"|\bST_GeometryType\b.*\b(?:DISTINCT|GROUP\s+BY)\b"
    r"|\b(?:DISTINCT|GROUP\s+BY)\b.*\bST_GeometryType\b"
    r"|\b(?:list|array_agg)\s*\(\s*ST_GeometryType\b"
)

#: The bbox fields a Python-side fold reads off each row group / batch / file.
_BBOX_FIELDS = frozenset({"xmin", "ymin", "xmax", "ymax", "zmin", "zmax"})


def _reads_a_bbox_field(node: ast.AST) -> bool:
    """``b["xmin"]`` or ``b.xmin``: one bbox field of one element."""
    if isinstance(node, ast.Subscript):
        key = node.slice
        return isinstance(key, ast.Constant) and key.value in _BBOX_FIELDS
    return isinstance(node, ast.Attribute) and node.attr in _BBOX_FIELDS


def _is_python_stats_fold(node: ast.AST) -> bool:
    """``min(b["xmin"] for b in boxes)`` / ``max(s.ymax for s in stats)``, or
    ``pc.min_max(...)``: a fold of extents written in Python."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Attribute) and func.attr == "min_max":
        return True
    if not (isinstance(func, ast.Name) and func.id in ("min", "max") and len(node.args) == 1):
        return False
    arg = node.args[0]
    return isinstance(arg, (ast.GeneratorExp, ast.ListComp)) and _reads_a_bbox_field(arg.elt)


def _string_template(node: ast.AST) -> str | None:
    """A string literal's text, an f-string's interpolations spelled ``{}``."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            v.value if isinstance(v, ast.Constant) and isinstance(v.value, str) else "{}"
            for v in node.values
        )
    return None


def _find_geometry_stats(m: _Module) -> Iterator[Hit]:
    """A geometry-stats or bounds aggregate outside ``core/geo_metadata.py``:
    ``ST_Extent_Agg``, ``MIN``/``MAX`` of ``ST_XMin``..``ST_YMax``, of
    ``ST_X``/``ST_Y`` or of a bbox struct's ``.xmin``..``.ymax``, or
    ``ST_GeometryType`` under ``DISTINCT`` / ``GROUP BY`` / ``list(`` /
    ``array_agg(``; and the same fold in Python -- ``min``/``max`` over a
    comprehension of a bbox field (``min(b["xmin"] for b in boxes)``) or
    ``pc.min_max``.

    ``geo_metadata.geometry_stats_of`` / ``bounds_of`` / ``geometry_types_of``
    are the one stats routine with one degenerate-geometry policy. Would have
    caught the 15+ bounds queries #1367 found, which disagreed on empty input
    (NULLs, ``[0, 0, 0, 0]`` or a raise), on EMPTY geometries and on the
    antimeridian, and the non-finite coordinate that one of them let through
    as ``Infinity`` while another died on it (#236, #1165). A per-row
    ``ST_XMin(g)`` building a bbox *column* is not an aggregate and passes.

    Not seen (named so a reviewer looks for them): an aggregate whose
    expression is interpolated from another string (``MIN({x})`` with
    ``x = "ST_XMin(g)"``), one assembled by concatenation, ``MIN(xmin)`` over
    an aliased subquery, a fold indexing by position (``min(b[0] for ...)``),
    and a hand-written loop with ``if v < lo: lo = v``.
    """
    for node in ast.walk(m.tree):
        if id(node) in m.docstrings or isinstance(m.parents.get(id(node)), ast.JoinedStr):
            continue
        template = _string_template(node)
        if template is not None and _STATS_AGGREGATE.search(template):
            yield m.hit(node)
        elif _is_python_stats_fold(node):
            yield m.hit(node)


#: The values of a GeoParquet ``edges`` field (and of a Parquet GEOGRAPHY's
#: edge algorithm). Spelled out here, not imported, so the arm still works on a
#: tree whose owner is broken.
_EDGE_NAMES = frozenset({"planar", "spherical", "vincenty", "thomas", "andoyer", "karney"})
_EDGES_KEY = "edges"
_KEY_METHODS = {"get", "pop", "setdefault"}


def _is_edges_key(node: ast.AST | None) -> bool:
    return isinstance(node, ast.Constant) and node.value == _EDGES_KEY


def _edges_key_access(node: ast.AST) -> bool:
    """``x["edges"]``, ``x.get("edges")`` / ``.pop`` / ``.setdefault``, or a
    comparison against the key (``"edges" in x``, ``k != "edges"``)."""
    if isinstance(node, ast.Subscript):
        return _is_edges_key(node.slice)
    if isinstance(node, ast.Call):
        return (
            isinstance(node.func, ast.Attribute)
            and node.func.attr in _KEY_METHODS
            and bool(node.args)
            and _is_edges_key(node.args[0])
        )
    if isinstance(node, ast.Compare):
        return any(_is_edges_key(n) for n in (node.left, *node.comparators))
    return False


#: The owner's constant names: a dict literal may name a field with one of
#: these (or ``None``) without deciding anything.
_EDGE_CONSTANTS = frozenset({"PLANAR", "SPHERICAL", "VINCENTY", "THOMAS", "ANDOYER", "KARNEY"})
#: Literal text of a Parquet GEOGRAPHY type's edges, built or parsed by hand.
_LOGICAL_TYPE_EDGES_TEXT = ("GEOGRAPHY(", "algorithm=")


def _computes_a_value(node: ast.AST | None) -> bool:
    """A dict value that is an expression, not a field-type table entry or a name."""
    if node is None or isinstance(node, (ast.Constant, ast.Tuple, ast.Lambda)):
        return False
    return not (isinstance(node, ast.Name) and node.id in _EDGE_CONSTANTS)


def _edges_key_written(node: ast.AST) -> bool:
    """``{"edges": e}`` (so ``d.update({"edges": e})`` too) and ``dict(edges=e)``:
    the key written past ``set_edges``'s spell-or-omit rule."""
    if isinstance(node, ast.Dict):
        return any(
            _is_edges_key(k) and _computes_a_value(v)
            for k, v in zip(node.keys, node.values, strict=True)
        )
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dict":
        return any(kw.arg == _EDGES_KEY for kw in node.keywords)
    return False


def _names_edges(node: ast.AST) -> bool:
    name = node.id if isinstance(node, ast.Name) else getattr(node, "attr", "")
    return "edge" in name.lower() or "algorithm" in name.lower()


def _edge_type_access(node: ast.AST) -> bool:
    """``ga.EdgeType.SPHERICAL``, ``ga.EdgeType[e.upper()]``, ``t.edge_type``: the
    GeoArrow carrier, which the owner's reader and writer hold."""
    if isinstance(node, ast.Attribute):
        return node.attr == "edge_type" or _is_edge_type_enum(node.value)
    if isinstance(node, ast.Subscript):
        return _is_edge_type_enum(node.value)
    return False


def _is_edge_type_enum(node: ast.AST) -> bool:
    return (isinstance(node, ast.Attribute) and node.attr == "EdgeType") or (
        isinstance(node, ast.Name) and node.id == "EdgeType"
    )


def _is_case_fold_of_edges(node: ast.AST) -> bool:
    """``edges.lower()`` / ``.upper()`` / ``.casefold()`` of an edges value,
    compared or not: the owner's ``normalized_edges`` re-done."""
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"lower", "upper", "casefold"}
        and _names_edges(node.func.value)
    )


def _logical_type_edges_text(node: ast.AST) -> bool:
    """``f"GEOGRAPHY(OGC:CRS84, {alg})"``, ``f"algorithm={a}"``, an
    ``algorithm=(\\w+)`` pattern: a GEOGRAPHY type's edges built or parsed by hand."""
    if isinstance(node, ast.JoinedStr):
        text = "".join(v.value for v in node.values if isinstance(v, ast.Constant))
        return any(part in text for part in _LOGICAL_TYPE_EDGES_TEXT)
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and "algorithm=(" in node.value
    )


def _is_edge_constant(node: ast.AST) -> bool:
    """``PLANAR`` or ``edges.PLANAR``: an owner constant, by either spelling."""
    name = node.id if isinstance(node, ast.Name) else getattr(node, "attr", None)
    return isinstance(node, (ast.Name, ast.Attribute)) and name in _EDGE_CONSTANTS


def _decides_with_edge_constant(node: ast.AST) -> bool:
    """``e == SPHERICAL``, ``e != PLANAR``, ``e in (PLANAR, ...)``, ``e or PLANAR``:
    a decision the owner's predicates (``is_nonplanar``, ``is_geodesic``,
    ``declared_edges``) make, re-made with its constants."""
    if isinstance(node, ast.Compare):
        operands = (node.left, *node.comparators)
        return any(
            _is_edge_constant(n)
            or (
                isinstance(n, (ast.Tuple, ast.List, ast.Set))
                and any(_is_edge_constant(e) for e in n.elts)
            )
            for n in operands
        )
    if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
        return any(_is_edge_constant(v) for v in node.values)
    return False


def _edge_type_import(node: ast.AST) -> bool:
    """``from geoarrow.pyarrow import EdgeType`` (``as ET`` or not): the carrier's
    enum under a name the attribute and subscript shapes cannot see."""
    return isinstance(node, ast.ImportFrom) and any(a.name == "EdgeType" for a in node.names)


def _logical_type_concat(node: ast.AST) -> bool:
    """``"GEOGRAPHY(" + alg`` / ``"algorithm=%s" % a``: the f-string shape by
    concatenation or ``%``."""
    if not (isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Mod))):
        return False
    return any(
        isinstance(n, ast.Constant)
        and isinstance(n.value, str)
        and any(part in n.value for part in _LOGICAL_TYPE_EDGES_TEXT)
        for n in (node.left, node.right)
    )


#: Where a planar ``ST_Centroid(`` is a cell key: those sites take the
#: owner's ``representative_point_sql``, which spells it for planar edges.
#: ``index_ops.py`` is the IndexSpec driver, where add, sort and partition key.
_CELL_KEY_DIRS = (
    "geoparquet_io/core/index_ops.py",
    "geoparquet_io/core/add/",
    "geoparquet_io/core/partition/",
    "geoparquet_io/core/process/aggregate/",
)


def _planar_cell_key(m: _Module, node: ast.AST) -> bool:
    """A string or f-string spelling ``ST_Centroid(`` in a cell-key module
    (an f-string once, not again by its parts; never prose)."""
    if not m.path.startswith(_CELL_KEY_DIRS):
        return False
    if isinstance(node, ast.JoinedStr):
        text = "".join(v.value for v in node.values if isinstance(v, ast.Constant))
    elif (
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in m.docstrings
        and not isinstance(m.parents.get(id(node)), ast.JoinedStr)
    ):
        text = node.value
    else:
        return False
    return "ST_Centroid(" in text


def _find_edges(m: _Module) -> Iterator[Hit]:
    """An ``edges`` decision outside ``core/edges.py`` (#1367 concept 6): a
    string that *is* an edges value (``"spherical"``, ``"planar"``, an
    ellipsoidal algorithm name), or a read, write or test of the ``edges`` key.

    The owner holds the vocabulary, the 1.x/2.0 version gate, the one
    precedence between a request, a carried block and a native type, and the
    key's readers and writers. Would have caught the five literal sets and the
    readers #1367 found: two that recognised only ``SPHERICAL`` beside two that
    took all five algorithms, the ``("planar", "spherical")`` pair in the
    version gate duplicating ``validate.py``'s, and a carried-metadata fallback
    that counted a typo as non-planar where the file reader ignored it.

    The first audit added the shapes that passed it at 0: any case of a value
    (``"SPHERICAL"``); a dict literal's ``"edges"`` key holding an expression,
    ``.update`` with one and ``dict(edges=...)`` (a writer past ``set_edges``);
    ``ga.EdgeType`` members and ``.edge_type`` (the GeoArrow carrier's four
    hand-built writers and second reader); a case fold of an edges value; and a
    GEOGRAPHY type's ``algorithm=`` text built in an f-string or parsed with a
    pattern. A dict literal naming the field with ``None``, a constant, an owner
    constant or a field-type table entry (a tuple, a lambda) is not a hit.

    The second audit added a compare or ``or`` against an owner constant
    (``e != PLANAR``, ``e or PLANAR``, ``alg == SPHERICAL``: the owner's
    predicates re-made), an ``EdgeType`` imported by name (aliased or not),
    ``"GEOGRAPHY(" +`` concatenation, a case fold of an edges value outside a
    compare (``edges.casefold()``), and a planar ``ST_Centroid(`` cell key in
    an add, partition or aggregate module (those take
    ``representative_point_sql``).
    """
    for node in m.strings():
        if node.value.lower() in _EDGE_NAMES:
            yield m.hit(node)
    for node in ast.walk(m.tree):
        if (
            _edges_key_access(node)
            or _edges_key_written(node)
            or _edge_type_access(node)
            or _logical_type_edges_text(node)
            or _decides_with_edge_constant(node)
            or _edge_type_import(node)
            or _logical_type_concat(node)
            or _is_case_fold_of_edges(node)
            or _planar_cell_key(m, node)
        ):
            yield m.hit(node)


#: The footer patch's mechanism and its two refusals (``core/parquet_footer.py``).
#: The owner's private patch and fallback, and the names they had before.
_FOOTER_PATCH_PRIVATE = frozenset(
    {"_patch_footer_file", "_fall_back", "patch_footer", "patch_footer_kv"}
)
_FOOTER_PATCH_REFUSALS = frozenset({"FooterPatchUnsupported", "FooterPatchRaced"})
_FOOTER_PATCH_OWNER_MODULE = "geoparquet_io.core.parquet_footer"


def _caught_names(handler: ast.ExceptHandler) -> Iterator[str]:
    if handler.type is None:
        return
    kinds = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
    for kind in kinds:
        if isinstance(kind, ast.Name):
            yield kind.id
        elif isinstance(kind, ast.Attribute):
            yield kind.attr


def _calls_patch_or_rewrite(m: _Module, body: list[ast.stmt]) -> bool:
    return any(
        isinstance(sub, ast.Call)
        and (m.dotted(sub.func) or "").rsplit(".", 1)[-1] == "patch_or_rewrite"
        for stmt in body
        for sub in ast.walk(stmt)
    )


_BROAD_CATCHES = frozenset({"Exception", "BaseException"})


def _answers_the_refusal(handler: ast.ExceptHandler) -> bool:
    """A handler that catches the refusal's base, or anything broader, and does
    not re-raise: a second answer to what ``patch_or_rewrite`` already decided.
    A cleanup that ends in a bare ``raise`` hands the error on and is not one."""
    caught = set(_caught_names(handler))
    if "GeoParquetError" in caught:
        return True
    broad = handler.type is None or bool(caught & _BROAD_CATCHES)
    reraises = any(
        isinstance(sub, ast.Raise) and sub.exc is None
        for stmt in handler.body
        for sub in ast.walk(stmt)
    )
    return broad and not reraises


def _find_footer_patch(m: _Module) -> Iterator[Hit]:
    """The patch-else-rewrite decision spelled outside ``core/parquet_footer.py``
    (#1367 concept 2). ``parquet_footer.patch_or_rewrite`` owns it; six callers
    each hand-wrote it, one answered a race with a rewrite, one refused where
    the others re-encoded, and each chose its own fallback codec (#1015). Seen:

    * the private patch or fallback (``_patch_footer_file``, ``_fall_back``, or
      their old public names) imported from the owner under any alias, read
      as an attribute, or fetched by ``getattr``;
    * a refusal (``FooterPatchUnsupported``/``FooterPatchRaced``) imported from
      the owner, or caught -- bare, as an attribute, or in a tuple;
    * ``except GeoParquetError``, ``except Exception``/``BaseException`` or a
      bare ``except:`` around a ``patch_or_rewrite`` call that does not
      re-raise: the refusal caught to answer it a second way;
    * a direct call of the fallback, ``rewrite_footer_kv(...)``: a caller
      that rewrites without asking whether the footer could be patched. Callers
      hand it to ``patch_or_rewrite`` as a reference.
    """
    for node in ast.walk(m.tree):
        if isinstance(node, ast.ImportFrom) and node.module == _FOOTER_PATCH_OWNER_MODULE:
            for alias in node.names:
                if alias.name in _FOOTER_PATCH_PRIVATE | _FOOTER_PATCH_REFUSALS:
                    yield m.hit(node)
        elif isinstance(node, ast.Attribute) and node.attr in _FOOTER_PATCH_PRIVATE:
            yield m.hit(node)
        elif m.calls(node, "getattr") and len(node.args) >= 2:
            name = m.const(node.args[1])
            if name in _FOOTER_PATCH_PRIVATE | _FOOTER_PATCH_REFUSALS:
                yield m.hit(node)
        elif isinstance(node, ast.ExceptHandler):
            if any(name in _FOOTER_PATCH_REFUSALS for name in _caught_names(node)):
                yield m.hit(node)
        elif isinstance(node, ast.Try) and _calls_patch_or_rewrite(m, node.body):
            for handler in node.handlers:
                if _answers_the_refusal(handler):
                    yield m.hit(handler)
        elif (
            isinstance(node, ast.Call)
            and (m.dotted(node.func) or "").rsplit(".", 1)[-1] == "rewrite_footer_kv"
        ):
            yield m.hit(node)


#: A name that holds a geometry *column name*: ``geometry_column``, ``geom_col``,
#: ``geometry_col``... (not ``geometry_name``, an output name, nor ``bucket_point``).
_GEOM_COLUMN_NAME = re.compile(r"geom\w*col", re.IGNORECASE)
#: The WKB codec calls ``core/duckdb_utils.py`` spells once (``as_wkb_sql``,
#: ``geom_from_wkb_sql``) and ``core/geometry_detection.py`` builds reads from.
_WKB_CODEC_CALL = re.compile(r"(?i)\bST_(?:GeomFrom(?:HEX)?WKB|AsWKB)\s*\(")
_WKB_CODEC_OWNERS = ("geoparquet_io/core/duckdb_utils.py",)


def _is_geometry_literal(node: ast.AST | None) -> bool:
    return isinstance(node, ast.Constant) and node.value == "geometry"


def _names_a_geometry_column(node: ast.AST) -> bool:
    return isinstance(node, ast.Name) and bool(_GEOM_COLUMN_NAME.search(node.id))


def _own_returns(fn: ast.AST) -> Iterator[ast.Return]:
    """The ``return`` statements of *fn* itself, not of a def nested in it."""
    stack = list(ast.iter_child_nodes(fn))
    while stack:
        node = stack.pop()
        if isinstance(node, ast.Return):
            yield node
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            stack.extend(ast.iter_child_nodes(node))


def _fallback_returns(node: ast.AST) -> Iterator[ast.AST]:
    """``return "geometry"`` in a function with another return: a fallback."""
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return
    returns = list(_own_returns(node))
    if len(returns) > 1:
        yield from (r.value for r in returns if _is_geometry_literal(r.value))


def _literal_column_default(node: ast.AST) -> ast.AST | None:
    """The ``"geometry"`` literal *node* uses as a column-name default, if any."""
    if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
        return node.values[-1] if _is_geometry_literal(node.values[-1]) else None
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr == "get" and len(node.args) >= 2 and _is_geometry_literal(node.args[1]):
            return node.args[1]
    if isinstance(node, ast.Assign) and _is_geometry_literal(node.value):
        if any(_names_a_geometry_column(t) for t in node.targets):
            return node.value
    if isinstance(node, ast.AnnAssign) and _is_geometry_literal(node.value):
        if _names_a_geometry_column(node.target):
            return node.value
    if isinstance(node, ast.Compare) and _names_a_geometry_column(node.left):
        if any(_is_geometry_literal(c) for c in node.comparators):
            return next(c for c in node.comparators if _is_geometry_literal(c))
    return None


#: Column names some resolver has matched as "the geometry", as spelled (lower
#: case: ``("Geometry", "Geography")`` is the logical-type family pair, not
#: names). Two or more of them in one list/tuple/set literal is a second
#: candidates list; the owner's
#: is ``geometry_detection.STANDARD_GEOMETRY_NAMES`` (a source's own extras go
#: through ``primary_geometry_column(extra_names=...)``).
_GEOMETRY_NAMES = frozenset(
    {"geometry", "geom", "the_geom", "wkb_geometry", "shape", "geo", "geography"}
)


def _geometry_names_list(node: ast.AST) -> bool:
    if not isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return False
    named = {e.value for e in node.elts if isinstance(e, ast.Constant) and isinstance(e.value, str)}
    return len(named & _GEOMETRY_NAMES) >= 2


def _primary_literal(node: ast.AST) -> ast.AST | None:
    """``primary = "geometry"`` / ``primary_column: str = "geom"``: a primary
    column decided by a literal rather than by the resolver."""
    if isinstance(node, ast.Assign):
        targets, value = node.targets, node.value
    elif isinstance(node, ast.AnnAssign):
        targets, value = [node.target], node.value
    else:
        return None
    if not (isinstance(value, ast.Constant) and isinstance(value.value, str) and value.value):
        return None
    for target in targets:
        if isinstance(target, ast.Name) and "primary" in target.id.lower():
            return value
    return None


def _geometry_named_target(m: _Module, node: ast.AST) -> bool:
    """Whether *node* is the value of an assignment to, or a keyword named, a
    geometry column (``geom_col = ...``, ``primary = ...``,
    ``f(geometry_column=...)``)."""
    up = m.parents.get(id(node))
    if isinstance(up, ast.keyword):
        return bool(up.arg and _GEOM_COLUMN_NAME.search(up.arg))
    if isinstance(up, (ast.Assign, ast.AnnAssign)) and up.value is node:
        targets = up.targets if isinstance(up, ast.Assign) else [up.target]
        return any(
            _names_a_geometry_column(t) or (isinstance(t, ast.Name) and "primary" in t.id.lower())
            for t in targets
        )
    return False


def _is_str_constant(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and isinstance(node.value, str)


def _ifexp_literal(m: _Module, node: ast.AST) -> ast.AST | None:
    """``"geometry" if x else y`` / ``x if c else "geometry"``: a geometry name as
    one branch of a conditional is a column chosen by literal.

    When *both* branches are strings it is a column only if it lands in a
    geometry-named target or keyword (``geom_col = "geom" if legacy else
    "geometry"``); otherwise it is a word for a message (``"geometry" if n == 1
    else "geometries"``). ``None`` is not a string: ``"geometry" if has else
    None`` is a column choice wherever it lands.
    """
    if not isinstance(node, ast.IfExp):
        return None
    branches = (node.body, node.orelse)
    literal = next(
        (b for b in branches if _is_str_constant(b) and b.value in _GEOMETRY_NAMES), None
    )
    if literal is None:
        return None
    if all(_is_str_constant(b) for b in branches) and not _geometry_named_target(m, node):
        return None
    return literal


#: An expression that holds geometry columns (``geo_columns``,
#: ``detect_geometry_columns(f)``, ``geometry_cols``): its first entry taken as
#: *the* column is a "first native column" pick.
_GEOM_COLUMNS_TEXT = re.compile(r"(?i)geo\w*_col")


def _first_column_pick(m: _Module, node: ast.AST) -> bool:
    """``next(iter(x))`` / ``x[0]``: the first entry of a collection taken as the
    geometry column, instead of the resolver's rule -- either because the
    collection holds geometry columns (``geo_columns[0]``) or because the pick
    lands in a geometry-named target (``geom_col = columns[0]``)."""
    inner = None
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "next":
        arg = node.args[0] if node.args else None
        if isinstance(arg, ast.Call) and isinstance(arg.func, ast.Name) and arg.func.id == "iter":
            inner = arg.args[0] if arg.args else None
    elif isinstance(node, ast.Subscript):
        if isinstance(node.slice, ast.Constant) and node.slice.value == 0:
            inner = node.value
    if inner is None:
        return False
    return bool(_GEOM_COLUMNS_TEXT.search(ast.unparse(inner))) or _geometry_named_target(m, node)


def _primary_get(m: _Module, node: ast.AST) -> ast.AST | None:
    """``x.get("primary_column")`` outside the owner and the block decoder: the
    block's claim read as the column, skipping the resolver's "the schema has
    it" rule. The legitimate reads are ``[[G1.geometry_identity]]`` entries."""
    if not (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == "primary_column"
    ):
        return None
    return node


#: DuckDB type-name prefixes that decide a geometry read kind; the owner's
#: ``GeometryRead.for_type`` decides it, ``duckdb_utils`` names the GEOMETRY type.
_READ_KIND_TYPES = frozenset({"BLOB", "BINARY", "STRUCT", "GEOMETRY", "GEOGRAPHY", "VARCHAR"})


def _type_test(m: _Module, node: ast.AST) -> bool:
    """``t.upper().startswith("STRUCT")`` / ``startswith(("BLOB", "VARCHAR"))`` /
    ``"BLOB" in t``: a read kind decided outside ``GeometryRead``. A prefix tuple
    that also names non-geometry types (``"MAP"``, ``"UNION"``) is a general
    type classifier, not a read kind."""
    if not isinstance(node, (ast.Call, ast.Compare)):
        return False
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "startswith"
        and node.args
    ):
        arg = node.args[0]
        values = arg.elts if isinstance(arg, ast.Tuple) else [arg]
        named = [v.value for v in values if _is_str_constant(v)]
        return bool(named) and all(v in _READ_KIND_TYPES for v in named)
    if isinstance(node, ast.Compare) and any(isinstance(op, _MEMBERSHIP_OPS) for op in node.ops):
        return (
            _is_str_constant(node.left)
            and node.left.value in ("BLOB", "BINARY", "STRUCT")
            and not isinstance(node.comparators[0], (ast.List, ast.Tuple, ast.Set))
        )
    return False


def _positional_literal(node: ast.AST) -> ast.AST | None:
    """``bounds_of(src, "geometry")``: a column named by a positional literal.

    An exception's first argument is a parameter name, a mapping lookup's
    (``.get("geometry")``) a key of some other document (GeoJSON, STAC), and a
    Click declaration's strings are parameter names: none is a column.
    """
    if not isinstance(node, ast.Call):
        return None
    callee = (
        node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
    )
    if callee in _NOT_COLUMN_CALLS:
        return None
    if callee == "quote_identifier":
        # Quoting it as an identifier makes any geometry name a column name
        # (``quote_identifier("the_geom")``, #1392 pass 3).
        return next(
            (a for a in node.args if _is_str_constant(a) and a.value in _GEOMETRY_NAMES), None
        )
    skip_first = callee.endswith(("Error", "Exception")) or callee in _MAPPING_LOOKUPS
    args = node.args[1:] if skip_first else node.args
    return next((a for a in args if _is_geometry_literal(a)), None)


_NOT_COLUMN_CALLS = frozenset({"option", "argument"})
_MAPPING_LOOKUPS = frozenset({"get", "pop", "setdefault"})


def _any_param_default(node: ast.AST) -> Iterator[ast.AST]:
    """``def f(base="geometry")``: the literal as any parameter's default."""
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return
    args = node.args
    yield from (d for d in (*args.defaults, *args.kw_defaults) if _is_geometry_literal(d))


_CASE_FOLDS = frozenset({"lower", "upper", "casefold"})
_GEO_SUBSTRINGS = frozenset({"geo", "geom", "geometry", "geog", "geography"})


def _substring_heuristic(node: ast.AST) -> bool:
    """``"geo" in name.lower()``: a column taken as geometry because its name
    contains a fragment (BigQuery's ``building_geom_wkb`` rule)."""
    if not (isinstance(node, ast.Compare) and len(node.ops) == 1):
        return False
    if not isinstance(node.ops[0], ast.In):
        return False
    left, right = node.left, node.comparators[0]
    return (
        isinstance(left, ast.Constant)
        and left.value in _GEO_SUBSTRINGS
        and isinstance(right, ast.Call)
        and isinstance(right.func, ast.Attribute)
        and right.func.attr in _CASE_FOLDS
    )


def _literal_column_kwarg(node: ast.AST) -> ast.AST | None:
    """``f(t, geometry_column="geometry")``: a built column named by a literal
    rather than ``geometry_detection.BUILT_GEOMETRY_COLUMN``."""
    if isinstance(node, ast.Call):
        for kw in node.keywords:
            if kw.arg and _GEOM_COLUMN_NAME.search(kw.arg) and _is_geometry_literal(kw.value):
                return kw.value
    return None


def _calls_named(fn: ast.AST, name: str) -> list[ast.Call]:
    return [
        c
        for c in ast.walk(fn)
        if isinstance(c, ast.Call)
        and (
            (isinstance(c.func, ast.Name) and c.func.id == name)
            or (isinstance(c.func, ast.Attribute) and c.func.attr == name)
        )
    ]


def _hand_rolled_read(m: _Module, fn: ast.AST) -> Iterator[ast.AST]:
    """A function that decodes through the primitive (``geom_from_wkb_sql``)
    after its own type test (``"BLOB" in t``) or into a view it builds: the
    BLOB-vs-native read the owner's ``resolve_geometry_read`` /
    ``create_geometry_view`` answer, re-derived."""
    if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return
    decodes = _calls_named(fn, "geom_from_wkb_sql")
    if not decodes:
        return
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.Compare)
            and isinstance(node.left, ast.Constant)
            and node.left.value in ("BLOB", "BINARY")
            and any(isinstance(op, _MEMBERSHIP_OPS) for op in node.ops)
        ):
            yield node
            return
    for node in ast.walk(fn):
        if id(node) in m.docstrings:
            continue
        template = _string_template(node)
        if template is not None and re.search(r"(?i)\bCREATE\b.*\bVIEW\b", template):
            yield decodes[0]
            return


def _find_geometry_identity(m: _Module) -> Iterator[Hit]:
    """A literal ``"geometry"`` column-name default outside
    ``core/geometry_detection.py``, or a WKB codec call (``ST_GeomFromWKB(`` /
    ``ST_AsWKB(``) built outside it and ``core/duckdb_utils.py``.

    ``geometry_detection.primary_geometry_column`` is the one resolver and it
    never answers the literal: a schema with no geometry-like column is
    ``None``, which the caller turns into ``GeometryColumnNotFoundError``.
    Would have caught the nine resolvers #1367 found, four of which fell back
    to ``"geometry"`` and so named a column the file did not have (a ``geom``
    file read as having no CRS, #887 review), and the sort sentinel that
    treated an explicit ``--geometry-column geometry`` as "detect". The literal
    shapes: a parameter default named like a column (``geometry_column=``,
    ``geom_col=``), ``x or "geometry"``, ``.get(k, "geometry")``,
    ``geom_col = "geometry"``, ``geometry_column == "geometry"`` and a fallback
    ``return "geometry"`` (one of several returns), a ``primary = "<name>"``
    literal (``derive_geo_from_file``'s), and a second list of geometry
    column names (``extract_bigquery``'s, with ``geo``/``geography``).
    The audit's eight blind spots (#1392 pass 1) are shapes too: the literal
    as a branch of a conditional (``"geometry" if x else y``), the first of a
    set of geometry columns taken as the answer (``next(iter(geo_columns))``,
    ``geo_columns[0]``), the block's primary read inline as the column
    (``geometry_column = geo.get("primary_column")``), a name-fragment rule
    (``"geo" in name.lower()``), a built column named by a literal keyword
    (``geometry_column="geometry"``; ``BUILT_GEOMETRY_COLUMN`` is the name),
    ``ST_GeomFromHEXWKB(``, and a function that decodes through the
    primitive ``geom_from_wkb_sql`` after its own ``"BLOB" in t`` test or into
    a view it builds (``resolve_geometry_read`` / ``create_geometry_view`` are
    that read and that view).

    The second pass (#1392 pass 2) made the narrowings precise: a conditional
    whose branches are both strings is a column choice when it lands in a
    geometry-named target or keyword (``None`` is not a string); every
    ``.get("primary_column")`` outside the owner is a hit (``geo_metadata``,
    T3a's block decoder, and the legitimate reads are ``[[G1.geometry_identity]]``
    entries with their reasons); a first-entry pick into a geometry-named target is a hit whatever
    the collection is called. New shapes: ``"geometry"`` as any positional
    argument (not an exception's parameter name, a mapping key, or a Click
    declaration) or any parameter's default, and a read-kind type test
    (``startswith("STRUCT")``, ``startswith(("BLOB", "VARCHAR"))``,
    ``"BLOB" in t``) outside the owner and ``duckdb_utils``; one that tests a
    non-geometry column is an entry with its reason.

    ``geometry_detection.resolve_geometry_read`` is the one BLOB-vs-native read
    expression, and the WKB codec is spelled once in ``duckdb_utils``
    (``as_wkb_sql`` / ``geom_from_wkb_sql``). What this does *not* make
    impossible: a decode that tests the type some other way (``"GEOMETRY" ==
    t``, a type set) before calling the primitive, or one that holds the
    primitive's result in a variable another function decodes with.

    Not seen (named so a reviewer looks for them): the literal held in a
    constant first (``DEFAULT = "geometry"; col = x or DEFAULT``), a default
    assigned to a name that says neither ``geom...col`` nor ``primary``
    (``col = "geometry"``), a names list of one geometry name or built by
    concatenation, an output column *named* ``geometry`` by an SQL alias or a
    subscript assignment, a first-column pick from a container whose name does
    not say ``geo..._col`` (``next(iter(inferred))``), a substring test without
    a case fold (``"geo" in name``, indistinguishable from list membership),
    and a codec call assembled by concatenation (``"ST_As" + "WKB("``).
    """
    seen: set[int] = set()
    for found in _geometry_identity_nodes(m):
        # One finding per line: two shapes of one expression are one copy.
        if found.lineno not in seen:
            seen.add(found.lineno)
            yield m.hit(found)


def _geometry_identity_nodes(m: _Module) -> Iterator[ast.AST]:
    """Every node one of the geometry-identity shapes flags (may repeat)."""
    wkb_owner = _owns(m.path, _WKB_CODEC_OWNERS)
    for node in ast.walk(m.tree):
        yield from (*_any_param_default(node), *_fallback_returns(node))
        for shaped in (
            _literal_column_default(node),
            _primary_literal(node),
            _ifexp_literal(m, node),
            _primary_get(m, node),
            _literal_column_kwarg(node),
            _positional_literal(node),
        ):
            if shaped is not None:
                yield shaped
        if _geometry_names_list(node) or _first_column_pick(m, node) or _substring_heuristic(node):
            yield node
        if wkb_owner:
            continue
        yield from _hand_rolled_read(m, node)
        if _type_test(m, node):
            yield node
        if id(node) in m.docstrings or isinstance(m.parents.get(id(node)), ast.JoinedStr):
            continue
        template = _string_template(node)
        if template is not None and _WKB_CODEC_CALL.search(template):
            yield node


_FOLD_METHODS = frozenset({"lower", "upper", "casefold"})
#: An identifier that says it holds a column name; a type, a colour or a collection
#: is not one.
_COLUMNISH = re.compile(r"(col|field)", re.IGNORECASE)
_NOT_COLUMNISH = re.compile(r"(type|colo|collect|collis|colon|encod)", re.IGNORECASE)
#: A loop over these hands its target a column name.
_COLUMN_SOURCE = re.compile(r"(col|field|schema|names)", re.IGNORECASE)
#: A bare name that is a column name when the function around it deals in columns.
_BARE_NAMES = frozenset({"name", "requested", "candidate", "base"})
#: ``schema.names`` / ``table.column_names``: the case-sensitive Arrow lists.
_ARROW_NAME_LISTS = frozenset({"names", "column_names"})
#: A parameter that carries a column name the user typed. Geometry names are
#: resolved by ``geometry_detection`` before they are tested, so they are not here.
_USER_COLUMN_NAME = re.compile(
    r"(column_name|bbox_column|bbox_column_name|requested|requested_col)"
)
#: SQL that folds a column: ``lower(name)``, ``UPPER(column_name)``.
_SQL_FOLD = re.compile(r"\b(lower|upper)\(\s*\"?\w*(col|field|name)", re.IGNORECASE)


def _ident(node: ast.AST) -> str:
    """The last identifier of ``x``, ``a.x`` or ``a[i]`` (``a``): '' for anything else."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        return _ident(node.value)
    return ""


def _root(node: ast.AST) -> ast.AST:
    """The value a chain of calls, attributes and subscripts is made from:
    ``str(col).strip()`` and ``field.strip().split(" ")[0]`` both come from a name."""
    while True:
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                node = node.func.value
            elif node.args:
                node = node.args[0]
            else:
                return node
        elif isinstance(node, (ast.Attribute, ast.Subscript)):
            node = node.value
        else:
            return node


def _says_column(name: str) -> bool:
    return bool(_COLUMNISH.search(name)) and not _NOT_COLUMNISH.search(name)


def _column_targets(m: _Module) -> set[tuple[int, str]]:
    """``(loop id, target)`` for each ``for``/comprehension target that iterates columns."""
    found: set[tuple[int, str]] = set()
    for node in ast.walk(m.tree):
        if isinstance(node, (ast.For, ast.comprehension)):
            source = ast.unparse(node.iter)
            if _COLUMN_SOURCE.search(source) and not _NOT_COLUMNISH.search(source):
                for name in ast.walk(node.target):
                    if isinstance(name, ast.Name):
                        found.add((id(node), name.id))
    return found


def _enclosing_loops(m: _Module, node: ast.AST) -> Iterator[ast.AST]:
    """The ``for`` statements and comprehension clauses *node* sits inside."""
    up = m.parents.get(id(node))
    while up is not None:
        if isinstance(up, ast.For):
            yield up
        yield from getattr(up, "generators", [])
        up = m.parents.get(id(up))


def _enclosing_function(m: _Module, node: ast.AST) -> ast.AST | None:
    up = m.parents.get(id(node))
    while up is not None and not isinstance(up, (ast.FunctionDef, ast.AsyncFunctionDef)):
        up = m.parents.get(id(up))
    return up


def _function_deals_in_columns(m: _Module, node: ast.AST) -> bool:
    """Whether the function around *node* names a column (a parameter or a local)."""
    func = _enclosing_function(m, node)
    if func is None:
        return False
    names = {n.id for n in ast.walk(func) if isinstance(n, ast.Name)}
    names |= {a.arg for a in ast.walk(func) if isinstance(a, ast.arg)}
    return any(_says_column(n) for n in names)


def _builds_suffix(value: ast.AST) -> bool:
    """``f"{base}_{n}"`` (any f-string joining two values), ``base + "_" + str(n)``
    and the right side of ``name += "_"``."""
    if isinstance(value, ast.JoinedStr):
        return sum(isinstance(v, ast.FormattedValue) for v in value.values) >= 2
    if isinstance(value, ast.BinOp) and isinstance(value.op, ast.Add):
        return any(
            isinstance(n, ast.Constant) and isinstance(n.value, str) and n.value.startswith("_")
            for n in ast.walk(value)
        )
    return isinstance(value, ast.Constant) and isinstance(value.value, str) and value.value == "_"


def _suffix_loop(m: _Module, node: ast.AST) -> bool:
    """A loop that re-suffixes a name until a test says it is free: a ``while``
    whose condition mentions the rebuilt name, or a ``for`` that returns or
    breaks on ``name not in taken``."""
    if not isinstance(node, (ast.While, ast.For)):
        return False
    inner = list(ast.walk(node))
    suffixed = {
        t.id
        for a in inner
        if isinstance(a, (ast.Assign, ast.AugAssign)) and _builds_suffix(a.value)
        for t in (a.targets if isinstance(a, ast.Assign) else [a.target])
        if isinstance(t, ast.Name)
    }
    if not suffixed:
        return False
    if isinstance(node, ast.While):
        return _mentions(node.test, suffixed)
    return any(
        isinstance(i, ast.If)
        and isinstance(i.test, ast.Compare)
        and any(isinstance(op, ast.NotIn) for op in i.test.ops)  # codespell:ignore
        and _mentions(i.test.left, suffixed)
        and any(isinstance(b, (ast.Return, ast.Break)) for b in i.body)
        for i in inner
    )


def _mentions(node: ast.AST, names: set[str]) -> bool:
    return any(isinstance(n, ast.Name) and n.id in names for n in ast.walk(node))


def _is_fold_call(node: ast.AST) -> bool:
    return isinstance(node, ast.Call) and _ident(node.func) == "fold"


def _fold_keyed(node: ast.AST) -> bool:
    """A lookup map keyed by ``fold(...)``: ``{fold(c): c ...}``, ``d[fold(c)] = c``,
    ``d.setdefault(fold(c), c)`` -- the owner's lookup, re-made."""
    if isinstance(node, ast.DictComp):
        return _is_fold_call(node.key)
    if isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Store):
        return _is_fold_call(node.slice)
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "setdefault"
        and bool(node.args)
        and _is_fold_call(node.args[0])
    )


def _exact_arrow_membership(node: ast.AST) -> bool:
    """``bbox_column_name in schema.names`` / ``column not in table.column_names``:
    a case-sensitive test of a name a user typed. A literal on the left is the
    code's own name, not the user's."""
    if not isinstance(node, ast.Compare) or isinstance(node.left, ast.Constant):
        return False
    return any(
        isinstance(op, (ast.In, ast.NotIn))  # codespell:ignore
        and isinstance(right, ast.Attribute)
        and right.attr in _ARROW_NAME_LISTS
        for op, right in zip(node.ops, node.comparators, strict=True)
    ) and bool(_USER_COLUMN_NAME.fullmatch(_ident(node.left)))


def _str_method_fold(node: ast.AST) -> bool:
    """``str.lower(col)`` and ``map(str.lower, columns)``."""

    def is_str_fold(n: ast.AST) -> bool:
        return (
            isinstance(n, ast.Attribute)
            and n.attr in _FOLD_METHODS
            and isinstance(n.value, ast.Name)
            and n.value.id == "str"
        )

    if not isinstance(node, ast.Call):
        return False
    if is_str_fold(node.func):
        return bool(node.args) and _says_column(_ident(_root(node.args[0])))
    return (
        isinstance(node.func, ast.Name)
        and node.func.id == "map"
        and len(node.args) >= 2
        and is_str_fold(node.args[0])
        and bool(_COLUMN_SOURCE.search(ast.unparse(node.args[1])))
    )


def _ascii_fold_table(node: ast.AST) -> bool:
    """``str.maketrans(string.ascii_uppercase, string.ascii_lowercase)``: a fold table."""
    return (
        isinstance(node, ast.Call)
        and _ident(node.func) == "maketrans"
        and any(
            isinstance(n, ast.Attribute) and n.attr in {"ascii_uppercase", "ascii_lowercase"}
            for n in ast.walk(node)
        )
    )


def _names_a_type(chain: ast.AST) -> bool:
    """A chain that reads a ``"type"`` / ``"repetition_type"`` entry: a type string, not a name."""
    return any(
        isinstance(n, ast.Constant) and isinstance(n.value, str) and "type" in n.value
        for n in ast.walk(chain)
    )


def _fold_method_on_column(m: _Module, node: ast.AST, targets) -> bool:
    if not (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in _FOLD_METHODS
    ):
        return False
    root = _root(node.func.value)
    if _names_a_type(node.func.value):
        return False
    if _says_column(_ident(node.func.value)) or _says_column(_ident(root)):
        return True
    if not isinstance(root, ast.Name) or _NOT_COLUMNISH.search(root.id):
        return False
    if any((id(loop), root.id) in targets for loop in _enclosing_loops(m, node)):
        return True
    return root.id in _BARE_NAMES and _function_deals_in_columns(m, node)


def _find_column_fold(m: _Module) -> Iterator[Hit]:
    """A case fold applied to a column name, or a hand-written name suffixer or
    lookup map, outside ``core/column_selection.py`` (#1367 concept 12).

    ``column_selection.fold`` is the one rule (DuckDB's: ASCII only),
    ``find_column`` the one lookup (a name that two columns fold to is refused),
    ``matching_columns`` / ``has_column`` / ``same_column`` its other forms and
    ``unique_name`` the one suffixer. Would have caught the eleven folding sites
    (some ``.lower()``, some ASCII, one exact-only) that disagreed about whether
    ``--bucket-point PT`` meant ``pt`` (#1109), and the three suffixers that
    started at ``_1``, ``_2`` and ``_1`` with three notions of "taken".

    What it sees: ``.lower()`` / ``.upper()`` / ``.casefold()`` on a name that
    says it is a column or field, on the root of a call chain (``str(col)
    .strip().lower()``), on the target of a loop or comprehension over columns,
    names, fields or a schema, and on ``name`` / ``requested`` / ``base`` /
    ``candidate`` inside a function that deals in columns; ``str.lower(col)`` and
    ``map(str.lower, columns)``; a ``maketrans`` fold table; SQL ``lower(name)``
    in a query string; a ``while``/``for`` that rebuilds ``f"{base}_{n}"``,
    ``f"{base}{n}"``, ``base + "_" + str(n)`` or ``name += "_"`` until a
    membership test says the name is free; a lookup map keyed by ``fold(...)``
    (``{fold(c): c}``, ``d[fold(c)] = c``, ``d.setdefault(fold(c), c)``); and a
    case-sensitive ``column in schema.names`` / ``table.column_names``.

    Not seen (named so a reviewer looks for them): a fold of a name held in a
    variable that says nothing and is not a loop target, a suffix built with
    ``%`` or ``.format``, and a lookup keyed by a fold held in a variable first.
    """
    targets = _column_targets(m)
    for node in ast.walk(m.tree):
        if (
            _suffix_loop(m, node)
            or _fold_method_on_column(m, node, targets)
            or _str_method_fold(node)
            or _ascii_fold_table(node)
            or _fold_keyed(node)
            or _exact_arrow_membership(node)
        ):
            yield m.hit(node)
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in m.docstrings
            and _SQL_FOLD.search(node.value)
        ):
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
    "sql_escape_hand_quote": Arm(
        _find_sql_escape_hand_quote, ("geoparquet_io/core/duckdb_utils.py",)
    ),
    "cgroup": Arm(_find_cgroup, ("geoparquet_io/core/memory_limits.py",)),
    "duckdb_version": Arm(_find_duckdb_version, ("geoparquet_io/core/duckdb_utils.py",)),
    "parquet_metadata": Arm(_find_parquet_metadata, ("geoparquet_io/core/duckdb_metadata.py",)),
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
    "row_group_policy": Arm(_find_row_group_policy, _ROW_GROUP_OWNER),
    "scheme_test": Arm(_find_scheme_test, ("geoparquet_io/core/remote.py",), needs_repo=True),
    "overwrite_check": Arm(_find_overwrite_check, ("geoparquet_io/core/file_utils.py",)),
    "output_version": Arm(_find_output_version, _VERSION_OWNERS),
    "geometry_stats": Arm(_find_geometry_stats, ("geoparquet_io/core/geo_metadata.py",)),
    "edges": Arm(_find_edges, ("geoparquet_io/core/edges.py",)),
    "footer_patch": Arm(_find_footer_patch, ("geoparquet_io/core/parquet_footer.py",)),
    "column_fold": Arm(
        _find_column_fold,
        ("geoparquet_io/core/column_selection.py",),
    ),
    "geometry_identity": Arm(
        _find_geometry_identity, ("geoparquet_io/core/geometry_detection.py",)
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
    "fsspec": ("geoparquet_io/core/remote.py",),
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
    counts["G13.bbox_parsers"] = len(data["G13"].get("bbox_parsers", []))
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
    "sql_escape_hand_quote": (
        "from geoparquet_io.core.duckdb_utils import _escape_sql_string\n\n"
        "def where(v):\n    return f\"name = '{_escape_sql_string(v)}'\"\n"
    ),
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
    "row_group_policy": "rows = row_group_rows or 50000\n",
    "parquet_metadata": (
        "def rows(con, path):\n"
        "    return con.execute(f\"SELECT * FROM parquet_metadata('{path}')\").fetchall()\n"
    ),
    "scheme_test": 'def remote(p):\n    return p.startswith("s3://")\n',
    "output_version": ('def version(md):\n    return sanitized_carried_geo(md).get("version")\n'),
    "edges": 'def spherical(col):\n    return col.get("edges") is not None\n',
    "column_fold": "def same(col, other):\n    return col.lower() == other.lower()\n",
    "footer_patch": (
        "from geoparquet_io.core import parquet_footer\n\n"
        "def add(path, rewrite):\n    try:\n        rewrite(path)\n"
        "    except parquet_footer.FooterPatchUnsupported:\n        rewrite(path)\n"
    ),
    "overwrite_check": (
        "import os\n\ndef check(out, force):\n"
        "    if os.path.exists(out) and not force:\n        raise ValueError(out)\n"
    ),
    "geometry_identity": ('def column(geo):\n    return geo.get("primary_column") or "geometry"\n'),
    "geometry_stats": (
        "def bounds(con, q, g):\n"
        '    return con.execute(f"SELECT MIN(ST_XMin({g})), MAX(ST_XMax({g})) FROM ({q})")\n'
    ),
}

#: Near-misses each arm must not flag, so a planted hit is the pattern itself.
G1_CLEAN = {
    "geo_parse": "import json\n\ndef read(text):\n    return json.loads(text)\n",
    "set_memory_limit": '"""Never SET memory_limit here."""\n',
    "sql_escape": 'def esc(v):\n    return v.replace("a", "b")\n',
    "sql_escape_hand_quote": (
        "from geoparquet_io.core.duckdb_utils import sql_literal\n\n"
        'def where(v):\n    return f"name = {sql_literal(v)}"  # not _escape_sql_string\n'
    ),
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
    "row_group_policy": "rows = resolve_row_group_rows(row_group_rows, row_group_size_mb)\n",
    "parquet_metadata": (
        '"""Reads parquet_metadata() through the owner."""\n'
        "from geoparquet_io.core.duckdb_metadata import get_row_group_sizes\n"
    ),
    "scheme_test": (
        'def url(bucket, path):\n    scheme, _, rest = path.partition("://")\n'
        '    return f"s3://{bucket}/{rest}"\n'
    ),
    "output_version": ('def version(path):\n    return read_raw_geo(path).value.get("version")\n'),
    "column_fold": (
        "from geoparquet_io.core.column_selection import same_column\n\n"
        "def same(col, other):\n    return same_column(col, other)\n"
        "def kind(col_type):\n    return col_type.upper()\n"
        "def ext(path):\n    return path.suffix.lower()\n"
        "def label(name):\n    return name.lower()  # not said to be a column\n"
    ),
    "edges": (
        "from geoparquet_io.core.edges import is_nonplanar, edges_of\n\n"
        'def spherical(col):\n    return is_nonplanar(edges_of(col))  # not "spherical"\n'
        'REPORT = {"edges": None, "note": "spherical edges"}\n'
    ),
    "footer_patch": (
        "from geoparquet_io.core.parquet_footer import patch_or_rewrite\n\n"
        "def add(path, rewrite):\n"
        '    return patch_or_rewrite(path, {"k": "v"}, rewrite=rewrite)  # not patch_footer(\n'
    ),
    "overwrite_check": (
        "import os\n\ndef cleanup(tmp):\n    if os.path.exists(tmp):\n        os.remove(tmp)\n"
    ),
    "geometry_identity": (
        "from geoparquet_io.core.geometry_detection import primary_geometry_column\n\n"
        "def column(path):\n    return primary_geometry_column(path)\n"
    ),
    "geometry_stats": (
        "def bbox_column(g):\n"
        '    return f"STRUCT_PACK(xmin := ST_XMin({g}), ymax := ST_YMax({g})) AS bbox"\n'
    ),
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


#: Every spelling of the hand-written patch-else-rewrite the #1367 copies used
#: (T2d), and the one-hop dodges around it, each with the hits it must raise.
FOOTER_PATCH_SHAPES = {
    "old kv patch, imported and called": (
        "from geoparquet_io.core.parquet_footer import patch_footer_kv\n\n"
        'def add(p):\n    patch_footer_kv(p, {"k": "v"})\n',
        1,
    ),
    "private patch under an alias": (
        "from geoparquet_io.core.parquet_footer import _patch_footer_file as patch\n\n"
        'def add(p):\n    return patch(p, {"k": "v"})\n',
        1,
    ),
    "private patch as a module attribute": (
        "from geoparquet_io.core import parquet_footer as pf\n\n"
        "def add(p):\n    return pf._patch_footer_file(p, None)\n",
        1,
    ),
    "private patch passed, not called": (
        "from geoparquet_io.core import parquet_footer\n\n"
        "PATCH = parquet_footer._patch_footer_file\n",
        1,
    ),
    "private patch by getattr": (
        "from geoparquet_io.core import parquet_footer\n\n"
        'NAME = "_patch_footer_file"\n\n'
        "def add(p):\n    return getattr(parquet_footer, NAME)(p)\n",
        1,
    ),
    "private fallback imported": (
        "from geoparquet_io.core.parquet_footer import _fall_back\n",
        1,
    ),
    "refusal imported": (
        "from geoparquet_io.core.parquet_footer import FooterPatchRaced as Raced\n",
        1,
    ),
    "race caught as an attribute": (
        "from geoparquet_io.core import parquet_footer as pf\n\n"
        "def add(p, f):\n    try:\n        f(p)\n    except pf.FooterPatchRaced:\n        f(p)\n",
        1,
    ),
    "refusal in a tuple": (
        "def add(p, f):\n    try:\n        f(p)\n"
        "    except (OSError, FooterPatchUnsupported):\n        return None\n",
        1,
    ),
    "everything caught around the owner": (
        "from geoparquet_io.core.parquet_footer import patch_or_rewrite\n\n"
        "def add(p, rewrite):\n    try:\n"
        '        patch_or_rewrite(p, {"k": "v"}, rewrite=rewrite)\n'
        "    except Exception:\n        return None\n",
        1,
    ),
    "a bare except around the owner": (
        "from geoparquet_io.core.parquet_footer import patch_or_rewrite\n\n"
        "def add(p, rewrite):\n    try:\n"
        '        patch_or_rewrite(p, {"k": "v"}, rewrite=rewrite)\n'
        "    except:  # noqa: E722\n        return None\n",
        1,
    ),
    "the fallback called directly": (
        "from geoparquet_io.core.write_funnels import rewrite_footer_kv\n\n"
        'def add(p):\n    rewrite_footer_kv(p, p, {"k": "v"})\n',
        1,
    ),
    "the fallback called as a module attribute": (
        "from geoparquet_io.core import write_funnels as wf\n\n"
        'def add(p):\n    wf.rewrite_footer_kv(p, p, {"k": "v"}, compression="GZIP")\n',
        1,
    ),
    "refusal's base caught around the owner": (
        "from geoparquet_io.core.exceptions import GeoParquetError\n"
        "from geoparquet_io.core.parquet_footer import patch_or_rewrite\n\n"
        "def add(p, rewrite):\n    try:\n"
        '        patch_or_rewrite(p, {"k": "v"}, rewrite=rewrite)\n'
        "    except GeoParquetError:\n        return None\n",
        1,
    ),
}
#: ...and the look-alikes it must not.
FOOTER_PATCH_NEAR_MISSES = {
    "the owner's entry point": (
        "from geoparquet_io.core import parquet_footer\n"
        "from geoparquet_io.core.parquet_footer import patch_or_rewrite\n\n"
        "def add(p, rewrite):\n"
        '    parquet_footer.patch_or_rewrite(p, {"k": "v"}, rewrite=rewrite)\n'
        '    return patch_or_rewrite(p, {"k": "v"}, rewrite=rewrite)\n'
    ),
    "cleanup that re-raises around the owner": (
        "from geoparquet_io.core.parquet_footer import patch_or_rewrite\n\n"
        "def add(p, rewrite, tmp):\n    try:\n"
        '        patch_or_rewrite(p, {"k": "v"}, output_file=tmp, rewrite=rewrite)\n'
        "    except BaseException:\n        tmp.unlink()\n        raise\n"
    ),
    "the fallback handed to the owner": (
        "import functools\n\n"
        "from geoparquet_io.core.parquet_footer import patch_or_rewrite\n"
        "from geoparquet_io.core.write_funnels import rewrite_footer_kv\n\n"
        "def add(p):\n"
        '    return patch_or_rewrite(p, {"k": "v"}, rewrite=rewrite_footer_kv)\n\n'
        "def add_gzip(p):\n"
        '    fallback = functools.partial(rewrite_footer_kv, compression="GZIP")\n'
        '    return patch_or_rewrite(p, {"k": "v"}, rewrite=fallback)\n'
    ),
    "Exception caught around something else": (
        "def add(p, f):\n    try:\n        f(p)\n    except Exception:\n        return None\n"
    ),
    "GeoParquetError caught around something else": (
        "from geoparquet_io.core.exceptions import GeoParquetError\n\n"
        "def add(p, f):\n    try:\n        f(p)\n    except GeoParquetError:\n        return None\n"
    ),
    "a same-named function of another module": (
        "def _patch_footer(footer):\n    return footer\n\n"
        "def _fall_back_to_default(x):\n    return x\n"
    ),
}


@pytest.mark.parametrize("shape", sorted(FOOTER_PATCH_SHAPES))
def test_g12_footer_patch_arm_flags_every_shape(shape, tmp_path):
    source, expected = FOOTER_PATCH_SHAPES[shape]
    repo = _plant(tmp_path, "footer_patch", source)
    assert len(arm_hits(repo, "footer_patch")) == expected, arm_hits(repo, "footer_patch")


@pytest.mark.parametrize("shape", sorted(FOOTER_PATCH_NEAR_MISSES))
def test_g12_footer_patch_arm_leaves_the_owners_callers_alone(shape, tmp_path):
    repo = _plant(tmp_path, "footer_patch", FOOTER_PATCH_NEAR_MISSES[shape])
    assert arm_hits(repo, "footer_patch") == []


#: The other shapes of a scheme test the arm must see (PR #1387 review).
SCHEME_TEST_SHAPES = {
    "single-name compare": 'def is_s3(p):\n    return url_scheme(p) == "s3"\n',
    "reversed compare": 'def not_gs(p):\n    return "gs" != url_scheme(p)\n',
    "spelling dict": 'SPELLING = {"s3": "s3", "gcs": "gs"}\n',
}
#: ...and the look-alikes it must not.
SCHEME_TEST_NEAR_MISSES = {
    "format compare": 'def is_csv(fmt):\n    return fmt == "csv"\n',
    "mixed-key dict": 'OPTS = {"s3": 1, "format": "parquet"}\n',
    "family helper": "def is_s3(p):\n    return is_s3_url(p)\n",
}


@pytest.mark.parametrize("shape", sorted(SCHEME_TEST_SHAPES))
def test_g12_scheme_arm_flags_every_shape(shape, tmp_path):
    repo = _plant(tmp_path, "scheme_test", SCHEME_TEST_SHAPES[shape])
    assert len(arm_hits(repo, "scheme_test")) == 1


@pytest.mark.parametrize("shape", sorted(SCHEME_TEST_NEAR_MISSES))
def test_g12_scheme_arm_passes_its_look_alikes(shape, tmp_path):
    repo = _plant(tmp_path, "scheme_test", SCHEME_TEST_NEAR_MISSES[shape])
    assert arm_hits(repo, "scheme_test") == []


def test_g12_scheme_arm_fallback_reads_the_owner_table(tmp_path):
    from geoparquet_io.core.remote import REMOTE_SCHEMES

    assert _remote_scheme_names(tmp_path) == frozenset(REMOTE_SCHEMES)


#: Every spelling of a stats aggregate the #1367 sweep found (T3b), each of
#: which the geometry_stats arm must see...
STATS_SHAPES = {
    "ST_Extent_Agg": 'Q = "SELECT ST_XMin(ST_Extent_Agg(g)) FROM t"\n',
    "lower-case min over a line break": 'Q = """SELECT min(\n    ST_YMin(g)) FROM t"""\n',
    "aggregate over a bbox struct field": (
        'def q(b):\n    return f"SELECT MIN({b}.xmin), MAX({b}.ymax) FROM t"\n'
    ),
    "aggregate over a named struct field": 'Q = "SELECT max(bbox.xmax) FROM t"\n',
    "FILTER on the aggregate": 'Q = "SELECT MIN(ST_XMin(g)) FILTER (WHERE NOT ST_IsEmpty(g)) FROM t"\n',
    "distinct types": 'def q(c):\n    return f"SELECT DISTINCT ST_GeometryType({c}) FROM t"\n',
    "types grouped": 'Q = "SELECT ST_GeometryType(g), COUNT(*) FROM t GROUP BY 1"\n',
    "aggregate over point coordinates": 'Q = "SELECT MIN(ST_X(g)), 1 FROM t"\n',
    "types listed": 'Q = "SELECT list(ST_GeometryType(g)) FROM t"\n',
    "python fold over row-group dicts": (
        'def extent(boxes):\n    return min(b["xmin"] for b in boxes)\n'
    ),
    "python fold over attributes": "def top(stats):\n    return max([s.ymax for s in stats])\n",
    "pyarrow min_max": "def lo(pc, arr):\n    return pc.min_max(arr)\n",
}
#: ...and the per-row and probe shapes it must leave alone.
STATS_NEAR_MISSES = {
    "per-row bbox column": 'Q = "SELECT STRUCT_PACK(xmin := ST_XMin(g)) AS bbox FROM t"\n',
    "per-row predicate": 'def q(g):\n    return f"ST_XMin({g}) >= 0 AND ST_XMax({g}) <= 1"\n',
    "first-row type probe": 'Q = "SELECT ST_GeometryType(g) FROM t WHERE g IS NOT NULL LIMIT 1"\n',
    "window over part extents": 'Q = "SELECT lo, max(hi) OVER (ORDER BY lo) FROM parts"\n',
    "prose": 'def f():\n    """Uses MIN(ST_XMin(g)) through the owner."""\n',
    "python min of plain numbers": "def lo(values):\n    return min(v for v in values)\n",
    "python min of a non-bbox field": 'def first(rows):\n    return min(r["id"] for r in rows)\n',
}


@pytest.mark.parametrize("shape", sorted(STATS_SHAPES))
def test_g12_geometry_stats_arm_flags_every_shape(shape, tmp_path):
    repo = _plant(tmp_path, "geometry_stats", STATS_SHAPES[shape])
    assert len(arm_hits(repo, "geometry_stats")) == 1, STATS_SHAPES[shape]


@pytest.mark.parametrize("shape", sorted(STATS_NEAR_MISSES))
def test_g12_geometry_stats_arm_passes_its_look_alikes(shape, tmp_path):
    repo = _plant(tmp_path, "geometry_stats", STATS_NEAR_MISSES[shape])
    assert arm_hits(repo, "geometry_stats") == []


#: Every shape of a literal ``"geometry"`` column default and of a WKB codec
#: call the #1367 T3c sweep found, each of which the arm must see...
GEOMETRY_IDENTITY_SHAPES = {
    "or-fallback": 'def f(t):\n    return find(t) or "geometry"\n',
    "get-default": 'def f(geo):\n    return geo.get("primary_column", "geometry")\n',
    "parameter default": 'def f(t, geometry_column="geometry"):\n    return t\n',
    "annotated parameter default": 'def f(t, *, geom_col: str = "geometry"):\n    return t\n',
    "assigned fallback": 'def f(c):\n    if not c:\n        geom_col = "geometry"\n',
    "sentinel compare": 'def f(geometry_column):\n    return geometry_column == "geometry"\n',
    "fallback return": (
        'def f(geo):\n    if geo:\n        return geo["primary_column"]\n    return "geometry"\n'
    ),
    "decode in an f-string": 'def f(q):\n    return f"ST_GeomFromWKB({q})"\n',
    "encode in a plain string": 'Q = "SELECT ST_AsWKB(g) FROM t"\n',
    "lower-case codec": 'Q = "select st_aswkb (g) from t"\n',
    "second names list": 'NAMES = ["geometry", "geom", "the_geom", "geo"]\n',
    "names tuple in a loop": 'def f(cols):\n    for n in ("shape", "geography"):\n        pass\n',
    "primary literal": 'def f(cols):\n    primary = "geometry"\n    return primary\n',
    "annotated primary literal": 'def f():\n    primary_column: str = "geom"\n',
    # The audit's eight blind spots (#1392 pass 1).
    "conditional literal": 'def f(t, x):\n    return "geometry" if x else t\n',
    "conditional literal else": 'def f(t, x):\n    return t if x else "geometry"\n',
    "first native column": "def f(p):\n    return next(iter(detect_geometry_columns(p)))\n",
    "first of the geo columns": "def f(geo_columns):\n    return geo_columns[0]\n",
    "BLOB test then the primitive": (
        'def f(t, q):\n    return geom_from_wkb_sql(q) if "BLOB" in t else q\n'
    ),
    "view through the primitive": (
        "def f(con, c):\n"
        '    con.execute(f"CREATE VIEW v AS SELECT {geom_from_wkb_sql(c)} AS g FROM t")\n'
    ),
    "inline block primary": (
        'def f(geo, geometry_column=None):\n    geometry_column = geo.get("primary_column")\n'
    ),
    "hex WKB codec": 'Q = "SELECT ST_GeomFromHEXWKB(h) FROM t"\n',
    "substring heuristic": (
        'def f(fields):\n    return [n for n in fields if "geo" in n.lower()]\n'
    ),
    "keyword naming a built column": 'def f(t):\n    return sort(t, geometry_column="geometry")\n',
    # The audit's second pass (#1392 pass 2).
    "conditional with None": 'def f(has):\n    geometry_column = "geometry" if has else None\n',
    "two-name conditional into a geometry name": (
        'def f(legacy):\n    geom_col = "geom" if legacy else "geometry"\n'
    ),
    "block primary into its own name": (
        'def f(geo):\n    primary = geo.get("primary_column")\n    return primary\n'
    ),
    "decoded block primary": (
        "import json\n\ndef f(meta):\n"
        '    geo_info = json.loads(meta[b"geo"])\n'
        '    geometry_column = geo_info.get("primary_column")\n'
    ),
    "first of the columns into a geometry name": "def f(columns):\n    geom_col = columns[0]\n",
    "next of inferred into a geometry name": (
        "def f(inferred_columns):\n    primary = next(iter(inferred_columns))\n"
    ),
    "positional literal": 'def f(src):\n    return bounds_of(src, "geometry")\n',
    "a geometry name quoted as an identifier": 'def f():\n    return quote_identifier("the_geom")\n',
    "any parameter default": 'def f(cols, base="geometry"):\n    return base\n',
    "STRUCT prefix test": 'def f(t):\n    return t.upper().startswith("STRUCT")\n',
    "BLOB/VARCHAR prefix test": 'def f(t):\n    return not t.startswith(("BLOB", "VARCHAR"))\n',
    "BLOB substring test": 'def f(t):\n    return "BLOB" in t\n',
}
#: ...and the output names and prose it must leave alone.
GEOMETRY_IDENTITY_NEAR_MISSES = {
    "output column alias": 'Q = "SELECT x AS geometry FROM t"\n',
    "keyword naming the built constant": (
        "def f(t):\n    return sort(t, geometry_column=BUILT_GEOMETRY_COLUMN)\n"
    ),
    "enum value": "def f(t, bucket_point: str = BUCKET_POINT_GEOMETRY):\n    return t\n",
    "output name parameter": "def f(t, geometry_name=BUILT_GEOMETRY_COLUMN):\n    return t\n",
    "rename to the output name": 'def f(names, i):\n    names[i] = "geometry"\n',
    "single return of a dataset fact": 'def get_geometry_column():\n    return "geometry"\n',
    "type compare": 'def f(d):\n    return d.get("type") == "geometry"\n',
    "prose": 'def f():\n    """Calls ST_AsWKB(g) through the owner; falls back to "geometry"."""\n',
    "codec named without a call": 'MSG = "(the query uses ST_AsWKB for geometry columns)"\n',
    "one geometry-ish name": 'def f():\n    return load(extensions=["geography"])\n',
    "type names list": 'TYPES = ["Point", "LineString", "Polygon"]\n',
    "logical type families": 'def f(t):\n    return t in ("Geometry", "Geography")\n',
    "list built from the owner's names": 'def f():\n    return ["wkt", *STANDARD_GEOMETRY_NAMES]\n',
    "primary set to nothing": "def f(cols):\n    primary = None\n",
    "conditional on other names": 'def f(x):\n    return "geom" if x else "point"\n',
    "a word for a message": 'def f(n):\n    return "geometry" if n == 1 else "geometries"\n',
    "block read inside the decoder's owner": ('def f(geo):\n    return geo.get("primary")\n'),
    "first of anything else": "def f(rows):\n    return next(iter(rows)), rows[0]\n",
    "BLOB in a type list": 'def f(t):\n    return t in ("BLOB", "VARCHAR")\n',
    "positional built constant": "def f(s):\n    return bounds_of(s, BUILT_GEOMETRY_COLUMN)\n",
    "a parameter named geometry": (
        'def f():\n    raise InvalidParameterError("geometry", "needs a column")\n'
    ),
    "first of anything into its own name": "def f(rows):\n    first = rows[0]\n",
    "a message conditional into a non-geometry kwarg": (
        'def f(n):\n    return say(word="geometry" if n == 1 else "geometries")\n'
    ),
    "primitive without a type test": "def f(c):\n    return geom_from_wkb_sql(c)\n",
    "view without the primitive": 'def f(con):\n    con.execute("CREATE VIEW v AS SELECT * FROM t")\n',
    "list membership": 'def f(names):\n    return "geo" in names\n',
    "substring of a non-geo fragment": 'def f(n):\n    return "bbox" in n.lower()\n',
}


@pytest.mark.parametrize("shape", sorted(GEOMETRY_IDENTITY_SHAPES))
def test_g12_geometry_identity_arm_flags_every_shape(shape, tmp_path):
    repo = _plant(tmp_path, "geometry_identity", GEOMETRY_IDENTITY_SHAPES[shape])
    assert len(arm_hits(repo, "geometry_identity")) == 1, GEOMETRY_IDENTITY_SHAPES[shape]


@pytest.mark.parametrize("shape", sorted(GEOMETRY_IDENTITY_NEAR_MISSES))
def test_g12_geometry_identity_arm_passes_its_look_alikes(shape, tmp_path):
    repo = _plant(tmp_path, "geometry_identity", GEOMETRY_IDENTITY_NEAR_MISSES[shape])
    assert arm_hits(repo, "geometry_identity") == []


def test_g12_geometry_identity_arm_lets_duckdb_utils_spell_the_codec_only(tmp_path):
    """``duckdb_utils`` owns the codec spelling, not the column default."""
    target = tmp_path / "geoparquet_io" / "core" / "duckdb_utils.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        'def as_wkb_sql(e):\n    return f"ST_AsWKB({e})"\n\n'
        'def col(c):\n    return c or "geometry"\n',
        encoding="utf-8",
    )
    hits = arm_hits(tmp_path, "geometry_identity")
    assert [h.name for h in hits] == ["col"], hits


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


def test_g12_geo_parse_ignores_geo_in_a_callee_name(tmp_path):
    """The footer key, not the token ``geo`` in the name of a function whose result is parsed.

    ``crs_utils._crs_from_read_type`` parses the CRS out of
    ``parse_duckdb_geo_type(t)``; the key read by subscript or ``.get`` still flags.
    """
    clean = (
        "import json\n\ndef crs(t):\n    parsed = parse_duckdb_geo_type(t)\n"
        "    value = parsed[1]\n    return json.loads(value)\n"
    )
    assert arm_hits(_plant(tmp_path / "a", "geo_parse", clean), "geo_parse") == []
    for source in (
        "import json\n\ndef read(md):\n    raw = md.get(b'geo')\n    return json.loads(raw)\n",
        "import json\n\ndef read(geo_bytes):\n    return json.loads(geo_bytes)\n",
    ):
        repo = _plant(tmp_path / str(len(source)), "geo_parse", source)
        assert len(arm_hits(repo, "geo_parse")) == 1, source


@pytest.mark.parametrize(
    "request_line",
    ["return make_client().get(x)", "return c.fetch(build_url(x))"],
    ids=["client-factory-receiver", "url-builder-argument"],
)
def test_g12_retry_loop_reads_a_callee_name(tmp_path, request_line):
    """The retry arms read a callee's name (a client factory, a URL builder);
    only geo_parse ignores callees, so its fix must not blind them."""
    source = (
        "import time\n\ndef fetch(c, x):\n    for attempt in range(3):\n        try:\n"
        f"            {request_line}\n"
        "        except Exception:\n            time.sleep(attempt)\n"
    )
    repo = _plant(tmp_path, "retry_loop", source)
    assert arm_hits(repo, "retry_loop"), source


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
    ["_detect_version_from_table", "_crs_matches", "_validated_bbox"],
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
        "sql_escape_hand_quote",
        "escaped value quoted lines later",
        "from geoparquet_io.core.duckdb_utils import _escape_sql_string as esc\n"
        "def q(crs):\n    lit = esc(crs)\n    return f\"ST_Transform(g, '{lit}', 'EPSG:4326')\"\n",
    ),
    (
        "sql_escape_hand_quote",
        "through the module",
        "from geoparquet_io.core import duckdb_utils\n"
        'def q(v):\n    return "\'" + duckdb_utils._escape_sql_string(v) + "\'"\n',
    ),
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
    (
        "sql_escape_hand_quote",
        "the quotes cut off sql_path",
        "from geoparquet_io.core.duckdb_utils import sql_path\n"
        "def q(p, t):\n    return t.replace('{file}', sql_path(p)[1:-1])\n",
    ),
    (
        "sql_escape_hand_quote",
        "an index into sql_literal through the module",
        "from geoparquet_io.core import duckdb_utils\n"
        'def q(v):\n    return "\'" + duckdb_utils.sql_literal(v)[1:-1] + "\'"\n',
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
    (
        "sql_escape_hand_quote",
        "a subscript inside the sql_path argument",
        "from geoparquet_io.core.duckdb_utils import sql_path\n"
        'def q(paths):\n    return f"FROM {sql_path(paths[0])}"\n',
    ),
    (
        "sql_escape_hand_quote",
        "a subscript of another call",
        "def q(p):\n    return str(p)[1:-1]\n",
    ),
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


#: Second row-group policies the arm must catch, and owner answers it must not.
ROW_GROUP_POLICIES = [
    "x = 49152\n",
    "pq.write_table(t, p, row_group_size=100_000)\n",
    "writer.write_table(t, row_group_size=10000)\n",
    "con.execute('COPY t TO f (ROW_GROUP_SIZE 122880)')\n",
    "batch_size = row_group_rows or DEFAULT_BATCH_SIZE\n",
    "rows = self.row_group_rows or 50000\n",
    "rows = resolve_row_group_rows(None, None) or DEFAULT_ROW_GROUP_ROWS\n",
    "DEFAULT_BATCH_SIZE = 100_000\n",
    "ROW_GROUP_ROWS: int = 50_000\n",
]
ROW_GROUP_OWNER_ANSWERS = [
    "rows = resolve_row_group_rows(row_group_rows, row_group_size_mb)\n",
    "writer.write_table(t, row_group_size=rows_per_group)\n",
    "options.append(f'ROW_GROUP_SIZE {row_group_rows}')\n",
    "_LINEARIZE_BATCH_ROWS = 20_000\n",
    "count = len(groups) or 1\n",
    "chunks = get_native_geo_stats_by_row_group(path, col) or []\n",
    "if row_group_rows or row_group_size_mb:\n    pass\n",
    '"""A docstring may say ROW_GROUP_SIZE 100000."""\n',
]


@pytest.mark.parametrize("source", ROW_GROUP_POLICIES)
def test_g12_the_row_group_arm_catches_a_second_policy(tmp_path, source):
    assert arm_hits(_plant(tmp_path, "row_group_policy", source), "row_group_policy"), source


@pytest.mark.parametrize("source", ROW_GROUP_OWNER_ANSWERS)
def test_g12_the_row_group_arm_leaves_owner_answers_alone(tmp_path, source):
    assert arm_hits(_plant(tmp_path, "row_group_policy", source), "row_group_policy") == [], source


#: The copies of the output-version decision #1367 found (concept 1), each a
#: shape the arm must flag...
OUTPUT_VERSION_COPIES = [
    'def passthrough(md):\n    return sanitized_carried_geo(md).get("version")\n',
    'def detect(t):\n    geo = sanitized_carried_geo(t.schema.metadata)\n    return geo["version"]\n',
    'def detect(md):\n    return carried_version(sanitized_carried_geo(md).get("version"))\n',
    'v = resolve_output_geoparquet_version(req, input_file=p) or "1.1"\n',
    "v = geoparquet_version or DEFAULT_GEOPARQUET_VERSION\n",
    'v = geo.get("version", "1.1.0")\n',
    'def advice(p):\n    geo = sanitized_carried_geo_of_file(p)\n    return geo.get("version", "")\n',
    # The first audit's probes (each passed the first arm at 0):
    'v = version or "1.1"\n',
    'v = info.get("geo_version") or "1.0.0"\n',
    "shown = geo_version or '2.0'\n",
    'def read(geo_meta):\n    return geo_meta["version"]\n',
    'gp_version = "1.1"\n',
    'if geo_version == "1.0.0":\n    pass\n',
    'def copy_to(path, geoparquet_version: str = "1.1"):\n    return path\n',
    'def copy_to(path, *, geoparquet_version="2.0"):\n    return path\n',
    'cfg = GEOPARQUET_VERSIONS.get(v, GEOPARQUET_VERSIONS["1.1"])\n',
    'mv = cfg.get("metadata_version", "1.1.0")\n',
    'keep = (declared_geo_version or "").startswith("1.0")\n',
    'native = file_info["geo_version"].startswith("2.")\n',
    # The second audit's two shapes:
    'MAP = {"geoparquet_v2": "2.0", "parquet_geo_only": "2.0", "geoparquet_v1": "1.1"}\n',
    'def pick(ft):\n    if ft == "geoparquet_v2":\n        return "2.0"\n    return None\n',
    'def pick(info):\n    if info["file_type"] == "parquet_geo_only":\n'
    '        gp_version = "parquet-geo-only"\n    return gp_version\n',
    'v = "parquet-geo-only" if ft == "parquet_geo_only" else None\n',
    'write(table, out, geoparquet_version="1.1")\n',
    'write(table, out, output_geoparquet_version="2.0")\n',
]
#: ...and the readers it must leave alone: reporting a declared version,
#: protocol versions, and a call to the owner.
OUTPUT_VERSION_NON_DECISIONS = [
    'def show(p):\n    return read_raw_geo(p).value.get("version")\n',
    'def fetch(url, version="1.1.0"):\n    return url\n',
    'SUPPORTED = ("1.0.0", "1.1.0")\n',
    "v = resolve_output_geoparquet_version(req, input_file=p)\n",
    'def stac(payload):\n    return payload["version"]\n',
    "label = version or 'unknown'\n",
    "cfg = GEOPARQUET_VERSIONS[v]\n",
    'mv = cfg["metadata_version"]\n',
    'if version == "1.0.0":\n    pass\n',
    'output_version = "parquet-geo-only"\n',
    'native = wfs_version.startswith("2.")\n',
    'def show(payload):\n    return payload["version"]\n',
    'if ft == "geoparquet_v1":\n    label = "1.x"\n',
    'KINDS = {"geoparquet_v2": "native", "parquet_geo_only": "native"}\n',
    "write(table, out, geoparquet_version=requested)\n",
    'fetch(url, version="2.0.0")\n',
    'def block(v):\n    return {"version": v or "1.1.0", "primary_column": "geometry"}\n',
]


@pytest.mark.parametrize("source", OUTPUT_VERSION_COPIES)
def test_g12_the_output_version_arm_catches_every_copy(tmp_path, source):
    assert arm_hits(_plant(tmp_path, "output_version", source), "output_version"), source


@pytest.mark.parametrize("source", OUTPUT_VERSION_NON_DECISIONS)
def test_g12_the_output_version_arm_leaves_non_decisions_alone(tmp_path, source):
    assert arm_hits(_plant(tmp_path, "output_version", source), "output_version") == [], source


@pytest.mark.parametrize(
    "name", ["ensure_h3_column", "_ensure_h3_column", "ensure_index_column", "ensurecell_column"]
)
def test_g12_g2_index_column_ensure_has_one_owner(tmp_path, name):
    repo = _g2_tree(tmp_path, {"index_copy.py": f"def {name}(x):\n    return x\n"})
    result = _run_owner_defs("--repo", str(repo))
    assert result.returncode == 1 and name in result.stdout, result.stdout


#: Every shape of an ``edges`` decision the G1.edges arm must see.
EDGES_SHAPES = {
    "literal compare": 'def f(e):\n    return e == "spherical"\n',
    "literal tuple": 'ONE_X = ("planar", "spherical")\n',
    "algorithm set": 'ELL = frozenset({"vincenty", "thomas", "andoyer", "karney"})\n',
    "click choice": 'CHOICE = ["spherical", "planar"]\n',
    "key read": 'def f(col):\n    return col["edges"]\n',
    "key get": 'def f(col):\n    return col.get("edges", None)\n',
    "key write": 'def f(col, e):\n    col["edges"] = e\n',
    "key test": 'def f(col):\n    return "edges" in col\n',
    "key filter": 'def f(d):\n    return {k: v for k, v in d.items() if k != "edges"}\n',
    "f-string part": 'def f(n):\n    return f"{n}" + "karney"\n',
    # The first audit's shapes, each passing the arm at 0 before it.
    "dict literal key": 'def f(e):\n    return {"edges": e}\n',
    "dict update": 'def f(d, e):\n    d.update({"edges": e})\n',
    "dict call": "def f(e):\n    return dict(edges=e)\n",
    "geography f-string": 'def f(alg):\n    return f"GEOGRAPHY(OGC:CRS84, {alg})"\n',
    "algorithm f-string": 'def f(a):\n    return f"algorithm={a}"\n',
    "algorithm regex": 'import re\n\nPAT = re.compile(r"algorithm=(\\w+)")\n',
    "case fold compare": "def f(edges, x):\n    return edges.lower() == x\n",
    "uppercase literal": 'ALL = ("SPHERICAL",)\n',
    "edge type member": "import geoarrow.pyarrow as ga\n\nT = ga.EdgeType.SPHERICAL\n",
    "edge type subscript": "import geoarrow.pyarrow as ga\n\n\ndef f(e):\n    return ga.EdgeType[e.upper()]\n",
    "edge type read": "def f(t):\n    return t.edge_type.name.lower()\n",
    # The second audit's shapes.
    "or planar": "from geoparquet_io.core.edges import PLANAR\n\n\ndef f(e):\n    return e or PLANAR\n",
    "not planar": "from geoparquet_io.core.edges import PLANAR\n\n\ndef f(e):\n    return e != PLANAR\n",
    "is spherical": "from geoparquet_io.core import edges\n\n\ndef f(a):\n    return a == edges.SPHERICAL\n",
    "in constants": "from geoparquet_io.core.edges import PLANAR, SPHERICAL\n\n\ndef f(e):\n    return e in (PLANAR, SPHERICAL)\n",
    "aliased edge type": "from geoarrow.pyarrow import EdgeType as ET\n\nT = ET.SPHERICAL\n",
    "geography concat": 'def f(alg):\n    return "GEOGRAPHY(OGC:CRS84, " + alg + ")"\n',
    "casefold": "def f(edges):\n    e = edges.casefold()\n    return e\n",
}

#: A planar centroid cell key, planted where a cell key is built.
CELL_KEY_PLANTS = {
    "geoparquet_io/core/add/some_cells.py": 'def f(g):\n    return f"h3_latlng_to_cell(ST_Y(ST_Centroid({g})), 0, 9)"\n',
    "geoparquet_io/core/partition/some_cells.py": 'SQL = "SELECT ST_Centroid(geometry) FROM t"\n',
    "geoparquet_io/core/process/aggregate/some_cells.py": 'def f(g):\n    return f"ST_Centroid({g})"\n',
    # The IndexSpec driver, where every add/sort/partition cell key is built.
    "geoparquet_io/core/index_ops.py": 'def f(g):\n    return f"ST_Y(ST_Centroid({g}))"\n',
}


@pytest.mark.parametrize("where", sorted(CELL_KEY_PLANTS))
def test_g12_edges_arm_flags_a_planar_cell_key(where, tmp_path):
    target = tmp_path / where
    target.parent.mkdir(parents=True)
    target.write_text(CELL_KEY_PLANTS[where], encoding="utf-8")
    assert arm_hits(tmp_path, "edges"), where


def test_g12_edges_arm_leaves_a_centroid_elsewhere_alone(tmp_path):
    target = tmp_path / "geoparquet_io/core/pmtiles_chunks.py"
    target.parent.mkdir(parents=True)
    target.write_text('def f(g):\n    return f"ST_X(ST_Centroid({g}))"\n', encoding="utf-8")
    assert arm_hits(tmp_path, "edges") == []


@pytest.mark.parametrize("shape", sorted(EDGES_SHAPES))
def test_g12_edges_arm_flags_every_shape(shape, tmp_path):
    repo = _plant(tmp_path, "edges", EDGES_SHAPES[shape])
    assert arm_hits(repo, "edges"), shape


#: Every shape of a column-name fold or hand-written suffixer the G1.column_fold
#: arm must see (the base's eleven folding sites and three suffixers), and the
#: look-alikes it must not.
COLUMN_FOLD_SHAPES = {
    "lower of a column": "def f(col, x):\n    return col.lower() == x\n",
    "upper of a field": "def f(field_name):\n    return field_name.upper()\n",
    "casefold of a column": "def f(column):\n    return column.casefold()\n",
    "attribute": "def f(spec):\n    return spec.column.lower()\n",
    "subscript": "def f(columns):\n    return columns[0].lower()\n",
    "str() wrapped": "def f(col):\n    return str(col).lower()\n",
    "comprehension target": (
        "def f(schema_rows, want):\n    return [n for n, _ in schema_rows if n.lower() == want]\n"
    ),
    "for target": (
        "def f(columns, want):\n    for c in columns:\n        if c.lower() == want:\n"
        "            return c\n"
    ),
    "set of folded": "def f(cols):\n    return {c.lower() for c in cols}\n",
    "while suffixer": (
        "def f(base, taken):\n    name = base\n    n = 0\n    while name in taken:\n"
        '        n += 1\n        name = f"{base}_{n}"\n    return name\n'
    ),
    "while on a fold": (
        "def f(base, taken):\n    name = base\n    n = 0\n    while name.lower() in taken:\n"
        '        n += 1\n        name = f"{base}_{n}"\n    return name\n'
    ),
    "for suffixer": (
        "def f(base, taken):\n    for i in range(1, 100):\n"
        '        cand = f"{base}_{i}"\n        if cand not in taken:\n            return cand\n'
    ),
    # The first audit's blind spots (#1395): each passed the arm at 0.
    "loop over names": "def f(names, want):\n    return [n for n in names if n.lower() == want]\n",
    "for over names": (
        "def f(names, want):\n    for n in names:\n        if n.lower() == want:\n"
        "            return n\n"
    ),
    "lower map": "def f(names):\n    return {n.lower(): n for n in names}\n",
    "str.lower call": "def f(col):\n    return str.lower(col)\n",
    "map str.lower": "def f(columns):\n    return list(map(str.lower, columns))\n",
    "maketrans table": (
        "import string\n\nTABLE = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)\n"
    ),
    "sql lower": (
        'def f(path):\n    return f"SELECT name FROM parquet_schema({path}) WHERE lower(name) = 1"\n'
    ),
    "sql upper of a column": 'Q = "SELECT UPPER(column_name) FROM t"\n',
    "three-part f-string suffix": (
        "def f(base, taken):\n    n = 0\n    name = base\n    while name in taken:\n"
        '        n += 1\n        name = f"{base}_{n}_{n}"\n    return name\n'
    ),
    "joined f-string suffix": (
        "def f(base, taken):\n    n = 0\n    name = base\n    while name in taken:\n"
        '        n += 1\n        name = f"{base}{n}"\n    return name\n'
    ),
    "concatenated suffix": (
        "def f(base, taken):\n    n = 0\n    name = base\n    while name in taken:\n"
        '        n += 1\n        name = base + "_" + str(n)\n    return name\n'
    ),
    "underscore padding": (
        'def f(idx, geom):\n    while idx.lower() == geom.lower():\n        idx += "_"\n'
        "    return idx\n"
    ),
    "name receiver in a column function": (
        "def f(name, column_names):\n    return [c for c in column_names if c == name.lower()]\n"
    ),
    "requested receiver": ("def f(requested, columns):\n    return requested.lower() in columns\n"),
    "chained receiver": (
        "def f(field):\n    return field.strip().split(' ')[0].strip('\"').lower()\n"
    ),
    "str() chain": "def f(col):\n    return str(col).strip().lower()\n",
    "fold map": "from m import fold\n\ndef f(names):\n    return {fold(n): n for n in names}\n",
    "fold subscript map": (
        "from m import fold\n\ndef f(names):\n    out = {}\n    for n in names:\n"
        "        out[fold(n)] = n\n    return out\n"
    ),
    "fold setdefault map": (
        "from m import fold\n\ndef f(names):\n    out = {}\n    for n in names:\n"
        "        out.setdefault(fold(n), n)\n    return out\n"
    ),
    "exact schema membership": (
        "def f(schema, bbox_column_name):\n    return bbox_column_name in schema.names\n"
    ),
    "exact table membership": (
        "def f(table, column_name):\n    return column_name not in table.column_names\n"
    ),
}

COLUMN_FOLD_NEAR_MISSES = {
    "a type": "def f(col_type):\n    return col_type.upper()\n",
    "a path suffix": "def f(path):\n    return path.suffix.lower()\n",
    "a codec": "def f(codec):\n    return codec.lower()\n",
    "a name that says nothing": "def f(name):\n    return name.lower()\n",
    "a one-shot suffix": 'def f(prefix, v):\n    base = f"{prefix}_{v}"\n    return base in {"a"}\n',
    "a loop over plain names": "def f(items):\n    return [i.lower() for i in items]\n",
    "an exact membership on a literal": ("def f(schema):\n    return 'id' in schema.names\n"),
    "a table's own name": "def f(table):\n    return table.name.lower()\n",
    "sql lower of a path": 'Q = "SELECT lower(path) FROM t"\n',
    "a fold of a constant": "from m import fold\n\nKNOWN = {fold('A'): 1}\n",
    "the owner's lookups": (
        "from geoparquet_io.core.column_selection import matching_columns, unique_name\n\n"
        "def f(c, cols, taken):\n    return matching_columns(c, cols), unique_name(c, taken)\n"
    ),
}


@pytest.mark.parametrize("shape", sorted(COLUMN_FOLD_SHAPES))
def test_g12_column_fold_arm_flags_every_shape(shape, tmp_path):
    repo = _plant(tmp_path, "column_fold", COLUMN_FOLD_SHAPES[shape])
    assert arm_hits(repo, "column_fold"), shape


@pytest.mark.parametrize("shape", sorted(COLUMN_FOLD_NEAR_MISSES))
def test_g12_column_fold_arm_passes_its_look_alikes(shape, tmp_path):
    repo = _plant(tmp_path, "column_fold", COLUMN_FOLD_NEAR_MISSES[shape])
    assert arm_hits(repo, "column_fold") == [], shape
