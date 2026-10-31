"""Guard against the DuckDB result-object API that 2.0 removed.

DuckDB 2.0's Python client drops ``.arrow()``, ``.fetch_record_batch()`` and
``.pl()`` from what ``execute()`` returns (``AttributeError``), and drops
``.fetchone()`` / ``.fetchmany()`` / ``.execute()`` from relations. The form
that runs on 1.5 and 2.0 alike is the relation API: ``con.sql(q).arrow()``,
``con.sql(q).to_arrow_reader(batch_size=n)``.

Most of the suite runs on the locked 1.5.x, where the removed calls still
work, so a new ``con.execute(q).arrow()`` would pass every test there and
fail only on the 2.0 CI leg. This walks the source tree instead, so the
regression fails on every leg.

The walk is the shared scan's (``tests/_guard_scan.py``: each file parsed once
per process). A file whose text cannot hold a shape is not parsed at all: a
flagged call names its method after a ``.`` in the source, and a flagged lambda
is a literal whose text spells ``->`` and a ``list_``/``array_``/``map_`` call
(or is written with an escape). Not seen, so named: an arrow or a function name
split across two implicitly concatenated literals (``"p -" "> p.x"``).
"""

from __future__ import annotations

import ast
import re
import unicodedata
from pathlib import Path

import pytest

from tests._guard_scan import parse, python_files, walk

REPO = Path(__file__).resolve().parents[1]
PACKAGE = REPO / "geoparquet_io"
# The tests run on both DuckDB legs as well, so they are held to the same API.
SCANNED = [PACKAGE, REPO / "tests"]


def _sources() -> list[Path]:
    here = Path(__file__).resolve()
    return sorted(p for p in python_files(*SCANNED) if p.resolve() != here)


# Methods an execute() result no longer has on 2.0.
REMOVED_ON_EXECUTE_RESULTS = {"arrow", "fetch_record_batch", "pl"}


def _is_execute_call(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "execute"
    )


# Not removed on 2.0, but a partial read leaves the result open; see fetch_one.
LEAVES_RESULT_OPEN = {"fetchone", "fetchmany"}

#: A test that deliberately leaves an execute() result open (to pin the open-
#: result behavior itself) marks the call line with this and the guard skips it.
#: Only the open-result arm honors it: the removed API crashes on 2.0 regardless.
EXEMPT_MARK = "guard: open-result ok"


#: What a file must spell to call one of those methods: a ``.``, then (across
#: whitespace, a line continuation or a comment inside brackets) the name.
_RESULT_METHOD_CALL = re.compile(
    r"\.(?:[\s\\]|#[^\n]*)*(?:arrow|fetch_record_batch|pl|fetchone|fetchmany)\b"
)


def _identifier_text(path: Path) -> str:
    """The file's text, NFKC-normalized when it is not ASCII: Python reads an
    identifier that way, so ``ｆｅｔｃｈｏｎｅ`` is ``fetchone``."""
    text = path.read_text(encoding="utf-8")
    return text if text.isascii() else unicodedata.normalize("NFKC", text)


def _calls_by_outer_function(tree: ast.Module) -> tuple[list, dict[int, set[str]]]:
    """One walk of *tree*: every ``X.attr(...)`` call with the outermost
    function around it (``None`` at module level), and for each outermost
    function the names it (nested functions included) assigns straight from an
    ``X.execute(...)`` call."""
    functions = (ast.FunctionDef, ast.AsyncFunctionDef)
    calls: list[tuple[ast.Call, ast.AST | None]] = []
    bound: dict[int, set[str]] = {}
    stack: list[tuple[ast.AST, ast.AST | None]] = [(tree, None)]
    while stack:
        node, outer = stack.pop()
        if outer is None and isinstance(node, functions):
            outer = node
            bound[id(node)] = set()
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            calls.append((node, outer))
        elif outer is not None and isinstance(node, ast.Assign) and _is_execute_call(node.value):
            bound[id(outer)].update(t.id for t in node.targets if isinstance(t, ast.Name))
        stack.extend((child, outer) for child in ast.iter_child_nodes(node))
    return calls, bound


def _violations(path: Path) -> list[str]:
    """A call is checked against the names its outermost enclosing function
    binds from ``execute()``: that set holds every inner function's too, so it
    answers for each scope around the call at once."""
    if not _RESULT_METHOD_CALL.search(_identifier_text(path)):
        return []
    parsed = parse(path)
    lines = parsed.lines
    calls, bound_by = _calls_by_outer_function(parsed.tree)
    found: list[str] = []
    for node, outer in calls:
        bound = bound_by[id(outer)] if outer is not None else set()
        attr, receiver = node.func.attr, node.func.value
        exempt = attr in LEAVES_RESULT_OPEN and EXEMPT_MARK in lines[node.lineno - 1]
        on_bound_name = isinstance(receiver, ast.Name) and receiver.id in bound
        if attr == "fetch_record_batch":
            found.append(f"{path.name}:{node.lineno} .fetch_record_batch()")
        elif attr in REMOVED_ON_EXECUTE_RESULTS and _is_execute_call(receiver):
            found.append(f"{path.name}:{node.lineno} .execute(...).{attr}()")
        elif attr in LEAVES_RESULT_OPEN and _is_execute_call(receiver) and not exempt:
            found.append(f"{path.name}:{node.lineno} .execute(...).{attr}() (use fetch_one)")
        elif attr in REMOVED_ON_EXECUTE_RESULTS and on_bound_name:
            found.append(f"{path.name}:{node.lineno} {receiver.id}.{attr}() on an execute result")
        elif attr in LEAVES_RESULT_OPEN and on_bound_name and not exempt:
            found.append(
                f"{path.name}:{node.lineno} {receiver.id}.{attr}() on an execute "
                "result (use fetch_one)"
            )
    return sorted(set(found))


def test_no_source_calls_the_execute_result_arrow_api():
    violations = [v for p in _sources() for v in _violations(p)]
    assert violations == [], (
        "These call result-object methods DuckDB 2.0 removed, or leave an execute() "
        "result open. Use con.sql(q).arrow(), con.sql(q).to_arrow_reader(batch_size=n), "
        "or fetch_one(con.execute(q)):\n" + "\n".join(violations)
    )


# A SQL lambda in the arrow form DuckDB 2.0's binder rejects: `p -> ...` or
# `(p, i) -> ...` as an argument (after `(` or `,`). The `lambda p, i: ...`
# form binds on 1.5 and 2.0 alike.
# Literals are checked joined (implicit concatenation and f-string parts), so a
# lambda split across lines is seen whole; only text that calls a list, array
# or map function is SQL enough to check, which keeps prose arrows out.
_ARROW_LAMBDA = re.compile(r"[(,]\s*(?:\w+|\(\s*\w+(?:\s*,\s*\w+)*\s*\))\s*->")
_LAMBDA_TAKING_CALL = re.compile(r"\b(?:list|array|map)_\w+\s*\(")


def _literal_text(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value if isinstance(v, ast.Constant) else "{}" for v in node.values)
    return None


_LAMBDA_FUNCTION_NAME = re.compile(r"(?:list|array|map)_")
#: An escape that can spell a character of either (``\x2d``, ``\u003e``,
#: ``\N{...}``, octal), or a backslash-newline that joins a literal's lines.
_SPELLING_ESCAPE = re.compile(r"\\(?:[xuU0-7]|N\{|\r?\n)")


def _may_hold_an_arrow_lambda(text: str) -> bool:
    """Whether a literal in *text* can join to an arrow lambda: it spells ``->``
    and a lambda-taking function's name, or an escape could spell either."""
    escaped = bool(_SPELLING_ESCAPE.search(text))
    return ("->" in text or escaped) and (escaped or bool(_LAMBDA_FUNCTION_NAME.search(text)))


def _arrow_lambdas(path: Path) -> list[str]:
    found = []
    if not _may_hold_an_arrow_lambda(path.read_text(encoding="utf-8")):
        return found
    for node in walk(parse(path).tree):
        text = _literal_text(node)
        if text and _LAMBDA_TAKING_CALL.search(text) and _ARROW_LAMBDA.search(text):
            found.append(f"{path.name}:{node.lineno} {text.strip()[:70]}")
    return found


def test_no_sql_lambda_uses_the_arrow_syntax():
    found = [v for p in _sources() for v in _arrow_lambdas(p)]
    assert found == [], (
        "DuckDB 2.0 rejects `x -> ...` lambdas (BinderException); write "
        "`lambda x: ...`, which 1.5 accepts too:\n" + "\n".join(found)
    )


@pytest.mark.parametrize(
    ("text", "flagged"),
    [
        ('"list_transform(l, p -> p.x)"', True),
        ('"list_transform({r}, (p, i) -> p * i)"', True),
        ('"list_transform(l, lambda p: p.x)"', False),
        ('"""Map column -> edges."""', False),
        ('"CRS transformation (4326->3857)"', False),
        ('("list_transform(l, " "p -> p.x)")', True),
    ],
)
def test_the_lambda_guard_tells_sql_from_prose(tmp_path, text, flagged):
    probe = tmp_path / "probe.py"
    probe.write_text(f"X = {text}\n", encoding="utf-8")
    assert bool(_arrow_lambdas(probe)) is flagged


@pytest.mark.parametrize(
    "snippet",
    [
        "def f(con):\n    return con.execute('q').arrow().read_all()\n",
        "def f(con):\n    r = con.execute('q')\n    return r.arrow()\n",
        "def f(con):\n    return con.execute('q').fetch_record_batch(10)\n",
        "def f(cur):\n    return cur.execute('q', [1]).pl()\n",
        "def f(con):\n    return con.execute('q').fetchone()[0]\n",
        "def f(con):\n    return con.execute('q').fetchmany(5)\n",
        "def f(con):\n    r = con.execute('q')\n    return r.fetchone()[0]\n",
        "def f(con):\n    r = con.execute('q')\n    return r.fetchmany(5)\n",
    ],
)
def test_the_guard_catches_each_removed_form(tmp_path, snippet):
    probe = tmp_path / "probe.py"
    probe.write_text(snippet, encoding="utf-8")
    assert _violations(probe)


@pytest.mark.parametrize(
    "snippet",
    [
        "def f(con):\n    return (con.execute('q')\n        .  # the row\n        fetchone())\n",
        "def f(con):\n    return con.execute('q').\\\n        fetchmany(5)\n",
        "def f(con):\n    return con.execute('q').\uff46\uff45\uff54\uff43\uff48\uff4f\uff4e\uff45()\n",
    ],
)
def test_the_text_prefilter_keeps_a_call_spelled_across_lines(tmp_path, snippet):
    """A file is skipped only when its text cannot spell a flagged call."""
    probe = tmp_path / "probe.py"
    probe.write_text(snippet, encoding="utf-8")
    assert _violations(probe)


@pytest.mark.parametrize(
    "literal",
    ['"list_transform(l, p -\\x3e p.x)"', '"list_transform(l, p -\\\n> p.x)"'],
)
def test_the_text_prefilter_keeps_an_escaped_arrow_lambda(tmp_path, literal):
    """An arrow spelled by an escape, or joined across a backslash-newline."""
    probe = tmp_path / "probe.py"
    probe.write_text(f"X = {literal}\n", encoding="utf-8")
    assert _arrow_lambdas(probe)


def test_the_marked_line_is_exempt_from_the_open_result_arm_only(tmp_path):
    probe = tmp_path / "probe.py"
    probe.write_text(
        "def f(con):\n"
        "    r = con.execute('q')\n"
        f"    a = r.fetchone()  # {EXEMPT_MARK}\n"
        f"    b = con.execute('q').fetchone()  # {EXEMPT_MARK}\n"
        f"    return a, b, r.arrow()  # {EXEMPT_MARK} (must not exempt removed API)\n",
        encoding="utf-8",
    )
    assert _violations(probe) == ["probe.py:5 r.arrow() on an execute result"]


def test_the_guard_passes_the_relation_forms(tmp_path):
    probe = tmp_path / "probe.py"
    probe.write_text(
        "def f(con):\n"
        "    a = con.sql('q').arrow().read_all()\n"
        "    b = con.sql('q', params=[1]).to_arrow_reader(batch_size=10)\n"
        "    rel = con.sql('q')\n"
        "    return rel.arrow(), a, b\n",
        encoding="utf-8",
    )
    assert _violations(probe) == []
