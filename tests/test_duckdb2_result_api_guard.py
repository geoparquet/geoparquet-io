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
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PACKAGE = REPO / "geoparquet_io"
# The tests run on both DuckDB legs as well, so they are held to the same API.
SCANNED = [PACKAGE, REPO / "tests"]


def _sources() -> list[Path]:
    here = Path(__file__).resolve()
    return sorted(p for root in SCANNED for p in root.rglob("*.py") if p.resolve() != here)


# Methods an execute() result no longer has on 2.0.
REMOVED_ON_EXECUTE_RESULTS = {"arrow", "fetch_record_batch", "pl"}


def _is_execute_call(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "execute"
    )


def _execute_bound_names(func: ast.AST) -> set[str]:
    """Names assigned straight from an ``X.execute(...)`` call in *func*."""
    names: set[str] = set()
    for node in ast.walk(func):
        if isinstance(node, ast.Assign) and _is_execute_call(node.value):
            names.update(t.id for t in node.targets if isinstance(t, ast.Name))
    return names


def _violations(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: list[str] = []
    scopes = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    for scope in [tree, *scopes]:
        bound = _execute_bound_names(scope) if scope is not tree else set()
        for node in ast.walk(scope):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
                continue
            attr, receiver = node.func.attr, node.func.value
            if attr == "fetch_record_batch":
                found.append(f"{path.name}:{node.lineno} .fetch_record_batch()")
            elif attr in REMOVED_ON_EXECUTE_RESULTS and _is_execute_call(receiver):
                found.append(f"{path.name}:{node.lineno} .execute(...).{attr}()")
            elif attr == "fetchone" and _is_execute_call(receiver):
                # Not removed, but on 2.0 it leaves the result open; see fetch_one.
                found.append(f"{path.name}:{node.lineno} .execute(...).fetchone() (use fetch_one)")
            elif (
                attr in REMOVED_ON_EXECUTE_RESULTS
                and isinstance(receiver, ast.Name)
                and receiver.id in bound
            ):
                found.append(
                    f"{path.name}:{node.lineno} {receiver.id}.{attr}() on an execute result"
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


def _arrow_lambdas(path: Path) -> list[str]:
    found = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
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
    ],
)
def test_the_guard_catches_each_removed_form(tmp_path, snippet):
    probe = tmp_path / "probe.py"
    probe.write_text(snippet, encoding="utf-8")
    assert _violations(probe)


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
