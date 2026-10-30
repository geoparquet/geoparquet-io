"""Guard: the HTTP retry policy has one owner, ``core/http_retry.py`` (#1367, concept 14).

A ``for``/``while`` loop that makes an ``httpx``/``requests`` call (or a DuckDB
remote read) and sleeps between attempts is a private retry policy. Those belong
in ``http_retry.run_with_retry``; callers pass what differs as arguments.
"""

from __future__ import annotations

import ast
from pathlib import Path

PACKAGE = Path(__file__).resolve().parent.parent / "geoparquet_io"
OWNER = PACKAGE / "core" / "http_retry.py"
# Attribute names that mean "this statement talks to a server".
_NETWORK_CALLS = {"get", "post", "stream", "request", "send", "_get_shared_http_client"}
_NETWORK_ROOTS = {"httpx", "requests", "client", "conn", "con"}


def _is_sleep(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "sleep"
    )


def _is_network_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id in {
            "_get_shared_http_client",
            "get_shared_http_client",
            "get_duckdb_connection",
        }
    if isinstance(func, ast.Attribute) and func.attr in _NETWORK_CALLS:
        root = func.value
        while isinstance(root, ast.Attribute):
            root = root.value
        return isinstance(root, ast.Name) and root.id in _NETWORK_ROOTS
    return False


def find_retry_loops(path: Path) -> list[str]:
    return _loops_in(path.read_text(encoding="utf-8"), str(path.relative_to(PACKAGE.parent)))


def _loops_in(source: str, label: str) -> list[str]:
    tree = ast.parse(source)
    found = []
    for loop in ast.walk(tree):
        if not isinstance(loop, (ast.For, ast.While)):
            continue
        nodes = list(ast.walk(loop))
        if any(_is_sleep(n) for n in nodes) and any(_is_network_call(n) for n in nodes):
            found.append(f"{label}:{loop.lineno}")
    return found


def test_no_retry_loop_outside_the_owner():
    offenders = []
    for path in sorted(PACKAGE.rglob("*.py")):
        if path != OWNER:
            offenders.extend(find_retry_loops(path))
    assert offenders == [], f"private retry loops (use http_retry.run_with_retry): {offenders}"


def test_the_guard_flags_an_injected_retry_loop():
    """Injected fault: a hand-rolled httpx retry loop is recognised."""
    fault = (
        "import time\n"
        "def f(client):\n"
        "    for attempt in range(3):\n"
        "        r = client.get('u')\n"
        "        time.sleep(1)\n"
    )
    assert _loops_in(fault, "fault.py") == ["fault.py:3"]
