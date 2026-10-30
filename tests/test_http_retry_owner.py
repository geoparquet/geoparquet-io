"""Guard: the HTTP retry policy has one owner, ``core/http_retry.py`` (#1367, concept 14).

Outside the owner nothing may:

* loop over an ``httpx``/``requests`` call (or a DuckDB remote read) with a sleep;
* define ``DEFAULT_MAX_RETRIES`` / ``DEFAULT_RETRY_DELAY`` / ``DEFAULT_TIMEOUT``;
* give ``max_retries`` / ``retry_delay`` a numeric default in a signature, or pass
  a numeric literal for either as a call keyword (a caller that needs another
  value passes a named constant from ``http_retry`` or its own module, with a
  comment saying why);
* write a status-code set containing 429 or a 5xx;
* parse ``Retry-After``;
* wrap the owner in a function that just passes its arguments through.

Callers import the owner's constants or pass an explicit call argument.
"""

from __future__ import annotations

import ast
from pathlib import Path

PACKAGE = Path(__file__).resolve().parent.parent / "geoparquet_io"
OWNER = PACKAGE / "core" / "http_retry.py"
_NETWORK_CALLS = {"get", "post", "stream", "request", "send"}
_NETWORK_ROOTS = {"httpx", "requests", "client", "conn", "con"}
_POLICY_NAMES = {"DEFAULT_MAX_RETRIES", "DEFAULT_RETRY_DELAY", "DEFAULT_TIMEOUT"}
_POLICY_ARGS = {"max_retries", "retry_delay"}
_OWNER_CALLS = {"make_request_with_retry", "run_with_retry", "get_shared_http_client"}


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
            "get_shared_http_client",
            "get_duckdb_connection",
        }
    if isinstance(func, ast.Attribute) and func.attr in _NETWORK_CALLS:
        root = func.value
        while isinstance(root, ast.Attribute):
            root = root.value
        return isinstance(root, ast.Name) and root.id in _NETWORK_ROOTS
    return False


def _loops_in(source: str, label: str) -> list[str]:
    found = []
    for loop in ast.walk(ast.parse(source)):
        if not isinstance(loop, (ast.For, ast.While)):
            continue
        nodes = list(ast.walk(loop))
        if any(_is_sleep(n) for n in nodes) and any(_is_network_call(n) for n in nodes):
            found.append(f"{label}:{loop.lineno} retry loop")
    return found


def _is_retry_status(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and (value in (408, 429) or 501 <= value < 600)
    )


def _policy_in(source: str, label: str) -> list[str]:
    """Private policy constants, signature defaults, status sets, Retry-After parsing."""
    found = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for t in targets:
                if isinstance(t, ast.Name) and t.id in _POLICY_NAMES:
                    found.append(f"{label}:{node.lineno} defines {t.id}")
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = node.args
            pos = args.posonlyargs + args.args
            pairs = list(zip(pos[len(pos) - len(args.defaults) :], args.defaults, strict=True))
            pairs += [
                (a, d)
                for a, d in zip(args.kwonlyargs, args.kw_defaults, strict=True)
                if d is not None
            ]
            for arg, default in pairs:
                if (
                    arg.arg in _POLICY_ARGS
                    and isinstance(default, ast.Constant)
                    and isinstance(default.value, (int, float))
                ):
                    found.append(f"{label}:{node.lineno} {arg.arg} defaults to a literal")
        if isinstance(node, ast.Set):
            elts = node.elts
            if len(elts) >= 2 and any(
                isinstance(e, ast.Constant) and _is_retry_status(e.value) for e in elts
            ):
                found.append(f"{label}:{node.lineno} status-code set with 429/5xx")
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if (
                    kw.arg in _POLICY_ARGS
                    and isinstance(kw.value, ast.Constant)
                    and isinstance(kw.value.value, (int, float))
                    and not isinstance(kw.value.value, bool)
                ):
                    found.append(f"{label}:{node.lineno} passes a literal {kw.arg}")
        if isinstance(node, ast.Constant) and node.value == "Retry-After":
            found.append(f"{label}:{node.lineno} parses Retry-After")
    return found


def _wrapper_calls(call: ast.Call) -> bool:
    func = call.func
    name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
    return name in _OWNER_CALLS


def _wrappers_in(source: str, label: str) -> list[str]:
    """Functions whose whole body is one call to the owner, arguments passed through."""
    found = []
    for fn in ast.walk(ast.parse(source)):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        body = [
            s
            for s in fn.body
            if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant))
        ]
        if len(body) != 1 or not isinstance(body[0], (ast.Return, ast.Expr)):
            continue
        call = body[0].value
        if not isinstance(call, ast.Call) or not _wrapper_calls(call):
            continue
        params = {a.arg for a in fn.args.posonlyargs + fn.args.args + fn.args.kwonlyargs}
        passed = {
            n.id
            for arg in [*call.args, *(k.value for k in call.keywords)]
            for n in ast.walk(arg)
            if isinstance(n, ast.Name)
        }
        if params & passed:
            found.append(f"{label}:{fn.lineno} {fn.name} wraps the owner")
    return found


def _scan(check) -> list[str]:
    offenders = []
    for path in sorted(PACKAGE.rglob("*.py")):
        if path != OWNER:
            label = str(path.relative_to(PACKAGE.parent))
            offenders.extend(check(path.read_text(encoding="utf-8"), label))
    return offenders


def test_no_retry_loop_outside_the_owner():
    assert _scan(_loops_in) == []


def test_no_private_retry_policy_outside_the_owner():
    assert _scan(_policy_in) == []


def test_no_wrapper_around_the_owner_outside_it():
    assert _scan(_wrappers_in) == []


def test_the_guard_flags_an_injected_retry_loop():
    fault = (
        "import time\n"
        "def f(client):\n"
        "    for attempt in range(3):\n"
        "        r = client.get('u')\n"
        "        time.sleep(1)\n"
    )
    assert _loops_in(fault, "fault.py") == ["fault.py:3 retry loop"]


def test_the_guard_flags_injected_private_policy():
    fault = (
        "DEFAULT_RETRY_DELAY = 2.0\n"
        "RETRY = {408, 429, 503}\n"
        "def f(max_retries: int = 3, retry_delay=1.0): ...\n"
        "def g(r):\n"
        "    return r.headers.get('Retry-After')\n"
        "h(max_retries=5)\n"
    )
    found = _policy_in(fault, "fault.py")
    assert len(found) == 6, found
    assert any("passes a literal max_retries" in f for f in found)


def test_the_guard_flags_an_injected_client_wrapper():
    fault = "def _client(timeout):\n    return get_shared_http_client(timeout=timeout)\n"
    assert _wrappers_in(fault, "fault.py") == ["fault.py:1 _client wraps the owner"]


def test_the_guard_flags_an_injected_wrapper():
    fault = (
        "def _make_request(method, url, timeout=60):\n"
        '    """doc"""\n'
        "    return make_request_with_retry(method=method, url=url, timeout=timeout)\n"
        "def fine(url):\n"
        "    data = make_request_with_retry('GET', url)\n"
        "    return data['x']\n"
    )
    assert _wrappers_in(fault, "fault.py") == ["fault.py:1 _make_request wraps the owner"]
