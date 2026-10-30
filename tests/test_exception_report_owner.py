"""Caught-exception text reaches the user only through the sanitizing owners (#1367, #1034).

An ``except ... as e`` handler (or a function handed an exception) that formats
the exception straight into ``click.echo``, ``print``, a ``ClickException`` or
``could_not_run`` prints text nobody cleaned: httpx, OWSLib and DuckDB all embed
the request URL, query string and credentials included. The owners are
``core.exceptions.clean_error_text`` (inside ``GeoParquetError`` and
``logging_config.warn``/``error``) and ``cli.exception_handler.error_text`` /
``click_error_for``; ``sanitize_error_message`` is accepted too.

A site is flagged when it calls a reporter with an argument that mentions the
exception and is not wrapped in one of those. ``warn``/``error`` are not
reporters here: the logging layer cleans once. ``UNSANITIZED_REMAINDER`` lists
the justified sites that remain (a reason for each); the list only shrinks.
"""

from __future__ import annotations

import ast
import pathlib

PACKAGE = pathlib.Path(__file__).resolve().parent.parent / "geoparquet_io"

#: Where text reaches the user uncleaned. ``warn``/``error`` are not here: the
#: logging layer cleans once (``logging_config``), so a caught
#: exception can be interpolated into a log line without a call-site wrap.
REPORTERS = {"echo", "print", "ClickException", "BadParameter", "UsageError", "could_not_run"}
#: Parameter names / annotations that mean "this function was handed an exception".
EXCEPTION_PARAMS = {"e", "exc", "err", "error", "exception"}
EXCEPTION_ANNOTATIONS = {"Exception", "BaseException"}
OWNERS = {"core/exceptions.py", "cli/exception_handler.py"}
# `type(e)` names a class, not text the exception carries.
SANITIZERS = {
    "sanitize_error_message",
    "sanitize_url_for_logging",
    "clean_error_text",
    "error_text",
    "type",
}

#: ``relative path:function`` -> reason the exception text is safe to print.
UNSANITIZED_REMAINDER: dict[str, str] = {}


def _names(node: ast.AST) -> set[str]:
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}


def _call_name(call: ast.Call) -> str | None:
    func = call.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None


def _wrapped(arg: ast.AST) -> ast.AST:
    """The argument with every ``sanitize_*`` call subtree removed."""

    class _Strip(ast.NodeTransformer):
        def visit_Call(self, node):  # noqa: N802
            if _call_name(node) in SANITIZERS:
                return ast.Constant(value="")
            return self.generic_visit(node)

    import copy

    return _Strip().visit(copy.deepcopy(arg))


def _reports(node: ast.AST, names: set[str], scope: str, found: list[tuple[str, int]]) -> None:
    for call in (n for n in ast.walk(node) if isinstance(n, ast.Call)):
        if _call_name(call) not in REPORTERS:
            continue
        args = [*call.args, *(k.value for k in call.keywords)]
        if any(names & _names(_wrapped(a)) for a in args):
            found.append((scope, call.lineno))


def _exception_params(func: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    names = set()
    for arg in [*func.args.args, *func.args.kwonlyargs, *func.args.posonlyargs]:
        annotation = ast.unparse(arg.annotation) if arg.annotation else ""
        if arg.arg in EXCEPTION_PARAMS or any(t in annotation for t in EXCEPTION_ANNOTATIONS):
            names.add(arg.arg)
    return names


def unsanitized_reports(source: str) -> list[tuple[str, int]]:
    """``(enclosing function, line)`` for each raw-exception report in ``source``.

    Two shapes: an ``except ... as e`` body that reports ``e``, and a function
    handed an exception (by parameter name or annotation) that reports it
    outside any ``except`` body -- the helper that builds a ``ClickException``
    from ``exc.reason`` is invisible to the first shape.
    """
    tree = ast.parse(source)
    found: list[tuple[str, int]] = []

    def visit(node: ast.AST, scope: str) -> None:
        for child in ast.iter_child_nodes(node):
            inner = scope
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                inner = child.name
                names = _exception_params(child)
                if names:
                    _reports(child, names, inner, found)
            if isinstance(child, ast.ExceptHandler) and child.name:
                for stmt in child.body:
                    _reports(stmt, {child.name}, scope, found)
            visit(child, inner)

    visit(tree, "<module>")
    return sorted(set(found))


def _scan() -> dict[str, list[int]]:
    hits: dict[str, list[int]] = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        rel = path.relative_to(PACKAGE).as_posix()
        if rel in OWNERS:
            continue
        for scope, line in unsanitized_reports(path.read_text()):
            hits.setdefault(f"{rel}:{scope}", []).append(line)
    return hits


def test_no_handler_reports_raw_exception_text_outside_the_owners():
    hits = _scan()
    unexplained = {k: v for k, v in hits.items() if k not in UNSANITIZED_REMAINDER}
    assert not unexplained, (
        "except handlers that print/raise raw exception text; wrap it in "
        "sanitize_error_message or raise a GeoParquetError: " + repr(unexplained)
    )
    stale = [k for k in UNSANITIZED_REMAINDER if k not in hits]
    assert not stale, f"remove from UNSANITIZED_REMAINDER (now clean): {stale}"


def test_the_detector_flags_a_raw_report_and_passes_a_sanitized_one():
    raw = (
        "def f():\n    try:\n        g()\n    except OSError as e:\n        click.echo(f'x {e}')\n"
    )
    safe = (
        "def f():\n    try:\n        g()\n    except OSError as e:\n"
        "        click.echo(f'x {sanitize_error_message(str(e))}')\n"
    )
    assert unsanitized_reports(raw) == [("f", 5)]
    assert unsanitized_reports(safe) == []


def test_the_detector_flags_a_click_error_built_from_an_exception_argument():
    """No ``except`` body here: the helper is handed the exception."""
    by_name = "def to_click(exc):\n    return click.ClickException(exc.reason)\n"
    by_annotation = "def to_click(problem: Exception):\n    raise click.UsageError(str(problem))\n"
    clean = (
        "def to_click(exc):\n    return click.ClickException(sanitize_error_message(str(exc)))\n"
    )
    assert unsanitized_reports(by_name) == [("to_click", 2)]
    assert unsanitized_reports(by_annotation) == [("to_click", 2)]
    assert unsanitized_reports(clean) == []


def test_the_detector_flags_could_not_run_fed_a_raw_exception():
    raw = "def f():\n    try:\n        g()\n    except Exception as e:\n        return could_not_run('x', str(e))\n"
    assert unsanitized_reports(raw) == [("f", 5)]


def test_logging_redacts_credentials_once(caplog):
    """``warn``/``error`` are the single point for logged exception text."""
    import logging

    from geoparquet_io.core.logging_config import error, warn

    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        warn("could not read s3://k:secret@bucket/x.parquet (api_key=SECRETKEY)")
        error("failed with token: SECRETTOKEN on s3://bucket/y.parquet")
    assert "secret@" not in caplog.text and "SECRETKEY" not in caplog.text
    assert "SECRETTOKEN" not in caplog.text
    # URLs are otherwise intact: the line still names the file it is about.
    assert "s3://k:***@bucket/x.parquet" in caplog.text
    assert "s3://bucket/y.parquet" in caplog.text


def test_logging_applies_the_one_error_policy(caplog):
    """An Azure SAS ``sig=`` and an ANSI escape do not survive ``warn()``."""
    import logging

    from geoparquet_io.core.logging_config import warn

    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        warn(
            "could not read https://acct.blob.core.windows.net/c/x.parquet?sv=2020&sig=SASSECRET \x1b[2Kdone"
        )
    assert "SASSECRET" not in caplog.text
    assert "\x1b" not in caplog.text
    assert "https://acct.blob.core.windows.net/c/x.parquet" in caplog.text
