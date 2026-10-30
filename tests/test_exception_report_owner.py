"""Caught-exception text reaches the user only through the sanitizing owners (#1367, #1034).

An ``except ... as e`` handler that formats ``e`` straight into ``click.echo``,
``print``, a ``warn``/``error`` log call or a ``ClickException`` prints text
nobody sanitized: httpx, OWSLib and DuckDB all embed the request URL, query
string and credentials included. The owners are
``core.exceptions.sanitize_error_message`` / ``GeoParquetError`` (which
sanitizes in its constructor) and ``cli.exception_handler.cli_error_for``.

A handler is flagged when its body calls a reporting function with an argument
that mentions the bound exception and is not wrapped in
``sanitize_error_message``. ``UNSANITIZED_REMAINDER`` lists the justified
handlers that remain (a reason for each); the list only shrinks.
"""

from __future__ import annotations

import ast
import pathlib

PACKAGE = pathlib.Path(__file__).resolve().parent.parent / "geoparquet_io"

REPORTERS = {"echo", "print", "warn", "error", "ClickException", "BadParameter", "UsageError"}
OWNERS = {"core/exceptions.py", "cli/exception_handler.py"}
# `type(e)` names a class, not text the exception carries.
SANITIZERS = {"sanitize_error_message", "sanitize_url_for_logging", "type"}

#: ``relative path:function`` -> reason the exception text is safe to print.
_USER_TEXT = "echoes the user's own option value back, no network text"
_HANDOFF = (
    "hand-off to T-owner of the file: DuckDB/Arrow text on a local or "
    "gpio-minted path; wrap in sanitize_error_message when the file is next edited"
)
_HANDOFF_NET = "hand-off: can carry a remote URL; wrap in sanitize_error_message (not T4b's file)"
UNSANITIZED_REMAINDER: dict[str, str] = {
    "cli/commands/extract.py:extract_arcgis": _USER_TEXT,
    "cli/commands/extract.py:_check_bigquery_source": "exc.reason is gpio's own sentence",
    "cli/decorators.py:parse_row_group_options": _USER_TEXT,
    "cli/decorators.py:_validate_write_memory": _USER_TEXT,
    "cli/decorators.py:handle_directory_sub_partition": "StreamingError/ValueError sentences gpio wrote",
    "core/add/bbox_metadata.py:add_bbox_metadata": _HANDOFF,
    "core/add/country_codes.py:_print_results_summary": _HANDOFF,
    "core/admin_datasets.py:get_or_cache_dataset": _HANDOFF_NET,
    "core/check_fixes.py:fix_bboxless_covering": _HANDOFF,
    "core/check_spatial_order.py:check_spatial_order": _HANDOFF,
    "core/common.py:get_dataset_bounds": _HANDOFF,
    "core/crs_utils.py:detect_crs_from_spatial_file": _HANDOFF,
    "core/crs_utils.py:geoarrow_crs_to_projjson": "CRS decode text, no URL",
    "core/geometry_repair.py:repair_arrow_table_geometry": _HANDOFF,
    "core/hilbert_order.py:_cleanup_temp_file": "OSError on a gpio temp path",
    "core/overture.py:get_latest_overture_release": _HANDOFF_NET,
    "core/partition/auto_resolution.py:_probe_extent_resolution": _HANDOFF_NET,
    "core/partition/by_a5.py:_run_preview": _HANDOFF,
    "core/partition/by_h3.py:_run_preview": _HANDOFF,
    "core/partition/by_kdtree.py:_show_partition_preview": _HANDOFF,
    "core/partition/by_quadkey.py:_run_quadkey_preview": _HANDOFF,
    "core/partition/by_s2.py:_run_preview": _HANDOFF,
    "core/partition/by_string.py:partition_by_string": _HANDOFF,
    "core/remote.py:remote_write_context": _HANDOFF_NET,
    "core/sub_partition.py:sub_partition_directory": _HANDOFF,
    "core/write_funnels.py:collect_nonplanar_edges": _HANDOFF_NET,
    "core/write_funnels.py:apply_nonplanar_edges": _HANDOFF,
    "core/write_funnels.py:_geodesic_bounds": _HANDOFF,
    "core/write_funnels.py:_ensure_v2_geo_metadata": _HANDOFF,
    "core/write_funnels.py:read_preserved_kv_metadata": _HANDOFF_NET,
}


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


def unsanitized_reports(source: str) -> list[tuple[str, int]]:
    """``(enclosing function, line)`` for each raw-exception report in ``source``."""
    tree = ast.parse(source)
    found: list[tuple[str, int]] = []

    def visit(node: ast.AST, scope: str) -> None:
        for child in ast.iter_child_nodes(node):
            inner = (
                child.name if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) else scope
            )
            if isinstance(child, ast.ExceptHandler) and child.name:
                for stmt in child.body:
                    for call in (n for n in ast.walk(stmt) if isinstance(n, ast.Call)):
                        if _call_name(call) not in REPORTERS:
                            continue
                        args = [*call.args, *(k.value for k in call.keywords)]
                        if any(child.name in _names(_wrapped(a)) for a in args):
                            found.append((scope, call.lineno))
            visit(child, inner)

    visit(tree, "<module>")
    return found


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
    raw = "def f():\n    try:\n        g()\n    except OSError as e:\n        warn(f'x {e}')\n"
    safe = (
        "def f():\n    try:\n        g()\n    except OSError as e:\n"
        "        warn(f'x {sanitize_error_message(str(e))}')\n"
    )
    assert unsanitized_reports(raw) == [("f", 5)]
    assert unsanitized_reports(safe) == []
