"""The two ``geo`` block decoders (#1367 concept 3, approved change 6).

``core/geo_metadata.py`` owns the only two ways a ``geo`` value is decoded:

* :func:`read_raw_geo` -- the raw diagnostic decoder, for the readers that must
  report what a file literally holds (``check spec``, ``inspect meta``). It
  never sanitizes and never warns; an undecodable value comes back as a
  structured result that carries why.
* :func:`sanitized_carried_geo` -- the sanitized carried decoder, for every
  write and transform path. Bytes or str keys; bad UTF-8 or bad JSON warns once
  and reads as absent (``{}``); a non-object top level and non-object
  ``columns`` entries are sanitized to the documented shapes.

The table below pins both across key spelling x value encoding x payload, and
the guard at the bottom proves nothing outside the owner calls ``json.loads`` on
a ``geo`` value.
"""

from __future__ import annotations

import ast
import json
import logging
from pathlib import Path

import pytest

from geoparquet_io.core.geo_metadata import (
    RawGeo,
    read_raw_geo,
    reset_malformed_geo_warnings,
    sanitized_carried_geo,
)

GOOD = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
}
ENTRY_NOT_OBJECT = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {"geometry": {"encoding": "WKB", "geometry_types": []}, "other": "WKB"},
}
ENTRY_NOT_OBJECT_SANITIZED = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {"geometry": {"encoding": "WKB", "geometry_types": []}},
}

#: (case, raw bytes of the value, sanitized result, warns, raw value, raw error fragment)
PAYLOADS = [
    ("dict", json.dumps(GOOD).encode(), GOOD, False, GOOD, None),
    ("list", b'["not", "an", "object"]', {}, True, ["not", "an", "object"], None),
    ("string", b'"geometry"', {}, True, "geometry", None),
    ("entry-not-object", json.dumps(ENTRY_NOT_OBJECT).encode(), ENTRY_NOT_OBJECT_SANITIZED, True,
     ENTRY_NOT_OBJECT, None),
    ("invalid-json", b'{"version": "1.1.0", ', {}, True, None, "not valid JSON"),
    ("invalid-utf8", b'\xff\xfe{"version": "1.1.0"}', {}, True, None, "not valid UTF-8"),
]  # fmt: skip

KEYS = [pytest.param(b"geo", id="bytes-key"), pytest.param("geo", id="str-key")]
ENCODINGS = [pytest.param(bytes, id="bytes-value"), pytest.param(str, id="str-value")]


def _value(raw: bytes, encoding):
    if encoding is bytes:
        return raw
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        pytest.skip("a str value cannot hold invalid UTF-8")


def _malformed(records) -> list[str]:
    return [r.getMessage() for r in records if "malformed 'geo'" in r.getMessage()]


@pytest.fixture(autouse=True)
def _fresh_warnings():
    reset_malformed_geo_warnings()
    yield
    reset_malformed_geo_warnings()


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("encoding", ENCODINGS)
@pytest.mark.parametrize(
    ("case", "raw", "sanitized", "warns", "_raw_value", "_error"),
    PAYLOADS,
    ids=[p[0] for p in PAYLOADS],
)
def test_sanitized_carried_geo_table(
    caplog, key, encoding, case, raw, sanitized, warns, _raw_value, _error
):
    metadata = {key: _value(raw, encoding)}
    with caplog.at_level(logging.WARNING):
        result = sanitized_carried_geo(metadata)
    assert result == sanitized
    assert isinstance(result, dict)
    assert bool(_malformed(caplog.records)) is warns, caplog.text


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("encoding", ENCODINGS)
@pytest.mark.parametrize(
    ("case", "raw", "_sanitized", "_warns", "raw_value", "error"),
    PAYLOADS,
    ids=[p[0] for p in PAYLOADS],
)
def test_read_raw_geo_table(caplog, key, encoding, case, raw, _sanitized, _warns, raw_value, error):
    metadata = {key: _value(raw, encoding)}
    with caplog.at_level(logging.WARNING):
        result = read_raw_geo(metadata)
    assert isinstance(result, RawGeo)
    assert result.present
    assert result.value == raw_value
    if error is None:
        assert result.error is None
    else:
        assert error in result.error
    # Diagnostic: it reports, it does not warn or repair.
    assert _malformed(caplog.records) == []


@pytest.mark.parametrize("metadata", [None, {}, {b"other": b"x"}, {"ARROW:schema": "x"}])
def test_absent_key(metadata):
    assert sanitized_carried_geo(metadata) == {}
    raw = read_raw_geo(metadata)
    assert raw == RawGeo(present=False)
    assert raw.value is None and raw.error is None and raw.block is None


def test_raw_block_is_the_object_or_none():
    assert read_raw_geo({b"geo": json.dumps(GOOD).encode()}).block == GOOD
    assert read_raw_geo({b"geo": b"[1, 2]"}).block is None
    assert read_raw_geo({b"geo": b"{"}).block is None


def test_sanitized_warning_names_its_source(caplog):
    """#957: the path is in the message and in the warn-once key."""
    bad = {b"geo": json.dumps({**GOOD, "primary_column": 5}).encode()}
    with caplog.at_level(logging.WARNING):
        sanitized_carried_geo(bad, source="a.parquet")
        sanitized_carried_geo(bad, source="b.parquet")
    messages = _malformed(caplog.records)
    assert len(messages) == 2, messages
    assert "a.parquet" in messages[0]
    assert "b.parquet" in messages[1]


def test_undecodable_warning_names_its_source(caplog):
    with caplog.at_level(logging.WARNING):
        sanitized_carried_geo({b"geo": b"{"}, source="a.parquet")
        sanitized_carried_geo({b"geo": b"{"}, source="b.parquet")
    messages = _malformed(caplog.records)
    assert len(messages) == 2, messages
    assert all("not valid JSON" in m for m in messages)


# ---------------------------------------------------------------------------
# Guard: no raw `geo` decode outside the owner
# ---------------------------------------------------------------------------

PACKAGE = Path(__file__).resolve().parents[1] / "geoparquet_io"
OWNER = PACKAGE / "core" / "geo_metadata.py"


def _is_json_loads(node: ast.Call) -> bool:
    func = node.func
    if isinstance(func, ast.Attribute) and func.attr == "loads":
        return isinstance(func.value, ast.Name) and func.value.id == "json"
    return isinstance(func, ast.Name) and func.id == "loads"


def _mentions_geo(node: ast.AST) -> bool:
    for sub in ast.walk(node):
        if isinstance(sub, ast.Constant) and sub.value in ("geo", b"geo"):
            return True
        # The key named inside SQL: `... WHERE key = 'geo'`.
        if isinstance(sub, ast.Constant) and isinstance(sub.value, str) and "'geo'" in sub.value:
            return True
        if isinstance(sub, ast.Name) and "geo" in sub.id.lower().split("_"):
            return True
        if isinstance(sub, ast.Attribute) and "geo" in sub.attr.lower().split("_"):
            return True
    return False


def _calls_by_scope(tree: ast.Module):
    """Each call paired with its innermost enclosing function (or the module)."""
    scopes: dict[ast.AST, list[ast.Call]] = {}

    def visit(node: ast.AST, scope: ast.AST) -> None:
        for child in ast.iter_child_nodes(node):
            inner = child if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) else scope
            if isinstance(child, ast.Call):
                scopes.setdefault(scope, []).append(child)
            visit(child, inner)

    visit(tree, tree)
    return scopes.items()


def _bindings(func: ast.AST) -> dict[str, list[ast.AST]]:
    out: dict[str, list[ast.AST]] = {}
    for sub in ast.walk(func):
        if isinstance(sub, ast.Assign):
            for target in sub.targets:
                if isinstance(target, ast.Name):
                    out.setdefault(target.id, []).append(sub.value)
    return out


def _geo_bound(node: ast.AST, bindings: dict, depth: int = 2) -> bool:
    if _mentions_geo(node):
        return True
    if depth == 0:
        return False
    return any(
        isinstance(sub, ast.Name)
        and any(_geo_bound(v, bindings, depth - 1) for v in bindings.get(sub.id, []))
        for sub in ast.walk(node)
    )


def raw_geo_decodes(root: Path = PACKAGE) -> list[str]:
    """``json.loads`` whose argument mentions ``geo`` (by key, name, attribute or
    a variable assigned from one, up to two assignments back), outside the owner."""
    hits = []
    for path in sorted(root.rglob("*.py")):
        if path == OWNER:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for scope, calls in _calls_by_scope(tree):
            bindings = _bindings(scope)
            for node in calls:
                if _is_json_loads(node) and any(_geo_bound(a, bindings) for a in node.args):
                    hits.append(f"{path.relative_to(root.parent)}:{node.lineno}")
    return sorted(set(hits))


def test_no_raw_geo_decode_outside_the_owner():
    assert raw_geo_decodes() == []


def test_the_guard_sees_a_planted_raw_decode(tmp_path):
    """The guard is not vacuous: each spelling of the defect it pins is caught."""
    pkg = tmp_path / "geoparquet_io"
    pkg.mkdir()
    (pkg / "planted.py").write_text(
        "import json\n"
        "def a(md):\n    return json.loads(md[b'geo'])\n"
        "def b(md):\n    raw = md.get('geo')\n    text = raw.decode()\n    return json.loads(text)\n"
        "def c(geo_bytes):\n    return json.loads(geo_bytes)\n"
        "def d(md):\n    return json.loads(md[b'collection'])\n"
        "def e(con):\n    row = con.execute(\"SELECT value WHERE key = 'geo'\").fetchone()\n"
        "    text = row[0].decode()\n    return json.loads(text)\n"
    )
    assert set(raw_geo_decodes(pkg)) == {
        "geoparquet_io/planted.py:3",
        "geoparquet_io/planted.py:7",
        "geoparquet_io/planted.py:9",
        "geoparquet_io/planted.py:15",
    }
