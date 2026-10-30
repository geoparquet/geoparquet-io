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

The table below pins both across key spelling x value encoding x payload. That
nothing outside the owner calls ``json.loads`` on a ``geo`` value is the
``G1.geo_parse`` owner guard's job (pinned at zero); the last test here checks
that arm sees each spelling the old sites used.
"""

from __future__ import annotations

import json
import logging

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
# Callers that read a carried block through the owner (T3a second audit)
# ---------------------------------------------------------------------------


def _parquet_with_geo(path, raw):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"geometry": pa.array([b"\x01"], pa.binary())})
    pq.write_table(table.replace_schema_metadata({b"geo": raw}), path)
    return str(path)


def test_geoparquet_crs_is_null_warns_with_the_path(tmp_path, caplog):
    """``crs_utils.geoparquet_crs_is_null`` names the file it could not decode (#957)."""
    from geoparquet_io.core.crs_utils import geoparquet_crs_is_null

    path = _parquet_with_geo(tmp_path / "trunc.parquet", b'{"version": ')
    with caplog.at_level(logging.WARNING):
        assert geoparquet_crs_is_null(path) is False
    messages = _malformed(caplog.records)
    assert len(messages) == 1, messages
    assert path in messages[0]


def test_rewrite_helpers_warn_on_an_undecodable_carried_block(caplog):
    """The prune runs on a carried input block, so bad JSON warns there and the
    value is left as found rather than rewritten."""
    from geoparquet_io.core.geo_metadata import prune_geo_metadata_to_columns

    metadata = {b"geo": b"{", b"other": b"x"}
    with caplog.at_level(logging.WARNING):
        assert prune_geo_metadata_to_columns(metadata, ["geometry"]) == metadata
    assert any("not valid JSON" in m for m in _malformed(caplog.records))


def test_read_geo_key_is_the_object_or_none(tmp_path):
    """``write_funnels._read_geo_key`` returns what its type says: a dict or None."""
    from geoparquet_io.core.write_funnels import _read_geo_key

    assert _read_geo_key(_parquet_with_geo(tmp_path / "list.parquet", b"[1]")) is None
    good = _parquet_with_geo(tmp_path / "good.parquet", json.dumps(GOOD).encode())
    assert _read_geo_key(good) == GOOD


@pytest.mark.parametrize(
    ("metadata", "expected"),
    [
        (None, False),
        ({b"other": b"x"}, False),
        ({b"geo": b"{"}, True),
        ({b"geo": json.dumps({**GOOD, "version": "2.0.0"}).encode()}, False),
        ({"geo": json.dumps({**GOOD, "version": "2.0.0"})}, False),
        ({b"geo": json.dumps(GOOD).encode()}, True),
        # The owner's precedence: the bytes key wins over the str one.
        (
            {b"geo": json.dumps(GOOD).encode(), "geo": json.dumps({**GOOD, "version": "2.0.0"})},
            True,
        ),
    ],
    ids=["none", "no-geo", "bad-json", "bytes-2.0", "str-2.0", "1.1", "bytes-wins"],
)
def test_needs_metadata_rewrite_for_2_0_reads_through_the_owner(metadata, expected):
    from geoparquet_io.core.write_strategies.base import needs_metadata_rewrite

    assert needs_metadata_rewrite("2.0", metadata) is expected


# ---------------------------------------------------------------------------
# The G1.geo_parse arm (tests/test_owner_guards.py) sees every spelling
# ---------------------------------------------------------------------------


def test_the_owner_guard_sees_each_spelling_of_a_raw_decode(tmp_path):
    """The tree-wide zero is pinned by ``G1.geo_parse`` (EXPECTED_COUNTS = 0); this
    plants the spellings the #1367 sites used and checks that arm catches each."""
    from tests.test_owner_guards import _plant, arm_hits

    source = (
        "import json\n"
        "def a(md):\n    return json.loads(md[b'geo'])\n"
        "def b(md):\n    raw = md.get('geo')\n    text = raw.decode()\n    return json.loads(text)\n"
        "def c(geo_bytes):\n    return json.loads(geo_bytes)\n"
        "def d(md):\n    return json.loads(md[b'collection'])\n"
        "def e(con):\n    row = con.execute(\"SELECT value WHERE key = 'geo'\").fetchone()\n"
        "    text = row[0].decode()\n    return json.loads(text)\n"
    )
    repo = _plant(tmp_path, "geo_parse", source)
    assert sorted(hit.name for hit in arm_hits(repo, "geo_parse")) == ["a", "b", "c", "e"]
