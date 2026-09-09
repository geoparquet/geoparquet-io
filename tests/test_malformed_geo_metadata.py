"""A malformed carried ``geo`` block must not crash the write (#771).

The ``geo`` key on an input file is arbitrary JSON written by somebody else's
tool. gpio's writers read it and index into it -- ``geo["columns"][col]`` --
without checking that ``columns`` is the mapping-of-mappings the spec requires,
so a block whose ``columns`` is null, a list, a string, or a mapping to
non-objects aborted the write with a bare ``TypeError`` from three frames deep.

The decision recorded in #771: a malformed block is a property of the *input*,
not a caller error, so it is treated the way an absent block is -- the malformed
parts are dropped, fresh metadata is built from the table, and one warning names
what was wrong. Nothing raises.
"""

from __future__ import annotations

import json
import logging
import struct

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import _apply_geoparquet_metadata
from geoparquet_io.core.geo_metadata import (
    reset_malformed_geo_warnings,
    sanitize_geo_metadata,
)

# One WKB point (1.0, 2.0), little-endian.
POINT_WKB = struct.pack("<BI2d", 1, 1, 1.0, 2.0)

WRITE_STRATEGIES = ["in-memory", "streaming", "disk-rewrite", "duckdb-kv"]

#: gpio's own detection list holds five standard names; ``geom`` stands for
#: every one of them that is not ``geometry``. A recovery that falls back to
#: the literal ``"geometry"`` looks correct on the first and silently reports
#: no CRS on the second, so every reader test is run against both (#887 review).
GEOMETRY_COLUMN_NAMES = ["geometry", "geom"]

# (id, the value carried as geo["columns"], the substring the warning must name)
MALFORMED_COLUMNS = [
    ("null", None, "'columns'"),
    ("list", ["geometry"], "'columns'"),
    ("string", "geometry", "'columns'"),
    ("entry_not_an_object", {"geometry": "WKB"}, "'columns'"),
]


def _table_with_geo(geo_block, col: str = "geometry") -> pa.Table:
    table = pa.table({"id": [1], col: pa.array([POINT_WKB], type=pa.binary())})
    return table.replace_schema_metadata({b"geo": json.dumps(geo_block).encode("utf-8")})


def _malformed_table(columns) -> pa.Table:
    return _table_with_geo({"version": "1.1.0", "primary_column": "geometry", "columns": columns})


def _geo_of(table: pa.Table) -> dict:
    return json.loads(table.schema.metadata[b"geo"].decode("utf-8"))


def _malformed_warnings(records) -> list[str]:
    return [r.getMessage() for r in records if "malformed" in r.getMessage().lower()]


def _assert_fresh_and_valid(geo: dict, col: str = "geometry") -> None:
    """The rebuilt block must be the one gpio would have written with no input block."""
    assert geo["primary_column"] == col
    assert isinstance(geo["columns"], dict)
    assert isinstance(geo["columns"][col], dict)
    assert geo["columns"][col]["encoding"]


# =============================================================================
# The sanitizer itself
# =============================================================================


class TestSanitizeGeoMetadata:
    @pytest.mark.parametrize(("case", "columns", "_hint"), MALFORMED_COLUMNS)
    def test_drops_malformed_columns(self, case, columns, _hint):
        reset_malformed_geo_warnings()
        cleaned = sanitize_geo_metadata(
            {"version": "1.1.0", "primary_column": "geometry", "columns": columns}
        )
        assert cleaned is not None
        assert cleaned.get("columns", {}) == {}

    def test_keeps_the_good_entries_beside_a_bad_one(self):
        reset_malformed_geo_warnings()
        cleaned = sanitize_geo_metadata(
            {"columns": {"geometry": {"encoding": "WKB"}, "other": "WKB"}}
        )
        assert cleaned["columns"] == {"geometry": {"encoding": "WKB"}}

    def test_drops_a_non_string_primary_column(self):
        reset_malformed_geo_warnings()
        cleaned = sanitize_geo_metadata({"primary_column": 123, "columns": {}})
        assert "primary_column" not in cleaned

    @pytest.mark.parametrize("col", GEOMETRY_COLUMN_NAMES)
    def test_repairs_a_dropped_primary_column_from_the_only_column(self, col):
        """One column is an unambiguous primary: name it rather than leave a hole.

        Dropping ``primary_column`` and stopping there is what turned #887's
        loud crash into silently wrong coordinates: the readers then fell back
        to the literal ``"geometry"``, missed a ``geom`` column, and reported
        the file's CRS as absent (#887 review).
        """
        reset_malformed_geo_warnings()
        cleaned = sanitize_geo_metadata(
            {"version": "1.1.0", "primary_column": 123, "columns": {col: {"encoding": "WKB"}}}
        )
        assert cleaned["primary_column"] == col

    def test_does_not_invent_a_primary_column_when_two_could_be_meant(self):
        """Two columns and no declared primary is a guess; the callers ask the schema."""
        reset_malformed_geo_warnings()
        cleaned = sanitize_geo_metadata(
            {
                "primary_column": 123,
                "columns": {"geom": {"encoding": "WKB"}, "other": {"encoding": "WKB"}},
            }
        )
        assert "primary_column" not in cleaned

    def test_repairs_from_the_kept_columns_not_the_dropped_ones(self):
        """A column entry that is itself malformed is not a candidate primary."""
        reset_malformed_geo_warnings()
        cleaned = sanitize_geo_metadata(
            {"primary_column": 123, "columns": {"geom": {"encoding": "WKB"}, "bogus": "WKB"}}
        )
        assert cleaned["primary_column"] == "geom"

    def test_a_block_that_is_not_an_object_is_dropped_entirely(self):
        reset_malformed_geo_warnings()
        assert sanitize_geo_metadata(["geometry"]) is None
        reset_malformed_geo_warnings()
        assert sanitize_geo_metadata("geometry") is None

    def test_none_passes_through(self):
        assert sanitize_geo_metadata(None) is None

    def test_a_well_formed_block_is_returned_unchanged(self):
        block = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "bbox": [0, 0, 1, 1]}},
        }
        assert sanitize_geo_metadata(block) is block

    def test_does_not_mutate_the_caller_s_dict(self):
        reset_malformed_geo_warnings()
        block = {"primary_column": "geometry", "columns": None}
        sanitize_geo_metadata(block)
        assert block["columns"] is None

    def test_names_the_offending_key_and_its_json_type(self, caplog):
        reset_malformed_geo_warnings()
        with caplog.at_level(logging.WARNING):
            sanitize_geo_metadata({"columns": ["geometry"]})
        messages = _malformed_warnings(caplog.records)
        assert len(messages) == 1
        assert "'geo'" in messages[0]
        assert "'columns'" in messages[0]
        assert "array" in messages[0]

    def test_warns_once_per_distinct_problem(self, caplog):
        reset_malformed_geo_warnings()
        with caplog.at_level(logging.WARNING):
            sanitize_geo_metadata({"columns": None})
            sanitize_geo_metadata({"columns": None})
        assert len(_malformed_warnings(caplog.records)) == 1


# =============================================================================
# The helper the issue reproduces on
# =============================================================================


@pytest.mark.parametrize(("case", "columns", "hint"), MALFORMED_COLUMNS)
@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_apply_metadata_survives_a_malformed_columns_block(case, columns, hint, version, caplog):
    reset_malformed_geo_warnings()
    table = _malformed_table(columns)

    with caplog.at_level(logging.WARNING):
        result = _apply_geoparquet_metadata(
            table, "geometry", version, original_metadata=table.schema.metadata
        )

    _assert_fresh_and_valid(_geo_of(result))

    messages = _malformed_warnings(caplog.records)
    assert len(messages) == 1, messages
    assert "'geo'" in messages[0]
    assert hint in messages[0]


@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_apply_metadata_rebuilds_a_non_string_primary_column(version):
    reset_malformed_geo_warnings()
    table = _table_with_geo(
        {"version": "1.1.0", "primary_column": 123, "columns": {"geometry": {"encoding": "WKB"}}}
    )
    result = _apply_geoparquet_metadata(
        table, "geometry", version, original_metadata=table.schema.metadata
    )
    _assert_fresh_and_valid(_geo_of(result))


@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_apply_metadata_survives_a_geo_block_that_is_not_an_object(version):
    reset_malformed_geo_warnings()
    table = _table_with_geo(["geometry"])
    result = _apply_geoparquet_metadata(
        table, "geometry", version, original_metadata=table.schema.metadata
    )
    _assert_fresh_and_valid(_geo_of(result))


def test_a_well_formed_block_still_carries_through(caplog):
    """Regression: the shape check must not disturb a valid input block."""
    reset_malformed_geo_warnings()
    table = _table_with_geo(
        {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "orientation": "counterclockwise"}},
        }
    )
    with caplog.at_level(logging.WARNING):
        result = _apply_geoparquet_metadata(
            table, "geometry", "1.1", original_metadata=table.schema.metadata
        )

    geo = _geo_of(result)
    _assert_fresh_and_valid(geo)
    assert geo["columns"]["geometry"]["orientation"] == "counterclockwise"
    assert _malformed_warnings(caplog.records) == []


def test_an_absent_geo_block_still_works(caplog):
    reset_malformed_geo_warnings()
    table = pa.table({"id": [1], "geometry": pa.array([POINT_WKB], type=pa.binary())})
    with caplog.at_level(logging.WARNING):
        result = _apply_geoparquet_metadata(table, "geometry", "1.1", original_metadata=None)
    _assert_fresh_and_valid(_geo_of(result))
    assert _malformed_warnings(caplog.records) == []


# =============================================================================
# Every write-path reader of the raw block goes through the one shape check
# =============================================================================


@pytest.mark.parametrize(("case", "columns", "_hint"), MALFORMED_COLUMNS)
def test_strategy_base_reader_sanitizes(case, columns, _hint):
    """``write_strategies.base`` builds metadata through its own reader (#771)."""
    from geoparquet_io.core.write_strategies.base import build_geo_metadata

    reset_malformed_geo_warnings()
    raw = json.dumps({"primary_column": "geometry", "columns": columns}).encode("utf-8")
    geo = build_geo_metadata("geometry", "1.1", original_metadata={b"geo": raw})
    _assert_fresh_and_valid(geo)


@pytest.mark.parametrize(
    "metadata",
    [
        {b"geo": json.dumps({"columns": None}).encode("utf-8")},
        {b"geo": json.dumps({"columns": None})},
        {b"geo": {"columns": None}},
        {"geo": json.dumps({"columns": None})},
        {"geo": {"columns": None}},
    ],
    ids=["bytes_key_bytes", "bytes_key_str", "bytes_key_dict", "str_key_str", "str_key_dict"],
)
def test_strategy_base_reader_sanitizes_every_key_shape(metadata):
    """The shape check applies whichever way the ``geo`` key was handed over."""
    from geoparquet_io.core.write_strategies.base import _parse_existing_geo_metadata

    reset_malformed_geo_warnings()
    assert _parse_existing_geo_metadata(metadata) == {}


def test_parse_geo_metadata_quietly_delegates_to_the_shared_check():
    """``common._parse_geo_metadata_quietly`` must not keep a second copy of the check."""
    from geoparquet_io.core.common import _parse_geo_metadata_quietly

    reset_malformed_geo_warnings()
    raw = json.dumps({"primary_column": 1, "columns": ["geometry"]}).encode("utf-8")
    assert _parse_geo_metadata_quietly({b"geo": raw}) == {}


def test_extract_crs_from_table_survives_a_malformed_block():
    """``Table.write`` resolves the CRS before building metadata, through this reader."""
    from geoparquet_io.core.streaming import extract_crs_from_table

    reset_malformed_geo_warnings()
    assert extract_crs_from_table(_malformed_table({"geometry": "WKB"}), "geometry") is None
    assert extract_crs_from_table(_malformed_table(None), "geometry") is None


# =============================================================================
# The CRS and geometry-column readers reached before a write (#887)
# =============================================================================

# #883 routed four write-path readers through the shared check. These are the
# rest: `crs_utils` resolves the input's CRS and `convert` resolves its geometry
# columns *before* any metadata is built, and both indexed the raw block. They
# are write-path readers too -- every caller (`convert geoparquet`, `convert
# reproject`, `format_writers`, `process aggregate`, and the table-centric API)
# feeds the answer into a transform or an output file, and none of them is
# `gpio check` -- so they sanitize rather than report.


#: Every malformed shape these readers can be handed, block-level included.
#: The entries are otherwise complete 1.1 blocks, so the shape named in the id
#: is the only thing wrong with each one.
#:
#: Spelled for an arbitrary geometry column name, because "the file calls its
#: geometry ``geometry``" is exactly the assumption these readers must not make:
#: a recovery that falls back to the literal ``"geometry"`` looks correct on a
#: ``geometry`` file and silently reports no CRS on a ``geom`` one (#887 review).
def _malformed_blocks(col: str):
    return [
        ("columns_null", {"version": "1.1.0", "primary_column": col, "columns": None}),
        ("columns_list", {"version": "1.1.0", "primary_column": col, "columns": [col]}),
        ("columns_string", {"version": "1.1.0", "primary_column": col, "columns": col}),
        (
            "entry_not_an_object",
            {"version": "1.1.0", "primary_column": col, "columns": {col: "WKB"}},
        ),
        ("block_is_a_list", [col]),
        ("block_is_a_string", col),
        (
            "primary_column_is_a_number",
            {
                "version": "1.1.0",
                "primary_column": 123,
                "columns": {col: {"encoding": "WKB", "geometry_types": ["Point"]}},
            },
        ),
        (
            "encoding_is_a_number",
            {
                "version": "1.1.0",
                "primary_column": col,
                "columns": {col: {"encoding": 123, "geometry_types": ["Point"]}},
            },
        ),
    ]


MALFORMED_BLOCKS = [
    (col, case, block) for col in GEOMETRY_COLUMN_NAMES for case, block in _malformed_blocks(col)
]
MALFORMED_BLOCK_IDS = [f"{col}-{case}" for col, case, _ in MALFORMED_BLOCKS]


def _file_with_geo(tmp_path, name: str, geo_block, col: str = "geometry") -> str:
    """A real Parquet file carrying ``geo_block`` verbatim as its ``geo`` key."""
    path = tmp_path / f"{name}.parquet"
    pq.write_table(_table_with_geo(geo_block, col=col), path)
    return str(path)


@pytest.mark.parametrize(("col", "case", "block"), MALFORMED_BLOCKS, ids=MALFORMED_BLOCK_IDS)
def test_crs_utils_extract_crs_from_parquet_survives_a_malformed_block(col, case, block, tmp_path):
    """``convert``/``reproject``/``aggregate`` resolve the input CRS through here."""
    from geoparquet_io.core.crs_utils import extract_crs_from_parquet

    reset_malformed_geo_warnings()
    assert extract_crs_from_parquet(_file_with_geo(tmp_path, case, block, col=col)) is None


@pytest.mark.parametrize(("col", "case", "block"), MALFORMED_BLOCKS, ids=MALFORMED_BLOCK_IDS)
def test_crs_utils_extract_crs_from_table_survives_a_malformed_block(col, case, block):
    """The aggregate grid-keying reader (distinct from ``streaming``'s)."""
    from geoparquet_io.core.crs_utils import extract_crs_from_table

    reset_malformed_geo_warnings()
    assert extract_crs_from_table(_table_with_geo(block, col=col), col) is None


@pytest.mark.parametrize(("col", "case", "block"), MALFORMED_BLOCKS, ids=MALFORMED_BLOCK_IDS)
def test_geoparquet_crs_is_null_survives_a_malformed_block(col, case, block, tmp_path):
    """``convert reproject`` asks this before it decides how to read the input."""
    from geoparquet_io.core.crs_utils import geoparquet_crs_is_null

    reset_malformed_geo_warnings()
    assert geoparquet_crs_is_null(_file_with_geo(tmp_path, case, block, col=col)) is False


@pytest.mark.parametrize(("col", "case", "block"), MALFORMED_BLOCKS, ids=MALFORMED_BLOCK_IDS)
def test_crs_string_from_table_survives_a_malformed_block(col, case, block):
    """The table-centric transform-string reader, via ``crs_string_from_geo_meta``."""
    from geoparquet_io.core.crs_utils import crs_string_from_table

    reset_malformed_geo_warnings()
    assert crs_string_from_table(_table_with_geo(block, col=col), col) is None


@pytest.mark.parametrize("col", GEOMETRY_COLUMN_NAMES)
def test_geoparquet_crs_is_null_still_sees_an_explicit_null_crs(col, tmp_path):
    """Sanitizing must not swallow ``crs: null`` -- it is the spec's "unknown"."""
    reset_malformed_geo_warnings()
    from geoparquet_io.core.crs_utils import geoparquet_crs_is_null

    path = _file_with_geo(
        tmp_path,
        "null_crs",
        {"version": "1.1.0", "primary_column": col, "columns": {col: {"crs": None}}},
        col=col,
    )
    assert geoparquet_crs_is_null(path) is True


@pytest.mark.parametrize("col", GEOMETRY_COLUMN_NAMES)
def test_geometry_detection_never_returns_a_non_string_column_name(col, tmp_path):
    """The column-name readers are shared with ``gpio check``, so they guard, not sanitize.

    ``check spatial`` and ``check row-group`` ask ``find_primary_geometry_column``
    what the file calls its geometry -- they must keep seeing the file as it is.
    But a name is a string: a carried ``primary_column: 123`` handed back here
    reaches ``quote_identifier`` and fails there with a bare ``TypeError``
    (#887). Falling through to schema detection answers with the real column --
    or, when DuckDB will not open the file to describe it either, with ``None``,
    which the callers already turn into "No geometry column detected".
    """
    from geoparquet_io.core.geometry_detection import (
        detect_parquet_geometry_column,
        find_primary_geometry_column,
    )

    reset_malformed_geo_warnings()
    readable = _file_with_geo(
        tmp_path,
        "pc_number_only",
        {
            "version": "1.1.0",
            "primary_column": 123,
            "columns": {col: {"encoding": "WKB", "geometry_types": ["Point"]}},
        },
        col=col,
    )
    assert detect_parquet_geometry_column(readable) == col
    assert find_primary_geometry_column(readable) == col

    unreadable = _file_with_geo(
        tmp_path,
        "pc_number_cols_null",
        {"version": "1.1.0", "primary_column": 123, "columns": None},
        col=col,
    )
    assert detect_parquet_geometry_column(unreadable) is None
    assert find_primary_geometry_column(unreadable) == "geometry"


def test_geometry_detection_names_the_ignored_primary_column_key(tmp_path, caplog):
    """An ignored metadata key gets named in a warning -- #883's rule (#887 review).

    Without it ``gpio sort hilbert`` / ``check spatial`` / ``add bbox`` exit 0
    with nothing said, while ``gpio convert geoparquet`` on the same file warns.
    """
    from geoparquet_io.core.geometry_detection import find_primary_geometry_column

    reset_malformed_geo_warnings()
    src = _file_with_geo(
        tmp_path,
        "pc_number_warn",
        {
            "version": "1.1.0",
            "primary_column": 123,
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
        },
    )
    with caplog.at_level(logging.WARNING):
        assert find_primary_geometry_column(src) == "geometry"

    messages = _malformed_warnings(caplog.records)
    assert any("'primary_column'" in m and "number" in m for m in messages), messages


def test_geometry_detection_warns_once_per_file_not_once_per_process(tmp_path, caplog):
    """The warn-once cache keys on the message, so the message has to name the file.

    Two different malformed inputs in one process (a Python API loop, a shell
    ``for``) otherwise share one cache entry and the second file is ignored in
    silence (#887 review).
    """
    from geoparquet_io.core.geometry_detection import find_primary_geometry_column

    reset_malformed_geo_warnings()
    block = {
        "version": "1.1.0",
        "primary_column": 123,
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
    }
    first = _file_with_geo(tmp_path, "first", block)
    second = _file_with_geo(tmp_path, "second", block)

    with caplog.at_level(logging.WARNING):
        find_primary_geometry_column(first)
        find_primary_geometry_column(second)

    messages = _malformed_warnings(caplog.records)
    assert any("first.parquet" in m for m in messages), messages
    assert any("second.parquet" in m for m in messages), messages


@pytest.mark.parametrize(
    ("case", "name", "expected"),
    [("string", "geom", "geom"), ("number", 123, "geometry")],
    ids=["a_string_name_is_used", "a_number_name_falls_back"],
)
def test_find_primary_geometry_column_guards_the_legacy_list_block(case, name, expected, tmp_path):
    """The pre-1.0 list-shaped ``geo`` block needs the same string guard (#887)."""
    from geoparquet_io.core.geometry_detection import find_primary_geometry_column

    reset_malformed_geo_warnings()
    src = _file_with_geo(tmp_path, f"legacy_{case}", [{"name": name, "primary": True}])
    assert find_primary_geometry_column(src) == expected


@pytest.mark.parametrize(("col", "case", "block"), MALFORMED_BLOCKS, ids=MALFORMED_BLOCK_IDS)
def test_detect_all_geometry_columns_survives_a_malformed_block(col, case, block, tmp_path):
    """``convert`` resolves the geometry columns here, before it builds any query.

    Every name and encoding this returns is spliced into SQL or copied into the
    output block, so each one has to be a string -- never the ``123`` a carried
    ``primary_column`` or ``encoding`` can hold.
    """
    from geoparquet_io.core.convert import detect_all_geometry_columns

    reset_malformed_geo_warnings()
    info = detect_all_geometry_columns(_file_with_geo(tmp_path, case, block, col=col))

    assert info["primary"] is None or isinstance(info["primary"], str)
    assert all(isinstance(name, str) for name in info["metadata"])
    for col_meta in info["metadata"].values():
        assert isinstance(col_meta, dict)
        assert isinstance(col_meta.get("encoding", "WKB"), str)


@pytest.mark.parametrize("col", GEOMETRY_COLUMN_NAMES)
def test_detect_all_geometry_columns_falls_back_when_the_columns_are_unusable(col, tmp_path):
    """Nothing left of ``columns`` means the block-less answer: detect from the schema."""
    from geoparquet_io.core.convert import detect_all_geometry_columns

    reset_malformed_geo_warnings()
    src = _file_with_geo(
        tmp_path,
        "cols_null",
        {"version": "1.1.0", "primary_column": col, "columns": None},
        col=col,
    )
    assert detect_all_geometry_columns(src) == {
        "primary": col,
        "secondary": [],
        "metadata": {col: {"encoding": "WKB"}},
    }


def test_detect_all_geometry_columns_asks_the_schema_when_no_primary_is_left(tmp_path):
    """Two columns and no usable ``primary_column``: the schema decides, not ``"geometry"``.

    The single-column case is repaired by the sanitizer; this is the one it
    cannot repair, and falling back to the literal name would pick a column the
    file does not have (#887 review).
    """
    from geoparquet_io.core.convert import detect_all_geometry_columns

    reset_malformed_geo_warnings()
    table = pa.table(
        {
            "id": [1],
            "geom": pa.array([POINT_WKB], type=pa.binary()),
            "other_geom": pa.array([POINT_WKB], type=pa.binary()),
        }
    )
    entry = {"encoding": "WKB", "geometry_types": ["Point"]}
    block = {
        "version": "1.1.0",
        "primary_column": 123,
        "columns": {"geom": dict(entry), "other_geom": dict(entry)},
    }
    src = tmp_path / "two_cols.parquet"
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(block).encode("utf-8")}), src)

    info = detect_all_geometry_columns(str(src))
    assert info["primary"] == "geom"
    assert info["secondary"] == ["other_geom"]


def test_convert_geoparquet_recovers_from_a_non_string_primary_column(tmp_path, caplog):
    """``primary_column: 123`` used to reach ``quote_identifier`` as a bare TypeError.

    Nothing else about this file is wrong, so the conversion now runs to
    completion and the output names the geometry column found in the schema.
    """
    from geoparquet_io.core.convert import convert_to_geoparquet

    reset_malformed_geo_warnings()
    src = _file_with_geo(
        tmp_path,
        "pc_number",
        {
            "version": "1.1.0",
            "primary_column": 123,
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
        },
    )
    out = tmp_path / "pc_number_out.parquet"
    with caplog.at_level(logging.WARNING):
        convert_to_geoparquet(src, str(out), compression="SNAPPY")

    _assert_fresh_and_valid(_geo_of_file(out))
    assert pq.ParquetFile(out).metadata.num_rows == 1

    messages = _malformed_warnings(caplog.records)
    assert any("'primary_column'" in m and "number" in m for m in messages), messages


@pytest.mark.parametrize(("col", "case", "block"), MALFORMED_BLOCKS, ids=MALFORMED_BLOCK_IDS)
def test_convert_geoparquet_never_fails_with_a_bare_type_error(col, case, block, tmp_path, caplog):
    """``gpio convert geoparquet`` on somebody else's file: no ``TypeError`` escapes.

    Some of these files cannot be converted at all -- DuckDB's own GeoParquet
    reader refuses to open a block whose ``columns`` or ``encoding`` is the
    wrong type, which is outside gpio's reach (#887, #771). What gpio owes the
    user either way is a domain error naming the real cause, plus the warning
    that says which key was malformed -- not a ``TypeError`` from indexing the
    block three frames deep.
    """
    from geoparquet_io.core.convert import convert_to_geoparquet
    from geoparquet_io.core.exceptions import GeoParquetError

    reset_malformed_geo_warnings()
    out = tmp_path / f"{case}_out.parquet"
    with caplog.at_level(logging.WARNING):
        try:
            convert_to_geoparquet(
                _file_with_geo(tmp_path, case, block, col=col), str(out), compression="SNAPPY"
            )
        except GeoParquetError as exc:
            chain, seen = [], exc.__cause__
            while seen is not None and seen not in chain:
                chain.append(seen)
                seen = seen.__cause__
            assert not any(isinstance(c, (TypeError, AttributeError)) for c in chain), chain
        else:
            _assert_fresh_and_valid(_geo_of_file(out), col=col)
            assert pq.ParquetFile(out).metadata.num_rows == 1


def test_convert_geoparquet_names_the_malformed_key(tmp_path, caplog):
    """One warning, naming the offending key and the JSON type actually found."""
    from geoparquet_io.core.convert import convert_to_geoparquet
    from geoparquet_io.core.exceptions import GeoParquetError

    reset_malformed_geo_warnings()
    src = _file_with_geo(
        tmp_path, "warn", {"version": "1.1.0", "primary_column": "geometry", "columns": None}
    )
    with caplog.at_level(logging.WARNING), pytest.raises(GeoParquetError):
        convert_to_geoparquet(src, str(tmp_path / "warn_out.parquet"), compression="SNAPPY")

    messages = _malformed_warnings(caplog.records)
    assert any("'columns'" in m and "null" in m for m in messages), messages


# =============================================================================
# The public API boundary -- every write strategy
# =============================================================================


@pytest.mark.parametrize("strategy", WRITE_STRATEGIES)
@pytest.mark.parametrize(("case", "columns", "_hint"), MALFORMED_COLUMNS)
def test_table_write_survives_a_malformed_columns_block(case, columns, _hint, strategy, tmp_path):
    from geoparquet_io.api import Table

    reset_malformed_geo_warnings()
    out = tmp_path / f"{case}_{strategy}.parquet"
    Table(_malformed_table(columns)).write(
        out, write_strategy=strategy, geoparquet_version="1.1", compression="SNAPPY"
    )

    written = pq.ParquetFile(out).schema_arrow.metadata
    _assert_fresh_and_valid(json.loads(written[b"geo"].decode("utf-8")))


# =============================================================================
# Undecodable ``geo`` bytes: truncated JSON and invalid UTF-8
# =============================================================================

# A `geo` value can fail one step before the shape check: bytes that are not
# UTF-8 crash `.decode`, and a truncated payload crashes `json.loads`. Both are
# properties of the input, so they get the same treatment as a malformed shape:
# fresh metadata, one warning naming the cause (#883 review).

UNDECODABLE_GEO = [
    ("truncated_json", b'{"version": "1.1.0", "columns": {', "JSON"),
    ("invalid_utf8", b'\xff\xfe{"columns": {}}', "UTF-8"),
]


def _corrupt_geo_file(tmp_path, cause: str):
    """A real GeoParquet file whose ``geo`` bytes are overwritten in place.

    Byte surgery on a valid gpio-written 2.0 file keeps the thrift footer and
    the native GEOMETRY logical type intact, so only the ``geo`` value itself
    is undecodable -- the shape a partially-written or wrongly-encoded footer
    actually takes in the wild.
    """
    from geoparquet_io.api import Table

    src = tmp_path / "valid_src.parquet"
    table = pa.table({"id": [1], "geometry": pa.array([POINT_WKB], type=pa.binary())})
    Table(table).write(src, geoparquet_version="2.0", compression="SNAPPY")

    geo = pq.read_metadata(src).metadata[b"geo"]
    raw = src.read_bytes()
    assert raw.count(geo) == 1
    replacement = b"{" + b" " * (len(geo) - 1) if cause == "JSON" else b"\xff" * len(geo)
    corrupted = tmp_path / "corrupt_geo.parquet"
    corrupted.write_bytes(raw.replace(geo, replacement))
    return corrupted


@pytest.mark.parametrize("cause", ["JSON", "UTF-8"])
def test_get_geo_metadata_tolerates_undecodable_bytes_on_both_paths(cause, tmp_path):
    """Both readers give the answer invalid JSON already got: no usable metadata."""
    from geoparquet_io.core.common import get_duckdb_connection
    from geoparquet_io.core.duckdb_metadata import get_geo_metadata

    src = _corrupt_geo_file(tmp_path, cause)
    assert get_geo_metadata(str(src)) is None  # PyArrow fast path
    con = get_duckdb_connection()
    assert get_geo_metadata(str(src), con=con) is None  # DuckDB path (remote files)


@pytest.mark.parametrize("version", ["1.1", "2.0"])
@pytest.mark.parametrize("cause", ["JSON", "UTF-8"])
def test_extract_survives_undecodable_geo_bytes(cause, version, tmp_path, caplog):
    """The reviewer's repro: ``gpio extract geoparquet in.parquet out.parquet --limit 1``.

    2.0 output takes a second raw-decode path before the rewrite decision
    (``needs_metadata_rewrite``), so both versions must survive.
    """
    from geoparquet_io.core.extract import extract

    reset_malformed_geo_warnings()
    src = _corrupt_geo_file(tmp_path, cause)
    out = tmp_path / "out.parquet"

    with caplog.at_level(logging.WARNING):
        extract(str(src), str(out), limit=1, geoparquet_version=version)

    _assert_fresh_and_valid(_geo_of_file(out))
    messages = _malformed_warnings(caplog.records)
    assert any("'geo'" in m and cause in m for m in messages), messages


@pytest.mark.parametrize(("case", "geo_bytes", "cause"), UNDECODABLE_GEO)
def test_apply_metadata_survives_undecodable_geo_bytes(case, geo_bytes, cause, caplog):
    """The exact bytes-level repro from the #883 review of `_parse_existing_geo_metadata`."""
    reset_malformed_geo_warnings()
    table = pa.table({"id": [1], "geometry": pa.array([POINT_WKB], type=pa.binary())})

    with caplog.at_level(logging.WARNING):
        result = _apply_geoparquet_metadata(
            table, "geometry", "1.1", original_metadata={b"geo": geo_bytes}
        )

    _assert_fresh_and_valid(_geo_of(result))
    messages = _malformed_warnings(caplog.records)
    assert len(messages) == 1, messages
    assert cause in messages[0]


@pytest.mark.parametrize("version", ["1.1", "2.0"])
@pytest.mark.parametrize("strategy", WRITE_STRATEGIES)
@pytest.mark.parametrize(("case", "geo_bytes", "_cause"), UNDECODABLE_GEO)
def test_table_write_survives_undecodable_geo_bytes(
    case, geo_bytes, _cause, strategy, version, tmp_path
):
    """Undecodable bytes must not abort any strategy (2.0 also reads them pre-rewrite)."""
    from geoparquet_io.api import Table

    reset_malformed_geo_warnings()
    table = pa.table({"id": [1], "geometry": pa.array([POINT_WKB], type=pa.binary())})
    table = table.replace_schema_metadata({b"geo": geo_bytes})
    out = tmp_path / f"{case}_{strategy}_{version}.parquet"
    Table(table).write(
        out, write_strategy=strategy, geoparquet_version=version, compression="SNAPPY"
    )
    _assert_fresh_and_valid(_geo_of_file(out))


# =============================================================================
# Wrong-typed carried values: well-shaped entries whose values readers reject
# =============================================================================

# A column entry can be an object and still poison the output: `"crs": 42`
# passes a shape-only check, is carried verbatim, and the written file is then
# refused by DuckDB ("has invalid CRS") and by `gpio check spec`. The decision
# from the #883 review: type-check the carried values too, drop-and-warn the
# wrong-typed ones.

WRONG_TYPED_VALUES = [
    ("crs", 42, "number"),
    ("crs", [4326], "array"),
    ("crs", True, "boolean"),
    ("encoding", 7, "number"),
    ("epoch", "2020", "string"),
    ("epoch", True, "boolean"),
    ("orientation", 1, "number"),
    ("edges", {"type": "spherical"}, "object"),
    ("covering", "bbox", "string"),
    ("geometry_types", "Point", "string"),
    ("geometry_types", [1, 2], "array"),
    ("bbox", "0,0,1,1", "string"),
    ("bbox", [0.0, 0.0, 1.0], "array"),
]

#: Values of the right type must carry through untouched -- including the
#: spec-sanctioned ``crs: null`` ("CRS is unknown"), which is not wrong-typed.
WELL_TYPED_VALUES = [
    ("crs", None),
    ("crs", "OGC:CRS84"),
    ("crs", {"type": "GeographicCRS", "id": {"authority": "OGC", "code": "CRS84"}}),
    ("encoding", "WKB"),
    ("epoch", 2020),
    ("epoch", 2020.5),
    ("orientation", "counterclockwise"),
    ("edges", "spherical"),
    ("covering", {"bbox": {"xmin": ["bbox", "xmin"]}}),
    ("geometry_types", ["Point", "MultiPolygon Z"]),
    ("bbox", [0.0, 0.0, 1.0, 1.0]),
    ("bbox", [0, 0, 0, 1, 1, 1]),
]


def _geo_of_file(path) -> dict:
    return json.loads(pq.ParquetFile(path).schema_arrow.metadata[b"geo"].decode("utf-8"))


class TestSanitizeWrongTypedValues:
    @pytest.mark.parametrize(("key", "value", "type_name"), WRONG_TYPED_VALUES)
    def test_drops_and_names_the_key_and_type(self, key, value, type_name, caplog):
        reset_malformed_geo_warnings()
        block = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", key: value}},
        }
        if key == "encoding":
            block["columns"]["geometry"] = {key: value}

        with caplog.at_level(logging.WARNING):
            cleaned = sanitize_geo_metadata(block)

        assert key not in cleaned["columns"]["geometry"]
        messages = _malformed_warnings(caplog.records)
        assert len(messages) == 1, messages
        assert f"'{key}'" in messages[0]
        assert "'geometry'" in messages[0]
        assert type_name in messages[0]

    @pytest.mark.parametrize(("key", "value"), WELL_TYPED_VALUES)
    def test_keeps_well_typed_values_untouched(self, key, value, caplog):
        reset_malformed_geo_warnings()
        block = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", key: value}},
        }
        with caplog.at_level(logging.WARNING):
            assert sanitize_geo_metadata(block) is block
        assert _malformed_warnings(caplog.records) == []

    def test_does_not_mutate_the_carried_entry(self):
        reset_malformed_geo_warnings()
        entry = {"encoding": "WKB", "crs": 42}
        block = {"columns": {"geometry": entry}}
        cleaned = sanitize_geo_metadata(block)
        assert entry["crs"] == 42
        assert block["columns"]["geometry"] is entry
        assert "crs" not in cleaned["columns"]["geometry"]

    def test_keeps_the_rest_of_the_entry(self):
        reset_malformed_geo_warnings()
        cleaned = sanitize_geo_metadata(
            {"columns": {"geometry": {"encoding": "WKB", "crs": 42, "epoch": 2020.0}}}
        )
        assert cleaned["columns"]["geometry"] == {"encoding": "WKB", "epoch": 2020.0}


@pytest.mark.parametrize("strategy", WRITE_STRATEGIES)
def test_table_write_drops_a_wrong_typed_crs(strategy, tmp_path, caplog):
    """The reviewer's exact repro: ``"crs": 42`` must not reach the output.

    The recovered output must actually be *valid*: gpio's own ``check spec``
    passes and DuckDB agrees to open it (it refuses a non-object ``crs`` with
    "Geoparquet column 'geometry' has invalid CRS").
    """
    from geoparquet_io.api import Table
    from geoparquet_io.core.common import get_duckdb_connection
    from geoparquet_io.core.duckdb_utils import sql_path
    from geoparquet_io.core.validate import validate_geoparquet

    reset_malformed_geo_warnings()
    table = _table_with_geo(
        {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "crs": 42}},
        }
    )
    out = tmp_path / f"crs42_{strategy}.parquet"
    with caplog.at_level(logging.WARNING):
        Table(table).write(
            out, write_strategy=strategy, geoparquet_version="1.1", compression="SNAPPY"
        )

    geo = _geo_of_file(out)
    _assert_fresh_and_valid(geo)
    assert "crs" not in geo["columns"]["geometry"]

    messages = _malformed_warnings(caplog.records)
    assert any("'crs'" in m and "number" in m for m in messages), messages

    failed = {c.name for c in validate_geoparquet(str(out)).checks if c.status.value == "failed"}
    assert not failed

    con = get_duckdb_connection(load_spatial=True)
    assert con.execute(f"SELECT count(*) FROM read_parquet({sql_path(out)})").fetchone()[0] == 1


# =============================================================================
# The recovery must not answer with the wrong column (#887 review)
# =============================================================================

# Dropping a malformed `primary_column` and then falling back to the literal
# string "geometry" turns a loud crash into silently wrong output: on a file
# whose geometry column is `geom` (or `wkb_geometry`, `shape`, `the_geom` --
# all in gpio's own detection list) the lookup misses and the file's CRS is
# reported as absent. Two consequences, both verified below:
#   (a) `convert reproject` bypasses the explicit-null-CRS guard and transforms
#       unknown coordinates as if they were lon/lat;
#   (b) `process aggregate h3` keys a projected file as if it were lon/lat.
# The fix is the sanitizer *repairing* `primary_column` from the only column
# there is, plus schema detection (never the literal name) where it cannot.


def _crs_5070() -> dict:
    from pyproj import CRS

    return CRS.from_epsg(5070).to_json_dict()


def _malformed_primary_file(tmp_path, name: str, col: str, col_meta: dict) -> str:
    """A one-row file on column ``col`` whose ``primary_column`` is unusable."""
    return _file_with_geo(
        tmp_path,
        name,
        {"version": "1.1.0", "primary_column": 123, "columns": {col: col_meta}},
        col=col,
    )


@pytest.mark.parametrize("col", GEOMETRY_COLUMN_NAMES)
def test_reproject_still_refuses_an_explicit_null_crs_after_recovery(col, tmp_path):
    """(a) The null-CRS guard must survive the recovery, whatever the column is called.

    ``crs: null`` means *unknown*. Reprojecting it as if it were CRS84 writes
    coordinates that are simply wrong, and says "Source CRS: EPSG:4326" while
    doing it -- worse than the ``TypeError`` this recovery replaced.
    """
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    reset_malformed_geo_warnings()
    src = _malformed_primary_file(
        tmp_path, "null_crs_bad_primary", col, {"encoding": "WKB", "crs": None}
    )
    result = CliRunner().invoke(
        cli,
        ["convert", "reproject", src, str(tmp_path / "out.parquet"), "--dst-crs", "EPSG:3857"],
    )
    assert result.exit_code != 0, result.output
    assert "explicit null CRS" in result.output
    assert "--assume-crs84" in result.output


@pytest.mark.parametrize("col", GEOMETRY_COLUMN_NAMES)
def test_reproject_table_still_refuses_an_explicit_null_crs_after_recovery(col):
    """The same guard on the table-centric API (``ops.reproject`` / ``Table.reproject``)."""
    from geoparquet_io.core.reproject import reproject_table

    reset_malformed_geo_warnings()
    table = _table_with_geo(
        {
            "version": "1.1.0",
            "primary_column": 123,
            "columns": {col: {"encoding": "WKB", "crs": None}},
        },
        col=col,
    )
    with pytest.raises(ValueError, match="null CRS"):
        reproject_table(table, target_crs="EPSG:3857")


@pytest.mark.parametrize("col", GEOMETRY_COLUMN_NAMES)
def test_aggregate_h3_keys_a_projected_file_the_same_way_with_or_without_the_block(col, tmp_path):
    """(b) A projected file must not be aggregated as lon/lat because of a bad key.

    The control is the identical file with a well-formed ``primary_column``:
    the recovered run has to land in the same H3 cell, not somewhere off the
    coast of Africa.
    """
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    crs = _crs_5070()
    cells = {}
    for label, primary in (("control", col), ("recovered", 7)):
        src = _file_with_geo(
            tmp_path,
            f"h3_{label}",
            {
                "version": "1.1.0",
                "primary_column": primary,
                "columns": {col: {"encoding": "WKB", "crs": crs, "geometry_types": ["Point"]}},
            },
            col=col,
        )
        out = tmp_path / f"h3_{label}_out.parquet"
        reset_malformed_geo_warnings()
        result = CliRunner().invoke(
            cli, ["process", "aggregate", "h3", src, str(out), "--resolution", "5"]
        )
        assert result.exit_code == 0, result.output
        cells[label] = pq.read_table(out).column("h3_cell").to_pylist()

    assert cells["recovered"] == cells["control"]


@pytest.mark.parametrize("col", GEOMETRY_COLUMN_NAMES)
def test_extract_crs_from_parquet_finds_a_projected_crs_after_recovery(col, tmp_path):
    """The unit under (b): the CRS is still found once ``primary_column`` is repaired."""
    from geoparquet_io.core.crs_utils import extract_crs_from_parquet

    reset_malformed_geo_warnings()
    src = _malformed_primary_file(
        tmp_path, "proj_bad_primary", col, {"encoding": "WKB", "crs": _crs_5070()}
    )
    assert extract_crs_from_parquet(src) is not None


@pytest.mark.parametrize("col", GEOMETRY_COLUMN_NAMES)
def test_extract_crs_from_table_finds_a_projected_crs_after_recovery(col):
    """The table-centric sibling, used by ``process aggregate`` before grid keying."""
    from geoparquet_io.core.crs_utils import extract_crs_from_table

    reset_malformed_geo_warnings()
    table = _table_with_geo(
        {
            "version": "1.1.0",
            "primary_column": 123,
            "columns": {col: {"encoding": "WKB", "crs": _crs_5070()}},
        },
        col=col,
    )
    assert extract_crs_from_table(table) is not None


# =============================================================================
# `reproject` reads the carried block too (#887 bullet 3, still live on the API)
# =============================================================================


@pytest.mark.parametrize("col", GEOMETRY_COLUMN_NAMES)
def test_ops_reproject_survives_a_non_string_primary_column(col):
    """``ops.reproject`` / ``Table.reproject`` raised the exact ``TypeError`` from #887.

    ``reproject._detect_geometry_column_from_table`` handed ``primary_column:
    123`` straight to ``quote_identifier``, which fails with
    ``argument of type 'int' is not iterable``.
    """
    from geoparquet_io.api import Table, ops

    reset_malformed_geo_warnings()
    table = _table_with_geo(
        {"version": "1.1.0", "primary_column": 123, "columns": {col: {"encoding": "WKB"}}},
        col=col,
    )
    assert col in ops.reproject(table, target_crs="EPSG:3857").column_names
    assert col in Table(table).reproject("EPSG:3857").table.column_names


@pytest.mark.parametrize(("col", "case", "block"), MALFORMED_BLOCKS, ids=MALFORMED_BLOCK_IDS)
def test_reproject_table_readers_survive_a_malformed_block(col, case, block):
    """The three ``reproject`` readers of the raw block: no ``TypeError``/``AttributeError``.

    A string- or list-shaped block, ``columns: null`` and a non-object column
    entry each crashed one of them with a bare ``AttributeError`` (#887 review).
    """
    from geoparquet_io.core.reproject import (
        _detect_crs_from_table,
        _detect_geometry_column_from_table,
        _table_geo_column_meta,
    )

    reset_malformed_geo_warnings()
    table = _table_with_geo(block, col=col)

    detected = _detect_geometry_column_from_table(table)
    assert isinstance(detected, str)
    assert isinstance(_detect_crs_from_table(table, detected), str)
    assert isinstance(_table_geo_column_meta(table, detected), dict)


@pytest.mark.parametrize(("col", "case", "block"), MALFORMED_BLOCKS, ids=MALFORMED_BLOCK_IDS)
def test_reproject_table_never_fails_with_a_bare_type_error(col, case, block, tmp_path):
    """``Table.reproject`` end to end: a domain error is fine, a bare TypeError is not."""
    from geoparquet_io.core.reproject import reproject_table

    reset_malformed_geo_warnings()
    table = _table_with_geo(block, col=col)
    try:
        result = reproject_table(table, target_crs="EPSG:3857")
    except (ValueError, duckdb.Error):
        pass  # a domain error naming the real cause is the contract
    else:
        assert result.num_rows == 1


def test_carried_column_name_says_nothing_about_an_absent_key(caplog):
    """An absent ``primary_column`` is not malformed -- only a wrong-typed one warns."""
    from geoparquet_io.core.geo_metadata import carried_column_name

    reset_malformed_geo_warnings()
    with caplog.at_level(logging.WARNING):
        assert carried_column_name(None) is None
        assert carried_column_name("") is None
        assert carried_column_name("geom") == "geom"
    assert _malformed_warnings(caplog.records) == []


def test_geoparquet_crs_is_null_says_false_when_no_column_can_be_named(tmp_path):
    """Nothing names a column and DuckDB will not describe the file: not a null CRS.

    ``columns: null`` leaves the block with no ``columns`` to repair a
    ``primary_column`` from, and the same malformed block stops DuckDB opening
    the file to detect one, so there is no column to ask about (#887 review).
    """
    from geoparquet_io.core.crs_utils import geoparquet_crs_is_null

    reset_malformed_geo_warnings()
    src = _file_with_geo(
        tmp_path,
        "no_primary_no_columns",
        {"version": "1.1.0", "primary_column": 123, "columns": None},
    )
    assert geoparquet_crs_is_null(src) is False


def test_extract_crs_from_table_says_none_when_no_column_can_be_named():
    """No usable ``primary_column`` and no standard name in the schema: no CRS."""
    from geoparquet_io.core.crs_utils import extract_crs_from_table

    reset_malformed_geo_warnings()
    table = pa.table({"id": [1], "footprint": pa.array([POINT_WKB], type=pa.binary())})
    table = table.replace_schema_metadata(
        {
            b"geo": json.dumps(
                {
                    "version": "1.1.0",
                    "primary_column": 123,
                    "columns": {"a": {"encoding": "WKB"}, "b": {"encoding": "WKB"}},
                }
            ).encode("utf-8")
        }
    )
    assert extract_crs_from_table(table) is None


def test_detect_all_geometry_columns_on_a_non_parquet_input(tmp_path):
    """The non-GeoParquet arm answers with the one column ``ST_Read`` shows."""
    from geoparquet_io.core.convert import detect_all_geometry_columns

    src = tmp_path / "one.geojson"
    src.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"id": 1},
                        "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    info = detect_all_geometry_columns(str(src))
    assert info["primary"] == "geom"
    assert info["metadata"] == {"geom": {"encoding": "WKB"}}
    assert info["secondary"] == []
