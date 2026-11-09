"""One wrong-typed ``geo`` column value, one FAILED verdict, no exception (#1089).

``check spec`` parses each column entry once (``geo_metadata.parse_column_meta``)
and the checks read the model. Before that every check re-derived the value's
shape, so a wrong-typed value either crashed the run or was judged again by the
checks downstream of the one that owns it: one defect, two or three FAILEDs.

Each case is a file that is valid except for one value, checked at 1.1 (plain
WKB) and at 2.0 (native GEOMETRY, which adds the v2 consistency checks), and
the run must end with exactly one FAILED: the check that owns the key.
"""

from __future__ import annotations

import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.duckdb_utils import sql_literal, sql_path
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet

# POINT (1 2), little-endian WKB.
WKB_POINT = bytes.fromhex("0101000000000000000000f03f0000000000000040")

#: ``(id, column override, the one check that must FAIL, message fragment)``.
WRONG_TYPED = [
    ("encoding_number", {"encoding": 42}, "encoding_valid", "must have valid encoding"),
    ("geometry_types_numbers", {"geometry_types": [1]}, "geometry_types_list", "invalid"),
    ("geometry_types_nested", {"geometry_types": [["Point"]]}, "geometry_types_list", "invalid"),
    ("crs_number", {"crs": 42}, "crs_valid", "CRS must be null or valid PROJJSON object"),
    ("orientation_number", {"orientation": 42}, "orientation_valid", "orientation"),
    ("edges_number", {"edges": 42}, "edges_valid", "edges"),
    ("bbox_string", {"bbox": "1234"}, "bbox_valid", "bbox must be an array"),
    ("epoch_boolean", {"epoch": True}, "epoch_valid", "epoch must be a number"),
    ("covering_string", {"covering": "bbox"}, "covering_is_object", "must be an object"),
    ("covering_bbox_string", {"covering": {"bbox": "b"}}, "covering_bbox_paths", "an object"),
    (
        "covering_bbox_xmin_string",
        {"covering": {"bbox": {k: ["bbox", k] for k in ("ymin", "xmax", "ymax")} | {"xmin": "b"}}},
        "covering_bbox_paths",
        "must be a path array",
    ),
]


def _geo(version: str, override: dict) -> str:
    column = {"encoding": "WKB", "geometry_types": ["Point"], **override}
    return json.dumps(
        {"version": version, "primary_column": "geometry", "columns": {"geometry": column}}
    )


def _write_v1(path, override: dict) -> str:
    table = pa.table({"geometry": pa.array([WKB_POINT] * 2, pa.binary())})
    pq.write_table(table.replace_schema_metadata({b"geo": _geo("1.1.0", override).encode()}), path)
    return str(path)


def _write_v2(path, override: dict) -> str:
    """Native GEOMETRY with the ``geo`` key written verbatim, malformation included."""
    con = duckdb.connect()
    try:
        con.execute("LOAD spatial")
        con.execute(
            "COPY (SELECT ST_Point(1, 2) AS geometry FROM range(2)) "
            f"TO {sql_path(path)} (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', "
            f"KV_METADATA {{geo: {sql_literal(_geo('2.0.0', override))}}})"
        )
    finally:
        con.close()
    return str(path)


WRITERS = {"1.1": _write_v1, "2.0": _write_v2}


def _failed(path) -> dict[str, str]:
    result = validate_geoparquet(path)
    return {c.name: c.message for c in result.checks if c.status == CheckStatus.FAILED}


@pytest.mark.parametrize("version", sorted(WRITERS))
def test_a_wrong_typed_value_fails_exactly_once(tmp_path, version):
    """Every case differs from a clean baseline by one value; each must FAIL once,
    in the check that owns the key. One test per version keeps the fast suite's
    collected count down; the message names every case that misbehaved."""
    write = WRITERS[version]
    assert _failed(write(tmp_path / "baseline.parquet", {})) == {}
    wrong = {}
    for case, override, check, fragment in WRONG_TYPED:
        try:
            failed = _failed(write(tmp_path / f"{case}.parquet", override))
        except Exception as exc:  # the crash is the finding, reported with the rest
            wrong[case] = f"raised {type(exc).__name__}: {exc}"
            continue
        if list(failed) != [f"{check}_geometry"] or fragment not in failed[f"{check}_geometry"]:
            wrong[case] = failed
    assert not wrong, wrong


@pytest.mark.parametrize("version", sorted(WRITERS))
def test_a_crs_whose_type_is_not_a_string_is_reported_not_raised(tmp_path, version):
    """``{"type": []}`` is an object, so the model keeps it; its unhashable ``type``
    member crashed the PROJJSON type lookup with ``TypeError``."""
    failed = _failed(WRITERS[version](tmp_path / "f.parquet", {"crs": {"type": []}}))
    assert "unknown PROJJSON type" in failed["crs_valid_geometry"]
