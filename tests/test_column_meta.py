"""``geo_metadata.parse_column_meta`` and ``bbox_xy_or_none``: the read model (#1089).

A ``geo`` column entry is parsed once; a wrong-typed value is recorded as a
problem (the FAILED text for the check that owns the key) and never handed to a
check as if it were well-typed.
"""

from __future__ import annotations

from geoparquet_io.core.geo_metadata import (
    ColumnMeta,
    CoveringBbox,
    bbox_xy_or_none,
    parse_column_meta,
)

PATHS = {key: ["bbox", key] for key in ("xmin", "ymin", "xmax", "ymax")}

#: ``(key, wrong-typed value)``: at least one per key ``parse_column_meta`` types.
WRONG_TYPED = [
    ("encoding", 42),
    ("geometry_types", "Point"),
    ("geometry_types", [1]),
    ("crs", 42),
    ("orientation", ["counterclockwise"]),
    ("edges", {"spherical": True}),
    ("bbox", "1234"),
    ("bbox", [0, 0, 1]),
    ("bbox", [0, 0, float("nan"), 1]),
    ("epoch", True),
    ("covering", "bbox"),
]


def test_a_wrong_typed_value_is_a_problem_not_a_value():
    for key, value in WRONG_TYPED:
        meta = parse_column_meta("geometry", {key: value})
        assert list(meta.problems) == [key], (key, value)
        assert '"geometry"' in meta.problems[key]
        assert getattr(meta, key) is None, (key, value)


def test_well_typed_values_are_kept():
    raw = {
        "encoding": "WKB",
        "geometry_types": ["Point"],
        "crs": {"type": "GeographicCRS"},
        "orientation": "counterclockwise",
        "edges": "spherical",
        "bbox": [0, 0, 0, 0, 1, 1, 1, 1],
        "epoch": 2020.5,
        "covering": {"bbox": PATHS},
    }
    meta = parse_column_meta("geometry", raw)
    assert meta.problems == {}
    assert (meta.encoding, meta.geometry_types, meta.crs) == ("WKB", ["Point"], raw["crs"])
    assert (meta.orientation, meta.edges, meta.epoch) == ("counterclockwise", "spherical", 2020.5)
    assert meta.bbox == raw["bbox"]
    assert meta.covering_bbox == CoveringBbox(paths=PATHS, column="bbox")


def test_null_reads_as_absent_and_a_non_object_as_empty():
    meta = parse_column_meta("g", dict.fromkeys(("orientation", "edges", "bbox", "epoch", "crs")))
    assert meta.problems == {}
    assert meta.crs is None and meta.bbox is None
    empty = parse_column_meta("g", "not an object")
    assert isinstance(empty, ColumnMeta) and empty.raw == {} and empty.problems == {}


def test_covering_bbox_is_resolved_once():
    cases = [
        ({"bbox": PATHS}, "bbox", None),
        ({"bbox": "x"}, None, "covering bbox must be an object of [column, field] paths"),
        ({"bbox": {**PATHS, "xmin": "bbox.xmin"}}, None, "its xmin must be a path array"),
        ({"bbox": {"ymin": ["bbox", "ymin"]}}, None, "it has no xmin path"),
    ]
    for covering, column, fragment in cases:
        ref = parse_column_meta("g", {"covering": covering}).covering_bbox
        assert ref is not None and ref.column == column, covering
        assert (ref.problem is None) if fragment is None else (fragment in ref.problem)
    assert parse_column_meta("g", {"covering": {"h3": {}}}).covering_bbox is None
    assert parse_column_meta("g", {"covering": "bbox"}).covering_bbox is None


def test_bbox_xy_or_none():
    xy = (0.0, 1.0, 2.0, 3.0)
    assert bbox_xy_or_none([0, 1, 2, 3]) == xy
    assert bbox_xy_or_none([0, 1, 9, 2, 3, 9]) == xy
    assert bbox_xy_or_none([0, 1, 9, 9, 2, 3, 9, 9]) == xy
    unusable = [
        "1234",
        {"a": 0, "b": 1, "c": 2, "d": 3},
        [0, 1, 2],
        ["0", "1", "2", "3"],
        [True, 0, 1, 1],
        [0, 0, float("inf"), 1],
        [0, 0, 10**400, 1],
        None,
    ]
    for bbox in unusable:
        assert bbox_xy_or_none(bbox) is None, bbox
