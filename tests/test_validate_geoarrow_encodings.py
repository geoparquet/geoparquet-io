"""Validation of GeoArrow-encoded GeoParquet 1.1 files (gpio #691).

GeoParquet 1.1 permits the single-geometry-type GeoArrow encodings
("point", "linestring", "polygon", "multipoint", "multilinestring",
"multipolygon") alongside "WKB". 1.0 and 2.0 are WKB-only per their spec
text. gpio's own ``--geoparquet-version 1.1-geoarrow`` output must therefore
pass ``validate_geoparquet`` end-to-end, and no check may crash (DuckDB binder
error) on a nested GeoArrow column.
"""

import json

import pyarrow.parquet as pq
import pytest
import shapely
from shapely import wkt

from geoparquet_io.core.common import (
    get_duckdb_connection,
    get_parquet_metadata,
    write_parquet_with_metadata,
)
from geoparquet_io.core.validate import (
    CheckStatus,
    _check_covering_bbox_field_types,
    _check_encoding_valid,
    _check_geometry_byte_array,
    _check_geometry_not_grouped,
    _geoarrow_zm_suffix,
    validate_geoparquet,
)

# encoding -> (WKTs, declared geometry_types)
GEOARROW_CASES = {
    "point": (["POINT (1 2)", "POINT (3 4)"], ["Point"]),
    "linestring": (["LINESTRING (0 0, 1 1, 2 0)"], ["LineString"]),
    "polygon": (["POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"], ["Polygon"]),
    "multipoint": (["MULTIPOINT ((0 0), (1 1))"], ["MultiPoint"]),
    "multilinestring": (["MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))"], ["MultiLineString"]),
    "multipolygon": (
        ["MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))"],
        ["MultiPolygon"],
    ),
}


def _write_wkb_source(path, wkts, geometry_types, version="1.1.0"):
    """Write a plain WKB GeoParquet file (the input the writer converts from)."""
    import pyarrow as pa

    geo = {
        "version": version,
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": geometry_types}},
    }
    geoms = [None if w is None else shapely.to_wkb(wkt.loads(w), flavor="iso") for w in wkts]
    table = pa.table({"id": list(range(len(geoms))), "geometry": geoms})
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), path)
    return path


def _write_geoarrow(tmp_path, name, wkts, geometry_types):
    """Produce a 1.1-geoarrow file through gpio's own write path."""
    src = _write_wkb_source(tmp_path / f"{name}_src.parquet", wkts, geometry_types)
    out = tmp_path / f"{name}.parquet"
    con = get_duckdb_connection()
    try:
        metadata, _ = get_parquet_metadata(str(src))
        write_parquet_with_metadata(
            con,
            f"SELECT * FROM read_parquet('{src.as_posix()}')",
            str(out),
            original_metadata=metadata,
            geoparquet_version="1.1-geoarrow",
            input_file=str(src),
        )
    finally:
        con.close()
    return out


def _rewrite_geo_metadata(path, out_path, mutate):
    """Copy a parquet file, mutating only its 'geo' schema metadata."""
    table = pq.read_table(path)
    geo = json.loads(table.schema.metadata[b"geo"])
    mutate(geo)
    other = {k: v for k, v in (table.schema.metadata or {}).items() if k != b"geo"}
    other[b"geo"] = json.dumps(geo).encode()
    pq.write_table(table.replace_schema_metadata(other), out_path)
    return out_path


def _checks_by_name(result):
    return {c.name: c for c in result.checks}


def _binder_errors(result):
    return [c.name for c in result.checks if "Binder Error" in (c.message or "")]


@pytest.fixture(scope="module")
def geoarrow_files(tmp_path_factory):
    """One 1.1-geoarrow file per permitted GeoArrow encoding."""
    base = tmp_path_factory.mktemp("geoarrow")
    return {
        encoding: _write_geoarrow(base, encoding, wkts, types)
        for encoding, (wkts, types) in GEOARROW_CASES.items()
    }


class TestGeoArrowFilesValidate:
    @pytest.mark.parametrize("encoding", sorted(GEOARROW_CASES))
    def test_geoarrow_file_passes_validation(self, geoarrow_files, encoding):
        path = geoarrow_files[encoding]
        written = json.loads(pq.read_schema(path).metadata[b"geo"])
        assert written["columns"]["geometry"]["encoding"] == encoding

        result = validate_geoparquet(str(path), validate_data=True, sample_size=0)
        failures = [
            f"{c.name}: {c.message}" for c in result.checks if c.status == CheckStatus.FAILED
        ]
        assert failures == [], failures
        assert result.is_valid

    @pytest.mark.parametrize("encoding", sorted(GEOARROW_CASES))
    def test_no_check_hits_a_duckdb_binder_error(self, geoarrow_files, encoding):
        result = validate_geoparquet(
            str(geoarrow_files[encoding]), validate_data=True, sample_size=0
        )
        assert _binder_errors(result) == []

    @pytest.mark.parametrize("encoding", sorted(GEOARROW_CASES))
    def test_data_checks_actually_run(self, geoarrow_files, encoding):
        """The data scans must report on the column, not silently vanish."""
        result = validate_geoparquet(
            str(geoarrow_files[encoding]), validate_data=True, sample_size=0
        )
        checks = _checks_by_name(result)
        for name in (
            "encoding_matches_data_geometry",
            "geometry_types_match_data_geometry",
            "bbox_contains_data_geometry",
            "coordinates_valid_for_crs_geometry",
        ):
            assert name in checks, sorted(checks)
            assert checks[name].status != CheckStatus.FAILED, checks[name].message


class TestEncodingVersionGating:
    """1.1 permits GeoArrow encodings; 1.0 and 2.0 are WKB-only per spec text."""

    @pytest.mark.parametrize("encoding", sorted(GEOARROW_CASES))
    def test_geoarrow_accepted_for_1_1(self, encoding):
        check = _check_encoding_valid({"encoding": encoding}, "geometry", "1.1.0")
        assert check.status == CheckStatus.PASSED, check.message

    @pytest.mark.parametrize("encoding", sorted(GEOARROW_CASES))
    def test_geoarrow_rejected_for_1_0(self, encoding):
        check = _check_encoding_valid({"encoding": encoding}, "geometry", "1.0.0")
        assert check.status == CheckStatus.FAILED
        assert "1.1" in check.message

    @pytest.mark.parametrize("encoding", sorted(GEOARROW_CASES))
    def test_geoarrow_rejected_for_2_0(self, encoding):
        check = _check_encoding_valid({"encoding": encoding}, "geometry", "2.0.0")
        assert check.status == CheckStatus.FAILED

    @pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "2.0.0"])
    def test_wkb_always_accepted(self, version):
        check = _check_encoding_valid({"encoding": "WKB"}, "geometry", version)
        assert check.status == CheckStatus.PASSED, check.message

    @pytest.mark.parametrize("encoding", ["WKT", "geoarrow.point", "Point", "", None])
    def test_unknown_encodings_still_rejected(self, encoding):
        check = _check_encoding_valid({"encoding": encoding}, "geometry", "1.1.0")
        assert check.status == CheckStatus.FAILED

    def test_1_0_file_claiming_geoarrow_encoding_fails_end_to_end(self, tmp_path):
        """A 1.0 file may not claim a GeoArrow encoding, even with matching data."""
        src = _write_wkb_source(
            tmp_path / "v10_src.parquet", ["POINT (1 2)"], ["Point"], version="1.0.0"
        )
        out = _rewrite_geo_metadata(
            src,
            tmp_path / "v10_geoarrow_claim.parquet",
            lambda geo: geo["columns"]["geometry"].update({"encoding": "point"}),
        )
        result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
        check = _checks_by_name(result)["encoding_valid_geometry"]
        assert check.status == CheckStatus.FAILED
        assert not result.is_valid


class TestWkbBehaviorUnchanged:
    def test_wkb_file_still_validates(self, tmp_path):
        src = _write_wkb_source(tmp_path / "wkb.parquet", ["POINT (1 2)", "POINT (3 4)"], ["Point"])
        result = validate_geoparquet(str(src), validate_data=True, sample_size=0)
        checks = _checks_by_name(result)
        assert checks["encoding_valid_geometry"].status == CheckStatus.PASSED
        assert checks["geometry_byte_array_geometry"].status == CheckStatus.PASSED
        assert checks["encoding_matches_data_geometry"].status == CheckStatus.PASSED
        assert checks["geometry_types_match_data_geometry"].status == CheckStatus.PASSED

    def test_byte_array_column_may_not_claim_a_geoarrow_encoding(self, tmp_path):
        """encoding "point" over a BYTE_ARRAY column contradicts the spec layout."""
        src = _write_wkb_source(tmp_path / "wkb_src.parquet", ["POINT (1 2)"], ["Point"])
        out = _rewrite_geo_metadata(
            src,
            tmp_path / "wkb_claiming_point.parquet",
            lambda geo: geo["columns"]["geometry"].update({"encoding": "point"}),
        )
        result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
        checks = _checks_by_name(result)
        assert checks["geometry_byte_array_geometry"].status == CheckStatus.FAILED
        assert not result.is_valid
        assert _binder_errors(result) == []


class TestGeoArrowDataChecksStillCatchErrors:
    def test_undeclared_geometry_type_is_detected(self, geoarrow_files, tmp_path):
        out = _rewrite_geo_metadata(
            geoarrow_files["polygon"],
            tmp_path / "polygon_wrong_types.parquet",
            lambda geo: geo["columns"]["geometry"].update({"geometry_types": ["Point"]}),
        )
        result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
        check = _checks_by_name(result)["geometry_types_match_data_geometry"]
        assert check.status == CheckStatus.FAILED, check.message

    def test_bbox_not_containing_data_is_detected(self, geoarrow_files, tmp_path):
        out = _rewrite_geo_metadata(
            geoarrow_files["polygon"],
            tmp_path / "polygon_wrong_bbox.parquet",
            lambda geo: geo["columns"]["geometry"].update({"bbox": [10.0, 10.0, 11.0, 11.0]}),
        )
        result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
        check = _checks_by_name(result)["bbox_contains_data_geometry"]
        assert check.status == CheckStatus.FAILED, check.message

    def test_metadata_naming_an_absent_column_reports_cleanly(self, tmp_path):
        """Every GeoArrow data scan must explain itself, never leak a binder error."""
        import pyarrow as pa

        out = tmp_path / "missing_col.parquet"
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "point", "geometry_types": ["Point"]}},
        }
        # The metadata names "geometry"; the file has no such column.
        table = pa.table({"id": [1], "other": [b"\x00"]})
        pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), out)

        result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
        checks = _checks_by_name(result)
        for name in (
            "encoding_matches_data_geometry",
            "geometry_types_match_data_geometry",
            "coordinates_valid_for_crs_geometry",
        ):
            assert checks[name].status == CheckStatus.FAILED, checks[name].message
            assert "does not match GeoArrow encoding" in checks[name].message
        assert _binder_errors(result) == []

    def test_encoding_not_matching_the_stored_layout_is_detected(self, geoarrow_files, tmp_path):
        """A struct-of-points column may not claim the "polygon" nesting."""
        out = _rewrite_geo_metadata(
            geoarrow_files["point"],
            tmp_path / "point_claiming_polygon.parquet",
            lambda geo: geo["columns"]["geometry"].update(
                {"encoding": "polygon", "geometry_types": ["Polygon"]}
            ),
        )
        result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
        check = _checks_by_name(result)["encoding_matches_data_geometry"]
        assert check.status == CheckStatus.FAILED, check.message
        assert "Binder Error" not in check.message


class TestGeoArrowEdgeCases:
    def test_z_dimension_is_reported_in_geometry_types(self, tmp_path):
        out = _write_geoarrow(tmp_path, "pointz", ["POINT Z (1 2 3)"], ["Point Z"])
        result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
        failures = [
            f"{c.name}: {c.message}" for c in result.checks if c.status == CheckStatus.FAILED
        ]
        assert failures == [], failures

    def test_z_dimension_missing_from_declaration_is_detected(self, tmp_path):
        src = _write_geoarrow(tmp_path, "pointz2", ["POINT Z (1 2 3)"], ["Point Z"])
        out = _rewrite_geo_metadata(
            src,
            tmp_path / "pointz_flat_claim.parquet",
            lambda geo: geo["columns"]["geometry"].update({"geometry_types": ["Point"]}),
        )
        result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
        check = _checks_by_name(result)["geometry_types_match_data_geometry"]
        assert check.status == CheckStatus.FAILED, check.message

    def test_empty_and_null_geometries_do_not_break_checks(self, tmp_path):
        out = _write_geoarrow(
            tmp_path,
            "empties",
            ["POLYGON EMPTY", "POLYGON ((0 0, 1 0, 1 1, 0 0))"],
            ["Polygon"],
        )
        result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
        failures = [
            f"{c.name}: {c.message}" for c in result.checks if c.status == CheckStatus.FAILED
        ]
        assert failures == [], failures

    def test_empty_point_nan_coordinates_do_not_break_checks(self, tmp_path):
        out = _write_geoarrow(tmp_path, "emptypoint", ["POINT EMPTY", "POINT (1 2)"], ["Point"])
        result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
        failures = [
            f"{c.name}: {c.message}" for c in result.checks if c.status == CheckStatus.FAILED
        ]
        assert failures == [], failures

    def test_null_geometry_rows_do_not_break_checks(self, tmp_path):
        out = _write_geoarrow(tmp_path, "nullgeom", [None, "POINT (1 2)"], ["Point"])
        result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
        failures = [
            f"{c.name}: {c.message}" for c in result.checks if c.status == CheckStatus.FAILED
        ]
        assert failures == [], failures

    def test_all_empty_geometries_skip_the_coordinate_scans(self, tmp_path):
        """With no extent anywhere, the scans must skip rather than claim a pass."""
        out = _write_geoarrow(tmp_path, "allempty", ["POLYGON EMPTY"], ["Polygon"])
        result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
        checks = _checks_by_name(result)
        assert checks["coordinates_valid_for_crs_geometry"].status == CheckStatus.SKIPPED
        failures = [
            f"{c.name}: {c.message}" for c in result.checks if c.status == CheckStatus.FAILED
        ]
        assert failures == [], failures


class TestDuckDBSchemaPath:
    """The schema checks must survive parquet_schema()'s group nodes.

    get_schema_info() falls back to DuckDB whenever the pyarrow reader bails —
    which happens locally, not just for remote files (an ``srid:``-style CRS
    makes geoarrow-pyarrow raise, see duckdb_metadata._pyarrow_get_schema_info).
    That path reports a nested column as a group node whose "type" is None,
    where a native GeoArrow column has children and no type string at all.
    """

    def test_schema_checks_survive_a_typeless_group_node(self):
        """type=None is present-but-None, so .get("type", "") returns None."""
        schema_info = [{"name": "geometry", "type": None, "num_children": 1}]
        byte_array = _check_geometry_byte_array(schema_info, "geometry", "polygon")
        assert byte_array.status == CheckStatus.PASSED, byte_array.message
        not_grouped = _check_geometry_not_grouped(schema_info, "geometry", "polygon")
        assert not_grouped.status == CheckStatus.PASSED, not_grouped.message

    def test_wkb_column_with_no_type_string_does_not_crash(self):
        schema_info = [{"name": "geometry", "type": None, "num_children": 0}]
        check = _check_geometry_byte_array(schema_info, "geometry", "WKB")
        assert check.status == CheckStatus.FAILED
        assert "BYTE_ARRAY" in check.message

    def test_covering_bbox_field_types_survives_typeless_children(self):
        """Same bug class: a covering.bbox naming a group column, DuckDB path.

        The child rows of a group are themselves groups with type=None, so the
        field-type scan hit the same `.get("type", "")` trap.
        """
        schema_info = [
            {"name": "geometry", "type": None, "num_children": 1},
            {"name": "list", "type": None, "num_children": 1},
            {"name": "element", "type": None, "num_children": 2},
            {"name": "x", "type": "DOUBLE", "num_children": None},
            {"name": "y", "type": "DOUBLE", "num_children": None},
        ]
        col_meta = {
            "covering": {
                "bbox": {
                    "xmin": ["geometry", "xmin"],
                    "ymin": ["geometry", "ymin"],
                    "xmax": ["geometry", "xmax"],
                    "ymax": ["geometry", "ymax"],
                }
            }
        }
        check = _check_covering_bbox_field_types(col_meta, "geometry", schema_info)
        assert check.status == CheckStatus.FAILED, check.message

    def test_covering_bbox_field_types_still_accepts_a_real_bbox_column(self):
        schema_info = [
            {"name": "bbox", "type": None, "num_children": 4},
            {"name": "xmin", "type": "DOUBLE", "num_children": None},
            {"name": "ymin", "type": "DOUBLE", "num_children": None},
            {"name": "xmax", "type": "DOUBLE", "num_children": None},
            {"name": "ymax", "type": "DOUBLE", "num_children": None},
        ]
        col_meta = {
            "covering": {
                "bbox": {
                    "xmin": ["bbox", "xmin"],
                    "ymin": ["bbox", "ymin"],
                    "xmax": ["bbox", "xmax"],
                    "ymax": ["bbox", "ymax"],
                }
            }
        }
        check = _check_covering_bbox_field_types(col_meta, "geometry", schema_info)
        assert check.status == CheckStatus.PASSED, check.message

    @pytest.mark.parametrize("encoding", sorted(GEOARROW_CASES))
    def test_real_duckdb_schema_of_a_geoarrow_file(self, geoarrow_files, encoding):
        """End-to-end over the actual parquet_schema() rows, not a stand-in."""
        from geoparquet_io.core.duckdb_metadata import get_schema_info

        con = get_duckdb_connection()
        try:
            schema_info = get_schema_info(str(geoarrow_files[encoding]), con=con)
        finally:
            con.close()

        # Guard the guard: this must really be the group-node shape.
        geom_rows = [c for c in schema_info if c.get("name") == "geometry"]
        assert geom_rows and geom_rows[0].get("type") is None, geom_rows

        byte_array = _check_geometry_byte_array(schema_info, "geometry", encoding)
        assert byte_array.status == CheckStatus.PASSED, byte_array.message
        not_grouped = _check_geometry_not_grouped(schema_info, "geometry", encoding)
        assert not_grouped.status == CheckStatus.PASSED, not_grouped.message


class TestGeoArrowDimensionSuffix:
    """GeoArrow keeps dimensionality in the coordinate struct, not per value."""

    @pytest.mark.parametrize(
        ("col_type", "expected"),
        [
            ("STRUCT(x DOUBLE, y DOUBLE)", ""),
            ("STRUCT(x DOUBLE, y DOUBLE, z DOUBLE)", " Z"),
            ("STRUCT(x DOUBLE, y DOUBLE, m DOUBLE)", " M"),
            ("STRUCT(x DOUBLE, y DOUBLE, z DOUBLE, m DOUBLE)", " ZM"),
            ("STRUCT(x DOUBLE, y DOUBLE, z DOUBLE)[][]", " Z"),
            ("BLOB", ""),
            ("", ""),
        ],
    )
    def test_suffix_from_column_type(self, col_type, expected):
        assert _geoarrow_zm_suffix(col_type) == expected


# Encodings that share a list depth, and therefore cannot be told apart by
# nesting level alone: swapping the labels within a pair leaves a file whose
# coordinate data is byte-identical to a valid file of the other encoding.
DEPTH_TWIN_SWAPS = [
    ("linestring", "multipoint"),
    ("multipoint", "linestring"),
    ("polygon", "multilinestring"),
    ("multilinestring", "polygon"),
]


class TestGeoArrowDepthTwinsAreDistinguished:
    """A mislabelled encoding must fail even when its list depth is right.

    `_GEOARROW_LIST_DEPTH` cannot separate linestring from multipoint (both one
    list level) or polygon from multilinestring (both two). Without a second
    signal, relabelling a column as its twin produces a file that validates
    clean, because every depth-based check agrees with the lie. The outer list
    field name geoarrow.pyarrow renders (`vertices` / `points` / `rings` /
    `linestrings`) is what actually separates them.
    """

    @pytest.mark.parametrize("real,claimed", DEPTH_TWIN_SWAPS)
    def test_relabelled_twin_is_rejected(self, tmp_path, real, claimed):
        wkts, geometry_types = GEOARROW_CASES[real]
        source = _write_geoarrow(tmp_path, f"{real}_real", wkts, geometry_types)

        _, claimed_types = GEOARROW_CASES[claimed]

        def relabel(geo):
            col = geo["columns"][geo["primary_column"]]
            col["encoding"] = claimed
            col["geometry_types"] = claimed_types

        lying = _rewrite_geo_metadata(source, tmp_path / f"{real}_as_{claimed}.parquet", relabel)

        result = validate_geoparquet(str(lying))
        assert not result.is_valid, (
            f"a {real} column relabelled as {claimed} validated clean; the two share a "
            f"list depth, so depth-based checks alone cannot catch this"
        )

        layout = _checks_by_name(result)["geometry_byte_array_geometry"]
        assert layout.status == CheckStatus.FAILED, layout.message
        # The message must name both sides, so the report says which is wrong.
        assert claimed in layout.message, layout.message
        assert real in layout.message, layout.message

    @pytest.mark.parametrize("encoding", sorted(GEOARROW_CASES))
    def test_honestly_labelled_file_still_passes(self, tmp_path, encoding):
        """The discriminator must not reject correctly labelled files."""
        wkts, geometry_types = GEOARROW_CASES[encoding]
        path = _write_geoarrow(tmp_path, f"{encoding}_honest", wkts, geometry_types)

        result = validate_geoparquet(str(path))
        layout = _checks_by_name(result)["geometry_byte_array_geometry"]
        assert layout.status == CheckStatus.PASSED, layout.message

    def test_unnamed_list_field_is_not_treated_as_a_mismatch(self):
        """A type string without a recognised field name says nothing either way.

        `get_schema_info()` only renders geoarrow's field names on its local
        pyarrow path. The DuckDB path used for remote files reports the group
        node with no type at all, and a plain-Arrow list renders the generic
        `element`. Neither is evidence of a wrong encoding, so neither may fail.
        """
        from geoparquet_io.core.validate import _check_geoarrow_layout, _list_field_names

        assert _list_field_names("LIST<ELEMENT: STRUCT<X: DOUBLE, Y: DOUBLE>>") is None
        assert _list_field_names("STRUCT<X: DOUBLE, Y: DOUBLE>") is None
        assert _list_field_names("LIST<VERTICES: STRUCT<X: DOUBLE, Y: DOUBLE>>") == frozenset(
            {"vertices"}
        )
        # A mix of known and unknown names is still not geoarrow's rendering.
        assert _list_field_names("LIST<VERTICES: LIST<ELEMENT: STRUCT<X: DOUBLE>>>") is None

        generic = "LIST<ELEMENT: STRUCT<X: DOUBLE, Y: DOUBLE>>"
        check = _check_geoarrow_layout(generic, "geometry", "multipoint")
        assert check.status == CheckStatus.PASSED, check.message


class TestPyArrowSchemaPath:
    """The pyarrow fast path renders nesting in the type string, not in rows.

    It emits child rows only for structs, so a list or map column declares
    ``num_children: 0`` (issue #931). "Is this a group?" therefore has to be
    read off the type, or a geometry column stored as a list under a declared
    "WKB" encoding sails through the not-grouped check on local files.
    """

    def test_wkb_geometry_stored_as_a_list_is_grouped(self):
        schema_info = [{"name": "geometry", "type": "list<item: double>", "num_children": 0}]

        check = _check_geometry_not_grouped(schema_info, "geometry", "WKB")

        assert check.status == CheckStatus.FAILED, check.message
        assert "must not be grouped" in check.message

    def test_wkb_geometry_stored_as_a_struct_is_grouped(self):
        schema_info = [
            {"name": "geometry", "type": "struct<x: double, y: double>", "num_children": 2},
            {"name": "x", "type": "double", "num_children": 0},
            {"name": "y", "type": "double", "num_children": 0},
        ]

        check = _check_geometry_not_grouped(schema_info, "geometry", "WKB")

        assert check.status == CheckStatus.FAILED, check.message

    def test_wkb_geometry_stored_as_binary_is_not_grouped(self):
        schema_info = [{"name": "geometry", "type": "binary", "num_children": 0}]

        check = _check_geometry_not_grouped(schema_info, "geometry", "WKB")

        assert check.status == CheckStatus.PASSED, check.message
