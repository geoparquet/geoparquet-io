"""covering.bbox rules the spec states that the checks did not verify: path field names,
single column, child field order, zmin/zmax pairing and types of all children."""

import json

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.common import get_duckdb_connection
from geoparquet_io.core.duckdb_metadata import get_schema_info
from geoparquet_io.core.validate import (
    CheckStatus,
    _check_covering_bbox_column_exists,
    _check_covering_bbox_field_types,
    _check_covering_bbox_paths,
    _check_covering_bbox_structure,
    validate_geoparquet,
)

WKB_POINT = bytes.fromhex("0101000000000000000000f03f0000000000000040")  # POINT (1 2)
PATHS_4 = {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}
F64 = pa.float64()


def _write(path, children, paths=PATHS_4):
    struct = pa.StructArray.from_arrays(
        [pa.array([0, 1], type=t) for _, t in children], names=[n for n, _ in children]
    )
    table = pa.table({"geometry": pa.array([WKB_POINT] * 2, pa.binary()), "bbox": struct})
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "covering": {"bbox": paths},
            }
        },
    }
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), path)
    return str(path)


def _covering_checks(path, con=None):
    col_meta = json.loads(pq.read_metadata(path).metadata[b"geo"])["columns"]["geometry"]
    schema_info = get_schema_info(path, con=con)
    return {
        "paths": _check_covering_bbox_paths(col_meta, "geometry"),
        "exists": _check_covering_bbox_column_exists(col_meta, "geometry", schema_info),
        "structure": _check_covering_bbox_structure(col_meta, "geometry", schema_info),
        "types": _check_covering_bbox_field_types(col_meta, "geometry", schema_info),
    }


XY = [("xmin", F64), ("ymin", F64), ("xmax", F64), ("ymax", F64)]
XYZ = [("xmin", F64), ("ymin", F64), ("zmin", F64), ("xmax", F64), ("ymax", F64), ("zmax", F64)]


class TestPaths:
    def test_second_element_must_be_the_field_name(self, tmp_path):
        paths = dict(PATHS_4, xmin=["bbox", "minx"])
        check = _covering_checks(_write(tmp_path / "f.parquet", XY, paths))["paths"]
        assert check.status == CheckStatus.FAILED
        assert "xmin" in check.message

    def test_all_paths_must_name_one_column(self, tmp_path):
        paths = dict(PATHS_4, ymax=["other", "ymax"])
        check = _covering_checks(_write(tmp_path / "f.parquet", XY, paths))["paths"]
        assert check.status == CheckStatus.FAILED
        assert "other" in check.message


class TestStructure:
    def test_xy_order(self, tmp_path):
        swapped = [XY[1], XY[0], XY[2], XY[3]]
        check = _covering_checks(_write(tmp_path / "f.parquet", swapped))["structure"]
        assert check.status == CheckStatus.FAILED

    def test_xyz_order(self, tmp_path):
        wrong = XY + [("zmin", F64), ("zmax", F64)]
        assert (
            _covering_checks(_write(tmp_path / "bad.parquet", wrong))["structure"].status
            == CheckStatus.FAILED
        )
        assert (
            _covering_checks(_write(tmp_path / "ok.parquet", XYZ))["structure"].status
            == CheckStatus.PASSED
        )

    def test_zmin_requires_zmax(self, tmp_path):
        check = _covering_checks(_write(tmp_path / "f.parquet", XYZ[:5]))["structure"]
        assert check.status == CheckStatus.FAILED

    def test_extra_child_fails(self, tmp_path):
        check = _covering_checks(_write(tmp_path / "f.parquet", XY + [("area", F64)]))["structure"]
        assert check.status == CheckStatus.FAILED


class TestFieldTypes:
    def test_all_children_are_checked(self, tmp_path):
        children = XYZ[:5] + [("zmax", pa.int32())]
        check = _covering_checks(_write(tmp_path / "f.parquet", children))["types"]
        assert check.status == CheckStatus.FAILED
        assert "int" in check.message.lower()

    def test_float_children_pass(self, tmp_path):
        children = [(n, pa.float32()) for n, _ in XYZ]
        assert (
            _covering_checks(_write(tmp_path / "f.parquet", children))["types"].status
            == CheckStatus.PASSED
        )


def _write_with_decoy(path, children=None, bbox_column="bbox"):
    """A file whose first column is a struct carrying a child field named ``bbox``.

    The real root ``bbox`` column comes after it, so a flat schema scan that stops
    at the first name match lands on the nested field instead of the column.
    ``bbox_column=None`` drops the real column, leaving only the nested field.
    """
    children = children or XY
    decoy = pa.StructArray.from_arrays(
        [pa.array(["shadow", "shadow"]), pa.array([0, 1], type=pa.int32())],
        names=["bbox", "other"],
    )
    columns = {"meta": decoy, "geometry": pa.array([WKB_POINT] * 2, pa.binary())}
    if bbox_column is not None:
        columns[bbox_column] = pa.StructArray.from_arrays(
            [pa.array([0, 1], type=t) for _, t in children], names=[n for n, _ in children]
        )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "covering": {"bbox": PATHS_4},
            }
        },
    }
    table = pa.table(columns)
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), path)
    return str(path)


class TestRootPlacement:
    """v1.1.0: "The bounding box column MUST be at the root of the schema"."""

    def _both_schema_paths(self, path):
        con = get_duckdb_connection()
        try:
            return {"pyarrow": _covering_checks(path), "duckdb": _covering_checks(path, con=con)}
        finally:
            con.close()

    def test_nested_bbox_field_does_not_shadow_the_root_column(self, tmp_path):
        path = _write_with_decoy(tmp_path / "shadowed.parquet")
        for source, checks in self._both_schema_paths(path).items():
            assert all(c.status == CheckStatus.PASSED for c in checks.values()), (
                source,
                {k: c.message for k, c in checks.items()},
            )

    def test_nested_bbox_field_does_not_shadow_a_6_field_root_column(self, tmp_path):
        path = _write_with_decoy(tmp_path / "shadowed_xyz.parquet", children=XYZ)
        for source, checks in self._both_schema_paths(path).items():
            assert checks["structure"].status == CheckStatus.PASSED, (
                source,
                checks["structure"].message,
            )

    def test_bbox_column_only_nested_is_not_at_the_root(self, tmp_path):
        path = _write_with_decoy(tmp_path / "nested_only.parquet", bbox_column=None)
        for source, checks in self._both_schema_paths(path).items():
            exists = checks["exists"]
            assert exists.status == CheckStatus.FAILED, (source, exists.message)
            assert "root" in exists.message
            assert checks["structure"].status == CheckStatus.FAILED, source
            assert "found: []" not in checks["structure"].message, source

    def test_missing_bbox_column_says_so(self, tmp_path):
        path = _write(tmp_path / "f.parquet", XY, {k: ["nope", k] for k in PATHS_4})
        checks = _covering_checks(path)
        assert checks["exists"].status == CheckStatus.FAILED
        assert checks["structure"].status == CheckStatus.FAILED
        assert "nope" in checks["structure"].message


class TestRealFiles:
    def test_valid_xy_and_xyz_files_pass_everything(self, tmp_path):
        for name, children in (("xy", XY), ("xyz", XYZ)):
            checks = _covering_checks(_write(tmp_path / f"{name}.parquet", children))
            assert all(c.status == CheckStatus.PASSED for c in checks.values()), {
                k: c.message for k, c in checks.items()
            }

    def test_austria_covering_fixture_still_passes(self, test_data_dir):
        result = validate_geoparquet(
            str(test_data_dir / "austria_bbox_covering.parquet"), validate_data=False
        )
        covering = [c for c in result.checks if c.name.startswith("covering_")]
        assert covering
        assert all(c.status == CheckStatus.PASSED for c in covering), [
            (c.name, c.message) for c in covering
        ]

    def test_full_validation_reports_bad_paths(self, tmp_path):
        path = _write(tmp_path / "f.parquet", XY, dict(PATHS_4, xmin=["bbox", "minx"]))
        result = validate_geoparquet(path, validate_data=False)
        (check,) = [c for c in result.checks if c.name == "covering_bbox_paths_geometry"]
        assert check.status == CheckStatus.FAILED
