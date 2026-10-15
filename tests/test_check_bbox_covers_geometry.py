"""Does the bbox column's arithmetic actually describe the geometry beside it?

``check_bbox_structure`` answers whether a bbox column exists and is declared.
These tests cover the separate question in ``core/check_bbox_values.py``: do
the four numbers on each row cover that row's geometry. The asymmetry is the
point -- an undersized bbox makes a reader's spatial filter prune rows that do
intersect, which is a wrong answer; an oversized one only costs an over-fetch.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import check
from geoparquet_io.core.check_bbox_values import (
    DEFAULT_BBOX_SAMPLE_ROWS,
    check_bbox_covers_geometry,
)
from geoparquet_io.core.check_parquet_structure import check_bbox, check_metadata_and_bbox
from geoparquet_io.core.duckdb_utils import get_duckdb_connection

#: A square whose envelope is exactly (0, 0, 10, 10) in float64.
SQUARE = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"
SQUARE_BBOX = (0.0, 0.0, 10.0, 10.0)

#: A real-world-magnitude square, to exercise tolerances that scale with the
#: coordinate rather than with a fixed epsilon.
SF = "POLYGON ((-122.4 37.7, -122.3 37.7, -122.3 37.8, -122.4 37.8, -122.4 37.7))"
SF_BBOX = (-122.4, 37.7, -122.3, 37.8)


def _wkb_values(wkts):
    """WKB bytes for each WKT, with None passed through as a NULL geometry."""
    con = get_duckdb_connection(load_spatial=True)
    try:
        return [
            None
            if wkt is None
            else con.execute("SELECT ST_AsWKB(ST_GeomFromText(?))", [wkt]).fetchone()[0]
            for wkt in wkts
        ]
    finally:
        con.close()


def _write_geoparquet(
    path,
    wkts,
    bboxes,
    *,
    bbox_type=pa.float64(),
    version="1.1.0",
    geometry=None,
    crs=None,
    edges=None,
    file_bbox=None,
    covering=True,
    use_dictionary=False,
    extra_columns=None,
):
    """A GeoParquet 1.1 file with exactly the bbox values asked for.

    Written straight through pyarrow rather than through gpio's own writers on
    purpose: the point of every test here is a bbox that disagrees with its
    geometry, which no gpio write path will produce.

    ``geometry`` replaces the WKB built from ``wkts`` with raw bytes (corrupt
    values included); a bbox tuple may hold None for a NULL field inside a
    non-NULL struct. ``crs``, ``edges`` and ``file_bbox`` go into the column's
    ``geo`` entry; ``covering=False`` leaves the bbox column undeclared.
    """
    if geometry is None:
        geometry = _wkb_values(wkts)
    geometry = pa.array(geometry, type=pa.binary())
    fields = [
        pa.array([None if box is None else box[i] for box in bboxes], type=bbox_type)
        for i in range(4)
    ]
    bbox = pa.StructArray.from_arrays(
        fields,
        ["xmin", "ymin", "xmax", "ymax"],
        mask=pa.array([box is None for box in bboxes], type=pa.bool_()),
    )
    columns = {"id": pa.array(range(len(geometry))), "geometry": geometry, "bbox": bbox}
    columns.update(extra_columns or {})
    table = pa.table(columns)
    column_meta = {"encoding": "WKB", "geometry_types": []}
    if covering:
        column_meta["covering"] = {
            "bbox": {
                "xmin": ["bbox", "xmin"],
                "ymin": ["bbox", "ymin"],
                "xmax": ["bbox", "xmax"],
                "ymax": ["bbox", "ymax"],
            }
        }
    if crs is not None:
        column_meta["crs"] = crs
    if edges is not None:
        column_meta["edges"] = edges
    if file_bbox is not None:
        column_meta["bbox"] = list(file_bbox)
    geo = {"version": version, "primary_column": "geometry", "columns": {"geometry": column_meta}}
    table = table.replace_schema_metadata({"geo": json.dumps(geo)})
    pq.write_table(table, str(path), use_dictionary=use_dictionary)
    return str(path)


@pytest.fixture
def honest_file(tmp_path):
    """Three rows whose bboxes are exactly their geometries' envelopes."""
    return _write_geoparquet(
        tmp_path / "honest.parquet", [SQUARE, SF, SQUARE], [SQUARE_BBOX, SF_BBOX, SQUARE_BBOX]
    )


class TestHonestFiles:
    """A bbox that covers its geometry is not reported as anything."""

    def test_exact_bboxes_pass(self, honest_file):
        report = check_bbox_covers_geometry(honest_file, quiet=True)
        assert report["passed"] is True
        assert report["checked"] is True
        assert report["checked_rows"] == 3
        assert report["total_rows"] == 3
        assert report["undersized_rows"] == 0
        assert report["oversized_rows"] == 0
        assert report["issues"] == []

    def test_real_file_with_correct_bboxes_passes(self, places_with_covering_file):
        """The canonical 1.1 fixture, written by gpio itself, has honest bboxes."""
        report = check_bbox_covers_geometry(places_with_covering_file, quiet=True)
        assert report["checked"] is True
        assert report["passed"] is True
        assert report["undersized_rows"] == 0


class TestUndersizedIsAFailure:
    """A bbox smaller than its geometry makes a reader drop rows that match."""

    def test_shrunken_bbox_fails(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "shrunk.parquet",
            [SQUARE, SQUARE],
            [SQUARE_BBOX, (0.0, 0.0, 9.0, 9.0)],
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["passed"] is False
        assert report["undersized_rows"] == 1
        assert report["worst_shortfall"] == pytest.approx(1.0)
        assert [example["row"] for example in report["undersized_examples"]] == [1]
        assert any("does not cover" in issue for issue in report["issues"])

    def test_every_axis_is_checked(self, tmp_path):
        """One row short on each of the four bounds; all four are caught."""
        path = _write_geoparquet(
            tmp_path / "axes.parquet",
            [SQUARE] * 4,
            [
                (1.0, 0.0, 10.0, 10.0),  # xmin too high
                (0.0, 1.0, 10.0, 10.0),  # ymin too high
                (0.0, 0.0, 9.0, 10.0),  # xmax too low
                (0.0, 0.0, 10.0, 9.0),  # ymax too low
            ],
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["undersized_rows"] == 4
        assert report["passed"] is False

    def test_examples_name_the_worst_rows_and_are_capped(self, tmp_path):
        """Twelve bad rows, five named, worst first -- not an unbounded dump."""
        shortfalls = [0.1 * (index + 1) for index in range(12)]
        path = _write_geoparquet(
            tmp_path / "many.parquet",
            [SQUARE] * 12,
            [(0.0, 0.0, 10.0 - shortfall, 10.0) for shortfall in shortfalls],
        )
        report = check_bbox_covers_geometry(path, quiet=True, max_examples=5)
        assert report["undersized_rows"] == 12
        assert [example["row"] for example in report["undersized_examples"]] == [11, 10, 9, 8, 7]
        assert report["undersized_examples"][0]["shortfall"] == pytest.approx(1.2)

    def test_null_bbox_beside_a_geometry_fails(self, tmp_path):
        """A NULL bbox prunes the row out of every covering-based filter."""
        path = _write_geoparquet(
            tmp_path / "null_bbox.parquet", [SQUARE, SQUARE], [SQUARE_BBOX, None]
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["passed"] is False
        assert report["null_bbox_rows"] == 1
        assert report["null_bbox_examples"] == [1]
        assert any("NULL bbox" in issue for issue in report["issues"])


class TestOversizedIsOnlyAWarning:
    """A bbox larger than its geometry costs an over-fetch, not a wrong answer."""

    def test_inflated_bbox_is_a_warning_not_a_failure(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "inflated.parquet",
            [SQUARE, SQUARE],
            [SQUARE_BBOX, (-5.0, -5.0, 15.0, 15.0)],
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["severity"] == "warning"
        assert report["oversized_rows"] == 1
        assert report["undersized_rows"] == 0
        assert report["worst_excess"] == pytest.approx(5.0)
        assert [example["row"] for example in report["oversized_examples"]] == [1]
        assert any("larger than" in issue for issue in report["issues"])

    def test_a_row_short_on_one_axis_counts_as_undersized_only(self, tmp_path):
        """Short on x and long on y is a failure, counted once, not two findings."""
        path = _write_geoparquet(tmp_path / "mixed.parquet", [SQUARE], [(0.0, -5.0, 9.0, 15.0)])
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["undersized_rows"] == 1
        assert report["oversized_rows"] == 0
        assert report["passed"] is False


class TestTolerance:
    """Exact equality is the wrong test; the slack is the storage type's own."""

    def test_float32_bbox_of_an_exact_geometry_passes(self, tmp_path):
        """float32 rounds to nearest, so some bounds land *inside* the geometry."""
        path = _write_geoparquet(
            tmp_path / "f32.parquet", [SF, SQUARE], [SF_BBOX, SQUARE_BBOX], bbox_type=pa.float32()
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["passed"] is True
        assert report["undersized_rows"] == 0
        assert report["oversized_rows"] == 0

    def test_float32_tolerance_does_not_hide_a_real_shrink(self, tmp_path):
        """A millidegree is ~70x the float32 slack at this magnitude."""
        path = _write_geoparquet(
            tmp_path / "f32_bad.parquet",
            [SF],
            [(-122.399, 37.7, -122.3, 37.8)],
            bbox_type=pa.float32(),
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["passed"] is False
        assert report["undersized_rows"] == 1

    def test_float64_tolerance_is_tight(self, tmp_path):
        """A float64 bbox gets double-precision slack, not float32's."""
        path = _write_geoparquet(
            tmp_path / "f64_bad.parquet", [SF], [(-122.4 + 1e-9, 37.7, -122.3, 37.8)]
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["passed"] is False
        assert report["undersized_rows"] == 1

    def test_tolerance_scales_with_the_coordinate(self, tmp_path):
        """A projected CRS puts coordinates in the millions; slack scales with them."""
        wkt = "POLYGON ((500000 4000000, 500100 4000000, 500100 4000100, 500000 4000100, 500000 4000000))"
        path = _write_geoparquet(
            tmp_path / "utm.parquet",
            [wkt],
            [(500000.0, 4000000.0, 500100.0, 500100.0 + 3999999.99999999)],
            bbox_type=pa.float32(),
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        # float32 cannot represent 4000100 exactly; rounding is inside the slack.
        assert report["undersized_rows"] == 0


class TestNullAndEmptyGeometry:
    """Rows with nothing to compare are counted, not failed."""

    def test_null_geometry_with_null_bbox_is_skipped(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "null_geom.parquet", [SQUARE, None], [SQUARE_BBOX, None]
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["passed"] is True
        assert report["absent_geometry_rows"] == 1
        assert report["checked_rows"] == 2
        assert report["null_bbox_rows"] == 0

    def test_null_geometry_with_a_bbox_warns_not_fails(self, tmp_path):
        """No envelope to compare against: a bbox there describes nothing, which is advice."""
        path = _write_geoparquet(
            tmp_path / "null_geom_bbox.parquet", [SQUARE, None], [SQUARE_BBOX, SQUARE_BBOX]
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["severity"] == "warning"
        assert report["absent_geometry_rows"] == 1
        assert report["undersized_rows"] == 0

    def test_empty_geometry_is_skipped(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "empty_geom.parquet",
            [SQUARE, "POLYGON EMPTY"],
            [SQUARE_BBOX, None],
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["passed"] is True
        assert report["absent_geometry_rows"] == 1

    def test_empty_geometry_with_a_bbox_warns(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "empty_geom_bbox.parquet",
            [SQUARE, "POLYGON EMPTY"],
            [SQUARE_BBOX, (0.0, 0.0, 0.0, 0.0)],
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["severity"] == "warning"
        assert report["bbox_without_geometry_rows"] == 1


class TestNothingToCheck:
    """The cases where the question does not arise must not regress."""

    def test_file_without_a_bbox_column_is_not_checked(self, buildings_test_file):
        report = check_bbox_covers_geometry(buildings_test_file, quiet=True)
        assert report["checked"] is False
        assert report["passed"] is True
        assert report["bbox_column_name"] is None
        assert "no bbox column" in report["skip_reason"]

    def test_empty_file_is_not_a_failure(self, tmp_path):
        path = _write_geoparquet(tmp_path / "empty.parquet", [], [])
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["passed"] is True
        assert report["total_rows"] == 0
        assert report["checked_rows"] == 0


class TestOddFiles:
    """Files that make the obvious query impossible still get an answer."""

    def test_file_that_owns_the_name_file_row_number(self, tmp_path):
        """DuckDB refuses its own row-number option on such a file; rows are still named."""
        path = tmp_path / "frn.parquet"
        _write_geoparquet(path, [SQUARE, SQUARE], [SQUARE_BBOX, (0.0, 0.0, 9.0, 10.0)])
        table = pq.read_table(path)
        renamed = table.append_column("file_row_number", pa.array([7, 8], type=pa.int64()))
        pq.write_table(renamed.replace_schema_metadata(table.schema.metadata), path)

        report = check_bbox_covers_geometry(str(path), quiet=True)
        assert report["undersized_rows"] == 1
        # Still the file's own row position, not the colliding column's values.
        assert [example["row"] for example in report["undersized_examples"]] == [1]

    def test_geoarrow_encoded_geometry_is_not_compared(self, tmp_path):
        """A nested GeoArrow column is neither WKB nor native GEOMETRY."""
        points = pa.StructArray.from_arrays(
            [pa.array([1.0, 2.0]), pa.array([1.0, 2.0])], ["x", "y"]
        )
        bbox = pa.StructArray.from_arrays(
            [pa.array([1.0, 2.0])] * 4, ["xmin", "ymin", "xmax", "ymax"]
        )
        table = pa.table({"geometry": points, "bbox": bbox})
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "point", "geometry_types": ["Point"]}},
        }
        path = tmp_path / "geoarrow.parquet"
        pq.write_table(table.replace_schema_metadata({"geo": json.dumps(geo)}), path)

        report = check_bbox_covers_geometry(str(path), quiet=True)
        assert report["checked"] is False
        assert report["passed"] is True
        assert "not WKB or native GEOMETRY" in report["skip_reason"]

    def test_bbox_column_with_no_geometry_column(self, tmp_path):
        bbox = pa.StructArray.from_arrays([pa.array([0.0])] * 4, ["xmin", "ymin", "xmax", "ymax"])
        path = tmp_path / "no_geom.parquet"
        pq.write_table(pa.table({"bbox": bbox, "name": pa.array(["a"])}), path)

        report = check_bbox_covers_geometry(str(path), quiet=True)
        assert report["checked"] is False
        assert report["passed"] is True
        assert "no geometry column" in report["skip_reason"]

    def test_verbose_logs_the_query(self, tmp_path, caplog):
        import logging

        path = _write_geoparquet(tmp_path / "verbose.parquet", [SQUARE], [SQUARE_BBOX])
        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            check_bbox_covers_geometry(path, verbose=True, quiet=True)
        assert "Comparing bbox 'bbox' against geometry 'geometry'" in caplog.text


class TestSampling:
    """A 7M-row file is normal, so the default must not decode all of it."""

    def test_default_samples_and_says_so(self, tmp_path):
        rows = DEFAULT_BBOX_SAMPLE_ROWS + 500
        path = _write_geoparquet(tmp_path / "big.parquet", [SQUARE] * rows, [SQUARE_BBOX] * rows)
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["sampled"] is True
        assert report["checked_rows"] == DEFAULT_BBOX_SAMPLE_ROWS
        assert report["total_rows"] == rows

    def test_all_rows_checks_every_row(self, tmp_path):
        rows = DEFAULT_BBOX_SAMPLE_ROWS + 500
        path = _write_geoparquet(tmp_path / "big2.parquet", [SQUARE] * rows, [SQUARE_BBOX] * rows)
        report = check_bbox_covers_geometry(path, all_rows=True, quiet=True)
        assert report["sampled"] is False
        assert report["checked_rows"] == rows

    def test_a_file_smaller_than_the_sample_is_checked_whole(self, honest_file):
        report = check_bbox_covers_geometry(honest_file, sample_rows=1000, quiet=True)
        assert report["sampled"] is False
        assert report["checked_rows"] == 3

    def test_sample_rows_zero_skips_the_scan(self, tmp_path):
        path = _write_geoparquet(tmp_path / "skip.parquet", [SQUARE], [(0.0, 0.0, 1.0, 1.0)])
        report = check_bbox_covers_geometry(path, sample_rows=0, quiet=True)
        assert report["checked"] is False
        assert report["passed"] is True
        assert "not requested" in report["skip_reason"]

    def test_sampling_finds_a_wholesale_corruption(self, tmp_path):
        """Every row shrunk: a sample of any size must see it."""
        rows = DEFAULT_BBOX_SAMPLE_ROWS + 500
        path = _write_geoparquet(
            tmp_path / "all_bad.parquet", [SQUARE] * rows, [(0.0, 0.0, 9.0, 9.0)] * rows
        )
        report = check_bbox_covers_geometry(path, sample_rows=100, quiet=True)
        assert report["passed"] is False
        assert report["undersized_rows"] == 100
        assert report["checked_rows"] == 100


@pytest.fixture
def native_geometry_file(tmp_path, honest_file):
    """A native-Parquet-GEOMETRY file carrying a bbox column that is too small.

    GeoParquet 2.0 and parquet-geo-only files discourage a bbox column, but one
    that is there and wrong is wrong in exactly the way a 1.1 covering is, and
    DuckDB hands the geometry back as ``GEOMETRY`` rather than ``BLOB`` -- the
    other half of :func:`_geometry_expression`.
    """
    con = get_duckdb_connection(load_spatial=True)
    native = tmp_path / "native.parquet"
    try:
        con.execute(
            f"""
            COPY (SELECT id, geometry,
                         {{'xmin': 0.0::DOUBLE, 'ymin': 0.0::DOUBLE,
                           'xmax': 9.0::DOUBLE, 'ymax': 9.0::DOUBLE}} AS bbox
                  FROM '{honest_file}')
            TO '{native.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')
            """
        )
    finally:
        con.close()
    return str(native)


class TestNativeGeometry:
    """A native GEOMETRY column is compared the same way a WKB one is."""

    def test_native_geometry_bbox_is_compared(self, native_geometry_file):
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        (column_type,) = con.execute(
            f"SELECT column_type FROM (DESCRIBE SELECT geometry FROM '{native_geometry_file}')"
        ).fetchone()
        con.close()
        # Non-vacuity: this must exercise the native branch, not the WKB one.
        assert column_type.upper().startswith("GEOMETRY")

        report = check_bbox_covers_geometry(native_geometry_file, quiet=True)
        assert report["checked"] is True
        assert report["passed"] is False
        assert report["undersized_rows"] >= 1


class TestFoldedIntoCheckBbox:
    """``check_bbox`` carries the verdict; ``check_metadata_and_bbox`` stays footer-only."""

    def test_footer_check_never_compares(self, tmp_path):
        path = _write_geoparquet(tmp_path / "quiet.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)])
        result = check_metadata_and_bbox(path, return_results=True, quiet=True)
        assert result["passed"] is True
        assert "bbox_values" not in result

    def test_off_by_default(self, tmp_path):
        path = _write_geoparquet(tmp_path / "off.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)])
        result = check_bbox(path, return_results=True, quiet=True)
        assert result["passed"] is True
        assert result["bbox_values"]["checked"] is False

    def test_verify_rows_flips_the_verdict(self, tmp_path):
        path = _write_geoparquet(tmp_path / "loud.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)])
        result = check_bbox(path, return_results=True, quiet=True, verify_rows=1000)
        assert result["passed"] is False
        assert result["severity"] == "failure"
        assert "failed" not in result
        assert result["bbox_values"]["undersized_rows"] == 1
        assert any("does not cover" in issue for issue in result["issues"])
        assert result["recommendations"][0].startswith("Regenerate the column")
        # The repair is a column rewrite, so --fix must still decline.
        assert result["fix_available"] is False

    def test_printing_without_return_results(self, tmp_path, caplog):
        """The print-only call shape still runs and reports the comparison."""
        import logging

        path = _write_geoparquet(tmp_path / "print.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)])
        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            assert check_bbox(path, verify_rows=1000) is None
        assert "does not cover" in caplog.text

    def test_plain_parquet_still_returns_nothing_to_print_only_callers(self, tmp_path):
        """A file with no geo indicators keeps the old print-and-return-None shape."""
        path = tmp_path / "plain.parquet"
        pq.write_table(pa.table({"name": pa.array(["a"])}), path)
        assert check_metadata_and_bbox(str(path), quiet=True) is None

    def test_oversized_is_a_warning_in_the_combined_result(self, tmp_path):
        path = _write_geoparquet(tmp_path / "wide.parquet", [SQUARE], [(-5.0, -5.0, 15.0, 15.0)])
        result = check_bbox(path, return_results=True, quiet=True, verify_rows=1000)
        assert result["severity"] == "warning"
        assert any("larger than" in issue for issue in result["issues"])

    def test_could_not_run_reaches_the_combined_result(self, tmp_path):
        path = _write_geoparquet(tmp_path / "geomstr2.parquet", [SQUARE], [SQUARE_BBOX])
        table = pq.read_table(path)
        idx = table.schema.get_field_index("geometry")
        pq.write_table(table.set_column(idx, "geometry", pa.array(["POINT (1 1)"])), path)
        result = check_bbox(path, return_results=True, quiet=True, verify_rows=1000)
        assert result["severity"] == "failure"
        assert result["could_not_run"]
        assert result["error"]


class TestExitCodes:
    """The verdict has to be usable as a gate, not only readable.

    Asserting on stdout alone is how a check that printed ``❌`` and exited 0
    shipped, so the exit code is pinned for all three outcomes.
    """

    def _exit_code(self, path, *args):
        return CliRunner().invoke(check, ["bbox", str(path), *args]).exit_code

    def test_clean_file_exits_zero(self, honest_file):
        assert self._exit_code(honest_file) == 0

    def test_oversized_file_exits_zero(self, tmp_path):
        """A warning by design: results stay correct, so it must not fail a build."""
        path = _write_geoparquet(
            tmp_path / "exit_wide.parquet", [SQUARE], [(-5.0, -5.0, 15.0, 15.0)]
        )
        assert self._exit_code(path) == 0

    def test_undersized_file_exits_one(self, tmp_path):
        path = _write_geoparquet(tmp_path / "exit_short.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)])
        assert self._exit_code(path) == 1

    def test_null_bbox_exits_one(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "exit_null.parquet", [SQUARE, SQUARE], [SQUARE_BBOX, None]
        )
        assert self._exit_code(path) == 1

    def test_skipping_the_comparison_exits_zero(self, tmp_path):
        """``--sample-rows 0`` asked no question, so it cannot answer "failed"."""
        path = _write_geoparquet(tmp_path / "exit_skip.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)])
        assert self._exit_code(path, "--sample-rows", "0") == 0

    def test_file_with_no_bbox_column_keeps_its_old_exit_code(self, buildings_test_file):
        """Pre-existing verdicts are not turned into gates by this change."""
        assert self._exit_code(buildings_test_file) == 0

    def test_partition_exits_one_and_names_the_bad_file(self, tmp_path):
        """A multi-file run gates on any file, and says which."""
        part = tmp_path / "part"
        part.mkdir()
        _write_geoparquet(part / "good.parquet", [SQUARE], [SQUARE_BBOX])
        _write_geoparquet(part / "bad.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)])
        result = CliRunner().invoke(check, ["bbox", str(part), "--all-files"])
        assert result.exit_code == 1
        assert "bad.parquet" in result.output


class TestCheckBboxCli:
    """``gpio check bbox`` reports the comparison by default."""

    def test_good_file_reports_the_comparison(self, honest_file):
        result = CliRunner().invoke(check, ["bbox", honest_file])
        assert result.exit_code == 0
        assert "3 of 3 rows" in result.output
        assert "covers its geometry" in result.output

    def test_bad_file_names_the_rows(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "cli_bad.parquet",
            [SQUARE, SQUARE, SQUARE],
            [SQUARE_BBOX, (0.0, 0.0, 9.0, 10.0), SQUARE_BBOX],
        )
        result = CliRunner().invoke(check, ["bbox", path])
        assert result.exit_code == 1
        assert "does not cover" in result.output
        assert "row 1" in result.output
        assert "gpio add bbox" in result.output

    def test_oversized_file_warns_without_failing(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "cli_wide.parquet", [SQUARE], [(-5.0, -5.0, 15.0, 15.0)]
        )
        result = CliRunner().invoke(check, ["bbox", path])
        assert result.exit_code == 0
        assert "larger than their geometry" in result.output
        assert "over by 5" in result.output
        assert "does not cover" not in result.output

    def test_null_bbox_is_named(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "cli_null.parquet", [SQUARE, SQUARE], [SQUARE_BBOX, None]
        )
        result = CliRunner().invoke(check, ["bbox", path])
        assert result.exit_code == 1
        assert "NULL bbox" in result.output
        assert "row 1" in result.output

    def test_all_rows_flag(self, tmp_path):
        rows = DEFAULT_BBOX_SAMPLE_ROWS + 10
        path = _write_geoparquet(
            tmp_path / "cli_all.parquet", [SQUARE] * rows, [SQUARE_BBOX] * rows
        )
        result = CliRunner().invoke(check, ["bbox", path, "--all-rows"])
        assert result.exit_code == 0
        assert f"{rows:,} of {rows:,} rows" in result.output

    def test_sample_rows_flag(self, tmp_path):
        rows = 200
        path = _write_geoparquet(tmp_path / "cli_s.parquet", [SQUARE] * rows, [SQUARE_BBOX] * rows)
        result = CliRunner().invoke(check, ["bbox", path, "--sample-rows", "50"])
        assert result.exit_code == 0
        assert "50 of 200 rows" in result.output

    def test_sample_rows_zero_skips(self, tmp_path):
        path = _write_geoparquet(tmp_path / "cli_zero.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)])
        result = CliRunner().invoke(check, ["bbox", path, "--sample-rows", "0"])
        assert result.exit_code == 0
        assert "does not cover" not in result.output

    def test_all_rows_and_sample_rows_conflict(self, honest_file):
        result = CliRunner().invoke(
            check, ["bbox", honest_file, "--all-rows", "--sample-rows", "5"]
        )
        assert result.exit_code == 2
        assert "--all-rows" in result.output

    def test_file_without_bbox_column_still_reports_the_old_verdict(self, buildings_test_file):
        result = CliRunner().invoke(check, ["bbox", buildings_test_file])
        assert result.exit_code == 0
        assert "No bbox column found" in result.output

    def test_fix_declines_a_wrong_bbox_with_a_hint(self, tmp_path):
        path = _write_geoparquet(tmp_path / "cli_fix.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)])
        result = CliRunner().invoke(check, ["bbox", path, "--fix"])
        # --fix cannot repair it, so the file is still broken when the run ends.
        assert result.exit_code == 1
        assert "No automatic fix available" in result.output
        assert "gpio add bbox" in result.output


class TestPythonApi:
    """Both new CLI options have a twin on ``Table.check_bbox``.

    ``Table`` is the whole Python door for the ``check`` group: `ops` is
    table-in/table-out and a check returns a report, which is why
    ``tests/test_api.py`` pins every ``check_*`` method as ``Table``-only.
    """

    def test_table_check_bbox_detects_a_wrong_bbox(self, tmp_path):
        import geoparquet_io as gpio

        path = _write_geoparquet(tmp_path / "api.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)])
        result = gpio.read(path).check_bbox()
        assert result.passed() is False
        assert any("does not cover" in failure for failure in result.failures())

    def test_table_check_bbox_all_rows(self, tmp_path):
        import geoparquet_io as gpio

        rows = DEFAULT_BBOX_SAMPLE_ROWS + 10
        path = _write_geoparquet(
            tmp_path / "api_all.parquet", [SQUARE] * rows, [SQUARE_BBOX] * rows
        )
        result = gpio.read(path).check_bbox(all_rows=True)
        assert result.passed() is True
        assert result.to_dict()["bbox_values"]["checked_rows"] == rows

    def test_table_check_bbox_can_skip_the_scan(self, tmp_path):
        import geoparquet_io as gpio

        path = _write_geoparquet(tmp_path / "api_skip.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)])
        result = gpio.read(path).check_bbox(sample_rows=0)
        assert result.passed() is True


# --------------------------------------------------------------------------
# Review findings on #1239. Each class pins one behaviour the first version
# got wrong; the fixtures are the reviewers' repros, shrunk.
# --------------------------------------------------------------------------

#: A point's WKB header with a truncated body: parses as nothing.
BAD_WKB = b"\x01\x01\x00\x00\x00garbage"

#: A projected CRS, as far as ``is_geographic_crs`` reads one.
UTM_31N = {
    "type": "ProjectedCRS",
    "name": "WGS 84 / UTM zone 31N",
    "id": {"authority": "EPSG", "code": 32631},
}

#: Fiji, cut at the antimeridian as RFC 7946 tells producers to.
FIJI = "MULTIPOLYGON (((177 -18, 180 -18, 180 -16, 177 -16, 177 -18)), ((-180 -18, -178 -18, -178 -16, -180 -16, -180 -18)))"
FIJI_BBOX = (177.0, -18.0, -178.0, -16.0)


def _labels(report):
    """``{issue: label}`` for every issue, through the shared verdict reader."""
    from geoparquet_io.core.check_verdict import labelled_issues

    return {issue: level.label for level, issue in labelled_issues(report)}


class TestUnparsableGeometry:
    """A geometry value that is not WKB is a finding, never a crash or a pass (P1-1)."""

    def test_one_bad_row_is_counted_and_the_rest_still_compared(self, tmp_path):
        good = _wkb_values([SQUARE])[0]
        path = _write_geoparquet(
            tmp_path / "bad_wkb.parquet",
            None,
            [(0.0, 0.0, 9.0, 9.0)] * 3,
            geometry=[good, BAD_WKB, good],
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["unparsable_geometry_rows"] == 1
        assert report["undersized_rows"] == 2
        assert report["absent_geometry_rows"] == 0
        assert report["passed"] is False
        assert report["severity"] == "failure"
        assert any("not valid WKB" in issue for issue in report["issues"])

    def test_unparsable_alone_fails(self, tmp_path):
        good = _wkb_values([SQUARE])[0]
        path = _write_geoparquet(
            tmp_path / "bad_only.parquet",
            None,
            [SQUARE_BBOX, (0.0, 0.0, 1.0, 1.0)],
            geometry=[good, BAD_WKB],
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["unparsable_geometry_rows"] == 1
        assert report["undersized_rows"] == 0
        assert report["severity"] == "failure"

    def test_zero_length_value_is_unparsable(self, tmp_path):
        good = _wkb_values([SQUARE])[0]
        path = _write_geoparquet(
            tmp_path / "zero.parquet", None, [SQUARE_BBOX] * 2, geometry=[good, b""]
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["unparsable_geometry_rows"] == 1
        assert report["severity"] == "failure"

    def test_dictionary_encoded_bad_row_does_not_null_the_rest(self, tmp_path):
        """pyarrow dictionary-encodes repeated WKB; one bad entry must not hide 999 rows."""
        good = _wkb_values([SQUARE])[0]
        path = _write_geoparquet(
            tmp_path / "dict.parquet",
            None,
            [(1.0, 1.0, 9.0, 9.0)] * 1000,
            geometry=[good] * 999 + [BAD_WKB],
            use_dictionary=True,
        )
        # Non-vacuity: the geometry column really is dictionary-encoded.
        encodings = pq.ParquetFile(path).metadata.row_group(0).column(1).encodings
        assert any("DICTIONARY" in str(e) for e in encodings)

        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["unparsable_geometry_rows"] == 1
        assert report["undersized_rows"] == 999
        assert report["passed"] is False

    def test_cli_reports_instead_of_crashing(self, tmp_path):
        good = _wkb_values([SQUARE])[0]
        path = _write_geoparquet(
            tmp_path / "cli_bad_wkb.parquet", None, [SQUARE_BBOX] * 2, geometry=[good, BAD_WKB]
        )
        result = CliRunner().invoke(check, ["bbox", path])
        assert result.exit_code == 1
        assert "not valid WKB" in result.output
        assert "Invalid Input Error" not in result.output

    def test_table_check_bbox_reports_instead_of_crashing(self, tmp_path):
        import geoparquet_io as gpio

        good = _wkb_values([SQUARE])[0]
        path = _write_geoparquet(
            tmp_path / "api_bad_wkb.parquet", None, [SQUARE_BBOX] * 2, geometry=[good, BAD_WKB]
        )
        result = gpio.read(path).check_bbox()
        assert result.verdict() == "failure"
        assert any("not valid WKB" in f for f in result.failures())


class TestAntimeridian:
    """xmin > xmax on a geographic CRS crosses the antimeridian (RFC 7946 5.2) (P1-2)."""

    def test_fiji_covers(self, tmp_path):
        path = _write_geoparquet(tmp_path / "fiji.parquet", [FIJI], [FIJI_BBOX])
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["undersized_rows"] == 0
        assert report["oversized_rows"] == 0
        assert report["passed"] is True

    def test_line_across_the_antimeridian_covers(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "line.parquet", ["LINESTRING (179 0, -179 1)"], [(179.0, 0.0, -179.0, 1.0)]
        )
        assert check_bbox_covers_geometry(path, quiet=True)["passed"] is True

    def test_a_vertex_outside_the_wrapped_range_is_undersized(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "short.parquet", ["LINESTRING (179 0, -170 1)"], [(179.0, 0.0, -179.0, 1.0)]
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["undersized_rows"] == 1
        assert report["worst_shortfall"] == pytest.approx(9.0)

    def test_a_wrapped_bbox_wider_than_needed_is_oversized(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "wide.parquet", ["LINESTRING (179 0, -179 1)"], [(170.0, 0.0, -170.0, 1.0)]
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["undersized_rows"] == 0
        assert report["oversized_rows"] == 1
        assert report["worst_excess"] == pytest.approx(9.0)

    def test_projected_crs_does_not_wrap(self, tmp_path):
        """For a projected CRS xmin > xmax is broken, as ``check spec`` reads it."""
        path = _write_geoparquet(
            tmp_path / "utm.parquet",
            ["LINESTRING (179 0, -179 1)"],
            [(179.0, 0.0, -179.0, 1.0)],
            crs=UTM_31N,
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["undersized_rows"] == 1

    def test_a_reversed_bbox_outside_longitude_range_is_not_a_wrap(self, tmp_path):
        """Only a longitude can wrap; x = 20000 with xmin > xmax is just wrong."""
        path = _write_geoparquet(
            tmp_path / "far.parquet", ["POINT (20000 1)"], [(20000.5, 1.0, 20000.0, 1.0)]
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["undersized_rows"] == 1
        assert report["wrapped_rows"] == 0

    def test_agrees_with_check_spec(self, tmp_path):
        """The same wrapped extent passes spec's bbox check and this one alike."""
        from geoparquet_io.core.validate import validate_geoparquet

        path = _write_geoparquet(
            tmp_path / "agree.parquet", [FIJI], [FIJI_BBOX], file_bbox=FIJI_BBOX
        )
        spec = validate_geoparquet(path, validate_data=True, verbose=False)
        contains = [c for c in spec.checks if c.name.startswith("bbox_contains_data")]
        assert [c.status.value for c in contains] == ["passed"]
        assert check_bbox_covers_geometry(path, quiet=True)["passed"] is True


class TestNullAndNanFields:
    """A bbox no filter can match is a NULL bbox, however it got that way (P1-3, P3-10)."""

    @pytest.mark.parametrize(
        "box",
        [(None, 10.0, 10.0, 10.0), (10.0, 10.0, 10.0, None), (None, None, None, None)],
        ids=["xmin", "ymax", "all-four"],
    )
    def test_a_null_field_is_a_null_bbox(self, tmp_path, box):
        path = _write_geoparquet(tmp_path / "nf.parquet", ["POINT (10 10)"], [box])
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["null_bbox_rows"] == 1
        assert report["passed"] is False
        assert report["severity"] == "failure"

    def test_nan_is_named_as_nan(self, tmp_path):
        nan = float("nan")
        path = _write_geoparquet(
            tmp_path / "nan.parquet", ["POINT (1 1)", "POINT (1 1)"], [(nan,) * 4, (1.0,) * 4]
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["nan_bbox_rows"] == 1
        assert report["undersized_rows"] == 0
        assert report["severity"] == "failure"
        assert any("bbox contains NaN" in issue for issue in report["issues"])
        assert not any("nan)" in issue for issue in report["issues"])


class TestSeverity:
    """Undersized fails; oversized warns -- and each issue carries its own level (#1252)."""

    def test_oversized_is_a_warning_not_a_pass(self, tmp_path):
        path = _write_geoparquet(tmp_path / "wide.parquet", [SQUARE], [(-5.0, -5.0, 15.0, 15.0)])
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["passed"] is False
        assert report["severity"] == "warning"

    def test_mixed_findings_keep_their_own_levels(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "mixed.parquet",
            [SQUARE, SQUARE],
            [(0.0, 0.0, 9.0, 9.0), (-5.0, -5.0, 15.0, 15.0)],
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["severity"] == "failure"
        labels = _labels(report)
        assert {label for issue, label in labels.items() if "larger than" in issue} == {"warning"}
        assert {label for issue, label in labels.items() if "does not cover" in issue} == {
            "failure"
        }

    def test_cli_prints_each_issue_at_its_own_level(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "cli_mixed.parquet",
            [SQUARE, SQUARE],
            [(0.0, 0.0, 9.0, 9.0), (-5.0, -5.0, 15.0, 15.0)],
        )
        result = CliRunner().invoke(check, ["bbox", path])
        assert result.exit_code == 1
        lines = result.output.splitlines()
        assert any("WARNING" in line and "larger than" in line for line in lines)
        assert any("ERROR" in line and "does not cover" in line for line in lines)

    def test_strict_fails_an_oversized_bbox(self, tmp_path):
        path = _write_geoparquet(tmp_path / "strict.parquet", [SQUARE], [(-5.0, -5.0, 15.0, 15.0)])
        assert CliRunner().invoke(check, ["bbox", path]).exit_code == 0
        assert CliRunner().invoke(check, ["bbox", path, "--strict"]).exit_code == 1

    def test_glyphs_are_the_shared_ones(self, tmp_path):
        from geoparquet_io.core.check_verdict import FAILURE_GLYPH

        path = _write_geoparquet(tmp_path / "glyph.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)])
        output = CliRunner().invoke(check, ["bbox", path]).output
        assert "❌" not in output
        assert "⚠️" not in output.split("Bbox vs geometry")[1]
        assert FAILURE_GLYPH in output.split("Bbox vs geometry")[1]

    def test_check_all_does_not_compare_values(self, tmp_path):
        """``check all`` stays a metadata pass: no geometry is decoded for the bbox."""
        from geoparquet_io.core.check_parquet_structure import check_all as core_check_all

        path = _write_geoparquet(tmp_path / "all.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)])
        results = core_check_all(path, return_results=True, quiet=True)
        assert "bbox_values" not in results["bbox"]
        assert "Bbox vs geometry" not in CliRunner().invoke(check, ["all", path]).output


class TestFixCarriesTheValueVerdict:
    """``--fix`` repairs metadata, not numbers, so a wrong bbox still fails after it."""

    def test_metadata_only_fix_still_exits_one(self, tmp_path):
        """The fix adds the missing covering; the numbers it declares stay wrong."""
        path = _write_geoparquet(
            tmp_path / "undeclared.parquet", [SQUARE], [(0.0, 0.0, 9.0, 9.0)], covering=False
        )
        before = check_bbox(path, return_results=True, quiet=True, verify_rows=100)
        # Non-vacuity: --fix has something to apply (the missing covering).
        assert before["fix_available"] is True
        assert before["needs_bbox_metadata"] is True
        out = tmp_path / "fixed.parquet"
        result = CliRunner().invoke(check, ["bbox", path, "--fix", "--fix-output", str(out)])
        assert "Added bbox covering metadata" in result.output
        after = check_bbox(str(out), return_results=True, quiet=True, verify_rows=100)
        assert after["needs_bbox_metadata"] is False
        assert after["bbox_values"]["undersized_rows"] == 1
        assert result.exit_code == 1
        # Say what the fix did, and that the file still fails -- not "success".
        assert "optimized successfully" not in result.output
        assert "Added the bbox covering metadata, but the file still fails" in result.output
        assert any(
            line.startswith("✗") and "does not cover its geometry" in line
            for line in result.output.splitlines()
        )
        assert "gpio add bbox" in result.output.split("still fails")[1]

    def test_a_fix_that_leaves_the_file_passing_still_says_success(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "undeclared_ok.parquet", [SQUARE], [SQUARE_BBOX], covering=False
        )
        out = tmp_path / "fixed_ok.parquet"
        result = CliRunner().invoke(check, ["bbox", path, "--fix", "--fix-output", str(out)])
        assert result.exit_code == 0
        assert "Bbox optimized successfully!" in result.output
        assert "still fails" not in result.output


class TestUnusableTypes:
    """A column the comparison cannot read is reported, never a traceback (P2-4)."""

    def test_varchar_bbox_fields_are_not_compared(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "strings.parquet", [SQUARE], [("0", "0", "10", "10")], bbox_type=pa.string()
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["checked"] is False
        assert "FLOAT or DOUBLE" in report["skip_reason"]
        result = CliRunner().invoke(check, ["bbox", path])
        assert "Binder Error" not in result.output
        assert result.exit_code == 1  # the structure check already fails it

    def test_varchar_geometry_could_not_run(self, tmp_path):
        path = _write_geoparquet(tmp_path / "geomstr.parquet", [SQUARE], [SQUARE_BBOX])
        table = pq.read_table(path)
        idx = table.schema.get_field_index("geometry")
        table = table.set_column(idx, "geometry", pa.array(["POINT (1 1)"]))
        pq.write_table(table, path)
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["severity"] == "failure"
        assert report["could_not_run"]
        result = CliRunner().invoke(check, ["bbox", path])
        assert result.exit_code == 1
        assert "INTERNAL Error" not in result.output
        assert "Traceback" not in result.output

    def test_a_duckdb_error_is_a_could_not_run_failure(self, tmp_path, monkeypatch):
        import duckdb

        from geoparquet_io.core import check_bbox_values

        def boom(*args, **kwargs):
            raise duckdb.IOException("disk on fire")

        monkeypatch.setattr(check_bbox_values, "_comparison_query", boom)
        path = _write_geoparquet(tmp_path / "boom.parquet", [SQUARE], [SQUARE_BBOX])
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["severity"] == "failure"
        assert "disk on fire" in report["could_not_run"][0]

    def test_multi_file_crash_is_not_called_a_failed_fix(self, tmp_path):
        part = tmp_path / "part"
        part.mkdir()
        _write_geoparquet(part / "good.parquet", [SQUARE], [SQUARE_BBOX])
        bad = _write_geoparquet(part / "bad.parquet", [SQUARE], [SQUARE_BBOX])
        table = pq.read_table(bad)
        idx = table.schema.get_field_index("geometry")
        pq.write_table(table.set_column(idx, "geometry", pa.array(["POINT (1 1)"])), bad)
        result = CliRunner().invoke(check, ["bbox", str(part), "--all-files"])
        assert result.exit_code == 1
        assert "Failed to fix" not in result.output
        assert "bad.parquet" in result.output


class TestRowNumbersAndSampling:
    """Rows are named by their place in the file, and a sample is a sample (P2-5, P2-7)."""

    def test_sampled_rows_keep_their_file_position_when_the_file_owns_the_name(self, tmp_path):
        """Row r's bbox is short by r/1000, so a wrongly numbered example cannot match."""
        rows = 3000
        points = [f"POINT ({i} {i})" for i in range(rows)]
        boxes = [(float(i), i + i / 1000, float(i), float(i)) for i in range(rows)]
        path = _write_geoparquet(
            tmp_path / "frn.parquet",
            points,
            boxes,
            extra_columns={"file_row_number": pa.array(range(rows, 2 * rows))},
        )
        report = check_bbox_covers_geometry(path, sample_rows=50, quiet=True)
        assert report["sampled"] is True
        assert report["undersized_examples"]
        for example in report["undersized_examples"]:
            assert example["shortfall"] == pytest.approx(example["row"] / 1000)

    def test_a_corrupt_tail_is_found_by_the_sample(self, tmp_path):
        """A LIMIT reads the head; a sample across the file must see the tail."""
        rows = 6000
        boxes = [SQUARE_BBOX] * (rows - 1000) + [(0.0, 0.0, 9.0, 9.0)] * 1000
        path = _write_geoparquet(tmp_path / "tail.parquet", [SQUARE] * rows, boxes)
        report = check_bbox_covers_geometry(path, sample_rows=500, quiet=True)
        assert report["sampled"] is True
        assert report["undersized_rows"] > 0
        assert min(e["row"] for e in report["undersized_examples"]) >= rows - 1000

    def test_the_sample_is_repeatable(self, tmp_path):
        rows = 6000
        boxes = [SQUARE_BBOX if i % 7 else (0.0, 0.0, 9.0, 9.0) for i in range(rows)]
        path = _write_geoparquet(tmp_path / "rep.parquet", [SQUARE] * rows, boxes)
        first = check_bbox_covers_geometry(path, sample_rows=300, quiet=True)
        second = check_bbox_covers_geometry(path, sample_rows=300, quiet=True)
        assert first["undersized_rows"] == second["undersized_rows"]
        assert first["undersized_examples"] == second["undersized_examples"]


class TestWhatTheOutputSays:
    """A skipped comparison, spherical edges and an orphan bbox are all said (P3-9/12/13)."""

    def test_skip_reason_is_printed(self, tmp_path):
        path = _write_geoparquet(tmp_path / "skip.parquet", [SQUARE], [SQUARE_BBOX])
        result = CliRunner().invoke(check, ["bbox", path, "--sample-rows", "0"])
        assert "not compared" in result.output
        assert "not requested" in result.output

    def test_spherical_edges_are_noted(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "sph.parquet",
            ["LINESTRING (-60 60, 60 60)"],
            [(-60.0, 60.0, 60.0, 60.0)],
            edges="spherical",
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["edges"] == "spherical"
        result = CliRunner().invoke(check, ["bbox", path])
        assert "spherical" in result.output

    def test_a_bbox_beside_no_geometry_warns(self, tmp_path):
        path = _write_geoparquet(
            tmp_path / "orphan.parquet", [None, SQUARE], [SQUARE_BBOX, SQUARE_BBOX]
        )
        report = check_bbox_covers_geometry(path, quiet=True)
        assert report["bbox_without_geometry_rows"] == 1
        assert report["severity"] == "warning"


class TestFooterReads:
    """``check_bbox`` hands what it already read to the comparison (P3-14)."""

    def test_bbox_structure_is_read_once(self, tmp_path, monkeypatch):
        from geoparquet_io.core import bbox_structure, check_bbox_values, check_parquet_structure

        calls = []
        real = bbox_structure.check_bbox_structure

        def counting(*args, **kwargs):
            calls.append(args)
            return real(*args, **kwargs)

        monkeypatch.setattr(check_parquet_structure, "check_bbox_structure", counting)
        monkeypatch.setattr(check_bbox_values, "check_bbox_structure", counting, raising=False)
        path = _write_geoparquet(tmp_path / "once.parquet", [SQUARE], [SQUARE_BBOX])
        result = check_bbox(path, return_results=True, quiet=True, verify_rows=100)
        assert result["bbox_values"]["checked"] is True
        assert len(calls) == 1
