"""Every ``gpio check --fix`` path, run on a broken file, judged on its output.

WP-1 of #1018. Every test here runs a fix on an input that **actually has the
defect** -- a fix that declines to run proves nothing -- then asks the fix's own
check whether the defect is gone, and hands the output to
:func:`tests.fix_output_oracle.assert_fix_output_is_sound`.

The four shapes in :data:`SHAPES` are the four a fix has to answer the version
and CRS questions about differently, and two of them are deliberately not in
the default CRS: a file already in CRS84 cannot tell a path that keeps the CRS
from one that loses it, because an absent ``crs`` already means CRS84 (#993).
"""

from __future__ import annotations

import importlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests.fix_output_oracle import (
    CRS84,
    NO_GEO_BLOCK,
    PLACES_ROWS,
    assert_bbox_column_matches_geometry,
    assert_fix_output_is_sound,
    covering_of,
    spec_failures,
)


def run_cli(*args: object) -> str:
    """Invoke ``gpio`` and insist it succeeded."""
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output
    return result.output


@dataclass(frozen=True)
class Shape:
    """One input shape, and what a ``--fix`` output built from it must look like.

    The two ``*_version``/``*_covering`` pairs differ on purpose: the rewrite
    fixes (compression, row-group, spatial) let the write facade pick the output
    version from the input, while ``check all`` names it explicitly from its own
    checks (``get_geoparquet_version_from_check_results``). Where the two
    disagree that is a fact about gpio, so it is written down rather than
    papered over.
    """

    name: str
    fixture: str
    rows: int
    crs: object
    id_column: str
    bbox_column: str | None
    rewrite_version: str
    rewrite_covering: bool
    check_all_version: str
    check_all_covering: bool
    #: The one complaint ``check all --fix`` legitimately leaves behind, if any.
    check_all_residual: str | None = None


SHAPES = [
    # 1.0 WKB, CRS84, with a bbox column and (by 1.0 rules) no covering.
    Shape(
        name="v1_0_crs84",
        fixture="places_test_file",
        rows=PLACES_ROWS,
        crs=CRS84,
        id_column="fsq_place_id",
        bbox_column="bbox",
        rewrite_version="1.1",
        rewrite_covering=True,
        check_all_version="1.0",
        check_all_covering=False,
        # `check all --fix` keeps the version the user handed it -- a fix that
        # quietly upgrades a file is a defect of the same family as one that
        # loses its CRS -- so the "outdated" advisory survives its own repair.
        check_all_residual="GeoParquet version 1.0.0 is outdated",
    ),
    # 1.1 WKB, a projected CRS, a non-default bbox column name, with a covering.
    Shape(
        name="v1_1_epsg31287",
        fixture="austria_bbox_covering_file",
        rows=30,
        crs={"authority": "EPSG", "code": 31287},
        id_column="id",
        bbox_column="geometry_bbox",
        rewrite_version="1.1",
        rewrite_covering=True,
        check_all_version="1.1",
        check_all_covering=True,
    ),
    # 2.0 native, CRS84, no bbox column (the native stats replace it).
    Shape(
        name="v2_0_crs84",
        fixture="fields_v2_file",
        rows=100,
        crs=CRS84,
        id_column="id",
        bbox_column=None,
        rewrite_version="2.0",
        rewrite_covering=False,
        check_all_version="2.0",
        check_all_covering=False,
    ),
    # Native geo types, EPSG:5070, no `geo` key at all -- the CRS lives in one
    # place only, so a rewrite that forgets the witness loses it (#1001).
    Shape(
        name="pgo_epsg5070",
        fixture="projected_conus",
        rows=200,
        crs={"authority": "EPSG", "code": 5070},
        id_column="id",
        bbox_column=None,
        rewrite_version="2.0",
        rewrite_covering=False,
        check_all_version=NO_GEO_BLOCK,
        check_all_covering=False,
    ),
]
SHAPE_BY_NAME = {spec.name: spec for spec in SHAPES}


@pytest.fixture(params=SHAPES, ids=lambda shape: shape.name)
def shape(request) -> tuple[Shape, Path]:
    """One :class:`Shape`, with ``source`` pointing at a clean file of that shape."""
    spec = request.param
    source = request.getfixturevalue(spec.fixture)
    return spec, Path(str(source))


# ---------------------------------------------------------------------------
# Defect injection. A fix only runs on a file it finds something wrong with, so
# every input below is deliberately broken in the one way the fix repairs.
# ---------------------------------------------------------------------------


def _read(source: Path) -> pa.Table:
    """Read preserving native geo types, so a rewrite does not flatten them to binary."""
    import geoarrow.pyarrow  # noqa: F401  (registers the extension types)

    return pq.read_table(str(source))


def make_snappy(source: Path, target: Path) -> Path:
    """SNAPPY instead of ZSTD: ``check compression --fix`` has work."""
    pq.write_table(_read(source), str(target), compression="SNAPPY")
    return target


def make_tiny_row_groups(source: Path, target: Path, rows_per_group: int = 5) -> Path:
    """Groups far below the band: ``check row-group --fix`` has work."""
    pq.write_table(_read(source), str(target), compression="ZSTD", row_group_size=rows_per_group)
    return target


def make_unsorted(source: Path, target: Path, compression: str = "ZSTD") -> Path:
    """Shuffled into tiny row groups: ``check spatial --fix`` has work.

    Both halves matter. A single-row-group file has no pairs of group bounding
    boxes to compare, so the spatial check calls it well ordered whatever the
    rows contain (#940).
    """
    table = _read(source)
    order = np.random.RandomState(0).permutation(table.num_rows)
    pq.write_table(
        table.take(pa.array(order)), str(target), compression=compression, row_group_size=10
    )
    return target


def make_without_bbox(source: Path, target: Path, bbox_column: str) -> Path:
    """Bbox column and its covering both gone: ``check bbox --fix`` has work."""
    table = _read(source).drop([bbox_column])
    metadata = dict(table.schema.metadata or {})
    if b"geo" in metadata:
        block = json.loads(metadata[b"geo"])
        for column in (block.get("columns") or {}).values():
            column.pop("covering", None)
        metadata[b"geo"] = json.dumps(block).encode("utf-8")
        table = table.replace_schema_metadata(metadata)
    pq.write_table(table, str(target), compression="ZSTD")
    return target


def make_without_covering(source: Path, target: Path, geometry_column: str = "geometry") -> Path:
    """Bbox column kept, its covering dropped: the ``fix_bbox_metadata`` path."""
    table = _read(source)
    metadata = dict(table.schema.metadata or {})
    block = json.loads(metadata[b"geo"])
    block["columns"][geometry_column].pop("covering", None)
    metadata[b"geo"] = json.dumps(block).encode("utf-8")
    pq.write_table(table.replace_schema_metadata(metadata), str(target), compression="ZSTD")
    return target


def make_with_geometry_column_named(source: Path, target: Path, name: str) -> Path:
    """``buildings`` with its geometry column renamed, ``geo`` block to match."""
    table = _read(source)
    metadata = dict(table.schema.metadata or {})  # rename_columns drops it
    table = table.rename_columns([name if c == "geometry" else c for c in table.column_names])
    block = json.loads(metadata[b"geo"])
    block["primary_column"] = name
    block["columns"] = {name: block["columns"]["geometry"]}
    metadata[b"geo"] = json.dumps(block).encode("utf-8")
    pq.write_table(table.replace_schema_metadata(metadata), str(target), compression="ZSTD")
    return target


def assert_clean_input(path: Path) -> None:
    """Non-vacuity: "the output has no failures" says nothing if the input had some."""
    failures = spec_failures(path)
    assert failures == {}, (
        f"the fixture is already broken, so the oracle proves nothing: {failures}"
    )


def assert_no_unexpected_residue(shape: Shape, output: str) -> None:
    """``check all --fix`` says what it could not repair; pin exactly that much."""
    if shape.check_all_residual:
        assert shape.check_all_residual in output, output
        assert output.count("   - ") == 1, f"more than the one expected residual issue:\n{output}"
    else:
        assert "Some issues remain after fixes" not in output, output


def assert_shape_output(shape: Shape, path: Path, *, rewrite: bool) -> None:
    """The oracle, with the expectations this shape carries for this kind of fix."""
    assert_fix_output_is_sound(
        path,
        expected_rows=shape.rows,
        expected_crs=shape.crs,
        expects_covering=shape.rewrite_covering if rewrite else shape.check_all_covering,
        expected_version_prefix=shape.rewrite_version if rewrite else shape.check_all_version,
    )


# ---------------------------------------------------------------------------
# fix_compression
# ---------------------------------------------------------------------------


class TestCompressionFixOutput:
    """``check compression --fix`` -> ``check_fixes.fix_compression``."""

    @pytest.mark.parametrize("in_place", [False, True], ids=["fix-output", "in-place"])
    def test_output_is_sound(self, shape, in_place, tmp_path):
        spec, source = shape
        broken = make_snappy(source, tmp_path / "snappy.parquet")
        assert_clean_input(broken)
        fixed = broken if in_place else tmp_path / "fixed.parquet"
        args = [] if in_place else ["--fix-output", fixed]

        output = run_cli("check", "compression", broken, "--fix", *args)

        assert "No fix needed" not in output, "the fix declined, so nothing was measured"
        if in_place:
            assert Path(f"{broken}.bak").exists(), "an in-place fix must leave a backup"
        # The fix's own check no longer finds the defect: valid is not the same as fixed.
        assert "ZSTD recommended" not in run_cli("check", "compression", fixed)
        assert_shape_output(spec, fixed, rewrite=True)


# ---------------------------------------------------------------------------
# fix_row_groups
# ---------------------------------------------------------------------------


class TestRowGroupFixOutput:
    """``check row-group --fix`` -> ``check_fixes.fix_row_groups``."""

    @pytest.mark.parametrize("in_place", [False, True], ids=["fix-output", "in-place"])
    def test_output_is_sound(self, shape, in_place, tmp_path):
        spec, source = shape
        broken = make_tiny_row_groups(source, tmp_path / "tiny.parquet")
        assert_clean_input(broken)
        assert pq.read_metadata(str(broken)).num_row_groups > 1
        fixed = broken if in_place else tmp_path / "fixed.parquet"
        args = [] if in_place else ["--fix-output", fixed]

        output = run_cli("check", "row-group", broken, "--fix", *args)

        assert "No fix needed" not in output, "the fix declined, so nothing was measured"
        if in_place:
            assert Path(f"{broken}.bak").exists(), "an in-place fix must leave a backup"
        assert pq.read_metadata(str(fixed)).num_row_groups == 1
        assert_shape_output(spec, fixed, rewrite=True)


# ---------------------------------------------------------------------------
# fix_spatial_ordering
# ---------------------------------------------------------------------------


class TestSpatialFixOutput:
    """``check spatial --fix`` -> ``check_fixes.fix_spatial_ordering``."""

    @pytest.mark.parametrize("in_place", [False, True], ids=["fix-output", "in-place"])
    def test_output_is_sound(self, shape, in_place, tmp_path):
        """Sorted, and a permutation: the same rows, in another order."""
        spec, source = shape
        broken = make_unsorted(source, tmp_path / "unsorted.parquet")
        assert_clean_input(broken)
        before = _read(broken).column(spec.id_column).to_pylist()
        fixed = broken if in_place else tmp_path / "fixed.parquet"
        args = [] if in_place else ["--fix-output", fixed]

        output = run_cli("check", "spatial", broken, "--fix", *args, "--random-sample-size", 20)

        assert "No fix needed" not in output, "the fix declined, so nothing was measured"
        if in_place:
            assert Path(f"{broken}.bak").exists(), "an in-place fix must leave a backup"
        after = _read(fixed).column(spec.id_column).to_pylist()
        assert sorted(after) == sorted(before), "the rows are not the input's rows"
        assert after != before, "the fix wrote the rows back in the order it found them"
        # The fix's own check no longer finds the defect: valid is not the same as fixed.
        assert "Poor spatial ordering" not in run_cli(
            "check", "spatial", fixed, "--random-sample-size", 20
        )
        assert_shape_output(spec, fixed, rewrite=True)


# ---------------------------------------------------------------------------
# fix_bbox_column / fix_bbox_metadata / fix_bbox_all
# (fix_bbox_removal is judged in test_check_fix_preserves_native_geo.py)
# ---------------------------------------------------------------------------


class TestBboxFixOutput:
    """``check bbox --fix`` -- version-aware: it adds for 1.x, removes for native."""

    @pytest.mark.parametrize("shape_name", ["v1_0_crs84", "v1_1_epsg31287"], ids=["v1_0", "v1_1"])
    @pytest.mark.parametrize("in_place", [False, True], ids=["fix-output", "in-place"])
    def test_a_missing_bbox_column_is_added_and_declared(
        self, shape_name, in_place, tmp_path, request
    ):
        spec = SHAPE_BY_NAME[shape_name]
        source = Path(str(request.getfixturevalue(spec.fixture)))
        broken = make_without_bbox(source, tmp_path / "no_bbox.parquet", spec.bbox_column)
        assert_clean_input(broken)
        assert spec.bbox_column not in pq.read_schema(str(broken)).names
        fixed = broken if in_place else tmp_path / "fixed.parquet"
        args = [] if in_place else ["--fix-output", fixed]

        run_cli("check", "bbox", broken, "--fix", *args)

        assert "bbox" in pq.read_schema(str(fixed)).names, "the fix did not run"
        if in_place:
            assert Path(f"{broken}.bak").exists(), "an in-place fix must leave a backup"
        assert_fix_output_is_sound(
            fixed,
            expected_rows=spec.rows,
            expected_crs=spec.crs,
            expects_covering=True,
            expected_version_prefix="1.1",
        )
        assert_bbox_column_matches_geometry(fixed)

    def test_a_missing_covering_is_added(self, austria_bbox_covering_file, tmp_path):
        """The ``fix_bbox_metadata`` path: the column is there, the covering is not."""
        spec = SHAPE_BY_NAME["v1_1_epsg31287"]
        broken = make_without_covering(
            Path(austria_bbox_covering_file), tmp_path / "no_cov.parquet"
        )
        assert_clean_input(broken)
        assert covering_of(broken) is None
        fixed = tmp_path / "fixed.parquet"

        run_cli("check", "bbox", broken, "--fix", "--fix-output", fixed)

        assert_fix_output_is_sound(
            fixed,
            expected_rows=spec.rows,
            expected_crs=spec.crs,
            expects_covering=True,
            expected_version_prefix="1.1",
        )
        assert_bbox_column_matches_geometry(fixed)

    @pytest.mark.parametrize(
        "name",
        ['geom"x', "geom'x", "géométrie", "geom; drop table t; --"],
        ids=["double-quote", "single-quote", "unicode", "sql"],
    )
    def test_an_adversarial_primary_column_name_survives_the_fix(
        self, name, buildings_test_file, tmp_path
    ):
        """``geo.primary_column`` is interpolated into SQL by the bbox fix.

        CLAUDE.md calls it an injection surface; a quoting bug would show here
        as a column that disappears, is duplicated, or holds the wrong bounds.
        """
        broken = make_with_geometry_column_named(
            Path(str(buildings_test_file)), tmp_path / "named.parquet", name
        )
        assert_clean_input(broken)
        fixed = tmp_path / "fixed.parquet"

        run_cli("check", "bbox", broken, "--fix", "--fix-output", fixed)

        assert pq.read_schema(str(fixed)).names == ["id", name, "bbox"]
        assert_fix_output_is_sound(
            fixed,
            expected_rows=42,
            expected_crs=CRS84,
            geometry_column=name,
            expects_covering=True,
            expected_version_prefix="1.1",
        )
        assert_bbox_column_matches_geometry(fixed, geometry_column=name)


# ---------------------------------------------------------------------------
# apply_all_fixes
# ---------------------------------------------------------------------------


class TestCheckAllFixOutput:
    """``check all --fix`` -> ``check_fixes.apply_all_fixes``, every fix chained."""

    def test_output_is_sound(self, shape, tmp_path):
        """Every defect at once: SNAPPY, tiny row groups, shuffled rows."""
        spec, source = shape
        broken = make_unsorted(source, tmp_path / "broken.parquet", compression="SNAPPY")
        assert_clean_input(broken)
        fixed = tmp_path / "fixed.parquet"

        output = run_cli(
            "check", "all", broken, "--fix", "--fix-output", fixed, "--random-sample-size", 20
        )

        assert_no_unexpected_residue(spec, output)
        assert_shape_output(spec, fixed, rewrite=False)

    def test_in_place_output_is_sound(self, shape, tmp_path):
        spec, source = shape
        target = make_snappy(source, tmp_path / "inplace.parquet")

        output = run_cli("check", "all", target, "--fix", "--random-sample-size", 20)

        assert_no_unexpected_residue(spec, output)
        assert Path(f"{target}.bak").exists()
        assert_shape_output(spec, target, rewrite=False)


# ---------------------------------------------------------------------------
# Multi-file
# ---------------------------------------------------------------------------


class TestMultiFileFixOutput:
    """``--all-files --fix`` rewrites a whole partition; every output is judged."""

    def test_every_rewritten_file_is_sound(self, austria_bbox_covering_file, tmp_path):
        partition = tmp_path / "partition"
        partition.mkdir()
        for index in range(3):
            make_snappy(Path(austria_bbox_covering_file), partition / f"p{index}.parquet")

        run_cli("check", "compression", partition, "--all-files", "--fix")

        spec = SHAPE_BY_NAME["v1_1_epsg31287"]
        for index in range(3):
            written = partition / f"p{index}.parquet"
            assert Path(f"{written}.bak").exists()
            assert_shape_output(spec, written, rewrite=True)


# ---------------------------------------------------------------------------
# A fixture whose failure is not the fix's fault
# ---------------------------------------------------------------------------


class TestAnExpectedFailureStaysExactlyOneFailure:
    """``fields_pgo_5070_snappy`` is labelled EPSG:5070 with data over Europe.

    ``check spec`` scores it ``✗ coordinates outside valid range for CRS``
    whatever the command under test did, so the oracle is given that one failure
    as an explicit baseline rather than being weakened to "no *new* failures".
    A second failure still fails the test, and so does this one disappearing.
    """

    OUT_OF_AREA = {
        "coordinates_valid_for_crs_geometry": (
            "the fixture's coordinates are over Europe while its CRS is EPSG:5070 "
            "(CONUS); the fix cannot repair data that never matched its label"
        )
    }

    def test_compression_fix_adds_no_second_failure(
        self, fields_geom_type_only_5070_file, tmp_path
    ):
        target = tmp_path / "pgo_5070.parquet"
        shutil.copy2(fields_geom_type_only_5070_file, target)
        assert set(spec_failures(target)) == set(self.OUT_OF_AREA), "the baseline is the input's"
        fixed = tmp_path / "fixed.parquet"

        run_cli("check", "compression", target, "--fix", "--fix-output", fixed)

        assert_fix_output_is_sound(
            fixed,
            expected_rows=100,
            expected_crs={"authority": "EPSG", "code": 5070},
            expects_covering=False,
            known_spec_failures=self.OUT_OF_AREA,
        )


# ---------------------------------------------------------------------------
# The census itself
# ---------------------------------------------------------------------------


#: Every public entry point in ``core/check_fixes.py`` and the test that judges
#: its output, as ``module:qualname`` so a renamed or deleted test is noticed.
COVERED_ENTRY_POINTS = {
    "fix_compression": "tests.test_check_fix_output_is_valid:TestCompressionFixOutput",
    "fix_row_groups": "tests.test_check_fix_output_is_valid:TestRowGroupFixOutput",
    "fix_spatial_ordering": "tests.test_check_fix_output_is_valid:TestSpatialFixOutput",
    "fix_bbox_column": (
        "tests.test_check_fix_output_is_valid:"
        "TestBboxFixOutput.test_a_missing_bbox_column_is_added_and_declared"
    ),
    "fix_bbox_metadata": (
        "tests.test_check_fix_output_is_valid:TestBboxFixOutput.test_a_missing_covering_is_added"
    ),
    # Both bbox paths route through it.
    "fix_bbox_all": "tests.test_check_fix_output_is_valid:TestBboxFixOutput",
    "fix_bbox_removal": (
        "tests.test_check_fix_preserves_native_geo:"
        "test_removing_a_bbox_column_keeps_the_crs_it_is_removed_from"
    ),
    "apply_all_fixes": "tests.test_check_fix_output_is_valid:TestCheckAllFixOutput",
}


def _resolve(reference: str) -> object:
    module_name, qualname = reference.split(":")
    target = importlib.import_module(module_name)
    for part in qualname.split("."):
        target = getattr(target, part)
    return target


def test_every_fix_entry_point_is_covered():
    """A ``--fix`` path added without an output oracle has to be visible.

    WP-1 exists because five commands grew seven write paths and no test ever
    looked at what any of them wrote. Reading the entry points off the module
    rather than listing them by hand is what keeps the eighth from arriving
    unnoticed; resolving each reference is what keeps the list honest.
    """
    from geoparquet_io.core import check_fixes

    public = {
        name
        for name, value in vars(check_fixes).items()
        if (name.startswith("fix_") or name == "apply_all_fixes")
        and callable(value)
        and getattr(value, "__module__", None) == check_fixes.__name__
    }

    assert public == set(COVERED_ENTRY_POINTS), (
        "core/check_fixes.py's entry points and this file's oracle coverage have "
        "drifted. Add a test for the new path (or drop the stale entry) in "
        f"COVERED_ENTRY_POINTS. Uncovered: {sorted(public - set(COVERED_ENTRY_POINTS))}; "
        f"stale: {sorted(set(COVERED_ENTRY_POINTS) - public)}"
    )
    for entry_point, reference in COVERED_ENTRY_POINTS.items():
        assert callable(_resolve(reference)), f"{entry_point}: {reference} is not a test"


# ---------------------------------------------------------------------------
# What the oracle found
# ---------------------------------------------------------------------------


class TestKnownDefectsInTheFixes:
    """Defects WP-1 turned up, pinned rather than repaired here: this repo fixes
    cross-cutting defects in their own PR."""

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "gpio #1035: check compression/row-group/spatial --fix upgrade a 1.0 "
            "file to 1.1 and declare a covering over its bbox column without "
            "checking that the column can legally be one. With Overture's "
            "xmin/xmax/ymin/ymax field order the output fails gpio's own "
            "`check spec` on covering_bbox_structure -- a valid input turned "
            "into an invalid output."
        ),
    )
    def test_a_rewrite_fix_does_not_declare_a_covering_the_spec_rejects(
        self, unsorted_test_file, tmp_path
    ):
        """``tests/data/unsorted.parquet`` is the shape as shipped: 1.0, bbox in
        Overture's ``xmin, xmax, ymin, ymax`` order, valid because 1.0 cannot
        carry a covering to point at the column."""
        broken = tmp_path / "overture.parquet"
        shutil.copy2(unsorted_test_file, broken)
        assert_clean_input(broken)
        fixed = tmp_path / "fixed.parquet"

        output = run_cli("check", "row-group", broken, "--fix", "--fix-output", fixed)

        assert "No fix needed" not in output, "the fix declined, so nothing was measured"
        assert_fix_output_is_sound(fixed, expected_rows=1445, expected_crs=CRS84)
