"""Every ``gpio check --fix`` path, run on a broken file, judged on its output.

WP-1 of #1018. The suite had 40-odd ``--fix`` invocations and none of them
looked at the file the fix wrote beyond the single metric it had just repaired:
``current_compression == "ZSTD"``, ``num_row_groups == 1``,
``"bbox" in schema``. A repair that fixes its metric while corrupting the
``geo`` block was invisible, which is the "gpio writes a file gpio rejects"
pattern of #890, #954, #972 and #1003 arriving at the test level.

Every test here runs a fix on an input that **actually has the defect** -- a fix
that declines to run proves nothing -- and then hands the output to
:func:`tests.fix_output_oracle.assert_fix_output_is_sound`: zero FAILED checks
from ``check spec``, the row count preserved, the CRS read from the ``geo``
block and the Parquet logical type **separately**, and the covering resolved
against the real schema.

The four shapes in :data:`SHAPES` are the four a fix has to answer the version
and CRS questions about differently, and two of them are deliberately not in
the default CRS: a file already in CRS84 cannot tell a path that keeps the CRS
from one that loses it, because an absent ``crs`` already means CRS84 (#993).

Two defects this file found are pinned as ``xfail(strict=True)`` in
:class:`TestKnownDefectsInTheFixes` rather than repaired here -- ``--fix`` lives
in ``core/check_fixes.py``, which #1033 is rewriting, and this repo fixes
cross-cutting defects in their own PR.
"""

from __future__ import annotations

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
from tests.fix_output_oracle import CRS84, assert_fix_output_is_sound, covering_of, spec_failures
from tests.native_geo_probes import geo_block

#: ``expected_version_prefix`` sentinel: the output must carry no ``geo`` key.
NO_GEO_BLOCK = "<no geo block>"


def run_cli(*args):
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
    geometry_column: str
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
        rows=766,
        crs=CRS84,
        geometry_column="geometry",
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
        geometry_column="geometry",
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
        geometry_column="geometry",
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
        geometry_column="geometry",
        bbox_column=None,
        rewrite_version="2.0",
        rewrite_covering=False,
        check_all_version=NO_GEO_BLOCK,
        check_all_covering=False,
    ),
]


@pytest.fixture(params=SHAPES, ids=lambda shape: shape.name)
def shape(request):
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


def make_unsorted(source: Path, target: Path) -> Path:
    """Shuffled into tiny row groups: ``check spatial --fix`` has work.

    Both halves matter. A single-row-group file has no pairs of group bounding
    boxes to compare, so the spatial check calls it well ordered whatever the
    rows contain (#940).
    """
    table = _read(source)
    order = np.random.RandomState(0).permutation(table.num_rows)
    pq.write_table(table.take(pa.array(order)), str(target), compression="ZSTD", row_group_size=10)
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


def make_without_covering(source: Path, target: Path, geometry_column: str) -> Path:
    """Bbox column kept, its covering dropped: the ``fix_bbox_metadata`` path."""
    table = _read(source)
    metadata = dict(table.schema.metadata or {})
    block = json.loads(metadata[b"geo"])
    block["columns"][geometry_column].pop("covering", None)
    metadata[b"geo"] = json.dumps(block).encode("utf-8")
    pq.write_table(table.replace_schema_metadata(metadata), str(target), compression="ZSTD")
    return target


def assert_clean_input(path) -> None:
    """Non-vacuity: "the output has no failures" says nothing if the input had some."""
    failures = spec_failures(path)
    assert failures == {}, (
        f"the fixture is already broken, so the oracle proves nothing: {failures}"
    )


def assert_no_unexpected_residue(shape: Shape, output: str) -> None:
    """``check all --fix`` says what it could not repair. Pin exactly that much."""
    if shape.check_all_residual:
        assert shape.check_all_residual in output, output
    else:
        assert "Some issues remain after fixes" not in output, output


def assert_shape_output(shape: Shape, path, *, rewrite: bool) -> None:
    """The oracle, with the expectations this shape carries for this kind of fix."""
    version = shape.rewrite_version if rewrite else shape.check_all_version
    covering = shape.rewrite_covering if rewrite else shape.check_all_covering
    if version == NO_GEO_BLOCK:
        assert geo_block(path) is None, (
            f"{path}: a native-geo-only input must stay native-geo-only, "
            f"found a geo block: {geo_block(path)!r}"
        )
        version = None
    assert_fix_output_is_sound(
        path,
        expected_rows=shape.rows,
        expected_crs=shape.crs,
        geometry_column=shape.geometry_column,
        expects_covering=covering,
        expected_version_prefix=version,
    )


# ---------------------------------------------------------------------------
# fix_compression
# ---------------------------------------------------------------------------


class TestCompressionFixOutput:
    """``check compression --fix`` -> ``check_fixes.fix_compression``."""

    def test_output_is_sound(self, shape, tmp_path):
        spec, source = shape
        broken = make_snappy(source, tmp_path / "snappy.parquet")
        assert_clean_input(broken)
        fixed = tmp_path / "fixed.parquet"

        output = run_cli("check", "compression", broken, "--fix", "--fix-output", fixed)

        assert "No fix needed" not in output, "the fix declined, so nothing was measured"
        assert_shape_output(spec, fixed, rewrite=True)

    def test_in_place_output_is_sound(self, shape, tmp_path):
        """The default: no ``--fix-output``, so the user's own file is rewritten."""
        spec, source = shape
        target = make_snappy(source, tmp_path / "inplace.parquet")

        run_cli("check", "compression", target, "--fix")

        assert Path(f"{target}.bak").exists(), "an in-place fix must leave a backup"
        assert_shape_output(spec, target, rewrite=True)


# ---------------------------------------------------------------------------
# fix_row_groups
# ---------------------------------------------------------------------------


class TestRowGroupFixOutput:
    """``check row-group --fix`` -> ``check_fixes.fix_row_groups``."""

    def test_output_is_sound(self, shape, tmp_path):
        spec, source = shape
        broken = make_tiny_row_groups(source, tmp_path / "tiny.parquet")
        assert_clean_input(broken)
        assert pq.ParquetFile(str(broken)).metadata.num_row_groups > 1
        fixed = tmp_path / "fixed.parquet"

        output = run_cli("check", "row-group", broken, "--fix", "--fix-output", fixed)

        assert "No fix needed" not in output, "the fix declined, so nothing was measured"
        assert pq.ParquetFile(str(fixed)).metadata.num_row_groups == 1
        assert_shape_output(spec, fixed, rewrite=True)


# ---------------------------------------------------------------------------
# fix_spatial_ordering
# ---------------------------------------------------------------------------


class TestSpatialFixOutput:
    """``check spatial --fix`` -> ``check_fixes.fix_spatial_ordering``."""

    def test_output_is_sound(self, shape, tmp_path):
        spec, source = shape
        broken = make_unsorted(source, tmp_path / "unsorted.parquet")
        assert_clean_input(broken)
        fixed = tmp_path / "fixed.parquet"

        output = run_cli(
            "check",
            "spatial",
            broken,
            "--fix",
            "--fix-output",
            fixed,
            "--random-sample-size",
            "20",
        )

        assert "No fix needed" not in output, "the fix declined, so nothing was measured"
        assert_shape_output(spec, fixed, rewrite=True)

    def test_the_rows_are_reordered_not_replaced(self, shape, tmp_path):
        """A Hilbert sort is a permutation: the same rows, in another order."""
        spec, source = shape
        broken = make_unsorted(source, tmp_path / "unsorted.parquet")
        before = _read(broken).column("id" if spec.name != "v1_0_crs84" else "fsq_place_id")
        fixed = tmp_path / "fixed.parquet"

        run_cli(
            "check",
            "spatial",
            broken,
            "--fix",
            "--fix-output",
            fixed,
            "--random-sample-size",
            "20",
        )

        after = _read(fixed).column("id" if spec.name != "v1_0_crs84" else "fsq_place_id")
        assert sorted(after.to_pylist()) == sorted(before.to_pylist())
        assert_shape_output(spec, fixed, rewrite=True)


# ---------------------------------------------------------------------------
# fix_bbox_column / fix_bbox_metadata / fix_bbox_all / fix_bbox_removal
# ---------------------------------------------------------------------------


class TestBboxFixOutput:
    """``check bbox --fix`` -- version-aware: it adds for 1.x, removes for native."""

    @pytest.mark.parametrize("shape_name", ["v1_0_crs84", "v1_1_epsg31287"], ids=["v1_0", "v1_1"])
    def test_a_missing_bbox_column_is_added_and_declared(self, shape_name, tmp_path, request):
        spec = next(s for s in SHAPES if s.name == shape_name)
        source = Path(str(request.getfixturevalue(spec.fixture)))
        broken = make_without_bbox(source, tmp_path / "no_bbox.parquet", spec.bbox_column)
        assert_clean_input(broken)
        assert spec.bbox_column not in pq.read_schema(str(broken)).names
        fixed = tmp_path / "fixed.parquet"

        run_cli("check", "bbox", broken, "--fix", "--fix-output", fixed)

        assert "bbox" in pq.read_schema(str(fixed)).names, "the fix did not run"
        assert_fix_output_is_sound(
            fixed,
            expected_rows=spec.rows,
            expected_crs=spec.crs,
            expects_covering=True,
            expected_version_prefix="1.1",
        )

    def test_a_missing_covering_is_added(self, austria_bbox_covering_file, tmp_path):
        """The ``fix_bbox_metadata`` path: the column is there, the covering is not."""
        spec = next(s for s in SHAPES if s.name == "v1_1_epsg31287")
        broken = make_without_covering(
            Path(austria_bbox_covering_file), tmp_path / "no_covering.parquet", "geometry"
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

    def test_a_native_file_loses_the_bbox_column_and_keeps_its_crs(self, projected_conus, tmp_path):
        """``fix_bbox_removal``: native stats replace the column, the CRS stays.

        The input is built the way a user gets one -- ``gpio add bbox`` over a
        native-geo-only file -- and then has its covering stripped, because a
        *declared* bbox column is "optimal" and never removed.
        """
        with_bbox = tmp_path / "with_bbox.parquet"
        run_cli("add", "bbox", projected_conus, with_bbox)
        table = _read(with_bbox)
        block = json.loads(table.schema.metadata[b"geo"])
        block["columns"]["geometry"].pop("covering", None)
        metadata = dict(table.schema.metadata)
        metadata[b"geo"] = json.dumps(block).encode("utf-8")
        pq.write_table(table.replace_schema_metadata(metadata), str(with_bbox), compression="zstd")
        assert_clean_input(with_bbox)
        fixed = tmp_path / "fixed.parquet"

        run_cli("check", "bbox", with_bbox, "--fix", "--fix-output", fixed)

        assert "bbox" not in pq.read_schema(str(fixed)).names, "the fix did not run"
        assert_fix_output_is_sound(
            fixed,
            expected_rows=200,
            expected_crs={"authority": "EPSG", "code": 5070},
            expects_covering=False,
            expected_version_prefix="2.0",
        )


# ---------------------------------------------------------------------------
# apply_all_fixes
# ---------------------------------------------------------------------------


class TestCheckAllFixOutput:
    """``check all --fix`` -> ``check_fixes.apply_all_fixes``, every fix chained."""

    def test_output_is_sound(self, shape, tmp_path):
        """Every defect at once: SNAPPY, tiny row groups, shuffled rows."""
        spec, source = shape
        broken = tmp_path / "broken.parquet"
        table = _read(source)
        order = np.random.RandomState(1).permutation(table.num_rows)
        pq.write_table(
            table.take(pa.array(order)), str(broken), compression="SNAPPY", row_group_size=10
        )
        assert_clean_input(broken)
        fixed = tmp_path / "fixed.parquet"

        output = run_cli(
            "check",
            "all",
            broken,
            "--fix",
            "--fix-output",
            fixed,
            "--random-sample-size",
            "20",
        )

        assert_no_unexpected_residue(spec, output)
        assert_shape_output(spec, fixed, rewrite=False)

    def test_in_place_output_is_sound(self, shape, tmp_path):
        spec, source = shape
        target = make_snappy(source, tmp_path / "inplace.parquet")

        output = run_cli("check", "all", target, "--fix", "--random-sample-size", "20")

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

        spec = next(s for s in SHAPES if s.name == "v1_1_epsg31287")
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

    def test_the_baseline_is_the_input_s_own(self, fields_geom_type_only_5070_file):
        assert set(spec_failures(fields_geom_type_only_5070_file)) == set(self.OUT_OF_AREA)

    def test_compression_fix_adds_no_second_failure(
        self, fields_geom_type_only_5070_file, tmp_path
    ):
        target = tmp_path / "pgo_5070.parquet"
        shutil.copy2(fields_geom_type_only_5070_file, target)
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


#: Every public entry point in ``core/check_fixes.py``, and what drives it here.
#: A new one with no oracle test fails ``test_every_fix_entry_point_is_covered``.
COVERED_ENTRY_POINTS = {
    "fix_compression": "TestCompressionFixOutput",
    "fix_row_groups": "TestRowGroupFixOutput",
    "fix_spatial_ordering": "TestSpatialFixOutput",
    "fix_bbox_column": "TestBboxFixOutput.test_a_missing_bbox_column_is_added_and_declared",
    "fix_bbox_metadata": "TestBboxFixOutput.test_a_missing_covering_is_added",
    "fix_bbox_all": "TestBboxFixOutput (both bbox paths route through it)",
    "fix_bbox_removal": "TestBboxFixOutput.test_a_native_file_loses_the_bbox_column_and_keeps_its_crs",
    "apply_all_fixes": "TestCheckAllFixOutput",
}


def test_every_fix_entry_point_is_covered():
    """A ``--fix`` path added without an output oracle has to be visible.

    WP-1 exists because five commands grew seven write paths and no test ever
    looked at what any of them wrote. Reading the entry points off the module
    rather than listing them by hand is what keeps the eighth from arriving
    unnoticed.
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


# ---------------------------------------------------------------------------
# What the oracle found
# ---------------------------------------------------------------------------


class TestKnownDefectsInTheFixes:
    """Defects WP-1 turned up. Pinned, not repaired: ``core/check_fixes.py`` is
    being rewritten by #1033, and this repo fixes cross-cutting defects in their
    own PR."""

    @staticmethod
    def _overture_order_bbox(source: Path, target: Path) -> Path:
        """A 1.0 file whose bbox struct is ``xmin, xmax, ymin, ymax``.

        Not a contrivance: that is the order Overture writes, and it is the order
        this repo's own ``tests/data/country_partition/*.parquet`` and
        ``tests/data/unsorted.parquet`` carry. At 1.0 the file is *valid* --
        there is no covering to point at the column, and 1.0 cannot carry one --
        so ``check spec`` passes it.
        """
        table = _read(source)
        rows = table.num_rows
        bbox = pa.StructArray.from_arrays(
            [
                pa.array([0.0] * rows, type=pa.float32()),
                pa.array([1.0] * rows, type=pa.float32()),
                pa.array([0.0] * rows, type=pa.float32()),
                pa.array([1.0] * rows, type=pa.float32()),
            ],
            names=["xmin", "xmax", "ymin", "ymax"],
        )
        if "bbox" in table.column_names:
            table = table.drop(["bbox"])
        metadata = dict(table.schema.metadata or {})
        block = json.loads(metadata[b"geo"])
        block["version"] = "1.0.0"
        block["columns"]["geometry"].pop("covering", None)
        metadata[b"geo"] = json.dumps(block).encode("utf-8")
        table = table.append_column("bbox", bbox).replace_schema_metadata(metadata)
        pq.write_table(table, str(target), compression="SNAPPY")
        return target

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
        self, places_test_file, tmp_path
    ):
        broken = self._overture_order_bbox(Path(places_test_file), tmp_path / "overture.parquet")
        assert_clean_input(broken)
        fixed = tmp_path / "fixed.parquet"

        run_cli("check", "compression", broken, "--fix", "--fix-output", fixed)

        assert_fix_output_is_sound(
            fixed, expected_rows=766, expected_crs=CRS84, expects_covering=True
        )

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "gpio #1036: `check bbox --fix --fix-output OTHER` on a file that "
            "needs only covering metadata *moves* the input to the output path "
            "(fix_bbox_all's shutil.move), so the user's original file is gone "
            "and no backup was taken."
        ),
    )
    def test_a_fix_to_another_path_leaves_the_input_where_it_was(
        self, austria_bbox_covering_file, tmp_path
    ):
        broken = make_without_covering(
            Path(austria_bbox_covering_file), tmp_path / "input.parquet", "geometry"
        )
        before = broken.read_bytes()
        fixed = tmp_path / "output.parquet"

        run_cli("check", "bbox", broken, "--fix", "--fix-output", fixed)

        assert broken.exists(), "--fix-output wrote elsewhere but destroyed the input"
        assert broken.read_bytes() == before
