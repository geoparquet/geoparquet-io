"""``gpio check --fix`` must not leave the file worse than it found it.

``check --fix`` is remedial: whatever it writes is the file gpio itself will
check next. Three of its rewrites called ``write_parquet_with_metadata`` without
``input_file=``, so the write facade (#990) had no witness for the two questions
a rewrite has to answer about a *native-geo-only* input -- which version, and
which CRS -- and one of them also withheld the input's own ``geo`` block from a
2.0 rewrite. Two symptoms, one deferral:

* **#1001.** ``gpio check compression --fix`` and ``gpio check row-group --fix``
  on a native-geo-only EPSG:5070 file wrote 1.1 WKB and dropped the CRS with the
  logical type it lived in. The output then failed ``check spec`` with
  ``coordinates outside valid range for CRS`` -- projected metres read as
  degrees, because an absent ``crs`` means ``OGC:CRS84``.
* **#1003.** ``gpio add bbox`` writes native 2.0 with a ``covering`` since #997.
  ``check all --fix`` over that output discarded the covering and then complained
  about the file it had just written::

      ⚠️  Some issues remain after fixes:
         - Bbox column 'bbox' is not declared in the 'covering' metadata

Every end-to-end assertion below reads the ``geo`` block and the Parquet logical
type **separately** and then asks ``gpio check spec`` whether they agree.
``crs_utils.source_crs_string`` reads the first, falls back to the second and
returns whichever answered, so it is structurally incapable of seeing the two
disagree -- which is how #993 first shipped green with the bug still in it.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1001
Refs: https://github.com/geoparquet/geoparquet-io/issues/1003
Refs: https://github.com/geoparquet/geoparquet-io/issues/997
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests.fix_output_oracle import assert_fix_output_is_sound
from tests.native_geo_probes import (
    EPSG_5070,
    conus_wkb,
    geo_block,
    geo_block_crs_id,
    geo_version,
    logical_geo_types,
    projjson,
    spec_problems,
    write_native_geo_only,
)

#: The 200 CONUS polygons every fixture here is built from. A fix never changes
#: the row count, and until WP-1 (#1018) nothing in this file checked that.
CONUS_ROWS = 200


def assert_conus_output_is_sound(path, *, version, covering=False):
    """The shared ``--fix`` oracle, with this file's constants.

    Adds to what each test already asserts: the rows survived, and every
    ``covering`` path resolves against the real schema. The CRS halves and the
    ``check spec`` verdict it re-checks are the point of this file, so they are
    deliberately asserted twice -- once here as the shared contract, once above
    in the terms of the issue the test is about.
    """
    assert_fix_output_is_sound(
        path,
        expected_rows=CONUS_ROWS,
        expected_crs=EPSG_5070,
        expects_covering=covering,
        expected_version_prefix=version,
    )


def _run_cli(*args) -> str:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output
    return result.output


def _covering(path) -> dict | None:
    """One column's declared ``covering``, read off the ``geo`` block."""
    columns = (geo_block(path) or {}).get("columns") or {}
    return (columns.get("geometry") or {}).get("covering")


def _rewrite_geo_block(path, block: dict) -> None:
    table = pq.read_table(str(path))
    metadata = dict(table.schema.metadata or {})
    metadata[b"geo"] = json.dumps(block).encode("utf-8")
    pq.write_table(table.replace_schema_metadata(metadata), str(path), compression="zstd")


def _strip_covering(path) -> None:
    """Rewrite ``path`` with its primary column's ``covering`` removed from the block."""
    block = geo_block(path)
    block["columns"]["geometry"].pop("covering", None)
    _rewrite_geo_block(path, block)


def _declare_geo_block(path, *, crs_epsg: int) -> None:
    """Give a ``geo``-less file a 2.0 block naming ``crs_epsg`` and no covering."""
    column = {
        "encoding": "WKB",
        "geometry_types": ["Polygon"],
        "crs": json.loads(projjson(crs_epsg)),
    }
    _rewrite_geo_block(
        path, {"version": "2.0.0", "primary_column": "geometry", "columns": {"geometry": column}}
    )


def _geometry_compression(path) -> str:
    """The codec the geometry column is stored with, from the footer."""
    metadata = pq.ParquetFile(str(path)).metadata
    schema = pq.ParquetFile(str(path)).schema
    index = next(i for i in range(len(schema)) if schema.column(i).name == "geometry")
    return metadata.row_group(0).column(index).compression


# ---------------------------------------------------------------------------
# Fixtures. `projected_conus` (conftest) is the clean native-geo-only EPSG:5070
# input; these are the same 200 polygons written so that `check --fix` has
# something to repair, because a fix that never runs proves nothing.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _conus_5070_rows():
    return conus_wkb("ST_Transform(cell, 'EPSG:4326', 'EPSG:5070', always_xy := true)")


@pytest.fixture
def native_geo_snappy(tmp_path, _conus_5070_rows) -> Path:
    """Native-geo-only EPSG:5070, SNAPPY: ``check compression --fix`` has work."""
    import geoarrow.pyarrow as ga

    return write_native_geo_only(
        tmp_path / "pgo_snappy.parquet",
        _conus_5070_rows,
        {"geometry": (2, ga.wkb().with_crs(projjson(5070)))},
        compression="snappy",
    )


@pytest.fixture
def native_geo_tiny_row_groups(tmp_path, _conus_5070_rows) -> Path:
    """Native-geo-only EPSG:5070 in 5-row groups: ``check row-group --fix`` has work."""
    import geoarrow.pyarrow as ga

    return write_native_geo_only(
        tmp_path / "pgo_tiny_groups.parquet",
        _conus_5070_rows,
        {"geometry": (2, ga.wkb().with_crs(projjson(5070)))},
        row_group_size=5,
    )


# ---------------------------------------------------------------------------
# #1001: the version and the CRS both survive a --fix rewrite
# ---------------------------------------------------------------------------


def test_the_fixtures_are_clean_native_geo_only_epsg_5070_files(
    native_geo_snappy, native_geo_tiny_row_groups
):
    """Non-vacuity: "the output has no failures" only means something if the input had none."""
    for fixture in (native_geo_snappy, native_geo_tiny_row_groups):
        assert geo_block(fixture) is None
        assert logical_geo_types(fixture) == {"geometry": ("Geometry", EPSG_5070)}
        assert spec_problems(fixture) == []


def test_check_compression_fix_keeps_the_native_type_and_the_crs(native_geo_snappy):
    """#1001, through the compression fix (``check_fixes.fix_compression``)."""
    _run_cli("check", "compression", native_geo_snappy, "--fix")

    assert _geometry_compression(native_geo_snappy) == "ZSTD", "the fix did not run"
    assert (geo_version(native_geo_snappy) or "").startswith("2.0")
    assert logical_geo_types(native_geo_snappy)["geometry"] == ("Geometry", EPSG_5070)
    assert geo_block_crs_id(native_geo_snappy) == EPSG_5070
    assert_conus_output_is_sound(native_geo_snappy, version="2.0")


def test_check_row_group_fix_keeps_the_native_type_and_the_crs(native_geo_tiny_row_groups):
    """#1001, through the row-group fix (``check_fixes.fix_row_groups``)."""
    before = pq.ParquetFile(str(native_geo_tiny_row_groups)).metadata.num_row_groups
    assert before > 1, "fixture does not have row groups to merge"

    _run_cli("check", "row-group", native_geo_tiny_row_groups, "--fix")

    after = pq.ParquetFile(str(native_geo_tiny_row_groups)).metadata.num_row_groups
    assert after == 1, "the fix did not run"
    assert (geo_version(native_geo_tiny_row_groups) or "").startswith("2.0")
    assert logical_geo_types(native_geo_tiny_row_groups)["geometry"] == ("Geometry", EPSG_5070)
    assert geo_block_crs_id(native_geo_tiny_row_groups) == EPSG_5070
    assert_conus_output_is_sound(native_geo_tiny_row_groups, version="2.0")


def test_an_explicit_version_still_wins_and_still_states_the_crs(native_geo_snappy, tmp_path):
    """The witness informs auto mode; it does not override a caller who asked.

    1.1 has nowhere but the ``geo`` block to record a CRS, so this is also the
    case where the witness is the only thing standing between EPSG:5070 and a
    file that declares no CRS at all -- the quieter half of #993, one command on.
    """
    from geoparquet_io.core.check_fixes import fix_compression

    out = tmp_path / "explicit_1_1.parquet"

    fix_compression(str(native_geo_snappy), str(out), geoparquet_version="1.1")

    assert (geo_version(out) or "").startswith("1.1")
    assert logical_geo_types(out) == {}, "1.1 forbids a native Parquet geo type"
    assert geo_block_crs_id(out) == EPSG_5070
    assert_conus_output_is_sound(out, version="1.1")


def test_check_all_fix_leaves_a_native_geo_only_file_native_geo_only(native_geo_snappy):
    """The control for ``check all``, which names the version from its own checks.

    ``get_geoparquet_version_from_check_results`` answers ``parquet-geo-only``
    here, and an explicit answer wins, so the output keeps no ``geo`` key at all
    -- with its CRS still in the logical type, which is the only place that file
    shape has to put one.
    """
    _run_cli("check", "all", native_geo_snappy, "--fix")

    assert _geometry_compression(native_geo_snappy) == "ZSTD", "the fix did not run"
    assert geo_block(native_geo_snappy) is None
    assert logical_geo_types(native_geo_snappy)["geometry"] == ("Geometry", EPSG_5070)
    assert_conus_output_is_sound(native_geo_snappy, version=None)


def test_removing_a_bbox_column_keeps_the_crs_it_is_removed_from(projected_conus, tmp_path):
    """The third rewrite, ``check_fixes.fix_bbox_removal``.

    ``add bbox`` writes native 2.0 -- a ``geo`` block *and* a Parquet logical
    type, both naming EPSG:5070 -- and declares the column in a ``covering``,
    which ``check bbox`` calls optimal and leaves alone. Strip the covering and
    the column is *undeclared*, which is the 2.0 shape ``--fix`` removes it from.

    A guard rather than a reproduction: it passes with or without the witness,
    because DuckDB reads the CRS off the logical type and restates it in both
    places. The test below is the one the witness is load-bearing for.
    """
    with_bbox = tmp_path / "v2_with_bbox.parquet"
    _run_cli("add", "bbox", projected_conus, with_bbox)
    assert "bbox" in pq.read_schema(str(with_bbox)).names
    assert (geo_version(with_bbox) or "").startswith("2.0")
    _strip_covering(with_bbox)
    assert _covering(with_bbox) is None
    fixed = tmp_path / "removed.parquet"

    _run_cli("check", "bbox", with_bbox, "--fix", "--fix-output", fixed)

    # `read_schema` and not `ParquetFile.schema`: the latter lists Parquet
    # *leaves*, so a bbox struct shows up as xmin/ymin/xmax/ymax and the column
    # name this asserts on never appears at all.
    assert "bbox" not in pq.read_schema(str(fixed)).names, "the fix did not run"
    assert (geo_version(fixed) or "").startswith("2.0")
    assert logical_geo_types(fixed)["geometry"] == ("Geometry", EPSG_5070)
    assert geo_block_crs_id(fixed) == EPSG_5070
    assert_conus_output_is_sound(fixed, version="2.0")


def test_removing_a_bbox_column_keeps_a_crs_only_the_geo_block_states(tmp_path, _conus_5070_rows):
    """The input the witness at ``fix_bbox_removal`` exists for.

    A 2.0 file whose ``geo`` block says EPSG:5070 while its Parquet logical type
    carries no CRS at all. ``check spec`` fails it on ``v2_crs_consistency`` --
    but it is a file people have, and the rewrite that removes its undeclared
    bbox column does not carry the block (``original_metadata=None``), so the
    output's ``crs`` has exactly one source: the witness. Measured with the
    ``input_file=`` removed, the output said **nothing** in both places -- 5070
    metres labelled OGC:CRS84, ``coordinates outside valid range for CRS`` --
    the #945 shape, a fix that mislabels the data it was asked to repair.
    """
    import geoarrow.pyarrow as ga

    no_crs = write_native_geo_only(
        tmp_path / "no_crs.parquet", _conus_5070_rows, {"geometry": (2, ga.wkb())}
    )
    with_bbox = tmp_path / "block_says_5070.parquet"
    _run_cli("add", "bbox", no_crs, with_bbox)
    assert "bbox" in pq.read_schema(str(with_bbox)).names
    _declare_geo_block(with_bbox, crs_epsg=5070)
    assert geo_block_crs_id(with_bbox) == EPSG_5070
    assert logical_geo_types(with_bbox) == {"geometry": ("Geometry", None)}
    assert _covering(with_bbox) is None, "a declared bbox is 'optimal' and never removed"
    fixed = tmp_path / "removed.parquet"

    _run_cli("check", "bbox", with_bbox, "--fix", "--fix-output", fixed)

    assert "bbox" not in pq.read_schema(str(fixed)).names, "the fix did not run"
    assert (geo_version(fixed) or "").startswith("2.0")
    assert geo_block_crs_id(fixed) == EPSG_5070
    assert logical_geo_types(fixed)["geometry"] == ("Geometry", EPSG_5070)
    assert_conus_output_is_sound(fixed, version="2.0")


# ---------------------------------------------------------------------------
# #1003: the composition `add bbox` -> `check all --fix`
# ---------------------------------------------------------------------------


def test_add_bbox_then_check_all_fix_keeps_the_covering(projected_conus, tmp_path):
    """The composition #1003 asks for, end to end.

    ``add bbox`` writes native 2.0 with a ``covering``; ``check all --fix`` then
    rewrites the file for compression. Before this it wrote a 2.0 block built
    from nothing -- no covering -- and its own bbox check then failed the file:
    ``Bbox column 'bbox' is not declared in the 'covering' metadata``.
    """
    with_bbox = tmp_path / "with_bbox.parquet"
    _run_cli("add", "bbox", projected_conus, with_bbox, "--compression", "snappy")

    declared = _covering(with_bbox)
    assert declared, "`add bbox` did not declare a covering -- nothing to lose"
    assert (geo_version(with_bbox) or "").startswith("2.0")

    output = _run_cli("check", "all", with_bbox, "--fix")

    assert _geometry_compression(with_bbox) == "ZSTD", "the fix did not run"
    assert _covering(with_bbox) == declared
    assert "Some issues remain after fixes" not in output
    assert (geo_version(with_bbox) or "").startswith("2.0")
    assert logical_geo_types(with_bbox)["geometry"] == ("Geometry", EPSG_5070)
    assert geo_block_crs_id(with_bbox) == EPSG_5070
    assert_conus_output_is_sound(with_bbox, version="2.0", covering=True)


def test_the_bbox_column_the_fix_leaves_behind_is_still_declared(projected_conus, tmp_path):
    """The symptom as the user meets it: gpio's own bbox check, run again.

    ``check spec`` is not the only oracle #1003 named -- ``check bbox`` is the
    check that printed the complaint, so it gets asked directly.
    """
    with_bbox = tmp_path / "with_bbox.parquet"
    _run_cli("add", "bbox", projected_conus, with_bbox, "--compression", "snappy")
    _run_cli("check", "all", with_bbox, "--fix")

    output = _run_cli("check", "bbox", with_bbox)

    assert "not declared in 'covering'" not in output
    assert "Bbox covering column 'bbox' declared" in output
    assert_conus_output_is_sound(with_bbox, version="2.0", covering=True)


def test_a_1_1_input_keeps_its_version_and_its_covering(projected_conus, tmp_path):
    """The control: a 1.x file is repaired as 1.1, not upgraded behind the user.

    A fix that quietly upgrades the file a user asked gpio to *repair* is a
    defect of the same family as losing its CRS.
    """
    one_one = tmp_path / "v1_1.parquet"
    _run_cli(
        "convert",
        "geoparquet",
        projected_conus,
        one_one,
        "--geoparquet-version",
        "1.1",
        "--compression",
        "snappy",
    )
    declared = _covering(one_one)
    assert declared, "fixture has no covering to preserve"

    _run_cli("check", "all", one_one, "--fix")

    assert _geometry_compression(one_one) == "ZSTD", "the fix did not run"
    assert (geo_version(one_one) or "").startswith("1.1")
    assert logical_geo_types(one_one) == {}, "a 1.1 output must not carry a native geo type"
    assert _covering(one_one) == declared
    assert geo_block_crs_id(one_one) == EPSG_5070
    assert_conus_output_is_sound(one_one, version="1.1", covering=True)
