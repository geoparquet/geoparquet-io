"""A ``covering.bbox`` is only declared over a struct that may legally be one (#1035).

GeoParquet 1.1 fixes a bbox column's field order (``xmin, ymin, xmax, ymax``, or
the six-field Z form) and types (FLOAT/DOUBLE). Overture writes ``xmin, xmax,
ymin, ymax``: a legal column, and at 1.0 a valid file. Every gpio write that
invents a covering used to test the struct by membership, so rewriting such a
file at 1.1 declared a covering gpio's own ``check spec`` rejects.

The rule pinned here, at the one gate every writer now goes through: a struct
in a legal shape is declared; one that is not is carried through undeclared
and said so, once per process; a covering the *input* declared over an illegal
struct is dropped, and said so. The column itself is never reordered behind the
user -- ``gpio add bbox --force`` is the command that does that.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.geo_metadata import (
    _note_undeclarable_bbox_column,
    bbox_column_to_declare,
    bbox_covering_problem,
)
from tests.fix_output_oracle import (
    CRS84,
    assert_fix_output_is_sound,
    covering_of,
    run_cli,
    spec_failures,
)

UNSORTED_ROWS = 1445
OVERTURE_ORDER = ["xmin", "xmax", "ymin", "ymax"]
SPEC_ORDER = ["xmin", "ymin", "xmax", "ymax"]


def bbox_field_names(path: Path, column: str = "bbox") -> list[str]:
    return [f.name for f in pq.read_schema(str(path)).field(column).type]


def _with_bbox_fields(
    table: pa.Table, order: list[str], dtype: pa.DataType | None = None
) -> pa.Table:
    """*table* with its ``bbox`` struct rebuilt in *order* (and optionally retyped)."""
    bbox = table.column("bbox").combine_chunks()
    arrays = [bbox.field(name) if name in bbox.type.names else bbox.field("xmin") for name in order]
    if dtype is not None:
        arrays = [a.cast(dtype, safe=False) for a in arrays]
    rebuilt = pa.StructArray.from_arrays(arrays, names=order)
    out = table.set_column(table.schema.get_field_index("bbox"), "bbox", rebuilt)
    return out.replace_schema_metadata(table.schema.metadata)


def _write_with_defects(table: pa.Table, target: Path) -> Path:
    """SNAPPY in tiny row groups, so every rewrite fix has work to do."""
    pq.write_table(table, str(target), compression="SNAPPY", row_group_size=50)
    return target


def _rewrite_geo(source: Path, target: Path, **changes: object) -> Path:
    table = pq.read_table(str(source))
    metadata = dict(table.schema.metadata or {})
    block = json.loads(metadata[b"geo"])
    block.update(changes)
    metadata[b"geo"] = json.dumps(block).encode("utf-8")
    return _write_with_defects(table.replace_schema_metadata(metadata), target)


@pytest.fixture
def overture_v1_0(unsorted_test_file, tmp_path) -> Path:
    """``tests/data/unsorted.parquet`` as shipped: 1.0, bbox in Overture's order, valid."""
    target = tmp_path / "overture_v10.parquet"
    shutil.copy2(str(unsorted_test_file), target)
    assert bbox_field_names(target) == OVERTURE_ORDER
    assert spec_failures(target) == {}, "the fixture must be clean, or nothing below is proved"
    return target


@pytest.fixture
def overture_v1_1(unsorted_test_file, tmp_path) -> Path:
    """The same column at 1.1 with no covering: also valid, and version-preserving
    cannot help it -- it is already at the version it will be written at."""
    target = _rewrite_geo(
        Path(str(unsorted_test_file)), tmp_path / "overture_v11.parquet", version="1.1.0"
    )
    assert covering_of(target) is None
    assert spec_failures(target) == {}, "the fixture must be clean, or nothing below is proved"
    return target


@pytest.fixture
def spec_order_v1_0(unsorted_test_file, tmp_path) -> Path:
    """The control: the same file with its bbox struct in the spec's order."""
    table = _with_bbox_fields(pq.read_table(str(unsorted_test_file)), SPEC_ORDER)
    target = _write_with_defects(table, tmp_path / "spec_order_v10.parquet")
    assert spec_failures(target) == {}
    return target


@pytest.fixture
def spec_order_v1_1(spec_order_v1_0, tmp_path) -> Path:
    return _rewrite_geo(spec_order_v1_0, tmp_path / "spec_order_v11.parquet", version="1.1.0")


HONDURAS_ROWS = 1350


@pytest.fixture
def declared_illegal_v1_1(tmp_path) -> Path:
    """A 1.1 file that already *declares* a covering over an Overture-order struct.

    ``tests/data/country_partition/Honduras.parquet`` is that shape as shipped
    (written by an earlier gpio) and ``check spec`` rejects it; rewritten here
    with every other defect too, so each fix has work to do.
    """
    table = pq.read_table("tests/data/country_partition/Honduras.parquet")
    order = np.random.RandomState(0).permutation(table.num_rows)
    target = _write_with_defects(table.take(pa.array(order)), tmp_path / "declared.parquet")
    assert bbox_field_names(target) == OVERTURE_ORDER
    assert covering_of(target)
    assert "covering_bbox_structure_geometry" in spec_failures(target)
    return target


@pytest.fixture(autouse=True)
def _fresh_warnings():
    """The gate warns once per process; each test starts from none."""
    _note_undeclarable_bbox_column.cache_clear()


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


class TestTheGate:
    @pytest.mark.parametrize(
        ("names", "types", "problem"),
        [
            (SPEC_ORDER, None, None),
            (["xmin", "ymin", "zmin", "xmax", "ymax", "zmax"], None, None),
            (SPEC_ORDER, ["double"] * 4, None),
            (SPEC_ORDER, ["float", "float", "double", "double"], None),
            (OVERTURE_ORDER, None, "in that order"),
            (["ymin", "xmin", "xmax", "ymax"], None, "in that order"),
            (["xmin", "ymin", "xmax"], None, "in that order"),
            (SPEC_ORDER + ["srid"], None, "in that order"),
            ([], None, "in that order"),
            (None, None, "is not a struct"),
            (SPEC_ORDER, ["int32"] * 4, "requires FLOAT or DOUBLE"),
        ],
        ids=[
            "spec",
            "spec-3d",
            "double",
            "mixed-floats",
            "overture",
            "swapped",
            "short",
            "extra",
            "empty",
            "not-struct",
            "int32",
        ],
    )
    def test_order_and_type_decide(self, names, types, problem):
        verdict = bbox_covering_problem("bbox", names, types)
        if problem is None:
            assert verdict is None
        else:
            assert problem in verdict

    def test_a_legal_struct_is_declared_and_an_illegal_one_warned_once(self, caplog):
        legal = pa.schema([("bbox", pa.struct([(n, pa.float32()) for n in SPEC_ORDER]))])
        overture = pa.schema([("bbox", pa.struct([(n, pa.float32()) for n in OVERTURE_ORDER]))])

        assert bbox_column_to_declare(legal) == "bbox"
        with caplog.at_level("WARNING", logger="geoparquet_io"):
            assert bbox_column_to_declare(overture) is None
            assert bbox_column_to_declare(overture) is None
        assert caplog.text.count("Not declaring a 'covering'") == 1
        assert "gpio add bbox --force" in caplog.text


# ---------------------------------------------------------------------------
# Every write path, on a defective input: valid out, covering only where legal
# ---------------------------------------------------------------------------

#: ``(id, command line factory)``. Each writes *source* to *out* through a different route to the gate.
WRITE_PATHS = [
    (
        "check-compression",
        lambda src, out: ["check", "compression", src, "--fix", "--fix-output", out],
    ),
    ("check-row-group", lambda src, out: ["check", "row-group", src, "--fix", "--fix-output", out]),
    (
        "check-spatial",
        lambda src, out: [
            "check",
            "spatial",
            src,
            "--fix",
            "--fix-output",
            out,
            "--random-sample-size",
            20,
        ],
    ),
    (
        "check-all",
        lambda src, out: [
            "check",
            "all",
            src,
            "--fix",
            "--fix-output",
            out,
            "--random-sample-size",
            20,
        ],
    ),
    (
        "convert-1.1",
        lambda src, out: ["convert", "geoparquet", src, out, "--geoparquet-version", "1.1"],
    ),
    ("sort-hilbert", lambda src, out: ["sort", "hilbert", src, out]),
    (
        "extract-streaming",
        lambda src, out: ["extract", "geoparquet", src, out, "--write-strategy", "streaming"],
    ),
    (
        "sort-1.1-geoarrow",
        lambda src, out: ["sort", "hilbert", src, out, "--geoparquet-version", "1.1-geoarrow"],
    ),
]
WRITE_IDS = [name for name, _ in WRITE_PATHS]


class TestEveryWritePath:
    @pytest.mark.parametrize(("path_id", "argv"), WRITE_PATHS, ids=WRITE_IDS)
    @pytest.mark.parametrize(
        ("shape", "expects_covering"), [("overture", False), ("spec_order", True)]
    )
    def test_output_is_valid_and_declares_only_a_legal_covering(
        self, request, shape, expects_covering, path_id, argv, tmp_path, caplog
    ):
        # `check all --fix` keeps the input's version, so it is handed the 1.1
        # twins; the rewrite paths upgrade 1.0 to 1.1 themselves.
        version = "v1_1" if path_id == "check-all" else "v1_0"
        source = request.getfixturevalue(f"{shape}_{version}")
        out = tmp_path / f"{path_id}.parquet"

        with caplog.at_level("WARNING", logger="geoparquet_io"):
            output = run_cli(*argv(source, out))

        assert "No fix needed" not in output, "the fix declined, so nothing was measured"
        assert_fix_output_is_sound(
            out,
            expected_rows=UNSORTED_ROWS,
            expected_crs=CRS84,
            expects_covering=expects_covering,
            expected_version_prefix="1.1",
        )
        # Never reordered behind the user.
        assert bbox_field_names(out) == bbox_field_names(source)
        assert ("Not declaring a 'covering'" in caplog.text) is not expects_covering

    @pytest.mark.parametrize(("path_id", "argv"), WRITE_PATHS[:6], ids=WRITE_IDS[:6])
    def test_a_covering_the_input_declared_over_an_illegal_struct_is_dropped_and_said(
        self, declared_illegal_v1_1, path_id, argv, tmp_path, caplog
    ):
        """Both write facades agree: gpio does not carry a covering its validator rejects."""
        out = tmp_path / f"{path_id}.parquet"

        with caplog.at_level("WARNING", logger="geoparquet_io"):
            output = run_cli(*argv(declared_illegal_v1_1, out))

        assert "No fix needed" not in output
        assert pq.read_metadata(str(out)).num_rows == HONDURAS_ROWS
        assert spec_failures(out) == {}, "the illegal covering was carried through"
        assert covering_of(out) is None
        assert bbox_field_names(out) == OVERTURE_ORDER
        assert "Dropping the 'covering' declared over" in caplog.text

    def test_the_python_api_write(
        self, overture_v1_0, spec_order_v1_0, declared_illegal_v1_1, tmp_path
    ):
        import geoparquet_io as gpio

        gpio.read(str(overture_v1_0)).write(str(tmp_path / "a.parquet"), geoparquet_version="1.1")
        gpio.read(str(spec_order_v1_0)).write(str(tmp_path / "b.parquet"), geoparquet_version="1.1")
        gpio.read(str(declared_illegal_v1_1)).write(str(tmp_path / "c.parquet"))

        for name in ("a", "b", "c"):
            assert spec_failures(tmp_path / f"{name}.parquet") == {}
        assert covering_of(tmp_path / "a.parquet") is None
        assert covering_of(tmp_path / "b.parquet"), (
            "the control lost its covering -- the gate is too wide"
        )
        assert covering_of(tmp_path / "c.parquet") is None

    @pytest.mark.parametrize(
        ("order", "dtype", "declared"),
        [
            (OVERTURE_ORDER, None, False),
            (SPEC_ORDER[:3], None, False),
            (SPEC_ORDER, pa.int32(), False),
            (SPEC_ORDER, None, True),
        ],
        ids=["overture", "short", "int32", "spec"],
    )
    def test_the_arrow_table_writer(self, unsorted_test_file, order, dtype, declared, tmp_path):
        """``write_geoparquet_table`` reaches the gate from an Arrow table, not a file."""
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        table = _with_bbox_fields(pq.read_table(str(unsorted_test_file)), order, dtype)
        out = tmp_path / "table.parquet"

        write_geoparquet_table(table, str(out), geoparquet_version="1.1")

        assert bool(covering_of(out)) is declared
        assert spec_failures(out) == {}

    def test_a_partition_write_warns_once_not_once_per_file(self, overture_v1_0, tmp_path, caplog):
        with caplog.at_level("WARNING", logger="geoparquet_io"):
            run_cli("partition", "string", overture_v1_0, tmp_path / "parts", "--column", "version")

        files = list((tmp_path / "parts").rglob("*.parquet"))
        assert len(files) > 1
        assert caplog.text.count("Not declaring a 'covering'") == 1
        for written in files:
            assert spec_failures(written) == {}


# ---------------------------------------------------------------------------
# The paths that report or refuse rather than write
# ---------------------------------------------------------------------------


class TestCheckBbox:
    def test_an_undeclarable_column_is_the_issue_not_a_missing_covering(self, overture_v1_1):
        from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

        result = check_metadata_and_bbox(str(overture_v1_1), return_results=True, quiet=True)

        assert result["passed"] is False
        assert result["fix_available"] is False
        assert result["cannot_declare_covering"] is True
        assert any("in that order" in issue for issue in result["issues"]), result["issues"]
        assert not any("missing metadata covering" in issue for issue in result["issues"])

    def test_a_declared_illegal_covering_is_not_called_proper(self, declared_illegal_v1_1):
        """``passed=False`` with an empty issue list, and a green tick, was the old answer."""
        from geoparquet_io.core.bbox_structure import check_bbox_structure
        from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

        result = check_metadata_and_bbox(
            str(declared_illegal_v1_1), return_results=True, quiet=True
        )
        output = run_cli("check", "bbox", declared_illegal_v1_1)
        structure = check_bbox_structure(str(declared_illegal_v1_1))

        assert structure["status"] == "suboptimal", structure
        assert "declares a 'covering'" in structure["message"]
        assert result["passed"] is False
        assert any("Covering declared over" in issue for issue in result["issues"]), result[
            "issues"
        ]
        assert "proper metadata covering" not in output
        assert "gpio add bbox --force" in output

    def test_fix_declines_without_calling_the_file_optimal(self, overture_v1_1):
        before = pq.read_schema(str(overture_v1_1))

        output = run_cli("check", "bbox", overture_v1_1, "--fix")

        assert "No fix available" in output, output
        assert "optimal" not in output, output
        assert pq.read_schema(str(overture_v1_1)).equals(before), "the file was rewritten"

    def test_check_all_fix_does_not_call_it_optimal_either(self, overture_v1_1, tmp_path):
        """The second pass, once every other defect is repaired."""
        fixed = tmp_path / "all.parquet"
        run_cli(
            "check",
            "all",
            overture_v1_1,
            "--fix",
            "--fix-output",
            fixed,
            "--random-sample-size",
            20,
        )

        output = run_cli("check", "all", fixed, "--fix", "--random-sample-size", 20)

        assert "already optimal" not in output, output
        assert "Nothing --fix can repair here" in output, output


class TestAddBboxMetadata:
    """Declaring the covering is this command's whole job, so it refuses rather than skips."""

    def test_the_cli_refuses_and_names_the_rewrite(self, overture_v1_1):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        result = CliRunner().invoke(cli, ["add", "bbox-metadata", str(overture_v1_1)])

        assert result.exit_code != 0, result.output
        assert "in that order" in result.output
        assert f"gpio add bbox --force {overture_v1_1}" in result.output
        assert covering_of(overture_v1_1) is None, "the file was modified anyway"
        assert spec_failures(overture_v1_1) == {}

    def test_the_table_api_refuses_and_its_hint_actually_works(self, overture_v1_1, tmp_path):
        import geoparquet_io as gpio

        table = gpio.read(str(overture_v1_1))
        with pytest.raises(ValueError, match=r"in that order.*gpio\.read\(\.\.\.\)\.add_bbox\(\)"):
            table.add_bbox_metadata()

        repaired = table.add_bbox().add_bbox_metadata()
        repaired.write(str(tmp_path / "repaired.parquet"), geoparquet_version="1.1")
        assert spec_failures(tmp_path / "repaired.parquet") == {}
        assert covering_of(tmp_path / "repaired.parquet")

    def test_it_still_declares_a_spec_order_column(self, spec_order_v1_0, tmp_path):
        at_1_1 = _rewrite_geo(spec_order_v1_0, tmp_path / "spec_v11.parquet", version="1.1.0")

        run_cli("add", "bbox-metadata", at_1_1)

        assert covering_of(at_1_1), "the control lost its covering too"
        assert spec_failures(at_1_1) == {}

    def test_add_bbox_without_force_points_at_force(self, overture_v1_1, tmp_path):
        output = run_cli("add", "bbox", overture_v1_1, tmp_path / "copied.parquet")

        assert "use --force to rewrite it" in output, output
        assert "add bbox-metadata" not in output, output
