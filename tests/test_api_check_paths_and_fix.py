"""Path-level ``gpio.check_*`` functions (#1090) and ``Table.fix`` (#1078).

The path functions are the one call path for a check: a ``Table`` read from a
file hands them its source, so ``gpio.check_x(path)`` and
``gpio.read(path).check_x()`` cannot drift. ``Table.fix`` repairs into a new
Table and never touches the file it was read from.
"""

from __future__ import annotations

import json
import os
import shutil

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.api import check as check_api
from geoparquet_io.cli.commands import check as cli_check
from geoparquet_io.cli.main import cli
from tests.conftest import TEST_DATA_DIR

# Deterministic checks; check_spatial samples random pairs, so it is pinned by
# delegation below rather than by equal answers.
PAIRS = [
    ("check_compression", "check_compression"),
    ("check_row_groups", "check_row_groups"),
    ("check_bbox", "check_bbox"),
    ("check_bloom_filters", "check_bloom_filters"),
    ("check_optimization", "check_optimization"),
    ("validate", "validate"),
]
# Not check_spatial / check_spatial_pushdown: the path functions judge what
# `gpio check spatial` judges; the Table methods keep the bare measurements.


def test_every_public_check_wraps_the_check_table_methods_call():
    """``Table._run_check`` calls ``__wrapped__`` to skip the one-file refusal."""
    names = [n for n in gpio.__all__ if n.startswith("check_") or n == "validate"]
    assert len(names) == 9
    assert [n for n in names if not hasattr(getattr(gpio, n), "__wrapped__")] == []


@pytest.mark.parametrize(("function", "method"), PAIRS)
def test_path_function_answers_as_the_table_method(unsorted_test_file, function, method):
    path = os.path.abspath(unsorted_test_file)

    by_path = getattr(gpio, function)(path)
    by_table = getattr(gpio.read(path), method)()

    assert by_path.to_dict() == by_table.to_dict()
    assert by_path.check_type == by_table.check_type
    assert by_path.source_path == path == by_table.source_path


def test_a_table_check_goes_through_the_path_function(monkeypatch, unsorted_test_file):
    seen = []
    real = check_api._check_spatial_order

    def spy(path, **kwargs):
        seen.append((path, kwargs))
        return real(path, **kwargs)

    monkeypatch.setattr(check_api, "_check_spatial_order", spy)
    result = gpio.read(unsorted_test_file).check_spatial(sample_size=10, limit_rows=1000)

    assert seen == [(os.path.abspath(unsorted_test_file), {"sample_size": 10, "limit_rows": 1000})]
    assert result.source_path == os.path.abspath(unsorted_test_file)


@pytest.mark.parametrize(
    ("function", "command"), [("check_compression", "compression"), ("validate", "spec")]
)
@pytest.mark.parametrize("operand", ["dir", "glob"])
def test_a_directory_or_glob_is_refused(tmp_path, unsorted_test_file, function, command, operand):
    shutil.copy(unsorted_test_file, tmp_path / "a.parquet")
    path = str(tmp_path) if operand == "dir" else str(tmp_path / "*.parquet")

    with pytest.raises(gpio.InvalidParameterError, match=f"gpio check {command}"):
        getattr(gpio, function)(path)


def test_a_remote_url_goes_straight_to_core(monkeypatch):
    from geoparquet_io.core import check_parquet_structure, file_type

    url = "s3://bucket/data.parquet"
    seen = []
    # The not-GeoParquet gate probes the footer first; keep this test off the network.
    monkeypatch.setattr(
        file_type,
        "detect_geoparquet_file_type",
        lambda path, *_, **__: seen.append(path) or {"file_type": "geoparquet_v1"},
    )
    monkeypatch.setattr(
        check_parquet_structure,
        "check_compression",
        lambda path, **_: seen.append(path) or {"passed": True},
    )

    result = gpio.check_compression(url)

    assert seen == [url, url]  # the gate's probe, then the check itself, both on the URL
    assert result.source_path == url


@pytest.fixture
def unsorted_misdeclared(tmp_path, unsorted_test_file):
    """unsorted.parquet (a spatial-order problem) declaring Polygon over its Points (a spec one)."""
    table = pq.read_table(unsorted_test_file)
    geo = json.loads(table.schema.metadata[b"geo"])
    geo["columns"][geo["primary_column"]]["geometry_types"] = ["Polygon"]
    path = tmp_path / "misdeclared.parquet"
    metadata = {**table.schema.metadata, b"geo": json.dumps(geo).encode()}
    pq.write_table(table.replace_schema_metadata(metadata), path, row_group_size=100)
    return str(path)


def test_check_all_runs_what_gpio_check_all_runs(monkeypatch, unsorted_misdeclared):
    # The fixture is kilobytes: zero the spec's 5 MB floor so the spatial
    # verdict is judged rather than withheld (PORTO-FMT-058).
    from geoparquet_io.core import check_spatial_order

    monkeypatch.setattr(check_spatial_order, "SPATIAL_ORDER_MIN_FILE_BYTES", 0)
    categories = []
    real = cli_check.all_checks_result

    def spy(structure, spatial, spec):
        categories.extend([*structure, "spatial", "spec"])
        return real(structure, spatial, spec)

    monkeypatch.setattr(cli_check, "all_checks_result", spy)
    exit_code = CliRunner().invoke(cli, ["check", "all", unsorted_misdeclared]).exit_code

    result = gpio.check_all(unsorted_misdeclared)

    assert (exit_code, result.verdict()) == (1, "failure")
    assert list(result.to_dict()) == categories
    assert result.to_dict()["spatial"]["passed"] is False
    assert any(f.startswith("[spec] ") for f in result.failures())
    # Table.check() stays the structure checks only.
    structure = set(gpio.read(unsorted_misdeclared).check().to_dict())
    assert structure == set(categories) - {"spatial", "spec"}


@pytest.fixture(scope="module")
def check_fixtures(tmp_path_factory):
    """Files each check answers with something other than a plain pass."""
    test_data_dir = TEST_DATA_DIR
    tmp = tmp_path_factory.mktemp("check_parity")
    unsorted = pq.read_table(test_data_dir / "unsorted.parquet")
    # Ordered 1.x points with no bbox column: spatial order passes, pushdown warns.
    ordered = gpio.read(str(test_data_dir / "unsorted.parquet")).sort_hilbert().to_arrow()
    gpio.Table(ordered.drop_columns(["bbox"])).write(
        str(tmp / "ordered.parquet"), geoparquet_version="1.1", row_group_rows=100
    )
    # Unsorted, many row groups, declaring Polygon over its Points: spec fails.
    geo = json.loads(unsorted.schema.metadata[b"geo"])
    geo["columns"][geo["primary_column"]]["geometry_types"] = ["Polygon"]
    metadata = {**unsorted.schema.metadata, b"geo": json.dumps(geo).encode()}
    pq.write_table(
        unsorted.replace_schema_metadata(metadata), tmp / "misdeclared.parquet", row_group_size=100
    )
    pq.write_table(
        unsorted.drop_columns(["geometry"]).replace_schema_metadata({}), tmp / "plain.parquet"
    )
    return {
        "ordered_1x_no_bbox": str(tmp / "ordered.parquet"),
        "unsorted_misdeclared": str(tmp / "misdeclared.parquet"),
        # One row group, 1.0 with a bbox column: nothing for pushdown to skip.
        "single_row_group": str(test_data_dir / "places_test.parquet"),
        "not_geoparquet": str(tmp / "plain.parquet"),
    }


#: (path function, the `gpio check` leaf it twins, the block of that leaf's answer it is)
CLI_TWINS = [
    ("check_compression", "compression", None),
    ("check_row_groups", "row-group", None),
    ("check_bbox", "bbox", None),
    ("check_spatial", "spatial", None),
    ("check_optimization", "optimization", None),
    ("check_all", "all", None),
    ("validate", "spec", None),
    # No leaf of their own: each is one block of the leaf that runs it.
    ("check_spatial_pushdown", "spatial", "pushdown"),
    ("check_bloom_filters", "all", "bloom_filters"),
]


def _profile(results: dict, check_type: str) -> tuple:
    """What must match: the verdict and each category's, and how many issues land where."""
    from geoparquet_io.core.check_verdict import result_verdict

    result = check_api.CheckResult(results, check_type=check_type)
    categories = {k: result_verdict(v).label for k, v in results.items() if isinstance(v, dict)}
    if check_type == "all":
        # Its buckets are its categories', each pinned by that category's own leaf.
        return result.verdict(), categories
    buckets = (result.failures(), result.warnings(), result.recommendations())
    return result.verdict(), categories, *map(len, buckets)


def _cli_answer(monkeypatch, leaf: str, path: str) -> tuple[int, dict, dict]:
    """``gpio check <leaf> --strict``'s exit code, its results, and the last dict it judged."""
    from geoparquet_io.core.check_verdict import SEVERITY_KEY, spec_result

    judged, parts = [], {}
    record = cli_check.MultiFileCheckRunner.record_result
    monkeypatch.setattr(
        cli_check.MultiFileCheckRunner,
        "record_result",
        lambda self, f, r: judged.append(r) or record(self, f, r),
    )
    combine = cli_check.all_checks_result

    def categories(structure, spatial, spec):
        parts.update(structure, spatial=spatial, spec=spec_result(spec))
        return combine(structure, spatial, spec)

    monkeypatch.setattr(cli_check, "all_checks_result", categories)
    args = ["check", leaf, path, "--strict", *(["--json"] if leaf == "spec" else [])]
    run = CliRunner().invoke(cli, args)
    if leaf == "spec":
        report = json.loads(run.stdout)
        named = {
            s: [c["name"] for c in report["checks"] if c["status"] == s]
            for s in ("failed", "warning")
        }
        results = {SEVERITY_KEY: report["verdict"], "issues": named["failed"]}
        return run.exit_code, {**results, "warnings": named["warning"]}, results
    if leaf == "all":
        # The not-GeoParquet gate refuses the file before any check runs.
        return run.exit_code, parts or {"geoparquet": judged[-1]}, judged[-1]
    return run.exit_code, judged[-1], judged[-1]


@pytest.mark.parametrize(
    "fixture", ["ordered_1x_no_bbox", "unsorted_misdeclared", "single_row_group", "not_geoparquet"]
)
@pytest.mark.parametrize(("function", "leaf", "block"), CLI_TWINS, ids=[t[0] for t in CLI_TWINS])
def test_a_path_check_answers_as_its_cli_command(
    monkeypatch, check_fixtures, function, leaf, block, fixture
):
    """The verdict and categories of ``gpio.<function>(path)`` are ``gpio check <leaf>``'s."""
    path = check_fixtures[fixture]
    exit_code, cli_results, judged = _cli_answer(monkeypatch, leaf, path)
    if block is not None:
        # A file the gate refuses runs no check: the refusal is each block's answer.
        cli_results = judged if fixture == "not_geoparquet" else cli_results[block]

    result = getattr(gpio, function)(path)

    assert _profile(result.to_dict(), result.check_type) == _profile(cli_results, result.check_type)
    # passed() is False for a warning or a failure: the strict reading of verdict().
    assert bool(result) is result.passed() is (result.verdict() == "pass")
    # The not-GeoParquet gate labels its refusal from the function's name, as the check does.
    assert result.check_type == function.removeprefix("check_")
    if block is None:
        assert exit_code == (result.verdict() != "pass")


def _raises(message):
    def check(path, **_):
        raise RuntimeError(message)

    return check


def test_check_all_reports_a_stage_that_could_not_run(monkeypatch, unsorted_test_file):
    monkeypatch.setattr(check_api, "validate", _raises("spec reader exploded"))
    result = gpio.check_all(unsorted_test_file)

    assert result.verdict() == "failure"
    assert "spatial" in result.to_dict()
    assert any("Spec validation could not run" in f for f in result.failures())

    # An interrupt is a stop, never a check that failed.
    monkeypatch.setattr(check_api, "_check_spatial_order", _raises("Query interrupted"))
    with pytest.raises(RuntimeError, match="interrupted"):
        gpio.check_all(unsorted_test_file)


# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "url",
    [
        "https://b.s3.amazonaws.com/d/f.parquet?X-Amz-Signature=abc",
        "s3://bucket/year=2026/data.parquet",
        "s3://bucket/year=2026/data",
        "http://[::1]/data.parquet",
    ],
    ids=["presigned", "hive_prefix_object", "hive_prefix_extensionless_object", "ipv6"],
)
def test_a_single_remote_object_is_one_file(url):
    assert check_api._one_file(url, "compression") == url


@pytest.mark.parametrize(
    "url",
    [
        "s3://bucket/year=2026/",
        "s3://bucket/year=2026",
        "s3://bucket/*.parquet",
        "s3://bucket/data/*.parquet",
        "s3://bucket/data.v?.parquet",
        "http://[::1]/d/*.parquet",
    ],
    ids=[
        "prefix",
        "hive_segment_last",
        "bucket_glob",
        "glob",
        "object_store_question_mark",
        "ipv6_glob",
    ],
)
def test_a_remote_prefix_or_glob_is_still_refused(url):
    with pytest.raises(gpio.InvalidParameterError, match="directory or glob"):
        check_api._one_file(url, "compression")


# Table.fix
# --------------------------------------------------------------------------


@pytest.fixture
def unsorted_copy(tmp_path, unsorted_test_file):
    path = tmp_path / "in.parquet"
    shutil.copy(unsorted_test_file, path)
    return path


def test_fix_returns_a_fixed_table_and_leaves_the_input_alone(unsorted_copy):
    before = unsorted_copy.read_bytes()
    table = gpio.read(unsorted_copy)

    fixed = table.fix()

    assert unsorted_copy.read_bytes() == before
    assert fixed is not table
    assert fixed.source_path is None  # never a view of a scratch file
    assert fixed.num_rows == table.num_rows
    ids = fixed.to_arrow().column("id")
    assert ids != table.to_arrow().column("id")  # Hilbert-sorted


def test_fix_into_output_returns_a_view_of_the_repaired_file(tmp_path, unsorted_copy):
    out = tmp_path / "fixed.parquet"

    fixed = gpio.read(unsorted_copy).fix(output=out)

    assert fixed.source_path == os.path.abspath(out)
    assert gpio.check_compression(out).passed()
    assert gpio.check_row_groups(out).passed()


def test_fix_refuses_to_write_over_its_source(unsorted_copy):
    alias = os.path.join(os.path.dirname(unsorted_copy), ".", unsorted_copy.name)

    with pytest.raises(gpio.InvalidParameterError, match="source"):
        gpio.read(unsorted_copy).fix(output=alias)


def test_fix_refuses_its_remote_source_under_a_scheme_alias(monkeypatch, unsorted_copy):
    """``s3a://`` names the ``s3://`` object ``copy_file`` would overwrite."""
    table = gpio.read(unsorted_copy)
    monkeypatch.setattr(table, "_source_path", "s3://bucket/data.parquet")

    with pytest.raises(gpio.InvalidParameterError, match="source"):
        table.fix(output="s3a://bucket/data.parquet", overwrite=True)


def test_fix_refuses_an_unknown_check(unsorted_copy):
    with pytest.raises(gpio.InvalidParameterError, match="bloom_filters"):
        gpio.read(unsorted_copy).fix(checks=["compression", "bloom_filters"])


def test_fix_honours_the_checks_subset(tmp_path, unsorted_copy):
    out = tmp_path / "fixed.parquet"

    gpio.read(unsorted_copy).fix(checks=["compression"], output=out)

    source = pq.read_table(unsorted_copy)
    fixed = pq.read_table(out)
    assert pc.all(pc.equal(fixed.column("id"), source.column("id"))).as_py()  # not sorted
    assert gpio.check_compression(out).passed()


def test_fix_refuses_an_existing_output_unless_overwrite(tmp_path, unsorted_copy):
    """An existing output is the CLI's refusal (#1367); a derived Table cannot clobber its file."""
    taken = tmp_path / "taken.parquet"
    taken.write_bytes(b"keep me")
    before = unsorted_copy.read_bytes()

    with pytest.raises(gpio.GeoParquetError, match="overwrite=True"):
        gpio.read(unsorted_copy).fix(checks=[], output=taken)
    with pytest.raises(gpio.GeoParquetError, match="overwrite=True"):
        gpio.read(unsorted_copy).head(5).fix(checks=[], output=unsorted_copy)

    assert taken.read_bytes() == b"keep me"
    assert unsorted_copy.read_bytes() == before
    assert gpio.read(unsorted_copy).fix(checks=[], output=taken, overwrite=True).num_rows == 1445


def test_fix_to_a_remote_output_copies_through_the_object_store(monkeypatch, unsorted_copy):
    """A remote output is copied with the cloud-aware ``copy_file``, not ``shutil``."""
    from geoparquet_io.api import table as table_module
    from geoparquet_io.core import check_fixes

    landed = {}

    def fake_copy(source, dest, verbose=False):
        landed[dest] = pq.read_table(source).num_rows

    monkeypatch.setattr(check_fixes, "copy_file", fake_copy, raising=False)
    monkeypatch.setattr(table_module, "read", lambda path: path)

    gpio.read(unsorted_copy).fix(checks=["spatial"], output="s3://bucket/out.parquet")

    assert landed == {"s3://bucket/out.parquet": 1445}


def test_fix_row_groups_fuses_the_compression_rewrite(tmp_path, unsorted_copy):
    """Selecting ``row_groups`` is the one rewrite, so the output is ZSTD too."""
    out = tmp_path / "fixed.parquet"

    gpio.read(unsorted_copy).fix(checks=["row_groups"], output=out)

    meta = pq.ParquetFile(out).metadata
    assert meta.num_row_groups == 1
    assert meta.row_group(0).column(0).compression == "ZSTD"


def test_fix_covering_on_a_good_file_leaves_codec_and_row_groups(tmp_path, unsorted_copy):
    """No covering fix to apply: the SNAPPY, 15-row-group input is placed as it is."""
    out = tmp_path / "fixed.parquet"

    gpio.read(unsorted_copy).fix(checks=["covering"], output=out)

    meta = pq.ParquetFile(out).metadata
    assert meta.num_row_groups == 15
    assert meta.row_group(0).column(0).compression == "SNAPPY"


def test_a_table_read_from_a_directory_checks_it_as_main_did(tmp_path, unsorted_test_file):
    """The path functions refuse a directory; a Table read from one still hands it to core.

    Pinned from main: core measures the dataset (its first file, SNAPPY here),
    and the verdict names the directory as its source.
    """
    for name in ("a.parquet", "b.parquet"):
        shutil.copy(unsorted_test_file, tmp_path / name)
    table = gpio.read(str(tmp_path))
    compression, spec = table.check_compression(), table.validate()
    assert compression.to_dict()["current_compression"] == "SNAPPY"
    assert (compression.verdict(), spec.verdict()) == ("warning", "pass")
    for result in (compression, spec):
        assert (result.source_path, result.prospective) == (str(tmp_path), False)


def test_spec_warnings_reach_the_result(tmp_path, unsorted_test_file):
    """An explicit null CRS is a spec warning; `gpio check spec` prints it, so warnings() lists it."""
    table = pq.read_table(unsorted_test_file)
    geo = json.loads(table.schema.metadata[b"geo"])
    geo["columns"][geo["primary_column"]]["crs"] = None
    path = tmp_path / "null_crs.parquet"
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo)}), path)

    spec = gpio.validate(path)
    assert spec.verdict() == "warning"
    assert any("crs" in w.lower() for w in spec.warnings())
    assert set(spec.warnings()) <= {
        w.removeprefix("[spec] ") for w in gpio.check_all(path).warnings()
    }


@pytest.mark.parametrize("checks", [None, []], ids=["all", "none"])
def test_fix_on_a_table_read_from_a_directory_fixes_its_rows(tmp_path, unsorted_test_file, checks):
    """A dataset is fixed as one scratch write of its rows; its files are untouched."""
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    for name in ("a.parquet", "b.parquet"):
        shutil.copy(unsorted_test_file, dataset / name)
    before = {p.name: p.read_bytes() for p in dataset.iterdir()}

    fixed = gpio.read(str(dataset)).fix(checks=checks)

    assert fixed.num_rows == 2 * 1445
    assert fixed.source_path is None
    assert {p.name: p.read_bytes() for p in dataset.iterdir()} == before
