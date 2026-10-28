"""Every ``gpio check`` subcommand shares one exit-code contract (#1241, #1091).

* a **failure** -- a spec violation, a file that is not GeoParquet at all, data a
  reader would mis-read -- exits 1;
* an advisory **warning** -- "GeoParquet 2.0 is available", a non-ZSTD codec, a
  row-group size outside the band -- exits 0, and 1 under ``--strict``;
* a clean file exits 0 with or without ``--strict``;
* an input that matches no files -- a glob that resolves to nothing, an empty
  directory -- exits 1, with or without ``--fix``.

Before this, six of the eight subcommands printed a failure and exited 0, so
``gpio check all`` could not gate a CI build.
"""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.check_verdict import (
    Verdict,
    exit_code,
    result_verdict,
    severity,
    validation_verdict,
    worst,
)
from geoparquet_io.core.parquet_footer import patch_footer_kv

DATA = Path(__file__).parent / "data"
PLACES = DATA / "places_test.parquet"  # GeoParquet 1.0, SNAPPY, unsorted-enough
BUILDINGS = DATA / "buildings_test.parquet"  # GeoParquet 1.0, ZSTD, no bbox column
UNSORTED = DATA / "unsorted.parquet"

#: The subcommands that resolve their input through the shared file runner.
RUNNER_SUBCOMMANDS = ["all", "bbox", "compression", "optimization", "row-group", "spatial"]
FIXABLE_SUBCOMMANDS = ["all", "bbox", "compression", "row-group", "spatial"]


def _invoke(*args):
    return CliRunner().invoke(cli, ["check", *map(str, args)])


def _geo(path):
    return json.loads(pq.ParquetFile(str(path)).metadata.metadata[b"geo"])


def _refooter(src, dest, mutate):
    geo = _geo(src)
    mutate(geo)
    patch_footer_kv(str(src), {"geo": json.dumps(geo)}, output_file=str(dest))
    return str(dest)


@pytest.fixture(scope="module")
def module_tmp(tmp_path_factory):
    return tmp_path_factory.mktemp("check_exit_codes")


@pytest.fixture(scope="module")
def nogeo(module_tmp):
    """The issue's reproduction: a valid file with its ``geo`` key removed."""
    out = module_tmp / "nogeo.parquet"
    table = pq.read_table(PLACES)
    meta = dict(table.schema.metadata or {})
    meta.pop(b"geo", None)
    pq.write_table(table.replace_schema_metadata(meta), out)
    return str(out)


@pytest.fixture(scope="module")
def clean(module_tmp):
    """GeoParquet 2.0, Hilbert-sorted, ZSTD: passes every check."""
    out = module_tmp / "clean.parquet"
    result = CliRunner().invoke(
        cli, ["sort", "hilbert", str(PLACES), str(out), "--geoparquet-version", "2.0"]
    )
    assert result.exit_code == 0, result.output
    return str(out)


@pytest.fixture(scope="module")
def tiny_row_groups(module_tmp):
    """GeoParquet with 100-row groups: a row-group *warning*, nothing worse."""
    out = module_tmp / "tiny_row_groups.parquet"
    pq.write_table(pq.read_table(PLACES), out, row_group_size=100)
    return str(out)


@pytest.fixture(scope="module")
def illegal_covering(module_tmp):
    """A 1.0 file declaring the 1.1-only ``covering`` key: ``check spec`` rejects it."""

    def mutate(geo):
        geo["columns"][geo["primary_column"]]["covering"] = {
            "bbox": {
                "xmin": ["bbox", "xmin"],
                "ymin": ["bbox", "ymin"],
                "xmax": ["bbox", "xmax"],
                "ymax": ["bbox", "ymax"],
            }
        }

    return _refooter(PLACES, module_tmp / "illegal_covering.parquet", mutate)


@pytest.fixture(scope="module")
def bboxless_covering(module_tmp):
    """The #954 shape: a 1.1 covering with no ``bbox`` member; geopandas cannot open it."""

    def mutate(geo):
        geo["version"] = "1.1.0"
        geo["columns"][geo["primary_column"]]["covering"] = {
            "h3": {"column": "h3", "resolution": 9}
        }

    return _refooter(BUILDINGS, module_tmp / "bboxless.parquet", mutate)


@pytest.fixture(scope="module")
def bad_projjson(module_tmp):
    """A PROJJSON ``crs`` with no ``type`` member: a spec failure no structure check sees."""

    def mutate(geo):
        geo["columns"][geo["primary_column"]]["crs"] = {"name": "not really a crs"}

    return _refooter(PLACES, module_tmp / "bad_projjson.parquet", mutate)


@pytest.fixture(scope="module")
def spec_warning(module_tmp):
    """An explicit null CRS: ``check spec`` warns and fails nothing."""

    def mutate(geo):
        geo["columns"][geo["primary_column"]]["crs"] = None

    return _refooter(PLACES, module_tmp / "null_crs.parquet", mutate)


class TestVerdictHelpers:
    """The one place the severity and the exit code are decided."""

    def test_exit_code_table(self):
        assert exit_code(Verdict.PASS, strict=False) == 0
        assert exit_code(Verdict.PASS, strict=True) == 0
        assert exit_code(Verdict.WARNING, strict=False) == 0
        assert exit_code(Verdict.WARNING, strict=True) == 1
        assert exit_code(Verdict.FAILURE, strict=False) == 1
        assert exit_code(Verdict.FAILURE, strict=True) == 1

    def test_an_explicit_severity_wins(self):
        assert result_verdict({"passed": False, "severity": "failure"}) is Verdict.FAILURE
        assert result_verdict({"passed": False, "severity": "warning"}) is Verdict.WARNING
        assert result_verdict({"passed": True, "severity": "pass"}) is Verdict.PASS

    def test_an_unlabelled_miss_is_a_warning(self):
        """Conservative default: a check that did not say it failed only warns."""
        assert result_verdict({"passed": False}) is Verdict.WARNING
        assert result_verdict({"passed": True}) is Verdict.PASS
        assert result_verdict({}) is Verdict.PASS
        assert result_verdict(None) is Verdict.PASS

    def test_an_unknown_severity_is_refused(self):
        with pytest.raises(ValueError, match="severity"):
            result_verdict({"passed": False, "severity": "fatal"})

    def test_combined_results_are_the_worst(self):
        from geoparquet_io.core.check_verdict import combine_results

        results = [
            {"passed": True},
            {"passed": False},
            {"passed": False, "severity": "failure"},
            3,
        ]
        assert result_verdict(combine_results(results)) is Verdict.FAILURE
        assert result_verdict(combine_results([{"passed": False}])) is Verdict.WARNING
        assert result_verdict(combine_results([])) is Verdict.PASS

    def test_validation_verdict(self):
        assert validation_verdict(failed=1, warnings=5) is Verdict.FAILURE
        assert validation_verdict(failed=0, warnings=2) is Verdict.WARNING
        assert validation_verdict(failed=0, warnings=0) is Verdict.PASS

    def test_label(self):
        assert [v.label for v in Verdict] == ["pass", "warning", "failure"]

    def test_severity(self):
        assert severity(True) == "pass"
        assert severity(True, failure=True) == "pass"
        assert severity(False) == "warning"
        assert severity(False, failure=True) == "failure"

    def test_worst(self):
        assert worst([]) is Verdict.PASS
        assert worst([Verdict.WARNING, Verdict.PASS]) is Verdict.WARNING


class TestPythonVerdict:
    """``CheckResult.verdict()`` is the Python side of the exit-code contract."""

    def test_check_all_on_warnings_only(self):
        import geoparquet_io as gpio

        assert gpio.read(str(PLACES)).check().verdict() == "warning"

    def test_validate(self, spec_warning, bad_projjson, clean):
        import geoparquet_io as gpio

        assert gpio.read(spec_warning).validate().verdict() == "warning"
        assert gpio.read(bad_projjson).validate().verdict() == "failure"
        assert gpio.read(clean).validate().verdict() == "pass"

    def test_stac(self, tmp_path):
        from geoparquet_io import validate_stac

        missing = tmp_path / "missing.json"
        missing.write_text(json.dumps({"type": "Feature"}), encoding="utf-8")

        assert validate_stac(str(missing)).verdict() == "failure"


class TestStacVerdict:
    def test_a_validator_that_could_not_run_is_a_warning(self):
        from geoparquet_io.core.stac_check import stac_verdict

        results = {
            "valid": False,
            "errors": ["STAC validation unavailable: no jsonschema"],
            "warnings": [],
        }
        assert stac_verdict(results) is Verdict.WARNING
        assert stac_verdict({"valid": True, "errors": [], "warnings": []}) is Verdict.PASS


class TestNotGeoParquet:
    """The issue's table: a file with no ``geo`` key fails every subcommand."""

    @pytest.mark.parametrize("subcommand", [*RUNNER_SUBCOMMANDS, "spec"])
    def test_exits_1(self, subcommand, nogeo):
        result = _invoke(subcommand, nogeo)

        assert result.exit_code == 1, result.output
        assert "✗" in result.output
        assert "❌" not in result.output

    def test_the_failure_names_the_problem(self, nogeo):
        result = _invoke("row-group", nogeo)

        assert "Not a GeoParquet file" in result.output, result.output

    def test_verbose_says_so_too(self, nogeo):
        result = _invoke("row-group", nogeo, "--verbose")

        assert result.exit_code == 1, result.output
        assert "✗ Not a GeoParquet file" in result.output, result.output

    def test_the_core_checks_call_it_a_failure_themselves(self, nogeo, monkeypatch):
        """For callers that skip the CLI's up-front test (the Python API)."""
        from geoparquet_io.core import check_parquet_structure as structure

        assert structure.check_metadata_and_bbox(nogeo, return_results=True)["severity"] == (
            "failure"
        )

        monkeypatch.setattr(structure, "find_primary_geometry_column", lambda *a, **k: None)
        assert structure.check_compression(nogeo, return_results=True)["severity"] == "failure"

    @pytest.mark.parametrize("subcommand", ["bbox", "compression"])
    def test_fix_declines_and_still_exits_1(self, subcommand, nogeo, module_tmp):
        out = module_tmp / f"nogeo_fixed_{subcommand}.parquet"

        result = _invoke(subcommand, nogeo, "--fix", "--fix-output", out)

        assert result.exit_code == 1, result.output
        assert not out.exists(), "a file that is not GeoParquet was rewritten"


class TestClean:
    @pytest.mark.parametrize("subcommand", [*RUNNER_SUBCOMMANDS, "spec"])
    @pytest.mark.parametrize("strict", [False, True])
    def test_exits_0(self, subcommand, strict, clean):
        result = _invoke(subcommand, clean, *(["--strict"] if strict else []))

        assert result.exit_code == 0, result.output


#: (subcommand, fixture name) pairs whose only findings are advisory.
WARNING_CASES = [
    ("all", "places"),
    ("bbox", "buildings"),  # no bbox column on a 1.x file
    ("bbox", "places"),  # version 1.0 is outdated
    ("compression", "places"),  # SNAPPY
    ("optimization", "places"),
    ("row-group", "tiny_row_groups"),  # "poor" is still a recommendation
    ("spatial", "unsorted"),
    ("spec", "spec_warning"),  # used to exit 2
]


@pytest.fixture
def named_input(tiny_row_groups, spec_warning):
    return {
        "places": str(PLACES),
        "buildings": str(BUILDINGS),
        "unsorted": str(UNSORTED),
        "tiny_row_groups": tiny_row_groups,
        "spec_warning": spec_warning,
    }


class TestWarningsOnly:
    @pytest.mark.parametrize("subcommand,name", WARNING_CASES)
    def test_exits_0(self, subcommand, name, named_input):
        result = _invoke(subcommand, named_input[name])

        assert result.exit_code == 0, result.output

    @pytest.mark.parametrize("subcommand,name", WARNING_CASES)
    def test_strict_exits_1(self, subcommand, name, named_input):
        result = _invoke(subcommand, named_input[name], "--strict")

        assert result.exit_code == 1, result.output
        assert "--strict" in result.stderr, result.stderr

    def test_no_bbox_column_is_a_warning_glyph_not_a_failure_glyph(self):
        result = _invoke("bbox", BUILDINGS)

        assert "No bbox column found" in result.output
        assert "✗" not in result.output, result.output


class TestFailures:
    def test_check_bbox_fails_a_covering_the_version_does_not_allow(self, illegal_covering):
        result = _invoke("bbox", illegal_covering)

        assert result.exit_code == 1, result.output
        assert "✗" in result.output

    def test_check_bbox_fix_declines_and_still_exits_1(self, illegal_covering, module_tmp):
        """No repair is on offer for this file, so the failure stands."""
        out = module_tmp / "illegal_fixed.parquet"

        result = _invoke("bbox", illegal_covering, "--fix", "--fix-output", out)

        assert result.exit_code == 1, result.output
        assert not out.exists()

    def test_check_all_fails_a_covering_no_reader_can_open(self, bboxless_covering):
        result = _invoke("all", bboxless_covering)

        assert result.exit_code == 1, result.output

    def test_check_all_fix_that_repairs_the_failure_exits_0(self, bboxless_covering, tmp_path):
        out = tmp_path / "fixed.parquet"

        result = _invoke("all", bboxless_covering, "--fix", "--fix-output", out)

        assert result.exit_code == 0, result.output
        assert out.exists()

    def test_check_all_counts_a_spec_warning_as_a_warning(self, spec_warning):
        assert _invoke("all", spec_warning).exit_code == 0
        strict = _invoke("all", spec_warning, "--strict")

        assert strict.exit_code == 1, strict.output
        assert "Spec validation: 1 warnings" in strict.output, strict.output

    def test_check_all_fails_when_only_spec_fails(self, bad_projjson):
        """``check all`` folds the spec verdict in, not just the structure checks."""
        assert _invoke("bbox", bad_projjson).exit_code == 0
        assert _invoke("spec", bad_projjson).exit_code == 1

        result = _invoke("all", bad_projjson)

        assert result.exit_code == 1, result.output


class TestFixRecheck:
    """A successful ``--fix`` is judged on the file it wrote."""

    def test_compression_fix_rechecks_the_output(self, tmp_path):
        out = tmp_path / "zstd.parquet"

        result = _invoke("compression", PLACES, "--fix", "--fix-output", out, "--strict")

        # SNAPPY was the only finding; after the rewrite it is gone, so even
        # --strict has nothing to exit 1 for.
        assert result.exit_code == 0, result.output

    def test_a_fix_that_leaves_a_warning_still_trips_strict(self, tmp_path, monkeypatch):
        from geoparquet_io.core import check_fixes

        def no_op_fix(input_path, output_path, verbose=False, profile=None):
            Path(output_path).write_bytes(Path(input_path).read_bytes())

        monkeypatch.setattr(check_fixes, "fix_compression", no_op_fix)
        out = tmp_path / "still_snappy.parquet"

        result = _invoke("compression", PLACES, "--fix", "--fix-output", out, "--strict")

        assert result.exit_code == 1, result.output


class TestCheckCouldNotRun:
    """A check that could not run is a failure in the verdict, never a pass (#624)."""

    @pytest.mark.parametrize("strict", [False, True])
    def test_check_all_fails_when_spec_validation_cannot_run(self, clean, strict, monkeypatch):
        from geoparquet_io.core import validate

        def boom(*args, **kwargs):
            raise RuntimeError("validator exploded")

        monkeypatch.setattr(validate, "validate_geoparquet", boom)

        result = _invoke("all", clean, *(["--strict"] if strict else []))

        assert result.exit_code == 1, result.output
        assert "Spec validation could not run" in result.output
        assert "validator exploded" in result.output
        # A failure, not a warning: --strict has no reason to say it decided.
        assert "because --strict is set" not in result.output

    def test_all_checks_result_without_spec_is_a_failure(self):
        from geoparquet_io.core.check_verdict import all_checks_result, could_not_run

        outcome = all_checks_result(
            {}, {"passed": True}, could_not_run("Spec validation", "validator exploded")
        )

        assert outcome["severity"] == Verdict.FAILURE.label
        assert outcome["passed"] is False
        assert "Spec validation could not run, validator exploded" in outcome["issues"]

    def test_an_errored_file_stays_failed_after_a_clean_recheck(self):
        from geoparquet_io.cli.commands.check import MultiFileCheckRunner
        from geoparquet_io.core.check_verdict import could_not_run

        runner = MultiFileCheckRunner(["a.parquet"])
        runner.record_result("a.parquet", could_not_run("Spatial order check", "unreadable"))
        assert runner.verdicts["a.parquet"] is Verdict.FAILURE
        assert runner.failed == 1

        runner.recheck("a.parquet", "fixed.parquet", lambda path: {"passed": True})

        assert runner.verdicts["a.parquet"] is Verdict.FAILURE


class TestMultiFile:
    @pytest.fixture
    def mixed_dir(self, tmp_path, clean, nogeo):
        d = tmp_path / "mixed"
        d.mkdir()
        (d / "a_clean.parquet").write_bytes(Path(clean).read_bytes())
        (d / "b_nogeo.parquet").write_bytes(Path(nogeo).read_bytes())
        return d

    @pytest.fixture
    def warning_dir(self, tmp_path):
        d = tmp_path / "warnings"
        d.mkdir()
        for name in ("a.parquet", "b.parquet"):
            (d / name).write_bytes(PLACES.read_bytes())
        return d

    @pytest.mark.parametrize("subcommand", RUNNER_SUBCOMMANDS)
    def test_one_failing_file_fails_the_run(self, subcommand, mixed_dir):
        result = _invoke(subcommand, mixed_dir, "--all-files")

        assert result.exit_code == 1, result.output
        assert "1 failed" in result.output, result.output

    def test_a_fix_that_writes_an_unreadable_file_fails_the_run(
        self, warning_dir, tmp_path, monkeypatch
    ):
        """The re-check of a fixed file is guarded like the check: recorded, then exit 1."""
        from geoparquet_io.core import check_fixes

        def garbage_fix(input_path, output_path, verbose=False, profile=None):
            Path(output_path).write_bytes(b"not parquet")

        monkeypatch.setattr(check_fixes, "fix_compression", garbage_fix)
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        result = _invoke("compression", warning_dir, "--fix", "--fix-output", out_dir)

        assert result.exit_code == 1, result.output
        assert "Re-check could not run" in result.output, result.output

    def test_warnings_across_files_exit_0(self, warning_dir):
        result = _invoke("compression", warning_dir, "--all-files")

        assert result.exit_code == 0, result.output
        assert "2 warnings" in result.output, result.output

    def test_warnings_across_files_strict_exit_1(self, warning_dir):
        result = _invoke("compression", warning_dir, "--all-files", "--strict")

        assert result.exit_code == 1, result.output

    def test_glob_aggregates_too(self, mixed_dir):
        result = _invoke("bbox", f"{mixed_dir}/*.parquet", "--all-files")

        assert result.exit_code == 1, result.output


class TestZeroMatches:
    """#1091: a glob that matches nothing is not a success, with or without --fix."""

    @pytest.fixture
    def empty_dir(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()
        return d

    @pytest.mark.parametrize("subcommand", RUNNER_SUBCOMMANDS)
    def test_glob_with_no_matches(self, subcommand, tmp_path):
        result = _invoke(subcommand, f"{tmp_path}/nothing/*.parquet")

        assert result.exit_code == 1, result.output
        assert "No parquet files found" in result.stderr, result.stderr

    @pytest.mark.parametrize("subcommand", FIXABLE_SUBCOMMANDS)
    def test_glob_with_no_matches_and_fix(self, subcommand, tmp_path):
        result = _invoke(subcommand, f"{tmp_path}/*.parquet", "--fix")

        assert result.exit_code == 1, result.output
        assert "No parquet files found" in result.stderr, result.stderr
        assert "nothing was fixed" in result.stderr, result.stderr

    @pytest.mark.parametrize("subcommand", RUNNER_SUBCOMMANDS)
    def test_empty_directory(self, subcommand, empty_dir):
        result = _invoke(subcommand, empty_dir)

        assert result.exit_code == 1, result.output
        assert "No parquet files found" in result.stderr, result.stderr

    @pytest.mark.parametrize("subcommand", FIXABLE_SUBCOMMANDS)
    def test_empty_directory_with_fix(self, subcommand, empty_dir):
        result = _invoke(subcommand, empty_dir, "--fix")

        assert result.exit_code == 1, result.output


class TestStac:
    @pytest.fixture
    def item(self, tmp_path):
        from datetime import datetime, timezone

        import pystac

        def write(*, self_link: bool, drop: str | None = None):
            item = pystac.Item(
                id="t",
                geometry={"type": "Point", "coordinates": [0, 0]},
                bbox=[0, 0, 0, 0],
                datetime=datetime(2024, 1, 1, tzinfo=timezone.utc),
                properties={},
            )
            if self_link:
                item.add_link(pystac.Link("self", "./item.json", "application/json"))
            item.add_asset(
                "data",
                pystac.Asset(
                    "s3://bucket/t.parquet",
                    media_type="application/vnd.apache.parquet",
                    roles=["data"],
                ),
            )
            body = item.to_dict()
            if drop:
                body.pop(drop)
            path = tmp_path / f"item_{self_link}_{drop}.json"
            path.write_text(json.dumps(body), encoding="utf-8")
            return str(path)

        return write

    def test_clean(self, item):
        path = item(self_link=True)
        assert _invoke("stac", path).exit_code == 0
        assert _invoke("stac", path, "--strict").exit_code == 0

    def test_warning_only(self, item):
        path = item(self_link=False)

        assert _invoke("stac", path).exit_code == 0
        strict = _invoke("stac", path, "--strict")
        assert strict.exit_code == 1, strict.output

    def test_failure(self, item):
        result = _invoke("stac", item(self_link=True, drop="bbox"))

        assert result.exit_code == 1, result.output


# =============================================================================
# Review fixes on #1252
# =============================================================================

PGO_SNAPPY = DATA / "fields_pgo_5070_snappy.parquet"  # parquet-geo-only, SNAPPY, spec-failing


def _missing_column(tmp_path):
    """A 1.1 file whose ``geo.primary_column`` names a column it does not have."""
    import pyarrow as pa

    geo = {
        "version": "1.1.0",
        "primary_column": "nope",
        "columns": {"nope": {"encoding": "WKB", "geometry_types": ["Point"]}},
    }
    table = pa.table({"id": [1, 2]}).replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    path = tmp_path / "missing_column.parquet"
    pq.write_table(table, path)
    return str(path)


class TestOneCouldNotRunMechanism:
    """A check that could not run states its own failure; the CLI does not re-derive it."""

    def test_could_not_run_is_a_failure_result(self):
        from geoparquet_io.core.check_verdict import could_not_run

        result = could_not_run("Spatial order check", "unreadable")

        assert result_verdict(result) is Verdict.FAILURE
        assert result["error"] == "unreadable"
        assert result["issues"] == ["Spatial order check could not run, unreadable"]
        assert result["fix_available"] is False

    def test_spatial_core_stamps_its_own_failure(self, tmp_path):
        """The Python API reads the same verdict the CLI exits on."""
        from geoparquet_io.api.check import CheckResult
        from geoparquet_io.core.check_spatial_order import check_spatial_order

        result = check_spatial_order(_missing_column(tmp_path), 10, 100, False, return_results=True)

        assert result["severity"] == "failure"
        assert "❌" not in " ".join(result["issues"])
        assert CheckResult(result, "spatial").verdict() == "failure"

    def test_check_spatial_fix_fails_when_the_fixed_file_cannot_be_sampled(
        self, tmp_path, monkeypatch
    ):
        from geoparquet_io.cli.commands import check as cli_check
        from geoparquet_io.core.check_spatial_order import _errored_results

        out = tmp_path / "sorted.parquet"
        real = cli_check.check_spatial_impl

        def unreadable_output(path, *args, **kwargs):
            if Path(path) == out:
                return _errored_results("could not read the fixed file")
            return real(path, *args, **kwargs)

        monkeypatch.setattr(cli_check, "check_spatial_impl", unreadable_output)

        result = _invoke("spatial", UNSORTED, "--fix", "--fix-output", out)

        assert out.exists(), result.output
        assert result.exit_code == 1, result.output
        assert "could not read the fixed file" in result.output

    def test_check_all_fix_cannot_vouch_for_a_check_that_never_ran(self, tmp_path, monkeypatch):
        """Spatial order could not run on the input; a clean re-check of the output is not enough."""
        from geoparquet_io.cli.commands import check as cli_check

        real = cli_check.check_spatial_impl

        def unreadable_input(path, *args, **kwargs):
            if Path(path) == PLACES:
                raise RuntimeError("spatial exploded")
            return real(path, *args, **kwargs)

        monkeypatch.setattr(cli_check, "check_spatial_impl", unreadable_input)
        out = tmp_path / "fixed.parquet"

        result = _invoke("all", PLACES, "--fix", "--fix-output", out)

        assert out.exists(), result.output
        assert result.exit_code == 1, result.output
        assert "spatial exploded" in result.output

    @pytest.mark.parametrize("subcommand", ["compression", "row-group", "bbox", "optimization"])
    def test_single_file_check_that_raises_is_reported_not_a_traceback(
        self, subcommand, monkeypatch
    ):
        from geoparquet_io.core import check_optimization, check_parquet_structure

        def boom(*args, **kwargs):
            raise ValueError("boom")

        target = {
            "compression": (check_parquet_structure, "check_compression"),
            "row-group": (check_parquet_structure, "check_row_groups"),
            "bbox": (check_parquet_structure, "check_bbox"),
            "optimization": (check_optimization, "check_optimization"),
        }[subcommand]
        monkeypatch.setattr(*target, boom)

        result = _invoke(subcommand, BUILDINGS)

        assert result.exit_code == 1, result.output
        assert isinstance(result.exception, SystemExit), repr(result.exception)
        assert "could not run" in result.output
        assert "boom" in result.output

    def test_single_file_spatial_check_that_raises_is_reported(self, monkeypatch):
        from geoparquet_io.cli.commands import check as cli_check

        def boom(*args, **kwargs):
            raise ValueError("boom")

        monkeypatch.setattr(cli_check, "check_spatial_impl", boom)

        result = _invoke("spatial", BUILDINGS)

        assert result.exit_code == 1, result.output
        assert isinstance(result.exception, SystemExit), repr(result.exception)
        assert "could not run" in result.output

    def test_a_raising_check_is_not_called_a_failed_fix(self, tmp_path, monkeypatch):
        from geoparquet_io.core import check_parquet_structure

        def boom(*args, **kwargs):
            raise ValueError("boom")

        monkeypatch.setattr(check_parquet_structure, "check_compression", boom)
        d = tmp_path / "two"
        d.mkdir()
        for name in ("a.parquet", "b.parquet"):
            (d / name).write_bytes(BUILDINGS.read_bytes())

        result = _invoke("compression", d, "--all-files")

        assert result.exit_code == 1, result.output
        assert "Failed to fix" not in result.output
        assert "2 failed" in result.output


class TestPerIssueSeverity:
    """Each printed issue carries its own check's verdict, not the file's worst (#1239)."""

    def test_issue_severity_overrides_per_issue(self):
        from geoparquet_io.core.check_verdict import ISSUE_SEVERITY_KEY, labelled_issues

        result = {
            "passed": False,
            "severity": "warning",
            "issues": ["advice", "defect"],
            ISSUE_SEVERITY_KEY: {"defect": "failure"},
        }

        assert labelled_issues(result) == [
            (Verdict.WARNING, "advice"),
            (Verdict.FAILURE, "defect"),
        ]
        # The result is as bad as its worst issue.
        assert result_verdict(result) is Verdict.FAILURE

    def test_combine_keeps_each_results_level(self):
        from geoparquet_io.core.check_verdict import combine_results, labelled_issues

        combined = combine_results(
            [
                {"passed": False, "issues": ["SNAPPY"]},
                {"passed": False, "severity": "failure", "issues": ["bad crs"]},
                {"passed": True},
            ]
        )

        assert result_verdict(combined) is Verdict.FAILURE
        assert labelled_issues(combined) == [
            (Verdict.WARNING, "SNAPPY"),
            (Verdict.FAILURE, "bad crs"),
        ]

    def test_check_all_prints_snappy_as_a_warning_and_the_spec_failure_as_an_error(self):
        result = _invoke("all", PGO_SNAPPY)

        assert result.exit_code == 1, result.output
        lines = result.output.splitlines()
        snappy = [line for line in lines if "SNAPPY" in line and PGO_SNAPPY.name in line]
        spec = [line for line in lines if "Spec validation:" in line and PGO_SNAPPY.name in line]
        assert snappy and all("⚠ WARNING" in line for line in snappy), result.output
        assert spec and all("✗ ERROR" in line for line in spec), result.output


class TestPythonBuckets:
    """``failures()``/``warnings()`` bucket by severity, as ``verdict()`` does."""

    def test_a_warning_is_not_listed_as_a_failure(self):
        from geoparquet_io.api.check import CheckResult

        result = CheckResult({"passed": False, "issues": ["SNAPPY"]}, "compression")

        assert result.verdict() == "warning"
        assert result.failures() == []
        assert result.warnings() == ["SNAPPY"]

    def test_check_all_buckets_by_severity(self):
        import geoparquet_io as gpio

        result = gpio.read(str(PGO_SNAPPY)).check()

        assert result.verdict() == "warning"
        assert result.failures() == []
        assert any("SNAPPY" in w for w in result.warnings())

    def test_mixed_issue_levels(self):
        from geoparquet_io.api.check import CheckResult
        from geoparquet_io.core.check_verdict import ISSUE_SEVERITY_KEY

        result = CheckResult(
            {
                "passed": False,
                "issues": ["advice", "defect"],
                ISSUE_SEVERITY_KEY: {"defect": "failure"},
            },
            "bbox",
        )

        assert result.failures() == ["defect"]
        assert result.warnings() == ["advice"]


class TestSpecJson:
    def test_json_carries_the_verdict(self, spec_warning, bad_projjson, clean):
        for path, expected in (
            (spec_warning, "warning"),
            (bad_projjson, "failure"),
            (clean, "pass"),
        ):
            result = _invoke("spec", path, "--json")
            assert json.loads(result.output)["verdict"] == expected, result.output


class TestSpecZeroMatches:
    def test_glob_with_no_matches(self, tmp_path):
        result = _invoke("spec", f"{tmp_path}/nothing/*.parquet")

        assert result.exit_code == 1, result.output
        assert "No parquet files found" in result.stderr, result.stderr

    def test_empty_directory(self, tmp_path):
        result = _invoke("spec", tmp_path)

        assert result.exit_code == 1, result.output
        assert "No parquet files found" in result.stderr, result.stderr


class TestSamplingIsHonest:
    """Without --all-files a directory is judged by its first file, and the run says so."""

    @pytest.fixture
    def good_then_bad(self, tmp_path, clean, nogeo):
        d = tmp_path / "md"
        d.mkdir()
        (d / "a.parquet").write_bytes(Path(clean).read_bytes())
        (d / "b.parquet").write_bytes(Path(nogeo).read_bytes())
        return d

    def test_sampled_run_says_the_verdict_covers_only_the_first_file(self, good_then_bad):
        result = _invoke("all", good_then_bad)

        assert result.exit_code == 0, result.output
        assert "covers 1 of 2 files" in result.stderr, result.stderr
        assert "--all-files" in result.stderr

    def test_all_files_judges_every_file(self, good_then_bad):
        result = _invoke("all", good_then_bad, "--all-files")

        assert result.exit_code == 1, result.output
        assert "covers" not in result.stderr


class TestEveryGlyphCounts:
    """A ⚠ on stdout is a warning --strict sees; information is not marked ⚠."""

    def test_parquet_geo_only_notice_is_information(self):
        result = _invoke("bbox", PGO_SNAPPY)

        assert result.exit_code == 0, result.output
        assert "No GeoParquet metadata" in result.output
        assert "⚠" not in result.output, result.output
        assert _invoke("bbox", PGO_SNAPPY, "--strict").exit_code == 0

    def test_native_file_pushdown_is_information(self, clean):
        result = _invoke("spatial", clean)

        assert "Pushdown" in result.output
        assert "⚠" not in result.output, result.output

    def test_low_pushdown_efficiency_is_a_warning_strict_sees(self, monkeypatch, clean):
        from geoparquet_io.core import check_spatial_order

        def low(*args, **kwargs):
            return {
                "has_geo_bbox": True,
                "num_row_groups": 8,
                "estimated_skip_rate": 0.1,
                "avg_bbox_area_ratio": 0.9,
                "passed": False,
                "issues": ["Low spatial filter pushdown efficiency (estimated skip rate: 10%)"],
                "recommendations": [],
            }

        monkeypatch.setattr(check_spatial_order, "check_spatial_pushdown_readiness", low)

        assert _invoke("spatial", clean).exit_code == 0
        strict = _invoke("spatial", clean, "--strict")
        assert strict.exit_code == 1, strict.output
        assert "Low pushdown efficiency" in strict.output

    def test_no_bbox_column_on_a_1x_file_is_a_pushdown_warning(self):
        result = _invoke("spatial", BUILDINGS, "--strict")

        assert result.exit_code == 1, result.output
        assert "No geo_bbox column found" in result.output


class TestFixMessages:
    def test_unfixable_failure_does_not_say_no_fix_needed(self, illegal_covering, tmp_path):
        out = tmp_path / "illegal_fixed.parquet"

        result = _invoke("bbox", illegal_covering, "--fix", "--fix-output", out)

        assert result.exit_code == 1, result.output
        assert "No fix needed" not in result.output
        assert "✗ No automatic fix available" in result.output
        assert not out.exists()


class TestCheckBboxCovering:
    def test_check_bbox_fails_a_covering_with_no_bbox_member(self, bboxless_covering):
        result = _invoke("bbox", bboxless_covering)

        assert result.exit_code == 1, result.output
        assert "no 'bbox' member" in result.output or "declares no 'bbox'" in result.output


class TestFixOutputAlwaysWritten:
    """``--fix-output`` names a file a pipeline expects, fixed or not."""

    @pytest.mark.parametrize(
        "subcommand,source",
        [("compression", BUILDINGS), ("row-group", BUILDINGS), ("all", None)],
    )
    def test_nothing_to_fix_still_writes_the_output(self, subcommand, source, clean, tmp_path):
        src = Path(source or clean)
        out = tmp_path / "out.parquet"

        result = _invoke(subcommand, src, "--fix", "--fix-output", out)

        assert result.exit_code == 0, result.output
        assert out.exists(), result.output
        assert out.read_bytes() == src.read_bytes()

    def test_existing_output_needs_overwrite(self, tmp_path):
        out = tmp_path / "out.parquet"
        out.write_bytes(b"old")

        refused = _invoke("compression", BUILDINGS, "--fix", "--fix-output", out)
        assert refused.exit_code != 0, refused.output
        assert out.read_bytes() == b"old"

        allowed = _invoke("compression", BUILDINGS, "--fix", "--fix-output", out, "--overwrite")
        assert allowed.exit_code == 0, allowed.output
        assert out.read_bytes() == BUILDINGS.read_bytes()


class TestHelpNamesTheContract:
    def test_group_help_mentions_exit_codes_and_strict(self):
        result = CliRunner().invoke(cli, ["check", "--help"])

        assert "--strict" in result.output
        assert "exit" in result.output.lower()
