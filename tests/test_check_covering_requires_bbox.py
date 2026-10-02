"""``check spec`` fails a ``covering`` with no ``bbox`` member; ``--fix`` drops it.

Regression tests for #1173. gpio stopped *writing* such a covering in #954, but
the files gpio 1.6 and earlier wrote with ``gpio add h3/s2/a5/quadkey/kdtree``
or ``gpio partition <index> --keep-*-column`` over a bbox-less input are still
out there, and until now ``gpio check spec`` passed them (24 passed, 0 failed)
while ``geopandas.read_parquet`` raised ``KeyError: 'bbox'`` on the same file.

The spec text (GeoParquet 1.1, ``covering``) is explicit: "The keys of the
'covering' object MUST be a supported encoding. Currently the only supported
encoding is 'bbox'." An index entry *beside* a bbox member is gpio's own
deliberate extension (#694/#738) and stays valid; a covering with no bbox member
at all is the broken shape.
"""

import json

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.check_fixes import fix_bboxless_covering
from geoparquet_io.core.parquet_footer import patch_footer_kv
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet

H3_ENTRY = {"column": "h3", "resolution": 9}
BBOX_PATHS = {
    "xmin": ["bbox", "xmin"],
    "ymin": ["bbox", "ymin"],
    "xmax": ["bbox", "xmax"],
    "ymax": ["bbox", "ymax"],
}


def _kv(path):
    return pq.ParquetFile(str(path)).metadata.metadata or {}


def _geo(path):
    kv = _kv(path)
    assert b"geo" in kv, f"{path} carries no 'geo' key; keys: {sorted(kv)}"
    return json.loads(kv[b"geo"].decode("utf-8"))


def _covering(path):
    geo = _geo(path)
    return geo["columns"][geo["primary_column"]].get("covering")


def _refooter(src, dest, covering, version="1.1.0"):
    """*src* with *covering* on its primary column, data pages copied verbatim."""
    geo = _geo(src)
    geo["version"] = version
    column = geo["columns"][geo["primary_column"]]
    if covering is None:
        column.pop("covering", None)
    else:
        column["covering"] = covering
    patch_footer_kv(str(src), {"geo": json.dumps(geo)}, output_file=str(dest))
    return str(dest)


def _failed(result):
    return [c for c in result.checks if c.status == CheckStatus.FAILED]


def _named(result, name):
    matches = [c for c in result.checks if c.name == name]
    assert matches, f"no check named {name}; got {sorted(c.name for c in result.checks)}"
    return matches[0]


@pytest.fixture
def bboxless(buildings_test_file, tmp_path):
    """The #954 shape: a 1.1 file whose only covering member is an h3 entry."""
    return _refooter(buildings_test_file, tmp_path / "bboxless.parquet", {"h3": H3_ENTRY})


@pytest.fixture
def with_bbox_column(buildings_test_file, tmp_path):
    """A legal 1.1 file: real bbox column, covering with a bbox member."""
    out = tmp_path / "with_bbox.parquet"
    result = CliRunner().invoke(cli, ["add", "bbox", buildings_test_file, str(out)])
    assert result.exit_code == 0, result.output
    assert "bbox" in (_covering(out) or {}), _covering(out)
    return str(out)


@pytest.fixture
def declarable(with_bbox_column, tmp_path):
    """The repairable shape: a spec-order ``bbox`` struct the covering forgot.

    The file carries a real, legal, conventionally named bbox column *and* a
    real quadkey column, and its ``covering`` declares only the quadkey entry --
    the bbox member is missing, but it is supplyable from the file itself
    rather than lost. This is what ``gpio 1.6 add quadkey`` wrote over an input
    whose bbox column was undeclared.
    """
    indexed = tmp_path / "indexed.parquet"
    result = CliRunner().invoke(cli, ["add", "quadkey", with_bbox_column, str(indexed)])
    assert result.exit_code == 0, result.output
    entry = (_covering(indexed) or {}).get("quadkey")
    assert entry, _covering(indexed)
    return _refooter(indexed, tmp_path / "declarable.parquet", {"quadkey": entry})


class TestCheckSpec:
    def test_a_covering_with_no_bbox_member_fails(self, bboxless):
        result = validate_geoparquet(bboxless, validate_data=False)

        assert [c.name for c in _failed(result)] == ["covering_has_bbox_geometry"]
        assert "bbox" in _failed(result)[0].message

    def test_geopandas_is_why_it_fails_rather_than_warns(self, bboxless):
        """The premise of the verdict: a reader cannot open the file at all."""
        gpd = pytest.importorskip("geopandas")

        with pytest.raises(KeyError, match="bbox"):
            gpd.read_parquet(bboxless)

    def test_the_cli_exits_1(self, bboxless):
        result = CliRunner().invoke(cli, ["check", "spec", bboxless])

        assert result.exit_code == 1, result.output
        assert "covering" in result.output

    def test_an_index_entry_beside_a_bbox_member_still_passes(self, with_bbox_column, tmp_path):
        """gpio writes h3/s2/quadkey entries deliberately; only a missing bbox is wrong."""
        both = _refooter(
            with_bbox_column, tmp_path / "both.parquet", {"bbox": BBOX_PATHS, "h3": H3_ENTRY}
        )

        result = validate_geoparquet(both, validate_data=False)

        assert _failed(result) == []
        assert _named(result, "covering_has_bbox_geometry").status == CheckStatus.PASSED

    def test_a_file_with_no_covering_is_not_judged(self, buildings_test_file, tmp_path):
        none = _refooter(buildings_test_file, tmp_path / "none.parquet", None)

        result = validate_geoparquet(none, validate_data=False)

        assert _failed(result) == []
        assert _named(result, "covering_has_bbox_geometry").status == CheckStatus.SKIPPED

    def test_a_covering_that_is_not_an_object_is_reported_once(self, buildings_test_file, tmp_path):
        """``covering_is_object`` owns that verdict; this check declines to repeat it."""
        wrong = _refooter(buildings_test_file, tmp_path / "wrong.parquet", "bbox")

        result = validate_geoparquet(wrong, validate_data=False)

        assert [c.name for c in _failed(result)] == ["covering_is_object_geometry"]
        assert _named(result, "covering_has_bbox_geometry").status == CheckStatus.SKIPPED

    def test_a_1_0_file_is_not_judged_on_a_1_1_key(self, buildings_test_file, tmp_path):
        """``covering`` is a 1.1 concept; a 1.0 file is told its version is wrong instead."""
        old = _refooter(
            buildings_test_file, tmp_path / "old.parquet", {"h3": H3_ENTRY}, version="1.0.0"
        )

        result = validate_geoparquet(old, validate_data=False)

        assert "covering_has_bbox_geometry" not in [c.name for c in result.checks]


class TestFixDropsIt:
    def test_the_covering_is_gone_and_the_rest_of_the_block_survives(self, bboxless, tmp_path):
        out = tmp_path / "fixed.parquet"

        summary = fix_bboxless_covering(bboxless, str(out))

        assert summary["success"] is True
        assert summary["fix_applied"] is not None
        geo = _geo(out)
        column = geo["columns"]["geometry"]
        assert "covering" not in column
        assert column["encoding"] == "WKB"
        assert column["bbox"] == _geo(bboxless)["columns"]["geometry"]["bbox"]
        assert geo["version"] == "1.1.0"
        assert pq.read_table(str(out)).num_rows == pq.read_table(bboxless).num_rows

    def test_the_fixed_file_validates_clean_and_opens_in_geopandas(self, bboxless, tmp_path):
        gpd = pytest.importorskip("geopandas")
        out = tmp_path / "fixed.parquet"

        fix_bboxless_covering(bboxless, str(out))

        assert _failed(validate_geoparquet(str(out), validate_data=False)) == []
        assert len(gpd.read_parquet(str(out))) > 0

    def test_a_legal_covering_is_left_exactly_as_it_was(self, with_bbox_column, tmp_path):
        legal = _refooter(
            with_bbox_column, tmp_path / "legal.parquet", {"bbox": BBOX_PATHS, "h3": H3_ENTRY}
        )
        before = _covering(legal)
        out = tmp_path / "untouched.parquet"

        summary = fix_bboxless_covering(legal, str(out))

        assert summary["fix_applied"] is None
        assert _covering(legal) == before
        assert not out.exists(), "a file with nothing to repair was rewritten anyway"

    def test_other_footer_keys_are_kept(self, buildings_test_file, tmp_path):
        """A metadata-only repair must not drop the keys beside ``geo``."""
        with_extra = tmp_path / "extra.parquet"
        patch_footer_kv(
            buildings_test_file, {"stac:collection": "mine"}, output_file=str(with_extra)
        )
        bad = _refooter(with_extra, tmp_path / "bad.parquet", {"h3": H3_ENTRY})
        out = tmp_path / "fixed.parquet"

        fix_bboxless_covering(bad, str(out))

        assert _kv(out)[b"stac:collection"] == b"mine"

    def test_a_footer_that_cannot_be_patched_falls_back_to_a_rewrite(
        self, bboxless, tmp_path, monkeypatch
    ):
        """The write funnels apply the same gate, so the fallback drops it too."""
        from geoparquet_io.core import check_fixes
        from geoparquet_io.core.parquet_footer import FooterPatchUnsupported

        def refuse(*args, **kwargs):
            raise FooterPatchUnsupported("pretend this footer is unreadable")

        monkeypatch.setattr(check_fixes, "patch_footer_kv", refuse)
        out = tmp_path / "rewritten.parquet"

        fix_bboxless_covering(bboxless, str(out))

        assert "covering" not in _geo(out)["columns"]["geometry"]
        assert pq.read_table(str(out)).num_rows == pq.read_table(bboxless).num_rows


class TestFixSuppliesTheMissingMember:
    """When the file carries a declarable bbox column, the member is supplied.

    Dropping the covering is the repair for a file that has nothing to declare.
    A file whose ``bbox`` struct is right there, in the spec's field order and
    under the conventional name, loses a usable covering *and* its index entry
    if the repair just strips -- and the write funnels, asked the same question
    by ``bbox_column_to_declare``, would have supplied it. Both arms of the fix
    now ask that one gate, so the footer patch and the funnel rewrite cannot
    answer differently for the same bytes.
    """

    def test_the_member_is_supplied_and_the_index_entry_survives(self, declarable, tmp_path):
        out = tmp_path / "fixed.parquet"

        summary = fix_bboxless_covering(declarable, str(out))

        assert summary["success"] is True
        covering = _covering(out)
        assert set(covering) == {"quadkey", "bbox"}, covering
        assert covering["bbox"] == BBOX_PATHS
        assert covering["quadkey"] == _covering(declarable)["quadkey"]

    def test_the_fix_summary_names_the_column_it_declared(self, declarable, tmp_path):
        out = tmp_path / "fixed.parquet"

        summary = fix_bboxless_covering(declarable, str(out))

        assert "bbox" in summary["fix_applied"], summary
        assert "'bbox'" in summary["fix_applied"], summary
        assert "Dropped" not in summary["fix_applied"], summary

    def test_the_rewrite_fallback_writes_the_identical_covering(
        self, declarable, tmp_path, monkeypatch
    ):
        """The divergence this fix closes: same bytes, same covering, either arm."""
        from geoparquet_io.core import check_fixes
        from geoparquet_io.core.parquet_footer import FooterPatchUnsupported

        def refuse(*args, **kwargs):
            raise FooterPatchUnsupported("pretend this footer is unreadable")

        patched = tmp_path / "patched.parquet"
        fix_bboxless_covering(declarable, str(patched))
        monkeypatch.setattr(check_fixes, "patch_footer_kv", refuse)
        rewritten = tmp_path / "rewritten.parquet"

        fix_bboxless_covering(declarable, str(rewritten))

        assert _covering(rewritten) == _covering(patched)
        assert pq.read_table(str(rewritten)).num_rows == pq.read_table(declarable).num_rows

    def test_the_repaired_file_validates_clean(self, declarable, tmp_path):
        out = tmp_path / "fixed.parquet"

        fix_bboxless_covering(declarable, str(out))

        assert _failed(validate_geoparquet(str(out), validate_data=False)) == []


class TestCheckAllFix:
    def test_check_all_reports_it(self, bboxless):
        result = CliRunner().invoke(cli, ["check", "all", bboxless])

        assert "covering" in result.output
        assert "bbox" in result.output

    def test_check_all_fix_leaves_a_file_geopandas_can_open(self, bboxless, tmp_path):
        gpd = pytest.importorskip("geopandas")
        out = tmp_path / "fixed.parquet"

        result = CliRunner().invoke(
            cli, ["check", "all", bboxless, "--fix", "--fix-output", str(out)]
        )

        assert result.exit_code == 0, result.output
        covering = _covering(out)
        assert covering is None or "bbox" in covering, covering
        assert _failed(validate_geoparquet(str(out), validate_data=False)) == []
        assert len(gpd.read_parquet(str(out))) > 0

    def test_check_all_fix_repairs_a_2_0_file_that_needs_nothing_else(
        self, buildings_test_file, tmp_path
    ):
        """The case no other fix reaches: 2.0 wants no bbox column, so nothing else runs."""
        v2 = tmp_path / "v2.parquet"
        converted = CliRunner().invoke(
            cli, ["convert", buildings_test_file, str(v2), "--geoparquet-version", "2.0"]
        )
        assert converted.exit_code == 0, converted.output
        bad = _refooter(v2, tmp_path / "v2_bad.parquet", {"h3": H3_ENTRY}, version="2.0.0")
        out = tmp_path / "v2_fixed.parquet"

        result = CliRunner().invoke(cli, ["check", "all", bad, "--fix", "--fix-output", str(out)])

        assert result.exit_code == 0, result.output
        assert _covering(out) is None
        assert _failed(validate_geoparquet(str(out), validate_data=False)) == []


class TestTheGuardsOnMalformedInput:
    """The shared predicates read a block exactly as a file may hold it, so a
    malformed one is answered rather than crashed on (#947/#1062)."""

    @pytest.mark.parametrize("col_meta", ["not-a-dict", None, 42, ["bbox"]])
    def test_a_column_entry_that_is_not_an_object_declares_no_covering(self, col_meta):
        from geoparquet_io.core.geo_metadata import covering_lacks_bbox

        assert covering_lacks_bbox(col_meta) is False

    @pytest.mark.parametrize("geo_meta", [None, "not-a-dict", 42, []])
    def test_a_block_that_is_not_an_object_declares_no_columns(self, geo_meta):
        from geoparquet_io.core.geo_metadata import bboxless_covering_columns

        assert bboxless_covering_columns(geo_meta) == []

    @pytest.mark.parametrize("columns", ["not-a-dict", ["geometry"], 42, None])
    def test_a_columns_value_that_is_not_an_object_declares_no_columns(self, columns):
        from geoparquet_io.core.geo_metadata import bboxless_covering_columns

        assert bboxless_covering_columns({"columns": columns}) == []


class TestTheRepairOnAFileThatDoesNotNeedIt:
    def test_a_legal_file_reports_nothing_to_drop_and_writes_nothing(self, tmp_path, caplog):
        """The verbose 'nothing to drop' arm: no covering, no output file."""
        import logging

        src = tmp_path / "plain.parquet"
        CliRunner().invoke(cli, ["convert", "tests/data/buildings_test.parquet", str(src)])
        out = tmp_path / "never_written.parquet"

        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            summary = fix_bboxless_covering(str(src), str(out), verbose=True)

        assert summary == {"fix_applied": None, "success": True}
        assert not out.exists()
        assert "nothing to drop" in caplog.text.lower()

    def test_the_fix_step_keeps_the_input_when_the_repair_finds_nothing(self, tmp_path):
        """`_apply_covering_fix`'s disagreement arm: the check said yes, the
        repair says no, so the pipeline keeps using the file it had."""
        from geoparquet_io.core.check_fixes import _apply_covering_fix

        src = tmp_path / "plain.parquet"
        CliRunner().invoke(cli, ["convert", "tests/data/buildings_test.parquet", str(src)])
        temp_files: list[str] = []
        # The check offers the repair, but the file carries no illegal covering:
        # the two disagree, so the pipeline must keep the file it had.
        check_results = {"covering": {"fix_available": True}}

        current, applied = _apply_covering_fix(
            check_results, str(src), temp_files, verbose=False, profile=None
        )

        assert current == str(src)
        assert applied == []


class TestTheRemoteRepairPath:
    def test_a_remote_output_takes_the_funnel_rewrite_not_the_footer_patch(
        self, bboxless, monkeypatch
    ):
        """A remote URL cannot be footer-patched in place, so the repair falls
        through to the funnel rewrite (which applies the same gate)."""
        import geoparquet_io.core.check_fixes as cf

        src = bboxless
        patched: list[str] = []
        monkeypatch.setattr(cf, "patch_footer_kv", lambda *a, **k: patched.append("patched"))
        monkeypatch.setattr(cf, "is_remote_url", lambda p: str(p).startswith("s3://"))
        rewrote: list[str] = []
        monkeypatch.setattr(
            cf, "write_parquet_with_metadata", lambda *a, **k: rewrote.append("rewrote")
        )

        summary = cf.fix_bboxless_covering(str(src), "s3://bucket/out.parquet", verbose=False)

        assert patched == [], "a remote output must not be footer-patched"
        assert rewrote == ["rewrote"]
        assert summary["success"] is True
