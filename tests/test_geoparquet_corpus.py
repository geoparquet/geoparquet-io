"""
Conformance tests against the official geoparquet-testing corpus.

The corpus (https://github.com/geoparquet/geoparquet-testing) is vendored as a
git submodule at tests/data/geoparquet-testing, pinned to a specific commit.
Run ``git submodule update --init`` if these tests report SKIPPED.

Three tiers:
- data/ + samples/ (51 files): valid GeoParquet 2.0.0 — gpio must detect,
  read, and validate them cleanly.
- bad_data/ (22 files): deliberate spec violations with a manifest of expected
  failure codes — gpio must reject each one for the right reason, and must
  never crash on any of them.
- Write tier: representative corpus files rewritten through gpio must keep
  their GeoParquet 2.0 nature (native logical types, CRS, geo metadata).

Adjudication policy: this suite was built from the corpus's first complete run
against gpio (July 2026). Discrepancies were judged against the spec text, not
assumed to be gpio bugs. Confirmed gpio bugs are tracked in GitHub issues and
appear here as KNOWN_VALIDATION_BUGS entries or xfail marks referencing the
issue; they turn into hard failures/XPASS as fixes land.

Corpus bugs go in CORPUS_BUG_FILES while they reproduce; once a fixed corpus
pin lands, test_validates_clean fails loudly on each stale entry so the map
gets cleaned up instead of masking future regressions. (The data/zm/
geometry_types suffix bug found in July 2026 was fixed upstream in
geoparquet-testing 87891e7 and its entries removed with the pin bump.)
"""

import json
import re
from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.duckdb_metadata import (
    detect_geometry_columns,
    get_schema_info,
    parse_geometry_logical_type,
)
from geoparquet_io.core.file_type import detect_geoparquet_file_type
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet
from tests.conftest import get_geo_metadata, get_geoparquet_version, has_native_geo_types

pytestmark = [pytest.mark.corpus, pytest.mark.integration]

CORPUS = Path(__file__).parent / "data" / "geoparquet-testing"

# Validation checks that misfire on valid corpus files, with the gpio issue
# tracking each fix. test_validates_clean xfails while a file's failures are
# all covered here and fails hard on anything new. Remove entries as fixes land.
KNOWN_VALIDATION_BUGS: dict[str, str] = {
    # Row-group geospatial statistics are misread on some platforms (all-zero
    # bbox on Windows CI, so every geometry falls "outside"); also false
    # positives on multi-row-group files whose per-group stats are correct.
    "native_geo_stats_contains_data": "gpio #603",
}


# Floors just below the current per-tier corpus counts (data=42, samples=9,
# bad_data=22 at the pinned commit). An empty or restructured submodule makes
# rglob return [] and pytest's empty-parameter-set default silently SKIPs every
# parametrized test; test_corpus_tier_minimum_files turns that into a loud
# failure. Raise these floors when the corpus grows.
CORPUS_TIER_MINIMUMS = {"data": 40, "samples": 8, "bad_data": 15}


@pytest.mark.parametrize(("tier", "minimum"), sorted(CORPUS_TIER_MINIMUMS.items()))
def test_corpus_tier_minimum_files(tier, minimum):
    """Guard: a shrunken/missing tier must fail loudly, not skip everything silently."""
    if not (CORPUS / "data").exists():
        pytest.skip("run: git submodule update --init")
    count = len(list((CORPUS / tier).rglob("*.parquet")))
    assert count >= minimum, (
        f"{tier}/ has {count} parquet files, expected >= {minimum} — the corpus "
        "submodule pin changed or is incomplete; every missing file is a "
        "conformance test that silently stopped running"
    )


# Fixtures that themselves violate the spec (upstream geoparquet-testing bugs).
# test_validates_clean xfails while an entry still reproduces and fails hard
# once it stops reproducing, forcing removal of stale entries after a pin bump.
CORPUS_BUG_FILES: dict[str, str] = {}


def _corpus_files(*subdirs):
    """Parametrize over corpus files; one graceful skip if the submodule is absent."""
    if not (CORPUS / "data").exists():
        return [
            pytest.param(
                None,
                id="submodule-missing",
                marks=pytest.mark.skip(reason="run: git submodule update --init"),
            )
        ]
    files = [f for d in subdirs for f in sorted((CORPUS / d).rglob("*.parquet"))]
    return [pytest.param(f, id=f.relative_to(CORPUS).as_posix()) for f in files]


def _failed_checks(result):
    return [c.name for c in result.checks if c.status == CheckStatus.FAILED]


def _known_bug_refs(failed_names):
    """Issue refs covering the given failed check names, or None if any is uncovered."""
    refs = set()
    for name in failed_names:
        for prefix, ref in KNOWN_VALIDATION_BUGS.items():
            if name.startswith(prefix):
                refs.add(ref)
                break
        else:
            return None
    return refs


class TestValidFilesRead:
    """Every data/ and samples/ file must be detected, readable, and valid."""

    @pytest.mark.parametrize("path", _corpus_files("data", "samples"))
    def test_detected_as_v2(self, path):
        info = detect_geoparquet_file_type(str(path))
        assert info["file_type"] == "geoparquet_v2", info
        assert info["geo_version"] == "2.0.0", info

    @pytest.mark.parametrize("path", _corpus_files("data", "samples"))
    def test_schema_and_data_readable(self, path):
        """Both engines must fully read every file (all codecs, GEOGRAPHY, Z/M)."""
        import pyarrow.parquet as pq

        geom_cols = detect_geometry_columns(str(path))
        assert geom_cols, "no geometry column detected"

        table = pq.read_table(str(path))
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            (rows,) = con.execute(
                f"SELECT count(*) FROM read_parquet('{path.as_posix()}')"
            ).fetchone()
        finally:
            con.close()
        assert rows == table.num_rows

    @pytest.mark.parametrize("path", _corpus_files("data", "samples"))
    def test_validates_clean(self, path):
        rel = path.relative_to(CORPUS).as_posix()
        result = validate_geoparquet(str(path), validate_data=True)
        failed = _failed_checks(result)
        if rel in CORPUS_BUG_FILES:
            # Spec-violating fixture: failing validation is the CORRECT outcome
            # (today it fails for the wrong reason, gpio #583). If validation is
            # clean the fixture was fixed upstream — force removal of the stale
            # entry so it cannot mask a future regression on this file.
            if failed:
                pytest.xfail(CORPUS_BUG_FILES[rel])
            pytest.fail(
                f"CORPUS_BUG_FILES entry no longer needed for {rel} — validation "
                "is clean at this corpus pin; remove the entry (and the matching "
                "test_zm_dimensions_parsed xfail if it now XPASSes)"
            )
        if not failed:
            return
        refs = _known_bug_refs(failed)
        assert refs is not None, f"unexpected validation failures: {failed}"
        pytest.xfail(f"known gpio validation bugs: {sorted(refs)}")


class TestValidFilesSpotChecks:
    """Targeted assertions on specific spec axes."""

    @pytest.mark.parametrize(
        ("name", "expected_dim"),
        [
            ("linestring-xyz-native-geometry.parquet", "XYZ"),
            ("linestring-xym-native-geometry.parquet", "XYM"),
            ("linestring-xyzm-native-geometry.parquet", "XYZM"),
        ],
    )
    def test_zm_dimensions_parsed(self, name, expected_dim):
        """Spec: 'a \" Z\" suffix gets added' and geometry_types is 'strictly correct'."""
        path = CORPUS / "data" / "zm" / name
        if not path.exists():
            pytest.skip("run: git submodule update --init")
        geo = get_geo_metadata(str(path))
        declared = geo["columns"]["geometry"]["geometry_types"]
        suffix = expected_dim[2:]  # Z, M, or ZM
        assert declared == [f"LineString {suffix}"], declared

    def test_two_geometry_columns_different_crs(self):
        path = CORPUS / "data" / "multi_geometry" / "two-geom-columns-different-crs.parquet"
        if not path.exists():
            pytest.skip("run: git submodule update --init")
        crs_by_col = {}
        for col in get_schema_info(str(path)):
            parsed = parse_geometry_logical_type(col.get("logical_type") or "")
            if parsed:
                crs_by_col[col["name"]] = parsed.get("crs")
        assert set(crs_by_col) == {"footprint", "centroid"}, crs_by_col
        assert crs_by_col["footprint"] != crs_by_col["centroid"], crs_by_col

    @pytest.mark.parametrize(
        "edges", ["planar", "spherical", "vincenty", "thomas", "andoyer", "karney"]
    )
    def test_edges_metadata_preserved_on_read(self, edges):
        path = CORPUS / "data" / "edges" / f"edges-{edges}.parquet"
        if not path.exists():
            pytest.skip("run: git submodule update --init")
        geo = get_geo_metadata(str(path))
        col = geo["columns"]["geometry"]
        expected = None if edges == "planar" else edges
        assert col.get("edges", None) in (expected, edges), col.get("edges")


# filename -> (validate mode, regex patterns over failed check names, xfail reason).
# A file is "detected" when validation reports invalid AND at least one FAILED
# check matches a pattern. Mode "2.0" forces target_version="2.0" — used where
# auto-detect legitimately classifies the file as something valid (adjudicated
# in each entry's comment).
BAD_DATA_EXPECTATIONS = {
    "bbox-does-not-contain-geometry.parquet": ("auto", [r"bbox_contains_data_"], None),
    "crs-invalid-projjson.parquet": ("auto", [r"crs_valid_", r"native_crs_format_"], None),
    # Detected today by the coordinate heuristic; after #581/#586 the
    # deterministic metadata-vs-native comparison (v2_crs_consistency) carries it.
    "crs-mismatch-schema-vs-geo-metadata.parquet": (
        "auto",
        [r"v2_crs_consistency_", r"coordinates_valid_for_crs_"],
        None,
    ),
    "edges-spherical-with-planar-antimeridian-line.parquet": (
        "auto",
        [r"edges"],
        "#586 no spherical-vs-planar data heuristic (may be wontfix)",
    ),
    "epoch-on-unsupported-crs.parquet": ("auto", [r"epoch"], None),
    "geo-invalid-json.parquet": ("auto", [r"geo_metadata", r"geo_key"], None),
    "geo-invalid-utf8.parquet": ("auto", [r"geo_metadata", r"geo_key"], None),
    "geometry-types-mismatch-declared-point-actual-linestring.parquet": (
        "auto",
        [r"geometry_types_match_data_"],
        None,
    ),
    "geometry-types-missing-actual-type.parquet": (
        "auto",
        [r"geometry_types_match_data_"],
        None,
    ),
    "missing-columns.parquet": ("auto", [r"columns_present"], None),
    # Adjudicated: with no geo metadata and native types, auto-detect correctly
    # classifies this as valid parquet-geo-only; the corpus expectation holds
    # only when GeoParquet 2.0 is explicitly demanded.
    "missing-geo-metadata.parquet": ("2.0", [r"version_match"], None),
    "missing-primary-column.parquet": ("auto", [r"primary_column_present"], None),
    "missing-version.parquet": ("auto", [r"version_present"], None),
    "orientation-ccw-declared-rings-cw.parquet": ("auto", [r"orientation_matches_data_"], None),
    "primary-column-not-in-columns.parquet": ("auto", [r"primary_column_in_columns"], None),
    "version-1-0-with-2-0-features.parquet": ("auto", [r"version_features"], None),
    "version-unknown.parquet": ("auto", [r"version_known"], None),
    # The wkb-* fixtures carry no native logical type (writers refuse invalid
    # WKB), so 2.0-level detection fires on the missing native type rather
    # than on parsing the payload. A deterministic EWKB byte-probe is #586.
    "wkb-truncated.parquet": (
        "auto",
        [r"native_geo_type_present_", r"geometry_types_match_data_"],
        None,
    ),
    "wkb-with-srid-prefix.parquet": ("auto", [r"native_geo_type_present_"], None),
    "wkb-wrong-type-byte.parquet": ("auto", [r"native_geo_type_present_"], None),
    # Native stats are computed from the data itself, so native_geo_types_match_
    # can never fire here — they always agree with the data. Detection comes
    # solely from the geo-metadata declaration disagreeing with the scan.
    "zm-declared-xy-actual-xyz.parquet": (
        "auto",
        [r"geometry_types_match_data_"],
        None,
    ),
    "zm-declared-xyz-actual-xy.parquet": ("auto", [r"geometry_types_match_data_"], None),
}

# Files where validate_geoparquet raises instead of reporting. Empty since the
# #585 fixes; kept so a regression re-adds an entry instead of reshaping tests.
CRASHES_ON_VALIDATE: dict[str, str] = {}


def _bad_data_params():
    if not (CORPUS / "bad_data").exists():
        return [
            pytest.param(
                None,
                None,
                None,
                id="submodule-missing",
                marks=pytest.mark.skip(reason="run: git submodule update --init"),
            )
        ]
    params = []
    for fname, (mode, patterns, gap) in sorted(BAD_DATA_EXPECTATIONS.items()):
        marks = []
        if gap:
            marks.append(pytest.mark.xfail(strict=True, reason=f"gpio gap: {gap}"))
        params.append(
            pytest.param(CORPUS / "bad_data" / fname, mode, patterns, id=fname, marks=marks)
        )
    return params


class TestBadData:
    """Every bad_data/ file must be rejected for the manifest's reason."""

    def test_manifest_covers_expectations(self):
        """Guard: corpus updates adding/removing bad files must not go unnoticed."""
        manifest_path = CORPUS / "bad_data" / "manifest.json"
        if not manifest_path.exists():
            pytest.skip("run: git submodule update --init")
        manifest = set(json.loads(manifest_path.read_text()))
        # rglob to mirror _corpus_files collection: a nested bad_data file must
        # not dodge the guard just because it sits in a subdirectory.
        on_disk = {f.name for f in (CORPUS / "bad_data").rglob("*.parquet")}
        assert manifest == on_disk
        assert set(BAD_DATA_EXPECTATIONS) == manifest

    @pytest.mark.parametrize("path", _corpus_files("bad_data"))
    def test_never_crashes(self, path):
        """A validator must reject bad files, not raise on them."""
        if path.name in CRASHES_ON_VALIDATE:
            pytest.xfail(CRASHES_ON_VALIDATE[path.name])
        result = validate_geoparquet(str(path), validate_data=True, sample_size=0)
        assert result is not None

    @pytest.mark.parametrize(("path", "mode", "patterns"), _bad_data_params())
    def test_detected_for_right_reason(self, path, mode, patterns):
        if path.name in CRASHES_ON_VALIDATE and mode == "auto":
            pytest.xfail(CRASHES_ON_VALIDATE[path.name])
        result = validate_geoparquet(
            str(path),
            target_version="2.0" if mode == "2.0" else None,
            validate_data=True,
            sample_size=0,
        )
        failed = _failed_checks(result)
        assert not result.is_valid, "bad file passed validation"
        matched = [n for n in failed if any(re.match(p, n) for p in patterns)]
        assert matched, f"invalid, but not for the expected reason. Failed: {failed}"


WRITE_SUBSET = [
    "data/encodings/polygon-native-geometry.parquet",
    "data/crs/crs-epsg-3857.parquet",
    "data/geometry_types/geometrycollection.parquet",
    "data/compression/compression-brotli.parquet",
    "samples/us-states.parquet",
]


class TestWritePath:
    """Rewriting corpus files through gpio must preserve their 2.0 nature."""

    @pytest.mark.parametrize(
        "rel",
        WRITE_SUBSET
        + [
            # Geography input: the native type demotes to GEOMETRY (DuckDB has
            # no geography type) but the edges declaration must survive (#588).
            "data/encodings/point-native-geography.parquet",
            "data/zm/linestring-xyzm-native-geometry.parquet",
        ],
    )
    def test_explicit_v2_rewrite_preserves(self, rel, tmp_path):
        src = CORPUS / rel
        if not src.exists():
            pytest.skip("run: git submodule update --init")
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(src), str(out), skip_hilbert=True, geoparquet_version="2.0")

        assert has_native_geo_types(str(out)), "native logical type lost"
        assert get_geoparquet_version(str(out)) == "2.0.0"
        # GEOGRAPHY inputs must keep geography semantics (native type or edges).
        src_types = {c["name"]: c.get("logical_type") or "" for c in get_schema_info(str(src))}
        out_types = {c["name"]: c.get("logical_type") or "" for c in get_schema_info(str(out))}
        for col, lt in src_types.items():
            if "GeographyType" in lt:
                out_geo = get_geo_metadata(str(out))["columns"][col]
                assert "GeographyType" in out_types.get(col, "") or out_geo.get("edges") in (
                    "spherical",
                    "vincenty",
                    "thomas",
                    "andoyer",
                    "karney",
                )

    def test_explicit_v2_rewrite_preserves_native_crs(self, tmp_path):
        """Non-default CRS on the native logical type must survive a rewrite."""
        src = CORPUS / "data" / "crs" / "crs-epsg-3857.parquet"
        if not src.exists():
            pytest.skip("run: git submodule update --init")
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(src), str(out), skip_hilbert=True, geoparquet_version="2.0")
        (geom_lt,) = [
            c.get("logical_type") or ""
            for c in get_schema_info(str(out))
            if c["name"] == "geometry"
        ]
        parsed = parse_geometry_logical_type(geom_lt)
        assert parsed and parsed.get("crs"), "native CRS lost on rewrite"
        assert "3857" in str(parsed["crs"]) or "Pseudo-Mercator" in str(parsed["crs"])

    @pytest.mark.parametrize("rel", WRITE_SUBSET[:3])
    def test_auto_rewrite_preserves_v2(self, rel, tmp_path):
        src = CORPUS / rel
        if not src.exists():
            pytest.skip("run: git submodule update --init")
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(src), str(out), skip_hilbert=True, geoparquet_version=None)
        assert get_geoparquet_version(str(out)) == "2.0.0"
        assert has_native_geo_types(str(out))

    @pytest.mark.parametrize("rel", ["samples/us-states.parquet"])
    def test_roundtrip_2_0_to_1_1_to_2_0(self, rel, tmp_path):
        src = CORPUS / rel
        if not src.exists():
            pytest.skip("run: git submodule update --init")
        mid = tmp_path / "v11.parquet"
        back = tmp_path / "v20.parquet"
        convert_to_geoparquet(str(src), str(mid), skip_hilbert=True, geoparquet_version="1.1")
        convert_to_geoparquet(str(mid), str(back), skip_hilbert=True, geoparquet_version="2.0")
        assert get_geoparquet_version(str(mid)) == "1.1.0"
        assert not has_native_geo_types(str(mid))
        assert get_geoparquet_version(str(back)) == "2.0.0"
        assert has_native_geo_types(str(back))


class TestCliWiring:
    """One CLI-level test; everything else goes through the Python API for speed."""

    def test_check_spec_exit_codes(self):
        good = CORPUS / "data" / "bbox" / "bbox-present.parquet"
        bad = CORPUS / "bad_data" / "missing-columns.parquet"
        if not good.exists():
            pytest.skip("run: git submodule update --init")
        runner = CliRunner()
        good_result = runner.invoke(cli, ["check", "spec", str(good), "--json"])
        # Exit 0 (pass) or 2 (warnings only); 1 means spec failures, which a
        # valid corpus file must never trip unless a check is currently
        # adjudicated as buggy in KNOWN_VALIDATION_BUGS (then exit 1 is the
        # expected symptom of that bug on affected platforms).
        allowed = (0, 1, 2) if KNOWN_VALIDATION_BUGS else (0, 2)
        assert good_result.exit_code in allowed, good_result.output
        json.loads(good_result.output)
        bad_result = runner.invoke(cli, ["check", "spec", str(bad), "--json"])
        assert bad_result.exit_code == 1, bad_result.output
