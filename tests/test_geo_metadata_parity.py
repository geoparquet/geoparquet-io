"""Characterization suite: the geo-metadata readers must agree (#664, suite 2).

`gpio` parsed the GeoParquet ``geo`` key-value metadata in five places when
#664 inventoried them. #1367 (concept 3) consolidated them: the raw decoder is
``core/geo_metadata.py::read_raw_geo`` (bytes or str keys, never raises), and
``crs_utils.parse_geo_metadata_from_schema`` and its ``add/quadkey.py`` alias are
gone. The readers left, which must agree:

1. ``core/geo_metadata.py::read_raw_geo`` — the raw diagnostic decoder
2. ``core/geo_metadata.py::raw_geo_of_file`` — pyarrow fast path / DuckDB remote
   (decodes through reader 1)
3. ``core/common.py::get_parquet_metadata`` — the kv leg of the returned tuple,
   decoded here with reader 1

Reader 1 takes an Arrow schema metadata dict; readers 2 and 3 take a path.
``READERS`` adapts both families to one ``(path, schema_metadata)`` signature so
a single parametrized test can compare them.

The edge-case matrix (``test_edge_case_verdicts``) pins reader 1's verdict on
the shapes the five used to disagree about; the divergence #664 recorded
(str keys invisible to one reader) is resolved by adopting reader-2 semantics.

The CRS-equality trio (``_crs_equals`` / ``_is_crs84_equivalent`` /
``_crs_are_equivalent``) used to be the other exception. #699 fixed it: the
helpers now take :data:`~geoparquet_io.core.crs_utils.CRS_ABSENT` for an omitted
``crs`` key (the OGC:CRS84 default) and ``None`` for an explicit ``"crs": null``
(unknown CRS), and both binaries treat OGC:CRS84 and EPSG:4326 as the same CRS
because GeoParquet fixes the stored coordinate order to (x, y). The three cells
that this suite carried as ``xfail(strict=True)`` spec violations are now plain
assertions, the two binary matrices are identical, and ``CRS_DECISIONS`` /
``UNARY_DECISIONS`` are empty. They are kept as (empty) inventories because the
guards that check them recompute the disagreement set by *calling* the helpers,
so any fresh divergence fails loudly instead of accumulating silently.
"""

import json
from itertools import product
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.crs_utils import CRS_ABSENT, crs_from_column_meta
from geoparquet_io.core.geo_metadata import raw_geo_of_file, read_raw_geo
from geoparquet_io.core.inspect_utils import _crs_are_equivalent
from geoparquet_io.core.validate import (
    CheckStatus,
    _check_crs_valid,
    _crs_equals,
    _is_crs84_equivalent,
    _version_at_least,
)

DATA = Path(__file__).parent / "data"
CORPUS = DATA / "geoparquet-testing"


# =============================================================================
# Reader adapters
# =============================================================================


def _read_common_kv(path: Path, _metadata: dict | None) -> dict | None:
    """Reader 4: parse the ``geo`` entry out of ``get_parquet_metadata``'s kv leg.

    ``get_parquet_metadata`` returns raw kv bytes rather than a parsed dict, so
    the JSON decode that the other four readers do internally happens here. That
    decode is part of what the consolidation would absorb.
    """
    kv, _schema = get_parquet_metadata(str(path))
    return read_raw_geo(kv).value


READERS = {
    "geo_metadata.read_raw_geo": lambda path, md: read_raw_geo(md).value,
    "geo_metadata.raw_geo_of_file": lambda path, md: raw_geo_of_file(str(path)).value,
    "common.get_parquet_metadata[kv]": _read_common_kv,
}


def _normalize(parsed: dict | None) -> str:
    """Order-insensitive comparable form; key order must not count as divergence."""
    return json.dumps(parsed, sort_keys=True)


def _schema_metadata(path: Path) -> dict | None:
    return pq.ParquetFile(path).schema_arrow.metadata


def _assert_geo_key_sources_agree(path: Path) -> None:
    """Guard: the harness's two metadata *sources* must carry the same ``geo``.

    This suite feeds readers 1, 2 and 5 from ``schema_arrow.metadata`` while
    readers 3 and 4 go to the parquet footer key-value block themselves. Those
    are not the same dict. When a file carries an ``ARROW:schema`` footer key,
    pyarrow rebuilds ``schema_arrow.metadata`` by deserializing that key rather
    than by reading the footer kv, so a footer entry written *after*
    ``ARROW:schema`` is invisible to ``schema_arrow.metadata``. Most local
    fixtures carry ``ARROW:schema``.

    Without this guard a mismatch between the two sources would surface as a
    "readers disagree" failure and send the reader implementations, not the
    file, to the debugger. It also protects ``_has_geo_key`` (which asks only
    ``schema_arrow.metadata``): a file whose ``geo`` lives solely in the footer
    would be misfiled into ``NON_GEO_FIXTURES`` and fail there with an equally
    misleading message.
    """
    pf = pq.ParquetFile(path)
    from_schema = (pf.schema_arrow.metadata or {}).get(b"geo")
    from_footer = (pf.metadata.metadata or {}).get(b"geo")
    assert from_schema == from_footer, (
        f"{path.name}: the two metadata sources this harness uses disagree, so any "
        f"reader disagreement below would be a harness artifact, not a reader bug. "
        f"schema_arrow.metadata[geo]={from_schema!r} "
        f"footer kv[geo]={from_footer!r} "
        f"(ARROW:schema present: {b'ARROW:schema' in (pf.metadata.metadata or {})})"
    )


def _assert_readers_agree(path: Path) -> None:
    """Every reader must produce the same parsed ``geo`` dict for ``path``."""
    _assert_geo_key_sources_agree(path)
    metadata = _schema_metadata(path)
    results = {name: _normalize(reader(path, metadata)) for name, reader in READERS.items()}
    distinct = set(results.values())
    assert len(distinct) == 1, f"geo-metadata readers disagree on {path.name}:\n" + "\n".join(
        f"  {name}: {value}" for name, value in sorted(results.items())
    )


# =============================================================================
# Fixture enumeration (at collection time)
# =============================================================================


def _has_geo_key(path: Path) -> bool:
    """Whether ``path`` carries a ``geo`` kv key. Runs at COLLECTION time.

    An unreadable/corrupt parquet is treated as non-geo rather than allowed to
    raise: a raise here happens during collection and would take down the whole
    module. Routing it to NON_GEO_FIXTURES instead means the bad file surfaces as
    one failing case in ``test_readers_agree_on_non_geo_fixtures`` (where a reader
    will raise on it), which is a far more useful signal than a collection error.
    """
    try:
        metadata = _schema_metadata(path)
    except Exception:
        return False
    return bool(metadata) and b"geo" in metadata


def _local_fixtures() -> list[Path]:
    """Every non-corpus parquet fixture under tests/data/."""
    return sorted(p for p in DATA.rglob("*.parquet") if CORPUS not in p.parents)


GEO_FIXTURES = [p for p in _local_fixtures() if _has_geo_key(p)]
NON_GEO_FIXTURES = [p for p in _local_fixtures() if not _has_geo_key(p)]

# Representative corpus slice: one file per corpus dimension that could plausibly
# change how the ``geo`` blob is written (bbox covering, each CRS spelling, edge
# interpolation, native logical types, coordinate epoch, geometry collections,
# multiple geometry columns, ZM, and a real-world sample).
CORPUS_SAMPLE = [
    "data/bbox/bbox-present.parquet",
    "data/crs/crs-default.parquet",
    "data/crs/crs-epsg-3857.parquet",
    "data/crs/crs-projjson-full.parquet",
    "data/edges/edges-spherical.parquet",
    "data/encodings/point-native-geography.parquet",
    "data/epoch/epoch-itrf2014-2020.parquet",
    "data/geometry_types/geometrycollection.parquet",
    "data/multi_geometry/two-geom-columns-different-crs.parquet",
    "data/zm/linestring-xyzm-native-geometry.parquet",
    "samples/us-states.parquet",
]

# Floor just under the current count (16 geo fixtures). An emptied or moved
# tests/data would otherwise make rglob return [] and pytest's empty-parameter-set
# default would silently SKIP the whole parity test instead of failing.
MIN_GEO_FIXTURES = 14

# Same guard for the non-geo half (7 today). Without it, a change that made
# _has_geo_key wrongly report True for everything would empty this list and
# silently skip the whole absent-key test rather than failing.
MIN_NON_GEO_FIXTURES = 5


def test_geo_fixture_inventory_is_populated():
    """Guard: a vanished fixture set must fail loudly, not silently skip."""
    assert len(GEO_FIXTURES) >= MIN_GEO_FIXTURES, (
        f"expected >= {MIN_GEO_FIXTURES} geo fixtures under {DATA}, "
        f"found {len(GEO_FIXTURES)}: {[p.name for p in GEO_FIXTURES]}"
    )


def test_non_geo_fixture_inventory_is_populated():
    """Guard: the absent-geo-key cases must not silently vanish either."""
    assert len(NON_GEO_FIXTURES) >= MIN_NON_GEO_FIXTURES, (
        f"expected >= {MIN_NON_GEO_FIXTURES} non-geo fixtures under {DATA}, "
        f"found {len(NON_GEO_FIXTURES)}: {[p.name for p in NON_GEO_FIXTURES]}"
    )


# =============================================================================
# 1. Reader parity
# =============================================================================


@pytest.mark.parametrize("fixture", GEO_FIXTURES, ids=lambda p: p.name)
def test_readers_agree_on_local_fixtures(fixture):
    """All five readers parse every geo fixture to the identical dict."""
    _assert_readers_agree(fixture)


@pytest.mark.parametrize("fixture", NON_GEO_FIXTURES, ids=lambda p: p.name)
def test_readers_agree_on_non_geo_fixtures(fixture):
    """Files with no ``geo`` key: every reader returns None, none of them raise.

    These are parquet-geo-native and plain-parquet fixtures. This is the
    file-backed half of the "absent geo key" edge case, and it is the only place
    readers 3 and 4 (path-based) can be exercised against absence at all.
    """
    # Run first: if the file actually has a footer ``geo`` that
    # ``schema_arrow.metadata`` hides, it was misfiled into this list and the
    # source guard says so instead of "reader X returned non-None".
    _assert_geo_key_sources_agree(fixture)
    metadata = _schema_metadata(fixture)
    for name, reader in READERS.items():
        assert reader(fixture, metadata) is None, f"{name} returned non-None for {fixture.name}"


@pytest.mark.corpus
@pytest.mark.integration
@pytest.mark.parametrize("relative_path", CORPUS_SAMPLE)
def test_readers_agree_on_corpus_files(relative_path):
    """The same parity holds on the official geoparquet-testing corpus."""
    if not (CORPUS / "data").exists():
        pytest.skip("run: git submodule update --init")
    path = CORPUS / relative_path
    assert path.exists(), f"corpus file moved or removed: {relative_path}"
    _assert_readers_agree(path)


# =============================================================================
# 2. Edge cases — the readers legitimately DIVERGE here
# =============================================================================

# Reader 1's verdict on each shape the five readers of #664 handled differently.
# ``None`` means "no value" (absent or undecodable); a dict means "parsed to that
# dict". Readers 2 and 3 are path-based and decode through reader 1.
STR_KEY_META = {"geo": '{"version": "1.1.0", "columns": {}}'}
PARSED_STR_KEY = {"version": "1.1.0", "columns": {}}

EDGE_CASES = {
    # name: (metadata_in, expected value)
    # #664's headline divergence, resolved: str keys are read like bytes keys.
    "str_key_str_value": (STR_KEY_META, PARSED_STR_KEY),
    "bytes_key_invalid_json": ({b"geo": b"{not valid json"}, None),
    "str_key_invalid_json": ({"geo": "{not valid json"}, None),
    "bytes_key_invalid_utf8": ({b"geo": b"\xff\xfe{}"}, None),
    "absent_geo_key": ({b"other": b"value"}, None),
    "empty_dict": ({}, None),
    "none_metadata": (None, None),
    "empty_geo_value": ({b"geo": b""}, None),
}


@pytest.mark.parametrize("case_name", sorted(EDGE_CASES))
def test_edge_case_verdicts(case_name):
    """Pin the raw decoder's verdict; it never raises."""
    metadata, want = EDGE_CASES[case_name]
    got = read_raw_geo(metadata).value
    assert got == want, f"{case_name}: read_raw_geo returned {got!r}, pinned {want!r}"


# =============================================================================
# 3. CRS-equality trio
# =============================================================================

# Compact but structurally valid PROJJSON. Each carries an ``id`` member, so the
# helpers take their authority:code fast paths rather than pyproj's semantic
# comparison — that keeps the pinned matrices stable across pyproj versions.
CRS84_PROJJSON = {
    "type": "GeographicCRS",
    "name": "WGS 84 (CRS84)",
    "datum_ensemble": {
        "name": "World Geodetic System 1984 ensemble",
        "members": [{"name": "World Geodetic System 1984 (G2296)"}],
        "accuracy": "2.0",
    },
    "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
            {
                "name": "Geodetic longitude",
                "abbreviation": "Lon",
                "direction": "east",
                "unit": "degree",
            },
            {
                "name": "Geodetic latitude",
                "abbreviation": "Lat",
                "direction": "north",
                "unit": "degree",
            },
        ],
    },
    "id": {"authority": "OGC", "code": "CRS84"},
}

EPSG4326_PROJJSON = {
    "type": "GeographicCRS",
    "name": "WGS 84",
    "datum_ensemble": {
        "name": "World Geodetic System 1984 ensemble",
        "members": [{"name": "World Geodetic System 1984 (G2296)"}],
        "accuracy": "2.0",
    },
    "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
            {
                "name": "Geodetic latitude",
                "abbreviation": "Lat",
                "direction": "north",
                "unit": "degree",
            },
            {
                "name": "Geodetic longitude",
                "abbreviation": "Lon",
                "direction": "east",
                "unit": "degree",
            },
        ],
    },
    "id": {"authority": "EPSG", "code": 4326},
}

EPSG5070_PROJJSON = {
    "type": "ProjectedCRS",
    "name": "NAD83 / Conus Albers",
    "base_crs": {
        "name": "NAD83",
        "datum": {
            "type": "GeodeticReferenceFrame",
            "name": "North American Datum 1983",
            "ellipsoid": {
                "name": "GRS 1980",
                "semi_major_axis": 6378137,
                "inverse_flattening": 298.257222101,
            },
        },
        "coordinate_system": {
            "subtype": "ellipsoidal",
            "axis": [
                {
                    "name": "Geodetic latitude",
                    "abbreviation": "Lat",
                    "direction": "north",
                    "unit": "degree",
                },
                {
                    "name": "Geodetic longitude",
                    "abbreviation": "Lon",
                    "direction": "east",
                    "unit": "degree",
                },
            ],
        },
        "id": {"authority": "EPSG", "code": 4269},
    },
    "conversion": {
        "name": "Conus Albers",
        "method": {"name": "Albers Equal Area", "id": {"authority": "EPSG", "code": 9822}},
        "parameters": [
            {
                "name": "Latitude of false origin",
                "value": 23,
                "unit": "degree",
                "id": {"authority": "EPSG", "code": 8821},
            },
            {
                "name": "Longitude of false origin",
                "value": -96,
                "unit": "degree",
                "id": {"authority": "EPSG", "code": 8822},
            },
            {
                "name": "Latitude of 1st standard parallel",
                "value": 29.5,
                "unit": "degree",
                "id": {"authority": "EPSG", "code": 8823},
            },
            {
                "name": "Latitude of 2nd standard parallel",
                "value": 45.5,
                "unit": "degree",
                "id": {"authority": "EPSG", "code": 8824},
            },
            {
                "name": "Easting at false origin",
                "value": 0,
                "unit": "metre",
                "id": {"authority": "EPSG", "code": 8826},
            },
            {
                "name": "Northing at false origin",
                "value": 0,
                "unit": "metre",
                "id": {"authority": "EPSG", "code": 8827},
            },
        ],
    },
    "coordinate_system": {
        "subtype": "Cartesian",
        "axis": [
            {"name": "Easting", "abbreviation": "X", "direction": "east", "unit": "metre"},
            {"name": "Northing", "abbreviation": "Y", "direction": "north", "unit": "metre"},
        ],
    },
    "id": {"authority": "EPSG", "code": 5070},
}

# The five matrix inputs, expressed as the geo *column* metadata a caller holds.
# "absent" and "null" are distinct in the file — per the spec an absent ``crs``
# means the OGC:CRS84 default while an explicit ``null`` means an unknown /
# engineering CRS. They used to collapse into the same ``None`` at the helper
# boundary because every call site extracted the value with
# ``col_meta.get("crs")``; since #699 the extraction goes through
# ``crs_from_column_meta``, which yields ``CRS_ABSENT`` for the first shape.
# ``test_absent_and_explicit_null_survive_the_helper_boundary`` pins that.
CRS_COLUMN_META = {
    "absent": {},
    "null": {"crs": None},
    "crs84": {"crs": CRS84_PROJJSON},
    "epsg4326": {"crs": EPSG4326_PROJJSON},
    "epsg5070": {"crs": EPSG5070_PROJJSON},
}
CRS_NAMES = ["absent", "null", "crs84", "epsg4326", "epsg5070"]


def crs_value(name: str):
    """Extract a CRS the way every call site does: ``crs_from_column_meta``.

    Before #699 this was ``col_meta.get("crs")``, which returned ``None`` for
    both the "absent" and "null" shapes and made every ("absent", x) cell below
    a byte-identical duplicate of the ("null", x) cell — while the spec demands
    opposite answers of them. ``crs_from_column_meta`` yields ``CRS_ABSENT`` for
    the omitted key, so the two rows are now genuinely different calls.
    """
    return crs_from_column_meta(CRS_COLUMN_META[name])


# Generated by running each helper over the matrix once and hardcoding the
# result (regenerated 2026-09-02 for #699; pyproj as pinned in uv.lock). To
# regenerate after an intentional change, print
# `{a: {b: fn(crs_value(a), crs_value(b))}}` for each helper and paste the
# result back here — do not relax an assertion to make a surprise pass.
#
# The spec rules these encode:
#   - absent  => the OGC:CRS84 default (a known CRS)
#   - null    => the CRS is unknown; equal only to another unknown
#   - OGC:CRS84 == EPSG:4326, because GeoParquet fixes coordinate order to (x, y)

IS_CRS84_EQUIVALENT = {
    "absent": True,
    "null": False,
    "crs84": True,
    "epsg4326": True,
    "epsg5070": False,
}

CRS_EQUALS = {
    "absent": {"absent": True, "null": False, "crs84": True, "epsg4326": True, "epsg5070": False},
    "null": {"absent": False, "null": True, "crs84": False, "epsg4326": False, "epsg5070": False},
    "crs84": {"absent": True, "null": False, "crs84": True, "epsg4326": True, "epsg5070": False},
    "epsg4326": {"absent": True, "null": False, "crs84": True, "epsg4326": True, "epsg5070": False},
    "epsg5070": {
        "absent": False,
        "null": False,
        "crs84": False,
        "epsg4326": False,
        "epsg5070": True,
    },
}

# Identical to CRS_EQUALS since #699 — held as its own literal, not an alias, so
# that a future divergence in either helper shows up as a concrete cell diff.
CRS_ARE_EQUIVALENT = {
    "absent": {"absent": True, "null": False, "crs84": True, "epsg4326": True, "epsg5070": False},
    "null": {"absent": False, "null": True, "crs84": False, "epsg4326": False, "epsg5070": False},
    "crs84": {"absent": True, "null": False, "crs84": True, "epsg4326": True, "epsg5070": False},
    "epsg4326": {"absent": True, "null": False, "crs84": True, "epsg4326": True, "epsg5070": False},
    "epsg5070": {
        "absent": False,
        "null": False,
        "crs84": False,
        "epsg4326": False,
        "epsg5070": True,
    },
}

# Cells where the two binary helpers disagree — empty since #699 unified them.
# Kept as data so ``test_crs_helper_disagreement_inventory_is_exact`` fails if
# the set changes in either direction: a new disagreement means a fresh
# inconsistency has crept in. That guard recomputes the observed set by CALLING
# both helpers — it must never derive it from the pinned matrices above, or it
# degrades into a tautology over constants.
CRS_DECISIONS: dict[tuple[str, str], str] = {}

# The trio's largest split used to be between the unary helper and both
# binaries: ``_is_crs84_equivalent(x)`` asks "is x the CRS84 default?" while
# ``_crs_equals(x, crs84)`` / ``_crs_are_equivalent(x, crs84)`` ask "is x equal
# to the CRS84 value?" Those are the same question, and #699 made them answer
# alike for all five inputs. ``test_unary_vs_binary_disagreement_inventory_is_exact``
# recomputes the split by CALLING the helpers, so this dict is never the source
# the guard checks itself against.
UNARY_DECISIONS: dict[str, str] = {}

# Both binaries fall back to pyproj when a PROJJSON pair carries no ``id``, and
# both now pass ``ignore_axis_order=True`` there, for the same spec reason the
# fast path recognizes OGC:CRS84 == EPSG:4326. The 5x5 matrix cannot reach that
# path — every value in it carries a complete ``id`` — so
# ``test_id_less_projjson_axis_order_agrees`` covers it with id-less PROJJSON.


def _param_id_pair(name_a: str, name_b: str) -> str:
    return f"{name_a}-{name_b}"


CRS_PAIRS = [
    pytest.param(name_a, name_b, id=_param_id_pair(name_a, name_b))
    for name_a, name_b in product(CRS_NAMES, repeat=2)
]

CRS_UNARY_PARAMS = [pytest.param(name, id=name) for name in CRS_NAMES]


@pytest.mark.parametrize("name", CRS_UNARY_PARAMS)
def test_is_crs84_equivalent_matrix(name):
    """Pin ``validate._is_crs84_equivalent`` (unary) over the five CRS values.

    Note this helper takes ONE argument, so it contributes a 5-cell vector to the
    matrix rather than a 5x5 grid. The "absent" and "null" cells are the pair
    #699 was about: absent is the OGC:CRS84 default (True), while an explicit
    null means the CRS is unknown (False).
    """
    assert _is_crs84_equivalent(crs_value(name)) is IS_CRS84_EQUIVALENT[name]


@pytest.mark.parametrize(("name_a", "name_b"), CRS_PAIRS)
def test_crs_equals_matrix(name_a, name_b):
    """Pin ``validate._crs_equals`` over the full 5x5 matrix."""
    assert _crs_equals(crs_value(name_a), crs_value(name_b)) is CRS_EQUALS[name_a][name_b]


@pytest.mark.parametrize(("name_a", "name_b"), CRS_PAIRS)
def test_crs_are_equivalent_matrix(name_a, name_b):
    """Pin ``inspect_utils._crs_are_equivalent`` over the full 5x5 matrix.

    Identical to ``CRS_EQUALS`` since #699; any cell where the two helpers drift
    apart also shows up in ``test_crs_helper_disagreement_inventory_is_exact``.
    """
    assert (
        _crs_are_equivalent(crs_value(name_a), crs_value(name_b))
        is CRS_ARE_EQUIVALENT[name_a][name_b]
    )


def test_crs_helper_disagreement_inventory_is_exact():
    """The set of cells where the two binary helpers disagree must not drift.

    Empty since #699 unified them, so this now asserts the two helpers agree on
    all 25 cells. ``observed`` is built by CALLING both helpers, never by
    reading the pinned matrices. Deriving it from the literals would make this a
    tautology over constants that stays green through any production change —
    the exact failure mode it exists to catch.

    Scope: this covers the id fast path only, because every value in the matrix
    carries a complete ``id``. The pyproj-fallback half is covered by
    ``test_id_less_projjson_axis_order_agrees``.
    """
    observed = {
        (a, b)
        for a, b in product(CRS_NAMES, repeat=2)
        if _crs_equals(crs_value(a), crs_value(b))
        is not _crs_are_equivalent(crs_value(a), crs_value(b))
    }
    assert observed == set(CRS_DECISIONS), (
        "CRS helper disagreements changed.\n"
        f"  newly agreeing (drop from CRS_DECISIONS): {sorted(set(CRS_DECISIONS) - observed)}\n"
        f"  newly disagreeing (add to CRS_DECISIONS): {sorted(observed - set(CRS_DECISIONS))}"
    )


def test_unary_vs_binary_disagreement_inventory_is_exact():
    """ "Is x CRS84?" and "does x equal CRS84?" must give the same answer.

    Empty since #699, so this asserts the unary helper and both binaries agree
    for every input. ``observed`` comes from CALLING the three helpers, so a
    regression forces an explicit update to UNARY_DECISIONS instead of passing
    silently over stale literals.
    """
    crs84 = crs_value("crs84")
    observed = {
        name
        for name in CRS_NAMES
        if _is_crs84_equivalent(crs_value(name)) is not _crs_equals(crs_value(name), crs84)
        or _is_crs84_equivalent(crs_value(name)) is not _crs_are_equivalent(crs_value(name), crs84)
    }
    assert observed == set(UNARY_DECISIONS), (
        "unary-vs-binary CRS84 disagreements changed.\n"
        f"  newly agreeing (drop from UNARY_DECISIONS): {sorted(set(UNARY_DECISIONS) - observed)}\n"
        f"  newly disagreeing (add to UNARY_DECISIONS): {sorted(observed - set(UNARY_DECISIONS))}"
    )


def test_absent_and_explicit_null_survive_the_helper_boundary():
    """An absent ``crs`` and an explicit ``"crs": null`` must stay distinguishable.

    The spec gives them different meanings (absent => OGC:CRS84 default;
    null => unknown/engineering CRS). ``validate._check_crs_valid`` always
    preserved the distinction, by testing membership *before* extracting::

        if "crs" not in col_meta:      # -> defaults to OGC:CRS84
            ...
        crs = col_meta.get("crs")      # -> None for BOTH shapes

    Every CRS *equality* call site used only the second idiom, so the
    distinction was gone by the time the trio was called — the #699 defect.
    ``crs_from_column_meta`` is the fixed extraction: it yields ``CRS_ABSENT``
    for the omitted key and ``None`` only for the explicit null.

    The ``_check_crs_valid`` PASSED-vs-WARNING half is unchanged and still the
    pin on production behavior; the extraction half now asserts the two shapes
    come out different rather than identical.
    """
    absent_col = CRS_COLUMN_META["absent"]
    null_col = CRS_COLUMN_META["null"]

    # Production path that has always distinguished them: different outcomes.
    assert _check_crs_valid(absent_col, "geometry").status is CheckStatus.PASSED
    assert _check_crs_valid(null_col, "geometry").status is CheckStatus.WARNING

    assert "crs" not in absent_col and "crs" in null_col, (
        "fixture shapes must differ, otherwise the assertions below are trivial"
    )

    # The old idiom still collapses them — which is why call sites must not use it.
    assert absent_col.get("crs") is None
    assert null_col.get("crs") is None

    # The extraction the equality helpers now receive: distinct for the two shapes.
    assert crs_from_column_meta(absent_col) is CRS_ABSENT
    assert crs_from_column_meta(null_col) is None


def test_id_less_projjson_axis_order_agrees():
    """The binaries' pyproj fallbacks must both ignore axis order.

    This is the one behavioral difference between the two binary helpers that
    the 5x5 matrix cannot reach: every matrix value carries a complete ``id``,
    so both helpers return from their fast path and pyproj is never called.
    Strip the ``id`` and the fallbacks run. They used to split::

        validate._crs_equals              -> .equals(..., ignore_axis_order=True)
        inspect_utils._crs_are_equivalent -> .equals(...)   # axis order MATTERS

    #699 gave both ``ignore_axis_order=True``, for the same spec reason the id
    fast path treats OGC:CRS84 and EPSG:4326 as one CRS: GeoParquet fixes the
    stored coordinate order to (x, y), so a lon/lat and a lat/lon declaration of
    WGS 84 describe the same coordinates.

    The PROJJSON is generated by pyproj rather than hand-written because a
    hand-trimmed datum ensemble is not parseable ("ensemble should have at least
    2 datums"), which would send BOTH helpers down their ``except -> False``
    path and produce spurious agreement.
    """
    from pyproj import CRS as PyprojCRS

    lon_lat = PyprojCRS.from_user_input("OGC:CRS84").to_json_dict()
    lat_lon = PyprojCRS.from_epsg(4326).to_json_dict()
    for projjson in (lon_lat, lat_lon):
        projjson.pop("id", None)

    # Preconditions, or the assertions below could pass through the fast path.
    assert "id" not in lon_lat and "id" not in lat_lon
    assert [axis["abbreviation"] for axis in lon_lat["coordinate_system"]["axis"]] == ["Lon", "Lat"]
    assert [axis["abbreviation"] for axis in lat_lon["coordinate_system"]["axis"]] == ["Lat", "Lon"]
    # Both must be pyproj-parseable, else both helpers fail closed and "agree".
    assert PyprojCRS.from_json_dict(lon_lat) and PyprojCRS.from_json_dict(lat_lon)

    assert _crs_equals(lon_lat, lat_lon) is True
    assert _crs_are_equivalent(lon_lat, lat_lon) is True


# =============================================================================
# 4. Version gating
# =============================================================================


@pytest.mark.parametrize(
    ("version", "major", "minor", "expected"),
    [
        # The boundary itself: >= must include the equal case. An off-by-one
        # (">=" turned into ">") is invisible to every other test in the repo.
        ("1.1.0", 1, 1, True),
        ("1.1", 1, 1, True),
        ("2.0.0", 2, 0, True),
        ("1.0.0", 1, 0, True),
        # Just below the boundary.
        ("1.0.0", 1, 1, False),
        ("1.9.0", 2, 0, False),
        # Parsed numerically, not lexicographically: "1.10.0" > "1.2.0".
        ("1.10.0", 1, 2, True),
        ("1.2.0", 1, 10, False),
        # Missing minor defaults to 0; pre-release/build tags are stripped.
        ("2", 2, 0, True),
        ("1.0.0-beta.1", 1, 0, True),
        ("1.1.0-beta.1", 1, 1, True),
        ("1.0.0+build.5", 1, 0, True),
        # Unparsable or non-string input fails closed.
        ("not-a-version", 1, 0, False),
        ("", 1, 0, False),
        (None, 1, 0, False),
        (1.1, 1, 1, False),
    ],
)
def test_version_at_least_boundary(version, major, minor, expected):
    """Pin ``validate._version_at_least``, which gates version-specific geo checks.

    It decides which ``geo`` metadata rules apply (covering requires >= 1.1,
    the 2.0 branch changes the encoding vocabulary), so an off-by-one here
    silently applies the wrong spec version to a file's metadata.
    """
    assert _version_at_least(version, major, minor) is expected
