"""Write-contract characterization suite (issue #664, suite 4).

Pins the *observable* contract of gpio's write paths: what lands on disk, not
how it got there. Three matrices, decoupled so the cross-product stays small:

* **A1 — path x version.** The four public ways to write a GeoParquet file
  (``write_parquet_with_metadata`` from a query, ``write_geoparquet_table``
  from an Arrow table, ``api.Table.write``, and ``gpio convert geoparquet``)
  are given the same 5-row input and each requested version, and must satisfy
  the *same* expectation. This is the keystone for write-path unification: a
  facade may only merge paths that already agree.
* **A2 — strategy x shape.** The four write strategies against six input
  shapes. ``tests/test_write_strategies.py`` already covers the no-geometry
  shape across all four strategies; that combination is deliberately not
  repeated here.
* **A3 — kv passthrough.** Non-geo file-level key/value metadata (fiboa,
  vecorel, STAC sidecars) surviving each path, plus ``extra_kv_metadata``.

``core/validate.py::validate_geoparquet`` is the oracle — the same pattern
``tests/test_geoparquet_corpus.py`` uses. Assertions are *properties* (row
conservation, primary column, version, bbox, row-group structure) rather than
whole-metadata snapshots, which churn on float formatting and key ordering.
The exception is four tiny normalized geo-dict snapshots, one per version,
which exist only to make a silently added or removed metadata field visible.
Refresh them with ``GPIO_UPDATE_SNAPSHOT=1 uv run pytest tests/test_write_contract.py``.

Policy on disagreement: where paths already disagree, this suite records the
disagreement in ``KNOWN_DIVERGENCES`` / ``KNOWN_STRATEGY_DIVERGENCES`` and
xfails — it does **not** pin either side as correct. Deciding which side wins
is the write-facade's job. A divergence that stops reproducing fails loudly so
the stale entry gets removed; anything *new* fails hard rather than being
absorbed.

Both allowlists are empty today, and that is the point: every divergence this
suite was built to record has since been fixed, and the suite reported each one
as "no longer reproduces" the moment it was. In order — #686 (`gpio convert
--geoparquet-version 1.0` emitting the 1.1-only `covering` key) by gpio #714,
#687 (`write_geoparquet_table` ignoring `parquet-geo-only`) by #702, #688 (the
streaming geometry type decided by geoarrow import order) by #707, #689
(disk-rewrite ignoring row-group sizing) by #698, #691 (the validator rejecting
GeoParquet 1.1's GeoArrow encodings) by #715, and #690 (convert and
`Table.write` dropping sidecar kv metadata) by #710.

What survives are the two disagreements outside that batch, both still xfailed:
auto-version resolution splitting four ways on a native-geo input (gpio #600)
and the validator's `crs_valid` check flipping with geoarrow registration (the
gpio #603 family). Those two are the remaining write-facade decision list.
"""

from __future__ import annotations

import json
import os
import struct
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.api import read as api_read
from geoparquet_io.cli.main import cli
from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet
from geoparquet_io.core.write_funnels import write_geoparquet_table, write_parquet_with_metadata

SNAPSHOT_DIR = Path(__file__).parent / "data" / "snapshots"
UPDATE_SNAPSHOTS = os.environ.get("GPIO_UPDATE_SNAPSHOT") == "1"
#: A snapshot refresh is a human deciding a metadata change is intended. In CI
#: there is no human, so an auto-refresh would rewrite the canary to match
#: whatever the build just produced and report success — turning the drift
#: detector into a rubber stamp.
IN_CI = bool(os.environ.get("CI") or os.environ.get("GITHUB_ACTIONS"))

# ---------------------------------------------------------------------------
# Known cross-path divergences — recorded, not pinned (see module docstring).
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Divergence:
    """One recorded cross-path disagreement.

    ``assertion_contains`` narrows the xfail route: a structural
    ``AssertionError`` is only absorbed when its message contains this
    substring. Without it a bare ``except AssertionError`` would swallow *any*
    failure on a case carrying a reason — a future row-loss or a CLI crash
    would silently xfail instead of failing the build.
    """

    reason: str
    #: substring the AssertionError must contain to be absorbed; None means
    #: this divergence never surfaces as a structural assertion
    assertion_contains: str | None = None
    #: FAILED check-name prefixes the oracle may report for this divergence
    #: (per-column checks are named "<check>_<geometry column>")
    oracle_prefixes: tuple[str, ...] = ()
    #: True when the divergence only fires in some global-state configurations.
    #: Suppresses the stale-entry check: not reproducing on this run is not
    #: evidence that it was fixed, so silence must not be read as agreement.
    state_dependent: bool = False


AUTO_VERSION_NATIVE_DOWNGRADE = Divergence(
    reason=(
        "Auto version mode (geoparquet_version=None) splits four ways on a "
        "native-geo-only input. write_parquet_with_metadata and "
        "write_geoparquet_table resolve to 1.1.0 WKB, stripping the native "
        "GEOMETRY logical type, because extract_version_from_metadata returns "
        "None when there is no 'geo' key; api.Table.write and gpio convert "
        "resolve to 2.0.0 and keep it, via resolve_geoparquet_version_from_*. "
        "gpio #600 tracks the downgrade — note write_geoparquet_table is a "
        "call site #600 does not list. The facade must resolve auto in one "
        "place."
    ),
    assertion_contains="auto-resolved",
)

AUTO_2_0_CRS_STATE = Divergence(
    reason=(
        "The validator's crs_valid check on a 2.0 native-geometry file flips "
        "with global state: with geoarrow.pyarrow imported it reads the CRS "
        "off the extension type and reports 'CRS is missing the required "
        "PROJJSON type member'; without it, the same file passes. The bytes on "
        "disk are identical — only the reading changes "
        "(duckdb_metadata.py:227-250). Same registration flip as gpio #688, "
        "surfacing as a validator false positive of the gpio #603 family. "
        "Recorded as state-dependent: not firing on a given run is not "
        "evidence of a fix."
    ),
    oracle_prefixes=("crs_valid",),
    state_dependent=True,
)

# (path, version) -> Divergence. A1 only.
#
# Empty: every A1 divergence this suite was written to record has since been
# fixed (#686 by #714, #687 by #702). Kept rather than deleted -- the
# adjudication machinery below is what makes a *new* disagreement fail loudly
# instead of being absorbed, and an empty allowlist is the state that says
# "all four paths agree at every version".
KNOWN_DIVERGENCES: dict[tuple[str, str], Divergence] = {}

# (strategy, shape) -> Divergence. A2 only.
#
# Also empty: #688 (fixed by #707) covered six streaming shapes and #689 (fixed
# by #698) the disk-rewrite row-group cell.
KNOWN_STRATEGY_DIVERGENCES: dict[tuple[str, str], Divergence] = {}

# ---------------------------------------------------------------------------
# Fixture data: built in-test, no new files under tests/data.
# ---------------------------------------------------------------------------

POINTS = [(-122.4, 37.8), (-74.0, 40.7), (2.35, 48.86), (139.7, 35.7), (151.2, -33.9)]
BBOX = (-122.4, -33.9, 151.2, 48.86)
EXTRA_KV = {"fiboa": '{"schemas": ["example"]}', "custom_note": '{"hello": "world"}'}

VERSIONS = ["1.0", "1.1", "2.0", "parquet-geo-only"]
GEO_VERSION_FOR = {"1.0": "1.0.0", "1.1": "1.1.0", "2.0": "2.0.0", "parquet-geo-only": None}
STRATEGIES = ["duckdb-kv", "streaming", "in-memory", "disk-rewrite"]


def _wkb_point(x: float, y: float) -> bytes:
    return struct.pack("<BI2d", 1, 1, x, y)


def _wkb_point_zm(x: float, y: float, z: float, m: float) -> bytes:
    return struct.pack("<BI4d", 1, 3001, x, y, z, m)


def _geo_dict(column: str = "geometry", geometry_types: tuple[str, ...] = ("Point",)) -> dict:
    return {
        "version": "1.1.0",
        "primary_column": column,
        "columns": {column: {"encoding": "WKB", "geometry_types": list(geometry_types)}},
    }


def _write_source(
    path: Path,
    table: pa.Table,
    geo: dict,
    extra_kv: dict[str, str] | None = None,
    row_group_size: int | None = None,
) -> str:
    metadata = {b"geo": json.dumps(geo).encode()}
    for key, value in (extra_kv or {}).items():
        metadata[key.encode()] = value.encode()
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, path, row_group_size=row_group_size)
    return str(path)


@pytest.fixture(scope="module")
def normal_source(tmp_path_factory) -> str:
    """The one shared A1/A3 input: 5 WKB points, two attribute columns."""
    tmp = tmp_path_factory.mktemp("write_contract")
    table = pa.table(
        {
            "id": list(range(1, 6)),
            "name": ["a", "b", "c", "d", "e"],
            "geometry": [_wkb_point(x, y) for x, y in POINTS],
        }
    )
    return _write_source(tmp / "normal.parquet", table, _geo_dict(), extra_kv=EXTRA_KV)


@pytest.fixture(scope="module")
def auto_sources(tmp_path_factory, normal_source) -> dict[str, tuple[str, int]]:
    """Inputs for auto version mode, as {kind: (path, row count)}.

    ``native_geo_only`` is #600's repro, built the way that issue builds it:
    DuckDB ``GEOPARQUET_VERSION 'NONE'`` — no ``geo`` key, native logical type.
    """
    tmp = tmp_path_factory.mktemp("write_contract_auto")
    native = tmp / "native_geo_only.parquet"
    con = get_duckdb_connection()
    try:
        con.execute(
            "COPY (SELECT * FROM (VALUES (1, ST_Point(1, 2)), (2, ST_Point(3, 4)), "
            "(3, ST_Point(5, 6))) t(id, geometry)) "
            f"TO '{native.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')"
        )
    finally:
        con.close()
    assert _geo_metadata(str(native)) is None, "fixture should have no geo key"
    assert _uses_native_geometry_type(str(native)), "fixture should be native-geo-only"
    return {"wkb_1_1": (normal_source, 5), "native_geo_only": (str(native), 3)}


@pytest.fixture(scope="module")
def shape_sources(tmp_path_factory) -> dict[str, str]:
    """One input file per A2 shape."""
    tmp = tmp_path_factory.mktemp("write_contract_shapes")
    geoms = [_wkb_point(x, y) for x, y in POINTS]
    sources = {}

    sources["normal"] = _write_source(
        tmp / "normal.parquet",
        pa.table({"id": list(range(5)), "geometry": geoms}),
        _geo_dict(),
    )
    sources["zero_row"] = _write_source(
        tmp / "zero_row.parquet",
        pa.table({"id": pa.array([], type=pa.int64()), "geometry": pa.array([], type=pa.binary())}),
        _geo_dict(geometry_types=()),
    )
    nullable = list(geoms)
    nullable[1] = None
    nullable[3] = None
    sources["null_geoms"] = _write_source(
        tmp / "null_geoms.parquet",
        pa.table({"id": list(range(5)), "geometry": nullable}),
        _geo_dict(),
    )
    sources["zm"] = _write_source(
        tmp / "zm.parquet",
        pa.table(
            {
                "id": list(range(5)),
                "geometry": [
                    _wkb_point_zm(x, y, 10.0 + i, float(i)) for i, (x, y) in enumerate(POINTS)
                ],
            }
        ),
        _geo_dict(geometry_types=("Point ZM",)),
    )
    sources["geom_named"] = _write_source(
        tmp / "geom_named.parquet",
        pa.table({"id": list(range(5)), "geom": geoms}),
        _geo_dict(column="geom"),
    )
    sources["multi_row_group"] = _write_source(
        tmp / "multi_row_group.parquet",
        pa.table({"id": list(range(40)), "geometry": geoms * 8}),
        _geo_dict(),
        row_group_size=10,
    )
    return sources


# Rows, geometry column and declared types per A2 shape.
SHAPE_EXPECTATIONS = {
    "normal": (5, "geometry", ("Point",), BBOX),
    "zero_row": (0, "geometry", (), None),
    "null_geoms": (5, "geometry", ("Point",), BBOX),
    "zm": (5, "geometry", ("Point ZM",), BBOX),
    "geom_named": (5, "geom", ("Point",), BBOX),
    "multi_row_group": (40, "geometry", ("Point",), BBOX),
}


# ---------------------------------------------------------------------------
# The contract assertion helper
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Expect:
    """The observable contract one write must satisfy."""

    rows: int
    geometry_column: str = "geometry"
    #: geo metadata "version" string, or None when no 'geo' key may be written
    geo_version: str | None = "1.1.0"
    geometry_types: tuple[str, ...] | None = ("Point",)
    bbox: tuple[float, float, float, float] | None = BBOX
    encoding: str | None = "WKB"
    #: columns that must survive the write (extras such as a covering bbox are allowed)
    columns: tuple[str, ...] = ()
    #: file-level kv keys that must be present besides 'geo'
    kv_keys: tuple[str, ...] = ()
    #: exact row-group count, when the caller pinned row_group_rows
    row_groups: int | None = None


def _geo_metadata(path: str) -> dict | None:
    pf = pq.ParquetFile(path)
    try:
        raw = (pf.schema_arrow.metadata or {}).get(b"geo")
    finally:
        pf.close()
    return json.loads(raw) if raw else None


def _uses_native_geometry_type(path: str) -> bool:
    """True when any column carries a native Parquet GEOMETRY/GEOGRAPHY type.

    Read from the Parquet logical types rather than the Arrow schema so the
    answer does not shift with geoarrow.pyarrow's extension registration.
    """
    pf = pq.ParquetFile(path)
    try:
        logical = [str(pf.schema.column(i).logical_type) for i in range(len(pf.schema))]
    finally:
        pf.close()
    return any("Geometry" in lt or "Geography" in lt for lt in logical)


def assert_valid_geoparquet_output(path: str, expect: Expect) -> list[str]:
    """Assert the write contract; return the oracle's FAILED check names.

    Structural violations raise immediately. Oracle failures are *returned* so
    the caller can adjudicate them against the known-divergence tables instead
    of losing the structural coverage to an early xfail.
    """
    pf = pq.ParquetFile(path)
    try:
        assert pf.metadata.num_rows == expect.rows, (
            f"row conservation broken: wrote {pf.metadata.num_rows} rows, expected {expect.rows}"
        )
        column_names = list(pf.schema_arrow.names)
        kv = pf.schema_arrow.metadata or {}
        row_group_rows = [
            pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups)
        ]
        num_row_groups = pf.metadata.num_row_groups
    finally:
        pf.close()

    missing = [c for c in expect.columns if c not in column_names]
    assert not missing, f"write dropped input columns {missing} (kept {column_names})"

    # Row-group structure: every row must live in exactly one row group, and a
    # pinned row-group count must be honored.
    assert sum(row_group_rows) == expect.rows, (
        f"row groups hold {sum(row_group_rows)} rows but the file claims {expect.rows}"
    )
    if expect.row_groups is not None:
        assert num_row_groups == expect.row_groups, (
            f"expected {expect.row_groups} row groups, got {num_row_groups} ({row_group_rows})"
        )

    missing_kv = [k for k in expect.kv_keys if k.encode() not in kv]
    assert not missing_kv, (
        f"kv metadata {missing_kv} dropped by the write (kept {sorted(k.decode() for k in kv)})"
    )

    geo = json.loads(kv[b"geo"]) if b"geo" in kv else None
    if expect.geo_version is None:
        assert geo is None, (
            f"expected no 'geo' metadata key, got version {geo.get('version')!r}"  # type: ignore[union-attr]
        )
    else:
        assert geo is not None, "write produced no 'geo' metadata key"
        assert geo.get("version") == expect.geo_version, (
            f"geo version {geo.get('version')!r}, expected {expect.geo_version!r}"
        )
        assert geo.get("primary_column") == expect.geometry_column, (
            f"primary_column {geo.get('primary_column')!r}, expected {expect.geometry_column!r}"
        )
        column_meta = geo.get("columns", {}).get(expect.geometry_column)
        assert column_meta is not None, (
            f"geo.columns has no entry for {expect.geometry_column!r} "
            f"(has {sorted(geo.get('columns', {}))})"
        )
        if expect.encoding is not None:
            assert column_meta.get("encoding") == expect.encoding, (
                f"encoding {column_meta.get('encoding')!r}, expected {expect.encoding!r}"
            )
        if expect.geometry_types is not None:
            assert tuple(column_meta.get("geometry_types", ())) == expect.geometry_types, (
                f"geometry_types {column_meta.get('geometry_types')!r}, "
                f"expected {list(expect.geometry_types)!r}"
            )
        if expect.bbox is not None:
            written = column_meta.get("bbox")
            assert written is not None, "geo metadata carries no bbox"
            assert written == pytest.approx(list(expect.bbox), abs=1e-9), (
                f"bbox {written!r}, expected {list(expect.bbox)!r}"
            )

    result = validate_geoparquet(path, validate_data=True, sample_size=0)
    # No platform is excused: DuckDB <= 1.5.1 misread native-geo statistics on
    # Windows (#721), but gpio now floors DuckDB at 1.5.5, which reads them
    # correctly, so the oracle is enforced everywhere.
    return [c.name for c in result.checks if c.status == CheckStatus.FAILED]


def _adjudicate(failed: list[str], divergence: Divergence | None) -> None:
    """Xfail on a recorded divergence; fail hard on anything new or stale."""
    if failed:
        assert divergence is not None, f"unexpected validation failures: {failed}"
        prefixes = divergence.oracle_prefixes
        assert prefixes, f"divergence records no oracle_prefixes but the oracle failed: {failed}"
        unexpected = [n for n in failed if not any(n.startswith(p) for p in prefixes)]
        assert not unexpected, f"validation failures beyond the recorded divergence: {unexpected}"
        pytest.xfail(divergence.reason)
    if divergence is not None and not divergence.state_dependent:
        pytest.fail(
            "recorded divergence no longer reproduces — the write paths now agree. "
            f"Remove the entry so it cannot mask a regression.\nEntry: {divergence.reason}"
        )


def _xfail_if_expected(exc: AssertionError, divergence: Divergence | None) -> None:
    """Absorb a structural failure only when it is the one we recorded.

    Any other AssertionError — a lost row, a CLI crash, a dropped column — is
    re-raised, so a case carrying an xfail reason cannot silently swallow a new
    regression.
    """
    if divergence is None or divergence.assertion_contains is None:
        raise exc
    if divergence.assertion_contains not in str(exc):
        raise AssertionError(
            f"assertion failed on a case with a recorded divergence, but the "
            f"message does not match it.\n"
            f"  expected to contain: {divergence.assertion_contains!r}\n"
            f"  actual: {exc}\n"
            f"  divergence: {divergence.reason}"
        ) from exc
    pytest.xfail(divergence.reason)


# ---------------------------------------------------------------------------
# The four write paths, behind one signature
# ---------------------------------------------------------------------------


def _write_via_query(source: str, out: str, version: str, **kwargs) -> None:
    con = get_duckdb_connection()
    try:
        metadata, _ = get_parquet_metadata(source)
        write_parquet_with_metadata(
            con,
            f"SELECT * FROM read_parquet('{Path(source).as_posix()}')",
            out,
            original_metadata=metadata,
            geoparquet_version=version,
            input_file=source,
            **kwargs,
        )
    finally:
        con.close()


def _write_via_table(source: str, out: str, version: str, **kwargs) -> None:
    write_geoparquet_table(pq.read_table(source), out, geoparquet_version=version, **kwargs)


def _write_via_api(source: str, out: str, version: str, **kwargs) -> None:
    api_read(source).write(out, geoparquet_version=version, **kwargs)


def _write_via_cli(source: str, out: str, version: str | None, **kwargs) -> None:
    args = ["convert", "geoparquet", source, out, "--skip-hilbert"]
    # Auto mode on the CLI is the *absence* of the flag, not a None value.
    if version is not None:
        args += ["--geoparquet-version", version]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, f"CLI failed ({result.exit_code}):\n{result.output}"


WRITE_PATHS = {
    "write_parquet_with_metadata": _write_via_query,
    "write_geoparquet_table": _write_via_table,
    "api_table_write": _write_via_api,
    "cli": _write_via_cli,
}


# ---------------------------------------------------------------------------
# A1 — path x version
# ---------------------------------------------------------------------------


#: Paths that preserve the input's non-geo kv metadata, so A1 can exercise the
#: passing direction of the helper's kv leg. The two that drop it are recorded
#: as divergences and asserted in A3 instead.
KV_PRESERVING_PATHS = ("write_parquet_with_metadata", "write_geoparquet_table")


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("path_name", sorted(WRITE_PATHS))
def test_a1_path_version_contract(path_name, version, normal_source, tmp_path):
    """Every write path must satisfy the same contract for the same input."""
    out = tmp_path / f"{path_name}_{version}.parquet"
    expect = Expect(
        rows=5,
        geo_version=GEO_VERSION_FOR[version],
        columns=("id", "name", "geometry"),
        kv_keys=tuple(EXTRA_KV) if path_name in KV_PRESERVING_PATHS else (),
    )
    divergence = KNOWN_DIVERGENCES.get((path_name, version))

    try:
        WRITE_PATHS[path_name](normal_source, str(out), version)
        failed = assert_valid_geoparquet_output(str(out), expect)
    except AssertionError as exc:
        _xfail_if_expected(exc, divergence)
        raise  # unreachable; _xfail_if_expected always raises or xfails
    _adjudicate(failed, divergence)


def _auto_resolution(path: str) -> tuple[str | None, bool]:
    """(declared geo version, uses a native Parquet geometry logical type)."""
    geo = _geo_metadata(path)
    return (geo.get("version") if geo else None, _uses_native_geometry_type(path))


@pytest.mark.parametrize("source_kind", ["wkb_1_1", "native_geo_only"])
@pytest.mark.parametrize("path_name", sorted(WRITE_PATHS))
def test_a1_auto_version_is_self_consistent(path_name, source_kind, auto_sources, tmp_path):
    """Auto version mode must produce an internally consistent file.

    This is the default users hit (no ``--geoparquet-version``). Rather than
    pinning a version — auto is *supposed* to follow the input — this asserts
    self-consistency: the declared version and the actual on-disk encoding must
    agree. A 1.x declaration over a native GEOMETRY logical type, or a 2.0
    declaration over plain WKB, is a file that lies about itself. All four
    paths satisfy this today, in both directions, so it is a regression guard.

    ``native_geo_only`` is #600's repro: no ``geo`` key, native logical type.
    """
    source, rows = auto_sources[source_kind]
    out = tmp_path / f"auto_{path_name}_{source_kind}.parquet"
    WRITE_PATHS[path_name](source, str(out), None)

    version, native = _auto_resolution(str(out))
    declared_2_0 = version is not None and version.startswith("2.")
    # Only the 2.0-native outputs can trip the state-dependent CRS check.
    divergence = AUTO_2_0_CRS_STATE if declared_2_0 else None

    expect = Expect(
        rows=rows,
        # Self-referential on purpose: read back, not pinned — auto follows input.
        geo_version=version,
        geometry_types=None,
        bbox=None,
        encoding=None,
        columns=("id",),
    )
    failed = assert_valid_geoparquet_output(str(out), expect)

    # The invariant, stated independently of the oracle.
    if version is not None:
        if declared_2_0:
            assert native, (
                f"{path_name} auto-resolved to GeoParquet {version} but wrote no "
                "native Parquet GEOMETRY logical type"
            )
        else:
            assert not native, (
                f"{path_name} auto-resolved to GeoParquet {version} but wrote a "
                "native Parquet GEOMETRY logical type, which only 2.0 permits"
            )
    _adjudicate(failed, divergence)


@pytest.mark.parametrize("source_kind", ["wkb_1_1", "native_geo_only"])
def test_a1_auto_version_agrees_across_paths(source_kind, auto_sources, tmp_path):
    """All four paths must auto-resolve the same input to the same thing.

    Auto is the default, so a disagreement here means the same file written
    "the same way" through two supported entry points differs in version *and*
    in whether the native geometry type survives. This is the single most
    important cell for the write facade.
    """
    source, _ = auto_sources[source_kind]
    resolved = {}
    for path_name, write in sorted(WRITE_PATHS.items()):
        out = tmp_path / f"agree_{path_name}_{source_kind}.parquet"
        write(source, str(out), None)
        resolved[path_name] = _auto_resolution(str(out))

    divergence = AUTO_VERSION_NATIVE_DOWNGRADE if source_kind == "native_geo_only" else None
    distinct = set(resolved.values())
    try:
        assert len(distinct) == 1, (
            "auto-resolved differently across write paths for the same input: "
            + ", ".join(
                f"{name}={version or 'no geo key'}{' native' if native else ' WKB'}"
                for name, (version, native) in sorted(resolved.items())
            )
        )
    except AssertionError as exc:
        _xfail_if_expected(exc, divergence)
        raise
    if divergence is not None:
        pytest.fail(
            "recorded divergence no longer reproduces — the write paths now agree. "
            f"Remove the entry so it cannot mask a regression.\nEntry: {divergence.reason}"
        )


# ---------------------------------------------------------------------------
# A2 — strategy x shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("shape", sorted(SHAPE_EXPECTATIONS))
@pytest.mark.parametrize("strategy", STRATEGIES)
def test_a2_strategy_shape_contract(strategy, shape, shape_sources, tmp_path):
    """Every strategy must produce the same file for every input shape.

    The no-geometry shape is covered by tests/test_write_strategies.py and is
    deliberately not repeated here.
    """
    rows, geometry_column, geometry_types, bbox = SHAPE_EXPECTATIONS[shape]
    source = shape_sources[shape]
    out = tmp_path / f"{strategy}_{shape}.parquet"
    # Pin the row-group count only where there is more than one group's worth.
    #
    # An exact count is safe here but not universally: DuckDB's COPY TO can
    # split differently under multi-threaded writes, so the exact-count leg of
    # the contract is only meaningful because this fixture is 40 rows written
    # by a single writer. Assert ranges, not equality, if this ever grows.
    row_group_rows = 10 if shape == "multi_row_group" else None

    _write_via_query(
        source, str(out), "1.1", write_strategy=strategy, row_group_rows=row_group_rows
    )
    expect = Expect(
        rows=rows,
        geometry_column=geometry_column,
        geo_version="1.1.0",
        geometry_types=geometry_types,
        bbox=bbox,
        columns=("id", geometry_column),
        row_groups=4 if shape == "multi_row_group" else None,
    )
    divergence = KNOWN_STRATEGY_DIVERGENCES.get((strategy, shape))
    try:
        failed = assert_valid_geoparquet_output(str(out), expect)
    except AssertionError as exc:
        _xfail_if_expected(exc, divergence)
        raise  # unreachable; _xfail_if_expected always raises or xfails
    _adjudicate(failed, divergence)


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_a2_row_group_coarsen_contract(strategy, shape_sources, tmp_path):
    """A request LARGER than the source's row groups must coarsen them (#697).

    The ``multi_row_group`` leg of the A2 matrix above only exercises the
    splitting direction (source groups of 10, request 10). The rewrite in
    ``disk-rewrite`` issued one ``write_table`` per *source* row group, and each
    of those starts a new group, so it could make groups smaller but never
    larger: 40 groups of 10 came back as 40 groups of 10, silently ignoring a
    ``row_group_rows=40`` request. This is the same cross-strategy divergence
    family as #689, so it is pinned for all four.
    """
    source = shape_sources["multi_row_group"]
    out = tmp_path / f"{strategy}_coarsen.parquet"

    _write_via_query(source, str(out), "1.1", write_strategy=strategy, row_group_rows=40)

    expect = Expect(
        rows=40,
        geometry_column="geometry",
        geo_version="1.1.0",
        geometry_types=("Point",),
        bbox=BBOX,
        columns=("id", "geometry"),
        row_groups=1,
    )
    divergence = KNOWN_STRATEGY_DIVERGENCES.get((strategy, "coarsen"))
    try:
        failed = assert_valid_geoparquet_output(str(out), expect)
    except AssertionError as exc:
        _xfail_if_expected(exc, divergence)
        raise  # unreachable; _xfail_if_expected always raises or xfails
    _adjudicate(failed, divergence)


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_a2_geoarrow_version_agrees_across_strategies(strategy, shape_sources, tmp_path):
    """1.1-geoarrow must not depend on which strategy the caller asked for.

    The issue records "1.1-geoarrow is only writable by the streaming
    strategy" as a known asymmetry. That is no longer true:
    ``write_parquet_with_metadata`` auto-routes a WKB input to arrow-streaming
    whatever the caller asked for, so all four requests now produce the same
    natively encoded file, and since #715 taught the validator the GeoArrow
    encodings GeoParquet 1.1 permits, the result validates clean too.
    """
    out = tmp_path / f"geoarrow_{strategy}.parquet"
    _write_via_query(shape_sources["normal"], str(out), "1.1-geoarrow", write_strategy=strategy)
    expect = Expect(
        rows=5,
        geo_version="1.1.0",
        encoding="point",
        columns=("id",),  # the WKB column is replaced by a nested x/y struct
    )
    failed = assert_valid_geoparquet_output(str(out), expect)
    assert failed == [], f"1.1-geoarrow via {strategy} no longer validates: {failed}"


# ---------------------------------------------------------------------------
# A3 — kv / settings passthrough
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path_name", sorted(WRITE_PATHS))
def test_a3_input_kv_metadata_survives(path_name, normal_source, tmp_path):
    """Non-geo file-level kv metadata on the input must reach the output."""
    out = tmp_path / f"kv_{path_name}.parquet"
    WRITE_PATHS[path_name](normal_source, str(out), "1.1")

    pf = pq.ParquetFile(out)
    try:
        written = {k.decode() for k in (pf.schema_arrow.metadata or {})}
    finally:
        pf.close()

    # No allowlist here any more: #710 gave `gpio convert geoparquet` and
    # `Table.write` the same preservation every other path already had, so all
    # four paths must keep the input's sidecar keys unconditionally.
    dropped = sorted(set(EXTRA_KV) - written)
    assert not dropped, f"{path_name} dropped input kv metadata {dropped}"


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_a3_table_write_kv_survives_every_strategy(strategy, normal_source, tmp_path):
    """Table.write's kv preservation must not depend on the strategy.

    It used to. ``Table.write`` calls ``write_from_table`` directly, bypassing
    the preservation merge in ``write_parquet_with_metadata``, so it inherited
    whatever each strategy happened to do: the Arrow strategies copied non-geo
    schema metadata across and kept the keys, while the DuckDB COPY paths
    rebuilt the kv block from scratch and lost them. That made a caller's
    sidecar payload survive or vanish according to a performance setting.

    #710 pushed the input's preserved keys down to every strategy, so this now
    pins agreement rather than documenting the split.
    """
    out = tmp_path / f"api_kv_{strategy}.parquet"
    _write_via_api(normal_source, str(out), "1.1", write_strategy=strategy)

    pf = pq.ParquetFile(out)
    try:
        written = {k.decode() for k in (pf.schema_arrow.metadata or {})}
    finally:
        pf.close()

    dropped = sorted(set(EXTRA_KV) - written)
    assert not dropped, (
        f"Table.write with the {strategy} strategy dropped input kv metadata "
        f"{dropped}; preservation must not depend on the strategy"
    )


def test_a3_extra_kv_metadata_written(normal_source, tmp_path):
    """extra_kv_metadata lands alongside 'geo' and does not evict input keys."""
    out = tmp_path / "extra_kv.parquet"
    _write_via_query(
        normal_source,
        str(out),
        "1.1",
        extra_kv_metadata={"stac": json.dumps({"type": "Feature"})},
    )
    pf = pq.ParquetFile(out)
    try:
        kv = {k.decode(): v.decode() for k, v in (pf.schema_arrow.metadata or {}).items()}
    finally:
        pf.close()

    assert "stac" in kv, f"extra_kv_metadata not written (keys: {sorted(kv)})"
    assert json.loads(kv["stac"]) == {"type": "Feature"}
    assert set(EXTRA_KV).issubset(kv), "extra_kv_metadata evicted the input's own kv keys"
    assert "geo" in kv


# ---------------------------------------------------------------------------
# Normalized geo-dict snapshots — canaries for silent field drift
# ---------------------------------------------------------------------------


def _normalize_geo(geo: dict | None) -> dict:
    """Stable, diff-friendly view: sorted keys, bbox rounded, CRS collapsed."""
    if geo is None:
        return {"geo": None}
    normalized = {k: v for k, v in geo.items() if k != "columns"}
    columns = {}
    for name, meta in sorted(geo.get("columns", {}).items()):
        entry = {}
        for key, value in sorted(meta.items()):
            if key == "bbox" and value is not None:
                entry[key] = [round(float(v), 6) for v in value]
            elif key == "crs":
                # PROJJSON is huge and pyproj-version-sensitive; its identity is
                # covered by tests/test_crs_*.py. Record only presence + name.
                entry[key] = value.get("id") if isinstance(value, dict) else value
            else:
                entry[key] = value
        columns[name] = entry
    normalized["columns"] = columns
    return normalized


@pytest.mark.parametrize("version", VERSIONS)
def test_geo_metadata_snapshot(version, normal_source, tmp_path):
    """A field silently added to or removed from geo metadata must show up here."""
    out = tmp_path / f"snapshot_{version}.parquet"
    _write_via_query(normal_source, str(out), version)
    actual = _normalize_geo(_geo_metadata(str(out)))
    rendered = json.dumps(actual, indent=2, sort_keys=True) + "\n"

    snapshot = SNAPSHOT_DIR / f"write_contract_geo_{version}.json"
    if UPDATE_SNAPSHOTS and IN_CI:
        pytest.fail(
            "GPIO_UPDATE_SNAPSHOT=1 is set in CI. Refreshing a snapshot there "
            "would rewrite the canary to match the build's own output and pass "
            "unconditionally. Refresh locally and commit the result instead."
        )

    if UPDATE_SNAPSHOTS or not snapshot.exists():
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        snapshot.write_text(rendered)
        if not UPDATE_SNAPSHOTS:
            pytest.fail(f"snapshot {snapshot.name} was missing; wrote it — re-run to verify")
        return

    assert rendered == snapshot.read_text(), (
        f"geo metadata for version {version} drifted from {snapshot.name}. "
        "If the change is intended, refresh with "
        "GPIO_UPDATE_SNAPSHOT=1 uv run pytest tests/test_write_contract.py"
    )


# ---------------------------------------------------------------------------
# Global-state independence
# ---------------------------------------------------------------------------
#
# This suite's highest-value finding was that the arrow-streaming strategy's
# on-disk geometry type depended on whether anything in the process had
# imported geoarrow.pyarrow. It is fixed (#688 by gpio #707), and the
# regression test now lives in tests/test_streaming_write_determinism.py::
# test_streaming_output_is_independent_of_geoarrow_import.
#
# The version that lived here is deliberately not kept alongside it. It was
# strictly weaker -- it compared the geometry *type string* from two separate
# subprocesses, where the surviving test compares the SHA-256 of the whole
# written file -- and it carried a skipif for PYTEST_XDIST_WORKER, so under the
# -n 4 the CI fast lane uses it never executed at all. Two tests of one
# invariant, one of which cannot run where it matters, is worse than one.
