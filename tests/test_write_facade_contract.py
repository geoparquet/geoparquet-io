"""Contracts the write-facade refactor (Deep Review 3.3) has to turn green.

Two open bugs share one shape: the same fact about an output file is decided
independently in N write paths, and the paths disagree. Patching each path now
would mean N edits that the facade would then have to unpick, so the fix belongs
inside the refactor -- and the refactor should be a pure move, not a move that
also quietly changes behavior. So the behavior is pinned here first.

- #773: ``geoparquet_version="parquet-geo-only"`` on a table whose geometry
  column has been projected away. Three of the four write strategies return
  early and pass the carried ``geo`` key straight through.
- #600: auto version mode (no ``--geoparquet-version``) on a parquet-geo-only
  input. ``convert`` resolves the version from the file and keeps it native;
  ``sort``/``extract``/``partition`` fall through to a 1.1.0 WKB default and
  strip the native Parquet GEOMETRY logical type.

Every ``xfail`` in this file is ``strict=True``. That is the point: when the
facade lands and routes these paths through one place, the tests xpass, strict
mode turns the xpass into a failure, and the facade PR has to delete the marker.
That deletion is the signal -- a green suite that silently stopped testing
anything is not.

Two kinds of test live here alongside the xfails:

- **controls** (not marked): a path that already behaves correctly today. They
  prove the assertion is reachable, so an xfail next to them is a real defect
  rather than an oracle that can never pass.
- **current-behavior pins** (not marked, and named ``_today``): they assert what
  the code does *now*, not what it should do. They exist so the facade cannot
  change an unrelated mode without someone noticing.

Refs: https://github.com/geoparquet/geoparquet-io/issues/773
Refs: https://github.com/geoparquet/geoparquet-io/issues/600
"""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.api.table import Table
from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

# ---------------------------------------------------------------------------
# #773 -- parquet-geo-only leaks the carried `geo` key when geometry is absent
# ---------------------------------------------------------------------------

WRITE_STRATEGIES = ["in-memory", "streaming", "disk-rewrite", "duckdb-kv"]

CARRIED_GEO = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
}

#: An unrelated sidecar key, of the kind fiboa and STAC writers attach. It has
#: nothing to do with GeoParquet, so no version choice may drop it.
SIDECAR_KEY = b'{"schemas":["example"]}'


def _attributes_only_with_geo() -> pa.Table:
    """A table whose geometry column was projected away, still carrying ``geo``.

    This is the #701/#753 situation: the ``geo`` key describes a
    ``primary_column`` named ``geometry`` that the table no longer has.
    """
    return pa.table({"id": pa.array([1, 2])}).replace_schema_metadata(
        {b"geo": json.dumps(CARRIED_GEO).encode(), b"fiboa": SIDECAR_KEY}
    )


def _kv_keys(path) -> list[str]:
    """Key-value metadata keys of a written file, minus pyarrow's own bookkeeping.

    ``ARROW:schema`` is round-trip plumbing rather than a payload, so it is not
    part of what a version choice is allowed to decide.
    """
    metadata = pq.ParquetFile(str(path)).metadata.metadata or {}
    return sorted(k.decode() for k in metadata if k != b"ARROW:schema")


#: Was: the three strategies whose ``write_from_table`` guarded its metadata
#: work on the geometry column being present and otherwise wrote
#: ``table.schema`` verbatim (arrow_memory.py, arrow_streaming.py,
#: disk_rewrite.py). All four now route their file-level keys through
#: ``parquet_writer.apply_output_kv_metadata``, which runs before any such
#: guard, so the xfail markers are gone and the assertion is unchanged.
@pytest.mark.parametrize("strategy", WRITE_STRATEGIES)
def test_parquet_geo_only_drops_geo_and_keeps_sidecar(strategy, tmp_path):
    """Explicit parquet-geo-only writes no ``geo`` key and keeps unrelated keys.

    ``parquet-geo-only`` means "carry no GeoParquet metadata". A ``geo`` key in
    the output would name a ``primary_column`` the file does not have and would
    declare a GeoParquet version for a file explicitly asked to declare none.
    An unrelated sidecar key is not GeoParquet metadata, so it survives: the
    single correct answer for all four strategies is ``['fiboa']``.
    """
    out = tmp_path / f"{strategy}.parquet"

    Table(_attributes_only_with_geo()).write(
        out, geoparquet_version="parquet-geo-only", write_strategy=strategy
    )

    assert _kv_keys(out) == ["fiboa"]


@pytest.mark.parametrize("strategy", WRITE_STRATEGIES)
def test_auto_version_metadata_keys_today(strategy, tmp_path):
    """Pins *current* auto-mode behavior, not desired behavior.

    #753 deliberately scoped its fix to an *explicit* ``parquet-geo-only``
    request and left auto mode (``geoparquet_version=None``) alone, so the
    carried ``geo`` key surviving here is today's contract, not a bug this file
    claims. The assertion exists so that the facade refactor cannot change auto
    mode as a side effect of fixing the explicit mode above: if it does, this
    test fails and the change has to be argued for on purpose.
    """
    out = tmp_path / f"auto_{strategy}.parquet"

    Table(_attributes_only_with_geo()).write(out, geoparquet_version=None, write_strategy=strategy)

    expected = ["fiboa"] if strategy == "duckdb-kv" else ["fiboa", "geo"]
    assert _kv_keys(out) == expected


# ---------------------------------------------------------------------------
# #600 -- auto version mode downgrades native-geo-only inputs to 1.1 WKB
# ---------------------------------------------------------------------------


@pytest.fixture
def parquet_geo_only_file(tmp_path):
    """A native-geo-only input: Parquet GEOMETRY logical type, no ``geo`` key.

    ``GEOPARQUET_VERSION 'NONE'`` is how DuckDB spells parquet-geo-only, the
    same spelling ``tests/conftest.py`` uses. ``grp`` is a low-cardinality
    string so ``partition string`` has something to split on.
    """
    path = tmp_path / "pgo.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"""
            COPY (
                SELECT i AS id, 'g' || (i % 3) AS grp, ST_Point(i, i) AS geometry
                FROM range(50) t(i)
            ) TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')
            """
        )
    finally:
        con.close()

    # Non-vacuity: the fixture must really be native-geo-only, or every
    # assertion below is measuring the wrong thing.
    assert b"geo" not in (pq.ParquetFile(str(path)).metadata.metadata or {})
    assert _is_native_geometry(path), "fixture did not get a native GEOMETRY type"
    return path


def _is_native_geometry(path, column: str = "geometry") -> bool:
    """True when ``column`` carries a Parquet GEOMETRY/GEOGRAPHY logical type.

    Read without the spatial extension so the answer is the file's own schema
    rather than something DuckDB reconstructed, the same way
    ``tests/test_write_strategies.py`` reads carriers.
    """
    con = get_duckdb_connection(load_spatial=False)
    try:
        rows = con.execute(
            f"SELECT name, logical_type FROM parquet_schema({sql_path(str(path))})"
        ).fetchall()
    finally:
        con.close()
    return any(name == column and logical and "Geometry" in str(logical) for name, logical in rows)


def _geo_version(path) -> str | None:
    metadata = pq.ParquetFile(str(path)).metadata.metadata or {}
    if b"geo" not in metadata:
        return None
    return json.loads(metadata[b"geo"].decode("utf-8")).get("version")


def _assert_native_geo_preserved(path) -> None:
    """The #600 contract: auto mode upgrades native-geo-only to native 2.0.

    This is what ``resolve_geoparquet_version_from_file`` already gives
    ``convert`` and ``reproject`` (PR #594). Both halves matter: a 2.0 version
    string over a WKB column would be a spec violation, and a native column
    under a 1.1 declaration would be one too.
    """
    version = _geo_version(path)
    assert version is not None and version.startswith("2.0"), (
        f"expected a 2.0.x geo version, got {version!r}"
    )
    assert _is_native_geometry(path), (
        "geometry column lost its native Parquet GEOMETRY logical type"
    )


def _run_cli(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output


def _downgrade_param(command: str, *args):
    return pytest.param(command, args, id=command.replace(" ", "-"))


#: Was: every entry point but ``convert`` passed ``geoparquet_version=None``
#: straight to ``write_parquet_with_metadata``, whose
#: ``extract_version_from_metadata`` returns None for a native-geo-only input,
#: so the write defaulted to 1.1.0 WKB and stripped the native GEOMETRY logical
#: type. ``write_parquet_with_metadata`` now resolves auto mode through
#: ``parquet_writer.resolve_output_geoparquet_version``, which consults the
#: input file the way ``convert`` always did, so the xfail markers are gone and
#: the assertions are unchanged.
@pytest.mark.parametrize(
    ("command", "extra_args"),
    [
        # Control: convert already resolved the version from the input file, so
        # this param proves the oracle above is reachable -- and pins the
        # behavior every other entry point now reaches.
        _downgrade_param("convert geoparquet"),
        _downgrade_param("sort hilbert"),
        _downgrade_param("sort column", "id"),
        _downgrade_param("sort quadkey"),
        _downgrade_param("extract geoparquet", "--limit", "10"),
    ],
)
def test_auto_mode_preserves_native_geo(command, extra_args, parquet_geo_only_file, tmp_path):
    """Auto mode must not silently rewrite a native-geo-only input as 1.1 WKB.

    No ``--geoparquet-version`` is passed, which is the mode the shared help
    text documents as "preserve the input's version". For a parquet-geo-only
    input that means native 2.0, which is what ``convert`` does.
    """
    out = tmp_path / "out.parquet"

    _run_cli(*command.split(), parquet_geo_only_file, out, *extra_args)

    _assert_native_geo_preserved(out)


def test_partition_auto_mode_preserves_native_geo(parquet_geo_only_file, tmp_path):
    """The partition staging write took the same fall-through (staging.py).

    It could not be fixed the same way as the four above: a partition write sees
    only the staging file, which is always written ``GEOPARQUET_VERSION 'NONE'``,
    so resolving auto mode from what it reads would call every input
    native-geo-only. The driver resolves once from the real input instead and
    hands every partition the concrete answer.

    ``--force`` only silences the tiny-partition advisory; 50 rows over three
    groups is deliberately small so the test stays fast.
    """
    out_dir = tmp_path / "parts"

    _run_cli("partition", "string", parquet_geo_only_file, out_dir, "--column", "grp", "--force")

    written = sorted(out_dir.rglob("*.parquet"))
    assert written, "partition wrote no files"
    for part in written:
        _assert_native_geo_preserved(part)
