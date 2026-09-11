"""One owner for the auto-version decision -- and what still is not covered.

``tests/test_write_facade_contract.py`` pins the contracts the facade turned
green. This file pins the three things review found *around* them, each a case
where something other than the facade still answers the facade's own question,
or where the facade's answer reaches an output the input never described.

* **A stale second owner.** ``hilbert_order._resolve_output_version`` decides
  which version ``gpio sort hilbert`` and ``gpio sort str`` *say* they are
  writing, and it made that decision with its own copy of the pre-facade rule.
  On a native-geo-only input it said 1.1 and advised ``--geoparquet-version
  2.0`` over a file it had just written as 2.0 -- #738's bug from the other
  side, and the exact input class the facade exists to fix.
* **A version resolved from a lossy intermediate.** ``gpio sort quadkey`` and
  the five index-adding ``gpio partition`` drivers rewrite their input into a
  scratch file first (to add the index column), and that scratch write does not
  preserve native geo or the input's CRS. Resolving the *version* from the
  user's real file while reading the *data* from the scratch file is how
  ``sort quadkey`` came to stamp ``OGC:CRS84`` on an EPSG:5070 input.
* **A behaviour claimed wholesale.** Only ``partition string`` and
  ``partition admin`` read the user's file directly, so only they deliver the
  #600 fix; the index-adding drivers still write 1.1 WKB.

The gaps are marked ``xfail(strict=True)`` rather than left untested or
asserted as correct: strict mode means whoever fixes the underlying scratch-file
write has to come here and delete the marker, which is the signal. Certifying
them green -- which the contract file's ``crs=<null>`` fixture does by
omission -- is what let ``gpio check spec`` print
``✓ valid inline PROJJSON CRS`` over the wrong CRS.

Refs: https://github.com/geoparquet/geoparquet-io/issues/600
Refs: https://github.com/geoparquet/geoparquet-io/issues/738
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.crs_utils import source_crs_string
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

#: A native-geo-only input whose CRS is *not* the default. The distinction the
#: contract file's DuckDB-written fixture cannot make: it carries ``crs=<null>``,
#: which already means OGC:CRS84, so a path that loses the CRS and a path that
#: keeps it produce the same file and every assertion passes either way.
PROJECTED_PGO = Path(__file__).parent / "data" / "fields_pgo_5070_snappy.parquet"

#: What ``PROJECTED_PGO`` declares, as gpio's own reader reports it.
PROJECTED_CRS = "EPSG:5070"


def _run_cli(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output


def _run_cli_output(*args) -> str:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output
    return result.output


def _geo_version(path) -> str | None:
    metadata = pq.ParquetFile(str(path)).metadata.metadata or {}
    if b"geo" not in metadata:
        return None
    return json.loads(metadata[b"geo"].decode("utf-8")).get("version")


def _is_native_geometry(path, column: str = "geometry") -> bool:
    """True when ``column`` carries a Parquet GEOMETRY/GEOGRAPHY logical type."""
    con = get_duckdb_connection(load_spatial=False)
    try:
        rows = con.execute(
            f"SELECT name, logical_type FROM parquet_schema({sql_path(str(path))})"
        ).fetchall()
    finally:
        con.close()
    return any(name == column and logical and "Geometry" in str(logical) for name, logical in rows)


@pytest.fixture(scope="module")
def native_geo_only(tmp_path_factory):
    """A native-geo-only input with a low-cardinality column to partition on.

    Same shape as the contract file's fixture, with coordinates a quadkey/H3/S2
    index can actually bucket, and enough rows that a partition run produces
    more than one file.
    """
    path = tmp_path_factory.mktemp("version_owner") / "pgo.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"""
            COPY (
                SELECT i AS id, 'g' || (i % 3) AS grp,
                       ST_Point(i % 170 - 85, i % 80 - 40) AS geometry
                FROM range(300) t(i)
            ) TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')
            """
        )
    finally:
        con.close()

    assert b"geo" not in (pq.ParquetFile(str(path)).metadata.metadata or {})
    assert _is_native_geometry(path), "fixture did not get a native GEOMETRY type"
    return path


# ---------------------------------------------------------------------------
# The advisory's owner must be the facade, not a copy of the rule it replaced
# ---------------------------------------------------------------------------


def test_the_fixture_really_declares_a_projected_crs():
    """Non-vacuity: without this, every CRS assertion below could pass empty."""
    assert source_crs_string(str(PROJECTED_PGO), False) == PROJECTED_CRS
    assert _geo_version(PROJECTED_PGO) is None
    assert _is_native_geometry(PROJECTED_PGO)


def test_the_sort_advisory_resolves_the_version_the_write_will_use(native_geo_only):
    """``_resolve_output_version`` must answer what the facade answers.

    Its docstring claimed it "mirrors ``write_parquet_with_metadata``". After
    the facade it did not: it read the carried ``geo`` key, which a
    native-geo-only input does not have, and fell back to the 1.1 default while
    the write resolved 2.0 from the file itself.
    """
    from geoparquet_io.core.hilbert_order import _resolve_output_version

    assert _resolve_output_version(str(native_geo_only), None, verbose=False) == "2.0"


@pytest.mark.parametrize("command", ["hilbert", "str"])
def test_sort_does_not_advise_a_flag_that_would_change_nothing(command, native_geo_only, tmp_path):
    """The #738 bug from the other side.

    "Consider using --geoparquet-version 2.0" over a file being written as 2.0
    is advice to pass a flag that changes nothing -- noise that hides the real
    gaps the message exists to report.
    """
    out = tmp_path / f"{command}.parquet"

    output = _run_cli_output("sort", command, native_geo_only, out)

    assert "no spatial filter pushdown benefit" not in output, output
    assert (_geo_version(out) or "").startswith("2.0")


def test_the_row_group_guidance_can_fire_for_a_native_geo_only_input(native_geo_only, tmp_path):
    """The second consequence of the stale owner.

    ``hilbert_order`` gates its "consider a smaller --row-group-size" note on
    the effective version being 2.0 or parquet-geo-only. With the stale resolver
    saying 1.1, the note could never fire for these inputs -- though the output
    is exactly the 2.0 file the note is about.
    """
    out = tmp_path / "big_groups.parquet"

    output = _run_cli_output("sort", "hilbert", native_geo_only, out, "--row-group-size", "100000")

    assert "For optimal spatial filter pushdown with Hilbert sorting" in output, output


# ---------------------------------------------------------------------------
# The version must not outrun what the path can carry: CRS
# ---------------------------------------------------------------------------


#: The paths that read the user's file directly. Each writes native 2.0 *and*
#: keeps EPSG:5070, which is what makes the xfail below a real defect rather
#: than an oracle nothing could satisfy.
DIRECT_READ_COMMANDS = [
    pytest.param(("sort", "hilbert"), (), id="sort-hilbert"),
    pytest.param(("sort", "column"), ("id",), id="sort-column"),
    pytest.param(("sort", "str"), (), id="sort-str"),
    pytest.param(("extract", "geoparquet"), (), id="extract-geoparquet"),
]


@pytest.mark.parametrize(("command", "extra_args"), DIRECT_READ_COMMANDS)
def test_auto_mode_keeps_the_input_crs_while_upgrading_to_native_2_0(command, extra_args, tmp_path):
    """Upgrading the version must not restate the CRS as the default one."""
    out = tmp_path / "out.parquet"

    _run_cli(*command, PROJECTED_PGO, out, *extra_args)

    assert (_geo_version(out) or "").startswith("2.0")
    assert _is_native_geometry(out)
    assert source_crs_string(str(out), False) == PROJECTED_CRS


@pytest.mark.xfail(
    strict=True,
    reason=(
        "sort quadkey resolves the version from the user's file but reads the data "
        "from the scratch file `add quadkey` wrote, and that write drops the native "
        "GEOMETRY type and the CRS with it. The output is therefore native 2.0 "
        "declaring OGC:CRS84 over EPSG:5070 data -- a wrong CRS asserted where "
        "before it was merely absent, which `gpio check spec` blesses. Fixing it "
        "means making the index-adding scratch write preserve the input's version "
        "and CRS (the other half of #600); delete this marker then."
    ),
)
def test_sort_quadkey_keeps_the_input_crs(tmp_path):
    """Same assertion as the four above, on the one path with an intermediate."""
    out = tmp_path / "quadkey.parquet"

    _run_cli("sort", "quadkey", PROJECTED_PGO, out)

    assert source_crs_string(str(out), False) == PROJECTED_CRS


# ---------------------------------------------------------------------------
# Which `partition` drivers actually deliver the #600 fix
# ---------------------------------------------------------------------------


def _partition_versions(out_dir) -> set[tuple[str | None, bool]]:
    written = sorted(Path(out_dir).rglob("*.parquet"))
    assert written, "partition wrote no files"
    return {(_geo_version(part), _is_native_geometry(part)) for part in written}


def test_every_partition_write_options_gets_an_already_resolved_version():
    """``PartitionWriteOptions.geoparquet_version`` is documented as resolved.

    A partition write sees only the staging file, and staging is always written
    ``GEOPARQUET_VERSION 'NONE'``, so a write that resolved auto mode from what
    it reads would call every input native-geo-only. ``staging.py`` therefore
    states that the driver must resolve first -- and the PR that stated it left
    ``admin_hierarchical`` passing the raw ``geoparquet_version`` straight
    through. An invariant with an exempt construction site is a comment, so this
    checks every site there is, including ones added later.
    """
    import ast

    import geoparquet_io

    sites = []
    for source in sorted(Path(geoparquet_io.__file__).parent.rglob("*.py")):
        tree = ast.parse(source.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if getattr(node.func, "id", None) != "PartitionWriteOptions":
                continue
            version = next(
                (kw.value for kw in node.keywords if kw.arg == "geoparquet_version"), None
            )
            resolved = (
                isinstance(version, ast.Call)
                and getattr(version.func, "id", None) == "resolve_output_geoparquet_version"
            )
            sites.append((f"{source}:{node.lineno}", resolved))

    assert sites, "found no PartitionWriteOptions construction to check"
    assert all(resolved for _, resolved in sites), [site for site, ok in sites if not ok]


def test_partition_string_delivers_native_2_0(native_geo_only, tmp_path):
    """The control: the one driver that hands the user's own file to the facade."""
    out_dir = tmp_path / "parts"

    _run_cli("partition", "string", native_geo_only, out_dir, "--column", "grp", "--force")

    assert _partition_versions(out_dir) == {("2.0.0", True)}


@pytest.mark.parametrize("driver", ["quadkey", "h3", "s2", "a5", "kdtree"])
@pytest.mark.xfail(
    strict=True,
    reason=(
        "The index-adding partition drivers hand `partition_by_column` the scratch "
        "file their `add <index>` step wrote, not the user's input, so the facade "
        "resolves auto mode from a file that is already a 1.1 WKB rewrite. "
        "Resolving from the user's file instead would fix the version and import "
        "`sort quadkey`'s wrong-CRS defect (see test_sort_quadkey_keeps_the_input_crs) "
        "onto five more commands, so the scratch write is the thing to fix. Until "
        "then `gpio partition <index>` does not deliver #600."
    ),
)
def test_index_partition_drivers_deliver_native_2_0(driver, native_geo_only, tmp_path):
    """Pins the gap the PR body must not claim as fixed."""
    out_dir = tmp_path / driver
    extra = ["--auto"] if driver != "kdtree" else []

    _run_cli("partition", driver, native_geo_only, out_dir, "--force", *extra)

    assert _partition_versions(out_dir) == {("2.0.0", True)}
