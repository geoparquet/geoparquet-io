"""``gpio add *`` must not downgrade -- or contradict -- the file it rewrites.

Every ``gpio add`` subcommand rewrites its whole input to append one column.
That rewrite went through ``write_parquet_with_metadata`` without ``input_file=``,
so the write facade (#990) had no witness to answer either of the two questions
a rewrite of a *native-geo-only* file has to answer:

* **Which version?** Auto mode read the carried ``geo`` key, and a
  native-geo-only input has no ``geo`` key. It landed on the 1.1 default, DuckDB
  re-encoded the geometry column as plain WKB, and the Parquet ``GEOMETRY``
  logical type went with it.
* **Which CRS?** A native-geo-only file stores its CRS *only* inside that
  logical type, so ``input_crs`` was ``None`` and nothing described the output's
  CRS at all.

The two answers have to come from the same witness, and this is why. Passing
``input_file=`` alone fixes the version and leaves the CRS unanswered: the
output is then native 2.0 with the CRS in the Parquet logical type and **no**
``crs`` key in the ``geo`` block -- which GeoParquet resolves as ``OGC:CRS84``.
The file contradicts itself, and ``gpio check spec`` says so::

    ✗ CRS in geo metadata must match CRS in Parquet schema
        Metadata: OGC:CRS84 (default), Schema: NAD83 / Conus Albers (EPSG:5070)

So every end-to-end assertion here reads the ``geo`` block and the Parquet
logical type **separately**, and then asks ``gpio check spec`` whether they
agree. Reading the CRS through ``crs_utils.source_crs_string`` -- which consults
the ``geo`` block, falls back to the logical type, and returns the first answer
it finds -- is structurally incapable of reporting a disagreement between them,
and an oracle that cannot see the defect certified it green.

Refs: https://github.com/geoparquet/geoparquet-io/issues/993
Refs: https://github.com/geoparquet/geoparquet-io/issues/600
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests.native_geo_probes import (
    EPSG_3857,
    EPSG_5070,
    conus_wkb,
    geo_block,
    geo_version,
    logical_geo_types,
    projjson,
    spec_problems,
    write_native_geo_only,
)
from tests.native_geo_probes import geo_block_crs_id as _geo_block_crs_id


def _run_cli(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output


def _run_cli_in_a_fresh_process(*args) -> None:
    """Run one gpio command in a subprocess, importing nothing this module did.

    In-process is the wrong oracle for anything that depends on ``pyarrow``'s
    *global* extension-type registry. The fixtures below import
    ``geoarrow.pyarrow`` to write their files, and that import silently changes
    how every later pyarrow read in the process materialises a Parquet
    ``GEOMETRY`` column -- plain ``binary`` without it, ``geoarrow.wkb`` with it.
    A pyarrow rewrite that loses the logical type therefore passes under pytest
    and fails for the user. Measured, on ``add geometry-metrics`` over the
    EPSG:5070 fixture: 0 failures in-process, 4 from a fresh CLI.
    """
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "from geoparquet_io.cli.main import cli; cli()",
            *(str(a) for a in args),
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


# ---------------------------------------------------------------------------
# Fixtures. The EPSG:5070 one is `projected_conus`, shared via conftest with
# tests/test_write_facade_version_owner.py -- the same input, measured on both
# sides of the scratch-file write.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spherical_geography(tmp_path_factory) -> Path:
    """A native-geo-only file whose geometry column is a GEOGRAPHY.

    A GEOGRAPHY column's edges follow the sphere. Anything that rebuilds it as a
    plain GEOMETRY leaves the same bytes describing straight lines between the
    same vertices -- different shapes, with nothing in the file saying so.
    """
    import geoarrow.pyarrow as ga
    import geoarrow.types as gt

    return write_native_geo_only(
        tmp_path_factory.mktemp("geography") / "pgo.parquet",
        conus_wkb("cell"),
        {"geometry": (2, ga.wkb().with_edge_type(gt.EdgeType.SPHERICAL))},
    )


@pytest.fixture(scope="module")
def two_native_geometry_columns(tmp_path_factory) -> Path:
    """``geometry`` in EPSG:5070 and ``centroid`` in EPSG:3857, both native.

    Two columns with *different* CRSs, so a write that applies one column's CRS
    to both -- or drops one of them -- is visible rather than a no-op.
    """
    import geoarrow.pyarrow as ga

    rows = conus_wkb(
        "ST_Transform(cell, 'EPSG:4326', 'EPSG:5070', always_xy := true)",
        "ST_Transform(ST_Centroid(cell), 'EPSG:4326', 'EPSG:3857', always_xy := true)",
    )
    return write_native_geo_only(
        tmp_path_factory.mktemp("two_geo") / "pgo.parquet",
        rows,
        {
            "geometry": (2, ga.wkb().with_crs(projjson(5070))),
            "centroid": (3, ga.wkb().with_crs(projjson(3857))),
        },
    )


# ---------------------------------------------------------------------------
# Every `add` subcommand that rewrites its input's rows
# ---------------------------------------------------------------------------


#: ``bbox-metadata`` is excluded because it writes metadata only, and
#: ``admin-divisions`` because it needs a boundary dataset to join against
#: (covered by the network lane).
ROW_REWRITING_ADD_COMMANDS = [
    pytest.param("bbox", (), id="bbox"),
    pytest.param("quadkey", (), id="quadkey"),
    pytest.param("h3", (), id="h3"),
    pytest.param("s2", (), id="s2"),
    pytest.param("a5", (), id="a5"),
    pytest.param("kdtree", (), id="kdtree"),
    pytest.param("geometry-metrics", (), id="geometry-metrics"),
]


def test_the_fixture_is_a_clean_native_geo_only_epsg_5070_file(projected_conus):
    """Non-vacuity, and the baseline every "clean" below is measured against.

    If the input already failed ``check spec``, "the output has no failures"
    would be an assertion about the fixture rather than about the command.
    """
    assert geo_block(projected_conus) is None
    assert logical_geo_types(projected_conus) == {"geometry": ("Geometry", EPSG_5070)}
    assert spec_problems(projected_conus) == []


@pytest.mark.parametrize(("subcommand", "extra_args"), ROW_REWRITING_ADD_COMMANDS)
def test_add_keeps_the_file_describing_itself_consistently(
    subcommand, extra_args, projected_conus, tmp_path
):
    """One run, four facts: version, encoding, CRS *in the geo block*, agreement.

    The fourth is what the old oracle could not see. The third is what makes the
    fourth non-vacuous: a file can only contradict itself once both halves have
    said something.
    """
    out = tmp_path / f"{subcommand}.parquet"

    _run_cli("add", subcommand, projected_conus, out, *extra_args)

    assert (geo_version(out) or "").startswith("2.0")
    assert logical_geo_types(out)["geometry"] == ("Geometry", EPSG_5070)
    assert _geo_block_crs_id(out) == EPSG_5070
    assert spec_problems(out) == []


@pytest.mark.parametrize(("subcommand", "extra_args"), ROW_REWRITING_ADD_COMMANDS)
def test_add_states_the_crs_even_when_asked_for_1_1(
    subcommand, extra_args, projected_conus, tmp_path
):
    """An explicit version still wins -- and still has to say which CRS it is.

    1.1 has nowhere but the ``geo`` block to put a CRS, so a 1.1 rewrite of a
    native-geo-only input is the case where the witness is the *only* thing
    standing between EPSG:5070 and a file that declares nothing. This was
    silently lossy for every ``add`` subcommand before #993, and no version
    assertion could have caught it.
    """
    out = tmp_path / f"{subcommand}.parquet"

    _run_cli("add", subcommand, projected_conus, out, *extra_args, "--geoparquet-version", "1.1")

    assert (geo_version(out) or "").startswith("1.1")
    assert logical_geo_types(out) == {}, "1.1 forbids a native Parquet geo type"
    assert _geo_block_crs_id(out) == EPSG_5070
    assert spec_problems(out) == []


def test_geometry_metrics_keeps_the_native_type_from_a_fresh_process(projected_conus, tmp_path):
    """The one ``add`` subcommand that ends in a pyarrow rewrite of the whole file.

    ``add geometry-metrics`` runs ``constants._fix_vecorel_schema`` last, and
    pyarrow only re-emits a Parquet ``GEOMETRY`` logical type if
    ``geoarrow.pyarrow`` has registered its extension types in *that* process.
    Every other test in this file is blind to that, because the fixtures imported
    ``geoarrow.pyarrow`` to write their files. Run from a fresh interpreter the
    same command scored ``4 failed`` -- the native type gone, the CRS left in the
    ``geo`` block with nothing in the schema to match it -- which is why
    ``_fix_vecorel_schema`` now does the registering itself.
    """
    out = tmp_path / "geometry-metrics.parquet"

    _run_cli_in_a_fresh_process("add", "geometry-metrics", projected_conus, out)

    assert (geo_version(out) or "").startswith("2.0")
    assert logical_geo_types(out)["geometry"] == ("Geometry", EPSG_5070)
    assert _geo_block_crs_id(out) == EPSG_5070
    assert spec_problems(out) == []


def test_add_on_a_1_1_input_still_writes_1_1(tmp_path):
    """The control: auto mode preserves, it does not upgrade a 1.1 input to 2.0."""
    one_one = Path(__file__).parent / "data" / "austria_bbox_covering.parquet"
    assert (geo_version(one_one) or "").startswith("1."), "fixture is not a 1.x input"
    out = tmp_path / "bbox.parquet"

    _run_cli("add", "bbox", one_one, out)

    assert (geo_version(out) or "").startswith("1.1")
    assert logical_geo_types(out) == {}


# ---------------------------------------------------------------------------
# The two inputs the witness does not reach, measured rather than assumed
# ---------------------------------------------------------------------------


def test_the_geography_fixture_really_is_spherical(spherical_geography):
    """Non-vacuity for the xfail below."""
    assert logical_geo_types(spherical_geography)["geometry"][0] == "Geography"
    assert spec_problems(spherical_geography) == []


@pytest.mark.xfail(
    strict=True,
    reason=(
        "`add bbox` on a native GEOGRAPHY input writes a planar Parquet GEOMETRY "
        "logical type, while the `geo` block it builds declares "
        '`"edges": "spherical"` -- so `check spec` warns that the two halves of '
        "the output describe different shapes over the same bytes. It is still an "
        "improvement on main, which wrote 1.1 WKB and dropped the `edges` "
        "declaration outright, silently replanning the edges with nothing in the "
        "file saying so. The witness #993 adds carries the CRS, not the edge type: "
        "DuckDB reads a GEOGRAPHY column as a planar GEOMETRY and the write has "
        "nothing left to restate it from, so closing this means teaching the "
        "write path to round-trip the edge type (#999). Delete this marker then."
    ),
)
def test_add_keeps_a_geography_column_spherical(spherical_geography, tmp_path):
    """Measured, not assumed: the output must not silently replan the edges."""
    out = tmp_path / "bbox.parquet"

    _run_cli("add", "bbox", spherical_geography, out)

    assert logical_geo_types(out)["geometry"][0] == "Geography"
    assert spec_problems(out) == []
    assert spec_problems(out, status="warning") == []


def test_the_two_column_fixture_really_declares_two_crss(two_native_geometry_columns):
    """Non-vacuity for the two below."""
    assert logical_geo_types(two_native_geometry_columns) == {
        "geometry": ("Geometry", EPSG_5070),
        "centroid": ("Geometry", EPSG_3857),
    }
    assert spec_problems(two_native_geometry_columns) == []


def test_add_attaches_the_witness_crs_to_the_primary_column_only(
    two_native_geometry_columns, tmp_path
):
    """The half that does work: a per-file witness must not become a per-file CRS.

    ``resolve_input_crs`` answers for the file, and ``apply_output_crs`` applies
    that answer to the primary column alone. A secondary column in a *different*
    CRS is what makes the difference observable: EPSG:5070 must not land on
    ``centroid``.
    """
    out = tmp_path / "bbox.parquet"

    _run_cli("add", "bbox", two_native_geometry_columns, out)

    assert _geo_block_crs_id(out) == EPSG_5070
    assert logical_geo_types(out)["geometry"] == ("Geometry", EPSG_5070)
    assert logical_geo_types(out)["centroid"][1] != EPSG_5070


@pytest.mark.xfail(
    strict=True,
    reason=(
        "A secondary native geometry column survives the rewrite with its logical "
        "type but loses its own CRS, and nothing adds it to `geo.columns`, which "
        "GeoParquet 2.0 requires for every geometry column in the file: "
        "`✗ geometry columns not described in geo metadata: ['centroid']`. The "
        "caller is what is missing -- `add *` passes no `geometry_info`, so "
        "`build_geo_metadata` is never told there is a secondary column and no "
        "witness is read for it. That is a per-column change affecting every write "
        "path, not the per-file witness #993 adds, and it is filed as #1000 next to "
        "#952/#953. Main is worse here (a 1.1 file still carrying a native "
        "`centroid` type, two failures); this narrows it to one. Delete this "
        "marker then."
    ),
)
def test_add_describes_and_keeps_every_native_geometry_column(
    two_native_geometry_columns, tmp_path
):
    """Measured, not assumed: 2.0 requires every geometry column to be described."""
    out = tmp_path / "bbox.parquet"

    _run_cli("add", "bbox", two_native_geometry_columns, out)

    assert sorted((geo_block(out) or {}).get("columns", {})) == ["centroid", "geometry"]
    assert logical_geo_types(out)["centroid"] == ("Geometry", EPSG_3857)
    assert spec_problems(out) == []
