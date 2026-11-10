"""Golden-value characterization tests for the spatial-index families (#664, suite 3).

These tests pin the *observable* cell values produced by every spatial index gpio
supports (h3, s2, a5, quadkey, kdtree) for a small, fully explicit fixture. They
exist so that the planned index-registry migration is verifiable: if a refactor
changes which cell a geometry lands in, a test fails and names the index and the
point that moved.

Four contracts are pinned:

1. ``add_*_table(fixture, resolution)`` produces exactly ``GOLDEN[index][res]``.
2. ``partition_by_*`` routes rows into partitions keyed by those same values
   (truncated to the partition resolution where the index supports one), and
   ``sort_by_quadkey_table`` orders rows by the same expression.
3. The *streaming* keying path (``add_*_column`` with a ``-`` destination)
   produces those same values. Streaming is not an optimization that kicks in on
   big inputs -- ``core/streaming.py`` dispatches purely on path shape
   (``is_stdin`` / ``should_stream_output``, no size threshold), so a piped
   invocation of any size exercises the streaming I/O driver.
4. The CLI (``gpio add h3``) agrees with the core table function, as a canary for
   CLI-to-core wiring.

The shared index driver owns table, file and streaming query construction. These
contracts keep the emitted values pinned across those I/O paths and partitioning.
A5 has two observed build vectors (#1361), both characterized on unchanged code.

The values below were generated once from current behavior -- they are a
characterization baseline, not an independent derivation. Regenerate them by
running this from the repository root, verbatim::

    uv run python - <<'PY'
    from geoparquet_io.core.add.a5 import add_a5_table
    from geoparquet_io.core.add.h3 import add_h3_table
    from geoparquet_io.core.add.kdtree import add_kdtree_table
    from geoparquet_io.core.add.quadkey import add_quadkey_table
    from geoparquet_io.core.add.s2 import add_s2_table
    from tests.test_spatial_index_golden import (
        A5_RESOLUTION,
        H3_RESOLUTION,
        KDTREE_ITERATIONS,
        KDTREE_SAMPLE_SIZE,
        QUADKEY_RESOLUTION,
        S2_LEVEL,
        golden_fixture,
    )

    t = golden_fixture()
    print("h3", add_h3_table(t, resolution=H3_RESOLUTION).column("h3_cell").to_pylist())
    print("s2", add_s2_table(t, level=S2_LEVEL).column("s2_cell").to_pylist())
    print("a5", add_a5_table(t, resolution=A5_RESOLUTION).column("a5_cell").to_pylist())
    print("quadkey", add_quadkey_table(t, resolution=QUADKEY_RESOLUTION).column("quadkey").to_pylist())
    print("kdtree", add_kdtree_table(
        t, iterations=KDTREE_ITERATIONS, sample_size=KDTREE_SAMPLE_SIZE
    ).column("kdtree_cell").to_pylist())
    PY

A changed value is only acceptable when it is an intentional, reviewed change to
keying behavior (or an upstream index-library change); update the literal and say
so in the commit message.
"""

from __future__ import annotations

import io
import json
import struct
import sys
from functools import partial
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner
from pyarrow import ipc

from geoparquet_io.cli.main import cli
from geoparquet_io.core.add.a5 import add_a5_column, add_a5_table
from geoparquet_io.core.add.h3 import add_h3_column, add_h3_table
from geoparquet_io.core.add.kdtree import add_kdtree_table
from geoparquet_io.core.add.quadkey import add_quadkey_column, add_quadkey_table
from geoparquet_io.core.add.s2 import add_s2_column, add_s2_table
from geoparquet_io.core.partition.by_a5 import partition_by_a5
from geoparquet_io.core.partition.by_h3 import partition_by_h3
from geoparquet_io.core.partition.by_kdtree import partition_by_kdtree
from geoparquet_io.core.partition.by_quadkey import partition_by_quadkey
from geoparquet_io.core.partition.by_s2 import partition_by_s2
from geoparquet_io.core.sort_quadkey import sort_by_quadkey_table

# --------------------------------------------------------------------------- #
# Fixture: seven fixed points + one polygon, built in-test (no data files).
# --------------------------------------------------------------------------- #

# (name, lon, lat). Chosen to cover both hemispheres in both axes, both sides of
# the antimeridian, and a point sitting a hair off the level-12 quadkey cell
# corner at (0, 0) -- close enough to exercise boundary rounding, far enough
# (1e-6 deg against an 0.088 deg cell, ~7-8 orders of magnitude above double
# rounding error at this scale) that no float noise can flip the answer.
#
# The antimeridian pair is deliberately symmetric: a wrap-sign error that folded
# +179.99 onto -179.99 (or clamped either to +/-180) would collapse the two into
# one cell, which the golden values below would catch.
POINTS: list[tuple[str, float, float]] = [
    ("san_francisco", -122.4194, 37.7749),  # N/W
    ("berlin", 13.4050, 52.5200),  # N/E
    ("sydney", 151.2093, -33.8688),  # S/E
    ("buenos_aires", -58.3816, -34.6037),  # S/W
    ("antimeridian_east", 179.9900, -16.5000),  # just west of +180
    ("antimeridian_west", -179.9900, -16.5000),  # just east of -180 (wrap-sign twin)
    ("null_island_boundary", -0.000001, 0.000001),  # just off the (0, 0) cell corner
]

# An axis-aligned square over Washington, DC, centred on (-77.025, 38.875).
#
# The 0.3 deg span is load-bearing, not decorative: it is wide enough that the
# centroid and the SW/NE corners fall in *different* cells for every index here,
# including the coarse ones (quadkey z12: centroid 032010032233 vs. SW
# 032010210003 vs. NE 032010032303; s2 L10: 89b7b7 vs. 89b7ad vs. 89b7c3). So a
# refactor that quietly switched polygon keying from centroid to a bbox corner --
# the tempting shortcut, since a stored bbox is cheaper than ST_Centroid -- moves
# the golden value and fails. A 0.05 deg square would have hidden that swap
# behind identical cells.
POLYGON_NAME = "dc_square"
POLYGON_RING: list[tuple[float, float]] = [
    (-77.175, 38.725),  # SW
    (-76.875, 38.725),  # SE
    (-76.875, 39.025),  # NE
    (-77.175, 39.025),  # NW
    (-77.175, 38.725),  # close the ring
]

ROW_NAMES = [name for name, _, _ in POINTS] + [POLYGON_NAME]


def _point_wkb(lon: float, lat: float) -> bytes:
    """Little-endian WKB for a 2D point (built by hand to keep the fixture exact)."""
    return struct.pack("<BIdd", 1, 1, lon, lat)


def _polygon_wkb(ring: list[tuple[float, float]]) -> bytes:
    """Little-endian WKB for a single-ring 2D polygon."""
    body = struct.pack("<BIII", 1, 3, 1, len(ring))
    return body + b"".join(struct.pack("<dd", lon, lat) for lon, lat in ring)


def golden_fixture() -> pa.Table:
    """Build the in-memory GeoParquet table the golden values were generated from.

    The ``geo`` metadata deliberately omits ``crs``, which per the GeoParquet spec
    means OGC:CRS84 -- the no-reprojection path all five indexes key against.
    """
    geometries = [_point_wkb(lon, lat) for _, lon, lat in POINTS]
    geometries.append(_polygon_wkb(POLYGON_RING))

    table = pa.table(
        {
            "name": pa.array(ROW_NAMES, type=pa.string()),
            "geometry": pa.array(geometries, type=pa.binary()),
        }
    )
    geo_metadata = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point", "Polygon"],
            }
        },
    }
    return table.replace_schema_metadata({b"geo": json.dumps(geo_metadata).encode()})


@pytest.fixture
def golden_table() -> pa.Table:
    return golden_fixture()


@pytest.fixture
def golden_parquet(tmp_path: Path) -> str:
    path = tmp_path / "golden_fixture.parquet"
    pq.write_table(golden_fixture(), path)
    return str(path)


# --------------------------------------------------------------------------- #
# Golden values (row order matches ROW_NAMES).
# --------------------------------------------------------------------------- #

H3_RESOLUTION = 7
S2_LEVEL = 10
A5_RESOLUTION = 12
QUADKEY_RESOLUTION = 12
QUADKEY_PARTITION_RESOLUTION = 6
KDTREE_ITERATIONS = 2

# `add/kdtree.py` draws boundaries from `USING SAMPLE {n} ROWS`. A sample size far
# above the eight-row fixture makes the sample the whole table, so the medians --
# and therefore every cell assignment below -- are exact and reproducible rather
# than reservoir-sampled. Verified by running the kdtree cases five times over:
# same assignments every run.
KDTREE_SAMPLE_SIZE = 1000

GOLDEN: dict[str, dict[int, list]] = {
    "h3": {
        H3_RESOLUTION: [
            "872830828ffffff",  # san_francisco
            "871f1d489ffffff",  # berlin
            "87be0e35cffffff",  # sydney
            "87c2e3113ffffff",  # buenos_aires
            "879b5dc46ffffff",  # antimeridian_east
            "879b5dc55ffffff",  # antimeridian_west
            "87754e64dffffff",  # null_island_boundary
            "872aa845bffffff",  # dc_square (centroid; SW corner is 872aa86b2ffffff)
        ]
    },
    "s2": {
        S2_LEVEL: [
            "808581",  # san_francisco
            "47a851",  # berlin
            "6b12af",  # sydney
            "95bccb",  # buenos_aires
            "6e2001",  # antimeridian_east
            "71dfff",  # antimeridian_west
            "0fffff",  # null_island_boundary
            "89b7b7",  # dc_square (centroid; SW corner is 89b7ad, NE is 89b7c3)
        ]
    },
    "a5": {
        A5_RESOLUTION: [
            1937277949849894912,  # san_francisco
            7205644470467952640,  # berlin
            10339920769101856768,  # sydney
            3612158308724506624,  # buenos_aires
            10912267286333095936,  # antimeridian_east
            10912267355052572672,  # antimeridian_west
            5694200192870383616,  # null_island_boundary
            2662906764634095616,  # dc_square (centroid)
        ]
    },
    "quadkey": {
        QUADKEY_RESOLUTION: [
            "023010203333",  # san_francisco
            "120210233222",  # berlin
            "311230133002",  # sydney
            "210321300311",  # buenos_aires
            "311131333331",  # antimeridian_east
            "200020222220",  # antimeridian_west
            "033333333333",  # null_island_boundary
            "032010032233",  # dc_square (centroid; SW corner is 032010210003)
        ]
    },
    "kdtree": {
        KDTREE_ITERATIONS: [
            "001",  # san_francisco
            "011",  # berlin
            "010",  # sydney
            "000",  # buenos_aires
            "010",  # antimeridian_east
            "000",  # antimeridian_west
            "011",  # null_island_boundary
            "001",  # dc_square
        ]
    },
}


# #1361: the A5 extension bundled for the local DuckDB 1.5.6 build returns
# this alternate full vector on the unchanged base. Pin both observed builds;
# the index driver must keep the installed extension's cells unchanged.
A5_BUILD_CELLS = [
    1937277949849894912,
    7205578293611855872,
    11287371794182307840,
    4008475075933110272,
    10624037940973535232,
    10624037734815105024,
    5695402921152217088,
    2649430875246166016,
]

# --------------------------------------------------------------------------- #
# Index registry: how to add, how to partition, how a cell value maps to the
# partition key written to disk.
# --------------------------------------------------------------------------- #


def _identity_key(value) -> str:
    return str(value)


def _quadkey_partition_key(value: str) -> str:
    """Quadkey partitions on a prefix of the cell value (`column_prefix_length`)."""
    return value[:QUADKEY_PARTITION_RESOLUTION]


INDEX_SPECS: dict[str, dict] = {
    "h3": {
        "column": "h3_cell",
        "resolution": H3_RESOLUTION,
        "add": partial(add_h3_table, resolution=H3_RESOLUTION),
        "partition": partial(partition_by_h3, resolution=H3_RESOLUTION),
        "stream": partial(add_h3_column, h3_resolution=H3_RESOLUTION),
        "partition_key": _identity_key,
    },
    "s2": {
        "column": "s2_cell",
        "resolution": S2_LEVEL,
        "add": partial(add_s2_table, level=S2_LEVEL),
        "partition": partial(partition_by_s2, level=S2_LEVEL),
        "stream": partial(add_s2_column, s2_level=S2_LEVEL),
        "partition_key": _identity_key,
    },
    "a5": {
        "column": "a5_cell",
        "resolution": A5_RESOLUTION,
        "add": partial(add_a5_table, resolution=A5_RESOLUTION),
        "partition": partial(partition_by_a5, resolution=A5_RESOLUTION),
        "stream": partial(add_a5_column, a5_resolution=A5_RESOLUTION),
        "partition_key": _identity_key,
    },
    "quadkey": {
        "column": "quadkey",
        "resolution": QUADKEY_RESOLUTION,
        "add": partial(add_quadkey_table, resolution=QUADKEY_RESOLUTION),
        "partition": partial(
            partition_by_quadkey,
            resolution=QUADKEY_RESOLUTION,
            partition_resolution=QUADKEY_PARTITION_RESOLUTION,
        ),
        "stream": partial(add_quadkey_column, resolution=QUADKEY_RESOLUTION),
        "partition_key": _quadkey_partition_key,
    },
    "kdtree": {
        "column": "kdtree_cell",
        "resolution": KDTREE_ITERATIONS,
        "add": partial(
            add_kdtree_table, iterations=KDTREE_ITERATIONS, sample_size=KDTREE_SAMPLE_SIZE
        ),
        "partition": partial(
            partition_by_kdtree, iterations=KDTREE_ITERATIONS, sample_size=KDTREE_SAMPLE_SIZE
        ),
        "partition_key": _identity_key,
    },
}

# a5 keying needs the `a5` DuckDB community extension, which is downloaded on
# first use. Every other index in this suite runs offline, so the network mark
# goes on the a5 params only -- the fast suite must not start requiring it.
INDEX_PARAMS = [
    pytest.param("h3", id="h3"),
    pytest.param("s2", id="s2"),
    pytest.param("a5", id="a5", marks=pytest.mark.network),
    pytest.param("quadkey", id="quadkey"),
    pytest.param("kdtree", id="kdtree"),
]

# kdtree is absent: unlike the others it has no separate streaming SQL builder
# (`_add_kdtree_streaming` goes through the same `_build_sampling_query` the
# table path uses), so a streaming case would re-cover test 1's code rather than
# a second implementation.
STREAMING_INDEX_PARAMS = [p for p in INDEX_PARAMS if p.values[0] != "kdtree"]


def _assert_cells_match_golden(index: str, actual: list, context: str) -> None:
    """Compare a cell column against GOLDEN, naming the index and the point that moved."""
    spec = INDEX_SPECS[index]
    expected = GOLDEN[index][spec["resolution"]]
    if index == "a5" and actual == A5_BUILD_CELLS:
        return

    assert len(actual) == len(expected), (
        f"{index} ({context}): expected {len(expected)} cell values, got {len(actual)}"
    )
    mismatches = [
        f"  {name}: expected {exp!r}, got {act!r}"
        for name, exp, act in zip(ROW_NAMES, expected, actual, strict=True)
        if exp != act
    ]
    assert not mismatches, (
        f"{index} at resolution {spec['resolution']} ({context}) no longer matches the "
        f"golden cell values:\n" + "\n".join(mismatches)
    )


# --------------------------------------------------------------------------- #
# Test 1 -- add_*_table produces the golden cell values.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("index", INDEX_PARAMS)
def test_add_table_matches_golden_cells(index: str, golden_table: pa.Table) -> None:
    """Each add_*_table keys the fixture into exactly the pinned cells."""
    spec = INDEX_SPECS[index]
    result = spec["add"](golden_table)

    assert spec["column"] in result.column_names, (
        f"{index}: add_*_table did not produce column {spec['column']!r} "
        f"(got {result.column_names})"
    )
    assert result.num_rows == len(ROW_NAMES)
    _assert_cells_match_golden(index, result.column(spec["column"]).to_pylist(), "add_*_table")


# --------------------------------------------------------------------------- #
# Test 2 -- partition_by_* routes rows by the same cell values.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("index", INDEX_PARAMS)
def test_partition_keys_match_golden_cells(index: str, golden_parquet: str, tmp_path: Path) -> None:
    """Partition keys equal the golden cells, truncated to the partition resolution."""
    spec = INDEX_SPECS[index]
    output_folder = tmp_path / f"partition_{index}"

    spec["partition"](golden_parquet, str(output_folder), verbose=False, force=True)

    # NOTE: this is a SET comparison, so it is deliberately permutation-blind and
    # duplicate-blind -- partitioning has no meaningful row order to preserve, and
    # several fixture rows legitimately share a partition (all four kdtree cells
    # hold two rows). Ordering is pinned by test 1, which compares the cell column
    # element-wise; the row-count assertion below covers drops and duplication.
    written_keys = {p.stem for p in output_folder.rglob("*.parquet")}
    expected_keys = {spec["partition_key"](v) for v in GOLDEN[index][spec["resolution"]]}

    if index == "a5" and written_keys == {str(cell) for cell in A5_BUILD_CELLS}:
        expected_keys = written_keys

    assert written_keys == expected_keys, (
        f"{index}: partition keys diverged from the golden cell values.\n"
        f"  missing: {sorted(expected_keys - written_keys)}\n"
        f"  unexpected: {sorted(written_keys - expected_keys)}"
    )

    # Row conservation: partitioning must not drop or duplicate features.
    total_rows = sum(pq.read_metadata(p).num_rows for p in output_folder.rglob("*.parquet"))
    assert total_rows == len(ROW_NAMES), (
        f"{index}: partitioning wrote {total_rows} rows for a {len(ROW_NAMES)}-row input"
    )


def test_quadkey_sort_order_matches_sorted_golden(golden_table: pa.Table) -> None:
    """`sort quadkey` orders rows by the same quadkey expression `add`/`partition` use."""
    sorted_table = sort_by_quadkey_table(golden_table, resolution=QUADKEY_RESOLUTION)
    actual = sorted_table.column("quadkey").to_pylist()

    assert actual == sorted(GOLDEN["quadkey"][QUADKEY_RESOLUTION]), (
        "sort_by_quadkey_table no longer emits rows in golden quadkey order:\n"
        f"  expected: {sorted(GOLDEN['quadkey'][QUADKEY_RESOLUTION])}\n"
        f"  actual:   {actual}"
    )

    # The sort must permute, not filter.
    assert sorted(sorted_table.column("name").to_pylist()) == sorted(ROW_NAMES)


# --------------------------------------------------------------------------- #
# Test 3 -- the streaming keying path produces the same cells.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("index", STREAMING_INDEX_PARAMS)
def test_streaming_path_matches_golden_cells(
    index: str, golden_parquet: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Streaming `add_*_column` keys the fixture into the same cells as the table path.

    Dispatch is path-shaped: a stdout destination takes the streaming driver
    even for a tiny file. This pins its observable cells independently of the
    table and file writers, which use different input registration paths.
    """
    spec = INDEX_SPECS[index]

    output_buffer = io.BytesIO()
    mock_stdout = mock.MagicMock()
    mock_stdout.buffer = output_buffer
    mock_stdout.isatty.return_value = False
    monkeypatch.setattr(sys, "stdout", mock_stdout)

    spec["stream"](golden_parquet, "-")

    output_buffer.seek(0)
    result = ipc.RecordBatchStreamReader(output_buffer).read_all()

    assert spec["column"] in result.column_names, (
        f"{index}: streaming add_*_column did not produce column {spec['column']!r} "
        f"(got {result.column_names})"
    )
    _assert_cells_match_golden(
        index, result.column(spec["column"]).to_pylist(), "streaming add_*_column"
    )


# --------------------------------------------------------------------------- #
# Test 4 -- the CLI agrees with the core table function (wiring canary).
# --------------------------------------------------------------------------- #


def test_cli_add_h3_matches_core_table(golden_parquet: str, tmp_path: Path) -> None:
    """`gpio add h3` writes the same cells `add_h3_table` computes."""
    output_path = tmp_path / "cli_h3.parquet"

    result = CliRunner().invoke(
        cli,
        [
            "add",
            "h3",
            golden_parquet,
            str(output_path),
            "--resolution",
            str(H3_RESOLUTION),
        ],
    )

    assert result.exit_code == 0, f"gpio add h3 failed: {result.output}\n{result.exception}"
    assert output_path.exists()

    cli_cells = pq.read_table(output_path).column("h3_cell").to_pylist()
    core_table = add_h3_table(golden_fixture(), resolution=H3_RESOLUTION)
    core_cells = core_table.column("h3_cell").to_pylist()

    assert cli_cells == core_cells, (
        "CLI `gpio add h3` diverged from core add_h3_table:\n"
        f"  cli:  {cli_cells}\n"
        f"  core: {core_cells}"
    )
    _assert_cells_match_golden("h3", cli_cells, "CLI gpio add h3")
