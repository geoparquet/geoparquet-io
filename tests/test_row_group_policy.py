"""One row-group policy: every write path lands on the facade's count (#1367 T2c).

``parquet_writer.resolve_row_group_rows`` is the only place a row-group count is
decided: an explicit row count (snapped to a whole writer vector), else an MB
target measured from the data (``resolve_row_group_rows_for_table`` /
``resolve_row_group_rows_for_query``), else ``DEFAULT_ROW_GROUP_ROWS``. The
table below runs one fixture through every command that writes a file, with
each option and two table sizes, and asserts the group count each output has
is the count the owner computes from the input -- so a path that re-derives a
number, or leaves it to its writer, shows up as the one cell that disagrees.

The commands that take no row-group option (``process aggregate``,
``process overview``, ``add bbox-metadata``'s fallback rewrite) are held to the
default. Only the network (the admin boundary download) is replaced, by a local
boundary file; ``add bbox-metadata`` is made to take its fallback rewrite the
way its own tests do, by refusing the footer patch.

The guard at the bottom keeps the policy in one place: no ``49152`` literal, no
``row_group_size=`` / ``ROW_GROUP_SIZE`` with a literal count, no row-group
constant and no ``rows or <fallback>`` second default outside the owner.
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.parquet_writer import (
    DEFAULT_ROW_GROUP_ROWS,
    WRITER_VECTOR_ROWS,
    resolve_row_group_rows,
    resolve_row_group_rows_for_rewrite,
    resolve_row_group_rows_for_table,
)
from tests._defrag_fixtures import geo_block, grid_points_table

REPO = Path(__file__).resolve().parents[1]
PACKAGE = REPO / "geoparquet_io"
OWNER = PACKAGE / "core" / "parquet_writer.py"

SIZES = (25_000, 60_000)
#: option name -> CLI flags. 4.3 MB over ~380-byte rows is ~11,300 rows: the
#: middle of the 10,240-12,288 vector step, so a DuckDB sample and a pyarrow
#: table that weigh a few bytes apart still land on one count.
OPTIONS = {
    "default": (),
    "rows": ("--row-group-size", "10000"),
    "mb": ("--row-group-size-mb", "4.3"),
}
#: Paths that take --row-group-size / --row-group-size-mb, and those that take neither.
SIZED_PATHS = (
    "convert",
    "sort_hilbert",
    "extract",
    "partition_admin",
    *(f"table_write_{s}" for s in ("duckdb-kv", "in-memory", "streaming", "disk-rewrite")),
)
DEFAULT_ONLY_PATHS = ("aggregate_h3_none", "aggregate_h3_polygon", "overview", "add_bbox_metadata")


def _run(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, f"gpio {' '.join(map(str, args))}: {result.output[-1500:]}"


def _group_sizes(path: Path) -> list[int]:
    md = pq.ParquetFile(str(path)).metadata
    return [md.row_group(i).num_rows for i in range(md.num_row_groups)]


@pytest.fixture(scope="module", params=SIZES, ids=lambda n: f"{n}rows")
def source(request, tmp_path_factory) -> Path:
    """``n`` distinct points (one per H3 r15 cell) with a fixed-width pad column,
    written in 4,096-row groups so no policy matches the input's own layout.

    The input's groups are whole writer vectors on purpose. DuckDB's ``COPY``
    closes a row group on the first chunk that reaches ``ROW_GROUP_SIZE``, and a
    5,000-row input group reads back as 2,048 + 2,048 + 904, so a 12,288-row
    request over it lands at 14,096: the writer's chunking, not a policy.
    """
    n = request.param
    table = grid_points_table(n, with_bbox=False)
    table = table.append_column("pad", pa.array([f"{i:08d}" * 40 for i in range(n)]))
    path = tmp_path_factory.mktemp(f"rg_{n}") / "source.parquet"
    pq.write_table(
        table.replace_schema_metadata({b"geo": json.dumps(geo_block()).encode()}),
        str(path),
        row_group_size=4_096,
    )
    return path


def _expected(source: Path, option: str) -> int:
    """The group count the owner computes for ``source`` under ``option``."""
    table = pq.read_table(str(source))
    if option == "mb":
        rows = resolve_row_group_rows_for_table(table, None, float(OPTIONS["mb"][1]))
    elif option == "rows":
        rows = resolve_row_group_rows(int(OPTIONS["rows"][1]), None)
    else:
        rows = resolve_row_group_rows(None, None)
    return math.ceil(table.num_rows / rows)


def _use_local_admin(tmp_path: Path, monkeypatch) -> None:
    """One Overture-shaped country polygon over the fixture, instead of the download."""
    from geoparquet_io.core.admin_datasets import OvertureAdminDataset
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

    admin_file = tmp_path / "admin.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"""
            COPY (
              SELECT 'country' AS subtype, 'AA' AS country, NULL::VARCHAR AS region, g AS geometry,
                {{'xmin': ST_XMin(g), 'xmax': ST_XMax(g),
                  'ymin': ST_YMin(g), 'ymax': ST_YMax(g)}} AS bbox
              FROM (SELECT ST_GeomFromText('POLYGON((-11 -1, 11 -1, 11 21, -11 21, -11 -1))') AS g)
            ) TO {sql_path(str(admin_file))} (FORMAT PARQUET)
        """)
    finally:
        con.close()

    def create(*_args, verbose=False, **_kwargs):
        return OvertureAdminDataset(source_path=str(admin_file), verbose=verbose)

    monkeypatch.setattr(
        "geoparquet_io.core.admin_datasets.AdminDatasetFactory.create",
        staticmethod(create),
    )


def _write(path_name: str, src: Path, out_dir: Path, flags: tuple[str, ...]) -> Path:
    out = out_dir / "out.parquet"
    if path_name.startswith("table_write_"):
        kwargs = {}
        if flags and flags[0] == "--row-group-size":
            kwargs["row_group_rows"] = int(flags[1])
        elif flags:
            kwargs["row_group_size_mb"] = float(flags[1])
        strategy = path_name.removeprefix("table_write_")
        gpio.read(str(src)).write(str(out), write_strategy=strategy, **kwargs)
    elif path_name == "convert":
        _run("convert", "geoparquet", src, out, *flags)
    elif path_name == "sort_hilbert":
        _run("sort", "hilbert", src, out, *flags)
    elif path_name == "extract":
        _run("extract", src, out, *flags)
    elif path_name == "partition_admin":
        part = out_dir / "partitions"
        _run(
            "partition", "admin", src, part, "--dataset", "overture", "--levels", "country", *flags
        )
        (out,) = sorted(part.rglob("*.parquet"))
    return out


def _assert_owners_count(path_name: str, option: str, source: Path, tmp_path: Path) -> None:
    out = _write(path_name, source, tmp_path, OPTIONS[option])
    sizes = _group_sizes(out)
    assert sum(sizes) == pq.read_metadata(str(source)).num_rows
    assert len(sizes) == _expected(source, option), f"{path_name} {option}: {sizes}"


@pytest.mark.parametrize("option", sorted(OPTIONS))
@pytest.mark.parametrize("path_name", [p for p in SIZED_PATHS if p != "partition_admin"])
def test_every_sized_path_lands_on_the_owners_count(path_name, option, source, tmp_path):
    _assert_owners_count(path_name, option, source, tmp_path)


@pytest.mark.parametrize("option", sorted(OPTIONS))
def test_partition_admin_lands_on_the_owners_count(option, source, tmp_path, monkeypatch):
    _use_local_admin(tmp_path, monkeypatch)
    _assert_owners_count("partition_admin", option, source, tmp_path)


@pytest.mark.parametrize("path_name", DEFAULT_ONLY_PATHS)
def test_every_unsized_path_lands_on_the_default(path_name, source, tmp_path, monkeypatch):
    n = pq.read_metadata(str(source)).num_rows
    if path_name.startswith("aggregate_h3_"):
        geometry = path_name.removeprefix("aggregate_h3_")
        out = tmp_path / "cells.parquet"
        _run(
            "process",
            "aggregate",
            "h3",
            source,
            out,
            "--resolution",
            "15",
            "--out-geometry",
            geometry,
        )
    elif path_name == "overview":
        cells = tmp_path / "cells.parquet"
        _run(
            "process",
            "aggregate",
            "h3",
            source,
            cells,
            "--resolution",
            "15",
            "--out-geometry",
            "none",
        )
        levels = tmp_path / "levels"
        levels.mkdir()
        _run("process", "overview", cells, "--levels", "14", "--output-dir", levels)
        (out,) = sorted(levels.glob("*.parquet"))
    else:
        from geoparquet_io.core.add import bbox_metadata
        from geoparquet_io.core.parquet_footer import FooterPatchUnsupported

        def refuse(*args, **kwargs):
            raise FooterPatchUnsupported("planted: take the fallback rewrite")

        monkeypatch.setattr(bbox_metadata, "patch_footer_kv", refuse)
        # The same rows and layout with an undeclared `bbox` struct to declare.
        # Not made by `gpio add bbox`, whose own 49,152-row groups would hide
        # the input-average rule this rewrite used to apply.
        out = tmp_path / "out.parquet"
        pq.write_table(
            grid_points_table(n).replace_schema_metadata(
                {b"geo": json.dumps(geo_block()).encode()}
            ),
            str(out),
            row_group_size=pq.read_metadata(str(source)).row_group(0).num_rows,
        )
        _run("add", "bbox-metadata", out)
    sizes = _group_sizes(out)
    assert sum(sizes) == n, f"{path_name}: the fixture lost its one-row-per-cell shape"
    assert len(sizes) == math.ceil(n / DEFAULT_ROW_GROUP_ROWS), f"{path_name}: {sizes}"


def test_the_fixture_makes_the_options_distinguishable(source):
    """Non-vacuity: on the big size, the three options give three different counts."""
    n = pq.read_metadata(str(source)).num_rows
    if n < max(SIZES):
        pytest.skip("checked on the larger size")
    counts = {option: _expected(source, option) for option in OPTIONS}
    assert len(set(counts.values())) == len(counts), counts
    assert pq.read_metadata(str(source)).num_row_groups not in counts.values()


# ---------------------------------------------------------------------------
# The rewrite rule (#1098)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("num_rows", "groups", "expected"),
    [
        (60_000, 1, DEFAULT_ROW_GROUP_ROWS),  # nothing to keep
        (200_000, 2, DEFAULT_ROW_GROUP_ROWS),  # the default already keeps 2
        (60_000, 12, 4_096),  # 5,000 per group -> the largest whole vector below
        (60_000, 2, DEFAULT_ROW_GROUP_ROWS),  # ceil(60,000 / 49,152) == 2
        (10_000, 50, WRITER_VECTOR_ROWS),  # 200 per group: floored at one vector
        (0, 0, DEFAULT_ROW_GROUP_ROWS),
    ],
)
def test_a_rewrite_keeps_at_least_the_input_row_groups(num_rows, groups, expected):
    rows = resolve_row_group_rows_for_rewrite(num_rows, groups)
    assert rows == expected
    if rows > WRITER_VECTOR_ROWS and groups > 1:
        assert math.ceil(num_rows / rows) >= groups


# The guard against a second row-group policy outside the owner is G1's
# ``row_group_policy`` arm in tests/test_owner_guards.py; its exemptions are in
# scripts/owner_exemptions.toml with every other owner guard's.


# ---------------------------------------------------------------------------
# The pyarrow kwargs take the owner's answer; they hold no fallback of their own
# ---------------------------------------------------------------------------


def test_the_pyarrow_settings_take_the_resolved_count_and_decide_none_of_their_own():
    """``get_pyarrow_kwargs`` once fell back to ``self.row_group_rows or DEFAULT``.

    That branch skipped the validation and the vector snap and ignored an MB
    target: a second policy, dormant only because its one caller always passed
    the resolved count. The count is now a required argument and the settings
    carry no row-group fields to fall back to.
    """
    import dataclasses

    from geoparquet_io.core.parquet_writer import ParquetWriteSettings

    fields = {field.name for field in dataclasses.fields(ParquetWriteSettings)}
    assert not fields & {"row_group_rows", "row_group_size_mb"}
    with pytest.raises(TypeError):
        ParquetWriteSettings().get_pyarrow_kwargs()  # type: ignore[call-arg]
    assert ParquetWriteSettings().get_pyarrow_kwargs(4_096)["row_group_size"] == 4_096


def test_the_mb_option_states_the_owners_rule():
    """``--row-group-size-mb``'s help said only what units it took.

    The rule the owner applies -- measured once, snapped to whole writer
    vectors, never below one -- is the one the docs state; the help is built
    from the same constants as ``--row-group-size``'s, so it cannot drift.
    """
    command = cli.commands["sort"].commands["hilbert"]
    option = next(p for p in command.params if p.name == "row_group_size_mb")
    assert f"{WRITER_VECTOR_ROWS:,}-row" in option.help
    assert "never below" in option.help
    assert "--row-group-size" in option.help


# ---------------------------------------------------------------------------
# A dry run prints the COPY gpio runs, row-group count included
# ---------------------------------------------------------------------------

_DRY_RUN_COMMANDS = {
    "extract": ("extract",),
    "add_bbox": ("add", "bbox"),
}


@pytest.mark.parametrize("option", sorted(OPTIONS))
@pytest.mark.parametrize("command", sorted(_DRY_RUN_COMMANDS))
def test_a_dry_run_prints_the_row_group_count_the_write_uses(
    command, option, source, tmp_path, caplog
):
    """The printed ``COPY`` had no ``ROW_GROUP_SIZE``: pasted into DuckDB it
    wrote DuckDB's 122,880-row groups, not the owner's count gpio writes."""
    import logging

    if pq.read_metadata(str(source)).num_rows <= DEFAULT_ROW_GROUP_ROWS:
        pytest.skip("needs an input with more than one row group at every option")
    args = (*_DRY_RUN_COMMANDS[command], source, tmp_path / "out.parquet", *OPTIONS[option])
    with caplog.at_level(logging.INFO, logger="geoparquet_io"):
        _run(*args, "--dry-run")
    shown = re.findall(r"ROW_GROUP_SIZE (\d+)", caplog.text)
    assert shown, f"the dry run printed no ROW_GROUP_SIZE:\n{caplog.text[-1500:]}"

    _run(*args)
    assert [int(n) for n in shown] == [_group_sizes(tmp_path / "out.parquet")[0]]


# ---------------------------------------------------------------------------
# The owner's MB measurement, and the 32-bit offsets it counts at
# ---------------------------------------------------------------------------


def _int64_rows(n: int) -> pa.Table:
    """``n`` rows of one int64 column: exactly 8 bytes a row."""
    return pa.table({"x": pa.array(range(n), pa.int64())})


def test_an_mb_target_becomes_a_whole_vector_count():
    from geoparquet_io.core.parquet_writer import _rows_for_byte_target, estimate_row_size
    from geoparquet_io.core.sizing import mb_to_bytes

    sample = _int64_rows(10_000)
    assert estimate_row_size(sample) == 8
    # 10,000 rows' worth of bytes is not a whole vector: it snaps up to 10,240.
    assert _rows_for_byte_target(sample, 10_000 * 8 / mb_to_bytes(1)) == 10_240
    # Exactly five vectors' worth stays five vectors.
    assert _rows_for_byte_target(sample, 5 * WRITER_VECTOR_ROWS * 8 / mb_to_bytes(1)) == 10_240


def test_an_mb_target_below_one_vector_floors_at_one_vector():
    from geoparquet_io.core.parquet_writer import _rows_for_byte_target

    assert _rows_for_byte_target(_int64_rows(10_000), 1e-6) == WRITER_VECTOR_ROWS


@pytest.mark.parametrize("mb", [0.05, 0.3, 1, 4.3, 64])
def test_an_mb_target_is_always_a_whole_vector_and_grows_with_the_target(mb):
    from geoparquet_io.core.parquet_writer import _rows_for_byte_target

    sample = _int64_rows(10_000)
    rows = _rows_for_byte_target(sample, mb)
    assert rows % WRITER_VECTOR_ROWS == 0 and rows >= WRITER_VECTOR_ROWS
    assert _rows_for_byte_target(sample, mb * 2) >= rows


def test_large_offset_types_weigh_what_the_small_ones_do():
    """#1215: a DuckDB sample (large types) and a pyarrow table (small types)
    holding the same rows must weigh the same, or one MB target lands on two counts."""
    from geoparquet_io.core.parquet_writer import estimate_row_size

    names = [f"name-{i:05d}" for i in range(1_000)]
    blobs = [name.encode() * 3 for name in names]

    def table(string_type, binary_type):
        return pa.table(
            {
                "s": pa.array(names, string_type),
                "b": pa.array(blobs, binary_type),
                "i": pa.array(range(1_000), pa.int64()),
            }
        )

    small = table(pa.string(), pa.binary())
    large = table(pa.large_string(), pa.large_binary())
    assert large.get_total_buffer_size() > small.get_total_buffer_size()
    assert estimate_row_size(large) == estimate_row_size(small)


# ---------------------------------------------------------------------------
# The funnel's geometry-free door
# ---------------------------------------------------------------------------


def _plain_table(n: int) -> pa.Table:
    """A geometry-free table that still carries what an input's schema would."""
    table = pa.table(
        {
            "id": pa.array(range(n), pa.int64()),
            "geom": pa.array([b"\x01" * 8] * n, pa.binary()),
        }
    )
    return table.replace_schema_metadata(
        {
            b"geo": json.dumps(geo_block()).encode(),
            b"pandas": b"{}",
            b"stac:collection": b'{"id": "c"}',
        }
    )


def test_the_plain_door_writes_no_geo_key_and_keeps_sidecar_keys(tmp_path):
    from geoparquet_io.core.write_funnels import write_plain_parquet_table

    out = tmp_path / "plain.parquet"
    write_plain_parquet_table(_plain_table(100), str(out))

    keys = pq.read_metadata(str(out)).metadata
    assert b"geo" not in keys, "a geometry-free output declared GeoParquet"
    assert b"pandas" not in keys, "the input's pandas block was carried"
    assert keys[b"stac:collection"] == b'{"id": "c"}'
    # ``geom`` is a plain column here: nothing is geometry by request.
    assert pq.read_schema(str(out)).field("geom").type == pa.binary()


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({}, [DEFAULT_ROW_GROUP_ROWS, 60_000 - DEFAULT_ROW_GROUP_ROWS]),
        ({"row_group_rows": 25_000}, [26_624, 26_624, 60_000 - 2 * 26_624]),
    ],
)
def test_the_plain_door_writes_the_owners_row_groups(tmp_path, kwargs, expected):
    from geoparquet_io.core.write_funnels import write_plain_parquet_table

    out = tmp_path / "plain.parquet"
    write_plain_parquet_table(_plain_table(60_000), str(out), **kwargs)
    assert _group_sizes(out) == expected


def test_the_plain_door_takes_the_owners_codec(tmp_path):
    from geoparquet_io.core.write_funnels import write_plain_parquet_table

    default, snappy = tmp_path / "default.parquet", tmp_path / "snappy.parquet"
    write_plain_parquet_table(_plain_table(100), str(default))
    write_plain_parquet_table(_plain_table(100), str(snappy), compression="snappy")
    assert pq.read_metadata(str(default)).row_group(0).column(0).compression == "ZSTD"
    assert pq.read_metadata(str(snappy)).row_group(0).column(0).compression == "SNAPPY"


def test_the_plain_door_refuses_a_bad_count_before_writing(tmp_path):
    from geoparquet_io.core.exceptions import InvalidParameterError
    from geoparquet_io.core.write_funnels import write_plain_parquet_table

    out = tmp_path / "plain.parquet"
    with pytest.raises(InvalidParameterError):
        write_plain_parquet_table(_plain_table(10), str(out), row_group_rows=0)
    assert not out.exists()
