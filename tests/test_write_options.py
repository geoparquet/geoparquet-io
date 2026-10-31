"""The write-options object (#1367, T5c): one value carries the write facts.

Every write path used to forward the same seven keywords (``compression``,
``compression_level``, ``row_group_size_mb``, ``row_group_rows``, ``profile``,
``geoparquet_version``, ``memory_limit``) from its public entry point through
each private driver to the funnel; pylint's duplicate-code check reported those
lists as 18 copies. ``parquet_writer.WriteOptions`` is the one value they travel
in now: built once at the entry point, expanded once at the funnel.
"""

from __future__ import annotations

import dataclasses
import inspect

import pytest

from geoparquet_io.core.parquet_writer import WriteOptions
from geoparquet_io.core.stream_io import execute_transform, write_output
from geoparquet_io.core.write_funnels import write_parquet_with_metadata

FIELDS = (
    "compression",
    "compression_level",
    "row_group_size_mb",
    "row_group_rows",
    "profile",
    "geoparquet_version",
    "memory_limit",
)


def test_the_fields_are_the_forwarded_write_keywords():
    assert tuple(f.name for f in dataclasses.fields(WriteOptions)) == FIELDS


def test_it_is_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        WriteOptions().compression = "GZIP"  # type: ignore[misc]


def test_every_default_is_the_funnels_own_default():
    """An unset field means what leaving the keyword out of the funnel means."""
    signature = inspect.signature(write_parquet_with_metadata)
    for name, value in WriteOptions().funnel_kwargs().items():
        assert value == signature.parameters[name].default, name


@pytest.mark.parametrize("funnel", [write_parquet_with_metadata, write_output, execute_transform])
def test_every_funnel_takes_every_field(funnel):
    assert set(FIELDS) <= set(inspect.signature(funnel).parameters)


def test_pick_reads_the_fields_by_name_and_ignores_the_rest():
    values = {
        "compression": "GZIP",
        "compression_level": 6,
        "row_group_size_mb": 64.0,
        "row_group_rows": None,
        "profile": "p",
        "geoparquet_version": "2.0",
        "memory_limit": "2GB",
        "verbose": True,
        "input_parquet": "in.parquet",
    }
    options = WriteOptions.pick(values)
    assert options == WriteOptions("GZIP", 6, 64.0, None, "p", "2.0", "2GB")
    assert options.funnel_kwargs() == {k: values[k] for k in FIELDS}


def test_pick_leaves_an_absent_field_at_its_default():
    assert WriteOptions.pick({"compression": "LZ4"}) == WriteOptions(compression="LZ4")


# ---------------------------------------------------------------------------
# End to end: the options a user types land in the footer, per driver family.
# ---------------------------------------------------------------------------

ROWS = 5000
GROUP = 2048
WRITE_FLAGS = [
    "--compression",
    "brotli",
    "--compression-level",
    "5",
    "--row-group-size",
    str(GROUP),
]


@pytest.fixture(scope="module")
def points_file(tmp_path_factory):
    """5,000 points with a two-value category and a numeric sort key."""
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    root = tmp_path_factory.mktemp("write_options_e2e")
    csv = root / "points.csv"
    lines = ["lat,lon,cat,val"]
    for i in range(ROWS):
        lines.append(f"{(i % 170) - 85 + 0.5},{(i % 350) - 175 + 0.25},{'ab'[i % 2]},{ROWS - i}")
    csv.write_text("\n".join(lines) + "\n")
    out = root / "points.parquet"
    result = CliRunner().invoke(cli, ["convert", str(csv), str(out)])
    assert result.exit_code == 0, result.output
    return out


def _assert_footer(path, rows=None):
    import pyarrow.parquet as pq

    md = pq.read_metadata(path)
    total = md.num_rows if rows is None else rows
    assert md.num_rows == total
    assert md.num_row_groups == -(-total // GROUP), (path, md.num_row_groups)
    for rg in range(md.num_row_groups):
        assert md.row_group(rg).num_rows <= GROUP
        for col in range(md.num_columns):
            assert md.row_group(rg).column(col).compression == "BROTLI"


E2E = {
    "add bbox": ["add", "bbox", "{i}", "{o}"],
    "add h3": ["add", "h3", "{i}", "{o}", "--resolution", "5"],
    "sort column": ["sort", "column", "{i}", "{o}", "val"],
    "sort quadkey": ["sort", "quadkey", "{i}", "{o}"],
    "convert reproject": ["convert", "reproject", "{i}", "{o}", "--dst-crs", "EPSG:3857"],
    "process simplify": ["process", "simplify", "{i}", "{o}", "--tolerance", "0.001"],
}


@pytest.mark.parametrize("name", sorted(E2E))
def test_the_write_flags_reach_the_footer(name, points_file, tmp_path):
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    out = tmp_path / "out.parquet"
    argv = [a.format(i=points_file, o=out) for a in E2E[name]]
    result = CliRunner().invoke(cli, [*argv, *WRITE_FLAGS])
    assert result.exit_code == 0, result.output
    _assert_footer(out)


def test_the_write_flags_reach_every_partition_file(points_file, tmp_path):
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    out = tmp_path / "parts"
    result = CliRunner().invoke(
        cli, ["partition", "string", str(points_file), str(out), "--column", "cat", *WRITE_FLAGS]
    )
    assert result.exit_code == 0, result.output
    files = sorted(out.rglob("*.parquet"))
    assert len(files) == 2
    for path in files:
        _assert_footer(path, rows=ROWS // 2)


def test_piped_reproject_honours_the_write_flags(points_file, tmp_path):
    """The stdin path of `convert reproject` used to drop the row-group flags."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    table = pq.read_table(points_file)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli,
        ["convert", "reproject", "-", str(out), "--dst-crs", "EPSG:3857", *WRITE_FLAGS],
        input=sink.getvalue().to_pybytes(),
    )
    assert result.exit_code == 0, result.output
    _assert_footer(out)


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        (WriteOptions(), False),
        (WriteOptions(profile="p", memory_limit="1GB"), False),
        (WriteOptions(compression="zstd"), True),
        (WriteOptions(compression_level=3), True),
        (WriteOptions(row_group_size_mb=64), True),
        (WriteOptions(row_group_rows=1000), True),
        (WriteOptions(geoparquet_version="2.0"), True),
    ],
)
def test_sets_output_layout_names_the_byte_shaping_options(options, expected):
    assert options.sets_output_layout() is expected


def test_the_table_funnels_take_the_whole_value(tmp_path):
    """No per-site exclusions: both Arrow-table funnels accept all seven fields."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from geoparquet_io.core.write_funnels import write_geoparquet_table, write_plain_parquet_table

    full = WriteOptions(
        compression="zstd", row_group_rows=10, geoparquet_version="1.1", memory_limit="1GB"
    )
    write_plain_parquet_table(
        pa.table({"a": [1, 2]}), str(tmp_path / "p.parquet"), **full.funnel_kwargs()
    )
    assert pq.ParquetFile(tmp_path / "p.parquet").metadata.num_rows == 2
    shapely = pytest.importorskip("shapely")
    geoms = shapely.to_wkb([shapely.Point(0, 0)]).tolist()
    write_geoparquet_table(
        pa.table({"geometry": geoms}),
        str(tmp_path / "g.parquet"),
        geometry_column="geometry",
        **full.funnel_kwargs(),
    )
    assert b"geo" in pq.ParquetFile(tmp_path / "g.parquet").schema_arrow.metadata


def test_the_sample_resolver_owns_the_bound():
    """resolve_row_group_rows_for_sample measures at most the facade's sample rows."""
    import pyarrow as pa

    from geoparquet_io.core.parquet_writer import (
        _MB_ESTIMATE_SAMPLE_ROWS,
        resolve_row_group_rows_for_sample,
        resolve_row_group_rows_for_table,
    )

    small = pa.table({"s": ["x" * 10] * 1000})
    assert resolve_row_group_rows_for_sample(small, None, 1) == resolve_row_group_rows_for_table(
        small, None, 1
    )
    # Heavy rows after the sample do not move the answer.
    head = pa.table({"s": ["x" * 10] * _MB_ESTIMATE_SAMPLE_ROWS})
    tail = pa.table({"s": ["x" * 5000] * 1000})
    mixed = pa.concat_tables([head, tail])
    assert resolve_row_group_rows_for_sample(mixed, None, 1) == resolve_row_group_rows_for_table(
        head, None, 1
    )
    # Below the bound with more rows to come, a byte target waits for them.
    assert resolve_row_group_rows_for_sample(small, None, 1, final=False) is None
    # An explicit row count wins, exactly as the facade resolves it.
    assert resolve_row_group_rows_for_sample(mixed, 4096, None) == 4096
