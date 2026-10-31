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
