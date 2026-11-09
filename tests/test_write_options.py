"""The write-options object (#1367, T5c; reshaped by #761): one value carries the write facts.

Every write path used to forward the same seven keywords (``compression``,
``compression_level``, ``row_group_size_mb``, ``row_group_rows``, ``profile``,
``geoparquet_version``, ``memory_limit``) from its public entry point through
each private driver to the funnel; pylint's duplicate-code check reported those
lists as 18 copies. ``parquet_writer.WriteOptions`` is the one value they travel
in now: built once at the entry point, expanded once at the funnel. #761 made it
public (``gpio.WriteOptions``) and added ``write_strategy``.
"""

from __future__ import annotations

import dataclasses
import inspect

import pytest

from geoparquet_io.core.parquet_writer import WriteOptions
from geoparquet_io.core.stream_io import execute_transform, write_output
from geoparquet_io.core.write_funnels import write_geoparquet_table, write_parquet_with_metadata

#: The keywords every funnel takes; ``funnel_kwargs`` emits these and no others.
FUNNEL_FIELDS = (
    "compression",
    "compression_level",
    "row_group_size_mb",
    "row_group_rows",
    "profile",
    "geoparquet_version",
    "memory_limit",
)
FIELDS = (*FUNNEL_FIELDS, "write_strategy")


def test_the_fields_are_the_funnel_keywords_and_the_strategy():
    assert tuple(f.name for f in dataclasses.fields(WriteOptions)) == FIELDS


def test_every_field_defaults_to_not_asked():
    """None is "not asked" -- ZSTD and auto at the write step -- so asking is visible.

    A ZSTD default made ``WriteOptions()`` indistinguishable from an explicit
    ``compression="ZSTD"``: a `.csv` "written with ZSTD" was not refused, and the
    same codec set in the options and as a keyword raised no conflict.
    """
    assert all(getattr(WriteOptions(), name) is None for name in FIELDS)


def test_it_is_frozen_and_keyword_only():
    with pytest.raises(dataclasses.FrozenInstanceError):
        WriteOptions().compression = "GZIP"  # type: ignore[misc]
    with pytest.raises(TypeError):
        WriteOptions("GZIP")  # type: ignore[misc]


def test_every_default_means_what_the_funnel_default_means():
    """An unset field is left out, so the funnel's own default (ZSTD for the codec) applies."""
    from geoparquet_io.core.compression import resolve_codec

    assert WriteOptions().funnel_kwargs() == {}
    assert resolve_codec(None, None, context="write").name == "ZSTD"


@pytest.mark.parametrize(
    "funnel", [write_parquet_with_metadata, write_geoparquet_table, write_output, execute_transform]
)
def test_every_funnel_takes_every_funnel_field(funnel):
    assert set(FUNNEL_FIELDS) <= set(inspect.signature(funnel).parameters)


def test_funnel_kwargs_emits_only_the_funnel_keywords_and_never_none():
    full = WriteOptions(
        compression=None,
        compression_level=6,
        row_group_size_mb=None,
        row_group_rows=4096,
        profile="p",
        geoparquet_version=None,
        memory_limit="2GB",
        write_strategy="streaming",
    )
    assert full.funnel_kwargs() == {
        "compression_level": 6,
        "row_group_rows": 4096,
        "profile": "p",
        "memory_limit": "2GB",
    }
    assert set(WriteOptions(write_strategy="disk-rewrite").funnel_kwargs()) <= set(FUNNEL_FIELDS)


def test_pick_reads_the_fields_by_name_and_ignores_the_rest():
    values = {
        "compression": "GZIP",
        "compression_level": 6,
        "row_group_size_mb": 64.0,
        "row_group_rows": None,
        "profile": "p",
        "geoparquet_version": "2.0",
        "memory_limit": "2GB",
        "write_strategy": "streaming",
        "verbose": True,
        "input_parquet": "in.parquet",
    }
    options = WriteOptions.pick(values)
    assert options == WriteOptions(**{k: values[k] for k in FIELDS})
    assert options.funnel_kwargs() == {k: values[k] for k in FUNNEL_FIELDS if values[k] is not None}


def test_pick_leaves_an_absent_field_at_its_default():
    assert WriteOptions.pick({"compression": "LZ4"}) == WriteOptions(compression="LZ4")


def _points_table(rows=3000):
    import pyarrow as pa

    shapely = pytest.importorskip("shapely")
    geoms = shapely.to_wkb(shapely.points([[i * 0.01, i * 0.02] for i in range(rows)])).tolist()
    return pa.table({"id": list(range(rows)), "geometry": geoms})


def test_auto_is_the_query_funnels_duckdb_kv(tmp_path):
    """Each entry point resolves "auto" to the strategy it used to default to."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection()
    try:
        con.register("t", _points_table())
        query = "SELECT id, ST_GeomFromWKB(geometry) AS geometry FROM t"
        for name in ("auto", "duckdb-kv"):
            write_parquet_with_metadata(
                con, query, str(tmp_path / f"{name}.parquet"), write_strategy=name
            )
    finally:
        con.close()
    assert (tmp_path / "auto.parquet").read_bytes() == (tmp_path / "duckdb-kv.parquet").read_bytes()


def test_auto_is_the_table_funnels_arrow_writer(tmp_path):
    table = _points_table()
    write_geoparquet_table(table, str(tmp_path / "auto.parquet"), write_strategy="auto")
    write_geoparquet_table(table, str(tmp_path / "none.parquet"))
    assert (tmp_path / "auto.parquet").read_bytes() == (tmp_path / "none.parquet").read_bytes()


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
        (WriteOptions(compression=None), False),
        (
            WriteOptions(
                compression=None, profile="p", memory_limit="1GB", write_strategy="streaming"
            ),
            False,
        ),
        # Only None means "not asked", and every default is None.
        (WriteOptions(), False),
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


# ---------------------------------------------------------------------------
# Table.write(path, options=None, *, ...) (#761)
# ---------------------------------------------------------------------------


@pytest.fixture
def places(places_test_file):
    import geoparquet_io as gpio

    return gpio.read(places_test_file)


def _footer(path):
    import pyarrow.parquet as pq

    md = pq.read_metadata(path)
    return md.row_group(0).column(0).compression, md.row_group(0).num_rows


def test_every_table_write_argument_after_options_is_keyword_only():
    from geoparquet_io.api.table import Table

    params = list(inspect.signature(Table.write).parameters.values())
    assert [p.name for p in params[:3]] == ["self", "path", "options"]
    assert all(p.kind is inspect.Parameter.KEYWORD_ONLY for p in params[3:])


def test_options_and_keywords_write_the_same_bytes(places, tmp_path):
    # streaming writes a sub-vector row count exactly (duckdb-kv rounds it up).
    knobs = {"compression": "GZIP", "row_group_rows": 100, "write_strategy": "streaming"}
    places.write(tmp_path / "a.parquet", WriteOptions(**knobs))
    places.write(tmp_path / "b.parquet", **knobs)
    assert (tmp_path / "a.parquet").read_bytes() == (tmp_path / "b.parquet").read_bytes()
    assert _footer(tmp_path / "a.parquet") == ("GZIP", 100)


def test_a_format_in_the_options_slot_names_format(places, tmp_path):
    with pytest.raises(TypeError, match="format="):
        places.write(tmp_path / "out.dat", "csv")  # type: ignore[arg-type]


def test_keywords_merge_into_the_options(places, tmp_path):
    options = WriteOptions(compression="GZIP", write_strategy="streaming")
    places.write(tmp_path / "out.parquet", options, row_group_rows=100)
    assert _footer(tmp_path / "out.parquet") == ("GZIP", 100)


@pytest.mark.parametrize("asked", ["GZIP", "ZSTD"])
def test_one_field_set_both_ways_is_an_error(places, tmp_path, asked):
    """ZSTD too: the codec an unset field resolves to is still a codec asked for."""
    with pytest.raises(TypeError, match="compression"):
        places.write(tmp_path / "out.parquet", WriteOptions(compression=asked), compression="LZ4")


@pytest.mark.parametrize(
    ("name", "kwargs"),
    [
        ("out.csv", {"compression": "GZIP"}),
        ("out.csv", {"compression": "ZSTD"}),
        ("out.geojson", {"options": WriteOptions(memory_limit="1GB")}),
        ("out.gpkg", {"write_strategy": "streaming"}),
    ],
)
def test_a_write_option_on_another_format_is_refused(places, tmp_path, name, kwargs):
    """These were silently ignored; a `.csv` written "with GZIP" was not."""
    from geoparquet_io.core.exceptions import InvalidParameterError

    with pytest.raises(InvalidParameterError, match="GeoParquet"):
        places.write(tmp_path / name, **kwargs)
    assert not (tmp_path / name).exists()


def test_default_options_on_another_format_are_fine(places, tmp_path):
    places.write(tmp_path / "out.csv", WriteOptions())
    assert (tmp_path / "out.csv").exists()


def test_table_write_honours_memory_limit(places, tmp_path, caplog):
    places.write(tmp_path / "out.parquet", memory_limit="517MB", verbose=True)
    assert "DuckDB memory limit: 517MB" in caplog.text


# ---------------------------------------------------------------------------
# The CLI builds one WriteOptions per write command (#761)
# ---------------------------------------------------------------------------


def test_from_cli_maps_the_cli_spellings():
    built = WriteOptions.from_cli(
        compression="GZIP",
        compression_level=3,
        row_group_size=4096,
        row_group_size_mb=None,
        write_memory="1GB",
        geoparquet_version="2.0",
        write_strategy="streaming",
    )
    assert built == WriteOptions(
        compression="GZIP",
        compression_level=3,
        row_group_rows=4096,
        memory_limit="1GB",
        geoparquet_version="2.0",
        write_strategy="streaming",
    )


def test_from_cli_keeps_an_unset_flag_unset_and_never_reads_threads():
    """`process simplify --threads` is its coarsen workers, not a write setting."""
    built = WriteOptions.from_cli(compression=None, threads=8, verbose=True)
    assert built == WriteOptions(compression=None)
    assert not built.sets_output_layout()


def _packed(**params):
    from geoparquet_io.cli.decorators import pack_write_options

    return pack_write_options(lambda **kwargs: kwargs)(**params)


def test_the_packer_hands_over_one_write_and_keeps_the_rest():
    got = _packed(compression="LZ4", row_group_size=None, row_group_size_mb="2MB", verbose=True)
    assert got == {"verbose": True, "write": WriteOptions(compression="LZ4", row_group_size_mb=2.0)}


def test_the_packer_refuses_rows_and_megabytes_together():
    import click

    with pytest.raises(click.UsageError, match="mutually exclusive"):
        _packed(row_group_size=4096, row_group_size_mb="2MB")


#: The Click parameters the write decorators declare.
RAW_WRITE_PARAMS = frozenset(
    {
        "compression",
        "compression_level",
        "row_group_size",
        "row_group_size_mb",
        "write_memory",
        "geoparquet_version",
        "write_strategy",
    }
)

#: Leaves that carry a write flag but build no WriteOptions, and why.
NOT_WRITE_LEAVES = {
    "check spec": "--geoparquet-version names the version to validate against; nothing is written.",
    "pmtiles pyramid": (
        "--write-memory bounds the DuckDB queries that build the tiles; the output is "
        "PMTiles, with no GeoParquet write for a WriteOptions to describe."
    ),
}


def _write_leaves():
    import click

    from geoparquet_io.cli.main import cli

    out = {}

    def walk(cmd, path):
        if isinstance(cmd, click.Group):
            for name, sub in cmd.commands.items():
                walk(sub, (*path, name))
        elif {p.name for p in cmd.params} & RAW_WRITE_PARAMS:
            out[" ".join(path)] = cmd

    walk(cli, ())
    return {leaf: cmd for leaf, cmd in out.items() if leaf not in NOT_WRITE_LEAVES}


def test_no_write_leaf_takes_raw_write_flags():
    """Structural: each write command's body gets one `write`, never the flags.

    A body that takes the flags can drop one on the way to core (#627, #663);
    a body that takes only `write` passes it whole.
    """
    offenders = {}
    for leaf, cmd in _write_leaves().items():
        params = set(inspect.signature(inspect.unwrap(cmd.callback)).parameters)
        if params & RAW_WRITE_PARAMS or "write" not in params:
            offenders[leaf] = sorted(params & RAW_WRITE_PARAMS)
    assert len(_write_leaves()) >= 30
    assert not offenders, offenders


def test_the_exempt_leaves_still_carry_a_write_flag():
    from geoparquet_io.cli.main import cli

    for leaf in NOT_WRITE_LEAVES:
        group, name = leaf.split()
        params = {p.name for p in cli.commands[group].commands[name].params}
        assert params & RAW_WRITE_PARAMS, f"{leaf} is no longer exempt; drop it"


def test_the_packer_takes_every_write_flag():
    from geoparquet_io.cli.decorators import WRITE_OPTION_PARAMS

    assert set(WRITE_OPTION_PARAMS) == RAW_WRITE_PARAMS


#: A non-default value per write flag, and the core keyword it must arrive as.
NON_DEFAULT_FLAGS = {
    "compression": (("--compression", "GZIP"), "compression", "GZIP"),
    "compression_level": (("--compression-level", "5"), "compression_level", 5),
    "row_group_size": (("--row-group-size", "4096"), "row_group_rows", 4096),
    "geoparquet_version": (("--geoparquet-version", "1.0"), "geoparquet_version", "1.0"),
    "write_memory": (("--write-memory", "1GB"), "memory_limit", "1GB"),
    "write_strategy": (("--write-strategy", "in-memory"), "write_strategy", "in-memory"),
}


def _write_cases():
    from tests.test_cli_api_call_parity_scaffold import CASES_BY_ID

    return sorted(leaf for leaf in _write_leaves() if leaf in CASES_BY_ID)


@pytest.mark.parametrize("leaf", _write_cases())
def test_every_write_flag_reaches_core(leaf, places_test_file, geojson_input, tmp_path):
    """Behavioural: each flag a write leaf declares arrives at its core call.

    The leaf's `ParityCase` names the core boundary; the run adds a non-default
    value for every write flag the leaf has.
    """
    from tests.test_cli_api_call_parity_scaffold import (
        CASES_BY_ID,
        MISSING,
        Ctx,
        _capture,
    )

    declared = {p.name for p in _write_leaves()[leaf].params}
    flags = {name: spec for name, spec in NON_DEFAULT_FLAGS.items() if name in declared}
    extra = [word for argv, _core, _value in flags.values() for word in argv]

    class FlaggedCtx(Ctx):
        def run_cli(self, args):
            return super().run_cli([*args, *extra])

    import pyarrow.parquet as pq
    from click.testing import CliRunner

    ctx = FlaggedCtx(
        input_file=places_test_file,
        convert_input=geojson_input,
        output_file=str(tmp_path / "out.parquet"),
        output_dir=str(tmp_path / "out_dir"),
        table=pq.read_table(places_test_file),
        gpio_table=None,
        runner=CliRunner(),
        tmp_path=tmp_path,
    )
    call = _capture(CASES_BY_ID[leaf].cli, ctx)
    dropped = {
        flag: f"{core}={call.get(core, MISSING)!r}, wanted {value!r}"
        for flag, (_argv, core, value) in flags.items()
        if call.get(core, MISSING) != value
    }
    assert not dropped, f"`gpio {leaf}` dropped write flags on the way to core: {dropped}"


def test_write_options_is_a_root_export():
    import geoparquet_io as gpio

    assert "WriteOptions" in gpio.__all__
    assert gpio.WriteOptions is WriteOptions
