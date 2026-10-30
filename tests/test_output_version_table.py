"""One output-version decision, asked of every write path (#1367 concept 1).

``parquet_writer.resolve_output_geoparquet_version`` is the only place an
output GeoParquet version is decided. Auto mode (no ``--geoparquet-version``)
reads the input file, then the carried ``geo`` key, then the table's own
schema, then falls back to the default; an explicit request always wins. The
table below is that rule, and every path is held to it:

=====================  ==========  ===========  ===========
input                  auto        ``1.1``      ``2.0``
=====================  ==========  ===========  ===========
GeoParquet 1.0 + bbox  1.1.0       1.1.0        2.0.0
GeoParquet 1.1 + bbox  1.1.0       1.1.0        2.0.0
GeoParquet 2.0         2.0.0       1.1.0        2.0.0
native, no ``geo``     2.0.0       1.1.0        2.0.0
=====================  ==========  ===========  ===========

Before the owner held the whole decision, the paths answered it themselves.
``add bbox`` passed a 1.0 input's version straight through when the bbox column
was already there (approved change 7: it now writes 1.1 like every other
command), and ``write_geoparquet_table`` wrote a GeoArrow table with no ``geo``
key as parquet-geo-only while ``Table.write`` and the file paths wrote it as 2.0
(approved change 10).

``add bbox`` copies its input only when the facade's answer is the version the
input already declares. So a native-geo-only input that already has a bbox
column is written at 2.0, as the one without a bbox always was; before, the
copy kept it with no ``geo`` key (recorded under approved change 7, "the facade
decides", and flagged for the maintainer as the one cell beyond its letter).
A repair (``check --fix``) asks the same resolver with ``preserve_input=True``
and keeps the input's own version: 1.0 stays 1.0, native-geo-only stays keyless.
"""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

STRATEGIES = ("in-memory", "streaming", "duckdb-kv", "disk-rewrite")

#: (input, requested) -> the ``geo.version`` the output declares.
EXPECTED = {
    ("v10", None): "1.1.0",
    ("v11", None): "1.1.0",
    ("v20", None): "2.0.0",
    ("pgo", None): "2.0.0",
    **{(name, "1.1"): "1.1.0" for name in ("v10", "v11", "v20", "pgo")},
    **{(name, "2.0"): "2.0.0" for name in ("v10", "v11", "v20", "pgo")},
}

_ROWS = """
    SELECT i AS id, ST_Point(i % 170 - 85, i % 80 - 40) AS geometry
    FROM range(200) t(i)
"""
_ROWS_WITH_BBOX = f"""
    SELECT *, STRUCT_PACK(xmin := ST_XMin(geometry), ymin := ST_YMin(geometry),
                          xmax := ST_XMax(geometry), ymax := ST_YMax(geometry)) AS bbox
    FROM ({_ROWS})
"""


def _copy(query: str, path, version: str) -> None:
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"COPY ({query}) TO {sql_path(str(path))} "
            f"(FORMAT PARQUET, GEOPARQUET_VERSION '{version}')"
        )
    finally:
        con.close()


def _declare(path, version: str, *, covering: bool) -> None:
    """Rewrite the file's ``geo`` block to declare *version* (and a covering at 1.1)."""
    table = pq.read_table(str(path))
    metadata = dict(table.schema.metadata or {})
    geo = json.loads(metadata[b"geo"])
    geo["version"] = version
    column = geo["columns"]["geometry"]
    column.pop("covering", None)
    if covering:
        column["covering"] = {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}}
    metadata[b"geo"] = json.dumps(geo).encode()
    pq.write_table(table.replace_schema_metadata(metadata), str(path))


@pytest.fixture(scope="module")
def inputs(tmp_path_factory):
    root = tmp_path_factory.mktemp("version_table")
    paths = {name: root / f"{name}.parquet" for name in ("v10", "v11", "v20", "pgo")}
    _copy(_ROWS_WITH_BBOX, paths["v10"], "V1")
    _declare(paths["v10"], "1.0.0", covering=False)
    _copy(_ROWS_WITH_BBOX, paths["v11"], "V1")
    _declare(paths["v11"], "1.1.0", covering=True)
    _copy(_ROWS, paths["v20"], "V2")
    _copy(_ROWS, paths["pgo"], "NONE")
    paths["pgo_bbox"] = root / "pgo_bbox.parquet"
    _copy(_ROWS_WITH_BBOX, paths["pgo_bbox"], "NONE")
    assert b"geo" not in (pq.ParquetFile(str(paths["pgo"])).metadata.metadata or {})
    return paths


def _declared_version(path) -> str | None:
    """The written file's ``geo.version``, or None when it has no ``geo`` key."""
    kv = pq.ParquetFile(str(path)).metadata.metadata or {}
    if b"geo" not in kv:
        return None
    return json.loads(kv[b"geo"])["version"]


def _invoke(*args, stdin: bytes | None = None):
    result = CliRunner().invoke(cli, [str(a) for a in args], input=stdin)
    assert result.exit_code == 0, result.output
    return result


def _flag(requested: str | None) -> list[str]:
    return [] if requested is None else ["--geoparquet-version", requested]


def _cli(command: list[str]):
    def run(src, out, requested):
        _invoke(*command, src, out, *_flag(requested))

    return run


def _streaming(*extra: str):
    """A stdin stream into ``add bbox``: the ``stream_io.write_output`` path.

    Without ``--force`` an input that already has its bbox column is passed
    through rather than recomputed -- the branch that used to keep a 1.0 input
    at 1.0 (approved change 7).
    """

    def run(src, out, requested):
        stream = _invoke("extract", src, "-").stdout_bytes
        _invoke("add", "bbox", "-", out, *extra, *_flag(requested), stdin=stream)

    return run


def _table_write(strategy: str):
    def run(src, out, requested):
        import geoparquet_io as gpio

        gpio.read(str(src)).write(str(out), write_strategy=strategy, geoparquet_version=requested)

    return run


def _table_funnel(src, out, requested):
    """``write_geoparquet_table``, the funnel every Arrow-side writer ends in."""
    import geoarrow.pyarrow  # noqa: F401  (a native column arrives as geoarrow.wkb)

    from geoparquet_io.core.write_funnels import write_geoparquet_table

    write_geoparquet_table(pq.read_table(str(src)), str(out), geoparquet_version=requested)


PATHS = {
    "convert geoparquet": _cli(["convert", "geoparquet"]),
    "add bbox": _cli(["add", "bbox"]),
    "sort hilbert": _cli(["sort", "hilbert"]),
    "extract": _cli(["extract"]),
    "streaming add bbox --force": _streaming("--force"),
    "streaming add bbox pass-through": _streaming(),
    "write_geoparquet_table": _table_funnel,
    **{f"Table.write {s}": _table_write(s) for s in STRATEGIES},
}


@pytest.mark.parametrize("path_name", sorted(PATHS))
@pytest.mark.parametrize(("input_name", "requested"), sorted(EXPECTED, key=str))
def test_every_write_path_declares_the_owners_version(
    inputs, tmp_path, path_name, input_name, requested
):
    out = tmp_path / "out.parquet"
    PATHS[path_name](inputs[input_name], out, requested)
    assert _declared_version(out) == EXPECTED[(input_name, requested)]


def test_add_bbox_on_a_1_0_input_with_a_bbox_is_not_copied_through(inputs, tmp_path):
    """Approved change 7: the existing bbox no longer pins the output at 1.0."""
    out = tmp_path / "out.parquet"
    result = _invoke("add", "bbox", inputs["v10"], out)
    assert _declared_version(out) == "1.1.0"
    assert "unchanged" not in result.output


def test_add_bbox_still_copies_an_input_already_at_the_owners_version(inputs, tmp_path):
    """The copy shortcut (#728) survives where it writes what the owner would."""
    out = tmp_path / "out.parquet"
    _invoke("add", "bbox", inputs["v11"], out)
    assert out.read_bytes() == inputs["v11"].read_bytes()


def test_parquet_geo_only_request_writes_no_geo_key_on_every_path(inputs, tmp_path):
    for index, (name, run) in enumerate(sorted(PATHS.items())):
        out = tmp_path / f"out_{index}.parquet"
        run(inputs["v11"], out, "parquet-geo-only")
        assert _declared_version(out) is None, name


def test_add_bbox_on_a_native_geo_only_input_with_a_bbox_writes_2_0(inputs, tmp_path):
    """The copy would keep no ``geo`` key; the facade's auto answer is 2.0."""
    out = tmp_path / "out.parquet"
    result = _invoke("add", "bbox", inputs["pgo_bbox"], out)
    assert result.exit_code == 0, result.output
    assert _declared_version(out) == "2.0.0"


@pytest.mark.parametrize(
    ("input_name", "preserved"),
    [("v10", "1.0"), ("v11", "1.1"), ("v20", "2.0"), ("pgo", "parquet-geo-only")],
)
def test_the_repair_policy_keeps_the_inputs_own_version(inputs, input_name, preserved):
    from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox
    from geoparquet_io.core.parquet_writer import (
        resolve_output_geoparquet_version,
        version_for_file_type,
    )

    path = str(inputs[input_name])
    resolved = resolve_output_geoparquet_version(None, input_file=path, preserve_input=True)
    bbox = check_metadata_and_bbox(path, verbose=False, return_results=True, quiet=True)
    from_checks = version_for_file_type(
        bbox.get("file_type"), bbox.get("version"), preserve_input=True
    )
    assert resolved == from_checks == preserved


@pytest.mark.parametrize("requested", ["3.0", "1.2", "", "2"])
def test_an_unknown_explicit_version_is_refused_not_written_as_1_1(requested):
    from geoparquet_io.core.exceptions import InvalidParameterError
    from geoparquet_io.core.parquet_writer import resolve_output_geoparquet_version

    with pytest.raises(InvalidParameterError, match="Unknown GeoParquet version"):
        resolve_output_geoparquet_version(requested)


def test_table_write_refuses_an_unknown_version(inputs, tmp_path):
    import geoparquet_io as gpio
    from geoparquet_io.core.exceptions import InvalidParameterError

    with pytest.raises(InvalidParameterError):
        gpio.read(str(inputs["v11"])).write(str(tmp_path / "out.parquet"), geoparquet_version="3.0")
    assert not (tmp_path / "out.parquet").exists()


@pytest.mark.parametrize("bad", [2, 1.1, ["2.0"], {"v": "2.0"}, True])
def test_a_non_string_declared_version_reads_as_none_everywhere(bad):
    import pyarrow as pa

    from geoparquet_io.api.table import Table
    from geoparquet_io.core.parquet_writer import declared_geoparquet_version

    geo = {"version": bad, "primary_column": "geometry", "columns": {"geometry": {}}}
    assert declared_geoparquet_version(geo, default=None) is None
    assert declared_geoparquet_version(geo) == ""
    table = pa.table({"geometry": pa.array([b""], pa.binary())})
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    assert Table(table).geoparquet_version is None
