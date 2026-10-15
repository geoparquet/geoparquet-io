"""#1276: an unknown CRS stays unknown when gpio writes it back out.

The GeoParquet spec says an omitted ``crs`` is OGC:CRS84 and ``crs: null`` is
unknown. At 2.0 the Parquet GEOMETRY type of an unknown column SHOULD carry
``srid:0``, because an unset type CRS is OGC:CRS84 too. Before this fix every
write path dropped the null, so a file with an unknown CRS came out declaring
lon/lat.

The rule, per output version:

* 1.0 / 1.1 keep ``crs: null`` in the ``geo`` block (there is no native type).
* 2.0 keeps ``crs: null`` and types the column ``srid:0``.
* parquet-geo-only has no ``geo`` block and types the column ``srid:0``.

The inputs are the three spellings of "unknown": a 1.1 null, a 2.0 null with a
``srid:0`` type, and a parquet-geo-only ``srid:0`` type. An empty ``crs``
(#1267) is unknown too and comes out the same way.
"""

from __future__ import annotations

import json
import struct

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.crs_utils import PARQUET_UNKNOWN_CRS, extract_crs_from_parquet

VERSIONS = ["1.1", "2.0", "parquet-geo-only"]
STRATEGIES = ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"]
# Lon/lat-sized numbers, so the coordinate-range check has nothing to say and
# the only thing `check spec` can object to is the CRS.
POINTS = [(10.0 + i, 20.0 + i) for i in range(5)]


def _wkb_points() -> pa.Array:
    return pa.array([struct.pack("<BIdd", 1, 1, x, y) for x, y in POINTS], pa.binary())


def _geo_block(version: str, crs) -> dict:
    return {
        "version": version,
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": crs}},
    }


def _write_v11(path, crs) -> str:
    table = pa.table({"id": list(range(5)), "geometry": _wkb_points()})
    table = table.replace_schema_metadata({"geo": json.dumps(_geo_block("1.1.0", crs))})
    pq.write_table(table, path)
    return str(path)


def _duckdb_srid0(path, kv_geo: dict | None) -> str:
    rows = " UNION ALL ".join(
        f"SELECT {i} AS id, ST_SetCRS(ST_Point({x}, {y}), 'srid:0') AS geometry"
        for i, (x, y) in enumerate(POINTS)
    )
    kv = f", KV_METADATA {{geo: '{json.dumps(kv_geo)}'}}" if kv_geo else ""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"COPY ({rows}) TO '{path}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE'{kv})")
    con.close()
    return str(path)


INPUTS = {
    "v11_null": lambda d: _write_v11(d / "v11_null.parquet", None),
    "v20_null_srid0": lambda d: _duckdb_srid0(
        d / "v20_null_srid0.parquet", _geo_block("2.0.0", None)
    ),
    "pgo_srid0": lambda d: _duckdb_srid0(d / "pgo_srid0.parquet", None),
}


def _type_crs(path) -> str | None:
    schema = pq.ParquetFile(path).schema
    logical = schema.column(schema.names.index("geometry")).logical_type
    return json.loads(logical.to_json()).get("crs") if logical.type != "NONE" else None


def _is_default_type_crs(crs) -> bool:
    """No type crs, or DuckDB's inline OGC:CRS84 PROJJSON: both mean the default."""
    if crs in (None, ""):
        return True
    text = crs.removeprefix("projjson:") if isinstance(crs, str) else crs
    try:
        obj = json.loads(text) if isinstance(text, str) else text
    except ValueError:
        return False
    ident = obj.get("id", {}) if isinstance(obj, dict) else {}
    return (ident.get("authority"), str(ident.get("code"))) == ("OGC", "CRS84")


def _geo_column(path) -> dict | None:
    raw = (pq.ParquetFile(path).metadata.metadata or {}).get(b"geo")
    return json.loads(raw)["columns"]["geometry"] if raw else None


def _crs_check_failures(path) -> list[dict]:
    result = CliRunner().invoke(cli, ["check", "spec", str(path), "--json"])
    report = json.loads(result.output)
    return [c for c in report["checks"] if c["status"] == "failed"]


def assert_unknown(path, version: str) -> None:
    """The output says "unknown" in every place its version has one."""
    col = _geo_column(path)
    if version == "parquet-geo-only":
        assert col is None
    else:
        assert col is not None and "crs" in col, f"crs key dropped: {col}"
        assert col["crs"] is None, col
    if version in ("2.0", "parquet-geo-only"):
        assert _type_crs(path) == PARQUET_UNKNOWN_CRS
    else:
        assert _type_crs(path) is None
    assert extract_crs_from_parquet(str(path)) is None
    assert _crs_check_failures(path) == []


# --------------------------------------------------------------------------- #
# CLI rewrites
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("source", list(INPUTS))
@pytest.mark.parametrize(
    "command", [["sort", "hilbert"], ["extract", "geoparquet"]], ids=lambda c: "-".join(c)
)
def test_cli_rewrite_keeps_unknown(command, source, version, tmp_path):
    src = INPUTS[source](tmp_path)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(cli, [*command, src, str(out), "--geoparquet-version", version])
    assert result.exit_code == 0, result.output
    assert_unknown(out, version)


# --------------------------------------------------------------------------- #
# Python API, every strategy
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("strategy", STRATEGIES)
@pytest.mark.parametrize("source", list(INPUTS))
def test_table_write_keeps_unknown(source, strategy, version, tmp_path):
    src = INPUTS[source](tmp_path)
    out = tmp_path / "out.parquet"
    gpio.read(src).write(str(out), geoparquet_version=version, write_strategy=strategy)
    assert_unknown(out, version)


# --------------------------------------------------------------------------- #
# Empty values (#1267) come out as null too
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("version", VERSIONS)
def test_empty_crs_is_written_as_unknown(version, tmp_path):
    # Only `{}` here: DuckDB refuses to *read* a source whose crs is "", [] or
    # 0 ("has invalid CRS"), loudly, before gpio sees a row. The unit tests on
    # `apply_output_crs` below cover those values.
    src = _write_v11(tmp_path / "in.parquet", {})
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli, ["sort", "hilbert", src, str(out), "--geoparquet-version", version]
    )
    assert result.exit_code == 0, result.output
    assert_unknown(out, version)


# --------------------------------------------------------------------------- #
# Unchanged: the default and a real CRS
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("version", VERSIONS)
def test_absent_crs_stays_the_default(version, tmp_path):
    table = pa.table({"id": list(range(5)), "geometry": _wkb_points()})
    block = _geo_block("1.1.0", None)
    del block["columns"]["geometry"]["crs"]
    pq.write_table(
        table.replace_schema_metadata({"geo": json.dumps(block)}), tmp_path / "in.parquet"
    )
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli,
        [
            "sort",
            "hilbert",
            str(tmp_path / "in.parquet"),
            str(out),
            "--geoparquet-version",
            version,
        ],
    )
    assert result.exit_code == 0, result.output
    col = _geo_column(out)
    assert col is None or "crs" not in col
    assert _is_default_type_crs(_type_crs(out)), _type_crs(out)


def test_reproject_with_assume_crs84_writes_the_default(tmp_path):
    src = _write_v11(tmp_path / "in.parquet", None)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(cli, ["convert", "reproject", src, str(out), "--assume-crs84"])
    assert result.exit_code == 0, result.output
    col = _geo_column(out)
    assert col is not None and "crs" not in col


# --------------------------------------------------------------------------- #
# Read side: srid:0 is unknown, never a CRS string to carry
# --------------------------------------------------------------------------- #


def test_srid0_type_reads_as_unknown(tmp_path):
    src = INPUTS["pgo_srid0"](tmp_path)
    assert extract_crs_from_parquet(src) is None


def test_srid0_is_never_written_into_the_geo_block(tmp_path):
    src = INPUTS["pgo_srid0"](tmp_path)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli, ["extract", "geoparquet", src, str(out), "--geoparquet-version", "1.1"]
    )
    assert result.exit_code == 0, result.output
    assert _geo_column(out)["crs"] is None


# --------------------------------------------------------------------------- #
# The Arrow-side funnel (arcgis, carto, wfs, bigquery, aggregate, overview)
# --------------------------------------------------------------------------- #


def _unknown_table(crs) -> pa.Table:
    table = pa.table({"id": list(range(5)), "geometry": _wkb_points()})
    return table.replace_schema_metadata({"geo": json.dumps(_geo_block("1.1.0", crs))})


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("crs", [None, {}], ids=repr)
def test_write_geoparquet_table_keeps_unknown(crs, version, tmp_path):
    from geoparquet_io.core.write_funnels import write_geoparquet_table

    out = tmp_path / "out.parquet"
    write_geoparquet_table(_unknown_table(crs), str(out), geoparquet_version=version)
    assert_unknown(out, version)


# --------------------------------------------------------------------------- #
# Unit level: the helpers the write paths share
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("srid:0", True),
        (" SRID:0 ", True),
        ("srid:5070", False),
        ("", False),
        (None, False),
        ({"id": {"authority": "OGC", "code": "CRS84"}}, False),
    ],
)
def test_is_unknown_type_crs(value, expected):
    from geoparquet_io.core.crs_utils import is_unknown_type_crs

    assert is_unknown_type_crs(value) is expected


@pytest.mark.parametrize("value", [None, "", [], 0, {}], ids=repr)
def test_apply_output_crs_keeps_an_empty_or_null_crs_as_null(value):
    from geoparquet_io.core.crs_utils import apply_output_crs

    col_meta = {"encoding": "WKB", "crs": value}
    apply_output_crs(col_meta, None)
    assert "crs" in col_meta and col_meta["crs"] is None


def test_apply_output_crs_writes_null_for_the_unknown_marker():
    from geoparquet_io.core.crs_utils import apply_output_crs

    col_meta = {"encoding": "WKB"}
    apply_output_crs(col_meta, PARQUET_UNKNOWN_CRS)
    assert col_meta == {"encoding": "WKB", "crs": None}


def test_apply_output_crs_leaves_an_absent_crs_absent():
    from geoparquet_io.core.crs_utils import apply_output_crs

    col_meta = {"encoding": "WKB"}
    apply_output_crs(col_meta, None)
    assert "crs" not in col_meta


@pytest.mark.parametrize(
    ("version", "wrapped"),
    [("1.0", False), ("1.1", False), (None, False), ("2.0", True), ("parquet-geo-only", True)],
)
def test_wrap_query_sets_srid0_only_where_the_type_carries_it(version, wrapped):
    from geoparquet_io.core.crs_utils import _wrap_query_with_crs

    query = "SELECT * FROM t"
    out = _wrap_query_with_crs(query, "geometry", PARQUET_UNKNOWN_CRS, version)
    if wrapped:
        assert "ST_SetCRS" in out and "'srid:0'" in out
    else:
        assert out == query


@pytest.mark.parametrize("source", list(INPUTS))
def test_source_crs_is_unknown_for_every_unknown_input(source, tmp_path):
    from geoparquet_io.core.crs_utils import source_crs_is_unknown

    assert source_crs_is_unknown(INPUTS[source](tmp_path)) is True


def test_source_crs_is_unknown_is_false_for_absent_and_named(tmp_path):
    from geoparquet_io.core.crs_utils import source_crs_is_unknown

    table = pa.table({"id": list(range(5)), "geometry": _wkb_points()})
    block = _geo_block("1.1.0", None)
    del block["columns"]["geometry"]["crs"]
    absent = tmp_path / "absent.parquet"
    pq.write_table(table.replace_schema_metadata({"geo": json.dumps(block)}), absent)
    assert source_crs_is_unknown(str(absent)) is False

    named = _write_v11(tmp_path / "named.parquet", "EPSG:3857")
    assert source_crs_is_unknown(named) is False


def test_table_crs_is_unknown(tmp_path):
    from geoparquet_io.core.crs_utils import table_crs_is_unknown

    assert table_crs_is_unknown(_unknown_table(None), "geometry") is True
    assert table_crs_is_unknown(_unknown_table({}), "geometry") is True
    assert (
        table_crs_is_unknown(
            _unknown_table({"id": {"authority": "EPSG", "code": 3857}}), "geometry"
        )
        is False
    )
    assert table_crs_is_unknown(_unknown_table(None), None) is False
    assert table_crs_is_unknown(_unknown_table(None), "missing") is False

    bare = pa.table({"geometry": _wkb_points()})
    assert table_crs_is_unknown(bare, "geometry") is False

    def raw(ext_metadata):
        field = pa.field(
            "geometry",
            pa.binary(),
            metadata={
                "ARROW:extension:name": "geoarrow.wkb",
                "ARROW:extension:metadata": ext_metadata,
            },
        )
        return pa.Table.from_arrays([_wkb_points()], schema=pa.schema([field]))

    assert table_crs_is_unknown(raw('{"crs": "0", "crs_type": "srid"}'), "geometry") is True
    assert table_crs_is_unknown(raw('{"crs": "srid:0"}'), "geometry") is True
    assert table_crs_is_unknown(raw('{"crs": "5070", "crs_type": "srid"}'), "geometry") is False
    assert table_crs_is_unknown(raw("{not json"), "geometry") is False

    import geoarrow.pyarrow as ga

    typ = ga.wkb().with_crs("srid:0")
    ext = pa.table({"geometry": pa.ExtensionArray.from_storage(typ, _wkb_points())})
    assert table_crs_is_unknown(ext, "geometry") is True


def test_resolve_crs_reference_reads_srid0_as_unknown():
    from geoparquet_io.core.duckdb_metadata import resolve_crs_reference

    assert resolve_crs_reference("unused.parquet", "srid:0") is None
    assert resolve_crs_reference("unused.parquet", "SRID: 0 ") is None


@pytest.mark.parametrize(
    ("col_meta", "expected"),
    [
        ({"encoding": "WKB", "crs": None}, PARQUET_UNKNOWN_CRS),
        ({"encoding": "WKB"}, None),
    ],
)
def test_native_geometry_crs_types_an_explicit_null_srid0(col_meta, expected):
    from geoparquet_io.core.write_strategies.base import native_geometry_crs

    geo = {"version": "2.0.0", "primary_column": "geometry", "columns": {"geometry": col_meta}}
    assert native_geometry_crs("2.0", geo, "geometry") == {"geometry": expected}
    assert native_geometry_crs("1.1", geo, "geometry") == {}


def test_native_wkb_type_marks_the_unknown_marker_srid0():
    from geoparquet_io.core.geoarrow_encoding import native_wkb_type

    typ = native_wkb_type(PARQUET_UNKNOWN_CRS)
    assert "srid:0" in str(typ.crs.__geoarrow_crs_json_values__()["crs"])
