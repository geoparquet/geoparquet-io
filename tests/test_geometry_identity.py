"""Concept 7 of #1367: one primary-geometry-column resolver, one read expression.

``geometry_detection.primary_geometry_column(source)`` answers "which column is
the primary geometry" for every kind of source a caller holds -- a file, an
Arrow table or schema, a list of column names, or a described ``(name, type)``
schema -- with one rule: the block's primary when the schema has it, else the
first standard geometry name (priority order, case-insensitive, the schema's
own spelling), else the first native geometry column, else ``None``. Never the
literal ``"geometry"``: a schema with no geometry-like column answers ``None``.

``geometry_detection.resolve_geometry_read`` is the one place that turns a
column's DuckDB type into SQL that reads it as GEOMETRY (WKB BLOB, native
GEOMETRY/GEOGRAPHY, GeoArrow struct). The table below pins its answer on both
DuckDB majors.
"""

from __future__ import annotations

import json
import struct
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.duckdb_utils import (
    describe_query,
    get_duckdb_connection,
    register_arrow,
    sql_path,
)
from geoparquet_io.core.geometry_detection import (
    GeometryColumnNotFoundError,
    GeometryRead,
    create_geometry_view,
    primary_geometry_column,
    resolve_geometry_read,
)
from tests.test_duckdb_capabilities import pretend_duckdb_version


def _wkb(x: float, y: float) -> bytes:
    return struct.pack("<BI2d", 1, 1, x, y)


def _block(primary) -> dict | list:
    if isinstance(primary, (list, dict)):
        return primary
    block = {"version": "1.1.0", "columns": {}}
    if primary is not _ABSENT:
        block["primary_column"] = primary
        if isinstance(primary, str):
            block["columns"][primary] = {"encoding": "WKB", "geometry_types": []}
    return block


_ABSENT = object()

#: (case id, block primary -- or a legacy list block, or None for no ``geo`` key --,
#:  schema column names (each a WKB binary column unless named ``id``/``value``),
#:  expected primary)
#:
#: Most rows are pins: the case-insensitive, priority-ordered name step and the
#: block-first step existed before #1367 and these rows pass on the old
#: resolvers too. The rows that discriminate are the ones answering ``None``
#: (never the literal ``"geometry"``) and the block naming a column the schema
#: lacks (the block no longer stands on its own when the schema can be read).
RESOLVER_TABLE = [
    ("block-primary-present", "geom2", ["geometry", "geom2"], "geom2"),
    ("no-block-geometry", None, ["id", "geometry"], "geometry"),
    ("no-block-geom", None, ["id", "geom"], "geom"),
    ("no-block-wkb_geometry", None, ["id", "wkb_geometry"], "wkb_geometry"),
    ("no-block-upper-GEOM", None, ["id", "GEOM"], "GEOM"),
    ("no-block-mixed-Geometry", None, ["id", "Geometry"], "Geometry"),
    ("no-block-priority-order", None, ["shape", "geometry"], "geometry"),
    ("no-block-no-geometry", None, ["id", "value"], None),
    ("block-without-primary", _ABSENT, ["id", "geom"], "geom"),
    ("block-without-primary-no-geometry", _ABSENT, ["id", "value"], None),
    ("block-names-missing-column", "missing", ["id", "geom"], "geom"),
    ("block-names-missing-no-geometry", "missing", ["id", "value"], None),
    ("block-case-differs", "GEOMETRY", ["id", "geometry"], "geometry"),
    ("block-primary-not-a-string", 123, ["id", "geom"], "geom"),
    ("legacy-list-block", [{"name": "g2", "primary": True}], ["geometry", "g2"], "g2"),
    (
        "declared-column-survives",
        {"version": "1.1.0", "primary_column": "gone", "columns": {"gone": {}, "fp": {}}},
        ["id", "fp", "geometry"],
        "fp",
    ),
]


def _table(names: list[str], geo) -> pa.Table:
    columns = {
        name: (
            pa.array([1], pa.int64())
            if name in ("id", "value")
            else pa.array([_wkb(1.0, 2.0)], pa.binary())
        )
        for name in names
    }
    table = pa.table(columns)
    if geo is not None:
        table = table.replace_schema_metadata({b"geo": json.dumps(_block(geo)).encode()})
    return table


#: The Arrow sources read the block through the sanitized carried decoder,
#: which drops a legacy list block (not an object) before the resolver sees it.
_SANITIZED_AWAY = {"legacy-list-block": "geometry"}


@pytest.mark.parametrize(
    "case, geo, names, expected",
    RESOLVER_TABLE,
    ids=[c[0] for c in RESOLVER_TABLE],
)
@pytest.mark.parametrize("kind", ["path", "str", "table", "schema", "names"])
def test_primary_geometry_column_table(kind, case, geo, names, expected, tmp_path):
    """One rule for every source form: a file, a table, a schema and its names agree."""
    want = expected
    table = _table(names, geo)
    if kind in ("path", "str"):
        path = tmp_path / "f.parquet"
        pq.write_table(table, str(path))
        got = primary_geometry_column(path if kind == "path" else str(path))
    elif kind in ("table", "schema"):
        want = _SANITIZED_AWAY.get(case, want)
        got = primary_geometry_column(table if kind == "table" else table.schema)
    else:
        block = _block(geo) if geo is not None else None
        got = primary_geometry_column(list(names), geo=block)
    assert got == want


def test_a_file_answers_as_its_described_columns(tmp_path):
    """A native column with no ``geo`` key and no standard name: the type step,
    for a path exactly as for its ``describe_query`` (audit finding 11)."""
    path = tmp_path / "native.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            "COPY (SELECT 1 AS id, ST_Point(0, 0) AS location) "
            f"TO {sql_path(str(path))} (FORMAT parquet, GEOPARQUET_VERSION 'NONE')"
        )
        described = describe_query(con, f"SELECT * FROM read_parquet({sql_path(str(path))})")
    finally:
        con.close()
    assert b"geo" not in (pq.ParquetFile(str(path)).metadata.metadata or {})
    assert primary_geometry_column(described) == "location"
    assert primary_geometry_column(str(path)) == "location"


def test_never_the_literal_fallback(tmp_path):
    """A file with no geometry-like column answers None, not ``"geometry"``."""
    path = tmp_path / "plain.parquet"
    pq.write_table(pa.table({"id": [1, 2], "value": [10, 20]}), str(path))
    assert primary_geometry_column(str(path)) is None
    assert primary_geometry_column(pa.table({"id": [1]})) is None
    assert primary_geometry_column([]) is None


def test_native_typed_column_without_a_standard_name():
    """A described schema's first GEOMETRY column answers when no name does."""
    described = [("id", "INTEGER"), ("footprint", "GEOMETRY"), ("other", "GEOMETRY")]
    assert primary_geometry_column(described) == "footprint"
    # A Parquet logical type (a root-column list) types the column too.
    assert primary_geometry_column([("id", None), ("g", "GeographyType(crs=OGC:CRS84)")]) == "g"
    assert primary_geometry_column([("id", "INTEGER"), ("blob", "BLOB")]) is None


def test_the_shared_not_found_error_names_the_source():
    error = GeometryColumnNotFoundError("in.parquet")
    assert "in.parquet" in str(error)
    from geoparquet_io.core.exceptions import GeometryError

    assert isinstance(error, GeometryError)


# ---------------------------------------------------------------------------
# The read expression
# ---------------------------------------------------------------------------

_WKB_EXPR = (
    '(CASE WHEN (octet_length("g") > 0 AND TRY(ST_GeomFromWKB(list_extract(["g"], 1))) '
    'IS NOT NULL) THEN ST_GeomFromWKB(list_extract(["g"], 1)) END)'
)

#: DuckDB type -> (kind, shape_expr). ``shape_expr`` is the whole value as GEOMETRY.
READ_TABLE = [
    ("BLOB", "wkb", _WKB_EXPR),
    ("GEOMETRY", "native", '"g"'),
    ("GEOMETRY('EPSG:3857')", "native", '"g"'),
    ("GEOGRAPHY", "native", '"g"'),
    ("STRUCT(x DOUBLE, y DOUBLE)", "geoarrow", '"g"'),
    ("STRUCT(x DOUBLE, y DOUBLE)[]", "geoarrow", '"g"'),
    ("VARCHAR", "unsupported", '"g"'),
    (None, "missing", '"g"'),
]


@pytest.mark.parametrize("version", ["1.5.6", "2.0.0"])
@pytest.mark.parametrize(
    "duckdb_type, kind, shape", READ_TABLE, ids=[str(r[0]) for r in READ_TABLE]
)
def test_read_expression_table(version, duckdb_type, kind, shape):
    with pretend_duckdb_version(version):
        read = GeometryRead.for_type("g", duckdb_type)
        assert read.kind == kind
        assert read.shape_expr == shape
        assert read.native is (kind == "native")


def test_resolve_geometry_read_over_a_relation():
    """``relation=True`` reads the type off any FROM-able SQL, not only a file."""
    con = get_duckdb_connection(load_spatial=True)
    try:
        register_arrow(con, "t", pa.table({"g": pa.array([_wkb(1.0, 2.0)], pa.binary())}))
        read = resolve_geometry_read(con, "t", "g", relation=True)
        assert read.kind == "wkb"
        assert con.execute(f"SELECT ST_AsText({read.shape_expr}) FROM t").fetchall() == [
            ("POINT (1 2)",)
        ]
        native = resolve_geometry_read(con, "(SELECT ST_Point(3, 4) AS g)", "g", relation=True)
        assert native.kind == "native"
        assert native.shape_expr == '"g"'
    finally:
        con.close()


def test_resolve_geometry_read_of_a_zero_row_file_without_geo_key(tmp_path):
    """#1359's file: DuckDB writes no ``geo`` key with no rows, so the column reads as BLOB."""
    path = tmp_path / "zero.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            "COPY (SELECT 1 AS id, ST_Point(0, 0) AS geometry WHERE false) "
            f"TO {sql_path(str(path))} (FORMAT parquet)"
        )
        read = resolve_geometry_read(con, str(path), "geometry")
        assert read.kind == "wkb"
    finally:
        con.close()


def test_create_geometry_view_decodes_a_blob_and_keeps_native_columns():
    con = get_duckdb_connection(load_spatial=True)
    try:
        table = pa.table({"g": pa.array([_wkb(1.0, 2.0)], pa.binary()), "id": [7]})
        register_arrow(con, "__src", table)
        ref = create_geometry_view(con, "__src", "g", "__view")
        assert ref == "__view"
        # Decoded in place: every column keeps its position.
        assert con.execute("SELECT ST_AsText(g), id FROM __view").fetchall() == [("POINT (1 2)", 7)]
        assert [n for n, _ in describe_query(con, "SELECT * FROM __view")] == ["g", "id"]
        # Native already, or absent: no view, the source itself.
        con.execute("CREATE TABLE n AS SELECT ST_Point(0, 0) AS g")
        assert create_geometry_view(con, "n", "g") == "n"
        assert create_geometry_view(con, "__src", "missing") == "__src"
        # A fresh name when none is given.
        assert create_geometry_view(con, "__src", "g").startswith("__gpio_geometry_")
    finally:
        con.close()


def test_wkb_sql_primitives_live_in_duckdb_utils():
    from geoparquet_io.core.duckdb_utils import as_wkb_sql, geom_from_wkb_sql

    assert as_wkb_sql('"g"') == 'ST_AsWKB("g")'
    assert geom_from_wkb_sql('"g"') == 'ST_GeomFromWKB("g")'


# ---------------------------------------------------------------------------
# Hand-off (T3b): the degenerate-geometry policy holds before any repair
# ---------------------------------------------------------------------------


def _bowtie_with_an_infinite_coordinate(path) -> None:
    inf = float("inf")
    ring = [(0.0, 0.0), (1.0, 1.0), (1.0, 0.0), (inf, 1.0), (0.0, 0.0)]
    wkb = struct.pack("<BII", 1, 3, 1) + struct.pack("<I", len(ring))
    wkb += b"".join(struct.pack("<2d", *p) for p in ring)
    table = pa.table({"id": [1, 2], "geometry": pa.array([wkb, _wkb(1.0, 2.0)], pa.binary())})
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": []}},
    }
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), str(path))


def test_skip_hilbert_refuses_an_infinite_coordinate_before_repair(tmp_path):
    """``--skip-hilbert`` skips the bounds step, so the repair reached ST_MakeValid
    first and GEOS failed (``orientationIndex encountered NaN/Inf``). The bounds
    owner's refusal (``GeometryError`` naming the column) now runs before any repair.
    """
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    src = tmp_path / "inf.parquet"
    _bowtie_with_an_infinite_coordinate(src)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli, ["convert", "geoparquet", str(src), str(out), "--skip-hilbert"]
    )
    assert result.exit_code != 0
    assert "NaN/Inf" not in result.output
    assert "non-finite (infinite) coordinate" in result.output
    assert "Repaired" not in result.output
    assert not out.exists()


def test_repair_query_geometry_refuses_an_infinite_coordinate(tmp_path):
    from geoparquet_io.core.exceptions import GeometryError
    from geoparquet_io.core.geometry_repair import repair_query_geometry

    src = tmp_path / "inf.parquet"
    _bowtie_with_an_infinite_coordinate(src)
    con = get_duckdb_connection(load_spatial=True)
    try:
        query = f"SELECT * FROM read_parquet({sql_path(str(src))})"
        with pytest.raises(GeometryError, match="non-finite"):
            repair_query_geometry(con, query, "geometry", repair=True)
        # Opting out of repair never reaches ST_MakeValid, so there is nothing to refuse.
        assert repair_query_geometry(con, query, "geometry", repair=False) == query
    finally:
        con.close()


# ---------------------------------------------------------------------------
# One names list: a source's extras go through the resolver
# ---------------------------------------------------------------------------


def test_extra_names_come_after_the_standard_names():
    from geoparquet_io.core.geometry_detection import GEOGRAPHY_SOURCE_NAMES

    # Not standard: a `geo` column is as often a country code.
    assert primary_geometry_column(["id", "geo"]) is None
    assert primary_geometry_column(["id", "GEO"], extra_names=GEOGRAPHY_SOURCE_NAMES) == "GEO"
    assert primary_geometry_column(["geography", "geom"], extra_names=GEOGRAPHY_SOURCE_NAMES) == (
        "geom"
    )


def test_bigquery_detection_is_the_resolver_plus_its_extras():
    from geoparquet_io.core.extract_bigquery import _detect_geometry_column

    def table(*names):
        return pa.table({n: [b""] for n in names})

    assert _detect_geometry_column(table("id", "Geography")) == "Geography"
    assert _detect_geometry_column(table("geo", "the_geom")) == "the_geom"
    # The owner's order, not BigQuery's old list (which put the_geom first).
    assert _detect_geometry_column(table("the_geom", "shape")) == "shape"
    # The substring heuristic is gone: a name containing "geo" is not a column.
    assert _detect_geometry_column(table("id", "building_geom_wkb")) is None
    assert _detect_geometry_column(table("id", "name")) is None
    # A typed (GeoArrow) column resolves whatever its name.
    import geoarrow.pyarrow as ga

    typed = pa.table({"id": [1], "building_geom_wkb": ga.as_wkb(["POINT (0 1)"])})
    assert _detect_geometry_column(typed) == "building_geom_wkb"


def _blockless_native_file(path, names) -> None:
    con = get_duckdb_connection(load_spatial=True)
    try:
        select = ", ".join(f"ST_Point({i}, {i}) AS {n}" for i, n in enumerate(names))
        con.execute(
            f"COPY (SELECT {select}) TO {sql_path(str(path))} "
            "(FORMAT parquet, GEOPARQUET_VERSION 'NONE')"
        )
    finally:
        con.close()


@pytest.mark.parametrize(
    "names, requested, expected",
    [
        (["zb", "centroid", "geom"], None, "geom"),  # a standard name, then...
        (["zb", "centroid"], None, "centroid"),  # ...the alphabetically first native column
        (["centroid", "geometry"], None, "geometry"),
        (["geometry", "footprint"], "footprint", "footprint"),  # the caller's column wins
        (["geometry", "footprint"], "gone", "geometry"),  # one the file lacks does not
    ],
)
def test_derive_v2_geo_metadata_picks_its_primary_through_the_resolver(
    names, requested, expected, tmp_path
):
    """A native-geo file with no ``geo`` key: the resolver over its native columns."""
    from geoparquet_io.core.derive_geo_from_file import derive_v2_geo_metadata

    path = tmp_path / "native.parquet"
    _blockless_native_file(path, names)
    assert b"geo" not in (pq.ParquetFile(str(path)).metadata.metadata or {})
    block = derive_v2_geo_metadata(str(path), primary_column=requested)
    assert block["primary_column"] == expected


@pytest.mark.parametrize("sort", ["hilbert", "str"])
@pytest.mark.parametrize("output", ["-", "out.parquet"])
def test_an_explicit_missing_geometry_column_errors_on_every_sort_path(sort, output, tmp_path):
    """Only a *block* primary falls through to the schema (#1367 audit); a
    column the user named is the answer or an error, file and stream alike."""
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    src = str(Path(__file__).parent / "data" / "places_test.parquet")
    out = output if output == "-" else str(tmp_path / output)
    result = CliRunner().invoke(cli, ["sort", sort, src, out, "--geometry-column", "nope"])
    assert result.exit_code != 0, result.output
    assert "nope" in result.output
    assert "Traceback" not in result.output


def test_convert_geojson_reads_the_declared_primary_not_a_standard_named_secondary(tmp_path):
    """#1367 audit S1: the direct GeoJSON path described the file without its
    ``geo`` block, so a ``geometry`` secondary beat the declared ``footprint``."""
    from geoparquet_io.core.geojson_stream import convert_to_geojson_stream

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    table = (
        con.sql(
            "SELECT 1 AS id, ST_AsWKB(ST_Point(1, 2)) AS footprint, "
            "ST_AsWKB(ST_Point(50, 60)) AS geometry"
        )
        .arrow()
        .read_all()
    )
    con.close()
    entry = {"encoding": "WKB", "geometry_types": ["Point"]}
    geo = {
        "version": "1.1.0",
        "primary_column": "footprint",
        "columns": {"footprint": entry, "geometry": entry},
    }
    src = tmp_path / "two.parquet"
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), src)
    out = tmp_path / "out.geojson"
    convert_to_geojson_stream(str(src), str(out))
    feature = json.loads(out.read_text())["features"][0]
    assert feature["geometry"]["coordinates"] == [1.0, 2.0]
    assert "footprint" not in feature["properties"]


def _api_table_paths():
    from geoparquet_io.core.add.bbox import add_bbox_table
    from geoparquet_io.core.add.kdtree import add_kdtree_table
    from geoparquet_io.core.extract import extract_table
    from geoparquet_io.core.hilbert_order import hilbert_order_table
    from geoparquet_io.core.str_order import str_order_table

    return {
        "hilbert": lambda t: hilbert_order_table(t, geometry_column="nope"),
        "str": lambda t: str_order_table(t, geometry_column="nope"),
        "bbox": lambda t: add_bbox_table(t, geometry_column="nope"),
        "kdtree": lambda t: add_kdtree_table(t, geometry_column="nope", iterations=2),
        "extract": lambda t: extract_table(t, geometry_column="nope"),
    }


@pytest.mark.parametrize("path", ["hilbert", "str", "bbox", "kdtree", "extract"])
def test_a_named_missing_column_errors_on_the_table_paths_too(path):
    """#1367 audit S4/S5: ``resolve_geometry_column`` is the one front door; a
    named column the table lacks is refused, never swapped or bound late."""
    from geoparquet_io.core.exceptions import InvalidParameterError

    src = Path(__file__).parent / "data" / "places_test.parquet"
    table = pq.read_table(src)
    with pytest.raises(InvalidParameterError, match="geometry column 'nope' not found"):
        _api_table_paths()[path](table)


def test_resolve_geometry_column_takes_a_file_source(tmp_path):
    """#1367 audit S6: the front door takes every source the resolver takes."""
    from geoparquet_io.core.exceptions import InvalidParameterError
    from geoparquet_io.core.geometry_detection import resolve_geometry_column

    src = str(Path(__file__).parent / "data" / "places_test.parquet")
    assert resolve_geometry_column(src) == "geometry"
    assert resolve_geometry_column(src, "geometry") == "geometry"
    with pytest.raises(InvalidParameterError, match=r"'nope' not found in .*places_test"):
        resolve_geometry_column(src, "nope")
