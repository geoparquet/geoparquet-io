"""Readers and fixture builders for native-geo-only GeoParquet files.

A GeoParquet 2.0 file states its CRS in **two** places -- the ``geo`` block's
``columns.<name>.crs`` and the Parquet ``GEOMETRY``/``GEOGRAPHY`` logical type --
and the interesting failure is the two disagreeing. ``crs_utils.source_crs_string``
reads the first, falls back to the second and returns whichever answered, so it
reports *an* answer for a file that holds two: structurally incapable of seeing
the defect. #993 shipped green under exactly that oracle.

Everything here therefore comes in pairs. :func:`geo_block_crs_id` reads the
``geo`` block and only the ``geo`` block; :func:`logical_crs_id` reads the
Parquet schema and only the Parquet schema; :func:`spec_problems` asks gpio's
own validator whether what they say adds up.

Refs: https://github.com/geoparquet/geoparquet-io/issues/993
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

#: Authority identifiers the fixtures below declare, as PROJJSON ``id`` objects.
EPSG_5070 = {"authority": "EPSG", "code": 5070}
EPSG_3857 = {"authority": "EPSG", "code": 3857}


# ---------------------------------------------------------------------------
# Reader 1: the `geo` block, and only the `geo` block
# ---------------------------------------------------------------------------


def geo_block(path) -> dict | None:
    """The file-level ``geo`` key, or None when there is none."""
    metadata = pq.ParquetFile(str(path)).metadata.metadata or {}
    if b"geo" not in metadata:
        return None
    return json.loads(metadata[b"geo"].decode("utf-8"))


def geo_version(path) -> str | None:
    return (geo_block(path) or {}).get("version")


def geo_block_crs_id(path, column: str = "geometry"):
    """One column's CRS as the ``geo`` block states it, and nothing else.

    Returns the PROJJSON ``id`` object, or a string naming why there is none --
    a marker rather than ``None``, so a missing ``crs`` key cannot be mistaken
    for a deliberate ``crs: null`` and neither can slip past an ``is not None``.
    """
    block = geo_block(path)
    if block is None:
        return "<no geo key>"
    col_meta = (block.get("columns") or {}).get(column)
    if col_meta is None:
        return "<column not described>"
    if "crs" not in col_meta:
        # GeoParquet's default: an absent `crs` means OGC:CRS84.
        return "<no crs key -- resolves as OGC:CRS84>"
    if col_meta["crs"] is None:
        return "<crs: null -- unknown>"
    return (col_meta["crs"] or {}).get("id")


# ---------------------------------------------------------------------------
# Reader 2: the Parquet schema, and only the Parquet schema
# ---------------------------------------------------------------------------


def logical_geo_types(path) -> dict[str, tuple[str, dict | None]]:
    """``{column: (GEOMETRY|GEOGRAPHY, crs id)}`` read straight off the schema.

    Never through the ``geo`` block: the input class this exists for has no
    ``geo`` block, and its CRS lives only here.
    """
    schema = pq.ParquetFile(str(path)).schema
    native: dict[str, tuple[str, dict | None]] = {}
    for index in range(len(schema)):
        column = schema.column(index)
        described = json.loads(column.logical_type.to_json())
        if described.get("Type") not in ("Geometry", "Geography"):
            continue
        raw_crs = described.get("crs")
        crs = json.loads(raw_crs) if raw_crs else None
        native[column.name] = (described["Type"], (crs or {}).get("id"))
    return native


def logical_crs_id(path, column: str = "geometry"):
    """The CRS inside one column's Parquet logical type, or a reason there is none."""
    described = logical_geo_types(path).get(column)
    if described is None:
        return "<no native geo type>"
    return described[1] if described[1] is not None else "<no crs -- resolves as OGC:CRS84>"


# ---------------------------------------------------------------------------
# The arbiter: gpio's own validator
# ---------------------------------------------------------------------------


def spec_report(path) -> dict:
    """``gpio check spec --json`` on one file."""
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    result = CliRunner().invoke(cli, ["check", "spec", "--json", str(path)])
    # `check spec` exits non-zero when the file fails, which is the thing under
    # test, so a failing report is not an error here. A crash before the report
    # is, and it has to say so: slicing from the first `{` raised `ValueError:
    # substring not found` when there was no report at all, and sliced from the
    # wrong place when a log line printed ahead of it happened to contain one.
    # So try each `{` in turn and take the first that decodes.
    decoder = json.JSONDecoder()
    for start, char in enumerate(result.output):
        if char != "{":
            continue
        try:
            return decoder.raw_decode(result.output, start)[0]
        except json.JSONDecodeError:
            continue
    raise AssertionError(
        f"`check spec --json` emitted no JSON report for {path} "
        f"(exit {result.exit_code}): {result.output or result.exception!r}"
    )


def spec_problems(path, status: str = "failed") -> list[str]:
    """The messages of every ``check spec`` check with ``status``."""
    return [
        f"{check['name']}: {check['message']}"
        for check in spec_report(path)["checks"]
        if check["status"] == status
    ]


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


#: 200 small polygons on a grid across the lower 48, as lon/lat degrees.
#:
#: Spread matters twice over. ``tests/data/fields_pgo_5070_snappy.parquet`` is
#: labelled EPSG:5070 but its coordinates are over Europe, so ``check spec``
#: scores it ``✗ coordinates outside valid range for CRS`` whatever the command
#: under test did. "Clean" has to mean *zero* failures rather than "the same one
#: failure as before", or the assertion stops being able to see a new failure
#: appear.
_CONUS_GRID = """
    SELECT i, -122.0 + (i % 20) * 2.6 AS lon, 26.0 + (i // 20) * 2.1 AS lat
    FROM range(200) t(i)
"""


def conus_wkb(*projections: str) -> list[tuple]:
    """``id``, ``grp`` and one WKB column per projection, over the same 200 cells.

    ``projections`` are SQL expressions over a ``cell`` GEOMETRY in lon/lat.
    ``grp`` is the low-cardinality column ``gpio partition string`` needs.
    """
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection(load_spatial=True)
    try:
        selected = ", ".join(f"ST_AsWKB({expr})::BLOB" for expr in projections)
        return con.execute(
            f"""
            SELECT i AS id, 'g' || (i % 3) AS grp, {selected}
            FROM (
                SELECT i, ST_MakeEnvelope(lon, lat, lon + 0.4, lat + 0.3) AS cell
                FROM ({_CONUS_GRID})
            )
            ORDER BY i
            """
        ).fetchall()
    finally:
        con.close()


def write_native_geo_only(path, rows, columns, compression="zstd", **write_options) -> Path:
    """Write a Parquet file with native geo logical types and **no** ``geo`` key.

    Written through pyarrow rather than DuckDB because that is the only way to
    get this shape on purpose: a Parquet ``GEOMETRY``/``GEOGRAPHY`` logical type
    carrying the CRS, with nothing in the file-level metadata repeating it. It is
    exactly the shape the write facade had no witness for -- the version question
    and the CRS question both have to be answered from the schema, because there
    is no ``geo`` block to answer either.

    ``columns`` maps a column name to ``(row index, geoarrow type)``.

    ``compression`` and ``write_options`` (``row_group_size``, ...) go straight
    to ``pq.write_table``. They are what a ``gpio check --fix`` test needs: a fix
    only rewrites a file it finds something wrong with, so an input written
    SNAPPY or in 5-row groups is how those tests get a rewrite to measure at all
    (#1001).
    """
    arrays = {
        "id": pa.array([row[0] for row in rows], type=pa.int64()),
        "grp": pa.array([row[1] for row in rows]),
    }
    for name, (index, geo_type) in columns.items():
        arrays[name] = pa.ExtensionArray.from_storage(
            geo_type, pa.array([bytes(row[index]) for row in rows], type=pa.binary())
        )
    pq.write_table(pa.table(arrays), str(path), compression=compression, **write_options)
    return path


def projjson(epsg: int) -> str:
    """Full PROJJSON for an EPSG code.

    Not a hand-written ``{"id": ...}`` stub: GeoParquet requires a ``type``
    member, and a stub makes ``check spec`` report the *fixture* rather than the
    command under test.
    """
    from pyproj import CRS

    return json.dumps(CRS.from_epsg(epsg).to_json_dict())
