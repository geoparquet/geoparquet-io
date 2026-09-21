"""``geo.columns`` must not name a column the file does not contain.

When a file's sole geometry column is spelled in a case that is not the one in
``STANDARD_GEOMETRY_NAMES`` -- ``GEOMETRY`` rather than ``geometry`` -- every
gpio write path emits a ``geo.columns`` entry keyed by the *lowercase* literal.
No such column is in the schema, and the real one is left without the metadata
that belongs to it (its ``covering``, when ``--add-bbox`` computed one).

Measured with 1.5.0, on a 50-row file whose only geometry column is
``GEOMETRY``, after ``gpio sort str in.parquet out.parquet --add-bbox``:

    schema:  ['id', 'GEOMETRY', 'bbox']
    geo.primary_column: 'GEOMETRY'
    geo.columns['GEOMETRY'].covering  -> absent
    geo.columns['geometry'].covering  -> {'bbox': {'xmin': ['bbox', 'xmin'], ...}}

``geometry`` is a phantom. gpio's own validator says so about gpio's own output
-- ``gpio check spec`` on that file reports five failures, all of the form
``column "geometry" not found in schema``. So this is not a question of how
strictly a reader should treat a missing covering; the written file is
internally inconsistent, and a downstream validator that reads ``covering``
(the per-row box a row-order check needs) sees none.

**This is not an ``--add-bbox`` bug.** The fault is in the write funnel's
geometry detection, so it reaches every write path. On the same fixture:

    GEOMETRY  / gpio sort hilbert         phantom={'geometry'}  primary='GEOMETRY'
    GEOMETRY  / gpio add h3 -r 7          phantom={'geometry'}  primary='GEOMETRY'
    GEOMETRY  / gpio sort str --add-bbox  phantom={'geometry'}  primary='GEOMETRY'
    footprint / ...                       phantom=set()
    the_geom  / ...                       phantom=set()

``footprint`` and ``the_geom`` come out clean because no standard name matches
at all, so control falls to the type-based branch, which returns the column's
real name. The trigger is the *case* of a standard name, not the word -- so a
publisher whose GeoPackage column is ``GEOMETRY`` (INSPIRE-derived national
downloads use exactly that) can never get a valid ``geo`` block, and nothing on
the command line overrides it: passing ``-g GEOMETRY`` explicitly makes no
difference, and GeoParquet 1.1 output behaves the same way.

Root cause -- ``geoparquet_io/core/geometry_detection.py``,
``_detect_geometry_from_query``: it lowercases the ``DESCRIBE`` column list and
then, on a standard-name match, returns ``std_name`` (the lowercase literal)
rather than the column's own spelling, the way ``detect_geometry_column_from_names``
already does. ``write_funnels.write_parquet_with_metadata`` takes that as
``geometry_column`` and hands it to ``build_geo_metadata``, which writes
``geo["columns"]["geometry"]``.

This file only pins the behaviour; the fix is left to the maintainers.
"""

import json

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import sql_path


def _geo_metadata(path) -> dict:
    return json.loads(pq.ParquetFile(str(path)).metadata.metadata[b"geo"])


def _write_uppercase_geometry_parquet(path) -> None:
    """A minimal GeoParquet whose sole geometry column is named ``GEOMETRY``."""
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(
            'COPY (SELECT i AS id, ST_Point(i * 0.1, i * 0.1) AS "GEOMETRY" '
            f"FROM range(50) t(i)) TO {sql_path(path)} (FORMAT PARQUET)"
        )
    finally:
        con.close()

    # The whole scenario rests on the input declaring the uppercase column as
    # its primary: that is what keeps 'GEOMETRY' the output's primary while the
    # writer separately invents the lowercase key. If a DuckDB/spatial upgrade
    # stops emitting the geo block, the strict xfails below would keep xfailing
    # for an unrelated reason and quietly stop testing anything.
    geo = _geo_metadata(path)
    assert geo["primary_column"] == "GEOMETRY", (
        "fixture premise: the input must declare the uppercase column as primary"
    )


def _run(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output


def _sorted_with_bbox(tmp_path):
    """Run ``sort str --add-bbox`` over the uppercase-column file, return output path."""
    source = tmp_path / "upper.parquet"
    output = tmp_path / "upper.sorted.parquet"
    _write_uppercase_geometry_parquet(source)

    _run("sort", "str", source, output, "--add-bbox", "--overwrite")
    assert output.exists()
    return output


def _hilbert_sorted(tmp_path):
    """Run ``sort hilbert`` (no ``--add-bbox``) over the same file, return output path."""
    source = tmp_path / "upper.parquet"
    output = tmp_path / "upper.hilbert.parquet"
    _write_uppercase_geometry_parquet(source)

    _run("sort", "hilbert", source, output, "--overwrite")
    assert output.exists()
    return output


def test_add_bbox_writes_the_physical_bbox_column(tmp_path):
    """The bbox column itself is computed correctly -- only its declaration is wrong."""
    output = _sorted_with_bbox(tmp_path)
    table = pq.read_table(str(output))
    assert "bbox" in table.column_names
    assert "GEOMETRY" in table.column_names

    # Read the values, not just the names: the other tests are xfail(strict),
    # so a regression that computed the bbox off the wrong column, or wrote
    # nulls, would leave this file green if this positive control only counted
    # columns.
    boxes = table.column("bbox").combine_chunks().to_pylist()
    assert len(boxes) == 50
    ids = table.column("id").combine_chunks().to_pylist()
    by_id = dict(zip(ids, boxes, strict=True))
    for i in (0, 1, 49):
        expected = round(i * 0.1, 10)
        assert by_id[i] == pytest.approx(
            {"xmin": expected, "ymin": expected, "xmax": expected, "ymax": expected}
        )


@pytest.mark.xfail(
    strict=True,
    reason="gpio gap: the covering is declared under a lowercase 'geometry' key when "
    "the geometry column is 'GEOMETRY', leaving the real column without one",
)
def test_covering_is_declared_on_the_real_geometry_column(tmp_path):
    geo = _geo_metadata(_sorted_with_bbox(tmp_path))
    assert geo["primary_column"] == "GEOMETRY"
    covering = geo["columns"]["GEOMETRY"].get("covering")
    assert covering is not None, "the column gpio computed the bbox from carries no covering"
    assert covering["bbox"]["xmin"] == ["bbox", "xmin"]


@pytest.mark.xfail(
    strict=True,
    reason="gpio gap: the write funnel adds a geo.columns entry for a lowercase "
    "'geometry' column that is not in the schema",
)
def test_geo_metadata_names_no_column_absent_from_the_schema(tmp_path):
    output = _sorted_with_bbox(tmp_path)
    schema_names = set(pq.ParquetFile(str(output)).schema_arrow.names)
    declared = set(_geo_metadata(output)["columns"])
    assert declared <= schema_names, f"geo declares absent column(s): {declared - schema_names}"


@pytest.mark.xfail(
    strict=True,
    reason="gpio gap: the phantom lowercase 'geometry' entry is written by the shared "
    "write funnel, so a plain sort with no --add-bbox produces it too",
)
def test_geo_metadata_names_no_absent_column_without_add_bbox(tmp_path):
    """The blast radius: no ``--add-bbox`` anywhere, same invalid ``geo`` block."""
    output = _hilbert_sorted(tmp_path)
    schema_names = set(pq.ParquetFile(str(output)).schema_arrow.names)
    declared = set(_geo_metadata(output)["columns"])
    assert declared <= schema_names, f"geo declares absent column(s): {declared - schema_names}"
