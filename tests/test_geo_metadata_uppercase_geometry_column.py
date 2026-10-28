"""``geo.columns`` must not name a column the file does not contain.

A file whose sole geometry column is spelled in a case that is not the one in
``STANDARD_GEOMETRY_NAMES`` -- ``GEOMETRY`` rather than ``geometry`` -- used to
come out of every gpio write path with a ``geo.columns`` entry keyed by the
*lowercase* literal. No such column was in the schema, and the real one was left
without the metadata that belonged to it (its ``covering``, when ``--add-bbox``
had computed one).

Measured on 1.5.0, on a 50-row file whose only geometry column is ``GEOMETRY``,
after ``gpio sort str in.parquet out.parquet --add-bbox``:

    schema:  ['id', 'GEOMETRY', 'bbox']
    geo.primary_column: 'GEOMETRY'
    geo.columns['GEOMETRY'].covering  -> absent
    geo.columns['geometry'].covering  -> {'bbox': {'xmin': ['bbox', 'xmin'], ...}}

``geometry`` was a phantom, and gpio's own validator said so about gpio's own
output -- ``gpio check spec`` on that file reported five failures, all of the
form ``column "geometry" not found in schema``. So this was not a question of
how strictly a reader should treat a missing covering; the written file was
internally inconsistent, and a downstream validator reading ``covering`` (the
per-row box a row-order check needs) saw none.

**It was never an ``--add-bbox`` bug.** The fault sat in the write funnel's
geometry detection, so it reached every write path. On this fixture, before the
fix:

    GEOMETRY  / gpio sort hilbert         phantom={'geometry'}  primary='GEOMETRY'
    GEOMETRY  / gpio add h3 -r 7          phantom={'geometry'}  primary='GEOMETRY'
    GEOMETRY  / gpio sort str --add-bbox  phantom={'geometry'}  primary='GEOMETRY'
    footprint / ...                       phantom=set()
    the_geom  / ...                       phantom=set()

``footprint`` and ``the_geom`` came out clean because no standard name matches
them at all, so control fell to the type-based branch, which returns the
column's real name. The trigger was the *case* of a standard name, not the word
-- so a publisher whose GeoPackage column is ``GEOMETRY`` (INSPIRE-derived
national downloads use exactly that) could never get a valid ``geo`` block, and
nothing on the command line overrode it: ``-g GEOMETRY`` made no difference, and
GeoParquet 1.1 output behaved the same way.

Cause -- ``geoparquet_io/core/geometry_detection.py``,
``_detect_geometry_from_query``: it lowercased the ``DESCRIBE`` column list and
then, on a standard-name match, returned ``std_name`` (the lowercase literal)
rather than the column's own spelling, unlike its own ``primary_column`` branch
directly above it and unlike ``detect_geometry_column_from_names``.
``write_funnels.write_parquet_with_metadata`` took that as ``geometry_column``
and handed it to ``build_geo_metadata``, which wrote ``geo["columns"]["geometry"]``.

Found while mirroring Czech national open geodata (ČÚZK), where 19 layers could
not be published because the covering never appeared.
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
    # its primary -- that is what the writer has to carry through. If a
    # DuckDB/spatial upgrade stops emitting the geo block, the tests below would
    # go on passing for an unrelated reason and quietly stop testing anything,
    # so the fixture states its own premise rather than assuming it.
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
    """The bbox column itself: the values, not just the column names."""
    output = _sorted_with_bbox(tmp_path)
    table = pq.read_table(str(output))
    assert "bbox" in table.column_names
    assert "GEOMETRY" in table.column_names

    # Keyed by id rather than by row position: `sort str` is what produces
    # this file, so positional indices would pin its output order as a side
    # effect. Reading the values is the point -- a regression that computed the
    # bbox off the wrong column, or wrote nulls, passes a names-only check.
    boxes = table.column("bbox").combine_chunks().to_pylist()
    assert len(boxes) == 50
    ids = table.column("id").combine_chunks().to_pylist()
    by_id = dict(zip(ids, boxes, strict=True))
    for i in (0, 1, 49):
        expected = round(i * 0.1, 10)
        assert by_id[i] == pytest.approx(
            {"xmin": expected, "ymin": expected, "xmax": expected, "ymax": expected}
        )


def test_covering_is_declared_on_the_real_geometry_column(tmp_path):
    """The covering belongs to the column the bbox was computed from."""
    geo = _geo_metadata(_sorted_with_bbox(tmp_path))
    assert geo["primary_column"] == "GEOMETRY"
    covering = geo["columns"]["GEOMETRY"].get("covering")
    assert covering is not None, "the column gpio computed the bbox from carries no covering"
    assert covering["bbox"]["xmin"] == ["bbox", "xmin"]


def test_geo_metadata_names_no_column_absent_from_the_schema(tmp_path):
    """The spec point: every `geo.columns` key is a column the file has."""
    output = _sorted_with_bbox(tmp_path)
    schema_names = set(pq.ParquetFile(str(output)).schema_arrow.names)
    declared = set(_geo_metadata(output)["columns"])
    assert declared <= schema_names, f"geo declares absent column(s): {declared - schema_names}"


def test_geo_metadata_names_no_absent_column_without_add_bbox(tmp_path):
    """The blast radius: the same must hold with no ``--add-bbox`` anywhere."""
    output = _hilbert_sorted(tmp_path)
    schema_names = set(pq.ParquetFile(str(output)).schema_arrow.names)
    declared = set(_geo_metadata(output)["columns"])
    assert declared <= schema_names, f"geo declares absent column(s): {declared - schema_names}"
