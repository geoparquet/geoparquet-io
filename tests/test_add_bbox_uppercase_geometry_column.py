"""``--add-bbox`` must declare the covering on the geometry column that exists.

When the geometry column is spelled in a case that is not in
``STANDARD_GEOMETRY_NAMES`` -- ``GEOMETRY`` rather than ``geometry`` -- the bbox
column is computed and written correctly, but the ``covering`` is declared under
a *lowercase* ``geometry`` key that the file does not contain. The real column
is left without a covering.

Measured with 1.5.0, on a 50-row file whose only geometry column is
``GEOMETRY``, after ``gpio sort str in.parquet out.parquet --add-bbox``:

    schema:  ['id', 'GEOMETRY', 'bbox']
    geo.primary_column: 'GEOMETRY'
    geo.columns['GEOMETRY'].covering  -> absent
    geo.columns['geometry'].covering  -> {'bbox': {'xmin': ['bbox', 'xmin'], ...}}

``geometry`` is a phantom: no such column is in the schema. gpio's own validator
says so about gpio's own output -- ``gpio check spec`` on that file reports five
failures, all of the form ``column "geometry" not found in schema``. So this is
not a question of how strictly a reader should treat a missing covering; the
written file is internally inconsistent, and a downstream validator that reads
``covering`` (the per-row box a row-order check needs) sees none.

Two spellings that *do* get a correct covering, for contrast: ``geometry`` and
``geom``. The trigger is the case of the name, not the word -- so a publisher
whose GeoPackage column is ``GEOMETRY`` (INSPIRE-derived national downloads use
exactly that) cannot get a covering at all, and nothing on the command line
overrides it: passing ``-g GEOMETRY`` explicitly makes no difference, and
GeoParquet 1.1 output behaves the same way.

Where the two spellings diverge: ``geoparquet_io/core/geometry_detection.py``'s
``detect_geometry_column_from_names`` matches case-insensitively through a
lowered index, while ``geoparquet_io/core/add/bbox.py`` (``_add_bbox_query``,
around lines 447-453) tests ``if name in col_names`` against the lowercase
``STANDARD_GEOMETRY_NAMES`` and then falls back to the literal ``"geometry"``.
Which of the two is the right place to fix, and whether the fallback should
raise instead of guessing a name, is left to the maintainers -- this file only
pins the behaviour.
"""

import json

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli


def _write_uppercase_geometry_parquet(path) -> None:
    """A minimal GeoParquet whose sole geometry column is named ``GEOMETRY``."""
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(
            "COPY (SELECT i AS id, ST_Point(i * 0.1, i * 0.1) AS \"GEOMETRY\" "
            "FROM range(50) t(i)) TO '" + str(path) + "' (FORMAT PARQUET)"
        )
    finally:
        con.close()


def _sorted_with_bbox(tmp_path):
    """Run ``sort str --add-bbox`` over the uppercase-column file, return output path."""
    source = tmp_path / "upper.parquet"
    output = tmp_path / "upper.sorted.parquet"
    _write_uppercase_geometry_parquet(source)

    result = CliRunner().invoke(
        cli,
        ["sort", "str", str(source), str(output), "--add-bbox", "--overwrite"],
    )
    assert result.exit_code == 0, result.output
    assert output.exists()
    return output


def _geo_metadata(path) -> dict:
    import pyarrow.parquet as pq

    return json.loads(pq.ParquetFile(str(path)).metadata.metadata[b"geo"])


def test_add_bbox_writes_the_physical_bbox_column(tmp_path):
    """The bbox column itself is computed correctly -- only its declaration is wrong."""
    import pyarrow.parquet as pq

    output = _sorted_with_bbox(tmp_path)
    names = pq.ParquetFile(str(output)).schema_arrow.names
    assert "bbox" in names
    assert "GEOMETRY" in names


@pytest.mark.xfail(
    strict=True,
    reason="gpio gap: --add-bbox declares the covering under a lowercase 'geometry' "
    "key when the geometry column is 'GEOMETRY', leaving the real column without one",
)
def test_covering_is_declared_on_the_real_geometry_column(tmp_path):
    geo = _geo_metadata(_sorted_with_bbox(tmp_path))
    assert geo["primary_column"] == "GEOMETRY"
    covering = geo["columns"]["GEOMETRY"].get("covering")
    assert covering is not None, "the column gpio computed the bbox from carries no covering"
    assert covering["bbox"]["xmin"] == ["bbox", "xmin"]


@pytest.mark.xfail(
    strict=True,
    reason="gpio gap: --add-bbox adds a geo.columns entry for a lowercase 'geometry' "
    "column that is not in the schema",
)
def test_geo_metadata_names_no_column_absent_from_the_schema(tmp_path):
    import pyarrow.parquet as pq

    output = _sorted_with_bbox(tmp_path)
    schema_names = set(pq.ParquetFile(str(output)).schema_arrow.names)
    declared = set(_geo_metadata(output)["columns"])
    assert declared <= schema_names, f"geo declares absent column(s): {declared - schema_names}"
