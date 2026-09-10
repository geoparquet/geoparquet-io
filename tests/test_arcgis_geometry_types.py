"""``gpio extract arcgis`` must declare geometry_types the spec allows (#928).

The geo block used to name the type from a five-entry table of Esri geometry
types, falling back to the literal ``"Geometry"`` for anything else. That is
not a value the GeoParquet schema permits::

    ^(GeometryCollection|(Multi)?(Point|LineString|Polygon))( Z| M| ZM)?$

so every layer whose ``geometryType`` was outside the table — Esri's list is
longer than five — wrote a file that failed validation on its own metadata.

The types are now computed from the fetched WKB with the same helper the write
paths use, so they carry the spec's " Z"/" M"/" ZM" suffixes and match the data
rather than the layer's advertisement.

That fix kept the table as a fallback for whenever the compute came back empty,
which put the guess straight back on three reachable paths (#962): one malformed
row makes the all-or-nothing compute return nothing for the whole column, an
all-NULL geometry column has nothing to describe, and ``--exclude-cols geometry``
leaves no geometry column at all. The fallback is gone: when the data does not
say, the file says ``[]``.
"""

import json
import logging
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely import wkb as shapely_wkb
from shapely.geometry import LineString, Point, Polygon

from geoparquet_io.core.arcgis import ArcGISLayerInfo, arcgis_to_table

#: The GeoParquet JSON schema's pattern for one geometry_types entry.
GEOMETRY_TYPE_PATTERN = re.compile(
    r"^(GeometryCollection|(Multi)?(Point|LineString|Polygon))( Z| M| ZM)?$"
)


def _layer(geometry_type: str) -> ArcGISLayerInfo:
    return ArcGISLayerInfo(
        name="Test",
        geometry_type=geometry_type,
        spatial_reference={"wkid": 4326},
        fields=[{"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False}],
        max_record_count=1000,
        total_count=1,
    )


def _stub_stream(tmp_path, geometries):
    """Stand in for ``_stream_features_to_parquet``, yielding ``geometries`` as WKB."""
    import shutil

    temp_parquet = str(tmp_path / "streamed.parquet")
    pq.write_table(
        pa.table(
            {
                "geometry": pa.array(geometries, type=pa.binary()),
                "OBJECTID": list(range(1, len(geometries) + 1)),
            }
        ),
        temp_parquet,
    )

    def side_effect(*args, **kwargs):
        output_path = kwargs.get("output_path") or args[2]
        shutil.copy(temp_parquet, output_path)
        return len(geometries), None

    return side_effect


@pytest.fixture
def run_extract(tmp_path, monkeypatch):
    """Run ``arcgis_to_table`` over stubbed network calls, returning its geo block."""

    def _run(geometry_type, geometries, **kwargs):
        from geoparquet_io.core import arcgis

        monkeypatch.setattr(arcgis, "get_layer_info", lambda *a, **k: _layer(geometry_type))
        monkeypatch.setattr(
            arcgis, "_stream_features_to_parquet", _stub_stream(tmp_path, geometries)
        )
        table = arcgis_to_table("https://example.com/FeatureServer/0", **kwargs)
        _run.table = table
        return json.loads(table.schema.metadata[b"geo"])["columns"]["geometry"]

    return _run


def test_unknown_esri_geometry_type_is_computed_not_invented(run_extract):
    """An Esri type outside the mapping table must not write ``"Geometry"``."""
    # esriGeometryMultiPatch is a real Esri type the mapping table never covered.
    column = run_extract("esriGeometryMultiPatch", [shapely_wkb.dumps(Point(1, 2))])

    assert column["geometry_types"] == ["Point"]
    for name in column["geometry_types"]:
        assert GEOMETRY_TYPE_PATTERN.match(name), f"{name!r} is not a valid geometry_types entry"


@pytest.mark.parametrize("esri_type", ["esriGeometryMultiPatch", "esriGeometryPolygon"])
def test_unreadable_wkb_declares_nothing_whatever_the_layer_says(run_extract, esri_type):
    """Unreadable WKB declares nothing — including for a type the table covered.

    The mapped half is the regression: ``esriGeometryPolygon`` used to reach the
    five-entry table here and write ``["MultiPolygon"]`` over bytes nobody could
    read, which is the invented declaration #928 exists to stop.
    """
    column = run_extract(esri_type, [b"not wkb at all"])

    # An empty array is the spec's way to say the types are not known.
    assert column["geometry_types"] == []


def test_computed_types_beat_the_declared_mapping(run_extract):
    """Single Polygons stay ``Polygon`` even though the layer says polygon->Multi."""
    polygon = shapely_wkb.dumps(Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]))
    column = run_extract("esriGeometryPolygon", [polygon])

    assert column["geometry_types"] == ["Polygon"]


def test_computed_types_carry_dimension_suffixes(run_extract):
    """Z data is spelled ``"LineString Z"``, which the old table could never say."""
    line_z = shapely_wkb.dumps(LineString([(0, 0, 1), (1, 1, 2)]), output_dimension=3)
    column = run_extract("esriGeometryPolyline", [line_z])

    assert column["geometry_types"] == ["LineString Z"]


def test_mixed_geometries_are_all_declared(run_extract):
    """Every type present in the data is declared, not just the advertised one."""
    geometries = [
        shapely_wkb.dumps(Point(0, 0)),
        shapely_wkb.dumps(Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])),
    ]
    column = run_extract("esriGeometryMultiPatch", geometries)

    assert sorted(column["geometry_types"]) == ["Point", "Polygon"]


#: A self-intersecting "bowtie". ``ST_MakeValid`` splits it into two triangles,
#: so it comes back a MultiPolygon -- the cheapest geometry whose TYPE changes
#: under repair, which is what makes it the right probe for the ordering below.
BOWTIE = Polygon([(0, 0), (2, 2), (2, 0), (0, 2), (0, 0)])


def _actual_types(table):
    """The geometry types really present in a table's WKB, read back independently."""
    return sorted({shapely_wkb.loads(bytes(v.as_py())).geom_type for v in table.column("geometry")})


def test_repair_runs_before_the_geo_block_is_built(run_extract):
    """The declaration must describe the REPAIRED data that is actually written.

    ``ST_MakeValid`` can change a geometry's type, so building the geo block
    before the repair declares the input's types over the output's bytes. That
    is not a cosmetic ordering: it writes a file whose own metadata under-declares
    it, which ``gpio check spec`` fails and which makes a conformant reader skip
    rows. Moving the repair back below the block flips this to ``["Polygon"]``
    over MultiPolygon data, so this test is what pins the order.
    """
    column = run_extract("esriGeometryPolygon", [shapely_wkb.dumps(BOWTIE)])

    assert column["geometry_types"] == ["MultiPolygon"]
    # ...and that is genuinely what landed in the column, not just what was claimed.
    assert _actual_types(run_extract.table) == ["MultiPolygon"]


def test_declaration_follows_the_data_when_repair_is_off(run_extract):
    """The mirror of the above: no repair, no type change, so ``Polygon`` is right.

    Together the two pin the declaration to the data rather than to a fixed
    answer -- a hardcoded ``["MultiPolygon"]`` would pass the test above and
    fail this one.
    """
    column = run_extract("esriGeometryPolygon", [shapely_wkb.dumps(BOWTIE)], repair_geometry=False)

    assert column["geometry_types"] == ["Polygon"]
    assert _actual_types(run_extract.table) == ["Polygon"]


# ---------------------------------------------------------------------------
# #962: the compute coming back empty is not a licence to guess
# ---------------------------------------------------------------------------

VALID_POLYGON = shapely_wkb.dumps(Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]))


def test_one_malformed_row_does_not_resurrect_the_declared_guess(run_extract):
    """A single unreadable row must not turn the whole column into a guess.

    ``unique_geometry_types`` is all-or-nothing: it raises on the bad row rather
    than reporting the types it managed to read, so the compute returns nothing
    for a column that is almost entirely fine. Testing that emptiness for
    truthiness sent an ``esriGeometryPolygon`` layer back to the table, which
    declared ``["MultiPolygon"]`` over data whose only readable geometry is a
    single ``Polygon`` — a type that is not in the file at all.
    """
    column = run_extract("esriGeometryPolygon", [VALID_POLYGON, b"junk"])

    assert column["geometry_types"] == []
    # The guess is not merely different from the data, it names a type the file
    # does not contain -- which is why [] is the honest answer and not a cop-out.
    readable = []
    for value in run_extract.table.column("geometry"):
        try:
            readable.append(shapely_wkb.loads(bytes(value.as_py())).geom_type)
        except Exception:  # the malformed row, which is the whole point
            pass
    assert readable == ["Polygon"]


def test_all_null_geometry_column_declares_nothing(run_extract):
    """Zero geometries means zero types, not the layer's advertised one.

    FeatureServer records can carry ``"geometry": null``, and a ``--where`` can
    select only those. NULLs are filtered by the compute by design, so this is
    the "nothing to describe" case landing in the "compute failed" branch.
    """
    column = run_extract("esriGeometryPolygon", [None, None])

    assert column["geometry_types"] == []


def test_excluding_the_geometry_column_declares_nothing(run_extract):
    """``--exclude-cols geometry`` leaves nothing to read, so nothing is declared.

    The repair helper passes a table with no geometry column straight through,
    so this reaches the geo block, where the fallback used to describe a column
    that is not in the file.

    Only the declaration is asserted here. The block still names ``geometry`` as
    the primary_column when that column was excluded, which ``gpio check spec``
    fails -- a separate bug from this one, left for its own change.
    """
    column = run_extract("esriGeometryPolygon", [VALID_POLYGON], exclude_cols="geometry")

    assert "geometry" not in run_extract.table.column_names
    assert column["geometry_types"] == []


def test_could_not_read_and_nothing_to_read_are_told_apart(run_extract, caplog):
    """Both declare ``[]``, but the user is told which of the two happened.

    ``_compute_geometry_types`` swallows the geoarrow error and returns ``[]``,
    so the two outcomes are indistinguishable at the call site unless the
    non-NULL geometries are counted first. They are worth telling apart: one is
    a data-quality problem worth chasing, the other is just an empty column.
    """
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        assert run_extract("esriGeometryPolygon", [VALID_POLYGON, b"junk"])
        unreadable = caplog.text
        caplog.clear()
        assert run_extract("esriGeometryPolygon", [None, None])
        empty = caplog.text

    assert "could not be read" in unreadable
    # The layer's declared type survives as context for the warning, not as a
    # value written to the file.
    assert "esriGeometryPolygon" in unreadable
    assert "could not be read" not in empty
    assert "no geometries" in empty


def test_written_file_validates_for_an_unknown_esri_type(tmp_path, monkeypatch):
    """End to end: the file an unknown Esri type produces passes ``gpio check spec``."""
    from geoparquet_io.core import arcgis
    from geoparquet_io.core.validate import CheckStatus, validate_geoparquet

    monkeypatch.setattr(arcgis, "get_layer_info", lambda *a, **k: _layer("esriGeometryMultiPatch"))
    monkeypatch.setattr(
        arcgis,
        "_stream_features_to_parquet",
        _stub_stream(tmp_path, [shapely_wkb.dumps(Point(1, 2))]),
    )

    output = tmp_path / "out.parquet"
    arcgis.convert_arcgis_to_geoparquet(
        "https://example.com/FeatureServer/0",
        str(output),
        skip_hilbert=True,
        skip_bbox=True,
    )

    result = validate_geoparquet(str(output))
    failures = [c for c in result.checks if c.status == CheckStatus.FAILED]
    assert failures == [], [c.message for c in failures]
