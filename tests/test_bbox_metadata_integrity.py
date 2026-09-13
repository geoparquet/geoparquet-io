"""`gpio add bbox-metadata` must stay metadata-only and refuse plain Parquet.

Two defects found during the #686 review, fixed together because both live in
`core/add/bbox_metadata.py` and both end in "reports success, writes a file its
own validator rejects":

- #712: on a 1.x WKB input the rewrite silently re-materialized the geometry
  column as a native Parquet GEOMETRY logical type while leaving the declared
  version at 1.1.0.
- #713: on plain Parquet with a bbox column it invented a 1.1.0 `geo` block
  containing only `covering` -- no `encoding`, no `geometry_types`.

Review round 2 widened both: #712 was fixed for the primary geometry column only,
so a file with a second declared geometry column still went in valid and came out
invalid; and the Python API still carried the skeleton the core had dropped.
"""

import json
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata, add_bbox_metadata_table
from geoparquet_io.core.duckdb_utils import _wrap_query_with_blob_conversion, get_duckdb_connection
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.validate import validate_geoparquet

POINTS = "(1, ST_GeomFromText('POINT (30 10)')), (2, ST_GeomFromText('POINT (40 40)'))"


def _write(path, geo=None, native=False):
    """Write a two-point file with a bbox struct column.

    ``native`` writes a native Parquet GEOMETRY column (2.0); otherwise the
    geometry is a plain BLOB carrying WKB, as a 1.x file must have.
    """
    kv = f", KV_METADATA {{geo: '{geo}'}}" if geo else ""
    geometry_expr = "geom" if native else "ST_AsWKB(geom)::BLOB"
    version = "'V2'" if native else "'NONE'"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"""
            COPY (
              SELECT
                id,
                {geometry_expr} AS geometry,
                {{
                  'xmin': ST_XMin(geom), 'ymin': ST_YMin(geom),
                  'xmax': ST_XMax(geom), 'ymax': ST_YMax(geom)
                }} AS bbox
              FROM (VALUES {POINTS}) t(id, geom)
            ) TO '{path.as_posix()}'
            (FORMAT PARQUET, GEOPARQUET_VERSION {version}{kv})
        """)
    finally:
        con.close()
    return path


def _geo_key(path):
    metadata = pq.read_metadata(str(path)).metadata
    return json.loads(metadata[b"geo"].decode("utf-8")) if metadata and b"geo" in metadata else None


def _geometry_logical_type(path):
    """The Parquet *logical* type of the geometry column: None for plain WKB."""
    con = get_duckdb_connection(load_spatial=False)
    try:
        row = con.execute(
            f"SELECT logical_type FROM parquet_schema('{path}') WHERE name = 'geometry'"
        ).fetchone()
    finally:
        con.close()
    return str(row[0]) if row and row[0] else None


@pytest.fixture
def v11_wkb_with_bbox(tmp_path):
    """A valid 1.1 file: WKB geometry, bbox column, no covering."""
    geo = json.dumps(
        {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "bbox": [30.0, 10.0, 40.0, 40.0],
                }
            },
        }
    )
    path = _write(tmp_path / "v11.parquet", geo=geo)
    assert validate_geoparquet(str(path)).is_valid
    assert _geometry_logical_type(path) is None, "fixture must start as plain WKB"
    return path


@pytest.fixture
def v20_native_with_bbox(tmp_path):
    """A valid 2.0 file: native GEOMETRY column, bbox column, no covering."""
    geo = json.dumps(
        {
            "version": "2.0.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
        }
    )
    path = _write(tmp_path / "v20.parquet", geo=geo, native=True)
    assert _geometry_logical_type(path) is not None, "fixture must start native"
    return path


def _write_two_geometry_columns(path, geo):
    """A 1.x file with two declared geometry columns, both plain WKB blobs."""
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"""
            COPY (
              SELECT
                id,
                ST_AsWKB(geom)::BLOB AS geometry,
                ST_AsWKB(ST_Centroid(geom))::BLOB AS centroid,
                {{
                  'xmin': ST_XMin(geom), 'ymin': ST_YMin(geom),
                  'xmax': ST_XMax(geom), 'ymax': ST_YMax(geom)
                }} AS bbox
              FROM (VALUES {POINTS}) t(id, geom)
            ) TO '{path.as_posix()}'
            (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', KV_METADATA {{geo: '{geo}'}})
        """)
    finally:
        con.close()
    return path


def _logical_type(path, column):
    """The Parquet *logical* type of one column: None for plain WKB."""
    con = get_duckdb_connection(load_spatial=False)
    try:
        row = con.execute(
            f"SELECT logical_type FROM parquet_schema('{path}') WHERE name = '{column}'"
        ).fetchone()
    finally:
        con.close()
    return str(row[0]) if row and row[0] else None


@pytest.fixture
def v11_two_geometry_columns(tmp_path):
    """A valid 1.1 file declaring *two* WKB geometry columns plus a bbox column."""
    column_meta = {
        "encoding": "WKB",
        "geometry_types": ["Point"],
        "bbox": [30.0, 10.0, 40.0, 40.0],
    }
    geo = json.dumps(
        {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": dict(column_meta), "centroid": dict(column_meta)},
        }
    )
    path = _write_two_geometry_columns(tmp_path / "v11_two.parquet", geo)
    assert validate_geoparquet(str(path)).is_valid
    assert _logical_type(path, "geometry") is None
    assert _logical_type(path, "centroid") is None, "fixture must start as plain WKB"
    return path


@pytest.fixture
def plain_parquet_with_bbox(tmp_path):
    """Ordinary Parquet: no geo metadata at all, but it does have a bbox column."""
    path = _write(tmp_path / "plain.parquet")
    assert pq.read_metadata(str(path)).metadata is None
    return path


class TestKeepsTheGeometryColumnPhysicalType:
    """#712: a metadata-only command must not re-type the geometry column."""

    def test_v11_wkb_stays_wkb(self, v11_wkb_with_bbox):
        add_bbox_metadata(str(v11_wkb_with_bbox))

        assert _geometry_logical_type(v11_wkb_with_bbox) is None, (
            "1.x WKB geometry was rewritten as a native Parquet GEOMETRY type"
        )

    def test_v11_output_is_valid_geoparquet(self, v11_wkb_with_bbox):
        """The whole point: the file its own validator accepted still passes."""
        add_bbox_metadata(str(v11_wkb_with_bbox))

        result = validate_geoparquet(str(v11_wkb_with_bbox))
        failed = [c.message for c in result.checks if c.status.value == "failed"]
        assert result.is_valid, f"add bbox-metadata produced an invalid file: {failed}"

    def test_v11_version_and_covering_are_both_right(self, v11_wkb_with_bbox):
        add_bbox_metadata(str(v11_wkb_with_bbox))

        geo = _geo_key(v11_wkb_with_bbox)
        assert geo["version"] == "1.1.0"
        assert geo["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]

    def test_v11_data_survives_the_rewrite(self, v11_wkb_with_bbox):
        """The BLOB cast must not drop or mangle rows."""
        add_bbox_metadata(str(v11_wkb_with_bbox))

        table = pq.read_table(str(v11_wkb_with_bbox))
        assert table.num_rows == 2
        assert table.column("id").to_pylist() == [1, 2]

    def test_native_input_stays_native(self, v20_native_with_bbox):
        """A 2.0 input keeps its native type — that case was never broken."""
        add_bbox_metadata(str(v20_native_with_bbox))

        assert _geometry_logical_type(v20_native_with_bbox) is not None
        assert validate_geoparquet(str(v20_native_with_bbox)).is_valid
        assert _geo_key(v20_native_with_bbox)["version"] == "2.0.0"


class TestKeepsEveryDeclaredGeometryColumn:
    """#712, round 2: the fix must cover every geometry column, not just one.

    ``_wrap_query_with_blob_conversion`` casts the column it is handed, so a file
    carrying ``geometry`` *and* ``centroid`` -- both declared WKB in
    ``geo.columns`` -- still went in valid and came out declaring 1.1.0 while
    ``centroid`` carried a native Parquet GEOMETRY logical type.
    """

    def test_both_geometry_columns_stay_wkb(self, v11_two_geometry_columns):
        add_bbox_metadata(str(v11_two_geometry_columns))

        assert _logical_type(v11_two_geometry_columns, "geometry") is None
        assert _logical_type(v11_two_geometry_columns, "centroid") is None, (
            "a secondary declared geometry column was rewritten as native GEOMETRY"
        )

    def test_output_is_valid_geoparquet(self, v11_two_geometry_columns):
        add_bbox_metadata(str(v11_two_geometry_columns))

        result = validate_geoparquet(str(v11_two_geometry_columns))
        failed = [c.message for c in result.checks if c.status.value == "failed"]
        assert result.is_valid, f"add bbox-metadata produced an invalid file: {failed}"

    def test_secondary_geometry_bytes_round_trip(self, v11_two_geometry_columns):
        """The extra cast must not mangle the second column's WKB."""
        before = pq.read_table(str(v11_two_geometry_columns)).column("centroid").to_pylist()

        add_bbox_metadata(str(v11_two_geometry_columns))

        after = pq.read_table(str(v11_two_geometry_columns)).column("centroid").to_pylist()
        assert after == before

    def test_a_non_geometry_typed_column_is_left_alone(self, tmp_path):
        """`geo.columns` names columns by declaration, not by DuckDB type.

        GeoParquet 1.1 also permits GeoArrow encodings, which DuckDB reads as a
        STRUCT rather than as GEOMETRY. Casting one of those would put
        ST_AsWKB(STRUCT) into the query and abort the whole write, so only
        columns DuckDB actually typed GEOMETRY may be cast.
        """
        geo = json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "geometry_types": ["Point"],
                        "bbox": [30.0, 10.0, 40.0, 40.0],
                    },
                    "pt": {"encoding": "point", "geometry_types": ["Point"]},
                },
            }
        )
        path = tmp_path / "geoarrow_point.parquet"
        con = get_duckdb_connection(load_spatial=True)
        try:
            con.execute(f"""
                COPY (
                  SELECT
                    id,
                    ST_AsWKB(geom)::BLOB AS geometry,
                    {{'x': ST_X(geom), 'y': ST_Y(geom)}} AS pt,
                    {{
                      'xmin': ST_XMin(geom), 'ymin': ST_YMin(geom),
                      'xmax': ST_XMax(geom), 'ymax': ST_YMax(geom)
                    }} AS bbox
                  FROM (VALUES {POINTS}) t(id, geom)
                ) TO '{path.as_posix()}'
                (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', KV_METADATA {{geo: '{geo}'}})
            """)
        finally:
            con.close()
        before = pq.read_table(str(path)).column("pt").to_pylist()

        add_bbox_metadata(str(path))

        assert pq.read_table(str(path)).column("pt").to_pylist() == before
        assert _geo_key(path)["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]


class TestRefusesNonObjectGeoMetadata:
    """A `geo` key holding valid JSON that is not an object is not metadata.

    `not geo_meta` is False for a non-empty string, so the predicate fell through
    and `geo_meta.get("version", "")` raised a bare AttributeError.
    """

    def test_string_geo_is_refused_like_a_missing_one(self, tmp_path):
        path = _write(tmp_path / "scalar_geo.parquet", geo='"1.1.0"')

        with pytest.raises(GeoParquetError, match="no GeoParquet metadata"):
            add_bbox_metadata(str(path))

    def test_list_geo_is_refused_like_a_missing_one(self, tmp_path):
        path = _write(tmp_path / "list_geo.parquet", geo="[]")

        with pytest.raises(GeoParquetError, match="no GeoParquet metadata"):
            add_bbox_metadata(str(path))


class TestRefusesAFileWithNoBboxColumn:
    """The sibling "reports failure, exits 0" path in the same function.

    There is no covering to write without a bbox column, so this must fail the
    way the #713 branch fails rather than printing an error and returning 0.
    """

    @pytest.fixture
    def v11_without_bbox_column(self, tmp_path):
        path = tmp_path / "no_bbox.parquet"
        geo = json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
            }
        )
        con = get_duckdb_connection(load_spatial=True)
        try:
            con.execute(f"""
                COPY (
                  SELECT id, ST_AsWKB(geom)::BLOB AS geometry
                  FROM (VALUES {POINTS}) t(id, geom)
                ) TO '{path.as_posix()}'
                (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', KV_METADATA {{geo: '{geo}'}})
            """)
        finally:
            con.close()
        return path

    def test_raises_instead_of_returning(self, v11_without_bbox_column):
        with pytest.raises(GeoParquetError, match="No valid bbox column found"):
            add_bbox_metadata(str(v11_without_bbox_column))

    def test_cli_exits_non_zero(self, v11_without_bbox_column):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import add

        result = CliRunner().invoke(add, ["bbox-metadata", str(v11_without_bbox_column)])

        assert result.exit_code != 0
        assert "No valid bbox column found" in result.output


class TestRefusesPlainParquet:
    """#713: don't invent a `geo` block that fails five of its own spec checks."""

    def test_raises_instead_of_reporting_success(self, plain_parquet_with_bbox):
        with pytest.raises(GeoParquetError, match="no GeoParquet metadata"):
            add_bbox_metadata(str(plain_parquet_with_bbox))

    def test_error_names_the_command_that_does_work(self, plain_parquet_with_bbox):
        with pytest.raises(GeoParquetError, match="gpio convert geoparquet"):
            add_bbox_metadata(str(plain_parquet_with_bbox))

    def test_file_is_left_untouched(self, plain_parquet_with_bbox):
        before = pq.read_table(str(plain_parquet_with_bbox))
        with pytest.raises(GeoParquetError):
            add_bbox_metadata(str(plain_parquet_with_bbox))

        assert pq.read_metadata(str(plain_parquet_with_bbox)).metadata is None
        assert pq.read_table(str(plain_parquet_with_bbox)).equals(before)

    def test_cli_exits_non_zero(self, plain_parquet_with_bbox):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import add

        result = CliRunner().invoke(add, ["bbox-metadata", str(plain_parquet_with_bbox)])

        assert result.exit_code != 0
        assert "no GeoParquet metadata" in result.output

    def test_no_temp_file_is_left_behind(self, plain_parquet_with_bbox):
        with pytest.raises(GeoParquetError):
            add_bbox_metadata(str(plain_parquet_with_bbox))

        leftovers = list(plain_parquet_with_bbox.parent.glob("*.tmp"))
        assert leftovers == []


class TestPythonApiRefusesWhatTheCliRefuses:
    """#713, round 2: the CLI refused plain Parquet, the Python API did not.

    `Table.add_bbox_metadata` still carried the skeleton the core had dropped, so
    `gpio.read('plain.parquet').add_bbox_metadata()` succeeded where the CLI
    raised -- and `ops.add_bbox_metadata` did not exist at all, against the rule
    that every CLI command has a Python API in both `api/table.py` and
    `api/ops.py`.
    """

    @staticmethod
    def _cli_message(path):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import add

        result = CliRunner().invoke(add, ["bbox-metadata", str(path)])
        assert result.exit_code != 0
        return result.output

    def test_table_refuses_with_the_same_exception(self, plain_parquet_with_bbox):
        from geoparquet_io.api import read

        table = read(str(plain_parquet_with_bbox))
        with pytest.raises(GeoParquetError, match="no GeoParquet metadata"):
            table.add_bbox_metadata()

    def test_ops_refuses_with_the_same_exception(self, plain_parquet_with_bbox):
        from geoparquet_io.api import ops

        arrow_table = pq.read_table(str(plain_parquet_with_bbox))
        with pytest.raises(GeoParquetError, match="no GeoParquet metadata"):
            ops.add_bbox_metadata(arrow_table)

    def test_api_and_cli_give_the_same_explanation(self, plain_parquet_with_bbox):
        """One guard, one message: only the named subject differs (file vs table).

        The file path is a copy-pasteable `gpio convert geoparquet <path>` for the
        CLI; an in-memory table has no path, so it names itself, exactly as the
        1.0 gate says "this file" / "this table".
        """
        from geoparquet_io.api import ops

        cli_output = self._cli_message(plain_parquet_with_bbox)
        with pytest.raises(GeoParquetError) as excinfo:
            ops.add_bbox_metadata(pq.read_table(str(plain_parquet_with_bbox)))
        api_message = str(excinfo.value)

        shared = (
            "has no GeoParquet metadata, so there is nothing to describe the "
            "geometry column (encoding, geometry types) that the 'covering' key "
            "attaches to."
        )
        assert shared in api_message
        assert shared in " ".join(cli_output.split())
        assert "gpio convert geoparquet" in api_message
        assert "gpio convert geoparquet" in cli_output

        expected = api_message.replace("this table", str(plain_parquet_with_bbox)).replace(
            "input.parquet", str(plain_parquet_with_bbox)
        )
        assert " ".join(expected.split()) in " ".join(cli_output.split())

    def test_ops_writes_the_covering_on_a_valid_1_1_table(self, v11_wkb_with_bbox):
        """The success path: same key the CLI writes."""
        from geoparquet_io.api import ops

        result = ops.add_bbox_metadata(pq.read_table(str(v11_wkb_with_bbox)))

        geo = json.loads(result.schema.metadata[b"geo"].decode("utf-8"))
        assert geo["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]
        assert geo["version"] == "1.1.0"

    def test_table_writes_the_covering_on_a_valid_1_1_table(self, v11_wkb_with_bbox):
        from geoparquet_io.api import read

        result = read(str(v11_wkb_with_bbox)).add_bbox_metadata()

        columns = result.metadata().get("geo_metadata", {}).get("columns", {})
        assert columns["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]


_GEO_11 = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
}

# WKB for POINT (30 10), little-endian -- the same point the file fixtures use.
_WKB_POINT = struct.pack("<BI2d", 1, 1, 30.0, 10.0)


def _arrow_table(geo=None, *, with_geometry=True, with_bbox=True):
    """An in-memory table shaped like the file fixtures above."""
    columns = {"id": [1]}
    if with_geometry:
        columns["geometry"] = pa.array([_WKB_POINT], type=pa.binary())
    if with_bbox:
        columns["bbox"] = pa.array(
            [{"xmin": 30.0, "ymin": 10.0, "xmax": 30.0, "ymax": 10.0}],
            type=pa.struct(
                [
                    ("xmin", pa.float64()),
                    ("ymin", pa.float64()),
                    ("xmax", pa.float64()),
                    ("ymax", pa.float64()),
                ]
            ),
        )
    table = pa.table(columns)
    if geo is not None:
        table = table.replace_schema_metadata({b"geo": geo})
    return table


class TestTableEntryPointDefensivePaths:
    """`add_bbox_metadata_table` guards a caller-supplied table, not a file.

    A table handed in by a library caller has been through none of the file
    path's checks, so each of these is reachable from ordinary API use rather
    than being unreachable defensive code.
    """

    def test_a_table_with_no_geometry_column_is_refused(self):
        """Detection returns None rather than guessing a column."""
        table = _arrow_table(json.dumps(_GEO_11).encode(), with_geometry=False)

        with pytest.raises(ValueError, match="no geometry column detected"):
            add_bbox_metadata_table(table)

    def test_an_unparsable_geo_key_is_treated_as_absent(self):
        """Invalid JSON in `geo` must refuse, not raise a JSONDecodeError."""
        table = _arrow_table(b"{not json at all")

        with pytest.raises(GeoParquetError, match="no GeoParquet metadata"):
            add_bbox_metadata_table(table)

    def test_a_non_dict_columns_value_is_replaced(self):
        """`columns` is arbitrary JSON from the caller; a list must not crash."""
        geo = dict(_GEO_11, columns=["geometry"])
        table = _arrow_table(json.dumps(geo).encode())

        result = add_bbox_metadata_table(table)

        written = json.loads(result.schema.metadata[b"geo"].decode("utf-8"))
        assert written["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]

    def test_a_non_dict_column_entry_is_replaced(self):
        """The per-column entry can be a scalar; the covering still lands."""
        geo = dict(_GEO_11, columns={"geometry": "WKB"})
        table = _arrow_table(json.dumps(geo).encode())

        result = add_bbox_metadata_table(table)

        written = json.loads(result.schema.metadata[b"geo"].decode("utf-8"))
        assert written["columns"]["geometry"]["covering"]["bbox"]["ymax"] == ["bbox", "ymax"]


class TestBlobConversionWrapperDefensivePaths:
    """`_wrap_query_with_blob_conversion` runs against whatever DuckDB reports."""

    def test_a_query_duckdb_cannot_describe_falls_back_to_the_primary(self):
        """DESCRIBE can fail; the wrapper must still convert the primary column."""
        con = get_duckdb_connection()
        query = _wrap_query_with_blob_conversion(
            "SELECT * FROM a_table_that_does_not_exist", "geometry", con
        )

        assert 'ST_AsWKB("geometry")' in query

    def test_a_secondary_repeating_the_primary_is_not_cast_twice(self):
        """A geo block may name the primary column among its own columns."""
        con = get_duckdb_connection()
        con.execute("CREATE TABLE t AS SELECT 1 AS id, ST_Point(1, 2) AS geometry")
        query = _wrap_query_with_blob_conversion(
            "SELECT * FROM t", "geometry", con, secondary_columns=["geometry", "geometry"]
        )

        assert query.count('ST_AsWKB("geometry")') == 1
