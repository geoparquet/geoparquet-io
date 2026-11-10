"""VARIANT property columns must not crash gpio (#1352).

DuckDB 2.0 (and 1.4+) writes heterogeneous values as the Parquet VARIANT
logical type, but DuckDB 1.5 cannot export a VARIANT column over the Arrow C
interface: ``con.sql(...).arrow()`` raises ``OSError: Not implemented Error:
Unsupported Arrow type VARIANT``. That broke every surface that probes a
query's output schema through Arrow (``gpio extract``, ``gpio sort``) and the
``gpio inspect head``/``tail`` preview. These tests pin the graceful paths on
both DuckDB 1.5 and 2.0.
"""

import json

import duckdb
import pyarrow as pa
import pytest

from geoparquet_io.core.duckdb_utils import get_duckdb_connection, query_schema, sql_path
from geoparquet_io.core.extract import extract
from geoparquet_io.core.geo_metadata import GeoBlockOptions, QuerySource, compose_geo_block
from geoparquet_io.core.inspect_utils import get_preview_data

# One row per VARIANT shape: int, string, nested object, mixed list, NULL.
VARIANT_ROWS_SQL = """
SELECT * FROM (VALUES
    (1, ST_GeomFromText('POINT(1 1)'), '42'::JSON::VARIANT),
    (2, ST_GeomFromText('POINT(2 2)'), '"hello"'::JSON::VARIANT),
    (3, ST_GeomFromText('POINT(3 3)'),
        '{"name":"obj","depth":{"a":[1,2]},"n":1.5}'::JSON::VARIANT),
    (4, ST_GeomFromText('POINT(4 4)'), '[1,"two",3.0,null]'::JSON::VARIANT),
    (5, ST_GeomFromText('POINT(5 5)'), NULL::VARIANT)
) AS v(id, geometry, properties)
"""


def _arrow_can_export_variant() -> bool:
    """Whether the installed DuckDB exports VARIANT over Arrow (2.0: yes, 1.5: no)."""
    con = duckdb.connect()
    try:
        con.sql("SELECT '1'::JSON::VARIANT AS v LIMIT 0").arrow()
        return True
    except Exception:
        return False
    finally:
        con.close()


@pytest.fixture
def variant_file(tmp_path):
    """A GeoParquet file with a VARIANT properties column, as DuckDB writes it."""
    path = tmp_path / "variant.parquet"
    con = get_duckdb_connection()
    try:
        con.execute(f"COPY ({VARIANT_ROWS_SQL}) TO {sql_path(str(path))} (FORMAT parquet)")
    finally:
        con.close()
    return str(path)


class TestOutputQuerySchema:
    """The schema probe must survive a VARIANT column instead of raising."""

    def test_variant_column_does_not_break_probe(self, variant_file):
        con = get_duckdb_connection()
        try:
            query = f"SELECT * FROM read_parquet({sql_path(variant_file)})"
            schema = query_schema(con, query)
        finally:
            con.close()
        assert "id" in schema.names
        assert "geometry" in schema.names
        # The column must stay in the schema on BOTH versions: the covering
        # gates treat a column absent from the schema as somebody else's
        # question and keep its covering, so dropping it on 1.5 shipped a
        # spec-invalid covering 2.0 strips. 1.5 gets a sentinel binary field.
        assert "properties" in schema.names
        if not _arrow_can_export_variant():
            assert pa.types.is_binary(schema.field("properties").type)

    def test_all_variant_query_yields_schema_without_crash(self):
        con = get_duckdb_connection(load_spatial=False)
        try:
            schema = query_schema(con, "SELECT '1'::JSON::VARIANT AS v")
        finally:
            con.close()
        assert list(schema.names) == ["v"]
        if not _arrow_can_export_variant():
            assert pa.types.is_binary(schema.field("v").type)

    def test_struct_field_named_variant_id_survives_fallback(self):
        """A struct FIELD named variant_id is not a VARIANT (#1352 review).

        The fallback used to substring-match the DESCRIBE type, so
        ``STRUCT(variant_id INTEGER)`` beside a real VARIANT vanished from the
        probe schema on 1.5. It must keep its real struct type.
        """
        con = get_duckdb_connection(load_spatial=False)
        try:
            schema = query_schema(
                con,
                "SELECT 1 AS id, {'variant_id': 7} AS attrs, '1'::JSON::VARIANT AS props",
            )
        finally:
            con.close()
        assert list(schema.names) == ["id", "attrs", "props"]
        assert pa.types.is_struct(schema.field("attrs").type)
        if not _arrow_can_export_variant():
            assert pa.types.is_binary(schema.field("props").type)


class TestTypeContainsVariant:
    """The shared predicate matches the VARIANT type keyword, not field names."""

    def test_word_boundary_matching(self):
        from geoparquet_io.core.duckdb_utils import _type_contains_variant

        assert _type_contains_variant("VARIANT")
        assert _type_contains_variant("VARIANT[]")
        assert _type_contains_variant("STRUCT(a VARIANT)")
        assert _type_contains_variant("MAP(VARCHAR, VARIANT)")
        assert not _type_contains_variant("STRUCT(variant_id INTEGER)")
        assert not _type_contains_variant("STRUCT(variant VARCHAR)")
        assert not _type_contains_variant("INTEGER")


class TestCoveringGateParity:
    """A covering declared over a VARIANT column is stripped on 1.5 and 2.0 alike."""

    def test_covering_over_variant_column_is_stripped(self, variant_file):
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": [],
                    "covering": {
                        "bbox": {
                            "xmin": ["properties", "xmin"],
                            "ymin": ["properties", "ymin"],
                            "xmax": ["properties", "xmax"],
                            "ymax": ["properties", "ymax"],
                        }
                    },
                }
            },
        }
        con = get_duckdb_connection()
        try:
            query = f"SELECT * FROM read_parquet({sql_path(variant_file)})"
            gated = compose_geo_block(
                QuerySource(con, query, {b"geo": json.dumps(geo).encode()}),
                GeoBlockOptions("geometry", version="1.1", covering=None),
            )
        finally:
            con.close()
        assert "covering" not in gated["columns"]["geometry"]

    def test_non_variant_arrow_errors_are_reraised(self):
        class _BoomRelation:
            def arrow(self):
                raise OSError("some unrelated arrow failure")

        class _FakeCon:
            def sql(self, query, params=None):
                return _BoomRelation()

        with pytest.raises(OSError, match="unrelated arrow failure"):
            query_schema(_FakeCon(), "SELECT 1")


class TestExtractRoundTrip:
    """gpio's write path must carry a VARIANT column through unchanged."""

    def test_extract_preserves_variant_column(self, variant_file, tmp_path):
        out = tmp_path / "out.parquet"
        extract(variant_file, str(out))

        con = get_duckdb_connection()
        try:
            types = dict(
                con.sql(
                    "SELECT column_name, column_type FROM "
                    f"(DESCRIBE SELECT * FROM read_parquet({sql_path(str(out))}))"
                ).fetchall()
            )
            assert types["properties"] == "VARIANT"
            expected = con.sql(
                f"SELECT properties::JSON FROM read_parquet({sql_path(variant_file)}) ORDER BY id"
            ).fetchall()
            got = con.sql(
                f"SELECT properties::JSON FROM read_parquet({sql_path(str(out))}) ORDER BY id"
            ).fetchall()
        finally:
            con.close()
        assert got == expected


class TestPreview:
    """inspect head/tail render VARIANT values as JSON text on every DuckDB."""

    def test_struct_field_named_variant_id_renders_as_struct(self, tmp_path):
        """A struct with a field named variant_id keeps native rendering (#1352 review)."""
        path = tmp_path / "struct_variant_id.parquet"
        con = get_duckdb_connection()
        try:
            con.execute(
                "COPY (SELECT 1 AS id, ST_GeomFromText('POINT(1 1)') AS geometry, "
                f"{{'variant_id': 7}} AS attrs) TO {sql_path(str(path))} (FORMAT parquet)"
            )
        finally:
            con.close()
        table, mode = get_preview_data(str(path), head=1)
        assert mode == "head"
        assert table.column("attrs").to_pylist() == [{"variant_id": 7}]

    def test_head_renders_variant_as_json(self, variant_file):
        table, mode = get_preview_data(variant_file, head=5)
        assert mode == "head"
        values = table.column("properties").to_pylist()
        assert values[0] == "42"
        assert values[1] == '"hello"'
        assert json.loads(values[2]) == {"name": "obj", "depth": {"a": [1, 2]}, "n": 1.5}
        assert json.loads(values[3]) == [1, "two", 3.0, None]
        assert values[4] is None


class TestVariantThroughArrowSchemaProbes:
    """The Arrow schema probes in ``add bbox`` and the stream reader go through
    ``query_schema``; a local ``LIMIT 0`` Arrow probe raises on DuckDB 1.5."""

    def test_streaming_add_bbox_survives_a_variant_column(self, variant_file):
        from geoparquet_io.core.add.bbox import _make_streaming_bbox_query

        con = get_duckdb_connection()
        try:
            source = f"read_parquet({sql_path(variant_file)})"
            query, passed_through, name = _make_streaming_bbox_query(source, con, "bbox", False)
            columns = [d[0] for d in con.execute(f"SELECT * FROM ({query}) LIMIT 0").description]
        finally:
            con.close()
        assert not passed_through
        assert name == "bbox"
        assert {"properties", "bbox"} <= set(columns)

    def test_stream_input_metadata_survives_a_variant_column(self, variant_file):
        from geoparquet_io.core.stream_io import _file_stream_metadata

        con = get_duckdb_connection()
        try:
            source = f"read_parquet({sql_path(variant_file)})"
            # No Arrow schema in hand (the remote-adapter path): the probe runs.
            metadata = _file_stream_metadata(con, source, variant_file, None, {})
        finally:
            con.close()
        assert isinstance(metadata, dict)


class TestArrowExportRefusal:
    """#1370: the strategies that export the *data* name the column and the way out.

    #1362 rescued the LIMIT-0 schema probe, so the default duckdb-kv strategy
    round-trips a VARIANT column on both DuckDB versions (``TestExtractRoundTrip``
    above). ``streaming`` and ``in-memory`` hand the data itself to Arrow, and on
    1.5 that raised a bare ``OSError: Not implemented Error: Unsupported Arrow
    type VARIANT`` -- an unhandled traceback naming neither the column nor a way
    out. DuckDB 2.0 exports VARIANT for real, so the same writes work there;
    these pin "nobody sees a raw OSError" on both, and the message only where
    the export is genuinely missing.
    """

    needs_15 = pytest.mark.skipif(
        _arrow_can_export_variant(),
        reason="this DuckDB exports VARIANT over Arrow; there is nothing to refuse",
    )

    @pytest.mark.parametrize("strategy", ["streaming", "in-memory"])
    def test_an_arrow_strategy_names_the_variant_column(self, variant_file, strategy, tmp_path):
        from geoparquet_io.core.exceptions import VariantExportError

        out = tmp_path / "out.parquet"
        if _arrow_can_export_variant():
            extract(variant_file, str(out))
            assert out.exists()
            return

        with pytest.raises(VariantExportError) as excinfo:
            extract(variant_file, str(out), write_strategy=strategy)

        message = str(excinfo.value)
        assert "properties" in message
        assert "VARIANT" in message
        assert strategy in message
        # Both ways out, named: the strategy that works here, and the DuckDB
        # that exports VARIANT for real.
        assert "duckdb-kv" in message
        assert "2.0" in message

    @needs_15
    def test_the_refusal_is_a_gpio_error_the_cli_translates(self, variant_file, tmp_path):
        """``error_boundary`` claims ``GeoParquetError``; an OSError is a traceback."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli
        from geoparquet_io.core.exceptions import GeoParquetError

        out = tmp_path / "out.parquet"
        with pytest.raises(GeoParquetError):
            extract(variant_file, str(out), write_strategy="streaming")

        cli_out = tmp_path / "cli.parquet"
        result = CliRunner().invoke(
            cli,
            ["extract", "geoparquet", variant_file, str(cli_out), "--write-strategy", "streaming"],
        )
        assert result.exit_code != 0
        assert not isinstance(result.exception, OSError)
        assert "properties" in result.output

    def test_the_automatic_geoarrow_route_says_which_version_needs_it(self, variant_file, tmp_path):
        """1.1-geoarrow picks the streaming strategy itself, so name the version too."""
        from geoparquet_io.core.exceptions import VariantExportError

        out = tmp_path / "out.parquet"
        if _arrow_can_export_variant():
            extract(variant_file, str(out), geoparquet_version="1.1-geoarrow")
            assert out.exists()
            return

        with pytest.raises(VariantExportError) as excinfo:
            extract(variant_file, str(out), geoparquet_version="1.1-geoarrow")

        assert "1.1-geoarrow" in str(excinfo.value)

    @needs_15
    def test_a_non_variant_arrow_failure_is_not_claimed(self):
        """The guard re-raises anything else untouched, as the probe's check does."""
        from geoparquet_io.core.duckdb_utils import variant_export_guard

        with (
            pytest.raises(OSError, match="unrelated arrow failure"),
            variant_export_guard(None, "SELECT 1", strategy="streaming"),
        ):
            raise OSError("some unrelated arrow failure")

    def test_variant_columns_names_only_the_variant_ones(self):
        from geoparquet_io.core.duckdb_utils import variant_columns

        con = get_duckdb_connection(load_spatial=False)
        try:
            found = variant_columns(
                con,
                "SELECT 1 AS id, {'variant_id': 7} AS attrs, '1'::JSON::VARIANT AS props, "
                "['1'::JSON::VARIANT] AS many",
            )
        finally:
            con.close()
        assert found == ["props", "many"]

    def test_empty_columns_message_stays_grammatical(self):
        """No column name to give still reads as one sentence, not "Columns a column hold"."""
        from geoparquet_io.core.exceptions import VariantExportError

        message = str(VariantExportError([], "streaming", None, "1.5.6"))
        assert "Columns a column hold" not in message
        assert message.startswith("A column holds VARIANT values")
