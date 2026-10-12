"""
Tests for the extract bigquery command and API.

Tests are organized into:
- Unit tests (no BigQuery access needed): CLI structure, dry-run, backwards compatibility
- Integration tests (marked @pytest.mark.network): require BQ_TEST_TABLE env var
"""

from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import extract
from tests.conftest import safe_unlink


class TestBackwardsCompatibility:
    """Test that 'gpio extract input.parquet output.parquet' still works."""

    @pytest.fixture
    def input_file(self):
        """Create a test parquet file."""
        # Create a simple test parquet file
        schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
            ]
        )
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}, schema=schema)
        tmp_path = Path(tempfile.gettempdir()) / f"test_input_{uuid.uuid4()}.parquet"
        pq.write_table(table, str(tmp_path))
        yield str(tmp_path)
        safe_unlink(tmp_path)

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_output_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_extract_without_subcommand(self, input_file, output_file):
        """Test that extract without explicit subcommand defaults to geoparquet."""
        runner = CliRunner()
        result = runner.invoke(extract, [input_file, output_file])
        # Should work (exit 0) or give appropriate error
        # The key is it shouldn't fail with "Unknown subcommand"
        assert "Unknown command" not in result.output


class TestDetectGeometryColumn:
    """Test geometry column detection function."""

    def test_detect_common_names(self):
        """Test detection of common geometry column names."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column

        # Create table with "geometry" column
        table = pa.table(
            {
                "id": [1, 2],
                "geometry": [b"test", b"test2"],
            }
        )
        assert _detect_geometry_column(table) == "geometry"

        # Create table with "geography" column
        table = pa.table(
            {
                "id": [1, 2],
                "geography": [b"test", b"test2"],
            }
        )
        assert _detect_geometry_column(table) == "geography"

        # Create table with "geom" column
        table = pa.table(
            {
                "id": [1, 2],
                "geom": [b"test", b"test2"],
            }
        )
        assert _detect_geometry_column(table) == "geom"

    def test_detect_fallback_to_geo_in_name(self):
        """Test fallback to columns containing 'geo' in name."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column

        table = pa.table(
            {
                "id": [1, 2],
                "building_geom_wkb": [b"test", b"test2"],
            }
        )
        assert _detect_geometry_column(table) == "building_geom_wkb"

    def test_detect_returns_none_when_not_found(self):
        """Test that None is returned when no geometry column found."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column

        table = pa.table(
            {
                "id": [1, 2],
                "name": ["a", "b"],
            }
        )
        assert _detect_geometry_column(table) is None


class TestExtractBigQueryTable:
    """Test the extract_bigquery_table function for in-memory tables."""

    def test_extract_with_limit(self):
        """Test extracting with a row limit."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery_table

        table = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["a", "b", "c", "d", "e"],
            }
        )
        result = extract_bigquery_table(table, limit=3)
        assert result.num_rows == 3

    def test_extract_with_columns(self):
        """Test extracting specific columns."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery_table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "value": [10, 20, 30],
            }
        )
        result = extract_bigquery_table(table, columns=["id", "name"])
        assert result.column_names == ["id", "name"]

    def test_extract_with_exclude_columns(self):
        """Test excluding specific columns."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery_table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "value": [10, 20, 30],
            }
        )
        result = extract_bigquery_table(table, exclude_columns=["value"])
        assert "value" not in result.column_names
        assert "id" in result.column_names
        assert "name" in result.column_names


class TestBuildGeometrySelectExpr:
    """Test _build_geometry_select_expr handles different column types."""

    def test_varchar_type_uses_geomfromtext(self):
        """Test that VARCHAR columns get ST_GeomFromText wrapping."""
        from geoparquet_io.core.extract_bigquery import _build_geometry_select_expr

        expr = _build_geometry_select_expr("geometry", "VARCHAR")
        assert "ST_GeomFromText" in expr
        assert "ST_AsWKB" in expr
        assert '"geometry"' in expr

    def test_geometry_type_uses_direct_aswkb(self):
        """Test that GEOMETRY-typed columns use ST_AsWKB directly."""
        from geoparquet_io.core.extract_bigquery import _build_geometry_select_expr

        expr = _build_geometry_select_expr("geometry", "GEOMETRY")
        assert "ST_AsWKB" in expr
        assert "ST_GeomFromText" not in expr

    def test_string_type_uses_geomfromtext(self):
        """Test that STRING type (another non-GEOMETRY type) uses ST_GeomFromText."""
        from geoparquet_io.core.extract_bigquery import _build_geometry_select_expr

        expr = _build_geometry_select_expr("geom", "STRING")
        assert "ST_GeomFromText" in expr
        assert "ST_AsWKB" in expr

    def test_varchar_geometry_query_executes(self):
        """Test that the generated SQL for VARCHAR geometry actually works."""
        from geoparquet_io.core.extract_bigquery import _build_geometry_select_expr

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute("CREATE TABLE test_exec AS SELECT 1 AS id, 'POINT(1.5 2.5)' AS geometry")

        expr = _build_geometry_select_expr("geometry", "VARCHAR")
        result = con.execute(f"SELECT {expr} FROM test_exec").fetchall()
        assert len(result) == 1
        # geometry column should be BLOB (WKB bytes)
        assert isinstance(result[0][0], bytes)
        con.close()

    def test_geojson_format(self):
        """Test that GeoJSON format uses ST_GeomFromGeoJSON."""
        from geoparquet_io.core.extract_bigquery import _build_geometry_select_expr

        expr = _build_geometry_select_expr("geom", "VARCHAR", geometry_format="geojson")
        assert "ST_GeomFromGeoJSON" in expr
        assert "ST_AsWKB" in expr
        assert "ST_GeomFromText" not in expr

    def test_geojson_format_query_executes(self):
        """Test that GeoJSON format SQL actually works."""
        from geoparquet_io.core.extract_bigquery import _build_geometry_select_expr

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute(
            """CREATE TABLE test_geojson AS
            SELECT 1 AS id,
            '{"type":"Point","coordinates":[1.5,2.5]}' AS geom"""
        )

        expr = _build_geometry_select_expr("geom", "VARCHAR", geometry_format="geojson")
        result = con.execute(f"SELECT {expr} FROM test_geojson").fetchall()
        assert len(result) == 1
        assert isinstance(result[0][0], bytes)
        con.close()


class TestNoGeometryPlainParquet:
    """Test that tables without geometry are written as plain Parquet."""

    def test_no_geometry_detected_returns_none(self):
        """Test that _detect_geometry_column_from_schema returns None for non-GEOMETRY columns."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column_from_schema

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        # Table with a "geometry" column that is VARCHAR, not GEOMETRY type
        con.execute("CREATE TABLE test_no_geom AS SELECT 1 AS id, 'POINT(0 0)' AS geometry")

        # Should NOT detect "geometry" by name - only native GEOMETRY type
        result = _detect_geometry_column_from_schema(con, "test_no_geom", table_source="local")
        assert result is None
        con.close()

    def test_detects_native_geometry_type(self):
        """Test that native GEOMETRY columns are still detected."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column_from_schema

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute("CREATE TABLE test_native AS SELECT 1 AS id, ST_Point(0, 0) AS geometry")

        result = _detect_geometry_column_from_schema(con, "test_native", table_source="local")
        assert result == "geometry"
        con.close()

    def test_explicit_varchar_column_returns_none(self):
        """Test that specifying a VARCHAR column returns None (not error)."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column_from_schema

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute("CREATE TABLE test_varchar AS SELECT 1 AS id, 'POINT(0 0)' AS geom")

        # Should return None - column exists but isn't GEOMETRY type
        result = _detect_geometry_column_from_schema(
            con, "test_varchar", geography_column="geom", table_source="local"
        )
        assert result is None
        con.close()

    def test_nonexistent_column_raises_error(self):
        """Test that specifying a nonexistent column raises ValueError."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column_from_schema

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute("CREATE TABLE test_noexist AS SELECT 1 AS id")

        with pytest.raises(ValueError, match="not found"):
            _detect_geometry_column_from_schema(
                con, "test_noexist", geography_column="geom", table_source="local"
            )
        con.close()


class TestEdgesParameter:
    """Test edge interpretation logic for native vs VARCHAR geometry columns."""

    def test_python_api_has_edges_parameter(self):
        """Test that Python API exposes edges parameter."""
        import inspect

        from geoparquet_io.api import Table, ops

        # Check Table.from_bigquery
        sig = inspect.signature(Table.from_bigquery)
        assert "edges" in sig.parameters
        assert "geography_column" in sig.parameters
        assert "geometry_format" in sig.parameters

        # Check ops.read_bigquery
        sig = inspect.signature(ops.read_bigquery)
        assert "edges" in sig.parameters
        assert "geography_column" in sig.parameters
        assert "geometry_format" in sig.parameters

    def test_python_api_parameter_defaults(self):
        """Test that Python API has correct default values."""
        import inspect

        from geoparquet_io.api import Table, ops

        # Check Table.from_bigquery defaults
        sig = inspect.signature(Table.from_bigquery)
        assert sig.parameters["edges"].default is None
        assert sig.parameters["geography_column"].default is None
        assert sig.parameters["geometry_format"].default == "wkt"

        # Check ops.read_bigquery defaults
        sig = inspect.signature(ops.read_bigquery)
        assert sig.parameters["edges"].default is None
        assert sig.parameters["geography_column"].default is None
        assert sig.parameters["geometry_format"].default == "wkt"

    def test_core_function_has_edges_parameter(self):
        """Test that core extract_bigquery function has edges parameter."""
        import inspect

        from geoparquet_io.core.extract_bigquery import extract_bigquery

        sig = inspect.signature(extract_bigquery)
        assert "edges" in sig.parameters
        assert sig.parameters["edges"].default is None


class TestColumnNameResolution:
    """Test column name case resolution for VARCHAR columns."""

    def test_resolve_column_name_case_insensitive(self):
        """Test that _resolve_column_name finds the actual schema spelling."""
        from geoparquet_io.core.extract_bigquery import _resolve_column_name

        con = duckdb.connect()
        con.execute("CREATE TABLE test_case AS SELECT 1 AS id, 'POINT(0 0)' AS Geometry")

        # Should resolve "geometry" to "Geometry" (actual schema spelling)
        resolved = _resolve_column_name(con, "test_case", "geometry", table_source="local")
        assert resolved == "Geometry"

        # Should resolve "GEOMETRY" to "Geometry"
        resolved = _resolve_column_name(con, "test_case", "GEOMETRY", table_source="local")
        assert resolved == "Geometry"

        con.close()

    def test_resolve_column_name_returns_input_if_not_found(self):
        """Test that _resolve_column_name returns input if column not found."""
        from geoparquet_io.core.extract_bigquery import _resolve_column_name

        con = duckdb.connect()
        con.execute("CREATE TABLE test_missing AS SELECT 1 AS id")

        resolved = _resolve_column_name(con, "test_missing", "nonexistent", table_source="local")
        assert resolved == "nonexistent"

        con.close()


class TestBboxFiltersWithVarchar:
    """Test bbox filter generation for VARCHAR WKT/GeoJSON columns."""

    def test_bbox_filter_native_geometry(self):
        """Test that native GEOMETRY columns use direct ST_INTERSECTS."""
        from geoparquet_io.core.extract_bigquery import _build_bbox_filters

        bq_filters, local_filters = _build_bbox_filters(
            "0,0,10,10", "geom", use_server_side=True, is_native_geometry=True
        )
        assert len(bq_filters) == 1
        assert "ST_INTERSECTS(`geom`," in bq_filters[0]
        assert "ST_GEOGFROMTEXT(`geom`" not in bq_filters[0]

    def test_bbox_filter_varchar_wkt(self):
        """Test that VARCHAR WKT columns wrap with ST_GEOGFROMTEXT."""
        from geoparquet_io.core.extract_bigquery import _build_bbox_filters

        bq_filters, local_filters = _build_bbox_filters(
            "0,0,10,10",
            "geom",
            use_server_side=True,
            is_native_geometry=False,
            geometry_format="wkt",
        )
        assert len(bq_filters) == 1
        assert "ST_GEOGFROMTEXT(`geom`)" in bq_filters[0]

    def test_bbox_filter_varchar_geojson(self):
        """Test that VARCHAR GeoJSON columns wrap with ST_GEOGFROMGEOJSON."""
        from geoparquet_io.core.extract_bigquery import _build_bbox_filters

        bq_filters, local_filters = _build_bbox_filters(
            "0,0,10,10",
            "geom",
            use_server_side=True,
            is_native_geometry=False,
            geometry_format="geojson",
        )
        assert len(bq_filters) == 1
        assert "ST_GEOGFROMGEOJSON(`geom`)" in bq_filters[0]

    def test_local_filter_varchar_wkt(self):
        """Test that local filtering wraps VARCHAR WKT with ST_GeomFromText."""
        from geoparquet_io.core.extract_bigquery import _build_bbox_filters

        bq_filters, local_filters = _build_bbox_filters(
            "0,0,10,10",
            "geom",
            use_server_side=False,
            is_native_geometry=False,
            geometry_format="wkt",
        )
        assert len(local_filters) == 1
        assert 'ST_GeomFromText("geom")' in local_filters[0]


class TestDryRun:
    """Test dry-run functionality."""

    def test_dry_run_shows_sql(self):
        """Test that dry-run shows SQL without executing."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        # Dry-run should not raise an error even without credentials
        result = extract_bigquery(
            table_id="project.dataset.table",
            dry_run=True,
        )
        assert result is None


class TestWhereClauseValidation:
    """--where must be routed through validate_where_clause (gpio #612 parity).

    A statement-separator injection must be rejected before any network call,
    on both the dry-run path and the real (non-dry-run) path.
    """

    INJECTION_WHERE = "1=1); COPY (SELECT 1) TO 's3://attacker/x'; --"

    def test_dry_run_rejects_semicolon_injection(self):
        """Dry-run must reject a ';' statement separator before printing SQL."""
        from geoparquet_io.core.exceptions import ValidationError
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        with pytest.raises(ValidationError, match="not a single filtering expression"):
            extract_bigquery(
                table_id="project.dataset.table",
                dry_run=True,
                where=self.INJECTION_WHERE,
            )

    # Blocklisted keywords (GRANT, DROP, DELETE) appear inside quoted literals
    # here, so a validator that uppercases the whole clause rejects them.
    @pytest.mark.parametrize(
        "clause",
        [
            "name = 'Grant County'",
            "descr ILIKE '%drop off%'",
            "REPLACE(zip, '-', '') = '19104'",
        ],
    )
    def test_dry_run_allows_legitimate_where(self, clause):
        """A legitimate --where does not raise on the dry-run path."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        result = extract_bigquery(
            table_id="project.dataset.table",
            dry_run=True,
            where=clause,
        )
        assert result is None

    @patch("geoparquet_io.core.extract_bigquery._setup_bigquery_connection")
    def test_real_path_rejects_injection_before_network(self, mock_setup):
        """Non-dry-run path must validate before establishing a BigQuery connection."""
        from geoparquet_io.core.exceptions import ValidationError
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        mock_setup.side_effect = AssertionError(
            "network connection attempted before where-clause validation"
        )

        with pytest.raises(ValidationError, match="not a single filtering expression"):
            extract_bigquery(
                table_id="project.dataset.table",
                dry_run=False,
                where=self.INJECTION_WHERE,
            )

        mock_setup.assert_not_called()


def _strip_line_comments(sql: str) -> str:
    """Drop everything a ``--`` comment would swallow, line by line."""
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


class TestWhereClauseCannotSwallowLaterClauses:
    """A trailing ``--`` in --where must not comment out the rest of the query."""

    TRAILING_COMMENT_WHERE = "1=1) --"

    def test_build_bigquery_query_keeps_limit(self):
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        sql = _build_bigquery_query(
            con=None,
            validated_table_id="project.dataset.table",
            select_cols="*",
            bbox=None,
            bbox_mode="auto",
            bbox_threshold=1000,
            geom_col=None,
            where=self.TRAILING_COMMENT_WHERE,
            limit=10,
        )
        assert "LIMIT 10" in _strip_line_comments(sql)

    def test_dry_run_sql_keeps_bbox_and_limit(self, monkeypatch):
        import geoparquet_io.core.extract_bigquery as bq_module

        printed: list[str] = []
        monkeypatch.setattr(bq_module, "progress", printed.append)

        bq_module._handle_dry_run(
            validated_table_id="project.dataset.table",
            include_list=None,
            bbox="0,0,1,1",
            bbox_mode="local",
            bbox_threshold=1000,
            where=self.TRAILING_COMMENT_WHERE,
            limit=10,
        )

        sql = next(line for line in printed if line.startswith("SQL: "))
        live_sql = _strip_line_comments(sql)
        assert "ST_Intersects" in live_sql
        assert "LIMIT 10" in live_sql


def _filter_literal(sql: str) -> str:
    """Decode the ``filter='...'`` DuckDB string literal out of a bigquery_scan call.

    DuckDB itself does the decoding, so the assertion is about what the BigQuery
    extension actually receives, not about how the Python f-string looked.
    """
    literal = sql.split("filter=", 1)[1].rsplit(")", 1)[0]
    con = duckdb.connect()
    try:
        return con.execute(f"SELECT {literal}").fetchone()[0]
    finally:
        con.close()


class TestGeometryIdentifierQuoting:
    """The geometry column name is BigQuery schema text, so it must be quoted.

    ``geom_col`` comes from ``_detect_geometry_column_from_schema`` (a DESCRIBE
    over the remote table), so the *table publisher* chooses the string. A
    BigQuery flexible column name may legally contain an apostrophe, and the
    server-side filter is carried inside a DuckDB ``filter='...'`` literal, so an
    unescaped name breaks out of that literal. See gpio #932.

    Two dialects are in play and they disagree: DuckDB quotes identifiers with
    ``"..."`` while BigQuery reads ``"..."`` as a STRING literal and quotes
    identifiers with backticks.
    """

    # Legal as a BigQuery flexible column name (apostrophe is in the allowed
    # set), and an apostrophe is exactly what closes the DuckDB filter literal.
    HOSTILE = "geom') OR (1=1) OR ('x"
    # DuckDB's own delimiter, for the local-filter branch.
    HOSTILE_DUCKDB = 'geom") OR (1=1) OR ("x'

    def test_bq_native_filter_backtick_quotes_the_column(self):
        from geoparquet_io.core.extract_bigquery import _build_bbox_filters

        bq_filters, _ = _build_bbox_filters(
            "0,0,10,10", "geom", use_server_side=True, is_native_geometry=True
        )
        assert bq_filters[0].startswith("ST_INTERSECTS(`geom`,")
        # A double-quoted name would be a BigQuery STRING literal, not the column.
        assert '"geom"' not in bq_filters[0]

    def test_bq_varchar_wkt_filter_backtick_quotes_the_column(self):
        from geoparquet_io.core.extract_bigquery import _build_bbox_filters

        bq_filters, _ = _build_bbox_filters(
            "0,0,10,10",
            "geom",
            use_server_side=True,
            is_native_geometry=False,
            geometry_format="wkt",
        )
        assert "ST_GEOGFROMTEXT(`geom`)" in bq_filters[0]

    def test_bq_varchar_geojson_filter_backtick_quotes_the_column(self):
        from geoparquet_io.core.extract_bigquery import _build_bbox_filters

        bq_filters, _ = _build_bbox_filters(
            "0,0,10,10",
            "geom",
            use_server_side=True,
            is_native_geometry=False,
            geometry_format="geojson",
        )
        assert "ST_GEOGFROMGEOJSON(`geom`)" in bq_filters[0]

    @pytest.mark.parametrize(
        ("is_native", "geometry_format", "expected"),
        [
            (True, "wkt", 'ST_Intersects("weird ""geom""",'),
            (False, "wkt", 'ST_GeomFromText("weird ""geom""")'),
            (False, "geojson", 'ST_GeomFromGeoJSON("weird ""geom""")'),
        ],
    )
    def test_local_filter_quotes_the_column_for_duckdb(self, is_native, geometry_format, expected):
        """The local branch is DuckDB SQL, so it needs doubled double quotes."""
        from geoparquet_io.core.extract_bigquery import _build_bbox_filters

        _, local = _build_bbox_filters(
            "0,0,10,10",
            'weird "geom"',
            use_server_side=False,
            is_native_geometry=is_native,
            geometry_format=geometry_format,
        )
        assert expected in local[0]

    def test_local_filter_with_hostile_name_stays_one_expression(self):
        from geoparquet_io.core.extract_bigquery import _build_bbox_filters

        _, local = _build_bbox_filters(
            "0,0,10,10", self.HOSTILE_DUCKDB, use_server_side=False, is_native_geometry=True
        )
        # The whole hostile name stays inside one quoted identifier: the ')' and
        # 'OR' are data, not query structure.
        assert local[0].startswith('ST_Intersects("geom"") OR (1=1) OR (""x",')
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            # Parses as one expression referencing a column that does not exist,
            # rather than as an injected disjunction that evaluates to TRUE.
            with pytest.raises(duckdb.Error, match="not found|Referenced column"):
                con.execute(f"SELECT {local[0]}")
        finally:
            con.close()

    def test_apostrophe_column_round_trips_through_the_filter_literal(self):
        """A legal BigQuery flexible column name with an apostrophe must survive."""
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        sql = _build_bigquery_query(
            con=None,
            validated_table_id="project.dataset.table",
            select_cols="*",
            bbox="0,0,10,10",
            bbox_mode="server",
            bbox_threshold=1000,
            geom_col="o'brien geom",
            where=None,
            limit=None,
        )
        assert len(duckdb.extract_statements(sql)) == 1
        assert _filter_literal(sql).startswith("ST_INTERSECTS(`o'brien geom`,")

    def test_hostile_column_cannot_change_the_query_shape(self):
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        sql = _build_bigquery_query(
            con=None,
            validated_table_id="project.dataset.table",
            select_cols="*",
            bbox="0,0,10,10",
            bbox_mode="server",
            bbox_threshold=1000,
            geom_col=self.HOSTILE,
            where=None,
            limit=None,
        )
        # One DuckDB statement, and the whole hostile name is still one
        # backtick-quoted BigQuery identifier.
        assert len(duckdb.extract_statements(sql)) == 1
        assert _filter_literal(sql).startswith(f"ST_INTERSECTS(`{self.HOSTILE}`,")

    def test_backtick_in_name_is_escaped_for_bigquery(self):
        from geoparquet_io.core.extract_bigquery import _quote_bigquery_identifier

        assert _quote_bigquery_identifier("a`b") == "`a\\`b`"
        assert _quote_bigquery_identifier("a\\b") == "`a\\\\b`"

    @pytest.mark.parametrize("bad", ["", "a\x00b"])
    def test_unquotable_bigquery_name_is_rejected(self, bad):
        from geoparquet_io.core.extract_bigquery import _quote_bigquery_identifier

        with pytest.raises(ValueError):
            _quote_bigquery_identifier(bad)

    def test_local_schema_lookup_quotes_the_table_name(self):
        """The table_source="local" branch interpolated the table name bare."""
        from geoparquet_io.core.extract_bigquery import _resolve_column_name

        con = duckdb.connect()
        try:
            con.execute('CREATE TABLE "odd ""name"" tbl" AS SELECT 1 AS Id')
            assert _resolve_column_name(con, 'odd "name" tbl', "id", table_source="local") == "Id"
        finally:
            con.close()

    def test_dry_run_server_sql_is_unchanged(self):
        """The dry-run display escapes once too, and must render identically."""
        from geoparquet_io.core.extract_bigquery import _build_dry_run_query

        query = _build_dry_run_query(
            "project.dataset.table", "*", "0,0,1,1", bbox_mode="server", bbox_threshold=1000
        )
        assert query == (
            "SELECT * FROM bigquery_scan('project.dataset.table', "
            "filter='ST_INTERSECTS(<geometry_column>, ST_GEOGFROMTEXT(''POLYGON((0.0 0.0, "
            "1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))''))')"
        )

    def test_plain_name_query_text_is_unchanged(self):
        """Relayering the escape must not alter the SQL emitted for ordinary names."""
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        sql = _build_bigquery_query(
            con=None,
            validated_table_id="project.dataset.table",
            select_cols="*",
            bbox="0,0,1,1",
            bbox_mode="server",
            bbox_threshold=1000,
            geom_col="geom",
            where=None,
            limit=None,
        )
        assert sql == (
            "SELECT * FROM bigquery_scan('project.dataset.table', "
            "filter='ST_INTERSECTS(`geom`, ST_GEOGFROMTEXT(''POLYGON((0.0 0.0, "
            "1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0))''))')"
        )


class TestColumnValidation:
    """``--include-cols``/``--exclude-cols`` are checked against the table (#969).

    The parquet backend has run ``validate_columns`` since #731; the BigQuery
    backend had no equivalent, so a name the table does not carry was quoted
    into the SELECT and failed as a DuckDB binder error (or, for
    ``--exclude-cols``, silently excluded nothing). A blank entry was worse
    still: it reached ``quote_identifier()`` and raised
    ``ValueError: cannot quote an empty SQL identifier``.

    The Click layer rejects blank entries before any connection is opened
    (``tests/test_cli_column_list_guard.py``); this is the schema half, which
    only the backend can do, and it matches the parquet precedent by raising
    ``InvalidParameterError`` -- mapped to exit 2 by ``handle_core_exception``.

    The validator itself moved to ``core/column_selection.py`` when Carto and
    ArcGIS turned out to need the same check (#980); only the ``DESCRIBE`` that
    feeds it is BigQuery's. These cases stay here because the schema they
    exercise is one this module produces -- notably the local-table schema
    carrying both ``id`` and ``ID``.
    """

    def test_none_is_passed_through(self):
        from geoparquet_io.core.column_selection import resolve_columns_against_schema

        assert resolve_columns_against_schema(None, ["id", "geom"], "--include-cols") is None

    def test_known_columns_are_returned(self):
        from geoparquet_io.core.column_selection import resolve_columns_against_schema

        result = resolve_columns_against_schema(
            ["id", "geom"], ["id", "name", "geom"], "--include-cols"
        )
        assert result == ["id", "geom"]

    def test_case_is_resolved_to_the_schema_spelling(self):
        """DuckDB matches a quoted identifier case-insensitively, but the exclude
        filter in ``_build_column_list`` compares strings, so ``--exclude-cols
        ID`` used to exclude nothing at all. Resolving here makes both agree."""
        from geoparquet_io.core.column_selection import resolve_columns_against_schema

        result = resolve_columns_against_schema(["ID", "GeoM"], ["id", "geom"], "--exclude-cols")
        assert result == ["id", "geom"]

    def test_an_exact_match_wins_over_a_case_fold(self):
        """Case folding cannot be allowed to collapse two distinct columns.

        ``_schema_column_names`` also serves ``table_source="local"``, and DuckDB
        lets a local table carry both ``id`` and ``ID``. A ``{col.lower(): col}``
        map keeps only the last of those, so an exactly-spelled request has to be
        honoured as itself before any folding is tried.
        """
        from geoparquet_io.core.column_selection import resolve_columns_against_schema

        schema = ["id", "ID", "geom"]
        assert resolve_columns_against_schema(["id"], schema, "--exclude-cols") == ["id"]
        assert resolve_columns_against_schema(["ID"], schema, "--exclude-cols") == ["ID"]
        # A spelling that matches neither exactly still folds, as before.
        assert resolve_columns_against_schema(["GeoM"], schema, "--include-cols") == ["geom"]

    @pytest.mark.parametrize("requested", ["Id", "iD"])
    def test_an_ambiguous_fold_raises_instead_of_picking_one(self, requested):
        """A third spelling could mean either column, so the caller is asked which.

        ``{col.lower(): col}`` silently kept the last, which is arbitrary -- and
        on ``--exclude-cols`` an arbitrary choice deletes a column the caller did
        not name.
        """
        from geoparquet_io.core.column_selection import resolve_columns_against_schema
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError) as exc_info:
            resolve_columns_against_schema([requested], ["id", "ID", "geom"], "--exclude-cols")

        message = str(exc_info.value)
        assert "Ambiguous" in message
        assert "ID" in message and "id" in message

    def test_an_unambiguous_schema_never_reports_ambiguity(self):
        """The check must not fire on ordinary schemas, which is every real one."""
        from geoparquet_io.core.column_selection import resolve_columns_against_schema

        assert resolve_columns_against_schema(
            ["ID", "GeoM"], ["id", "name", "geom"], "--exclude-cols"
        ) == ["id", "geom"]

    def test_missing_column_is_an_invalid_parameter(self):
        from geoparquet_io.core.column_selection import resolve_columns_against_schema
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError) as exc_info:
            resolve_columns_against_schema(["id", "nope"], ["id", "geom"], "--include-cols")

        message = str(exc_info.value)
        assert "--include-cols" in message
        assert "nope" in message
        # The available columns are listed, as the parquet backend does.
        assert "id, geom" in message

    def test_blank_entry_is_an_invalid_parameter(self):
        """The Python API bypasses Click, so core must reject a blank too."""
        from geoparquet_io.core.column_selection import resolve_columns_against_schema
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="empty or whitespace-only"):
            resolve_columns_against_schema(["id", "   "], ["id", "geom"], "--exclude-cols")

    def test_blank_entry_never_reaches_quote_identifier(self):
        from geoparquet_io.core.column_selection import resolve_columns_against_schema
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError):
            resolve_columns_against_schema([""], ["id"], "--include-cols")

    def test_schema_column_names_reads_the_table(self):
        from geoparquet_io.core.extract_bigquery import _schema_column_names

        con = duckdb.connect()
        try:
            con.execute("CREATE TABLE test_cols AS SELECT 1 AS id, 'x' AS Name")
            assert _schema_column_names(con, "test_cols", table_source="local") == ["id", "Name"]
        finally:
            con.close()

    def test_schema_column_names_scans_the_remote_table(self):
        """The BigQuery branch reads through ``bigquery_scan``, not a table name."""
        from geoparquet_io.core.extract_bigquery import _schema_column_names

        con = MagicMock()
        con.execute.return_value.fetchall.return_value = [("id", "BIGINT"), ("geom", "GEOMETRY")]

        assert _schema_column_names(con, "project.dataset.table") == ["id", "geom"]
        query = con.execute.call_args[0][0]
        assert "bigquery_scan('project.dataset.table')" in query

    def test_build_column_list_reuses_the_schema_it_was_given(self):
        """The exclude branch takes the caller's schema instead of a DESCRIBE."""
        from geoparquet_io.core.extract_bigquery import _build_column_list

        # con is None: reusing the schema means no query is issued at all.
        assert _build_column_list(
            None, "project.dataset.table", None, ["name"], "geom", ["id", "name", "geom"]
        ) == ["id", "geom"]

    def test_extraction_validates_before_building_the_select(self):
        """The wiring: a bad name fails against the schema, not in the binder."""
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        con = MagicMock()
        with (
            patch("geoparquet_io.core.extract_bigquery.BigQueryConnection") as mock_conn,
            patch(
                "geoparquet_io.core.extract_bigquery._schema_rows",
                return_value=[("id", "BIGINT"), ("geom", "GEOMETRY")],
            ),
        ):
            mock_conn.return_value.__enter__.return_value = con
            with pytest.raises(InvalidParameterError, match="--include-cols"):
                extract_bigquery(
                    table_id="project-name.dataset.table",
                    output_parquet=None,
                    include_cols="id,nope",
                )
            # --exclude-cols runs through the same schema read.
            with pytest.raises(InvalidParameterError, match="--exclude-cols"):
                extract_bigquery(
                    table_id="project-name.dataset.table",
                    output_parquet=None,
                    exclude_cols="nope",
                )
        # Failed before any query was executed against the table.
        assert not any("SELECT" in str(call) for call in con.execute.call_args_list)

    def test_cli_reports_a_missing_column_as_a_usage_error(self, tmp_path):
        """Exit 2, matching ``gpio extract geoparquet --include-cols nope``."""
        from geoparquet_io.cli.main import cli

        with (
            patch("geoparquet_io.core.extract_bigquery.BigQueryConnection") as mock_conn,
            patch(
                "geoparquet_io.core.extract_bigquery._schema_rows",
                return_value=[("id", "BIGINT"), ("geom", "GEOMETRY")],
            ),
        ):
            mock_conn.return_value.__enter__.return_value = MagicMock()
            result = CliRunner().invoke(
                cli,
                [
                    "extract",
                    "bigquery",
                    "project-name.dataset.table",
                    str(tmp_path / "out.parquet"),
                    "--include-cols",
                    "nope",
                ],
            )

        assert result.exit_code == 2, result.output
        assert "nope" in result.output


class TestSchemaIsReadOnce:
    """Every schema read in this module goes through ``_schema_rows``.

    Five call sites each spelled out their own
    ``DESCRIBE SELECT * FROM bigquery_scan(...) LIMIT 0``. Consolidating them
    has two payoffs beyond the duplication: the raw ``{table_id}``
    interpolation that ``bigquery_scan`` forces (it takes a string literal, so
    ``sql_path``/``quote_identifier`` do not apply) now exists in exactly one
    place with the pre-validation contract written next to it; and a caller that
    already holds the rows can hand them on instead of paying for a second
    round-trip against BigQuery.
    """

    def test_describe_is_spelled_in_exactly_one_helper(self):
        """A new inline DESCRIBE fails here rather than quietly becoming a sixth."""
        import inspect

        from geoparquet_io.core import extract_bigquery

        module_source = Path(inspect.getsourcefile(extract_bigquery)).read_text(encoding="utf-8")
        helper_source = inspect.getsource(extract_bigquery._schema_rows)

        # One, now that the three sources a schema can come from -- a scan, a
        # local table, a pre-built --sql relation -- differ only in the
        # relation they name, and the DESCRIBE around it is written once.
        assert helper_source.count("DESCRIBE SELECT") == 1
        assert module_source.count("DESCRIBE SELECT") == 1

    def test_given_rows_are_used_without_touching_the_connection(self):
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column_from_schema

        rows = [("id", "BIGINT"), ("geom", "GEOMETRY")]
        # con is None: reusing the rows means no query can possibly be issued.
        assert _detect_geometry_column_from_schema(None, "p.d.t", schema_rows=rows) == "geom"

    def test_an_explicit_geography_column_resolves_from_the_given_rows(self):
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column_from_schema

        rows = [("id", "BIGINT"), ("Geom", "GEOMETRY")]
        assert (
            _detect_geometry_column_from_schema(None, "p.d.t", "GEOM", schema_rows=rows) == "Geom"
        )

    def test_column_type_comes_from_the_shared_read(self):
        from geoparquet_io.core.extract_bigquery import _get_column_type

        con = MagicMock()
        con.execute.return_value.fetchall.return_value = [("id", "BIGINT"), ("geom", "GEOMETRY")]

        assert _get_column_type(con, "p.d.t", "GEOM") == "GEOMETRY"
        assert _get_column_type(con, "p.d.t", "absent") == "VARCHAR"

    def test_selecting_all_columns_reads_them_through_the_helper(self):
        from geoparquet_io.core.extract_bigquery import _build_select_with_wkb

        con = MagicMock()
        con.execute.return_value.fetchall.return_value = [("id", "BIGINT"), ("geom", "GEOMETRY")]

        select_cols, _ = _build_select_with_wkb(None, None, con, "p.d.t")
        assert '"id"' in select_cols and '"geom"' in select_cols

    def test_column_validation_costs_no_extra_schema_read(self):
        """``--include-cols`` used to add a DESCRIBE the detection already paid for.

        The invariant: by the time ``_build_column_list`` is reached, the schema
        has been read exactly once, whether or not either column option was
        given.
        """
        import functools

        from geoparquet_io.core.extract_bigquery import extract_bigquery

        class _Stop(Exception):
            pass

        rows = [("id", "BIGINT"), ("name", "VARCHAR"), ("geom", "GEOMETRY")]

        def _spy(con, table_id=None, table_source="bigquery", relation=None, *, _reads):
            _reads.append(table_id)
            return rows

        for kwargs in ({}, {"include_cols": "id"}, {"exclude_cols": "name"}):
            reads: list[str] = []
            with (
                patch("geoparquet_io.core.extract_bigquery.BigQueryConnection") as mock_conn,
                patch(
                    "geoparquet_io.core.extract_bigquery._schema_rows",
                    side_effect=functools.partial(_spy, _reads=reads),
                ),
                patch(
                    "geoparquet_io.core.extract_bigquery._build_column_list",
                    side_effect=_Stop,
                ),
            ):
                mock_conn.return_value.__enter__.return_value = MagicMock()
                with pytest.raises(_Stop):
                    extract_bigquery(
                        table_id="project-name.dataset.table",
                        output_parquet=None,
                        **kwargs,
                    )

            assert reads == ["project-name.dataset.table"], kwargs


class TestPythonAPI:
    """Test the Python API for BigQuery."""

    def test_table_from_bigquery_exists(self):
        """Test that Table.from_bigquery method exists."""
        from geoparquet_io.api import Table

        assert hasattr(Table, "from_bigquery")
        assert callable(Table.from_bigquery)

    def test_ops_read_bigquery_exists(self):
        """Test that ops.read_bigquery function exists."""
        from geoparquet_io.api import ops

        assert hasattr(ops, "read_bigquery")
        assert callable(ops.read_bigquery)

    def test_read_bigquery_top_level_export(self):
        """Test that read_bigquery is exported at top level."""
        import geoparquet_io as gpio

        assert hasattr(gpio, "read_bigquery")
        assert callable(gpio.read_bigquery)


class TestBigQueryConnection:
    """Test BigQuery connection setup."""

    @patch("geoparquet_io.core.extract_bigquery.get_duckdb_connection")
    def test_connection_loads_extensions_in_order(self, mock_get_con):
        """Test that spatial is loaded before bigquery, and bigquery install uses try/except."""
        from geoparquet_io.core.extract_bigquery import _setup_bigquery_connection

        mock_con = MagicMock()
        mock_get_con.return_value = mock_con

        _setup_bigquery_connection()

        # Verify get_duckdb_connection was called with spatial=True
        mock_get_con.assert_called_once_with(load_spatial=True, load_httpfs=False)

        # Verify bigquery extension is loaded
        calls = [call[0][0] for call in mock_con.execute.call_args_list]
        load_bq_calls = [c for c in calls if "LOAD bigquery" in c]
        assert load_bq_calls, "LOAD bigquery should be called"

    @patch("geoparquet_io.core.extract_bigquery.get_duckdb_connection")
    def test_connection_no_deprecated_geography_setting(self, mock_get_con):
        """Test that deprecated bq_geography_as_geometry is NOT set (v1.5+)."""
        from geoparquet_io.core.extract_bigquery import _setup_bigquery_connection

        mock_con = MagicMock()
        mock_get_con.return_value = mock_con

        _setup_bigquery_connection()

        calls = [call[0][0] for call in mock_con.execute.call_args_list]
        geom_setting_calls = [c for c in calls if "geography_as_geometry" in c.lower()]
        assert not geom_setting_calls, (
            "bq_geography_as_geometry is deprecated in DuckDB 1.5 — should not be set"
        )

    @patch("geoparquet_io.core.extract_bigquery.get_duckdb_connection")
    def test_connection_sets_arrow_compression(self, mock_get_con):
        """Test that bq_arrow_compression is set for efficient data transfer."""
        from geoparquet_io.core.extract_bigquery import _setup_bigquery_connection

        mock_con = MagicMock()
        mock_get_con.return_value = mock_con

        _setup_bigquery_connection()

        calls = [call[0][0] for call in mock_con.execute.call_args_list]
        compression_calls = [c for c in calls if "bq_arrow_compression" in c.lower()]
        assert compression_calls, "bq_arrow_compression should be set for network efficiency"

    @patch("geoparquet_io.core.extract_bigquery.get_duckdb_connection")
    def test_connection_handles_install_race(self, mock_get_con):
        """Test that bigquery INSTALL failure (race condition) is handled gracefully."""
        from geoparquet_io.core.extract_bigquery import _setup_bigquery_connection

        mock_con = MagicMock()
        mock_get_con.return_value = mock_con

        # Simulate INSTALL failing (already installed by another worker)
        def execute_side_effect(sql):
            if "INSTALL" in sql and "bigquery" in sql.lower():
                raise duckdb.IOException("Extension already installed")
            return mock_con

        mock_con.execute.side_effect = execute_side_effect

        # Should not raise — INSTALL failure is caught internally
        _setup_bigquery_connection()

    @patch("geoparquet_io.core.extract_bigquery._setup_bigquery_connection")
    def test_credentials_file_validation(self, mock_setup):
        """Test that non-existent credentials file raises error."""
        from geoparquet_io.core.extract_bigquery import BigQueryConnection

        mock_setup.return_value = MagicMock()

        with pytest.raises(FileNotFoundError, match="Credentials file not found"):
            with BigQueryConnection(credentials_file="/nonexistent/path/credentials.json"):
                pass

    @patch("geoparquet_io.core.extract_bigquery._setup_bigquery_connection")
    def test_credentials_env_var_is_restored(self, mock_setup, tmp_path, monkeypatch):
        """The only credentials path must restore GOOGLE_APPLICATION_CREDENTIALS."""
        import os

        from geoparquet_io.core.extract_bigquery import BigQueryConnection

        mock_setup.return_value = MagicMock()
        creds = tmp_path / "sa.json"
        creds.write_text("{}")

        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        with BigQueryConnection(credentials_file=str(creds)):
            assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == str(creds)
        assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ

        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/host/creds.json")
        with BigQueryConnection(credentials_file=str(creds)):
            assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == str(creds)
        assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "/host/creds.json"

    def test_no_unrestored_credentials_helper(self):
        """The env-mutating helper with no restore must not exist any more."""
        import geoparquet_io.core.extract_bigquery as bq

        assert not hasattr(bq, "get_bigquery_connection"), (
            "get_bigquery_connection set GOOGLE_APPLICATION_CREDENTIALS without "
            "restoring it; BigQueryConnection is the only supported entry point"
        )

    @patch("geoparquet_io.core.extract_bigquery._setup_bigquery_connection")
    def test_context_manager_no_deprecated_geography_setting(self, mock_setup):
        """Test BigQueryConnection context manager doesn't set deprecated setting."""
        from geoparquet_io.core.extract_bigquery import BigQueryConnection

        mock_con = MagicMock()
        mock_setup.return_value = mock_con

        with BigQueryConnection() as _con:
            pass

        # _setup_bigquery_connection handles all DuckDB config;
        # BigQueryConnection should not add deprecated settings on top
        calls = [call[0][0] for call in mock_con.execute.call_args_list]
        geom_setting_calls = [c for c in calls if "geography_as_geometry" in c.lower()]
        assert not geom_setting_calls, (
            "BigQueryConnection should not set deprecated bq_geography_as_geometry in v1.5"
        )


def _wkb_point(x: float, y: float) -> bytes:
    """Little-endian WKB POINT, enough for the geometry branch to write."""
    import struct

    return struct.pack("<BIdd", 1, 1, x, y)


def _bq_result_table(num_rows: int) -> pa.Table:
    """One table both branches write, so a row-size estimate is identical.

    The geometry column is called ``geom`` -- a name
    ``detect_geometry_column_from_names`` matches -- on purpose: the
    geometry-less branch must stay plain Parquet even then.
    """
    return pa.table(
        {
            "id": pa.array(range(num_rows), pa.int64()),
            "name": pa.array([f"{i:0>100}" for i in range(num_rows)]),
            "geom": pa.array(
                [_wkb_point(i % 180 - 90, i % 90 - 45) for i in range(num_rows)], pa.binary()
            ),
        }
    )


def _extract_to(out: Path, *, table: pa.Table, with_geometry: bool, **kwargs) -> None:
    """Run ``extract_bigquery`` against a stubbed connection that returns ``table``.

    ``with_geometry`` only changes the type DuckDB reports for ``geom``, which
    is the single input that picks the write branch.
    """
    from geoparquet_io.core.extract_bigquery import extract_bigquery

    schema_rows = [
        ("id", "BIGINT"),
        ("name", "VARCHAR"),
        ("geom", "GEOMETRY" if with_geometry else "VARCHAR"),
    ]
    con = MagicMock()
    con.execute.return_value.arrow.return_value.read_all.return_value = table
    with (
        patch("geoparquet_io.core.extract_bigquery.BigQueryConnection") as mock_conn,
        patch("geoparquet_io.core.extract_bigquery._schema_rows", return_value=schema_rows),
    ):
        mock_conn.return_value.__enter__.return_value = con
        extract_bigquery(table_id="project-name.dataset.table", output_parquet=str(out), **kwargs)


def _row_group_rows(path: Path) -> list[int]:
    meta = pq.ParquetFile(path).metadata
    return [meta.row_group(i).num_rows for i in range(meta.num_row_groups)]


#: > DEFAULT_ROW_GROUP_ROWS, so the default is visible in the footer.
_FACADE_TEST_ROWS = 60_000


@pytest.fixture(scope="module")
def bq_result_table() -> pa.Table:
    return _bq_result_table(_FACADE_TEST_ROWS)


class TestRowGroupsMatchTheWriteFacade:
    """#1021: the geometry-less branch wrote one row group for the whole file.

    It called ``pq.write_table`` directly with a *byte* count in
    ``row_group_size`` (a row count), and with ``None`` when no flag was given
    -- so ``DEFAULT_ROW_GROUP_ROWS`` never applied. Same command, same flags,
    same input: five row groups with a geometry column and one without.
    """

    ROWS = _FACADE_TEST_ROWS

    def test_default_applies_without_a_flag(self, tmp_path, bq_result_table):
        """No row-group flag: the facade's 49,152, not pyarrow's own default."""
        from geoparquet_io.core.parquet_writer import DEFAULT_ROW_GROUP_ROWS

        out = tmp_path / "plain.parquet"
        _extract_to(out, table=bq_result_table, with_geometry=False)

        groups = _row_group_rows(out)
        assert groups == [DEFAULT_ROW_GROUP_ROWS, self.ROWS - DEFAULT_ROW_GROUP_ROWS]

    def test_mb_target_yields_several_groups(self, tmp_path, bq_result_table):
        """``--row-group-size-mb`` is a byte target, sized into rows -- not rows."""
        out = tmp_path / "plain-mb.parquet"
        _extract_to(out, table=bq_result_table, with_geometry=False, row_group_size_mb=1.0)

        groups = _row_group_rows(out)
        # 60,000 rows of ~110 bytes is ~6.5 MB; 1 MB groups cannot be one group.
        assert len(groups) > 1, groups
        assert max(groups) < self.ROWS

    @pytest.mark.parametrize(
        "flags",
        [
            pytest.param({}, id="no-flag"),
            pytest.param({"row_group_rows": 20_000}, id="row-count"),
            pytest.param({"row_group_size_mb": 1.0}, id="mb-target"),
            # A target below one writer vector's worth of rows: floors at 2,048
            # on both branches rather than writing seven-row groups.
            pytest.param({"row_group_size_mb": 0.001}, id="sub-vector-mb-target"),
        ],
    )
    def test_both_branches_agree(self, tmp_path, bq_result_table, flags):
        """One command, one flag, one input -- one row-group layout."""
        geo_out = tmp_path / "geo.parquet"
        plain_out = tmp_path / "plain.parquet"
        _extract_to(geo_out, table=bq_result_table, with_geometry=True, **flags)
        _extract_to(plain_out, table=bq_result_table, with_geometry=False, **flags)

        assert _row_group_rows(plain_out) == _row_group_rows(geo_out)

    def test_a_sub_vector_mb_target_floors_at_one_writer_vector(self, tmp_path, bq_result_table):
        """``--row-group-size-mb 1KB`` used to mean 8,572 seven-row groups."""
        from geoparquet_io.core.parquet_writer import WRITER_VECTOR_ROWS

        out = tmp_path / "plain-tiny-mb.parquet"
        _extract_to(out, table=bq_result_table, with_geometry=False, row_group_size_mb=0.001)

        groups = _row_group_rows(out)
        assert groups[0] == WRITER_VECTOR_ROWS
        assert min(groups[:-1]) == WRITER_VECTOR_ROWS

    @pytest.mark.parametrize("with_geometry", [True, False], ids=["geometry", "geometry-less"])
    def test_a_zero_mb_target_is_rejected_on_both_branches(
        self, tmp_path, bq_result_table, with_geometry
    ):
        """``0MB`` parses. It meant "given" to the facade and "absent" to the writer,
        and the geometry-less branch wrote one row group for the whole file --
        #1021 again, through the one value nobody had typed on purpose."""
        from geoparquet_io.core.exceptions import InvalidParameterError

        out = tmp_path / "never.parquet"
        with pytest.raises(InvalidParameterError, match="row-group-size-mb"):
            _extract_to(
                out, table=bq_result_table, with_geometry=with_geometry, row_group_size_mb=0.0
            )
        assert not out.exists()

    def test_an_explicit_row_count_is_aligned_on_both_branches(self, tmp_path, bq_result_table):
        """20,000 is not a size a row group can have; the facade says 20,480."""
        out = tmp_path / "plain-rows.parquet"
        _extract_to(out, table=bq_result_table, with_geometry=False, row_group_rows=20_000)

        assert _row_group_rows(out)[0] == 20_480

    def test_a_bad_row_count_is_rejected_rather_than_written(self, tmp_path, bq_result_table):
        """The facade's validation reaches this branch too."""
        from geoparquet_io.core.exceptions import InvalidParameterError

        out = tmp_path / "never.parquet"
        with pytest.raises(InvalidParameterError):
            _extract_to(out, table=bq_result_table, with_geometry=False, row_group_rows=0)

    def test_a_geom_named_column_stays_plain_parquet(self, tmp_path, bq_result_table):
        """Routing must not start auto-detecting geometry by column name.

        ``geom`` is in ``STANDARD_GEOMETRY_NAMES``, but DuckDB reported it as
        ``VARCHAR``: BigQuery said it is not geography, and the output must not
        claim otherwise.
        """
        out = tmp_path / "plain-geo-name.parquet"
        _extract_to(out, table=bq_result_table, with_geometry=False)

        assert b"geo" not in (pq.ParquetFile(out).schema_arrow.metadata or {})


class TestFacadeSizesAnMbTarget:
    """``resolve_row_group_rows_for_table`` -- one owner for MB -> rows."""

    def test_no_options_is_the_default(self):
        from geoparquet_io.core.parquet_writer import (
            DEFAULT_ROW_GROUP_ROWS,
            resolve_row_group_rows_for_table,
        )

        table = pa.table({"id": pa.array(range(10), pa.int64())})
        assert resolve_row_group_rows_for_table(table, None, None) == DEFAULT_ROW_GROUP_ROWS

    def test_an_mb_target_becomes_a_row_count(self):
        from geoparquet_io.core.parquet_writer import resolve_row_group_rows_for_table

        table = _bq_result_table(20_000)
        rows = resolve_row_group_rows_for_table(table, None, 1.0)
        assert rows is not None
        assert 1 <= rows < 20_000

    def test_an_mb_target_never_exceeds_the_table(self):
        from geoparquet_io.core.parquet_writer import resolve_row_group_rows_for_table

        table = _bq_result_table(100)
        assert resolve_row_group_rows_for_table(table, None, 1024.0) == 100

    def test_an_empty_table_keeps_the_mb_target_unsized(self):
        from geoparquet_io.core.parquet_writer import resolve_row_group_rows_for_table

        table = pa.table({"id": pa.array([], pa.int64())})
        assert resolve_row_group_rows_for_table(table, None, 1.0) is None

    def test_an_explicit_row_count_wins_over_an_mb_target(self):
        from geoparquet_io.core.parquet_writer import resolve_row_group_rows_for_table

        table = _bq_result_table(20_000)
        assert resolve_row_group_rows_for_table(table, 4_096, 1.0) == 4_096

    def test_a_sub_vector_mb_target_floors_at_one_writer_vector(self):
        from geoparquet_io.core.parquet_writer import (
            WRITER_VECTOR_ROWS,
            resolve_row_group_rows_for_table,
        )

        table = _bq_result_table(20_000)
        assert resolve_row_group_rows_for_table(table, None, 0.001) == WRITER_VECTOR_ROWS

    def test_the_floor_still_caps_at_the_table(self):
        from geoparquet_io.core.parquet_writer import resolve_row_group_rows_for_table

        table = _bq_result_table(100)
        assert resolve_row_group_rows_for_table(table, None, 0.001) == 100

    @pytest.mark.parametrize("mb", [0.0, -1.0])
    def test_a_non_positive_mb_target_is_rejected(self, mb):
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.parquet_writer import resolve_row_group_rows

        with pytest.raises(InvalidParameterError, match="--row-group-size-mb"):
            resolve_row_group_rows(None, mb)

    def test_the_mb_rejection_names_the_callers_own_parameter(self):
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.parquet_writer import resolve_row_group_rows

        with pytest.raises(InvalidParameterError, match="row_group_size_mb"):
            resolve_row_group_rows(None, 0.0, mb_param_name="row_group_size_mb")

    def test_estimate_row_size_falls_back_when_the_table_cannot_say(self):
        """A table-like object with neither buffer accessor gets 100 bytes/row."""
        from geoparquet_io.core.parquet_writer import estimate_row_size

        class _Opaque:
            num_rows = 10

        assert estimate_row_size(_Opaque()) == 100

    def test_estimate_row_size_survives_a_raising_accessor(self):
        from geoparquet_io.core.parquet_writer import estimate_row_size

        class _Broken:
            num_rows = 10

            def get_total_buffer_size(self):
                raise RuntimeError("no")

            @property
            def nbytes(self):
                raise RuntimeError("no")

        assert estimate_row_size(_Broken()) == 100


# Integration tests that require BigQuery access
@pytest.mark.network
class TestBigQueryIntegration:
    """Integration tests requiring BigQuery access.

    Set BQ_TEST_TABLE environment variable to run these tests:
    export BQ_TEST_TABLE=project.dataset.table
    """

    @pytest.fixture
    def bq_table_id(self):
        """Get BigQuery test table from environment."""
        table_id = os.environ.get("BQ_TEST_TABLE")
        if not table_id:
            pytest.skip("BQ_TEST_TABLE environment variable not set")
        return table_id

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_bq_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_extract_from_bigquery(self, bq_table_id, output_file):
        """Test extracting data from BigQuery to GeoParquet."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        extract_bigquery(
            table_id=bq_table_id,
            output_parquet=output_file,
            limit=10,
        )

        # Verify output file exists and is valid GeoParquet
        assert Path(output_file).exists()
        table = pq.read_table(output_file)
        assert table.num_rows == 10

    def test_extract_with_spherical_edges(self, bq_table_id, output_file):
        """Test that BigQuery output has spherical edges in metadata."""
        import json

        from geoparquet_io.core.extract_bigquery import extract_bigquery

        extract_bigquery(
            table_id=bq_table_id,
            output_parquet=output_file,
            limit=5,
        )

        # Check geo metadata for spherical edges
        pf = pq.ParquetFile(output_file)
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata, "Expected geo metadata in BigQuery-extracted Parquet"

        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        columns = geo_meta.get("columns", {})
        assert columns, "Expected at least one geometry column in geo metadata"

        # Verify all geometry columns have spherical edges
        for col_name, col_meta in columns.items():
            if "edges" in col_meta:
                assert col_meta["edges"] == "spherical", (
                    f"Expected spherical edges for column {col_name}, got {col_meta['edges']}"
                )

    def test_python_api_from_bigquery(self, bq_table_id):
        """Test Table.from_bigquery() method."""
        from geoparquet_io.api import Table

        table = Table.from_bigquery(bq_table_id, limit=5)
        assert isinstance(table, Table)
        assert table.num_rows == 5


def _inner_googlesql(sql: str) -> str:
    """Decode the GoogleSQL string literal out of a ``bigquery_query`` call.

    Scans for the second argument's quoted literal, honouring DuckDB's
    doubled-quote escape, because the call may be followed by an outer
    WHERE or LIMIT whose own parens and quotes would defeat a naive split.
    DuckDB does the final decoding, so the assertion is about the SQL
    BigQuery actually receives rather than how the f-string looked.
    """
    rest = sql.split("bigquery_query(", 1)[1].split(",", 1)[1]
    start = rest.index("'")
    i = start + 1
    while i < len(rest):
        if rest[i] == "'":
            if i + 1 < len(rest) and rest[i + 1] == "'":
                i += 2
                continue
            break
        i += 1
    literal = rest[start : i + 1]
    con = duckdb.connect()
    try:
        return con.execute(f"SELECT {literal}").fetchone()[0]
    finally:
        con.close()


class TestViewsRouteThroughBigQueryQuery:
    """A view or materialized view cannot be read with ``bigquery_scan``.

    The Storage Read API rejects it with "non-table entities cannot be read
    with the storage API". The same DuckDB extension ships ``bigquery_query``,
    which runs a real query job and reads a view fine, so the fix is to route
    the relation through that instead of refusing the source.
    """

    def test_view_query_uses_bigquery_query_not_scan(self):
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        sql = _build_bigquery_query(
            con=None,
            validated_table_id="my-project.ds.the_view",
            select_cols="*",
            bbox=None,
            bbox_mode="auto",
            bbox_threshold=1000,
            geom_col=None,
            where=None,
            limit=None,
            is_view=True,
        )
        assert "bigquery_scan(" not in sql
        assert "bigquery_query(" in sql

    def test_view_query_bills_the_project_from_the_table_id(self):
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        sql = _build_bigquery_query(
            con=None,
            validated_table_id="my-project.ds.the_view",
            select_cols="*",
            bbox=None,
            bbox_mode="auto",
            bbox_threshold=1000,
            geom_col=None,
            where=None,
            limit=None,
            is_view=True,
        )
        assert "bigquery_query('my-project'," in sql

    def test_view_query_selects_from_the_backtick_quoted_view(self):
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        sql = _build_bigquery_query(
            con=None,
            validated_table_id="my-project.ds.the_view",
            select_cols="*",
            bbox=None,
            bbox_mode="auto",
            bbox_threshold=1000,
            geom_col=None,
            where=None,
            limit=None,
            is_view=True,
        )
        assert _inner_googlesql(sql) == "SELECT * FROM `my-project.ds.the_view`"

    def test_server_side_bbox_becomes_a_googlesql_where_not_a_filter_arg(self):
        """``bigquery_query`` has no ``filter=``; the predicate goes in the SQL."""
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        sql = _build_bigquery_query(
            con=None,
            validated_table_id="my-project.ds.the_view",
            select_cols="*",
            bbox="0,0,10,10",
            bbox_mode="server",
            bbox_threshold=1000,
            geom_col="geog",
            where=None,
            limit=None,
            is_view=True,
        )
        assert "filter=" not in sql
        inner = _inner_googlesql(sql)
        assert inner.startswith("SELECT * FROM `my-project.ds.the_view` WHERE ")
        assert "ST_INTERSECTS(`geog`," in inner

    def test_hostile_column_cannot_break_out_of_the_googlesql_literal(self):
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        sql = _build_bigquery_query(
            con=None,
            validated_table_id="my-project.ds.the_view",
            select_cols="*",
            bbox="0,0,10,10",
            bbox_mode="server",
            bbox_threshold=1000,
            geom_col="x') OR TRUE --",
            where=None,
            limit=None,
            is_view=True,
        )
        assert len(duckdb.extract_statements(sql)) == 1

    def test_limit_and_where_still_apply_outside_the_view_relation(self):
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        sql = _build_bigquery_query(
            con=None,
            validated_table_id="my-project.ds.the_view",
            select_cols="*",
            bbox=None,
            bbox_mode="auto",
            bbox_threshold=1000,
            geom_col=None,
            where="cloud < 10",
            limit=7,
            is_view=True,
        )
        live = _strip_line_comments(sql)
        assert "LIMIT 7" in live
        assert "cloud < 10" in live

    def test_table_path_is_unchanged_by_the_view_parameter_default(self):
        """A physical table must still use bigquery_scan, byte for byte."""
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        kwargs = {
            "con": None,
            "validated_table_id": "my-project.ds.tbl",
            "select_cols": "*",
            "bbox": None,
            "bbox_mode": "auto",
            "bbox_threshold": 1000,
            "geom_col": None,
            "where": None,
            "limit": None,
        }
        assert _build_bigquery_query(**kwargs) == (
            "SELECT * FROM bigquery_scan('my-project.ds.tbl')"
        )


class TestViewBboxStrategy:
    """A view has no ``__TABLES__`` row count, and local filtering would be ruinous."""

    def test_auto_mode_on_a_view_filters_server_side(self):
        from geoparquet_io.core.extract_bigquery import _determine_bbox_strategy

        con = MagicMock()
        assert _determine_bbox_strategy(con, "p.d.v", "auto", 500_000, is_view=True) is True

    def test_explicit_local_still_wins_on_a_view(self):
        from geoparquet_io.core.extract_bigquery import _determine_bbox_strategy

        con = MagicMock()
        assert _determine_bbox_strategy(con, "p.d.v", "local", 500_000, is_view=True) is False


class TestViewFallsBackOnTheReadError:
    """Detection happens on the read, because the DESCRIBE does not fail.

    BigQuery answers ``DESCRIBE SELECT * FROM bigquery_scan('a.view')`` from
    table metadata without opening a Storage Read API session, so the schema
    comes back fine for a view and only the data read is refused. Anything
    that decided table-vs-view from the DESCRIBE would therefore never fire.
    Verified against a real materialized view and a real logical view: both
    describe and neither reads.
    """

    SCHEMA = [("id", "VARCHAR"), ("geom", "GEOMETRY")]
    NON_TABLE_ERROR = (
        "Binder Error: Error while creating read session: Permanent error, with a "
        "last message of request failed: non-table entities cannot be read with "
        "the storage API"
    )

    def _run(self, tmp_path, read_fails: bool):
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        table = pa.table({"id": pa.array(["a"]), "geom": pa.array([None], pa.binary())})
        issued: list[str] = []

        def execute(sql, *args, **kwargs):
            issued.append(str(sql))
            if read_fails and "bigquery_scan(" in str(sql) and "DESCRIBE" not in str(sql):
                raise duckdb.BinderException(self.NON_TABLE_ERROR)
            result = MagicMock()
            result.arrow.return_value.read_all.return_value = table
            return result

        con = MagicMock()
        con.execute.side_effect = execute
        with (
            patch("geoparquet_io.core.extract_bigquery.BigQueryConnection") as mock_conn,
            patch(
                "geoparquet_io.core.extract_bigquery._schema_rows",
                return_value=self.SCHEMA,
            ),
        ):
            mock_conn.return_value.__enter__.return_value = con
            extract_bigquery(
                table_id="my-project.ds.relation",
                output_parquet=str(tmp_path / "out.parquet"),
            )
        return issued

    def test_a_refused_read_is_retried_as_a_query(self, tmp_path):
        issued = self._run(tmp_path, read_fails=True)
        assert any("bigquery_query(" in q for q in issued), issued

    def test_the_retry_produces_the_output_file(self, tmp_path):
        self._run(tmp_path, read_fails=True)
        assert (tmp_path / "out.parquet").exists()

    def test_a_table_never_pays_for_a_second_attempt(self, tmp_path):
        issued = self._run(tmp_path, read_fails=False)
        assert any("bigquery_scan(" in q for q in issued)
        assert not any("bigquery_query(" in q for q in issued), issued

    def test_an_unrelated_read_error_is_not_retried(self, tmp_path):
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        con = MagicMock()
        con.execute.side_effect = duckdb.BinderException("Permission denied on relation")
        with (
            patch("geoparquet_io.core.extract_bigquery.BigQueryConnection") as mock_conn,
            patch(
                "geoparquet_io.core.extract_bigquery._schema_rows",
                return_value=self.SCHEMA,
            ),
        ):
            mock_conn.return_value.__enter__.return_value = con
            with pytest.raises(duckdb.Error, match="Permission denied"):
                extract_bigquery(
                    table_id="my-project.ds.relation",
                    output_parquet=str(tmp_path / "out.parquet"),
                )


class TestViewPushesFiltersIntoTheQueryJob:
    """On the view path --where and --limit must reach BigQuery, not DuckDB.

    ``bigquery_scan`` streams, so an outer DuckDB filter merely wastes
    bandwidth. A query job has to produce its whole result first, so the same
    outer filter asks BigQuery to materialise the entire relation: against a
    7.7-billion-row materialized view that returns "Response too large to
    return" rather than any rows. ``--where`` is documented as BigQuery SQL
    anyway, so BigQuery is where it belongs.
    """

    def _view_sql(self, **over):
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        kwargs = {
            "con": None,
            "validated_table_id": "my-project.ds.v",
            "select_cols": "*",
            "bbox": None,
            "bbox_mode": "auto",
            "bbox_threshold": 1000,
            "geom_col": None,
            "where": None,
            "limit": None,
            "is_view": True,
        }
        kwargs.update(over)
        return _build_bigquery_query(**kwargs)

    def test_where_lands_inside_the_googlesql(self):
        inner = _inner_googlesql(self._view_sql(where="acquired_at >= '2025-09-01'"))
        assert "WHERE" in inner
        assert "acquired_at >= '2025-09-01'" in inner

    def test_where_is_not_also_applied_outside(self):
        """Applying it twice is harmless for SQL but means it was not pushed."""
        sql = self._view_sql(where="cloud < 10")
        outer = sql.split(")", 1)[-1] if ")" in sql else sql
        assert "cloud < 10" not in outer.split("bigquery_query", 1)[0]

    def test_limit_lands_inside_the_googlesql(self):
        inner = _inner_googlesql(self._view_sql(limit=5))
        assert inner.rstrip().endswith("LIMIT 5")

    def test_bbox_and_where_combine_in_one_googlesql_where(self):
        inner = _inner_googlesql(
            self._view_sql(bbox="0,0,10,10", bbox_mode="server", geom_col="geog", where="a = 1")
        )
        assert "ST_INTERSECTS(`geog`," in inner
        assert "a = 1" in inner
        assert inner.count(" WHERE ") == 1

    def test_a_hostile_where_never_reaches_the_googlesql(self):
        """Counting DuckDB statements cannot see a GoogleSQL break-out, so the
        comment that would make one is refused before the job is built."""
        from geoparquet_io.core.exceptions import ValidationError

        with pytest.raises(ValidationError):
            self._view_sql(where="1=1) OR TRUE --")

    def test_the_table_path_still_filters_in_duckdb(self):
        """Unchanged behaviour for a physical table: no silent push-down."""
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        sql = _build_bigquery_query(
            con=None,
            validated_table_id="my-project.ds.t",
            select_cols="*",
            bbox=None,
            bbox_mode="auto",
            bbox_threshold=1000,
            geom_col=None,
            where="cloud < 10",
            limit=5,
        )
        live = _strip_line_comments(sql)
        assert "bigquery_scan('my-project.ds.t')" in live
        assert "cloud < 10" in live
        assert live.rstrip().endswith("LIMIT 5")


# Integration test that requires BigQuery access to a view
@pytest.mark.network
class TestBigQueryViewIntegration:
    """Set BQ_TEST_VIEW to a view or materialized view to run this.

    export BQ_TEST_VIEW=project.dataset.view

    Verified during development against a BigQuery materialized view of
    7.7 billion rows and against a logical view. Both describe through
    ``bigquery_scan`` and neither reads through it, which is the behaviour the
    fallback exists for.
    """

    @pytest.fixture
    def bq_view_id(self):
        view_id = os.environ.get("BQ_TEST_VIEW")
        if not view_id:
            pytest.skip("BQ_TEST_VIEW environment variable not set")
        return view_id

    @pytest.fixture
    def output_file(self):
        tmp_path = Path(tempfile.gettempdir()) / f"test_bq_view_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_a_view_extracts_to_parquet(self, bq_view_id, output_file):
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        extract_bigquery(table_id=bq_view_id, output_parquet=output_file, limit=10)

        assert Path(output_file).exists()
        assert pq.ParquetFile(output_file).metadata.num_rows == 10


class TestViewWithALocalBboxFilter:
    """--bbox-mode local on a view: the filter runs in DuckDB, so LIMIT
    cannot be pushed down with it.

    The inner query would otherwise cut candidates before the spatial filter
    saw them, returning fewer rows than asked for. This combination is the
    only path where the view relation carries an outer WHERE.
    """

    def _sql(self, limit):
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        return _build_bigquery_query(
            con=None,
            validated_table_id="my-project.ds.v",
            select_cols="*",
            bbox="0,0,10,10",
            bbox_mode="local",
            bbox_threshold=1000,
            geom_col="geom",
            where=None,
            limit=limit,
            is_view=True,
        )

    def test_the_local_filter_lands_outside_the_query_job(self):
        sql = self._sql(None)
        outer = sql.split("bigquery_query(", 1)[1]
        assert "ST_Intersects" in outer.rsplit(")", 1)[-1] or "WHERE" in sql
        assert "ST_Intersects" in sql

    def test_the_spatial_predicate_is_not_pushed_into_the_googlesql(self):
        inner = _inner_googlesql(self._sql(None))
        assert "ST_Intersects" not in inner
        assert "ST_INTERSECTS" not in inner

    def test_limit_stays_outside_when_a_local_filter_follows_it(self):
        sql = self._sql(7)
        inner = _inner_googlesql(sql)
        assert "LIMIT" not in inner, inner
        assert _strip_line_comments(sql).rstrip().endswith("LIMIT 7")

    def test_it_is_still_one_statement(self):
        assert len(duckdb.extract_statements(self._sql(7))) == 1


class TestBboxStrategyFallsThroughForATable:
    """The is_view shortcut must not swallow the table path."""

    def test_auto_mode_on_a_table_still_weighs_the_row_count(self):
        from geoparquet_io.core.extract_bigquery import _determine_bbox_strategy

        con = MagicMock()
        with patch(
            "geoparquet_io.core.extract_bigquery._get_table_row_count",
            return_value=1_000_000,
        ):
            assert _determine_bbox_strategy(con, "p.d.t", "auto", 500_000) is True
        with patch(
            "geoparquet_io.core.extract_bigquery._get_table_row_count",
            return_value=10,
        ):
            assert _determine_bbox_strategy(con, "p.d.t", "auto", 500_000) is False


class TestShowSqlDuringTheViewRetry:
    """--show-sql must print the query that actually ran, not only the one
    that was refused."""

    SCHEMA = [("id", "VARCHAR"), ("geom", "GEOMETRY")]
    NON_TABLE_ERROR = "Binder Error: non-table entities cannot be read with the storage API"

    def test_both_the_scan_and_the_retry_are_printed(self, tmp_path, monkeypatch):
        import geoparquet_io.core.extract_bigquery as bq
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        printed: list[str] = []
        monkeypatch.setattr(bq, "progress", printed.append)

        table = pa.table({"id": pa.array(["a"]), "geom": pa.array([None], pa.binary())})

        def execute(sql, *a, **k):
            if "bigquery_scan(" in str(sql) and "DESCRIBE" not in str(sql):
                raise duckdb.BinderException(self.NON_TABLE_ERROR)
            result = MagicMock()
            result.arrow.return_value.read_all.return_value = table
            return result

        con = MagicMock()
        con.execute.side_effect = execute
        with (
            patch.object(bq, "BigQueryConnection") as mock_conn,
            patch.object(bq, "_schema_rows", return_value=self.SCHEMA),
        ):
            mock_conn.return_value.__enter__.return_value = con
            extract_bigquery(
                table_id="my-project.ds.relation",
                output_parquet=str(tmp_path / "o.parquet"),
                show_sql=True,
            )

        sqls = [m for m in printed if m.startswith("SQL: ")]
        assert any("bigquery_scan(" in s for s in sqls), sqls
        assert any("bigquery_query(" in s for s in sqls), sqls


class TestViewQueryProjectsOnlyTheRequestedColumns:
    """A query job is billed by the columns it reads, so ``SELECT *`` over a
    view bills every column even when --include-cols asked for two. BigQuery
    prunes columns through a view only when the statement names them."""

    def _sql(self, **over):
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        kwargs = {
            "con": None,
            "validated_table_id": "my-project.ds.v",
            "select_cols": '"id", ST_AsWKB("geom") AS "geom"',
            "bbox": None,
            "bbox_mode": "auto",
            "bbox_threshold": 1000,
            "geom_col": "geom",
            "where": None,
            "limit": None,
            "is_view": True,
            "source_columns": ["id", "geom"],
        }
        kwargs.update(over)
        return _build_bigquery_query(**kwargs)

    def test_the_inner_statement_names_the_columns(self):
        assert _inner_googlesql(self._sql()) == "SELECT `id`, `geom` FROM `my-project.ds.v`"

    def test_all_columns_keeps_select_star(self):
        assert _inner_googlesql(self._sql(source_columns=None)) == (
            "SELECT * FROM `my-project.ds.v`"
        )

    def test_a_local_bbox_filter_keeps_its_geometry_column(self):
        """The outer DuckDB filter reads the geometry even when it is excluded."""
        inner = _inner_googlesql(
            self._sql(
                select_cols='"id"',
                source_columns=["id"],
                bbox="0,0,1,1",
                bbox_mode="local",
            )
        )
        assert inner == "SELECT `id`, `geom` FROM `my-project.ds.v`"

    def test_a_hostile_column_name_is_quoted_for_googlesql(self):
        sql = self._sql(select_cols='"a`b\'c"', source_columns=["a`b'c"], geom_col=None)
        assert len(duckdb.extract_statements(sql)) == 1
        assert _inner_googlesql(sql) == "SELECT `a\\`b'c` FROM `my-project.ds.v`"

    def test_the_table_path_ignores_source_columns(self):
        sql = self._sql(is_view=False, select_cols='"id"', source_columns=["id"], geom_col=None)
        assert sql == "SELECT \"id\" FROM bigquery_scan('my-project.ds.v')"


class TestViewWhereMustReadTheSameInGoogleSQL:
    """On a view --where runs inside BigQuery. Two DuckDB spellings parse
    there too but mean something else, silently: ``"col"`` is a STRING
    literal in GoogleSQL (so ``"name" = 'x'`` compares two constants and
    returns no rows), and a backslash in a string literal is an escape
    sequence. Refuse those rather than return a wrong answer."""

    def _sql(self, where, is_view=True):
        from geoparquet_io.core.extract_bigquery import _build_bigquery_query

        return _build_bigquery_query(
            con=None,
            validated_table_id="my-project.ds.v",
            select_cols="*",
            bbox=None,
            bbox_mode="auto",
            bbox_threshold=1000,
            geom_col=None,
            where=where,
            limit=None,
            is_view=is_view,
        )

    @pytest.mark.parametrize("where", ["\"name\" = 'x'", '"my col" IS NOT NULL'])
    def test_a_double_quoted_identifier_is_refused(self, where):
        from geoparquet_io.core.exceptions import ValidationError

        with pytest.raises(ValidationError, match="double-quoted name"):
            self._sql(where)

    @pytest.mark.parametrize(
        "where",
        [
            "name = 'it''s'",
            "name = " + "'" * 3 + "x" + "'" * 3,
            "a = 1 -- trailing",
            "a = 1 /* x */ AND b = 2",
            "/* lead */ a = 1",
            "a = 1 /* /* nested */ ) OR TRUE */",
        ],
        ids=[
            "doubled-quote",
            "triple-quote",
            "line-comment",
            "block-comment",
            "leading-comment",
            "nested-comment",
        ],
    )
    def test_a_lexically_ambiguous_clause_is_refused(self, where):
        """DuckDB and GoogleSQL disagree where these strings and comments end,
        so what DuckDB validated as data could be code to BigQuery."""
        from geoparquet_io.core.exceptions import ValidationError

        with pytest.raises(ValidationError, match="runs inside BigQuery"):
            self._sql(where)

    def test_a_backslash_in_a_string_literal_is_refused(self):
        from geoparquet_io.core.exceptions import ValidationError

        with pytest.raises(ValidationError, match="backslash"):
            self._sql("path = 'C:\\new'")

    @pytest.mark.parametrize(
        "where",
        [
            "name = 'x'",
            "pop >= 10 AND kind IN ('a', 'b')",
            "note = 'say \"hi\"'",
            "d >= '2025-09-01' AND (x IS NOT NULL OR y <> 3.5)",
        ],
    )
    def test_portable_predicates_still_push_down(self, where):
        assert where in _inner_googlesql(self._sql(where))

    def test_the_table_path_still_accepts_a_double_quoted_identifier(self):
        assert "\"name\" = 'x'" in self._sql("\"name\" = 'x'", is_view=False)


class TestViewFallbackTellsTheUserAboutBilling:
    SCHEMA = [("id", "VARCHAR"), ("geom", "GEOMETRY")]

    def test_the_fallback_is_announced(self, tmp_path, monkeypatch):
        import geoparquet_io.core.extract_bigquery as bq

        printed: list[str] = []
        monkeypatch.setattr(bq, "progress", printed.append)
        table = pa.table({"id": pa.array(["a"]), "geom": pa.array([None], pa.binary())})

        def execute(sql, *a, **k):
            if "bigquery_scan(" in str(sql) and "DESCRIBE" not in str(sql):
                raise duckdb.BinderException(
                    "non-table entities cannot be read with the storage API"
                )
            result = MagicMock()
            result.arrow.return_value.read_all.return_value = table
            return result

        con = MagicMock()
        con.execute.side_effect = execute
        with (
            patch.object(bq, "BigQueryConnection") as mock_conn,
            patch.object(bq, "_schema_rows", return_value=self.SCHEMA),
        ):
            mock_conn.return_value.__enter__.return_value = con
            bq.extract_bigquery(
                table_id="my-project.ds.relation",
                output_parquet=str(tmp_path / "o.parquet"),
                include_cols="id",
            )

        assert any("query job" in m and "billed" in m for m in printed), printed
        issued = [str(c.args[0]) for c in con.execute.call_args_list]
        job = next(q for q in issued if "bigquery_query(" in q)
        assert _inner_googlesql(job).startswith("SELECT `id`, `geom` FROM")


# --- gpio extract bigquery --sql -------------------------------------------
#
# --sql replaces the table source: the user's GoogleSQL becomes the relation,
# run through the same bigquery_query() machinery a view goes through. The
# point of the flag is everything --where cannot express -- a window function,
# a join, a GROUP BY -- so there is no attempt to parse or rewrite it.

_WINDOW_SQL = (
    "SELECT * EXCEPT(rn) FROM ("
    "SELECT *, ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY version DESC) AS rn "
    "FROM `p.d.v` WHERE item_type = 'PSScene') WHERE rn = 1"
)


def _relation_googlesql(sql: str) -> str:
    """The GoogleSQL DuckDB will hand BigQuery, decoded by DuckDB itself."""
    return _inner_googlesql(sql)


class TestBigQueryQueryRelation:
    """``_bigquery_query_relation`` is the one place GoogleSQL is wrapped.

    The view path (PR #1) and the --sql path both end up calling it, so the
    escaping contract -- escape exactly once, at the point of interpolation --
    is stated and tested in exactly one place.
    """

    def test_it_builds_a_bigquery_query_relation(self):
        from geoparquet_io.core.extract_bigquery import _bigquery_query_relation

        assert _bigquery_query_relation("my-project", "SELECT 1") == (
            "bigquery_query('my-project', 'SELECT 1')"
        )

    def test_the_googlesql_survives_the_round_trip_verbatim(self):
        """Escaped once: what BigQuery receives is what the user typed."""
        from geoparquet_io.core.extract_bigquery import _bigquery_query_relation

        relation = _bigquery_query_relation("my-project", _WINDOW_SQL)
        assert _relation_googlesql(f"SELECT * FROM {relation}") == _WINDOW_SQL

    def test_an_apostrophe_in_the_user_sql_is_escaped_exactly_once(self):
        from geoparquet_io.core.extract_bigquery import _bigquery_query_relation

        user_sql = "SELECT * FROM `p.d.t` WHERE name = 'O''Brien'"
        relation = _bigquery_query_relation("my-project", user_sql)
        assert _relation_googlesql(f"SELECT * FROM {relation}") == user_sql

    def test_the_view_relation_still_delegates_to_it_unchanged(self):
        """PR #1's output is byte-for-byte what it was before the generalization."""
        from geoparquet_io.core.extract_bigquery import _bigquery_view_relation

        assert _bigquery_view_relation("my-project.ds.v", ["a = 1"], 5) == (
            "bigquery_query('my-project', 'SELECT * FROM `my-project.ds.v` WHERE a = 1 LIMIT 5')"
        )


class TestHostileSqlCannotBreakTheDuckDbWrapper:
    """The goal is statement containment, not SQL safety.

    --sql exists so a user can run arbitrary BigQuery SQL; refusing arbitrary
    BigQuery SQL would refuse the feature. What must hold is that the value
    cannot escape the DuckDB string literal it travels in and become a second
    *DuckDB* statement -- which is where it could reach COPY TO, ATTACH, or
    the local filesystem. Mirrors
    ``test_hostile_column_cannot_break_out_of_the_googlesql_literal``.
    """

    HOSTILE = [
        "SELECT 1'); COPY (SELECT 1) TO 'file.csv'; --",
        "SELECT 1' , 'x') ; ATTACH 'evil.db' ; SELECT * FROM bigquery_query('p",
        "SELECT 1; DROP TABLE t",
        "SELECT '\\'' ; SELECT 2",
        "'''; SELECT 1; --",
    ]

    @pytest.mark.parametrize("hostile", HOSTILE)
    def test_a_hostile_sql_stays_one_duckdb_statement(self, hostile):
        from geoparquet_io.core.extract_bigquery import _bigquery_query_relation

        sql = f"SELECT * FROM {_bigquery_query_relation('my-project', hostile)}"
        assert len(duckdb.extract_statements(sql)) == 1

    @pytest.mark.parametrize("hostile", HOSTILE)
    def test_the_hostile_text_arrives_at_bigquery_intact(self, hostile):
        """Contained, not mangled: BigQuery gets exactly what was typed."""
        from geoparquet_io.core.extract_bigquery import _bigquery_query_relation

        sql = f"SELECT * FROM {_bigquery_query_relation('my-project', hostile)}"
        assert _relation_googlesql(sql) == hostile


class TestSqlReplacesTheTableSource:
    """The core takes ``sql=`` instead of a table id, and never scans."""

    def _run(self, tmp_path, geom_type="GEOMETRY", **kwargs):
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        table = pa.table({"id": pa.array(["a"]), "geom": pa.array([None], pa.binary())})
        issued: list[str] = []

        def execute(sql, *args, **kw):
            issued.append(str(sql))
            result = MagicMock()
            result.arrow.return_value.read_all.return_value = table
            return result

        con = MagicMock()
        con.execute.side_effect = execute
        with (
            patch("geoparquet_io.core.extract_bigquery.BigQueryConnection") as mock_conn,
            patch(
                "geoparquet_io.core.extract_bigquery._schema_rows",
                return_value=[("id", "VARCHAR"), ("geom", geom_type)],
            ),
        ):
            mock_conn.return_value.__enter__.return_value = con
            extract_bigquery(
                sql=_WINDOW_SQL,
                project="my-project",
                output_parquet=str(tmp_path / "out.parquet"),
                **kwargs,
            )
        return issued

    def test_the_read_goes_through_bigquery_query(self, tmp_path):
        issued = self._run(tmp_path)
        assert any("bigquery_query(" in q for q in issued), issued

    def test_nothing_is_ever_scanned(self, tmp_path):
        """No bigquery_scan: there is no table id to scan, and no fallback."""
        issued = self._run(tmp_path)
        assert not any("bigquery_scan(" in q for q in issued), issued

    def test_the_user_sql_reaches_bigquery_verbatim(self, tmp_path):
        issued = self._run(tmp_path)
        read = next(q for q in issued if "bigquery_query(" in q)
        assert _relation_googlesql(read) == _WINDOW_SQL

    def test_it_writes_the_output_file(self, tmp_path):
        self._run(tmp_path)
        assert (tmp_path / "out.parquet").exists()

    def test_the_schema_is_described_from_the_user_sql(self, tmp_path):
        """``_schema_rows`` must describe the query, not a table."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        seen = {}

        def fake_schema_rows(con, table_id=None, table_source="bigquery", relation=None):
            seen["table_id"] = table_id
            seen["relation"] = relation
            return [("id", "VARCHAR")]

        con = MagicMock()
        con.execute.return_value.arrow.return_value.read_all.return_value = pa.table(
            {"id": pa.array(["a"])}
        )
        with (
            patch("geoparquet_io.core.extract_bigquery.BigQueryConnection") as mock_conn,
            patch("geoparquet_io.core.extract_bigquery._schema_rows", fake_schema_rows),
        ):
            mock_conn.return_value.__enter__.return_value = con
            extract_bigquery(
                sql="SELECT 1 AS id",
                project="my-project",
                output_parquet=str(tmp_path / "out.parquet"),
            )
        assert seen["table_id"] is None
        assert "bigquery_query('my-project'" in (seen["relation"] or "")

    def test_a_native_geography_column_is_still_converted(self, tmp_path):
        issued = self._run(tmp_path, geography_column="geom")
        read = next(q for q in issued if "bigquery_query(" in q)
        assert 'ST_AsWKB("geom")' in read

    def test_a_varchar_geometry_column_is_still_parsed(self, tmp_path):
        """--geography-column/--geometry-format work exactly as on a table."""
        issued = self._run(
            tmp_path, geom_type="VARCHAR", geography_column="geom", geometry_format="geojson"
        )
        read = next(q for q in issued if "bigquery_query(" in q)
        assert "ST_AsWKB(ST_GeomFromGeoJSON(" in read

    def test_the_table_path_is_untouched(self, tmp_path):
        """Regression: no --sql means the Storage Read API path, unchanged."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        issued: list[str] = []
        table = pa.table({"id": pa.array(["a"])})

        def execute(sql, *args, **kw):
            issued.append(str(sql))
            result = MagicMock()
            result.arrow.return_value.read_all.return_value = table
            return result

        con = MagicMock()
        con.execute.side_effect = execute
        with (
            patch("geoparquet_io.core.extract_bigquery.BigQueryConnection") as mock_conn,
            patch(
                "geoparquet_io.core.extract_bigquery._schema_rows",
                return_value=[("id", "VARCHAR")],
            ),
        ):
            mock_conn.return_value.__enter__.return_value = con
            extract_bigquery(
                table_id="my-project.ds.t", output_parquet=str(tmp_path / "out.parquet")
            )
        assert any("bigquery_scan('my-project.ds.t')" in q for q in issued), issued
        assert not any("bigquery_query(" in q for q in issued), issued


class TestSqlSourceValidation:
    """--sql needs a billing project, and refuses a table id."""

    def test_sql_without_a_project_is_refused(self):
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        with pytest.raises(InvalidParameterError, match="requires"):
            extract_bigquery(sql="SELECT 1", dry_run=True)

    def test_neither_a_table_id_nor_sql_is_refused(self):
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        with pytest.raises(InvalidParameterError, match="TABLE_ID"):
            extract_bigquery(dry_run=True)

    def test_a_table_id_and_sql_together_are_refused(self):
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        with pytest.raises(InvalidParameterError, match="replaces the table source"):
            extract_bigquery(table_id="p.d.t", sql="SELECT 1", project="p", dry_run=True)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"include_cols": "a,b"},
            {"exclude_cols": "a"},
            {"where": "a = 1"},
            {"bbox": "0,0,1,1"},
            {"bbox_mode": "server"},
            {"bbox_threshold": 10},
            {"limit": 5},
        ],
    )
    def test_a_query_shaping_argument_alongside_sql_is_refused(self, kwargs):
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        with pytest.raises(InvalidParameterError, match="express columns, filters and limits"):
            extract_bigquery(sql="SELECT 1", project="my-project", dry_run=True, **kwargs)

    def test_an_output_shaping_argument_alongside_sql_is_fine(self):
        """--geography-column and friends shape the file, not the query."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        assert (
            extract_bigquery(
                sql="SELECT 1",
                project="my-project",
                geography_column="geom",
                geometry_format="geojson",
                edges="planar",
                dry_run=True,
            )
            is None
        )


class TestSqlDryRun:
    """--dry-run prints the statement without connecting to anything."""

    def test_it_prints_the_bigquery_query_relation(self, caplog):
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        with caplog.at_level("INFO"):
            extract_bigquery(sql=_WINDOW_SQL, project="my-project", dry_run=True)
        assert "bigquery_query('my-project'" in caplog.text

    def test_it_prints_the_user_sql_verbatim(self, caplog):
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        with caplog.at_level("INFO"):
            extract_bigquery(sql=_WINDOW_SQL, project="my-project", dry_run=True)
        printed = next(line for line in caplog.text.splitlines() if "bigquery_query(" in line)
        assert _relation_googlesql(printed.split("SQL: ", 1)[1]) == _WINDOW_SQL

    @patch("geoparquet_io.core.extract_bigquery._setup_bigquery_connection")
    def test_it_never_connects(self, mock_setup):
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        mock_setup.side_effect = AssertionError("dry run connected to BigQuery")
        extract_bigquery(sql="SELECT 1", project="my-project", dry_run=True)
        mock_setup.assert_not_called()


class TestSqlCommandLine:
    """The CLI wiring: argument arity, and the mutual-exclusion rule."""

    def _invoke(self, args):
        return CliRunner().invoke(extract, ["bigquery", *args])

    def _invoke_captured(self, args):
        """Invoke with the core patched out; returns (result, call kwargs)."""
        with patch("geoparquet_io.core.extract_bigquery.extract_bigquery") as mock:
            mock.return_value = None
            result = self._invoke(args)
        return result, (mock.call_args.kwargs if mock.call_args else None)

    def test_a_lone_positional_after_sql_is_the_output_not_the_table_id(self, tmp_path):
        """The arity trap: Click binds positionals left to right.

        ``gpio extract bigquery --sql "..." out.parquet`` must write
        out.parquet, not try to read a BigQuery table called "out.parquet".
        """
        out = str(tmp_path / "out.parquet")
        result, kwargs = self._invoke_captured(
            ["--sql", "SELECT 1", "--project", "my-project", out]
        )
        assert result.exit_code == 0, result.output
        assert kwargs["output_parquet"] == out
        assert kwargs["table_id"] is None
        assert kwargs["sql"] == "SELECT 1"

    def test_a_table_id_and_an_output_still_bind_in_order(self, tmp_path):
        """Regression: the table path's two positionals are unchanged."""
        out = str(tmp_path / "out.parquet")
        result, kwargs = self._invoke_captured(["my-project.ds.t", out])
        assert result.exit_code == 0, result.output
        assert kwargs["table_id"] == "my-project.ds.t"
        assert kwargs["output_parquet"] == out
        assert kwargs["sql"] is None

    def test_two_positionals_with_sql_are_refused(self, tmp_path):
        out = str(tmp_path / "out.parquet")
        result = self._invoke(["--sql", "SELECT 1", "--project", "my-project", "p.d.t", out])
        assert result.exit_code != 0
        assert "replaces the table source" in result.output

    def test_no_table_id_and_no_sql_is_refused(self):
        """Both positionals are optional now, so "nothing at all" needs a message.

        A single positional and no --sql is still a TABLE_ID, not an output.
        """
        result = self._invoke([])
        assert result.exit_code != 0
        assert "TABLE_ID" in result.output

    def test_sql_without_project_is_refused(self, tmp_path):
        result = self._invoke(["--sql", "SELECT 1", str(tmp_path / "out.parquet")])
        assert result.exit_code != 0
        assert "--project" in result.output

    # Every query-shaping flag, including the two with non-None defaults:
    # --bbox-mode and --bbox-threshold have to be detected with
    # ctx.get_parameter_source, because "auto" and 500000 are what an
    # untouched command line also carries.
    @pytest.mark.parametrize(
        "flag",
        [
            ["--include-cols", "a,b"],
            ["--exclude-cols", "a"],
            ["--where", "a = 1"],
            ["--bbox", "0,0,1,1"],
            ["--bbox-mode", "server"],
            ["--bbox-mode", "auto"],
            ["--bbox-threshold", "10"],
            ["--bbox-threshold", "500000"],
            ["--limit", "5"],
        ],
    )
    def test_a_query_shaping_flag_with_sql_is_refused(self, flag, tmp_path):
        result = self._invoke(
            [
                "--sql",
                "SELECT 1",
                "--project",
                "my-project",
                str(tmp_path / "out.parquet"),
                *flag,
            ]
        )
        assert result.exit_code != 0, result.output
        assert "express columns, filters and limits in the SQL itself" in result.output

    @pytest.mark.parametrize(
        "flag",
        [
            ["--geography-column", "geom"],
            ["--geometry-format", "geojson"],
            ["--edges", "planar"],
            ["--compression", "snappy"],
            ["--row-group-size", "1000"],
            ["--geoparquet-version", "1.1"],
            ["--no-repair-geometry"],
            ["--overwrite"],
            ["--write-memory", "1GB"],
            ["--show-sql"],
            ["-v"],
        ],
    )
    def test_an_output_shaping_flag_with_sql_is_accepted(self, flag, tmp_path):
        result, _ = self._invoke_captured(
            [
                "--sql",
                "SELECT 1",
                "--project",
                "my-project",
                str(tmp_path / "out.parquet"),
                *flag,
            ]
        )
        assert result.exit_code == 0, result.output

    def test_the_help_documents_the_flag(self):
        result = self._invoke(["--help"])
        assert "--sql" in result.output

    def test_dry_run_prints_the_statement(self, tmp_path):
        result = self._invoke(
            [
                "--sql",
                "SELECT 1 AS id",
                "--project",
                "my-project",
                str(tmp_path / "out.parquet"),
                "--dry-run",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "bigquery_query('my-project'" in result.output


class TestSqlPythonApi:
    """Every CLI option needs a Python API path (CLAUDE.md)."""

    def test_read_bigquery_takes_sql(self):
        import inspect

        from geoparquet_io.api import ops

        params = inspect.signature(ops.read_bigquery).parameters
        assert "sql" in params
        assert params["table_id"].default is None

    def test_from_bigquery_takes_sql(self):
        import inspect

        from geoparquet_io.api.table import Table

        params = inspect.signature(Table.from_bigquery).parameters
        assert "sql" in params
        assert params["table_id"].default is None

    def test_read_bigquery_passes_the_sql_through(self):
        from geoparquet_io.api import ops

        with patch("geoparquet_io.core.extract_bigquery.extract_bigquery") as mock:
            mock.return_value = pa.table({"id": [1]})
            ops.read_bigquery(sql="SELECT 1 AS id", project="my-project")
        assert mock.call_args.kwargs["sql"] == "SELECT 1 AS id"
        assert mock.call_args.kwargs["table_id"] is None

    def test_from_bigquery_passes_the_sql_through(self):
        from geoparquet_io.api.table import Table

        with patch("geoparquet_io.core.extract_bigquery.extract_bigquery") as mock:
            mock.return_value = pa.table({"id": [1]})
            table = Table.from_bigquery(sql="SELECT 1 AS id", project="my-project")
        assert mock.call_args.kwargs["sql"] == "SELECT 1 AS id"
        assert table.num_rows == 1


# Integration test that requires BigQuery access
@pytest.mark.network
class TestBigQuerySqlIntegration:
    """Set BQ_TEST_SQL_PROJECT to a project you can bill a query job to.

    export BQ_TEST_SQL_PROJECT=my-project

    The statement is a window function over a public dataset, which is the
    case --where cannot express and so the reason this flag exists.
    """

    @pytest.fixture
    def bq_project(self):
        project = os.environ.get("BQ_TEST_SQL_PROJECT")
        if not project:
            pytest.skip("BQ_TEST_SQL_PROJECT environment variable not set")
        return project

    @pytest.fixture
    def output_file(self):
        tmp_path = Path(tempfile.gettempdir()) / f"test_bq_sql_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_a_window_function_extracts_to_geoparquet(self, bq_project, output_file):
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        extract_bigquery(
            sql=(
                "SELECT * EXCEPT(rn) FROM ("
                "SELECT zip_code, zip_code_geom, "
                "ROW_NUMBER() OVER (ORDER BY zip_code) AS rn "
                "FROM `bigquery-public-data.geo_us_boundaries.zip_codes`"
                ") WHERE rn <= 5"
            ),
            project=bq_project,
            output_parquet=output_file,
            geography_column="zip_code_geom",
        )

        assert Path(output_file).exists()
        meta = pq.ParquetFile(output_file).metadata
        assert meta.num_rows == 5
        assert b"geo" in meta.metadata


class TestSqlFalsyQueryShapingValues:
    """A query-shaping argument given as a falsy value is still given."""

    @pytest.mark.parametrize("kwargs", [{"limit": 0}, {"include_cols": ""}, {"where": ""}])
    def test_a_falsy_value_alongside_sql_is_refused(self, kwargs):
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        with pytest.raises(InvalidParameterError, match="express columns, filters and limits"):
            extract_bigquery(sql="SELECT 1", project="my-project", dry_run=True, **kwargs)


class TestSqlMustBeOneReadOnlyQuery:
    """``extract`` reads. A DML/DDL statement, or a script that smuggles one
    in after a ``;``, would run in BigQuery -- during the DESCRIBE that
    reads the schema, before gpio sees a row -- so it is refused up front."""

    @pytest.mark.parametrize(
        "sql",
        [
            "DELETE FROM `p.d.t` WHERE TRUE",
            "drop table `p.d.t`",
            "INSERT INTO `p.d.t` SELECT * FROM `p.d.u`",
            "CREATE OR REPLACE TABLE `p.d.t` AS SELECT 1 AS x",
            "MERGE `p.d.t` USING `p.d.u` ON TRUE WHEN MATCHED THEN DELETE",
            "SELECT 1; DELETE FROM `p.d.t` WHERE TRUE",
            "SELECT 1 /* ; */ ; DROP TABLE `p.d.t`",
            "-- leading comment\nTRUNCATE TABLE `p.d.t`",
        ],
        ids=[
            "delete",
            "drop",
            "insert",
            "create",
            "merge",
            "script",
            "script-after-comment",
            "commented-truncate",
        ],
    )
    def test_a_statement_that_is_not_one_query_is_refused(self, sql):
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        with pytest.raises(InvalidParameterError, match="single SELECT"):
            extract_bigquery(sql=sql, project="my-project", dry_run=True)

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1",
            "  select * from `p.d.t`;  ",
            "WITH a AS (SELECT 1 AS x) SELECT * FROM a",
            "(SELECT 1) UNION ALL (SELECT 2)",
            "/* why */ -- note\n# hash comment\nSELECT ';' AS semi, \"; DROP\" AS s2",
            "SELECT '''a ; b''' AS t, r'\\'; x' AS r, `odd;name` FROM `p.d.t`",
            "SELECT * FROM `p.d.t` WHERE note = 'it\\'s; fine'",
        ],
        ids=["plain", "trailing-semicolon", "with", "paren", "comments", "literals", "escape"],
    )
    def test_a_single_query_passes(self, sql):
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        assert extract_bigquery(sql=sql, project="my-project", dry_run=True) is None


class TestEmptySqlIsNotAbsentSql:
    """``--sql ""`` was read as "no --sql", so with a TABLE_ID it silently read
    the table instead, and with one positional it parsed the output path as a
    table id. An empty statement is a statement, and not a query."""

    def test_cli_empty_sql_with_a_table_id_is_refused(self, tmp_path):
        with patch("geoparquet_io.core.extract_bigquery.BigQueryConnection") as conn:
            result = CliRunner().invoke(
                extract,
                [
                    "bigquery",
                    "--sql",
                    "",
                    "--project",
                    "my-project",
                    "my-project.d.t",
                    str(tmp_path / "out.parquet"),
                ],
            )
        assert result.exit_code != 0
        assert not conn.called
        assert not (tmp_path / "out.parquet").exists()

    def test_cli_empty_sql_with_one_positional_names_the_sql(self, tmp_path):
        with patch("geoparquet_io.core.extract_bigquery.BigQueryConnection") as conn:
            result = CliRunner().invoke(
                extract,
                ["bigquery", "--sql", "", "--project", "my-project", str(tmp_path / "o.parquet")],
            )
        assert result.exit_code != 0
        assert not conn.called
        assert "single SELECT" in result.output

    def test_api_empty_sql_is_refused(self):
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        with pytest.raises(InvalidParameterError, match="single SELECT"):
            extract_bigquery(sql="", project="my-project", dry_run=True)


class TestSqlErrorsSpeakTheApisNames:
    """A Python caller passed ``columns=``, not ``--include-cols``."""

    def test_the_exclusivity_error_names_the_api_argument(self):
        from geoparquet_io.api import ops
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match=r"Given with sql: columns, limit\."):
            ops.read_bigquery(sql="SELECT 1", project="my-project", columns=["x"], limit=5)

    def test_a_bad_bbox_mode_does_not_report_table_none(self):
        from geoparquet_io.api import ops

        with pytest.raises(ValueError) as exc:
            ops.read_bigquery(sql="SELECT 1", project="my-project", bbox_mode="bogus")
        assert "'None'" not in str(exc.value)
