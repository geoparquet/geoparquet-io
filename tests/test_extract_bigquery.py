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

        # Two: the bigquery_scan branch and the local-table branch.
        assert helper_source.count("DESCRIBE SELECT") == 2
        assert module_source.count("DESCRIBE SELECT") == 2

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

        def _spy(con, table_id, table_source="bigquery", *, _reads):
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
