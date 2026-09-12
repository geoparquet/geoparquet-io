"""Tests for Carto SQL API extractor."""

import http.server
import json
import tempfile
import threading
from pathlib import Path
from unittest import mock
from urllib.parse import quote

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import carto as carto_module
from geoparquet_io.core.carto import (
    CartoError,
    _build_carto_count_query,
    _build_carto_query,
    _column_names_from_fields,
    _create_empty_geoparquet_table,
    _detect_geometry_column,
    _detect_table_shape,
    _fatal_status_error,
    _fetch_with_retry,
    _geometry_column_from_fields,
    _status_from_duckdb_error,
    _validate_carto_url,
    _validate_table_name,
    carto_to_table,
)
from geoparquet_io.core.common import InvalidParameterError


class TestValidateCartoUrl:
    """Tests for URL validation."""

    def test_valid_full_url(self):
        """Full SQL API URL passes validation."""
        url = _validate_carto_url("https://phl.carto.com/api/v2/sql")
        assert url == "https://phl.carto.com/api/v2/sql"

    def test_valid_v1_url(self):
        """V1 API URL is also valid."""
        url = _validate_carto_url("https://example.carto.com/api/v1/sql")
        assert url == "https://example.carto.com/api/v1/sql"

    def test_base_domain_gets_api_path(self):
        """Base domain gets /api/v2/sql appended."""
        url = _validate_carto_url("https://phl.carto.com")
        assert url == "https://phl.carto.com/api/v2/sql"

    def test_trailing_slash_removed(self):
        """Trailing slashes are stripped."""
        url = _validate_carto_url("https://phl.carto.com/api/v2/sql/")
        assert url == "https://phl.carto.com/api/v2/sql"

    def test_missing_scheme_raises(self):
        """URL without scheme raises error."""
        with pytest.raises(InvalidParameterError, match="http://"):
            _validate_carto_url("phl.carto.com/api/v2/sql")

    def test_non_http_scheme_raises(self):
        """Non-http(s) schemes (e.g. file://) are rejected."""
        with pytest.raises(InvalidParameterError, match="http://"):
            _validate_carto_url("file:///etc/passwd/api/v2/sql")

    def test_invalid_path_raises(self):
        """Invalid path raises error."""
        with pytest.raises(InvalidParameterError, match="Invalid Carto SQL API URL"):
            _validate_carto_url("https://phl.carto.com/some/other/path")


class TestValidateTableName:
    """Tests for table name validation (SQL injection protection)."""

    def test_valid_simple_name(self):
        """Simple table name passes validation."""
        assert _validate_table_name("my_table") == "my_table"

    def test_valid_schema_qualified(self):
        """Schema-qualified name passes validation."""
        assert _validate_table_name("public.my_table") == "public.my_table"

    def test_valid_with_numbers(self):
        """Table name with numbers passes."""
        assert _validate_table_name("table_123") == "table_123"

    def test_invalid_sql_injection_semicolon(self):
        """Table name with semicolon is rejected."""
        with pytest.raises(InvalidParameterError, match="Invalid table name"):
            _validate_table_name("users; DROP TABLE users--")

    def test_invalid_sql_injection_quotes(self):
        """Table name with quotes is rejected."""
        with pytest.raises(InvalidParameterError, match="Invalid table name"):
            _validate_table_name("users' OR '1'='1")

    def test_invalid_spaces(self):
        """Table name with spaces is rejected."""
        with pytest.raises(InvalidParameterError, match="Invalid table name"):
            _validate_table_name("my table")

    def test_invalid_special_chars(self):
        """Table name with special characters is rejected."""
        with pytest.raises(InvalidParameterError, match="Invalid table name"):
            _validate_table_name("my-table")

    def test_invalid_starts_with_number(self):
        """Table name starting with number is rejected."""
        with pytest.raises(InvalidParameterError, match="Invalid table name"):
            _validate_table_name("123_table")


class TestBuildCartoQuery:
    """Tests for SQL query building."""

    def test_simple_query(self):
        """Basic query with just table name."""
        sql = _build_carto_query("my_table")
        # Table name should be quoted
        assert 'FROM "my_table"' in sql
        assert "SELECT *" in sql

    def test_with_columns(self):
        """Query with column selection."""
        sql = _build_carto_query("my_table", columns=["id", "name"])
        # Column names should be quoted
        assert '"id"' in sql
        assert '"name"' in sql
        assert '"the_geom"' in sql
        assert 'FROM "my_table"' in sql

    def test_columns_include_geom(self):
        """the_geom is not duplicated if already in columns."""
        sql = _build_carto_query("my_table", columns=["id", "the_geom"])
        # Should only have one the_geom
        assert sql.count('"the_geom"') == 1

    def test_with_where(self):
        """Query with WHERE clause."""
        sql = _build_carto_query("my_table", where="status = 'active'")
        # The closing paren sits on its own line so a trailing '--' cannot eat it.
        assert "WHERE (status = 'active'\n)" in sql

    def test_with_bbox(self):
        """Query with bounding box filter."""
        sql = _build_carto_query("my_table", bbox=(-75.2, 39.9, -75.1, 40.0))
        assert "ST_Intersects" in sql
        assert '"the_geom"' in sql
        assert "ST_MakeEnvelope(-75.2, 39.9, -75.1, 40.0, 4326)" in sql

    def test_with_limit(self):
        """Query with LIMIT clause."""
        sql = _build_carto_query("my_table", limit=100)
        assert "LIMIT 100" in sql

    def test_with_limit_zero(self):
        """Query with LIMIT 0 (edge case - should not be falsy)."""
        sql = _build_carto_query("my_table", limit=0)
        assert "LIMIT 0" in sql

    def test_combined_filters(self):
        """Query with WHERE, bbox, and limit."""
        sql = _build_carto_query(
            "my_table",
            where="status = 'active'",
            bbox=(-75.2, 39.9, -75.1, 40.0),
            limit=100,
        )
        assert "WHERE (status = 'active'\n) AND ST_Intersects" in sql
        assert "LIMIT 100" in sql

    def test_no_geom_does_not_force_the_geom(self):
        """Tabular query (include_geom=False) does not append the_geom."""
        sql = _build_carto_query("my_table", columns=["id", "name"], include_geom=False)
        assert '"id"' in sql
        assert '"name"' in sql
        assert '"the_geom"' not in sql

    def test_no_geom_ignores_bbox(self):
        """Tabular query (include_geom=False) ignores the bbox spatial filter."""
        sql = _build_carto_query(
            "my_table",
            bbox=(-75.2, 39.9, -75.1, 40.0),
            include_geom=False,
        )
        assert "ST_Intersects" not in sql
        assert "ST_MakeEnvelope" not in sql

    def test_no_geom_honors_where_and_limit(self):
        """Tabular query still applies where/limit filters."""
        sql = _build_carto_query(
            "my_table",
            where="pop > 1000",
            limit=50,
            include_geom=False,
        )
        assert "WHERE (pop > 1000\n)" in sql
        assert "LIMIT 50" in sql


class TestGeometryColumnFromFields:
    """Tests for the pure Carto fields-schema geometry parser."""

    def test_detects_the_geom(self):
        """A field with type 'geometry' is detected."""
        fields = {
            "cartodb_id": {"type": "number"},
            "the_geom": {"type": "geometry"},
            "name": {"type": "string"},
        }
        assert _geometry_column_from_fields(fields) == "the_geom"

    def test_no_geometry_returns_none(self):
        """Schema with only scalar columns returns None."""
        fields = {
            "id": {"type": "number"},
            "name": {"type": "string"},
            "value": {"type": "number"},
        }
        assert _geometry_column_from_fields(fields) is None

    def test_webmercator_geometry_detected(self):
        """A webmercator geometry column still counts as geometry."""
        fields = {"the_geom_webmercator": {"type": "geometry"}}
        assert _geometry_column_from_fields(fields) == "the_geom_webmercator"

    def test_empty_returns_none(self):
        """Empty fields dict returns None."""
        assert _geometry_column_from_fields({}) is None

    def test_malformed_returns_none(self):
        """Non-dict input returns None instead of raising."""
        assert _geometry_column_from_fields(None) is None
        assert _geometry_column_from_fields("not a dict") is None
        assert _geometry_column_from_fields({"x": "not a dict"}) is None


class TestColumnNamesFromFields:
    """The other half of the same ``fields`` block: the column names (#980).

    ``--include-cols`` is checked against these, so the probe that decides
    geometry-vs-tabular pays for both answers with one request.
    """

    def test_names_come_back_in_order(self):
        fields = {
            "cartodb_id": {"type": "number"},
            "Owner": {"type": "string"},
            "the_geom": {"type": "geometry"},
        }
        assert _column_names_from_fields(fields) == ["cartodb_id", "Owner", "the_geom"]

    def test_empty_block_is_an_empty_list_not_none(self):
        """An empty schema is "no columns", which is not "no schema"."""
        assert _column_names_from_fields({}) == []

    @pytest.mark.parametrize("fields", [None, "not a dict", 42, []])
    def test_malformed_block_is_none(self, fields):
        """None means "nothing to check against", so the caller skips the check."""
        assert _column_names_from_fields(fields) is None


class TestDetectTableShape:
    """The probe, with the network mocked: both answers, one request each step."""

    @staticmethod
    def _payloads(schema_response, has_values=True):
        def _respond(url, sql, *args, **kwargs):
            if "IS NOT NULL" in sql:
                return {"rows": [{"has_geom": 1}] if has_values else []}
            if isinstance(schema_response, Exception):
                raise schema_response
            return schema_response

        return _respond

    def _patch(self, responder):
        return mock.patch.object(carto_module, "_carto_sql_json", side_effect=responder)

    def test_populated_geometry_column(self):
        fields = {"cartodb_id": {"type": "number"}, "the_geom": {"type": "geometry"}}
        with self._patch(self._payloads({"fields": fields, "rows": []})):
            assert _detect_table_shape("https://x.carto.com/api/v2/sql", "tbl") == (
                True,
                ["cartodb_id", "the_geom"],
            )

    def test_no_geometry_column_is_tabular_and_still_yields_the_schema(self):
        fields = {"id": {"type": "number"}, "name": {"type": "string"}}
        with self._patch(self._payloads({"fields": fields, "rows": []})):
            assert _detect_table_shape("https://x.carto.com/api/v2/sql", "tbl") == (
                False,
                ["id", "name"],
            )

    def test_all_null_geometry_is_tabular_and_still_yields_the_schema(self):
        fields = {"id": {"type": "number"}, "the_geom": {"type": "geometry"}}
        with self._patch(self._payloads({"fields": fields, "rows": []}, has_values=False)):
            assert _detect_table_shape("https://x.carto.com/api/v2/sql", "tbl") == (
                False,
                ["id", "the_geom"],
            )

    @pytest.mark.parametrize(
        "payload",
        [{"rows": []}, {"fields": None, "rows": []}, {"fields": "garbage"}],
        ids=["absent", "null", "not-a-mapping"],
    )
    def test_a_malformed_schema_block_decides_exactly_as_it_did_before(self, payload):
        """A *succeeded* probe that carries no usable schema is still tabular.

        ``_table_has_geometry`` returned False for all three of these on main,
        because ``_geometry_column_from_fields`` returns None for a non-mapping
        and "no geometry-typed column" means plain extraction. Only a *failed
        request* has ever fallen back to assuming geometry. Splitting the
        column names out of the same block must not move that line, so it is
        asserted rather than argued.
        """
        with self._patch(self._payloads(payload)):
            assert _detect_table_shape("https://x.carto.com/api/v2/sql", "tbl") == (False, None)

    def test_a_failed_schema_probe_assumes_geometry_and_reports_no_schema(self):
        with self._patch(self._payloads(CartoError("boom"))):
            assert _detect_table_shape("https://x.carto.com/api/v2/sql", "tbl") == (True, None)

    def test_a_failed_values_probe_assumes_geometry_but_keeps_the_schema(self):
        """Step 2 failing does not throw away what step 1 already read."""
        fields = {"id": {"type": "number"}, "the_geom": {"type": "geometry"}}

        def _respond(url, sql, *args, **kwargs):
            if "IS NOT NULL" in sql:
                raise CartoError("boom")
            return {"fields": fields, "rows": []}

        with self._patch(_respond):
            assert _detect_table_shape("https://x.carto.com/api/v2/sql", "tbl") == (
                True,
                ["id", "the_geom"],
            )

    def test_detect_geometry_column_reads_the_same_probe(self):
        fields = {"id": {"type": "number"}, "the_geom": {"type": "geometry"}}
        with self._patch(self._payloads({"fields": fields, "rows": []})) as probe:
            assert _detect_geometry_column("https://x.carto.com/api/v2/sql", "tbl") == "the_geom"
        assert probe.call_count == 1


class TestEmptyGeoparquetTable:
    """Tests for empty table creation with proper metadata."""

    def test_empty_table_has_geometry_column(self):
        """Empty table has geometry column."""
        table = _create_empty_geoparquet_table()
        assert "geometry" in table.column_names
        assert table.num_rows == 0

    def test_empty_table_has_geo_metadata(self):
        """Empty table has valid GeoParquet metadata."""
        table = _create_empty_geoparquet_table()
        assert b"geo" in table.schema.metadata

        geo_meta = json.loads(table.schema.metadata[b"geo"])
        assert geo_meta["version"] == "1.1.0"
        assert geo_meta["primary_column"] == "geometry"
        assert "geometry" in geo_meta["columns"]

    def test_empty_table_has_crs(self):
        """Empty table has CRS metadata."""
        table = _create_empty_geoparquet_table()
        geo_meta = json.loads(table.schema.metadata[b"geo"])

        crs = geo_meta["columns"]["geometry"]["crs"]
        assert crs is not None
        # Should be OGC:CRS84
        assert crs["id"]["authority"] == "OGC"
        assert crs["id"]["code"] == "CRS84"

    def test_empty_table_respects_version(self):
        """Empty table uses specified GeoParquet version."""
        table = _create_empty_geoparquet_table(geoparquet_version="1.0.0")
        geo_meta = json.loads(table.schema.metadata[b"geo"])
        assert geo_meta["version"] == "1.0.0"


class TestWhereClauseValidation:
    """--where must be routed through validate_where_clause (gpio #612 parity).

    A statement-separator injection must be rejected before any network call.
    """

    INJECTION_WHERE = "1=1); COPY (SELECT 1) TO 's3://attacker/x'; --"

    def test_rejects_semicolon_injection_before_network(self, monkeypatch):
        """A ';' statement separator in --where is rejected before any request."""
        import geoparquet_io.core.carto as carto_module
        from geoparquet_io.core.exceptions import ValidationError

        def _fail_urlopen(*_args, **_kwargs):
            raise AssertionError("network request attempted before where-clause validation")

        monkeypatch.setattr(carto_module.urllib.request, "urlopen", _fail_urlopen)

        with pytest.raises(ValidationError, match="not a single filtering expression"):
            carto_to_table(
                url="https://phl.carto.com/api/v2/sql",
                table_name="opa_properties_public",
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
    def test_allows_legitimate_where(self, monkeypatch, clause):
        """A legitimate --where does not raise and reaches the fetch stage."""
        import pyarrow as pa

        import geoparquet_io.core.carto as carto_module

        dummy_table = pa.table({"x": [1]})
        monkeypatch.setattr(
            carto_module, "_carto_plain_table", lambda *_args, **_kwargs: dummy_table
        )

        result = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            where=clause,
            geometry=False,
        )
        assert result is dummy_table


def _strip_line_comments(sql: str) -> str:
    """Drop everything a ``--`` comment would swallow, line by line."""
    return "\n".join(line.split("--")[0] for line in sql.splitlines())


class TestWhereClauseCannotSwallowLaterClauses:
    """A trailing ``--`` in --where must not comment out the rest of the query."""

    TRAILING_COMMENT_WHERE = "1=1) --"

    def test_build_carto_query_keeps_bbox_and_limit(self):
        sql = _build_carto_query(
            "t",
            where=self.TRAILING_COMMENT_WHERE,
            bbox=(0, 0, 1, 1),
            limit=10,
        )
        live_sql = _strip_line_comments(sql)
        assert "ST_Intersects" in live_sql
        assert "LIMIT 10" in live_sql

    def test_build_carto_count_query_keeps_bbox(self):
        sql = _build_carto_count_query("t", where=self.TRAILING_COMMENT_WHERE, bbox=(0, 0, 1, 1))
        assert "ST_Intersects" in _strip_line_comments(sql)


@pytest.mark.network
class TestCartoToTable:
    """Integration tests for Carto extraction (requires network)."""

    def test_basic_extraction(self):
        """Extract a small sample from Philadelphia Carto."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            limit=10,
        )
        assert table.num_rows == 10
        assert "geometry" in table.column_names
        # Verify the_geom was renamed to geometry
        assert "the_geom" not in table.column_names

    def test_output_has_geoparquet_metadata(self):
        """Extracted table has valid GeoParquet metadata."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            limit=5,
        )
        assert b"geo" in table.schema.metadata

        geo_meta = json.loads(table.schema.metadata[b"geo"])
        assert geo_meta["version"] == "1.1.0"
        assert geo_meta["primary_column"] == "geometry"

    def test_output_has_correct_crs(self):
        """Extracted table has OGC:CRS84 CRS."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            limit=5,
        )
        geo_meta = json.loads(table.schema.metadata[b"geo"])
        crs = geo_meta["columns"]["geometry"]["crs"]

        assert crs["id"]["authority"] == "OGC"
        assert crs["id"]["code"] == "CRS84"

    def test_output_has_wkb_encoding(self):
        """Extracted table uses WKB encoding."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            limit=5,
        )
        geo_meta = json.loads(table.schema.metadata[b"geo"])
        assert geo_meta["columns"]["geometry"]["encoding"] == "WKB"

    def test_with_where_filter(self):
        """Extract with WHERE filter."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            where="category_code_description = 'SINGLE FAMILY'",
            limit=5,
        )
        assert table.num_rows == 5

    def test_with_bbox_filter(self):
        """Extract with bounding box filter."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            bbox=(-75.18, 39.95, -75.15, 39.97),
            limit=10,
        )
        assert table.num_rows <= 10
        assert "geometry" in table.column_names

    def test_include_cols(self):
        """Extract with column selection."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            include_cols="cartodb_id,parcel_number,market_value",
            limit=5,
        )
        # Should have requested columns plus geometry
        assert "cartodb_id" in table.column_names
        assert "parcel_number" in table.column_names
        assert "market_value" in table.column_names
        assert "geometry" in table.column_names

    def test_exclude_cols(self):
        """Extract with column exclusion."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            exclude_cols="cartodb_id",
            limit=5,
        )
        assert "cartodb_id" not in table.column_names
        assert "geometry" in table.column_names

    def test_base_domain_url(self):
        """URL without /api/v2/sql works."""
        table = carto_to_table(
            url="https://phl.carto.com",
            table_name="opa_properties_public",
            limit=5,
        )
        assert table.num_rows == 5

    def test_invalid_table_raises(self):
        """Non-existent table raises CartoError."""
        with pytest.raises(CartoError):
            carto_to_table(
                url="https://phl.carto.com/api/v2/sql",
                table_name="nonexistent_table_12345",
                limit=1,
            )

    def test_sql_injection_table_name_rejected(self):
        """SQL injection in table name is rejected before request."""
        with pytest.raises(InvalidParameterError, match="Invalid table name"):
            carto_to_table(
                url="https://phl.carto.com/api/v2/sql",
                table_name="users; DROP TABLE opa_properties_public--",
                limit=1,
            )

    def test_exclude_cols_cannot_drop_geometry(self):
        """Excluding geometry column is prevented."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            exclude_cols="geometry,cartodb_id",
            limit=5,
        )
        # Geometry should still be present (cannot be excluded)
        assert "geometry" in table.column_names
        # Other columns should be excluded
        assert "cartodb_id" not in table.column_names

    def test_detect_geometry_column_on_geo_table(self):
        """Detection probe finds the geometry column on a spatial table."""
        geom_col = _detect_geometry_column(
            "https://phl.carto.com/api/v2/sql",
            "opa_properties_public",
        )
        assert geom_col is not None

    def test_table_has_geometry_true_for_spatial(self):
        """A populated spatial table is detected as geometry."""
        has_geometry, columns = _detect_table_shape(
            "https://phl.carto.com/api/v2/sql", "opa_properties_public"
        )
        assert has_geometry is True
        # The same probe carries the schema, which --include-cols is checked
        # against without a second request (#980).
        assert columns is not None
        assert "the_geom" in columns

    def test_table_has_geometry_false_for_tabular(self):
        """A tabular table whose Carto the_geom is all-NULL is detected as plain.

        Carto attaches an empty the_geom (type=geometry) to managed tabular
        tables, so schema inspection alone is insufficient; the non-NULL probe
        must classify hr_pay_range (0 non-null geometries) as plain.
        """
        has_geometry, columns = _detect_table_shape(
            "https://phl.carto.com/api/v2/sql", "hr_pay_range"
        )
        assert has_geometry is False
        assert columns

    def test_autodetect_tabular_returns_plain_table(self):
        """Auto-detect (geometry=None) on a tabular table yields no geo metadata."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="hr_pay_range",
        )
        assert not (table.schema.metadata and b"geo" in table.schema.metadata)
        assert "geometry" not in table.column_names

    def test_forced_no_geometry_returns_plain_table(self):
        """geometry=False extracts as a plain table with no geo metadata."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            geometry=False,
            limit=5,
        )
        # Plain/tabular output carries no GeoParquet metadata.
        assert not (table.schema.metadata and b"geo" in table.schema.metadata)
        # No geom→geometry rename happens on the plain path.
        assert "geometry" not in table.column_names

    def test_forced_no_geometry_honors_include_cols(self):
        """Plain extraction respects include_cols without forcing the_geom."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            geometry=False,
            include_cols="cartodb_id,parcel_number",
            limit=5,
        )
        assert "cartodb_id" in table.column_names
        assert "parcel_number" in table.column_names
        assert "the_geom" not in table.column_names


@pytest.mark.network
class TestCartoCli:
    """CLI integration tests for Carto extraction."""

    def test_cli_basic_extraction(self):
        """Basic CLI extraction works."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "output.parquet"
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "carto",
                    "https://phl.carto.com/api/v2/sql",
                    "opa_properties_public",
                    str(output_file),
                    "--limit",
                    "5",
                    "--skip-hilbert",
                    "--skip-bbox",
                ],
            )
            assert result.exit_code == 0, result.output
            assert output_file.exists()

    def test_cli_with_filters(self):
        """CLI with --where and --bbox filters."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "output.parquet"
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "carto",
                    "https://phl.carto.com/api/v2/sql",
                    "opa_properties_public",
                    str(output_file),
                    "--where",
                    "category_code_description = 'SINGLE FAMILY'",
                    "--bbox",
                    "-75.2,39.9,-75.1,40.0",
                    "--limit",
                    "5",
                    "--skip-hilbert",
                    "--skip-bbox",
                ],
            )
            assert result.exit_code == 0, result.output

    def test_cli_no_geometry_writes_plain_parquet(self):
        """--no-geometry writes plain Parquet with no 'geo' metadata key."""
        import pyarrow.parquet as pq

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "plain.parquet"
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "carto",
                    "https://phl.carto.com/api/v2/sql",
                    "opa_properties_public",
                    str(output_file),
                    "--no-geometry",
                    "--limit",
                    "5",
                ],
            )
            assert result.exit_code == 0, result.output
            assert output_file.exists()
            metadata = pq.read_table(str(output_file)).schema.metadata or {}
            assert b"geo" not in metadata

    def test_cli_autodetect_tabular_writes_plain_parquet(self):
        """Auto-detect (no flags) on a real tabular table writes plain Parquet.

        This is the issue #508 reprex: a geometry-less Carto table is extracted
        to plain Parquet with no 'geo' metadata key.
        """
        import pyarrow.parquet as pq

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "tabular.parquet"
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "carto",
                    "https://phl.carto.com/api/v2/sql",
                    "hr_pay_range",
                    str(output_file),
                ],
            )
            assert result.exit_code == 0, result.output
            assert output_file.exists()
            metadata = pq.read_table(str(output_file)).schema.metadata or {}
            assert b"geo" not in metadata

    def test_cli_invalid_bbox_format(self):
        """CLI rejects invalid bbox format."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "output.parquet"
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "carto",
                    "https://phl.carto.com/api/v2/sql",
                    "opa_properties_public",
                    str(output_file),
                    "--bbox",
                    "invalid,bbox",
                ],
            )
            assert result.exit_code != 0
            assert "Invalid bbox format" in result.output

    def test_cli_timeout_option(self):
        """CLI accepts --timeout option."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "output.parquet"
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "carto",
                    "https://phl.carto.com/api/v2/sql",
                    "opa_properties_public",
                    str(output_file),
                    "--limit",
                    "5",
                    "--timeout",
                    "60",
                    "--skip-hilbert",
                    "--skip-bbox",
                ],
            )
            assert result.exit_code == 0, result.output

    def test_cli_help(self):
        """CLI help shows all options."""
        runner = CliRunner()
        result = runner.invoke(cli, ["extract", "carto", "--help"])
        assert result.exit_code == 0
        assert "--where" in result.output
        assert "--bbox" in result.output
        assert "--limit" in result.output
        assert "--timeout" in result.output
        assert "--include-cols" in result.output
        assert "--exclude-cols" in result.output
        assert "--geometry" in result.output
        assert "--no-geometry" in result.output
        # Note: --aws-profile is hidden (global option) so not in help
        assert "CARTO_API_KEY" in result.output

    def test_cli_invalid_table_name(self):
        """CLI rejects invalid table names."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "output.parquet"
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "carto",
                    "https://phl.carto.com/api/v2/sql",
                    "users; DROP TABLE x--",
                    str(output_file),
                ],
            )
            assert result.exit_code != 0
            assert "Invalid table name" in result.output


class TestCartoReadExpressionEscaping:
    """core/carto.py: `_fetch_with_retry`'s read expression (#936).

    The CSV branch already wrapped the URL with ``sql_path``; the GeoJSON
    branch one line above hand-rolled ``f'ST_Read("{full_url}")'``. DuckDB
    tolerates a double-quoted string in that position, so the bug was silent
    until a URL contained a ``"`` -- which ``_validate_carto_url`` does not
    reject -- at which point the quoting broke apart. ``--url`` is
    operator-supplied, so this is primarily a correctness bug, but it is the
    same "quote at the boundary, exactly once" rule as everywhere else, and a
    URL that arrives from config or automation makes it an injection.
    """

    # Passes _validate_carto_url (https scheme, /api/v2/sql suffix) and
    # carries the one character the old spelling could not survive.
    HOSTILE_URL = 'https://ex"ample.carto.com/api/v2/sql'

    def _captured_read_sql(self, monkeypatch, fmt):
        """Run _fetch_with_retry against a connection that only records SQL."""
        from geoparquet_io.core import carto as carto_module

        seen: list[str] = []

        class _RecordingConnection:
            def execute(self, sql, *args, **kwargs):
                seen.append(sql)
                if sql.lstrip().upper().startswith("SELECT"):
                    raise RuntimeError("the read failed; only the SQL matters here")
                return self

        monkeypatch.setattr(
            carto_module, "get_duckdb_connection", lambda *a, **k: _RecordingConnection()
        )
        # max_retries=1 keeps this to one attempt. The failure classification is
        # not what is under test here, and since #1020 it no longer takes its cue
        # from the exception message.
        with pytest.raises(CartoError):
            carto_module._fetch_with_retry(
                url=self.HOSTILE_URL,
                table_name="t",
                sql="SELECT * FROM t",
                fmt=fmt,
                max_retries=1,
            )
        return next(s for s in seen if s.lstrip().upper().startswith("SELECT"))

    @pytest.mark.parametrize(("fmt", "reader"), [("GeoJSON", "ST_Read"), ("csv", "read_csv_auto")])
    def test_url_becomes_one_well_formed_string_literal(self, monkeypatch, fmt, reader):
        from urllib.parse import quote

        from geoparquet_io.core.duckdb_utils import sql_path

        sql = self._captured_read_sql(monkeypatch, fmt)

        fmt_param = "GeoJSON" if fmt == "GeoJSON" else "csv"
        full_url = f"{self.HOSTILE_URL}?q={quote('SELECT * FROM t')}&format={fmt_param}"
        assert sql == f"SELECT * FROM {reader}({sql_path(full_url)})"

    def test_geojson_expression_parses(self, monkeypatch):
        """The generated statement must be parseable SQL, not two half-tokens."""
        import duckdb

        sql = self._captured_read_sql(monkeypatch, "GeoJSON")

        con = duckdb.connect()
        try:
            # json_serialize_sql parses without binding or opening anything.
            (payload,) = con.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()
        finally:
            con.close()
        assert '"error":true' not in payload.replace(" ", ""), payload


# =============================================================================
# #1020: failures are classified by HTTP status, never by the exception message
# =============================================================================


class _AttemptCountingConnection:
    """A DuckDB connection stand-in that counts fetches and fails them all.

    Only the ``SELECT * FROM ST_Read(...)`` / ``read_csv_auto(...)`` statement
    is counted; the ``SET`` statements around it are not attempts.
    """

    def __init__(self, error: Exception):
        self.error = error
        self.attempts = 0

    def execute(self, sql, *args, **kwargs):
        if sql.lstrip().upper().startswith("SELECT"):
            self.attempts += 1
            raise self.error
        return self


@pytest.fixture
def status_http_server():
    """Serve loopback HTTP with a caller-chosen status code.

    Yields ``(base_url, state)``; set ``state["code"]`` to pick the status the
    next request gets. Loopback only, so this is not a ``network`` test.
    """
    state = {"code": 404}

    class _Handler(http.server.BaseHTTPRequestHandler):
        def _respond(self, body=b""):
            self.send_response(state["code"])
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

        def do_GET(self):  # noqa: N802 - http.server API
            self._respond(b'{"error": ["boom"]}')

        def do_HEAD(self):  # noqa: N802 - http.server API
            self._respond()

        def log_message(self, *args):  # pragma: no cover - silence stderr noise
            pass

    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}/api/v2/sql", state
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


class TestStatusFromDuckdbError:
    """``duckdb.HTTPException.status_code`` is used only when it is a real status."""

    def test_a_plain_exception_carries_no_status(self):
        assert _status_from_duckdb_error(RuntimeError("IO Error: 404 not found")) is None

    def test_a_real_status_is_read(self):
        exc = RuntimeError("boom")
        exc.status_code = 503
        assert _status_from_duckdb_error(exc) == 503

    def test_zero_is_not_a_status(self):
        """DuckDB reports status_code 0 when the request produced no status."""
        exc = RuntimeError("boom")
        exc.status_code = 0
        assert _status_from_duckdb_error(exc) is None

    def test_a_non_integer_status_is_ignored(self):
        exc = RuntimeError("boom")
        exc.status_code = "404"
        assert _status_from_duckdb_error(exc) is None


class TestFatalStatusError:
    """Which statuses are fatal, and which are the retryable class."""

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (404, "not found"),
            (401, "Unauthorized"),
            (403, "forbidden"),
            (400, "HTTP 400"),
            (418, "HTTP 418"),
        ],
    )
    def test_client_errors_are_fatal(self, status, expected):
        error = _fatal_status_error(status, "my_table")
        assert error is not None
        assert expected in str(error)
        assert "my_table" in str(error)

    @pytest.mark.parametrize("status", [None, 200, 429, 500, 502, 503, 504])
    def test_transient_and_absent_statuses_are_retryable(self, status):
        """No status at all -- connection refused, DNS -- is the retryable class."""
        assert _fatal_status_error(status, "my_table") is None


class TestFailureClassificationIgnoresTheUsersSql:
    """#1020: digits in the user's own SQL must not decide the verdict.

    Every row here is the same underlying failure -- connection refused against
    a dead port, the canonical retryable class -- and differs only in the SQL,
    which the old classifier saw because the request URL is embedded in the
    DuckDB exception message.
    """

    DEAD_URL = "http://127.0.0.1:1/api/v2/sql"

    @pytest.mark.parametrize(
        ("sql", "note"),
        [
            ("SELECT * FROM census_blocks LIMIT 10", "plain SQL, no magic digits"),
            ("SELECT * FROM census_blocks LIMIT 404", "user typed --limit 404"),
            ("SELECT * FROM parcels WHERE zone = '404'", "'404' inside a WHERE value"),
            ("SELECT * FROM room_401_sensors LIMIT 10", "'401' inside the table name"),
            ("SELECT * FROM logs WHERE msg = 'not found'", "the words, inside a WHERE value"),
            ("SELECT * FROM unauthorized_events LIMIT 10", "'unauthorized' in the table name"),
        ],
    )
    def test_the_sql_in_the_message_never_decides_the_verdict(self, monkeypatch, sql, note):
        """The classifier's old input, verbatim, over the issue's whole table.

        The exception carries the real ``ST_Read`` failure text, URL and all, so
        the digits the old classifier matched on are present exactly as they
        were. The port is still dead, so the status probe answers "no status" --
        the retryable class -- for every row.
        """
        gdal_message = (
            f"IO Error: Could not open GDAL dataset at: "
            f"{self.DEAD_URL}?q={quote(sql)}&format=GeoJSON"
        )
        conn = _AttemptCountingConnection(RuntimeError(gdal_message))
        monkeypatch.setattr(carto_module, "get_duckdb_connection", lambda *a, **k: conn)

        with pytest.raises(CartoError) as excinfo:
            _fetch_with_retry(
                url=self.DEAD_URL,
                table_name="t",
                sql=sql,
                max_retries=3,
                retry_delay=0.01,
                timeout=2,
            )
        message = str(excinfo.value)

        assert conn.attempts == 3, f"{note}: retries were lost"
        assert "Failed to fetch data from Carto after 3 attempts" in message, note
        assert "not found" not in message, note
        assert "Unauthorized" not in message, note
        assert "forbidden" not in message, note

    def test_a_real_dead_port_fetch_retries_end_to_end(self, monkeypatch):
        """The issue's reproduction, with DuckDB and GDAL actually in the loop.

        ``--limit 404`` used to cost every retry and report "Table 't' not
        found" for what was only a connection failure.
        """
        real_get_conn = carto_module.get_duckdb_connection
        seen = {"attempts": 0}

        class _Counting:
            def __init__(self, inner):
                self._inner = inner

            def execute(self, sql_text, *args, **kwargs):
                if sql_text.lstrip().upper().startswith("SELECT"):
                    seen["attempts"] += 1
                return self._inner.execute(sql_text, *args, **kwargs)

        monkeypatch.setattr(
            carto_module, "get_duckdb_connection", lambda *a, **k: _Counting(real_get_conn())
        )

        with pytest.raises(CartoError) as excinfo:
            _fetch_with_retry(
                url=self.DEAD_URL,
                table_name="t",
                sql="SELECT * FROM census_blocks LIMIT 404",
                max_retries=3,
                retry_delay=0.01,
                timeout=2,
            )

        assert seen["attempts"] == 3
        assert "Failed to fetch data from Carto after 3 attempts" in str(excinfo.value)
        assert "not found" not in str(excinfo.value)


class TestFailureClassificationUsesTheHttpStatus:
    """A real server's status decides the verdict, against a mocked server."""

    @pytest.mark.parametrize(
        ("status", "expected"),
        [(404, "not found"), (401, "Unauthorized"), (403, "forbidden")],
    )
    def test_a_real_client_error_fails_fast_with_the_right_message(
        self, monkeypatch, status_http_server, status, expected
    ):
        base_url, state = status_http_server
        state["code"] = status
        conn = _AttemptCountingConnection(RuntimeError("IO Error: Could not open GDAL dataset"))
        monkeypatch.setattr(carto_module, "get_duckdb_connection", lambda *a, **k: conn)

        with pytest.raises(CartoError) as excinfo:
            _fetch_with_retry(
                url=base_url,
                table_name="my_table",
                sql="SELECT * FROM my_table LIMIT 10",
                max_retries=3,
                retry_delay=0.01,
                timeout=5,
            )

        assert conn.attempts == 1, "a fatal status must not burn the retry budget"
        assert expected in str(excinfo.value)

    def test_a_real_503_retries_even_when_the_table_name_says_404(
        self, monkeypatch, status_http_server
    ):
        """The inverse of the bug: a name full of digits must not fail fast."""
        base_url, state = status_http_server
        state["code"] = 503
        conn = _AttemptCountingConnection(RuntimeError("IO Error: Could not open GDAL dataset"))
        monkeypatch.setattr(carto_module, "get_duckdb_connection", lambda *a, **k: conn)

        with pytest.raises(CartoError) as excinfo:
            _fetch_with_retry(
                url=base_url,
                table_name="room_404_sensors",
                sql="SELECT * FROM room_404_sensors LIMIT 404",
                max_retries=3,
                retry_delay=0.01,
                timeout=5,
            )

        assert conn.attempts == 3
        assert "Failed to fetch data from Carto after 3 attempts" in str(excinfo.value)

    def test_a_duckdb_http_status_is_used_without_a_probe(self, monkeypatch):
        """When DuckDB itself reports the status, no extra request is issued."""
        error = RuntimeError("HTTP Error: HTTP GET error")
        error.status_code = 404
        conn = _AttemptCountingConnection(error)
        monkeypatch.setattr(carto_module, "get_duckdb_connection", lambda *a, **k: conn)

        def _no_probe(*args, **kwargs):  # pragma: no cover - must not be reached
            raise AssertionError("the status was already known; no probe should be issued")

        monkeypatch.setattr(carto_module, "_probe_request_status", _no_probe)

        with pytest.raises(CartoError, match="not found"):
            _fetch_with_retry(
                url="http://127.0.0.1:1/api/v2/sql",
                table_name="my_table",
                sql="SELECT * FROM my_table",
                max_retries=3,
                retry_delay=0.01,
                timeout=5,
            )

        assert conn.attempts == 1

    def test_a_timeout_is_retried_then_reported_as_a_timeout(self, monkeypatch):
        """Timeouts keep their actionable hint, classified by type not by text."""
        conn = _AttemptCountingConnection(RuntimeError("IO Error: Could not open GDAL dataset"))
        monkeypatch.setattr(carto_module, "get_duckdb_connection", lambda *a, **k: conn)
        monkeypatch.setattr(
            carto_module,
            "_probe_request_status",
            lambda *a, **k: carto_module._CartoStatus(None, True),
        )

        with pytest.raises(CartoError, match="timed out after 3 attempts"):
            _fetch_with_retry(
                url="http://127.0.0.1:1/api/v2/sql",
                table_name="my_table",
                sql="SELECT * FROM my_table",
                max_retries=3,
                retry_delay=0.01,
                timeout=5,
            )

        assert conn.attempts == 3

    def test_a_probe_that_cannot_answer_is_retryable(self, monkeypatch):
        """An unusable URL breaks the probe itself; that means 'no status'."""
        import httpx

        def _boom(*args, **kwargs):
            raise httpx.InvalidURL("not a URL httpx will accept")

        monkeypatch.setattr(httpx, "stream", _boom)
        assert carto_module._probe_request_status("http://x/", 1.0) == carto_module._CartoStatus(
            None, False
        )
