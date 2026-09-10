"""Tests for core/duckdb_utils.py module."""

import os
from unittest import mock

import duckdb
import pytest

from geoparquet_io.core.duckdb_utils import (
    SPATIAL_JOIN_BBOX_PREFILTER,
    SPATIAL_JOIN_NATIVE,
    SPATIAL_JOIN_NO_BBOX,
    _add_bucket_needing_auth,
    _bucket_needs_auth,
    _clear_s3_cache,
    _escape_sql_string,
    _install_and_load_extension,
    _wrap_query_with_blob_conversion,
    build_spatial_join_condition,
    get_duckdb_connection,
    get_duckdb_connection_for_s3,
    load_community_extension,
    quote_identifier,
    s3_config_scope,
    spatial_join_strategy,
    validate_where_clause,
    where_condition_fragment,
    where_sql_fragment,
)
from geoparquet_io.core.exceptions import ExtensionUnavailableError, ValidationError


class TestEscapeSqlString:
    """Tests for SQL string escaping to prevent injection."""

    def test_normal_string_unchanged(self):
        """Normal strings without quotes pass through unchanged."""
        assert _escape_sql_string("normal") == "normal"
        assert _escape_sql_string("path/to/file.parquet") == "path/to/file.parquet"
        assert _escape_sql_string("s3://bucket/data.parquet") == "s3://bucket/data.parquet"

    def test_single_quote_escaped(self):
        """Single quotes are doubled for SQL escaping."""
        assert _escape_sql_string("it's") == "it''s"
        assert _escape_sql_string("O'Brien") == "O''Brien"

    def test_multiple_quotes_escaped(self):
        """Multiple single quotes are all escaped."""
        assert _escape_sql_string("test'';DROP") == "test'''';DROP"
        assert _escape_sql_string("a'b'c") == "a''b''c"

    def test_sql_injection_attempt_neutralized(self):
        """SQL injection attempts are safely escaped."""
        # Classic SQL injection pattern
        malicious = "s3://bucket/test'; DROP TABLE users; --"
        escaped = _escape_sql_string(malicious)
        assert escaped == "s3://bucket/test''; DROP TABLE users; --"
        # The doubled quote prevents the string from terminating early

    def test_empty_string(self):
        """Empty string returns empty string."""
        assert _escape_sql_string("") == ""

    def test_only_quotes(self):
        """String with only quotes gets them all escaped."""
        assert _escape_sql_string("'") == "''"
        assert _escape_sql_string("''") == "''''"
        assert _escape_sql_string("'''") == "''''''"


class TestQuoteIdentifier:
    """Tests for SQL identifier quoting."""

    def test_simple_identifier(self):
        """Simple identifiers are wrapped in double quotes."""
        assert quote_identifier("column") == '"column"'
        assert quote_identifier("table_name") == '"table_name"'

    def test_identifier_with_spaces(self):
        """Identifiers with spaces are safely quoted."""
        assert quote_identifier("my column") == '"my column"'

    def test_identifier_with_double_quotes(self):
        """Embedded double quotes are escaped by doubling."""
        assert quote_identifier('col"name') == '"col""name"'

    def test_reserved_words(self):
        """SQL reserved words are safely quoted."""
        assert quote_identifier("select") == '"select"'
        assert quote_identifier("from") == '"from"'


class TestBuildSpatialJoinCondition:
    """Tests for the spatial-join ON-clause builder.

    Regression guard for PR #460: the cheap bbox-overlap pre-filter must be
    emitted whenever both sides have a bbox column. It was silently removed in
    #457, which made `add admin-divisions --dataset overture` hang.
    """

    def test_without_bbox_columns_is_plain_intersects(self):
        """No bbox columns -> bare ST_Intersects, no pre-filter."""
        cond = build_spatial_join_condition("geometry", "geom")
        assert cond == 'ST_Intersects(b."geom", a."geometry")'

    def test_with_bbox_columns_adds_prefilter(self):
        """Both sides have bbox -> four-sided overlap test ANDed before ST_Intersects."""
        cond = build_spatial_join_condition("geometry", "geom", "bbox", "geom_bbox")
        assert 'a."bbox".xmin <= b."geom_bbox".xmax' in cond
        assert 'a."bbox".xmax >= b."geom_bbox".xmin' in cond
        assert 'a."bbox".ymin <= b."geom_bbox".ymax' in cond
        assert 'a."bbox".ymax >= b."geom_bbox".ymin' in cond
        assert 'ST_Intersects(b."geom", a."geometry")' in cond
        # The cheap bbox test must precede the expensive geometry intersection.
        assert cond.index("xmin") < cond.index("ST_Intersects")
        assert " AND " in cond

    def test_one_sided_bbox_falls_back_to_intersects(self):
        """A bbox on only one side cannot form a safe overlap test -> fall back."""
        plain = 'ST_Intersects(b."g", a."g")'
        assert build_spatial_join_condition("g", "g", input_bbox_col="bbox") == plain
        assert build_spatial_join_condition("g", "g", target_bbox_col="bbox") == plain

    def test_custom_aliases(self):
        """Table aliases are configurable for both sides."""
        cond = build_spatial_join_condition(
            "geometry",
            "geom",
            "bbox",
            "geom_bbox",
            input_alias="lhs",
            target_alias="rhs",
        )
        assert 'lhs."bbox".xmin <= rhs."geom_bbox".xmax' in cond
        assert 'ST_Intersects(rhs."geom", lhs."geometry")' in cond

    def test_identifiers_are_quoted(self):
        """Column names with special characters are safely quoted."""
        cond = build_spatial_join_condition("geo m", 'g"x', "b b", "q")
        assert '"geo m"' in cond
        assert '"g""x"' in cond  # embedded double-quote doubled

    def test_no_derived_bbox_from_geometry(self):
        """Regression guard for #538: the ON clause must never derive a bbox from
        the geometry (ST_XMin/XMax/...) and AND it in front of ST_Intersects.

        #462 proposed exactly that for native geometry; the #538 benchmark showed
        it defeats DuckDB's SPATIAL_JOIN and forces a ~73x slower BLOCKWISE_NL_JOIN.
        The builder must only ever use *stored* bbox covering columns as a
        pre-filter, never compute one inline.
        """
        for cond in (
            build_spatial_join_condition("geometry", "geom"),
            build_spatial_join_condition("geometry", "geom", "bbox", "geom_bbox"),
            build_spatial_join_condition(
                "geometry",
                "geom",
                "bbox",
                "geom_bbox",
                input_geom_sql="ST_Transform(a.\"geometry\", 'EPSG:5070', 'OGC:CRS84')",
            ),
        ):
            assert "ST_XMin" not in cond
            assert "ST_XMax" not in cond
            assert "ST_YMin" not in cond
            assert "ST_YMax" not in cond


class TestSpatialJoinStrategy:
    """Tests for spatial_join_strategy() (issue #538).

    The classifier keeps the user-facing status message in sync with the
    predicate build_spatial_join_condition actually emits, so native-geometry
    inputs are no longer misreported as a degraded "no bbox" fallback.
    """

    def test_native_geometry_is_fast_path(self):
        """Native geometry -> bare ST_Intersects SPATIAL_JOIN, regardless of bbox cols."""
        assert spatial_join_strategy(True, None, None) == SPATIAL_JOIN_NATIVE
        # Native wins even if bbox columns happen to be present.
        assert spatial_join_strategy(True, "bbox", "bbox") == SPATIAL_JOIN_NATIVE

    def test_explicit_bbox_both_sides_uses_prefilter(self):
        """1.x input with bbox on both sides -> bbox-overlap pre-filter."""
        assert spatial_join_strategy(False, "bbox", "bbox") == SPATIAL_JOIN_BBOX_PREFILTER

    def test_missing_bbox_is_no_bbox_fallback(self):
        """1.x input lacking a bbox column (either side) -> genuine no-bbox fallback."""
        assert spatial_join_strategy(False, None, None) == SPATIAL_JOIN_NO_BBOX
        assert spatial_join_strategy(False, "bbox", None) == SPATIAL_JOIN_NO_BBOX
        assert spatial_join_strategy(False, None, "bbox") == SPATIAL_JOIN_NO_BBOX


class TestGetDuckdbConnection:
    """Tests for DuckDB connection creation."""

    def test_basic_connection(self):
        """Basic connection can be created."""
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        assert con is not None
        result = con.execute("SELECT 1").fetchone()
        assert result == (1,)
        con.close()

    def test_thread_limit(self):
        """Connection respects thread limit."""
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False, threads=1)
        assert con is not None
        con.close()

    def test_temp_directory_is_set(self, tmp_path):
        """An explicit temp_directory is used verbatim (OOM guard, todo 013)."""
        spill = str(tmp_path / "spill")
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False, temp_directory=spill)
        try:
            value = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
            assert value == spill
        finally:
            con.close()

    def test_memory_limit_is_set(self):
        """memory_limit is applied when provided."""
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False, memory_limit="2GB")
        try:
            value = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
            # DuckDB normalises the units (e.g. "2.0 GiB"); just assert it changed.
            assert "GiB" in value or "GB" in value
        finally:
            con.close()

    def test_default_temp_directory_is_private_and_off_the_cwd(self):
        """No caller named one, so gpio picks one (see tests/test_spill_strategy.py).

        DuckDB's own default is the relative path ".tmp", which puts every spill
        under the working directory. Nothing is created unless a query actually
        spills, so small inputs behave exactly as before.
        """
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        try:
            value = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
            assert value != ".tmp"
            assert os.path.isabs(value)
            assert not os.path.exists(value)
            assert con.execute("SELECT 1").fetchone() == (1,)
        finally:
            con.close()


class TestLoadCommunityExtension:
    """Tests for community extension loading with clear errors (issue #491)."""

    @pytest.mark.network
    def test_unavailable_extension_raises_clear_error(self):
        """A missing/unpublished community extension yields ExtensionUnavailableError.

        DuckDB raises an opaque HTTP 404 when a community extension is not
        published for the running DuckDB version. We translate that into an
        actionable error rather than leaking the raw HTTP failure.
        """
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        try:
            with pytest.raises(ExtensionUnavailableError) as exc_info:
                load_community_extension(con, "definitely_not_a_real_extension_491")
        finally:
            con.close()

        message = str(exc_info.value)
        assert "definitely_not_a_real_extension_491" in message
        assert duckdb.__version__ in message
        assert exc_info.value.name == "definitely_not_a_real_extension_491"

    def test_duckdb_error_is_wrapped(self):
        """Any DuckDB error during INSTALL/LOAD becomes ExtensionUnavailableError."""

        class _FakeConnection:
            def execute(self, sql):
                raise duckdb.IOException("simulated HTTP 404 for community extension")

        with pytest.raises(ExtensionUnavailableError) as exc_info:
            load_community_extension(_FakeConnection(), "geography")

        assert "geography" in str(exc_info.value)
        assert "simulated HTTP 404" in str(exc_info.value)

    def test_feature_name_is_carried_into_the_error(self):
        """The caller's feature label reaches the message (#737)."""

        class _FakeConnection:
            def execute(self, sql):
                raise duckdb.IOException("simulated HTTP 404 for community extension")

        with pytest.raises(ExtensionUnavailableError) as exc_info:
            load_community_extension(_FakeConnection(), "geography", feature="gpio add s2")

        assert "gpio add s2" in str(exc_info.value)


class TestDuckDBVersionFloor:
    """The dependency floor must stay above the TRY() segfault (#737)."""

    def test_running_duckdb_has_the_try_selection_vector_fix(self):
        """DuckDB <= 1.5.1 segfaults in TRY() over a blob under conditional execution.

        That crash killed `repair_arrow_table_geometry` mid-extraction, discarding
        everything already downloaded (#737, duckdb/duckdb-spatial#858). It is a
        process-level SIGSEGV, so no amount of gpio-side error handling can catch
        it -- only the version floor prevents it.
        """
        version = tuple(int(part) for part in duckdb.__version__.split(".")[:3])
        assert version >= (1, 5, 5), (
            f"DuckDB {duckdb.__version__} segfaults in geometry repair (#737); "
            f"pyproject requires >= 1.5.5"
        )

    # A behavioural probe would be better than the version string above, which is
    # a tautology for anyone who installed per pyproject and cannot see a
    # backport, a vendored build, or a distro DuckDB reporting 1.5.5 over 1.5.1's
    # evaluator. One was attempted and does not exist cheaply. Measured on
    # 1.5.0/1.5.1/1.5.2/1.5.5, all of these behave IDENTICALLY on every version:
    #
    #   - TRY(CAST(VARCHAR AS INTEGER)) under a CASE, vs. the unguarded cast
    #   - the same to DECIMAL, DATE, LIST and STRUCT targets
    #   - the same under a WHERE that forces a selection vector, at 20k rows
    #   - TRY(ST_GeomFromWKB(...)) with NULLs filtered in one WHERE (the unsafe
    #     shape from `_layered_invalid_count_sql`), at 20k rows
    #
    # The real reproduction (#737) needs ~200k WKB polygons over ~300 chunks and
    # kills the process, so it cannot run in-process and is far too slow for the
    # fast lane. Do not add a probe here without first checking it actually FAILS
    # on 1.5.1 -- one that passes on both versions asserts nothing while looking
    # like it asserts everything.


class TestRequireCommunityExtension:
    """Fail-fast preflight for community extensions (#737)."""

    def test_raises_when_extension_cannot_be_loaded(self):
        """An unavailable extension surfaces before any work is attempted."""
        from geoparquet_io.core import duckdb_utils

        def _fail(con, name, feature=None):
            raise ExtensionUnavailableError(name, "1.5.5", "HTTP 404", feature=feature)

        with mock.patch.object(duckdb_utils, "load_community_extension", _fail):
            with pytest.raises(ExtensionUnavailableError) as exc_info:
                duckdb_utils.require_community_extension("geography", feature="gpio add s2")

        assert "gpio add s2" in str(exc_info.value)

    def test_passes_and_closes_its_connection_when_available(self):
        """The preflight uses a throwaway connection and leaves nothing open."""
        from geoparquet_io.core import duckdb_utils

        calls = []

        def _ok(con, name, feature=None):
            calls.append((con, name, feature))

        with mock.patch.object(duckdb_utils, "load_community_extension", _ok):
            duckdb_utils.require_community_extension("geography", feature="gpio add s2")

        assert len(calls) == 1
        con, name, feature = calls[0]
        assert (name, feature) == ("geography", "gpio add s2")
        with pytest.raises(duckdb.Error):
            con.execute("SELECT 1")


class TestS3CacheThreadSafety:
    """Tests for thread-safe S3 bucket authentication cache."""

    def test_clear_s3_cache_removes_buckets(self):
        """_clear_s3_cache removes all cached buckets."""
        # Add a test bucket
        _add_bucket_needing_auth("test-bucket-1")
        _add_bucket_needing_auth("test-bucket-2")
        assert _bucket_needs_auth("test-bucket-1")
        assert _bucket_needs_auth("test-bucket-2")

        # Clear should remove all
        _clear_s3_cache()
        assert not _bucket_needs_auth("test-bucket-1")
        assert not _bucket_needs_auth("test-bucket-2")

    def test_add_and_check_bucket(self):
        """_add_bucket_needing_auth and _bucket_needs_auth work correctly."""
        _clear_s3_cache()  # Start fresh

        # Initially empty
        assert not _bucket_needs_auth("new-bucket")

        # Add and verify
        _add_bucket_needing_auth("new-bucket")
        assert _bucket_needs_auth("new-bucket")

        # Cleanup
        _clear_s3_cache()

    def test_idempotent_add(self):
        """Adding same bucket multiple times is safe."""
        _clear_s3_cache()

        _add_bucket_needing_auth("idempotent-bucket")
        _add_bucket_needing_auth("idempotent-bucket")
        _add_bucket_needing_auth("idempotent-bucket")

        assert _bucket_needs_auth("idempotent-bucket")
        _clear_s3_cache()


class TestGetDuckdbConnectionS3Config:
    """Tests for S3 endpoint configuration on DuckDB connections."""

    def test_s3_endpoint_sets_duckdb_vars(self):
        """s3_endpoint param emits SET s3_endpoint, s3_url_style, s3_use_ssl."""
        con = get_duckdb_connection(
            load_spatial=False,
            load_httpfs=True,
            s3_endpoint="data.source.coop",
            s3_use_ssl=True,
        )
        result = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
        assert result[0] == "data.source.coop"

        result = con.execute("SELECT current_setting('s3_url_style')").fetchone()
        assert result[0] == "path"

        result = con.execute("SELECT current_setting('s3_use_ssl')").fetchone()
        assert result[0] is True
        con.close()

    def test_s3_region_sets_duckdb_var(self):
        """s3_region param emits SET s3_region."""
        con = get_duckdb_connection(
            load_spatial=False,
            load_httpfs=True,
            s3_region="eu-west-1",
        )
        result = con.execute("SELECT current_setting('s3_region')").fetchone()
        assert result[0] == "eu-west-1"
        con.close()

    def test_s3_no_ssl_sets_duckdb_var(self):
        """s3_use_ssl=False emits SET s3_use_ssl=false."""
        con = get_duckdb_connection(
            load_spatial=False,
            load_httpfs=True,
            s3_endpoint="minio.local:9000",
            s3_use_ssl=False,
        )
        result = con.execute("SELECT current_setting('s3_use_ssl')").fetchone()
        assert result[0] is False
        con.close()

    def test_no_s3_params_no_set_statements(self):
        """Without S3 params, no custom SET statements are emitted."""
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        result = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
        assert not result[0]
        con.close()

    def test_get_duckdb_connection_for_s3_forwards_endpoint(self):
        """get_duckdb_connection_for_s3() forwards S3 endpoint params."""
        con = get_duckdb_connection_for_s3(
            "/local/file.parquet",
            load_spatial=False,
            s3_endpoint="data.source.coop",
        )
        result = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
        assert result[0] == "data.source.coop"
        con.close()


class TestAmbientS3Config:
    """Tests for ambient S3 config via s3_config_scope()."""

    def test_ambient_config_picked_up_by_connection(self):
        """get_duckdb_connection() reads ambient config when no explicit kwargs."""
        with s3_config_scope({"s3_endpoint": "ambient.example.com", "s3_use_ssl": True}):
            con = get_duckdb_connection(load_spatial=False, load_httpfs=True)
            result = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
            assert result[0] == "ambient.example.com"
            con.close()

    def test_explicit_kwargs_override_ambient(self):
        """Explicit s3_endpoint kwarg wins over ambient config."""
        with s3_config_scope({"s3_endpoint": "ambient.example.com"}):
            con = get_duckdb_connection(
                load_spatial=False,
                load_httpfs=True,
                s3_endpoint="explicit.example.com",
            )
            result = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
            assert result[0] == "explicit.example.com"
            con.close()

    def test_ambient_config_cleared_after_scope(self):
        """Ambient config is cleaned up when scope exits."""
        with s3_config_scope({"s3_endpoint": "scoped.example.com"}):
            pass
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        result = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
        assert not result[0]
        con.close()

    def test_ambient_region_picked_up(self):
        """Ambient s3_region is picked up by get_duckdb_connection()."""
        with s3_config_scope({"s3_region": "eu-west-1"}):
            con = get_duckdb_connection(load_spatial=False, load_httpfs=True)
            result = con.execute("SELECT current_setting('s3_region')").fetchone()
            assert result[0] == "eu-west-1"
            con.close()


# Payloads that defeat a hand-rolled quote-state walker: each hides the ';'
# statement separator behind lexical syntax the walker does not model
# (dollar quoting, block comment, line comment, E-string escape). Every one of
# them parses as three statements, the second of which writes a file via COPY.
BYPASS_PAYLOADS = {
    "dollar_quote": "1=1 OR $$'$$='a'); COPY (SELECT 42 AS p) TO '/tmp/pwn.csv'; SELECT 1 WHERE (1=1",
    "block_comment": "1=1 /* ' */); COPY (SELECT 42 AS p) TO '/tmp/pwn.csv'; SELECT 1 WHERE (1=1",
    "line_comment": "1=1 -- '\n); COPY (SELECT 42 AS p) TO '/tmp/pwn.csv'; SELECT 1 WHERE (1=1",
    "e_string": "1=1 OR 'x'=E'\\''); COPY (SELECT 42 AS p) TO '/tmp/pwn.csv'; SELECT 1 WHERE (1=1",
}

# Legitimate filters that each embed a blocklisted keyword inside a quoted
# string literal (or use REPLACE(), a standard scalar function).
LEGITIMATE_CLAUSES = [
    "name = 'Grant County'",
    "street LIKE '%Alter Markt%'",
    "descr ILIKE '%drop off%'",
    "status = 'DELETE'",
    "name = 'Merge Lane'",
    "REPLACE(zip, '-', '') = '19104'",
]


class TestValidateWhereClauseStatementGate:
    """The statement gate must be parser-based, not a quote-state walker."""

    @pytest.mark.parametrize("payload", BYPASS_PAYLOADS.values(), ids=list(BYPASS_PAYLOADS))
    def test_bypass_payload_rejected(self, payload):
        """Each payload smuggles a ';' past naive quote tracking and must be rejected."""
        with pytest.raises(ValidationError):
            validate_where_clause(payload)

    @pytest.mark.parametrize("payload", BYPASS_PAYLOADS.values(), ids=list(BYPASS_PAYLOADS))
    def test_bypass_payload_really_is_multi_statement(self, payload):
        """Guard the premise: these payloads genuinely compose 3 SQL statements."""
        probe = f"SELECT 1 WHERE {where_condition_fragment(payload)}"
        assert len(duckdb.extract_statements(probe)) == 3

    def test_plain_semicolon_rejected(self):
        with pytest.raises(ValidationError):
            validate_where_clause("1=1;")

    def test_copy_injection_rejected(self):
        with pytest.raises(ValidationError, match=";"):
            validate_where_clause("1=1); COPY (SELECT 42 AS x) TO '/tmp/pwned.csv'; SELECT (1=1")

    def test_unparseable_clause_rejected_cleanly(self):
        """A clause that cannot be parsed raises ValidationError, not a duckdb error."""
        with pytest.raises(ValidationError, match="single filtering expression"):
            validate_where_clause("1=1 AND (((")

    def test_semicolon_inside_literal_still_allowed(self):
        validate_where_clause("name = 'a;b'")
        validate_where_clause("note = 'it''s; fine'")
        validate_where_clause('"weird;col" = 5')


class TestValidateWhereClauseQuotedLiterals:
    """Blocklisted keywords inside string literals must not trigger a rejection."""

    @pytest.mark.parametrize("clause", LEGITIMATE_CLAUSES)
    def test_legitimate_clause_accepted(self, clause):
        validate_where_clause(clause)

    def test_keyword_outside_literal_still_blocked(self):
        with pytest.raises(ValidationError, match="DELETE"):
            validate_where_clause("DELETE FROM users WHERE 1=1")

    def test_keyword_in_comment_not_flagged(self):
        """A comment is inert; it must not trip the keyword scan."""
        validate_where_clause("pop > 10 /* drop this later */")


class TestWhereConditionFragment:
    """The condition fragment must survive a trailing line comment."""

    def test_trailing_comment_cannot_swallow_closing_paren(self):
        fragment = where_condition_fragment("1=1 --")
        # Everything after the clause's own line is still live SQL.
        assert fragment.splitlines()[-1].strip() == ")"

    def test_appended_condition_survives_trailing_comment(self):
        sql = f"SELECT 1 WHERE {where_condition_fragment('1=1 --')} AND 2=2"
        rows = duckdb.sql(sql).fetchall()
        assert rows == [(1,)]
        # And a falsifying appended condition is genuinely applied.
        sql_false = f"SELECT 1 WHERE {where_condition_fragment('1=1 --')} AND 1=2"
        assert duckdb.sql(sql_false).fetchall() == []

    def test_where_sql_fragment_uses_condition_fragment(self):
        assert where_sql_fragment("a = 1") == f" WHERE {where_condition_fragment('a = 1')}"
        assert where_sql_fragment(None) == ""


class _FakeExtensionConnection:
    """Connection stub whose INSTALL/LOAD outcomes are scripted per test."""

    def __init__(self, install_error=None, load_error=None):
        self._install_error = install_error
        self._load_error = load_error
        self.statements = []

    def execute(self, sql):
        self.statements.append(sql)
        if sql.startswith("INSTALL") and self._install_error is not None:
            raise self._install_error
        if sql.startswith("LOAD") and self._load_error is not None:
            raise self._load_error


@pytest.mark.parametrize("name", ["spatial", "httpfs", "aws"])
class TestInstallAndLoadExtension:
    """A failed INSTALL must surface when LOAD fails, and stay quiet otherwise (issue #574)."""

    def test_install_failure_with_working_load_is_silent(self, name, caplog):
        """The parallel-install race and a loadable cached copy must not warn."""
        con = _FakeExtensionConnection(
            install_error=duckdb.IOException("permission denied creating extension dir")
        )

        with caplog.at_level("WARNING", logger="geoparquet_io"):
            _install_and_load_extension(con, name)

        assert con.statements == [f"INSTALL {name};", f"LOAD {name};"]
        assert not caplog.records, [r.message for r in caplog.records]

    def test_install_failure_explains_load_failure(self, name, caplog):
        """When LOAD also fails, the install error is warned and LOAD's error propagates."""
        con = _FakeExtensionConnection(
            install_error=duckdb.IOException("permission denied creating extension dir"),
            load_error=duckdb.IOException(f'Extension "{name}.duckdb_extension" not found'),
        )

        with caplog.at_level("WARNING", logger="geoparquet_io"):
            with pytest.raises(duckdb.IOException, match="not found"):
                _install_and_load_extension(con, name)

        assert any(
            name in record.message and "permission denied" in record.message
            for record in caplog.records
        ), f"no warning about the failed install: {[r.message for r in caplog.records]}"

    def test_load_failure_alone_warns_nothing(self, name, caplog):
        """A LOAD failure with a clean INSTALL has nothing extra to explain."""
        con = _FakeExtensionConnection(load_error=duckdb.IOException("load blew up"))

        with caplog.at_level("WARNING", logger="geoparquet_io"):
            with pytest.raises(duckdb.IOException, match="load blew up"):
                _install_and_load_extension(con, name)

        assert not caplog.records, [r.message for r in caplog.records]


class TestWrapQueryWithBlobConversion:
    """The v1.x BLOB cast, including which secondary columns it may touch (#712)."""

    @pytest.fixture
    def con(self):
        connection = get_duckdb_connection(load_spatial=True)
        yield connection
        connection.close()

    @staticmethod
    def _query(alias_types: str) -> str:
        return f"SELECT {alias_types}"

    def test_primary_geometry_column_is_cast(self, con):
        query = "SELECT 1 AS id, ST_GeomFromText('POINT (1 2)') AS geometry"

        wrapped = _wrap_query_with_blob_conversion(query, "geometry", con)

        assert "ST_AsWKB" in wrapped
        assert con.execute(f"DESCRIBE ({wrapped})").fetchall()[1][1] == "BLOB"

    def test_a_blob_primary_column_is_not_cast(self, con):
        """A BLOB primary is already the 1.x carrier; ST_AsWKB(BLOB) does not bind."""
        query = "SELECT 1 AS id, 'raw'::BLOB AS geometry"

        wrapped = _wrap_query_with_blob_conversion(query, "geometry", con)

        assert "ST_AsWKB" not in wrapped
        assert con.execute(wrapped).fetchall()[0][1] == b"raw"

    def test_a_varchar_primary_column_is_not_cast(self, con):
        """ST_AsWKB(VARCHAR) *does* bind, silently reinterpreting the text as WKT."""
        query = "SELECT 1 AS id, 'POINT (9 9)' AS geometry"

        wrapped = _wrap_query_with_blob_conversion(query, "geometry", con)

        assert "ST_AsWKB" not in wrapped
        assert con.execute(wrapped).fetchall()[0][1] == "POINT (9 9)"

    def test_an_undescribable_query_still_casts_the_primary(self, con):
        """DESCRIBE can fail; with no type info the primary is still the best guess."""
        wrapped = _wrap_query_with_blob_conversion(
            "SELECT * FROM a_table_that_does_not_exist", "geometry", con
        )

        assert 'ST_AsWKB("geometry")' in wrapped

    def test_secondary_geometry_columns_are_cast_too(self, con):
        query = (
            "SELECT 1 AS id, ST_GeomFromText('POINT (1 2)') AS geometry, "
            "ST_GeomFromText('POINT (3 4)') AS centroid"
        )

        wrapped = _wrap_query_with_blob_conversion(
            query, "geometry", con, secondary_columns=["centroid"]
        )

        types = {row[0]: row[1] for row in con.execute(f"DESCRIBE ({wrapped})").fetchall()}
        assert types["geometry"] == "BLOB"
        assert types["centroid"] == "BLOB"

    def test_a_declared_column_duckdb_types_as_blob_is_not_cast(self, con):
        """ST_AsWKB(BLOB) does not bind: casting it would abort the whole write."""
        query = "SELECT 1 AS id, ST_GeomFromText('POINT (1 2)') AS geometry, 'raw'::BLOB AS payload"

        wrapped = _wrap_query_with_blob_conversion(
            query, "geometry", con, secondary_columns=["payload"]
        )

        assert wrapped.count("ST_AsWKB") == 1
        assert con.execute(wrapped).fetchall()[0][2] == b"raw"

    def test_a_declared_column_duckdb_types_as_varchar_is_not_cast(self, con):
        """ST_AsWKB(VARCHAR) *does* bind, silently reinterpreting text as WKT."""
        query = "SELECT 1 AS id, ST_GeomFromText('POINT (1 2)') AS geometry, 'POINT (9 9)' AS label"

        wrapped = _wrap_query_with_blob_conversion(
            query, "geometry", con, secondary_columns=["label"]
        )

        assert con.execute(wrapped).fetchall()[0][2] == "POINT (9 9)"

    def test_a_struct_column_is_not_cast(self, con):
        """A STRUCT of a declared name is a bbox- or GeoArrow-style column."""
        query = (
            "SELECT 1 AS id, ST_GeomFromText('POINT (1 2)') AS geometry, {'x': 1.0, 'y': 2.0} AS pt"
        )

        wrapped = _wrap_query_with_blob_conversion(query, "geometry", con, secondary_columns=["pt"])

        assert con.execute(wrapped).fetchall()[0][2] == {"x": 1.0, "y": 2.0}

    def test_a_missing_secondary_column_is_ignored(self, con):
        query = "SELECT 1 AS id, ST_GeomFromText('POINT (1 2)') AS geometry"

        wrapped = _wrap_query_with_blob_conversion(
            query, "geometry", con, secondary_columns=["ghost"]
        )

        assert "ghost" not in wrapped
        types = {row[0]: row[1] for row in con.execute(f"DESCRIBE ({wrapped})").fetchall()}
        assert types == {"id": "INTEGER", "geometry": "BLOB"}

    def test_secondaries_need_a_connection_to_be_typed(self):
        """Without a connection the types are unknown, so secondaries are left alone."""
        query = "SELECT 1 AS id, geometry, centroid FROM t"

        wrapped = _wrap_query_with_blob_conversion(
            query, "geometry", None, secondary_columns=["centroid"]
        )

        assert wrapped.count("ST_AsWKB") == 1


class TestCommunityExtensionTelemetry:
    """The a5 community extension phones home on LOAD from a detached thread.

    That thread races process teardown and segfaults (issue #779), so gpio opts
    out before any community extension is loaded.
    """

    def _recording_connection(self, seen):
        class FakeConnection:
            def execute(self, sql):
                seen.append((sql, os.environ.get("QUERY_FARM_TELEMETRY_OPT_OUT")))

        return FakeConnection()

    def test_opt_out_is_set_before_load(self, monkeypatch):
        monkeypatch.delenv("QUERY_FARM_TELEMETRY_OPT_OUT", raising=False)
        seen = []
        load_community_extension(self._recording_connection(seen), "a5")
        load_sql = [entry for entry in seen if entry[0].upper().startswith("LOAD")]
        assert load_sql, "expected a LOAD statement"
        assert all(value is not None for _, value in load_sql), (
            "QUERY_FARM_TELEMETRY_OPT_OUT must be set before LOAD so the extension "
            "does not spawn its detached telemetry thread"
        )

    def test_existing_user_value_is_preserved(self, monkeypatch):
        monkeypatch.setenv("QUERY_FARM_TELEMETRY_OPT_OUT", "user-set")
        seen = []
        load_community_extension(self._recording_connection(seen), "a5")
        assert os.environ["QUERY_FARM_TELEMETRY_OPT_OUT"] == "user-set"


class TestNoCommunityExtensionBypass:
    """Every community-extension load must go through load_community_extension.

    The a5 extension's load-time telemetry is opted out of inside that helper
    (issue #779), so a raw ``INSTALL ... FROM community`` / ``LOAD`` pair brings
    the segfault back on whatever path skipped it. Three call sites did exactly
    that -- ``process aggregate a5``, ``process overview`` and the ``--auto``
    resolution probe -- which is how the first fix for #779 missed them.
    """

    # bigquery is not a Query Farm extension and carries no telemetry; its
    # FORCE INSTALL also has to stay a FORCE INSTALL, which the helper does not
    # express.
    ALLOWED = {"duckdb_utils.py", "exceptions.py", "extract_bigquery.py"}

    def test_no_module_installs_a_community_extension_directly(self):
        from pathlib import Path

        package = Path(__file__).parent.parent / "geoparquet_io"
        offenders = [
            path.relative_to(package.parent)
            for path in package.rglob("*.py")
            # encoding pinned: the default is cp1252 on Windows, and the
            # sources carry em dashes.
            if path.name not in self.ALLOWED
            and "FROM community" in path.read_text(encoding="utf-8")
        ]
        assert not offenders, (
            "these modules install a community extension directly instead of "
            f"calling load_community_extension(): {offenders}"
        )
