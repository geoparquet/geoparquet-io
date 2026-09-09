"""Regression tests: quote geometry-column identifiers at raw SQL interpolation.

Many SQL builders used to interpolate a column identifier raw into a query, so
any file whose geometry column has a space, uppercase letter, reserved word or
embedded quote broke with a ParserException -- and because the name is read
verbatim from the file's own ``geo.primary_column`` metadata, a crafted parquet
was an injection vector (see tests/test_admin_sql_injection.py for the sibling
guard that already covers admin-divisions / country-codes).

Two different escapes are involved, and mixing them up is the recurring bug:

* An **identifier** (column/table name) is wrapped in double quotes with any
  embedded ``"`` doubled -- :func:`duckdb_utils.quote_identifier`. Hand-rolling
  it as ``f'"{col}"'`` tolerates a space but silently breaks on an embedded
  ``"``, which is why the ``.pre-commit-config.yaml`` ``duckdb-antipatterns``
  hook now bans that spelling.
* A **string literal** (e.g. ``WHERE path_in_schema = '...'``) is wrapped in
  single quotes with any embedded ``'`` doubled --
  :func:`duckdb_utils._escape_sql_string`. A space is harmless there; only an
  embedded ``'`` breaks it, and it used to break *silently*, because the
  callers wrap the query in a broad ``except Exception`` and return
  ``None``/``{}``/``[]`` instead of raising.

Every adversarial-name test in this file runs against the same three names
(:data:`ADVERSARIAL_COLUMNS`) so no site is accidentally covered only by the
one flavour of hostility it happens to tolerate.
"""

from __future__ import annotations

import json

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import add, check, convert, extract, inspect, sort
from geoparquet_io.core import common as common_module
from geoparquet_io.core.check_spatial_order import check_spatial_order
from geoparquet_io.core.duckdb_metadata import (
    get_aggregated_native_geo_stats,
    get_compression_info,
    get_native_geo_statistics,
    get_per_row_group_bbox_stats,
    get_per_row_group_native_geo_stats,
)
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier
from geoparquet_io.core.stream_io import _wrap_query_with_wkb_conversion
from tests.conftest import skip_if_geography_unavailable

# A geometry column name with a space -- breaks unquoted identifier
# interpolation (`ST_XMin(geom col)` is a parse error).
SPACED_COL = "geom col"

# A geometry column name with an embedded double-quote -- breaks hand-rolled
# `f'"{col}"'` quoting, which does not double the `"`.
DQUOTE_COL = 'geo"m'

# A geometry column name with an embedded single-quote -- breaks a raw SQL
# STRING LITERAL comparison like `path_in_schema = '{col}'`.
SQUOTE_COL = "geo'm"

#: Applied uniformly: every site must survive every hostile name, not just the
#: one flavour of hostility that site's original bug happened to trip over.
ADVERSARIAL_COLUMNS = [SPACED_COL, DQUOTE_COL, SQUOTE_COL]

#: A name that is not merely unparsable but *executable* if quoting fails:
#: unquoted, `ST_XMin("g") AS x, (SELECT 1) AS pwned --")` parses fine and
#: injects two extra result columns. Asserting the output schema is unchanged
#: proves the injection was neutralised, not merely that nothing crashed.
INJECTION_COL = 'g") AS x, (SELECT 1) AS pwned --'

adversarial_column = pytest.mark.parametrize("column_name", ADVERSARIAL_COLUMNS)

POLYGON_WKTS = [
    "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
    "POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))",
]


def _write_geoparquet(table, column_name: str, geometry_types: list[str], path: str, bbox=None):
    """Attach v1.0.0 `geo` metadata naming ``column_name`` and write ``path``."""
    col_meta: dict = {"encoding": "WKB", "geometry_types": geometry_types}
    if bbox is not None:
        col_meta["bbox"] = bbox
    geo = {
        "version": "1.0.0",
        "primary_column": column_name,
        "columns": {column_name: col_meta},
    }
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    pq.write_table(table, path)
    return path


def _wkb_fixture(tmp_path, column_name: str, filename: str, wkts: list[str]) -> str:
    """Build a plain-WKB GeoParquet file whose geo.primary_column = column_name."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    quoted = quote_identifier(column_name)
    values_sql = ", ".join(
        f"({i + 1}, ST_AsWKB(ST_GeomFromText('{w}')))" for i, w in enumerate(wkts)
    )
    table = (
        con.execute(f"SELECT * FROM (VALUES {values_sql}) AS t(id, {quoted})").arrow().read_all()
    )
    con.close()
    return _write_geoparquet(table, column_name, ["Polygon"], str(tmp_path / filename))


def _points_fixture(tmp_path, column_name: str, filename: str, count: int = 10) -> str:
    """A ten-point WKB fixture with a declared bbox (so `check spec` is clean)."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    quoted = quote_identifier(column_name)
    values_sql = ", ".join(
        f"({i + 1}, ST_AsWKB(ST_GeomFromText('POINT ({i} {i})')))" for i in range(count)
    )
    table = (
        con.execute(f"SELECT * FROM (VALUES {values_sql}) AS t(id, {quoted})").arrow().read_all()
    )
    con.close()
    return _write_geoparquet(
        table,
        column_name,
        ["Point"],
        str(tmp_path / filename),
        bbox=[0.0, 0.0, float(count - 1), float(count - 1)],
    )


def _native_geo_fixture(tmp_path, column_name: str, filename: str) -> str:
    """Build a parquet-geo-only file (native GEOMETRY, geo_bbox row-group stats)."""
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    con.execute("SET geometry_always_xy = true;")
    quoted = quote_identifier(column_name)
    query = (
        "SELECT * FROM (VALUES "
        "(1, ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))')), "
        "(2, ST_GeomFromText('POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))'))"
        f") AS t(id, {quoted})"
    )
    path = str(tmp_path / filename)
    common_module.write_parquet_with_metadata(
        con, query, path, original_metadata=None, geoparquet_version="parquet-geo-only"
    )
    con.close()
    return path


# --- The helper itself: contract and edge cases -----------------------------


class TestQuoteIdentifierContract:
    """geoparquet_io/core/duckdb_utils.py: quote_identifier."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("geom", '"geom"'),
            (SPACED_COL, '"geom col"'),
            (DQUOTE_COL, '"geo""m"'),
            (SQUOTE_COL, '"geo\'m"'),
            ("SELECT", '"SELECT"'),
            ("Geometry", '"Geometry"'),
            ("a\nb", '"a\nb"'),
            (INJECTION_COL, '"g"") AS x, (SELECT 1) AS pwned --"'),
        ],
    )
    def test_quotes_hostile_names(self, raw, expected):
        assert quote_identifier(raw) == expected

    @adversarial_column
    def test_result_round_trips_through_duckdb(self, column_name):
        con = duckdb.connect()
        try:
            quoted = quote_identifier(column_name)
            name = con.execute(f"SELECT 1 AS {quoted}").arrow().read_all().column_names[0]
            assert name == column_name
        finally:
            con.close()

    def test_not_idempotent_by_contract(self):
        """Callers must pass a BARE name; re-quoting is a caller bug, not a no-op."""
        assert quote_identifier(quote_identifier("geom")) == '"""geom"""'

    def test_empty_name_is_rejected(self):
        # `""` is a zero-length delimited identifier: DuckDB refuses to parse it,
        # so emitting it would produce broken SQL instead of a clear error.
        with pytest.raises(ValueError, match="empty"):
            quote_identifier("")

    def test_nul_byte_is_rejected(self):
        # A NUL truncates the identifier inside DuckDB's parser, producing
        # "unterminated quoted identifier" rather than anything actionable.
        with pytest.raises(ValueError, match="NUL"):
            quote_identifier("ge\x00om")


# --- Site 1: stream_io._wrap_query_with_wkb_conversion ----------------------


class TestStreamIoWkbConversionQuoting:
    """geoparquet_io/core/stream_io.py: _wrap_query_with_wkb_conversion."""

    @adversarial_column
    def test_wraps_and_converts_correctly(self, column_name):
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            quoted = quote_identifier(column_name)
            con.execute(
                f"CREATE TABLE t AS SELECT 1 AS id, ST_GeomFromText('POINT (1 2)') AS {quoted}"
            )

            wrapped = _wrap_query_with_wkb_conversion("SELECT * FROM t", column_name)
            row = con.execute(wrapped).fetchone()

            assert row[0] == 1
            wkt = con.execute("SELECT ST_AsText(ST_GeomFromWKB(?))", [row[1]]).fetchone()[0]
            assert wkt == "POINT (1 2)"
        finally:
            con.close()

    def test_injection_in_column_name_is_neutralised(self):
        """The wrapper's REPLACE target must not be able to add columns."""
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            quoted = quote_identifier(INJECTION_COL)
            con.execute(
                f"CREATE TABLE t AS SELECT 1 AS id, ST_GeomFromText('POINT (1 2)') AS {quoted}"
            )

            wrapped = _wrap_query_with_wkb_conversion("SELECT * FROM t", INJECTION_COL)
            columns = con.execute(wrapped).arrow().read_all().column_names

            # The hostile name itself contains "pwned", so a substring check
            # would be vacuous: the proof is that the schema gained no EXTRA
            # column beyond the two the source table actually has.
            assert columns == ["id", INJECTION_COL]
        finally:
            con.close()


# --- Site 2: common.add_bbox's STRUCT_PACK expression ------------------------
# Plus sibling: core/add/bbox.py's _add_bbox_file_based (the real `gpio add
# bbox` CLI code path).


class TestCommonAddBboxQuoting:
    """geoparquet_io/core/common.py: add_bbox."""

    @adversarial_column
    def test_add_bbox_succeeds_with_correct_values(self, tmp_path, column_name):
        path = _wkb_fixture(tmp_path, column_name, "add_bbox_fixture.parquet", POLYGON_WKTS)

        common_module.add_bbox(path, bbox_column_name="bbox", verbose=False)

        table = pq.read_table(path)
        rows = table.column("bbox").combine_chunks().to_pylist()
        assert rows[0] == {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
        assert rows[1] == {"xmin": 2.0, "ymin": 2.0, "xmax": 3.0, "ymax": 3.0}


class TestCliAddBboxOnHostileColumn:
    """End-to-end `gpio add bbox`: core/add/bbox.py's _add_bbox_file_based ->
    common.py's add_computed_column -> the duckdb-kv write strategy."""

    @adversarial_column
    def test_cli_add_bbox_succeeds(self, tmp_path, column_name):
        input_path = _wkb_fixture(tmp_path, column_name, "cli_add_bbox_input.parquet", POLYGON_WKTS)
        output_path = str(tmp_path / "cli_add_bbox_output.parquet")

        runner = CliRunner()
        result = runner.invoke(add, ["bbox", input_path, output_path])

        assert result.exit_code == 0, result.output
        table = pq.read_table(output_path)
        rows = table.column("bbox").combine_chunks().to_pylist()
        assert rows[0] == {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
        assert rows[1] == {"xmin": 2.0, "ymin": 2.0, "xmax": 3.0, "ymax": 3.0}

    def test_injection_in_geometry_column_name_is_neutralised(self, tmp_path):
        input_path = _wkb_fixture(
            tmp_path, INJECTION_COL, "cli_add_bbox_injection.parquet", POLYGON_WKTS
        )
        output_path = str(tmp_path / "cli_add_bbox_injection_out.parquet")

        runner = CliRunner()
        result = runner.invoke(add, ["bbox", input_path, output_path])

        assert result.exit_code == 0, result.output
        names = pq.read_schema(output_path).names
        assert names == ["id", INJECTION_COL, "bbox"]


class TestAddComputedColumnQuotesUserSuppliedName:
    """geoparquet_io/core/common.py: add_computed_column.

    ``--bbox-name`` / ``--column`` are supplied directly on the command line and
    reach the shared ``add_computed_column`` backend used by `add bbox`,
    `add h3`, `add s2`, `add a5`, `add quadkey` and `add geometry-metrics`.
    """

    @adversarial_column
    def test_cli_bbox_name_is_quoted(self, tmp_path, column_name):
        input_path = _points_fixture(tmp_path, "geometry", "bbox_name_input.parquet")
        output_path = str(tmp_path / "bbox_name_output.parquet")

        runner = CliRunner()
        result = runner.invoke(add, ["bbox", input_path, output_path, "--bbox-name", column_name])

        assert result.exit_code == 0, result.output
        assert column_name in pq.read_schema(output_path).names

    def test_cli_bbox_name_injection_is_neutralised(self, tmp_path):
        input_path = _points_fixture(tmp_path, "geometry", "bbox_name_inj_input.parquet")
        output_path = str(tmp_path / "bbox_name_inj_output.parquet")

        runner = CliRunner()
        result = runner.invoke(add, ["bbox", input_path, output_path, "--bbox-name", INJECTION_COL])

        assert result.exit_code == 0, result.output
        names = pq.read_schema(output_path).names
        assert names == ["id", "geometry", INJECTION_COL]


# --- Site 3: check_spatial_order sampling-path queries ----------------------


class TestCheckSpatialOrderSamplingQuoting:
    """geoparquet_io/core/check_spatial_order.py: the sampling-method queries."""

    @adversarial_column
    def test_sampling_method_succeeds(self, tmp_path, column_name):
        path = _points_fixture(tmp_path, column_name, "spatial_order_fixture.parquet")

        result = check_spatial_order(
            path, random_sample_size=5, limit_rows=1000, verbose=False, return_results=True
        )

        assert result["method"] == "sampling"
        assert result["consecutive_avg"] is not None
        assert result["random_avg"] is not None
        # Points march evenly along the diagonal, so consecutive neighbours are
        # always closer than a random pair -> the ratio is safely under 1.
        assert result["ratio"] < 1.0

    @adversarial_column
    def test_cli_check_spatial_succeeds(self, tmp_path, column_name):
        path = _points_fixture(tmp_path, column_name, "spatial_order_cli_fixture.parquet")

        runner = CliRunner()
        result = runner.invoke(check, ["spatial", path])

        assert result.exit_code == 0, result.output


# --- Site 4: duckdb_metadata `path_in_schema = '{...}'` literal filters ----


class TestDuckdbMetadataNativeStatsEscaping:
    """geoparquet_io/core/duckdb_metadata.py: the native-geo-stat getters.

    These are STRING LITERAL comparisons, not identifiers, so the fix is
    `_escape_sql_string`. Only an embedded `'` can break them -- a space is a
    legal string-literal character -- so this class is deliberately narrowed to
    the name with teeth.
    """

    def test_native_geo_stat_getters_return_correct_values(self, tmp_path):
        path = _native_geo_fixture(tmp_path, SQUOTE_COL, "native_geo.parquet")

        single = get_native_geo_statistics(path, SQUOTE_COL)
        assert single is not None, "expected stats, got None (query silently failed)"
        assert single["bbox"][:4] == pytest.approx([0.0, 0.0, 3.0, 3.0])
        assert single["geometry_types"] == ["Polygon"]

        agg = get_aggregated_native_geo_stats(path, SQUOTE_COL)
        assert agg.get("bbox") is not None, "expected aggregated bbox, got empty dict"
        assert agg["bbox"][:4] == pytest.approx([0.0, 0.0, 3.0, 3.0])
        assert agg["geometry_types"] == ["Polygon"]

        per_rg = get_per_row_group_native_geo_stats(path, SQUOTE_COL)
        assert len(per_rg) == 1, "expected one row group of stats, got empty list"
        assert per_rg[0]["xmin"] == pytest.approx(0.0)
        assert per_rg[0]["ymin"] == pytest.approx(0.0)
        assert per_rg[0]["xmax"] == pytest.approx(3.0)
        assert per_rg[0]["ymax"] == pytest.approx(3.0)


class TestDuckdbMetadataSiblingLiteralEscaping:
    """Other `path_in_schema = '...'` filters: the bbox-stat and compression
    getters key off a column name the same way."""

    def test_per_row_group_bbox_stats_escapes_quote_in_bbox_column_name(self, tmp_path):
        bbox_col = "bb'ox"
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        quoted = quote_identifier(bbox_col)
        query = f"""
            SELECT * FROM (VALUES
                (1, {{'xmin': 0.0, 'ymin': 0.0, 'xmax': 1.0, 'ymax': 1.0}}),
                (2, {{'xmin': 2.0, 'ymin': 2.0, 'xmax': 3.0, 'ymax': 3.0}})
            ) AS t(id, {quoted})
        """
        table = con.execute(query).arrow().read_all()
        con.close()
        path = str(tmp_path / "bbox_col_quote.parquet")
        pq.write_table(table, path)

        result = get_per_row_group_bbox_stats(path, bbox_column=bbox_col)

        assert len(result) == 1
        assert result[0]["xmin"] == pytest.approx(0.0)
        assert result[0]["xmax"] == pytest.approx(3.0)

    def test_get_compression_info_escapes_quote_in_column_name(self, tmp_path):
        col = "va'l"
        con = duckdb.connect()
        quoted = quote_identifier(col)
        table = con.execute(f"SELECT 1 AS id, 2 AS {quoted}").arrow().read_all()
        con.close()
        path = str(tmp_path / "compression_col_quote.parquet")
        pq.write_table(table, path)

        result = get_compression_info(path, column_name=col)

        assert result, "expected a non-empty compression map, got {} (query silently failed)"
        assert all(k == col for k in result)


# --- core/add/bbox.py's manual `"{col}"` quoting ---------------------------
#
# add_bbox_table (backing Table.add_bbox() in api/table.py) built `"{col}"` by
# hand instead of calling quote_identifier, so it tolerated a space but not an
# embedded `"`. _make_add_bbox_query -- used by _add_bbox_streaming, the
# `gpio add bbox -` stdin/stdout CLI path -- had the identical bug.


class TestApiTableAddBboxQuoting:
    """geoparquet_io/core/add/bbox.py: add_bbox_table -- backs Table.add_bbox()."""

    @adversarial_column
    def test_table_add_bbox_succeeds_with_correct_values(self, column_name):
        from geoparquet_io.api.table import Table

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        quoted = quote_identifier(column_name)
        arrow_table = (
            con.execute(
                f"""
                SELECT * FROM (VALUES
                    (1, ST_AsWKB(ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'))),
                    (2, ST_AsWKB(ST_GeomFromText('POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))')))
                ) AS t(id, {quoted})
                """
            )
            .arrow()
            .read_all()
        )
        con.close()

        table = Table(arrow_table, geometry_column=column_name)
        result = table.add_bbox(column_name="bbox")

        rows = result.table.column("bbox").combine_chunks().to_pylist()
        assert rows[0] == {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
        assert rows[1] == {"xmin": 2.0, "ymin": 2.0, "xmax": 3.0, "ymax": 3.0}


class TestAddBboxStreamingQuoting:
    """geoparquet_io/core/add/bbox.py: _make_add_bbox_query, used by
    _add_bbox_streaming (the `gpio add bbox -` stdin/stdout CLI path).

    _add_bbox_streaming's own geometry-column auto-detection only checks
    STANDARD_GEOMETRY_NAMES (a separate, pre-existing limitation, out of
    scope for this fix), so the hostile name is added to that lookup via
    monkeypatch to let the *real* _add_bbox_streaming run end-to-end rather
    than testing the SQL-building helper in isolation.

    ``force=True`` takes the ``replace_existing`` branch, whose
    ``SELECT * EXCLUDE (...)`` interpolates the *bbox* column name -- so the
    fixture carries a pre-existing bbox column under an equally hostile name,
    holding a sentinel value that the run must overwrite.
    """

    # A bbox column name that only survives correct `""`-doubling quoting.
    HOSTILE_BBOX_COL = 'b"x'

    def _fixture(self, tmp_path, column_name: str, with_stale_bbox: bool) -> str:
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        quoted = quote_identifier(column_name)
        quoted_bbox = quote_identifier(self.HOSTILE_BBOX_COL)
        stale = ", {'xmin': -99.0, 'ymin': -99.0, 'xmax': -99.0, 'ymax': -99.0}"
        rows = ", ".join(
            f"({i + 1}, ST_AsWKB(ST_GeomFromText('{w}')){stale if with_stale_bbox else ''})"
            for i, w in enumerate(POLYGON_WKTS)
        )
        cols = f"id, {quoted}" + (f", {quoted_bbox}" if with_stale_bbox else "")
        table = con.execute(f"SELECT * FROM (VALUES {rows}) AS t({cols})").arrow().read_all()
        con.close()
        return _write_geoparquet(
            table,
            column_name,
            ["Polygon"],
            str(tmp_path / f"streaming_input_{int(with_stale_bbox)}.parquet"),
        )

    @pytest.mark.parametrize("force", [False, True])
    @adversarial_column
    def test_add_bbox_streaming_succeeds_with_correct_values(
        self, tmp_path, monkeypatch, column_name, force
    ):
        from geoparquet_io.core.add import bbox as bbox_module

        monkeypatch.setattr(
            bbox_module,
            "STANDARD_GEOMETRY_NAMES",
            [column_name, *bbox_module.STANDARD_GEOMETRY_NAMES],
        )
        input_path = self._fixture(tmp_path, column_name, with_stale_bbox=force)
        output_path = str(tmp_path / "streaming_output.parquet")

        if force:
            # Premise of the replace_existing branch: the stale value must be
            # there to begin with, so replacing it is observable.
            stale = pq.read_table(input_path).column(self.HOSTILE_BBOX_COL).to_pylist()
            assert stale[0]["xmin"] == -99.0

        bbox_module._add_bbox_streaming(
            input_path=input_path,
            output_path=output_path,
            bbox_column_name=self.HOSTILE_BBOX_COL,
            verbose=False,
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            profile=None,
            force=force,
            geoparquet_version="2.0",
            memory_limit=None,
        )

        table = pq.read_table(output_path)
        # EXCLUDE must have removed the stale column rather than leaving a
        # duplicate behind.
        assert table.column_names.count(self.HOSTILE_BBOX_COL) == 1
        rows = table.column(self.HOSTILE_BBOX_COL).combine_chunks().to_pylist()
        assert rows[0] == {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
        assert rows[1] == {"xmin": 2.0, "ymin": 2.0, "xmax": 3.0, "ymax": 3.0}


# --- Finishing the sweep: commands still broken after the first fix round ---


class TestInspectStatsQuoting:
    """geoparquet_io/core/inspect_utils.py: the per-column stats queries."""

    @adversarial_column
    def test_inspect_stats_succeeds(self, tmp_path, column_name):
        path = _points_fixture(tmp_path, column_name, "inspect_stats.parquet")

        runner = CliRunner()
        result = runner.invoke(inspect, ["stats", path])

        assert result.exit_code == 0, result.output
        assert "Parser Error" not in result.output


class TestCheckSpecQuoting:
    """geoparquet_io/core/validate.py: the data-validation queries.

    A parse error here does not just crash -- it is caught and reported as a
    *failed* spec check, so a hostile column name made gpio emit a wrong
    verdict about a perfectly valid file.
    """

    @adversarial_column
    def test_check_spec_reports_no_false_failures(self, tmp_path, column_name):
        path = _points_fixture(tmp_path, column_name, "check_spec.parquet")

        runner = CliRunner()
        result = runner.invoke(check, ["spec", path])

        assert result.exit_code == 0, result.output
        assert "Parser Error" not in result.output
        assert "0 failed" in result.output


class TestSortHilbertQuoting:
    """geoparquet_io/core/hilbert_order.py: the empty-geometry count and the
    ORDER BY ST_Hilbert query."""

    @adversarial_column
    def test_sort_hilbert_succeeds(self, tmp_path, column_name):
        path = _points_fixture(tmp_path, column_name, "hilbert_input.parquet")
        out = str(tmp_path / "hilbert_output.parquet")

        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", path, out])

        assert result.exit_code == 0, result.output
        assert pq.read_table(out).num_rows == 10


class TestExtractGeoparquetQuoting:
    """geoparquet_io/core/extract.py: the column-projection builders."""

    @adversarial_column
    def test_extract_geoparquet_succeeds(self, tmp_path, column_name):
        path = _points_fixture(tmp_path, column_name, "extract_input.parquet")
        out = str(tmp_path / "extract_output.parquet")

        runner = CliRunner()
        result = runner.invoke(extract, ["geoparquet", path, out])

        assert result.exit_code == 0, result.output
        assert column_name in pq.read_schema(out).names


class TestExtractSpatialFilterQuoting:
    """geoparquet_io/core/extract.py: build_spatial_filter's `--geometry` branch.

    The `--bbox` branch five lines above already used ``quote_identifier``; the
    `--geometry` branch hand-rolled ``f'"{col}"'`` (#936). The name it
    interpolates is the file's own ``geo.primary_column``, so a published
    GeoParquet file chose the text -- the #918/#923 threat model, and the one
    site of that family that lands in a ``WHERE`` clause, where an injected
    disjunct silently disables the filter instead of crashing.
    """

    #: A `geo.primary_column` that closes ST_Intersects, ORs in an
    #: always-true scalar subquery, and reopens ST_Intersects so the template's
    #: own tail still parses. Unquoted it is a *valid* predicate that matches
    #: every row; quoted it is one (nonexistent) identifier.
    BYPASS_COL = (
        "geometry\", ST_GeomFromText('POINT (0 0)')) "
        "OR ((SELECT 42) = 42) "
        'OR ST_Intersects("geometry'
    )

    @pytest.mark.parametrize("column_name", [*ADVERSARIAL_COLUMNS, INJECTION_COL, BYPASS_COL])
    def test_geometry_filter_quotes_the_identifier(self, column_name):
        """The predicate must contain the name as ONE quoted identifier.

        Asserted as an exact string rather than a substring: every hostile name
        here contains its own payload text, so `"payload" in sql` would pass
        even on the vulnerable spelling.
        """
        from geoparquet_io.core.extract import build_spatial_filter

        wkt = "POINT (999 999)"
        sql = build_spatial_filter(None, wkt, {}, column_name)

        assert sql == f"ST_Intersects({quote_identifier(column_name)}, ST_GeomFromText('{wkt}'))"

    @adversarial_column
    def test_cli_geometry_filter_returns_the_right_rows(self, tmp_path, column_name):
        """A hostile-but-honest column name must still filter correctly."""
        path = _points_fixture(tmp_path, column_name, "geom_filter_input.parquet")
        out = str(tmp_path / "geom_filter_output.parquet")

        runner = CliRunner()
        result = runner.invoke(
            extract,
            [
                "geoparquet",
                path,
                out,
                "--geometry",
                "POLYGON ((-1 -1, -1 3, 3 3, 3 -1, -1 -1))",
            ],
        )

        assert result.exit_code == 0, result.output
        # Points march along the diagonal (0 0)..(9 9); four fall in the box.
        assert pq.read_table(out).num_rows == 4

    def test_cli_geometry_filter_injection_cannot_bypass_the_filter(self, tmp_path):
        """End-to-end: a crafted file must not turn `--geometry` into a no-op.

        The fixture declares BOTH the real ``geometry`` column and the payload
        in ``geo.columns`` -- DuckDB only hands back a GEOMETRY (rather than a
        BLOB) for a column the `geo` metadata names, and the injected predicate
        has to bind for the bypass to be observable at all.

        Before the fix this exits 0 having written all ten rows, none of which
        intersect the requested point.
        """
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        values_sql = ", ".join(
            f"({i + 1}, ST_AsWKB(ST_GeomFromText('POINT ({i} {i})')))" for i in range(10)
        )
        table = (
            con.execute(f"SELECT * FROM (VALUES {values_sql}) AS t(id, geometry)")
            .arrow()
            .read_all()
        )
        con.close()

        col_meta = {"encoding": "WKB", "geometry_types": ["Point"], "bbox": [0.0, 0.0, 9.0, 9.0]}
        geo = {
            "version": "1.0.0",
            "primary_column": self.BYPASS_COL,
            "columns": {"geometry": col_meta, self.BYPASS_COL: col_meta},
        }
        path = str(tmp_path / "bypass_input.parquet")
        pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), path)
        out = tmp_path / "bypass_output.parquet"

        runner = CliRunner()
        result = runner.invoke(
            extract, ["geoparquet", path, str(out), "--geometry", "POINT (999 999)"]
        )

        # Nothing in the file intersects (999 999), so no row may be written.
        # Quoting turns the payload into a column name that does not exist, so
        # the run fails cleanly instead of silently returning the whole file.
        leaked = pq.read_table(out).num_rows if out.exists() else 0
        assert leaked == 0, f"spatial filter bypassed: {leaked} non-intersecting rows written"
        assert result.exit_code != 0


class TestAddCommandsQuoting:
    """Every `gpio add` subcommand builds its own geometry expression."""

    @pytest.mark.parametrize(
        "subcommand",
        ["h3", "s2", "a5", "quadkey", "geometry-metrics", "kdtree"],
    )
    @adversarial_column
    def test_add_subcommand_succeeds(self, tmp_path, subcommand, column_name):
        if subcommand == "s2":
            skip_if_geography_unavailable()
        path = _points_fixture(tmp_path, column_name, "add_cmd_input.parquet")
        out = str(tmp_path / "add_cmd_output.parquet")

        runner = CliRunner()
        result = runner.invoke(add, [subcommand, path, out])

        assert result.exit_code == 0, result.output
        assert column_name in pq.read_schema(out).names


class TestConvertQuoting:
    """geoparquet_io/core/convert.py + format_writers.py."""

    @pytest.mark.parametrize("fmt", ["geojson", "csv"])
    @adversarial_column
    def test_convert_succeeds(self, tmp_path, fmt, column_name):
        path = _points_fixture(tmp_path, column_name, "convert_input.parquet")
        out = str(tmp_path / f"convert_output.{fmt}")

        runner = CliRunner()
        result = runner.invoke(convert, [fmt, path, out])

        assert result.exit_code == 0, result.output
        with open(out) as fh:
            assert fh.read().strip()


class TestSortColumnQuoting:
    """geoparquet_io/core/sort_by_column.py: the ORDER BY clause takes the
    column name straight from the CLI."""

    @adversarial_column
    def test_sort_column_succeeds(self, tmp_path, column_name):
        base = _points_fixture(tmp_path, "geometry", "sort_col_input.parquet")
        table = pq.read_table(base)
        renamed = table.rename_columns([column_name, "geometry"])
        renamed = renamed.replace_schema_metadata(table.schema.metadata)
        src = str(tmp_path / "sort_col_renamed.parquet")
        pq.write_table(renamed, src)
        out = str(tmp_path / "sort_col_output.parquet")

        runner = CliRunner()
        result = runner.invoke(sort, ["column", src, out, column_name])

        assert result.exit_code == 0, result.output
        assert pq.read_table(out).num_rows == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
