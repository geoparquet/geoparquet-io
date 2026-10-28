"""Friendly errors for --metric/--breakdown columns that don't exist in the input.

Previously a missing metric column surfaced as a raw DuckDB binder error. The
common trap is `--metric count`: `count` is produced automatically for every
bucket, so requesting it (on input without a literal `count` column) now gets a
message explaining that, instead of failing on the generated `SUM("count")`.
"""

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.aggregate.common import (
    parse_metrics,
    validate_agg_columns,
)

# ---------------------------------------------------------------------------
# Unit: validate_agg_columns
# ---------------------------------------------------------------------------


def test_count_metric_gets_automatic_count_hint():
    with pytest.raises(InvalidParameterError, match="automatically"):
        validate_agg_columns({"geometry", "height"}, parse_metrics("count"), None)


def test_func_count_metric_gets_hint_too():
    with pytest.raises(InvalidParameterError, match="automatically"):
        validate_agg_columns({"geometry"}, parse_metrics("avg:count"), None)


def test_count_metric_allowed_when_column_exists():
    # A literal `count` column (e.g. re-aggregating an aggregate) is legitimate.
    validate_agg_columns({"geometry", "count"}, parse_metrics("sum:count"), None)


def test_missing_metric_column_lists_available():
    with pytest.raises(InvalidParameterError, match="'altitude' not found") as exc:
        validate_agg_columns({"geometry", "height"}, parse_metrics("avg:altitude"), None)
    assert "height" in str(exc.value)


def test_missing_breakdown_column_errors():
    with pytest.raises(InvalidParameterError, match="Breakdown column 'crop'"):
        validate_agg_columns({"geometry", "height"}, [], "crop")


def test_valid_columns_pass():
    validate_agg_columns({"geometry", "height", "crop"}, parse_metrics("avg:height"), "crop")


# ---------------------------------------------------------------------------
# Name matching follows DuckDB: ASCII case-insensitive, nothing more
# ---------------------------------------------------------------------------


def test_mixed_case_metric_column_resolves():
    # DuckDB binds SUM("HEIGHT") against a `Height` column, so validation must too.
    validate_agg_columns({"geometry", "Height"}, parse_metrics("sum:HEIGHT"), None)


def test_mixed_case_breakdown_column_resolves():
    validate_agg_columns({"geometry", "CropType"}, [], "croptype")


def test_count_metric_allowed_when_column_exists_in_other_case():
    # A real `COUNT` column must not be mistaken for the reserved automatic count.
    validate_agg_columns({"geometry", "COUNT"}, parse_metrics("sum:count"), None)


def test_non_ascii_case_is_not_folded():
    # DuckDB does not fold non-ASCII: "héight" does not bind to `HÉIGHT`.
    with pytest.raises(InvalidParameterError, match="not found"):
        validate_agg_columns({"geometry", "HÉIGHT"}, parse_metrics("sum:héight"), None)


def test_internal_aliases_are_not_listed_as_available():
    with pytest.raises(InvalidParameterError, match="not found") as exc:
        validate_agg_columns({"__pt", "__key", "height"}, parse_metrics("sum:area"), None)
    assert "__pt" not in str(exc.value)
    assert "height" in str(exc.value)


# ---------------------------------------------------------------------------
# Grid query builder applies the validation (no grid extension execution)
# ---------------------------------------------------------------------------


def _build(metric=None, breakdown=None, source="SELECT 1 AS height, 'x' AS crop, NULL AS __pt"):
    from geoparquet_io.core.process.aggregate.by_a5 import A5_SCHEME
    from geoparquet_io.core.process.aggregate.grid_common import build_grid_query

    con = duckdb.connect()
    try:
        return build_grid_query(
            con,
            A5_SCHEME,
            source,
            5,
            "a5_cell",
            metric,
            breakdown,
            20,
            "none",
        )
    finally:
        con.close()


def test_grid_query_count_metric_hint():
    with pytest.raises(InvalidParameterError, match="automatically"):
        _build(metric="count")


def test_grid_query_missing_metric_column():
    with pytest.raises(InvalidParameterError, match="'area' not found"):
        _build(metric="sum:area")


def test_grid_query_missing_breakdown_column():
    with pytest.raises(InvalidParameterError, match="Breakdown column"):
        _build(breakdown="croptype")


def test_grid_query_valid_columns_build():
    sql = _build(metric="avg:height")
    assert 'AVG("height") AS "avg_height"' in sql


def test_grid_query_mixed_case_metric_builds():
    sql = _build(metric="avg:HEIGHT", source="SELECT 1 AS Height, NULL AS __pt")
    assert 'AVG("HEIGHT")' in sql


def test_grid_query_missing_metric_omits_internal_alias():
    with pytest.raises(InvalidParameterError, match="'area' not found") as exc:
        _build(metric="sum:area")
    assert "__pt" not in str(exc.value)


# ---------------------------------------------------------------------------
# Admin path applies the validation (fake dataset, no network)
# ---------------------------------------------------------------------------


class _FakeAdminDataset:
    def __init__(self, path, fail_on_source=False):
        self._path = path
        self._fail_on_source = fail_on_source

    def supports_per_level_sources(self):
        return True

    def get_source_for_level(self, level):
        if self._fail_on_source:
            # Stands in for the multi-GB boundary-cache download.
            raise AssertionError("admin boundaries resolved before validating columns")
        return str(self._path)

    def get_level_column_mapping(self):
        return {"country": "country"}

    def get_geometry_column(self):
        return "geometry"

    def get_bbox_column(self):
        return None

    def configure_s3(self, con):
        pass

    def get_s3_config(self):
        return {"profile": None}


@pytest.fixture
def fake_admin_dataset(tmp_path, monkeypatch):
    admin_path = tmp_path / "admin.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""
        COPY (
            SELECT 'AA' AS country,
                   ST_GeomFromText('POLYGON((0 40, 20 40, 20 60, 0 60, 0 40))') AS geometry
        ) TO '{admin_path}' (FORMAT PARQUET)
        """
    )
    con.close()
    monkeypatch.setattr(
        "geoparquet_io.core.process.aggregate.by_admin._setup_admin_dataset",
        lambda dataset, verbose, levels: (_FakeAdminDataset(admin_path), None),
    )


def _write_points(path):
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(10.0, 50.0) AS geometry, 4.0 AS height
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


@pytest.mark.usefixtures("fake_admin_dataset")
def test_admin_count_metric_hint(tmp_path):
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    src = tmp_path / "pts.parquet"
    _write_points(src)
    with pytest.raises(InvalidParameterError, match="automatically"):
        aggregate_by_admin(str(src), str(tmp_path / "o.parquet"), level="country", metric="count")


def test_admin_validates_before_resolving_boundaries(tmp_path, monkeypatch):
    """A typo'd column must fail before the admin boundary cache is fetched."""
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    monkeypatch.setattr(
        "geoparquet_io.core.process.aggregate.by_admin._setup_admin_dataset",
        lambda dataset, verbose, levels: (_FakeAdminDataset(None, fail_on_source=True), None),
    )
    src = tmp_path / "pts.parquet"
    _write_points(src)
    with pytest.raises(InvalidParameterError, match="'altitude' not found"):
        aggregate_by_admin(
            str(src), str(tmp_path / "o.parquet"), level="country", metric="sum:altitude"
        )


# ---------------------------------------------------------------------------
# CLI surfaces the message cleanly (needs grid extension)
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.network
def test_cli_count_metric_clean_error(tmp_path):
    src = tmp_path / "pts.parquet"
    _write_points(src)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "aggregate",
            "a5",
            str(src),
            str(tmp_path / "o.parquet"),
            "--resolution",
            "5",
            "--metric",
            "count",
        ],
    )
    assert result.exit_code != 0
    assert "automatically" in result.output
    assert "Binder" not in result.output
