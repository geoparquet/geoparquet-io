"""``core/column_selection``: one folding rule, one suffixer, one exclusion guard (#1367, #1109).

Three tables pin the owner's three questions -- does this name mean that column,
what may a computed column be called, which columns survive an exclusion -- and
the cross-command tests pin that the names a user sees in an output file are the
ones every path produced before the copies were deleted (column names are output
bytes). The #1109 tests drive the real CLI.
"""

from __future__ import annotations

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import column_selection as cs
from geoparquet_io.core.exceptions import InvalidParameterError

# ---------------------------------------------------------------------------
# fold / matching_columns / find_column / has_column / same_column
# ---------------------------------------------------------------------------

FOLD_CASES = [
    ("height", "height"),
    ("HEIGHT", "height"),
    ("Height", "height"),
    # DuckDB folds ASCII only: a non-ASCII capital is a different name.
    ("STRASSE", "strasse"),
    ("Straße", "straße"),
    ("É", "É"),
    ("", ""),
]


@pytest.mark.parametrize(("name", "folded"), FOLD_CASES)
def test_fold_table(name, folded):
    assert cs.fold(name) == folded


MATCH_CASES = [
    # (name, columns, matches)
    ("id", ["id", "name"], ["id"]),
    ("ID", ["id", "name"], ["id"]),  # a unique folded match
    ("id", ["id", "ID"], ["id"]),  # exact first
    ("ID", ["id", "ID"], ["ID"]),
    ("Id", ["id", "ID"], ["id", "ID"]),  # ambiguous: every candidate, schema order
    ("nope", ["id", "name"], []),
    ("É", ["é"], []),  # not an ASCII fold
    ("É", ["É"], ["É"]),
]


@pytest.mark.parametrize(("name", "columns", "matches"), MATCH_CASES)
def test_matching_columns_table(name, columns, matches):
    assert cs.matching_columns(name, columns) == matches
    assert cs.has_column(columns, name) is bool(matches)


def test_find_column_returns_the_schema_spelling():
    assert cs.find_column("HEIGHT", ["id", "Height"]) == "Height"
    assert cs.find_column("nope", ["id"]) is None


def test_find_column_raises_naming_the_candidates_when_ambiguous():
    with pytest.raises(InvalidParameterError, match=r"Id matches ID, id"):
        cs.find_column("Id", ["id", "ID"], option_name="--column")


def test_bound_column_refuses_what_a_sql_binder_cannot_tell_apart():
    # An exact hit is not enough once a second spelling folds to it: DuckDB
    # binds "HEIGHT" to the first of the two.
    with pytest.raises(InvalidParameterError, match=r"HEIGHT matches HEIGHT, height"):
        cs.bound_column("HEIGHT", ["height", "HEIGHT"], option_name="metric")
    assert cs.bound_column("HEIGHT", ["id", "Height"], option_name="metric") == "Height"
    assert cs.bound_column("x", ["id"], option_name="metric") is None


def test_same_column_is_the_fold_rule():
    assert cs.same_column("BBOX", "bbox")
    assert not cs.same_column("É", "é")


# ---------------------------------------------------------------------------
# unique_name
# ---------------------------------------------------------------------------

UNIQUE_CASES = [
    # (base, taken, start, expected)
    ("bbox", ["id", "geometry"], 1, "bbox"),
    ("bbox", ["id", "bbox"], 1, "bbox_1"),
    ("bbox", ["id", "bbox", "bbox_1"], 1, "bbox_2"),
    ("bbox", ["BBOX"], 1, "bbox_1"),  # DuckDB binds case-insensitively
    ("bbox", ["bbox", "BBOX_1"], 1, "bbox_2"),
    ("count_a", ["count_a"], 2, "count_a_2"),  # aggregate's historic numbering
    ("count_a", ["count_a", "count_a_2"], 2, "count_a_3"),
    ("Id", ["id"], 1, "Id_1"),  # the base keeps its own spelling
    ("x", [], 1, "x"),
]


@pytest.mark.parametrize(("base", "taken", "start", "expected"), UNIQUE_CASES)
def test_unique_name_table(base, taken, start, expected):
    assert cs.unique_name(base, taken, start=start) == expected


def test_unique_name_does_not_change_taken():
    taken = {"a"}
    cs.unique_name("a", taken)
    assert taken == {"a"}


def test_unique_name_accepts_any_iterable_once():
    assert cs.unique_name("a", iter(["a"])) == "a_1"


# ---------------------------------------------------------------------------
# exclude_columns
# ---------------------------------------------------------------------------

COLS = ["id", "name", "geometry"]


def test_exclude_columns_keeps_schema_order():
    assert cs.exclude_columns(COLS, ["name"]) == ["id", "geometry"]


def test_exclude_columns_nothing_requested_keeps_everything():
    assert cs.exclude_columns(COLS, None) == COLS
    assert cs.exclude_columns(COLS, []) == COLS


def test_exclude_columns_resolves_a_case_slip():
    assert cs.exclude_columns(COLS, ["NAME"]) == ["id", "geometry"]


def test_exclude_columns_rejects_an_unknown_name():
    with pytest.raises(InvalidParameterError, match="not found"):
        cs.exclude_columns(COLS, ["nope"])


def test_exclude_columns_refuses_to_drop_everything_with_one_message():
    with pytest.raises(InvalidParameterError, match="would leave no columns at all") as exc:
        cs.exclude_columns(COLS, ["id", "name", "geometry"])
    assert "available: id, name, geometry" in str(exc.value)


def test_exclude_columns_protects_a_column_with_a_warning(caplog):
    with caplog.at_level("WARNING"):
        kept = cs.exclude_columns(COLS, ["geometry", "id"], protect="geometry")
    assert kept == ["name", "geometry"]
    assert "Cannot exclude 'geometry' column" in caplog.text


def test_exclude_columns_protected_alone_keeps_everything():
    assert cs.exclude_columns(COLS, ["geometry"], protect="geometry") == COLS


def test_exclude_columns_unprotected_may_drop_the_geometry():
    assert cs.exclude_columns(COLS, ["geometry"]) == ["id", "name"]


# ---------------------------------------------------------------------------
# Cross-command: the names a user sees are the names every path produced before
# ---------------------------------------------------------------------------


def _names(path):
    return pq.read_schema(str(path)).names


@pytest.fixture
def collision_file(tmp_path):
    """A GeoParquet file with BBOX, bbox_1 and a 'cat' column of colliding values."""
    raw = tmp_path / "raw.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    con.execute(
        f"""COPY (SELECT i AS id, ST_Point(i, i) AS geometry,
            CASE WHEN i % 3 = 0 THEN 'a b' WHEN i % 3 = 1 THEN 'a_b' ELSE 'A-B' END AS cat,
            i AS "BBOX", 7 AS bbox_1 FROM range(6) t(i)) TO '{raw}'"""
    )
    out = tmp_path / "collide.parquet"
    result = CliRunner().invoke(cli, ["convert", "geoparquet", str(raw), str(out)])
    assert result.exit_code == 0, result.output
    return out


def test_add_bbox_names_the_computed_column_beside_every_collision(collision_file, tmp_path):
    out = tmp_path / "bbox.parquet"
    result = CliRunner().invoke(
        cli, ["add", "bbox", str(collision_file), str(out), "--bbox-name", "bbox"]
    )
    assert result.exit_code == 0, result.output
    # `convert geoparquet` already took bbox_2 for its own covering column.
    assert _names(out) == ["id", "geometry", "cat", "BBOX", "bbox_1", "bbox_2", "bbox_3"]


def test_aggregate_breakdown_numbers_colliding_values_from_two(collision_file, tmp_path):
    out = tmp_path / "agg.parquet"
    result = CliRunner().invoke(
        cli,
        ["process", "aggregate", "h3", str(collision_file), str(out), "--resolution", "3"]
        + ["--breakdown", "cat"],
    )
    assert result.exit_code == 0, result.output
    assert _names(out) == [
        "h3_cell",
        "count",
        "count_a_b",
        "count_a_b_2",
        "count_a_b_3",
        "geometry",
    ]


# ---------------------------------------------------------------------------
# #1109: every spelling of a column name is resolved by the one rule
# ---------------------------------------------------------------------------


@pytest.fixture
def case_variant_file(tmp_path):
    raw = tmp_path / "variants.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    con.execute(
        f"""COPY (SELECT i AS height, (i * 10)::DOUBLE AS "HEIGHT", ST_Point(i, i) AS "PT",
            ST_Point(i, i) AS geometry FROM range(5) t(i)) TO '{raw}'"""
    )
    out = tmp_path / "variants_geo.parquet"
    result = CliRunner().invoke(cli, ["convert", "geoparquet", str(raw), str(out)])
    assert result.exit_code == 0, result.output
    return out


def _aggregate(source, out, *extra):
    return CliRunner().invoke(
        cli,
        ["process", "aggregate", "h3", str(source), str(out), "--resolution", "3", *extra],
    )


@pytest.mark.xfail(strict=True, reason="#1109")
def test_1109_metric_that_two_columns_fold_to_is_refused_not_bound_silently(
    case_variant_file, tmp_path
):
    result = _aggregate(case_variant_file, tmp_path / "o.parquet", "--metric", "sum:HEIGHT")
    assert result.exit_code != 0
    assert "HEIGHT matches HEIGHT, height" in result.output


@pytest.mark.xfail(strict=True, reason="#1109")
def test_1109_bucket_point_is_matched_like_a_metric(case_variant_file, tmp_path):
    # `--bucket-point pt` against a `PT` column: DuckDB binds it, so gpio accepts it.
    result = _aggregate(case_variant_file, tmp_path / "o.parquet", "--bucket-point", "pt")
    assert result.exit_code == 0, result.output


@pytest.mark.xfail(strict=True, reason="#1109")
def test_1109_reserved_internal_names_are_reserved_in_any_case(tmp_path):
    from geoparquet_io.core.process.aggregate.grid_common import build_exclude_clause

    con = duckdb.connect()
    con.execute('CREATE TABLE t AS SELECT 1 AS a, 2 AS "__PT"')
    assert build_exclude_clause(con, "t", ("__pt",)) == ' EXCLUDE ("__PT")'
