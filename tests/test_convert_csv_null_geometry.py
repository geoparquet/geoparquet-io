"""CSV/WKT and lat/lon rows with NULL geometry must survive conversion (#655).

`gpio convert` dropped them silently — `WHERE <wkt> IS NOT NULL` was applied to
the conversion query, not just to the bounds pass — so attribute data attached
to a missing geometry disappeared with no warning. Every other input path keeps
such rows, and after #651 NULL and empty geometry sort last rather than being
dropped or failing the conversion.

`--skip-invalid` keeps its documented meaning: rows whose WKT is present but
unparsable are still dropped. Only *missing* geometry is retained.
"""

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli


def _convert(source, output, *extra):
    return CliRunner().invoke(cli, ["convert", str(source), str(output), *extra])


def _rows(output):
    """(id, geometry_is_null) in stored order."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    try:
        return con.execute(f"SELECT id, geometry IS NULL FROM '{output}'").fetchall()
    finally:
        con.close()


def _write(path, text):
    path.write_text(text, encoding="utf-8")
    return path


class TestNullWktRows:
    def test_null_wkt_row_is_kept(self, tmp_path):
        source = _write(tmp_path / "wkt.csv", "id,geom\n1,POINT(1 1)\n2,\n3,POINT(5 5)\n")
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--wkt-column", "geom")

        assert result.exit_code == 0, result.output
        rows = _rows(output)
        assert sorted(i for i, _ in rows) == [1, 2, 3]
        assert [(i, is_null) for i, is_null in rows if is_null] == [(2, True)]

    def test_null_geometry_sorts_last(self, tmp_path):
        """#651's unorderable-first ordering has to cover these rows too."""
        source = _write(
            tmp_path / "wkt.csv",
            "id,geom\n1,POINT(9 9)\n2,\n3,POINT(1 1)\n4,\n5,POINT(5 5)\n",
        )
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--wkt-column", "geom")

        assert result.exit_code == 0, result.output
        rows = _rows(output)
        assert len(rows) == 5
        nulls = [is_null for _, is_null in rows]
        assert nulls == sorted(nulls), f"NULL rows are not last: {rows}"
        assert [i for i, is_null in rows if not is_null] == [3, 5, 1]  # Hilbert order

    def test_all_null_wkt_converts(self, tmp_path):
        source = _write(tmp_path / "wkt.csv", "id,geom\n1,\n2,\n")
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--wkt-column", "geom")

        assert result.exit_code == 0, result.output
        assert len(_rows(output)) == 2

    def test_unparsable_wkt_still_errors_without_skip_invalid(self, tmp_path):
        source = _write(tmp_path / "wkt.csv", "id,geom\n1,POINT(1 1)\n2,NOT WKT\n")
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--wkt-column", "geom")

        assert result.exit_code != 0


class TestSkipInvalid:
    def test_skip_invalid_drops_unparsable_but_keeps_null(self, tmp_path):
        source = _write(
            tmp_path / "wkt.csv",
            "id,geom\n1,POINT(1 1)\n2,\n3,NOT WKT\n4,POINT(5 5)\n",
        )
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--wkt-column", "geom", "--skip-invalid")

        assert result.exit_code == 0, result.output
        rows = _rows(output)
        assert sorted(i for i, _ in rows) == [1, 2, 4]  # 3 dropped, 2 retained
        assert [(i, is_null) for i, is_null in rows if is_null] == [(2, True)]

    def test_skip_invalid_output_is_hilbert_ordered(self, tmp_path):
        """--skip-invalid must not silently disable Hilbert ordering (#1157)."""
        source = _write(
            tmp_path / "wkt.csv",
            "id,geom\n1,POINT(9 9)\n2,NOT WKT\n3,POINT(1 1)\n4,\n5,POINT(5 5)\n",
        )
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--wkt-column", "geom", "--skip-invalid")

        assert result.exit_code == 0, result.output
        rows = _rows(output)
        assert sorted(i for i, _ in rows) == [1, 3, 4, 5]  # 2 dropped, NULL kept
        assert [i for i, is_null in rows if not is_null] == [3, 5, 1]  # Hilbert order
        nulls = [is_null for _, is_null in rows]
        assert nulls == sorted(nulls), f"NULL rows are not last: {rows}"

    def test_skip_invalid_builder_emits_hilbert_order_by(self):
        """The skip_invalid branch returns ORDER BY terms like every other path."""
        from geoparquet_io.core.convert import _build_csv_conversion_query

        geom_info = {"type": "wkt", "wkt_column": "geom", "csv_read": "read_csv_auto('x.csv')"}
        _, order_by = _build_csv_conversion_query(geom_info, False, (0.0, 0.0, 10.0, 10.0), True)
        assert order_by is not None
        assert "ST_Hilbert" in order_by

    def test_skip_invalid_with_skip_hilbert_emits_no_order_by(self):
        """--skip-hilbert still wins over the restored ordering."""
        from geoparquet_io.core.convert import _build_csv_conversion_query

        geom_info = {"type": "wkt", "wkt_column": "geom", "csv_read": "read_csv_auto('x.csv')"}
        _, order_by = _build_csv_conversion_query(geom_info, True, None, True)
        assert order_by is None


class TestSkipInvalidOrdering:
    """More of #1157: the restored ordering on the paths the first fix missed."""

    def test_a_wkt_column_named_geometry(self, tmp_path):
        """The parsed value used to be named `geometry` too: a duplicate EXCLUDE entry."""
        source = _write(
            tmp_path / "wkt.csv",
            "id,Geometry\n1,POINT(9 9)\n2,NOT WKT\n3,POINT(1 1)\n4,\n5,POINT(5 5)\n",
        )
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--skip-invalid")

        assert result.exit_code == 0, result.output
        rows = _rows(output)
        assert [i for i, is_null in rows if not is_null] == [3, 5, 1]
        assert [i for i, is_null in rows if is_null] == [4]

    def test_bounds_come_from_the_parsed_rows(self, tmp_path, monkeypatch):
        """The CSV is TRY-parsed once, into the temp table, not again for bounds."""
        from geoparquet_io.core import convert as convert_module

        def no_second_parse(*args, **kwargs):
            raise AssertionError("--skip-invalid WKT must not re-parse the CSV for bounds")

        monkeypatch.setattr(convert_module, "_calculate_csv_bounds", no_second_parse)
        source = _write(
            tmp_path / "wkt.csv",
            "id,geom\n1,POINT(9 9)\n2,NOT WKT\n3,POINT(1 1)\n5,POINT(5 5)\n",
        )
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--wkt-column", "geom", "--skip-invalid")

        assert result.exit_code == 0, result.output
        assert [i for i, _ in _rows(output)] == [3, 5, 1]

    def test_lat_lon_output_is_hilbert_ordered(self, tmp_path):
        """--skip-invalid used to switch ordering off for lat/lon CSVs as well."""
        source = _write(
            tmp_path / "ll.csv",
            "id,lat,lon\n1,9,9\n2,1,1\n3,5,5\n4,,\n",
        )
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--skip-invalid")

        assert result.exit_code == 0, result.output
        rows = _rows(output)
        assert [i for i, is_null in rows if not is_null] == [2, 3, 1]
        assert rows[-1] == (4, True)


class TestNonFiniteBounds:
    """A non-finite envelope must fall back to an unordered query (#1157).

    ``POINT(1e400 1)`` parses to an infinite coordinate, so the dataset bounds
    come back as ``inf``; interpolated as a Python float that renders as the
    bare token ``inf`` inside ``ST_MakeEnvelope`` and fails binding. (The
    later geo-stats pass still cannot digest such a geometry — a separate,
    pre-existing failure — so the guard is tested at the query level.)
    """

    def test_nonfinite_bounds_warn_and_are_discarded(self, caplog):
        import logging

        from geoparquet_io.core.convert import _usable_bounds

        with caplog.at_level(logging.WARNING):
            assert _usable_bounds((1.0, 1.0, float("inf"), 5.0)) is None
        # The no-bounds warning ends the same way; name the non-finite one.
        assert "NaN or infinite" in caplog.text

        caplog.clear()
        with caplog.at_level(logging.WARNING):
            assert _usable_bounds((float("nan"), 1.0, 5.0, 5.0)) is None
        assert "NaN or infinite" in caplog.text

    def test_finite_bounds_pass_through_silently(self, caplog):
        import logging

        from geoparquet_io.core.convert import _usable_bounds

        with caplog.at_level(logging.WARNING):
            assert _usable_bounds((1.0, 1.0, 5.0, 5.0)) == (1.0, 1.0, 5.0, 5.0)
        assert caplog.text == ""

    def test_wkt_inf_coordinate_builds_an_unordered_query(self, tmp_path, caplog):
        """End to end through _convert_csv_path: no ORDER BY, no binder error."""
        import logging

        from geoparquet_io.core.convert import _convert_csv_path
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        source = _write(
            tmp_path / "wkt.csv",
            "id,geom\n1,POINT(1 1)\n2,POINT(1e400 1)\n3,POINT(5 5)\n",
        )
        con = get_duckdb_connection()
        try:
            with caplog.at_level(logging.WARNING):
                query, _, order_by = _convert_csv_path(
                    con, str(source), None, "geom", None, None, "EPSG:4326", False, False, False
                )
            assert order_by is None
            assert "NaN or infinite" in caplog.text
            assert len(con.execute(query).fetchall()) == 3
        finally:
            con.close()


class TestNullLatLon:
    def test_null_lat_lon_row_is_kept(self, tmp_path):
        source = _write(tmp_path / "ll.csv", "id,lat,lon\n1,1,1\n2,,\n3,5,5\n")
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--lat-column", "lat", "--lon-column", "lon")

        assert result.exit_code == 0, result.output
        rows = _rows(output)
        assert sorted(i for i, _ in rows) == [1, 2, 3]
        assert [(i, is_null) for i, is_null in rows if is_null] == [(2, True)]

    def test_partially_missing_coordinate_is_kept_as_null(self, tmp_path):
        source = _write(tmp_path / "ll.csv", "id,lat,lon\n1,1,1\n2,5,\n")
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--lat-column", "lat", "--lon-column", "lon")

        assert result.exit_code == 0, result.output
        assert [(i, is_null) for i, is_null in _rows(output) if is_null] == [(2, True)]


class TestBoundsIgnoreNullRows:
    def test_bounds_come_from_real_geometry_only(self, tmp_path):
        """NULL rows must not widen or void the Hilbert envelope."""
        source = _write(tmp_path / "wkt.csv", "id,geom\n1,POINT(0 0)\n2,\n3,POINT(10 10)\n")
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--wkt-column", "geom")

        assert result.exit_code == 0, result.output
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        try:
            xmin, ymax = con.execute(
                f"SELECT MIN(ST_XMin(geometry)), MAX(ST_YMax(geometry)) FROM '{output}'"
            ).fetchall()[0]
        finally:
            con.close()
        assert (xmin, ymax) == pytest.approx((0.0, 10.0))


class TestWarningsMatchBehavior:
    """The NULL-row warnings must not promise a skip that no longer happens."""

    def test_wkt_warning_does_not_claim_rows_are_skipped(self, tmp_path, caplog):
        import logging

        source = _write(tmp_path / "wkt.csv", "id,geom\n1,POINT(1 1)\n2,\n")
        output = tmp_path / "out.parquet"

        with caplog.at_level(logging.WARNING):
            result = _convert(source, output, "--wkt-column", "geom")

        assert result.exit_code == 0, result.output
        message = "\n".join(r.message for r in caplog.records)
        assert "1 row" in message and "NULL" in message
        assert "skipped" not in message

    def test_latlon_warning_does_not_claim_rows_are_skipped(self, tmp_path, caplog):
        import logging

        source = _write(tmp_path / "ll.csv", "id,lat,lon\n1,10,10\n2,,\n3,20,20\n")
        output = tmp_path / "out.parquet"

        with caplog.at_level(logging.WARNING):
            result = _convert(source, output, "--lat-column", "lat", "--lon-column", "lon")

        assert result.exit_code == 0, result.output
        assert len(_rows(output)) == 3
        message = "\n".join(r.message for r in caplog.records)
        assert "skipped" not in message


class TestAllNullLatLon:
    def test_all_null_lat_lon_converts(self, tmp_path):
        """The lat/lon analogue of all-NULL WKT: range validation has nothing to
        compare, which used to surface as a raw TypeError from `None < -90`."""
        source = _write(tmp_path / "ll.csv", "id,lat,lon\n1,,\n2,,\n")
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--lat-column", "lat", "--lon-column", "lon")

        assert result.exit_code == 0, result.output
        rows = _rows(output)
        assert len(rows) == 2
        assert all(is_null for _, is_null in rows)


class TestSentinelFreeSkipInvalid:
    def test_column_named_like_the_old_sentinel_converts(self, tmp_path):
        """The skip_invalid path used to synthesize __gpio_wkt_missing, which a
        column of that name collided with."""
        source = _write(
            tmp_path / "wkt.csv",
            "id,geom,__gpio_wkt_missing\n1,POINT(1 1),x\n2,,y\n",
        )
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--wkt-column", "geom", "--skip-invalid")

        assert result.exit_code == 0, result.output
        assert len(_rows(output)) == 2


class TestPerAxisRangeValidation:
    """One axis being entirely NULL must not silence the other's range check.

    `MIN`/`MAX` ignore NULLs, so a column of nothing but empty values aggregates
    to None while the *other* column may still hold measurable — and invalid —
    coordinates. Guarding both axes together skipped a validation `main`
    performed, so each is guarded on its own.
    """

    def test_bad_lat_still_caught_when_lon_is_all_null(self, tmp_path):
        source = _write(tmp_path / "ll.csv", "id,lat,lon\n1,200,\n2,300,\n")
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--lat-column", "lat", "--lon-column", "lon")

        assert result.exit_code != 0
        assert "latitude" in result.output

    def test_bad_lon_still_caught_when_lat_is_all_null(self, tmp_path):
        source = _write(tmp_path / "ll.csv", "id,lat,lon\n1,,500\n2,,600\n")
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--lat-column", "lat", "--lon-column", "lon")

        assert result.exit_code != 0
        assert "longitude" in result.output

    def test_verbose_survives_a_wholly_null_axis(self, tmp_path):
        """The verbose range debug formats all four bounds with `:.6f`, which the
        combined early return used to shield from None."""
        source = _write(tmp_path / "ll.csv", "id,lat,lon\n1,10,\n2,20,\n")
        output = tmp_path / "out.parquet"

        result = _convert(source, output, "--lat-column", "lat", "--lon-column", "lon", "--verbose")

        assert result.exit_code == 0, result.output
        assert len(_rows(output)) == 2
