"""Every extract backend rejects a blank column entry from the Python API too.

The core-side sibling of ``tests/test_cli_column_list_guard.py``. That module
covers the Click callback, which fires during parameter processing -- but
**the Python API never goes through Click**. #973 gave that rationale for
adding ``validate_bigquery_columns`` alongside the callback, and it applies
verbatim to the other backends, which had only the Click guard (#980):

- ``ops.from_carto(..., include_cols="id,,name")`` reached
  ``_build_carto_query`` and raised a raw
  ``ValueError: cannot quote an empty SQL identifier`` traceback, after a
  network round-trip;
- ``ops.from_arcgis(..., include_cols="name,,pop")`` sent
  ``outFields=name,,pop`` to the server -- no local error at all.

So every test here drives the **Python API**, not the CLI. The network is
mocked out and each test asserts the failure happens *before* the mock that
stands in for a request is reached, since fast failure is half the point.
"""

from __future__ import annotations

from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.api import ops
from geoparquet_io.core import arcgis, carto, pmtiles
from geoparquet_io.core.column_selection import reject_blank_column_entries, split_column_list
from geoparquet_io.core.exceptions import InvalidParameterError

CARTO_URL = "https://example.carto.com/api/v2/sql"

# A Carto ``SELECT * ... LIMIT 0`` schema probe response.
CARTO_FIELDS = {
    "cartodb_id": {"type": "number"},
    "Owner": {"type": "string"},
    "the_geom": {"type": "geometry"},
}


def _carto_payload(sql: str) -> dict:
    """Stand in for :func:`carto._carto_sql_json` for the two probe queries."""
    if "IS NOT NULL" in sql:
        return {"rows": [{"has_geom": 1}]}
    return {"fields": CARTO_FIELDS, "rows": []}


def _patch_carto_probes(**kwargs):
    """Answer Carto's schema/geometry probes; fail loudly on a data fetch."""
    return mock.patch.object(
        carto,
        "_carto_sql_json",
        side_effect=lambda url, sql, *a, **kw: _carto_payload(sql),
        **kwargs,
    )


def _layer_info() -> arcgis.ArcGISLayerInfo:
    return arcgis.ArcGISLayerInfo(
        name="parcels",
        geometry_type="esriGeometryPolygon",
        spatial_reference={"wkid": 4326},
        fields=[
            {"name": "OBJECTID", "type": "esriFieldTypeOID"},
            {"name": "name", "type": "esriFieldTypeString"},
            {"name": "pop", "type": "esriFieldTypeInteger"},
        ],
        max_record_count=1000,
        total_count=10,
    )


ARCGIS_URL = "https://example.com/arcgis/rest/services/x/FeatureServer/0"


class TestSplitColumnList:
    """The splitter itself: one place that decides "unset" from "blank entry".

    Every backend reads ``--include-cols``/``--exclude-cols`` through this, so
    the distinction is pinned here once rather than re-derived per backend.
    ``resolve_columns_against_schema`` has its own cases in
    ``tests/test_extract_bigquery.py``, against a schema that module produces.
    """

    @pytest.mark.parametrize("value", [None, ""])
    def test_a_wholly_empty_value_is_unset_not_blank(self, value):
        assert split_column_list(value, "--include-cols") is None

    def test_entries_are_stripped(self):
        assert split_column_list(" id , name ", "--include-cols") == ["id", "name"]

    def test_a_single_name_is_a_one_entry_list(self):
        assert split_column_list("id", "--include-cols") == ["id"]

    @pytest.mark.parametrize("value", ["id,,name", "   ", ",id", "id,", "id, ,name"])
    def test_a_blank_entry_is_rejected(self, value):
        with pytest.raises(InvalidParameterError, match="empty or whitespace-only"):
            split_column_list(value, "--exclude-cols")

    def test_the_option_is_named_in_the_message(self):
        with pytest.raises(InvalidParameterError) as exc:
            split_column_list("id,,name", "--exclude-cols")
        assert "--exclude-cols" in str(exc.value)

    @pytest.mark.parametrize("requested", [None, []])
    def test_nothing_requested_is_nothing_to_reject(self, requested):
        """``core.extract.validate_columns`` calls the checker on an unset list."""
        assert reject_blank_column_entries(requested, "--include-cols") is None


class TestCartoBlankEntries:
    """``ops.from_carto`` rejects a blank entry before it is quoted."""

    @pytest.mark.parametrize("include_cols", ["id,,name", "   ", "cartodb_id, ,the_geom"])
    def test_blank_include_entry_rejected_without_network(self, include_cols):
        """The #980 crash: a blank entry used to reach quote_identifier()."""
        with mock.patch.object(
            carto, "_carto_sql_json", side_effect=AssertionError("contacted Carto")
        ):
            with pytest.raises(InvalidParameterError) as exc:
                ops.from_carto(CARTO_URL, "tbl", include_cols=include_cols)
        assert "--include-cols" in str(exc.value)
        assert "empty or whitespace-only" in str(exc.value)

    def test_blank_exclude_entry_rejected_without_network(self):
        with mock.patch.object(
            carto, "_carto_sql_json", side_effect=AssertionError("contacted Carto")
        ):
            with pytest.raises(InvalidParameterError) as exc:
                ops.from_carto(CARTO_URL, "tbl", exclude_cols="a,,b")
        assert "--exclude-cols" in str(exc.value)

    def test_wholly_empty_value_still_means_unset(self):
        """``--include-cols "$COLS"`` with COLS unset must keep working (#973)."""
        with (
            _patch_carto_probes(),
            mock.patch.object(
                carto, "_build_carto_query", side_effect=SystemExit("query built")
            ) as build,
        ):
            with pytest.raises(SystemExit):
                ops.from_carto(CARTO_URL, "tbl", include_cols="", exclude_cols="")
        assert build.call_args.kwargs["columns"] is None


class TestCartoSchemaCheck:
    """A non-blank name is checked against the schema the probe already read."""

    def test_unknown_include_column_rejected(self):
        with _patch_carto_probes():
            with pytest.raises(InvalidParameterError) as exc:
                ops.from_carto(CARTO_URL, "tbl", include_cols="cartodb_id,nope")
        assert "nope" in str(exc.value)
        assert "cartodb_id" in str(exc.value)

    def test_include_column_resolved_to_schema_spelling(self):
        """Carto is Postgres: a quoted "OWNER" would not match the column ``Owner``."""
        with (
            _patch_carto_probes(),
            mock.patch.object(
                carto, "_build_carto_query", side_effect=SystemExit("query built")
            ) as build,
        ):
            with pytest.raises(SystemExit):
                ops.from_carto(CARTO_URL, "tbl", include_cols="owner,CARTODB_ID")
        assert build.call_args.kwargs["columns"] == ["Owner", "cartodb_id"]

    def test_exclude_geometry_is_not_a_schema_error(self):
        """``exclude_cols`` names post-fetch columns, where ``the_geom`` is ``geometry``."""
        with (
            _patch_carto_probes(),
            mock.patch.object(carto, "_build_carto_query", side_effect=SystemExit("query built")),
        ):
            with pytest.raises(SystemExit):
                ops.from_carto(CARTO_URL, "tbl", exclude_cols="geometry")

    def test_no_schema_check_when_the_probe_is_skipped(self):
        """``geometry=True`` runs no probe, so there is no schema to check against."""
        with (
            mock.patch.object(
                carto, "_build_carto_query", side_effect=SystemExit("query built")
            ) as build,
            mock.patch.object(carto, "_get_row_count", return_value=1),
        ):
            with pytest.raises(SystemExit):
                ops.from_carto(CARTO_URL, "tbl", include_cols="whatever", geometry=True)
        assert build.call_args.kwargs["columns"] == ["whatever"]


class TestArcGISBlankEntries:
    """``ops.from_arcgis`` rejects a blank entry instead of sending it."""

    def test_blank_include_entry_rejected_before_any_request(self):
        """The #980 misbehavior: ``outFields=name,,pop`` went to the server."""
        with mock.patch.object(
            arcgis, "get_layer_info", side_effect=AssertionError("contacted the service")
        ):
            with pytest.raises(InvalidParameterError) as exc:
                ops.from_arcgis(ARCGIS_URL, include_cols="name,,pop")
        assert "--include-cols" in str(exc.value)
        assert "empty or whitespace-only" in str(exc.value)

    def test_blank_exclude_entry_rejected_before_any_request(self):
        with mock.patch.object(
            arcgis, "get_layer_info", side_effect=AssertionError("contacted the service")
        ):
            with pytest.raises(InvalidParameterError) as exc:
                ops.from_arcgis(ARCGIS_URL, exclude_cols="name,,pop")
        assert "--exclude-cols" in str(exc.value)


class TestArcGISSchemaCheck:
    """Non-blank names are checked against the layer metadata already fetched."""

    def test_unknown_include_field_rejected(self):
        with mock.patch.object(arcgis, "get_layer_info", return_value=_layer_info()):
            with pytest.raises(InvalidParameterError) as exc:
                ops.from_arcgis(ARCGIS_URL, include_cols="name,nope")
        assert "nope" in str(exc.value)
        assert "OBJECTID" in str(exc.value)

    def test_unknown_exclude_field_rejected(self):
        with mock.patch.object(arcgis, "get_layer_info", return_value=_layer_info()):
            with pytest.raises(InvalidParameterError) as exc:
                ops.from_arcgis(ARCGIS_URL, exclude_cols="nope")
        assert "nope" in str(exc.value)

    def test_exclude_geometry_is_accepted(self):
        """``geometry`` is not a layer field but is the table's first column."""
        with (
            mock.patch.object(arcgis, "get_layer_info", return_value=_layer_info()),
            mock.patch.object(
                arcgis, "_stream_features_to_parquet", side_effect=SystemExit("streamed")
            ),
        ):
            with pytest.raises(SystemExit):
                ops.from_arcgis(ARCGIS_URL, exclude_cols="geometry")

    def test_include_field_resolved_to_layer_spelling(self):
        with (
            mock.patch.object(arcgis, "get_layer_info", return_value=_layer_info()),
            mock.patch.object(
                arcgis, "_stream_features_to_parquet", side_effect=SystemExit("streamed")
            ) as stream,
        ):
            with pytest.raises(SystemExit):
                ops.from_arcgis(ARCGIS_URL, include_cols="objectid,Name")
        assert stream.call_args.kwargs["out_fields"] == "OBJECTID,name"

    def test_wholly_empty_value_still_means_unset(self):
        """``--include-cols "$COLS"`` with COLS unset must keep working (#973)."""
        with (
            mock.patch.object(arcgis, "get_layer_info", return_value=_layer_info()),
            mock.patch.object(
                arcgis, "_stream_features_to_parquet", side_effect=SystemExit("streamed")
            ) as stream,
        ):
            with pytest.raises(SystemExit):
                ops.from_arcgis(ARCGIS_URL, include_cols="", exclude_cols="")
        assert stream.call_args.kwargs["out_fields"] == "*"

    def test_star_is_still_the_all_fields_wildcard(self):
        """``outFields=*`` is ArcGIS's own spelling for "every field"."""
        with (
            mock.patch.object(arcgis, "get_layer_info", return_value=_layer_info()),
            mock.patch.object(
                arcgis, "_stream_features_to_parquet", side_effect=SystemExit("streamed")
            ) as stream,
        ):
            with pytest.raises(SystemExit):
                ops.from_arcgis(ARCGIS_URL, include_cols="*")
        assert stream.call_args.kwargs["out_fields"] == "*"


class TestParquetBackendSaysWhatIsWrong:
    """``ops.extract`` already raised; the message now names the real problem."""

    @staticmethod
    def _table() -> pa.Table:
        return pa.table({"id": [1], "name": ["a"]})

    def test_blank_column_entry_message(self):
        with pytest.raises(InvalidParameterError) as exc:
            ops.extract(self._table(), columns=["id", ""])
        assert "empty or whitespace-only" in str(exc.value)

    def test_blank_exclude_entry_message(self):
        with pytest.raises(InvalidParameterError) as exc:
            ops.extract(self._table(), exclude_columns=["   "])
        assert "empty or whitespace-only" in str(exc.value)

    def test_unknown_column_still_reports_the_schema(self):
        with pytest.raises(InvalidParameterError) as exc:
            ops.extract(self._table(), columns=["nope"])
        assert "Columns not found in schema: nope" in str(exc.value)


class TestPMTilesBlankEntries:
    """``ops.create_pmtiles`` forwards the value into a subprocess argv.

    On main the blank entry travelled verbatim as
    ``gpio extract geoparquet <in> --include-cols id,,name``, so it was rejected
    one process away from the user.
    """

    def test_blank_include_entry_rejected_before_the_subprocess(self, tmp_path):
        src = tmp_path / "in.parquet"
        pq.write_table(pa.table({"id": [1]}), src)
        with mock.patch("subprocess.Popen", side_effect=AssertionError("spawned a subprocess")):
            with pytest.raises(InvalidParameterError) as exc:
                ops.create_pmtiles(str(src), str(tmp_path / "out.pmtiles"), include_cols="id,,name")
        assert "--include-cols" in str(exc.value)

    def test_a_bad_option_is_not_reported_as_a_missing_binary(self, tmp_path):
        """The check runs before the tippecanoe probe.

        Otherwise a machine without tippecanoe -- CI included -- gets
        ``TippecanoeNotFoundError`` for what is a usage error, and this module's
        coverage of pmtiles would silently depend on the binary being installed.
        """
        src = tmp_path / "in.parquet"
        pq.write_table(pa.table({"id": [1]}), src)
        with mock.patch.object(pmtiles, "_check_tippecanoe", return_value=False):
            with pytest.raises(InvalidParameterError):
                ops.create_pmtiles(str(src), str(tmp_path / "out.pmtiles"), include_cols="id,,name")

    @pytest.mark.parametrize(
        ("include_cols", "expected"),
        [
            (" id , name ", "id,name,kind"),
            ("id,kind", "id,kind"),
        ],
        ids=["appended", "already-present"],
    )
    def test_the_layer_by_column_merge_survives_the_shared_splitter(
        self, tmp_path, include_cols, expected
    ):
        """``split_column_list`` replaced a hand-rolled split that did this merge."""
        src = tmp_path / "in.parquet"
        pq.write_table(pa.table({"id": [1]}), src)
        with (
            mock.patch.object(pmtiles, "_check_tippecanoe", return_value=True),
            mock.patch.object(
                pmtiles, "_build_gpio_commands", side_effect=SystemExit("built")
            ) as build,
        ):
            with pytest.raises(SystemExit):
                ops.create_pmtiles(
                    str(src),
                    str(tmp_path / "out.pmtiles"),
                    include_cols=include_cols,
                    layer_by_column="kind",
                )
        # _build_gpio_commands(input_path, bbox, where, include_cols, ...)
        assert build.call_args.args[3] == expected
