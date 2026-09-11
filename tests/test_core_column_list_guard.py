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

import geoparquet_io as gpio
from geoparquet_io.api import ops
from geoparquet_io.core import arcgis, carto, pmtiles
from geoparquet_io.core import extract_bigquery as extract_bigquery_module
from geoparquet_io.core.carto import CartoError
from geoparquet_io.core.column_selection import reject_blank_column_entries, split_column_list
from geoparquet_io.core.exceptions import InvalidParameterError

CARTO_URL = "https://example.carto.com/api/v2/sql"

# A Carto ``SELECT * ... LIMIT 0`` schema probe response.
CARTO_FIELDS = {
    "cartodb_id": {"type": "number"},
    "Owner": {"type": "string"},
    "the_geom": {"type": "geometry"},
}

# The same table with no geometry-typed column, i.e. the tabular path. Its
# fetched CSV columns *are* the schema columns -- there is no rename at all.
CARTO_TABULAR_FIELDS = {
    "cartodb_id": {"type": "number"},
    "Owner": {"type": "string"},
}


def _carto_payload(sql: str, fields: dict | None = None) -> dict:
    """Stand in for :func:`carto._carto_sql_json` for the two probe queries."""
    if "IS NOT NULL" in sql:
        return {"rows": [{"has_geom": 1}]}
    return {"fields": CARTO_FIELDS if fields is None else fields, "rows": []}


def _patch_carto_probes(fields: dict | None = None, **kwargs):
    """Answer Carto's schema/geometry probes; fail loudly on a data fetch."""
    return mock.patch.object(
        carto,
        "_carto_sql_json",
        side_effect=lambda url, sql, *a, **kw: _carto_payload(sql, fields),
        **kwargs,
    )


def _run_carto(fetched: pa.Table, fields: dict | None = None, **kwargs) -> pa.Table:
    """Run ``ops.from_carto`` end to end with every request mocked out.

    ``--exclude-cols`` is applied to the *fetched* table, so only a full run
    shows whether a name actually dropped a column or silently did nothing.
    """
    with (
        _patch_carto_probes(fields),
        mock.patch.object(carto, "_fetch_with_retry", return_value=fetched),
        mock.patch.object(carto, "_get_row_count", return_value=fetched.num_rows),
        mock.patch.object(
            carto, "repair_arrow_table_geometry", side_effect=lambda t, col, repair: (t, 0)
        ),
    ):
        return ops.from_carto(CARTO_URL, "tbl", **kwargs)


def _geo_fetch() -> pa.Table:
    """What ST_Read hands back for :data:`CARTO_FIELDS`: ``the_geom`` is ``geom``."""
    return pa.table({"cartodb_id": [1], "Owner": ["acme"], "geom": [b"\x00"]})


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


class TestCartoExcludeSchemaCheck:
    """``--exclude-cols`` is checked and resolved too (#991).

    #989 left it matched exactly, on the grounds that the post-fetch rename made
    the fetched column set unknowable. It is knowable: ``geometry`` plus the
    schema the probe already read, which is exactly what ``arcgis.py`` checks
    ``--exclude-cols`` against.
    """

    def test_a_typo_is_an_error_not_a_silent_no_op(self):
        with _patch_carto_probes():
            with pytest.raises(InvalidParameterError) as exc:
                ops.from_carto(CARTO_URL, "tbl", exclude_cols="Ownre")
        assert "--exclude-cols" in str(exc.value)
        assert "Ownre" in str(exc.value)
        assert "cartodb_id" in str(exc.value)

    @pytest.mark.parametrize("exclude_cols", ["Owner", "owner", "OWNER"])
    def test_a_case_mismatch_drops_the_column_it_names(self, exclude_cols):
        """Postgres is case-sensitive through a quoted identifier; the user is not."""
        table = _run_carto(_geo_fetch(), exclude_cols=exclude_cols)
        assert table.column_names == ["cartodb_id", "geometry"]

    def test_the_tabular_path_is_checked_too(self):
        """``_carto_plain_table`` has no rename at all, so the excuse never applied."""
        with _patch_carto_probes(CARTO_TABULAR_FIELDS):
            with pytest.raises(InvalidParameterError) as exc:
                ops.from_carto(CARTO_URL, "tbl", exclude_cols="Ownre")
        assert "Ownre" in str(exc.value)

    def test_the_tabular_path_resolves_the_spelling_too(self):
        table = _run_carto(
            pa.table({"cartodb_id": [1], "Owner": ["acme"]}),
            CARTO_TABULAR_FIELDS,
            exclude_cols="owner",
        )
        assert table.column_names == ["cartodb_id"]

    def test_excluding_geometry_in_any_spelling_is_refused_not_obeyed(self):
        """``geometry`` is required for GeoParquet output, so it is warned about.

        On main ``GEOMETRY`` missed that guard by exact match and then silently
        dropped nothing; now it resolves to ``geometry`` and is refused out loud.
        """
        with mock.patch.object(carto, "warn") as warn:
            table = _run_carto(_geo_fetch(), exclude_cols="GEOMETRY")
        assert table.column_names == ["cartodb_id", "Owner", "geometry"]
        assert any("Cannot exclude" in str(call.args[0]) for call in warn.call_args_list)


class TestCartoForcedModeStillChecks:
    """``--geometry``/``--no-geometry`` no longer skips the check (#991).

    #989 pinned the skip as deliberate: the shape probe is what supplies the
    schema, and forcing the mode skips it. But the guarantee "a typo is an
    error" should not hinge on an unrelated flag, so the column options now pay
    for their own ``SELECT * ... LIMIT 0`` -- and only when one is given.
    """

    @pytest.mark.parametrize("geometry", [True, False])
    def test_an_unknown_include_column_is_still_rejected(self, geometry):
        with _patch_carto_probes():
            with pytest.raises(InvalidParameterError) as exc:
                ops.from_carto(CARTO_URL, "tbl", include_cols="nope", geometry=geometry)
        assert "nope" in str(exc.value)

    def test_an_unknown_exclude_column_is_still_rejected(self):
        with _patch_carto_probes():
            with pytest.raises(InvalidParameterError) as exc:
                ops.from_carto(CARTO_URL, "tbl", exclude_cols="Ownre", geometry=True)
        assert "Ownre" in str(exc.value)

    def test_no_column_option_means_no_probe(self):
        """The flags' whole point is to skip the probe, so nothing else pays for it."""
        with (
            mock.patch.object(carto, "_carto_sql_json", side_effect=AssertionError("probed Carto")),
            mock.patch.object(
                carto, "_build_carto_query", side_effect=SystemExit("query built")
            ) as build,
            mock.patch.object(carto, "_get_row_count", return_value=1),
        ):
            with pytest.raises(SystemExit):
                ops.from_carto(CARTO_URL, "tbl", geometry=True)
        assert build.call_args.kwargs["columns"] is None

    def test_a_failed_probe_does_not_fail_the_extraction(self):
        """No schema in hand is "nothing to check against", as everywhere else."""
        with (
            mock.patch.object(carto, "_carto_sql_json", side_effect=CartoError("boom")),
            mock.patch.object(
                carto, "_build_carto_query", side_effect=SystemExit("query built")
            ) as build,
            mock.patch.object(carto, "_get_row_count", return_value=1),
        ):
            with pytest.raises(SystemExit):
                ops.from_carto(CARTO_URL, "tbl", include_cols="whatever", geometry=True)
        assert build.call_args.kwargs["columns"] == ["whatever"]

    def test_a_blank_entry_is_still_rejected_before_the_probe(self):
        with mock.patch.object(
            carto, "_carto_sql_json", side_effect=AssertionError("probed Carto")
        ):
            with pytest.raises(InvalidParameterError) as exc:
                ops.from_carto(CARTO_URL, "tbl", include_cols="id,,name", geometry=True)
        assert "empty or whitespace-only" in str(exc.value)


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


class TestBigQueryListArguments:
    """A blank entry in a *list* argument is a mistake, not an unset option (#992).

    ``ops.read_bigquery``/``Table.from_bigquery`` take ``columns`` as a list and
    join it into the string option the core takes. ``[""]`` joined to ``""``,
    which :func:`split_column_list` reads as "option not given" -- correctly, for
    a string (``--include-cols "$COLS"`` with ``COLS`` unset, #973). For a list
    it silently widened the request to **every column**, the widest possible
    answer to a caller mistake. ``["id", ""]`` and ``["", ""]`` were already
    caught only because they join to ``"id,"`` and ``","``.
    """

    @staticmethod
    def _forbid_extract():
        return mock.patch.object(
            extract_bigquery_module,
            "extract_bigquery",
            side_effect=AssertionError("reached extract_bigquery"),
        )

    @staticmethod
    def _spy_on_extract():
        return mock.patch.object(
            extract_bigquery_module, "extract_bigquery", side_effect=SystemExit("extracting")
        )

    @pytest.mark.parametrize("columns", [[""], ["  "], ["id", ""], ["", ""]])
    def test_a_blank_entry_in_columns_is_rejected(self, columns):
        with self._forbid_extract():
            with pytest.raises(InvalidParameterError) as exc:
                ops.read_bigquery("p.d.t", columns=columns)
        assert "columns" in str(exc.value)
        assert "empty or whitespace-only" in str(exc.value)

    def test_a_blank_entry_in_exclude_columns_is_rejected(self):
        with self._forbid_extract():
            with pytest.raises(InvalidParameterError) as exc:
                ops.read_bigquery("p.d.t", exclude_columns=[""])
        assert "exclude_columns" in str(exc.value)

    @pytest.mark.parametrize("columns", [[""], ["id", ""]])
    def test_the_table_constructor_rejects_it_too(self, columns):
        with self._forbid_extract():
            with pytest.raises(InvalidParameterError):
                gpio.Table.from_bigquery("p.d.t", columns=columns)

    def test_the_table_constructor_rejects_a_blank_exclusion(self):
        with self._forbid_extract():
            with pytest.raises(InvalidParameterError) as exc:
                gpio.Table.from_bigquery("p.d.t", exclude_columns=["  "])
        assert "exclude_columns" in str(exc.value)

    @pytest.mark.parametrize("columns", [None, []])
    def test_an_absent_list_still_means_every_column(self, columns):
        """The list argument's own "unset": None or empty, never a blank entry."""
        with self._spy_on_extract() as extract:
            with pytest.raises(SystemExit):
                ops.read_bigquery("p.d.t", columns=columns, exclude_columns=columns)
        assert extract.call_args.kwargs["include_cols"] is None
        assert extract.call_args.kwargs["exclude_cols"] is None

    def test_a_real_list_is_still_joined(self):
        with self._spy_on_extract() as extract:
            with pytest.raises(SystemExit):
                ops.read_bigquery("p.d.t", columns=["id", "name"], exclude_columns=["junk"])
        assert extract.call_args.kwargs["include_cols"] == "id,name"
        assert extract.call_args.kwargs["exclude_cols"] == "junk"


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
