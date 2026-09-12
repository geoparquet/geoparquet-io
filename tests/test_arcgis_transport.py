"""ArcGIS extraction over a stubbed transport.

Everything here runs offline through ``tests/http_transport.py``. The oracle is
the request the extractor actually issued (the merged query string, the page
offsets, the retry count, the value handed to ``time.sleep``) and the file it
wrote (``gpio check spec`` clean, both CRS carriers read separately) — never
"the mock was called".
"""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.arcgis import (
    ArcGISAuth,
    ArcGISLayerInfo,
    _get_reduced_batch_size,
    arcgis_to_table,
    convert_arcgis_to_geoparquet,
    fetch_all_features,
    fetch_features_page,
    generate_token,
    get_feature_count,
    get_layer_info,
    resolve_token,
    validate_arcgis_url,
)
from geoparquet_io.core.exceptions import (
    GeoParquetError,
    InvalidParameterError,
    RemoteAccessError,
)
from tests.http_transport import (
    FakeTransport,
    error_reply,
    html_reply,
    json_reply,
    protocol_error,
    timeout_error,
)
from tests.native_geo_probes import geo_block, geo_block_crs_id, logical_crs_id, spec_problems

SERVICE = "https://services.example.com/arcgis/rest/services/Cities/FeatureServer/0"

FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID"},
    {"name": "name", "type": "esriFieldTypeString", "length": 40},
    {"name": "pop", "type": "esriFieldTypeInteger"},
]


def _layer_json(*, max_record_count=1000, wkid=4326, geometry_type="esriGeometryPoint"):
    return {
        "id": 0,
        "name": "Cities",
        "type": "Feature Layer",
        "geometryType": geometry_type,
        "spatialReference": {"wkid": wkid, "latestWkid": wkid},
        "extent": {
            "xmin": -123.0,
            "ymin": 37.0,
            "xmax": -121.0,
            "ymax": 38.0,
            "spatialReference": {"wkid": wkid},
        },
        "maxRecordCount": max_record_count,
        "fields": FIELDS,
    }


def _feature(index: int) -> dict:
    return {
        "type": "Feature",
        "id": index,
        "geometry": {"type": "Point", "coordinates": [-122.0 + index * 0.01, 37.5]},
        "properties": {"OBJECTID": index, "name": f"city-{index}", "pop": 1000 + index},
    }


def _page(start: int, count: int) -> dict:
    return {
        "type": "FeatureCollection",
        "features": [_feature(i) for i in range(start, start + count)],
    }


def _is_count(request) -> bool:
    return request.params.get("returnCountOnly") == "true"


def _is_query(request) -> bool:
    return request.path.endswith("/query") and not _is_count(request)


def _offset_aware(total):
    """Serve whatever window the request asks for, out of ``total`` features."""

    def _serve(request):
        import httpx

        offset = int(request.url.params.get("resultOffset", 0))
        limit = int(request.url.params.get("resultRecordCount", total))
        remaining = max(0, total - offset)
        return httpx.Response(
            200,
            json=_page(offset, min(limit, remaining)),
            headers={"content-type": "application/json"},
        )

    return _serve


def stub_service(http, *, total, page_replies=None, layer=None, page_size=None):
    """Route a whole FeatureServer layer: metadata, count, then feature pages.

    ``page_replies`` overrides the feature-page answers (a sequence, so a test
    can fail the first page and succeed on the second). By default every page
    is served from the total, honouring ``resultOffset``/``resultRecordCount``.
    """
    http.respond(
        lambda r: r.path.endswith("/0") and not r.path.endswith("query"),
        json_reply(layer or _layer_json()),
    )
    http.respond(_is_count, json_reply({"count": total}))

    if page_replies is None:
        page_replies = (_offset_aware(total),)

    http.respond(_is_query, *page_replies)
    return http


# ---------------------------------------------------------------------------
# URL validation (pure - no transport needed)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "needle"),
    [
        ("https://h/rest/services/X/ImageServer", "ImageServer"),
        ("https://h/rest/services/X/GPServer/0", "FeatureServer or MapServer"),
        ("https://h/rest/services/X/FeatureServer", "Missing layer ID"),
    ],
)
def test_validate_arcgis_url_rejects(url, needle):
    with pytest.raises(InvalidParameterError) as excinfo:
        validate_arcgis_url(url)
    assert needle in str(excinfo.value)


def test_validate_arcgis_url_returns_layer_id():
    assert validate_arcgis_url(SERVICE + "/") == (SERVICE, 0)


@pytest.mark.parametrize(("current", "expected"), [(2000, 1000), (1000, 500), (2, 1), (1, None)])
def test_batch_ladder_only_ever_descends(current, expected):
    assert _get_reduced_batch_size(current) == expected


# ---------------------------------------------------------------------------
# Token generation - the module's only POST
# ---------------------------------------------------------------------------


def test_generate_token_posts_credentials_as_a_form(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond("generateToken", json_reply({"token": "tok-1", "expires": 123}))

    assert generate_token("u", "p", verbose=True) == "tok-1"

    sent = http.last
    assert sent.method == "POST"
    assert sent.url == "https://www.arcgis.com/sharing/rest/generateToken"
    assert sent.form == {
        "username": "u",
        "password": "p",
        "referer": "geoparquet-io",
        "f": "json",
        "expiration": "60",
    }


def test_generate_token_reports_a_tokenless_success_body(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond("generateToken", json_reply({"expires": 1}))

    with pytest.raises(GeoParquetError, match="no token in response"):
        generate_token("u", "p")
    assert len(http.requests) == 1


def test_generate_token_surfaces_an_esri_error_envelope(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(
        "generateToken",
        json_reply(
            {
                "error": {
                    "code": 400,
                    "message": "Unable to generate token",
                    "details": ["Invalid username"],
                }
            }
        ),
    )

    with pytest.raises(
        GeoParquetError, match="Error 400 - Unable to generate token. Invalid username"
    ):
        generate_token("u", "bad")


def test_expired_token_code_gets_the_reauthenticate_message(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, json_reply({"error": {"code": 498, "message": "Invalid token"}}))

    with pytest.raises(GeoParquetError, match="Invalid or expired token"):
        get_layer_info(SERVICE, token="stale")


def test_resolve_token_prefers_a_direct_token_over_the_network(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    assert resolve_token(ArcGISAuth(token="direct"), SERVICE, verbose=True) == "direct"
    assert http.requests == []


def test_resolve_token_reads_a_token_file(monkeypatch, tmp_path):
    http = FakeTransport.install(monkeypatch)
    token_file = tmp_path / "token.txt"
    token_file.write_text("  from-file\n", encoding="utf-8")

    assert (
        resolve_token(ArcGISAuth(token_file=str(token_file)), SERVICE, verbose=True) == "from-file"
    )
    assert http.requests == []


def test_resolve_token_reports_an_unreadable_token_file(monkeypatch, tmp_path):
    FakeTransport.install(monkeypatch)
    with pytest.raises(GeoParquetError, match="Failed to read token file"):
        resolve_token(ArcGISAuth(token_file=str(tmp_path / "absent.txt")), SERVICE)


def test_resolve_token_derives_the_enterprise_token_endpoint(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond("tokens/generateToken", json_reply({"token": "ent"}))

    auth = ArcGISAuth(username="u", password="p")
    assert resolve_token(auth, SERVICE, verbose=True) == "ent"
    assert http.last.url == "https://services.example.com/arcgis/tokens/generateToken"


def test_resolve_token_without_credentials_is_none(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    assert resolve_token(ArcGISAuth(), SERVICE) is None
    assert http.requests == []


def test_a_resolved_token_rides_on_every_query(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=1)

    get_layer_info(SERVICE, token="tok-9", verbose=True)

    assert [request.params.get("token") for request in http.requests] == ["tok-9", "tok-9"]


# ---------------------------------------------------------------------------
# What the metadata and count requests actually send
# ---------------------------------------------------------------------------


def test_layer_info_reads_the_metadata_the_server_returned(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=17, layer=_layer_json(max_record_count=250, wkid=3857))

    info = get_layer_info(SERVICE, where="pop > 10", bbox=(-123, 37, -121, 38), verbose=True)

    assert info == ArcGISLayerInfo(
        name="Cities",
        geometry_type="esriGeometryPoint",
        spatial_reference={"wkid": 3857, "latestWkid": 3857},
        fields=FIELDS,
        max_record_count=250,
        total_count=17,
    )
    metadata_request, count_request = http.requests
    assert metadata_request.params == {"f": "json"}
    assert count_request.params == {
        "where": "pop > 10",
        "returnCountOnly": "true",
        "f": "json",
        "geometry": "-123,37,-121,38",
        "geometryType": "esriGeometryEnvelope",
        "spatialRel": "esriSpatialRelIntersects",
        "inSR": "4326",
    }


def test_layer_info_falls_back_through_the_spatial_reference_carriers(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    layer = _layer_json()
    del layer["spatialReference"]
    stub_service(http, total=1, layer=layer)

    assert get_layer_info(SERVICE).spatial_reference == {"wkid": 4326}


def test_a_layer_without_max_record_count_defaults_to_1000(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    layer = _layer_json()
    del layer["maxRecordCount"]
    stub_service(http, total=1, layer=layer)

    assert get_layer_info(SERVICE).max_record_count == 1000


def test_feature_count_without_a_bbox_sends_no_spatial_params(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_count, json_reply({"count": 5}))

    assert get_feature_count(SERVICE, verbose=True) == 5
    assert "geometry" not in http.last.params
    assert http.last.path.endswith("/FeatureServer/0/query")


# ---------------------------------------------------------------------------
# What a feature page request actually sends
# ---------------------------------------------------------------------------


def test_a_geojson_page_request_carries_offset_and_record_count(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_query, json_reply(_page(40, 20)))

    page = fetch_features_page(SERVICE, offset=40, limit=20, out_fields="name,pop", verbose=True)

    assert len(page["features"]) == 20
    assert http.last.params == {
        "where": "1=1",
        "outFields": "name,pop",
        "returnGeometry": "true",
        "f": "geojson",
        "resultOffset": "40",
        "resultRecordCount": "20",
    }


def test_a_native_crs_page_switches_to_esrijson_and_sends_outsr(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_query, json_reply({"features": []}))

    fetch_features_page(SERVICE, offset=0, limit=10, output_wkid=25830)

    assert http.last.params["f"] == "json"
    assert http.last.params["outSR"] == "25830"


def test_generalization_defaults_outsr_to_wgs84(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_query, json_reply({"features": []}))

    fetch_features_page(SERVICE, offset=0, limit=10, max_allowable_offset=0.001)

    assert http.last.params["maxAllowableOffset"] == "0.001"
    assert http.last.params["outSR"] == "4326"


def test_a_page_bbox_becomes_an_envelope_query(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_query, json_reply({"features": []}))

    fetch_features_page(SERVICE, offset=0, limit=10, bbox=(-1.5, 2.0, 3.5, 4.0))

    assert http.last.params["geometry"] == "-1.5,2.0,3.5,4.0"
    assert http.last.params["geometryType"] == "esriGeometryEnvelope"
    assert http.last.params["spatialRel"] == "esriSpatialRelIntersects"


def test_a_page_surfaces_an_esri_error_envelope(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_query, json_reply({"error": {"code": 400, "message": "Invalid field"}}))

    with pytest.raises(GeoParquetError, match="Feature query: Error 400 - Invalid field"):
        fetch_features_page(SERVICE, offset=0, limit=10)


# ---------------------------------------------------------------------------
# Retry behaviour, asserted by request count and sleep value
# ---------------------------------------------------------------------------


def test_a_503_is_retried_with_linear_backoff(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_count, error_reply(503), error_reply(503), json_reply({"count": 3}))

    assert get_feature_count(SERVICE) == 3
    assert len(http.requests) == 3
    assert http.sleeps == [1.0, 2.0]


def test_a_429_honours_an_integer_retry_after(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_count, error_reply(429, retry_after=7), json_reply({"count": 1}))

    assert get_feature_count(SERVICE) == 1
    assert http.sleeps == [7.0]


def test_an_http_date_retry_after_degrades_to_linear_backoff(monkeypatch):
    """`Retry-After` is honoured only when `.isdigit()` (http_retry.py).

    RFC 9110 allows an HTTP-date, and a server that sends one gets gpio's own
    exponential backoff instead of the delay it asked for. That is graceful
    degradation rather than a defect, but it is invisible unless asserted:
    this test pins the current behaviour so a future change to parse the date
    form shows up as a failing test rather than a silent one.
    """
    http = FakeTransport.install(monkeypatch)
    http.respond(
        _is_count,
        error_reply(429, retry_after="Wed, 21 Oct 2026 07:28:00 GMT"),
        json_reply({"count": 1}),
    )

    assert get_feature_count(SERVICE) == 1
    assert http.sleeps == [1.0]  # retry_delay * (attempt + 1), not the date


def test_retries_are_exhausted_and_then_reported(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_count, error_reply(500))

    with pytest.raises(RemoteAccessError, match="HTTP error 500"):
        get_feature_count(SERVICE)
    assert len(http.requests) == 3
    assert http.sleeps == [1.0, 2.0]


@pytest.mark.parametrize(
    ("status", "needle"),
    [
        (401, "Authentication required"),
        (403, "Access denied"),
        (404, "Service not found"),
        (418, "HTTP error 418"),
    ],
)
def test_fatal_statuses_are_not_retried(monkeypatch, status, needle):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_count, error_reply(status))

    with pytest.raises(RemoteAccessError, match=needle):
        get_feature_count(SERVICE)
    assert len(http.requests) == 1
    assert http.sleeps == []


def test_a_timeout_is_retried_and_the_requested_timeout_is_sent(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_count, timeout_error(), json_reply({"count": 2}))

    assert get_feature_count(SERVICE, timeout=12.5) == 2
    assert [request.timeout for request in http.requests] == [12.5, 12.5]
    assert http.sleeps == [1.0]


def test_a_network_error_is_retried(monkeypatch):
    import httpx

    http = FakeTransport.install(monkeypatch)
    http.respond(_is_count, httpx.ConnectError("refused"), json_reply({"count": 2}))

    assert get_feature_count(SERVICE) == 2
    assert http.sleeps == [1.0]


def test_a_dropped_connection_resets_the_pool_before_retrying(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_count, protocol_error(), json_reply({"count": 2}))

    assert get_feature_count(SERVICE) == 2
    assert http.resets == 1
    assert http.sleeps == [1.0]


def test_an_html_block_page_on_a_countless_request_is_not_a_batch_problem(monkeypatch):
    """No batch was requested, so `BatchTooLargeError` would be misleading."""
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_count, html_reply(b"<html>403 by WAF</html>"))

    with pytest.raises(RemoteAccessError) as excinfo:
        get_feature_count(SERVICE)
    assert "Content-Type: text/html" in str(excinfo.value)
    assert len(http.requests) == 1


# ---------------------------------------------------------------------------
# Paging
# ---------------------------------------------------------------------------


def _layer_info(total, max_record_count=1000):
    return ArcGISLayerInfo(
        name="Cities",
        geometry_type="esriGeometryPoint",
        spatial_reference={"wkid": 4326},
        fields=FIELDS,
        max_record_count=max_record_count,
        total_count=total,
    )


def test_sequential_paging_walks_the_offsets_the_server_expects(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=250, page_size=100)

    pages = list(fetch_all_features(SERVICE, _layer_info(250), batch_size=100, verbose=True))

    assert [len(page["features"]) for page in pages] == [100, 100, 50]
    assert http.param_series("resultOffset") == ["0", "100", "200"]
    assert http.param_series("resultRecordCount") == ["100", "100", "50"]


def test_the_page_size_is_clamped_by_the_server_max_record_count(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=60)

    list(fetch_all_features(SERVICE, _layer_info(60, max_record_count=25), batch_size=2000))

    assert http.param_series("resultRecordCount") == ["25", "25", "10"]


def test_max_features_clamps_the_total_before_the_first_request(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=1000)

    pages = list(fetch_all_features(SERVICE, _layer_info(1000), batch_size=1000, max_features=50))

    assert sum(len(page["features"]) for page in pages) == 50
    assert http.param_series("resultRecordCount") == ["50"]


def test_max_features_clamps_the_parallel_path_identically(monkeypatch):
    """The two paths derive the page size from the same clamped ``total``."""
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=1000)

    pages = list(
        fetch_all_features(
            SERVICE, _layer_info(1000), batch_size=1000, max_features=50, max_workers=3
        )
    )

    assert sum(len(page["features"]) for page in pages) == 50
    assert http.param_series("resultRecordCount") == ["50"]


def test_parallel_paging_covers_every_offset_exactly_once(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=250, page_size=100)

    pages = list(fetch_all_features(SERVICE, _layer_info(250), batch_size=100, max_workers=2))

    assert sum(len(page["features"]) for page in pages) == 250
    assert sorted(int(offset) for offset in http.param_series("resultOffset")) == [0, 100, 200]


def test_parallel_pages_are_yielded_in_offset_order(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=30, page_size=10)

    pages = list(fetch_all_features(SERVICE, _layer_info(30), batch_size=10, max_workers=3))

    first_ids = [page["features"][0]["properties"]["OBJECTID"] for page in pages]
    assert first_ids == [0, 10, 20]


def test_an_empty_page_stops_sequential_paging(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(
        http,
        total=300,
        page_size=100,
        page_replies=(json_reply(_page(0, 100)), json_reply({"features": []})),
    )

    pages = list(fetch_all_features(SERVICE, _layer_info(300), batch_size=100))

    assert [len(page["features"]) for page in pages] == [100]
    assert len(http.matching(_is_query)) == 2


def test_a_short_page_realigns_the_next_offset(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(
        http,
        total=300,
        page_size=100,
        page_replies=(
            json_reply(_page(0, 60)),
            json_reply(_page(60, 100)),
            json_reply(_page(160, 100)),
            json_reply({"features": []}),
        ),
    )

    list(fetch_all_features(SERVICE, _layer_info(300), batch_size=100))

    assert http.param_series("resultOffset")[1:] == ["60", "160", "260"]


def test_too_many_workers_warns_but_still_runs(monkeypatch, caplog):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=5)

    with caplog.at_level("WARNING"):
        pages = list(fetch_all_features(SERVICE, _layer_info(5), max_workers=25))

    assert sum(len(page["features"]) for page in pages) == 5
    assert "max_workers=25 may trigger rate limits" in caplog.text


def test_zero_workers_is_rejected(monkeypatch):
    FakeTransport.install(monkeypatch)
    with pytest.raises(ValueError, match="max_workers must be at least 1"):
        list(fetch_all_features(SERVICE, _layer_info(5), max_workers=0))


# ---------------------------------------------------------------------------
# The batch-size fallback ladder
# ---------------------------------------------------------------------------


def test_an_oversized_batch_is_retried_at_the_same_offset_smaller(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(
        http,
        total=100,
        page_replies=(html_reply(b"<html>request too large</html>"), _offset_aware(100)),
    )

    pages = list(fetch_all_features(SERVICE, _layer_info(100), batch_size=1000))

    assert sum(len(page["features"]) for page in pages) == 100
    offsets_and_sizes = [
        (request.params["resultOffset"], request.params["resultRecordCount"])
        for request in http.matching(_is_query)
    ]
    # The rejected offset is re-requested at the reduced size, and the reduced
    # size sticks for the rest of the download.
    assert offsets_and_sizes == [("0", "100"), ("0", "50"), ("50", "50")]


def test_the_ladder_gives_up_at_batch_size_one(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=1, page_replies=(html_reply(b"<html>nope</html>"),))

    with pytest.raises(GeoParquetError, match="cannot handle even batch_size=1"):
        list(fetch_all_features(SERVICE, _layer_info(1), batch_size=1))


def test_the_parallel_path_retries_the_whole_window_smaller(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(
        http,
        total=200,
        page_replies=(
            html_reply(b"<html>too large</html>"),
            html_reply(b"<html>too large</html>"),
            _offset_aware(200),
        ),
    )

    pages = list(fetch_all_features(SERVICE, _layer_info(200), batch_size=100, max_workers=2))

    sizes = {request.params["resultRecordCount"] for request in http.matching(_is_query)}
    assert "50" in sizes  # the window came back at the reduced size
    assert sum(len(page["features"]) for page in pages) > 0


def test_the_parallel_ladder_gives_up_at_batch_size_one(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=2, page_replies=(html_reply(b"<html>nope</html>"),))

    with pytest.raises(GeoParquetError, match="cannot handle even batch_size=1"):
        list(fetch_all_features(SERVICE, _layer_info(2), batch_size=1, max_workers=2))


# ---------------------------------------------------------------------------
# arcgis_to_table - the table that comes back
# ---------------------------------------------------------------------------


def test_an_empty_filtered_layer_returns_an_empty_geometry_table(monkeypatch, caplog):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=0)

    with caplog.at_level("WARNING"):
        table = arcgis_to_table(SERVICE, where="pop > 1e9", bbox=(-1, -1, 1, 1))

    assert table.num_rows == 0
    assert table.column_names == ["geometry"]
    assert "No features match filter" in caplog.text
    assert "where='pop > 1e9'" in caplog.text


def test_an_empty_unfiltered_layer_says_so_differently(monkeypatch, caplog):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=0)

    with caplog.at_level("WARNING"):
        arcgis_to_table(SERVICE)

    assert "Layer has no features" in caplog.text


def test_include_cols_is_pushed_to_the_server_as_outfields(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=3)

    table = arcgis_to_table(SERVICE, include_cols="name,pop")

    assert http.matching(_is_query)[0].params["outFields"] == "name,pop"
    assert table.num_rows == 3


def test_exclude_cols_is_applied_after_the_download(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=3)

    table = arcgis_to_table(SERVICE, exclude_cols="pop")

    assert http.matching(_is_query)[0].params["outFields"] == "*"
    assert "pop" not in table.column_names
    assert "name" in table.column_names


def test_an_unknown_column_fails_before_the_first_page(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=3)

    with pytest.raises(Exception, match="nope"):
        arcgis_to_table(SERVICE, include_cols="nope")
    assert http.matching(_is_query) == []


def test_a_blank_entry_in_a_column_list_never_reaches_the_server(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=3)

    with pytest.raises(InvalidParameterError):
        arcgis_to_table(SERVICE, include_cols="name,,pop")
    assert http.requests == []


def test_a_bad_output_crs_fails_before_any_request(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    with pytest.raises(InvalidParameterError, match="output_crs"):
        arcgis_to_table(SERVICE, output_crs="not-a-crs")
    assert http.requests == []


def test_a_non_positive_generalization_tolerance_fails_before_any_request(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    with pytest.raises(InvalidParameterError, match="max_allowable_offset"):
        arcgis_to_table(SERVICE, max_allowable_offset=0)
    assert http.requests == []


def test_the_default_path_tags_the_table_crs84(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=4)

    table = arcgis_to_table(SERVICE)
    geo = json.loads(table.schema.metadata[b"geo"])

    assert geo["version"] == "1.1.0"
    assert geo["primary_column"] == "geometry"
    assert geo["columns"]["geometry"]["encoding"] == "WKB"
    assert geo["columns"]["geometry"]["geometry_types"] == ["Point"]
    assert geo["columns"]["geometry"]["crs"]["id"] == {"authority": "OGC", "code": "CRS84"}
    assert http.matching(_is_query)[0].params["f"] == "geojson"


def test_a_native_crs_request_warns_when_the_server_returns_another(monkeypatch, caplog):
    http = FakeTransport.install(monkeypatch)
    http.respond(
        lambda r: r.path.endswith("/0") and not r.path.endswith("query"), json_reply(_layer_json())
    )
    http.respond(_is_count, json_reply({"count": 2}))
    http.respond(
        _is_query,
        json_reply(
            {
                "geometryType": "esriGeometryPoint",
                "spatialReference": {"wkid": 4326},
                "fields": FIELDS,
                "features": [
                    {
                        "geometry": {"x": -122.0, "y": 37.5},
                        "attributes": {"OBJECTID": 1, "name": "a", "pop": 1},
                    },
                    {
                        "geometry": {"x": -122.1, "y": 37.6},
                        "attributes": {"OBJECTID": 2, "name": "b", "pop": 2},
                    },
                ],
            }
        ),
    )

    with caplog.at_level("WARNING"):
        table = arcgis_to_table(SERVICE, output_crs="EPSG:3857")

    assert "server returned WKID 4326" in caplog.text.replace("\n", " ")
    geo = json.loads(table.schema.metadata[b"geo"])
    assert geo["columns"]["geometry"]["crs"]["id"]["code"] in (4326, "4326")
    assert http.matching(_is_query)[0].params["outSR"] == "3857"


# ---------------------------------------------------------------------------
# convert_arcgis_to_geoparquet - the file that gets written
# ---------------------------------------------------------------------------


def test_the_written_file_is_spec_clean_and_carries_crs84(monkeypatch, tmp_path):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=12)
    out = tmp_path / "cities.parquet"

    convert_arcgis_to_geoparquet(SERVICE, str(out), verbose=True)

    assert spec_problems(out) == []
    assert pq.read_metadata(out).num_rows == 12
    assert geo_block(out)["primary_column"] == "geometry"
    # Both CRS carriers, read separately - never merged through
    # source_crs_string, which is what hid #997.
    # GeoParquet 1.1 spells the CRS84 default as an absent `crs` key, so the
    # marker the probe returns for that is the correct reading here.
    assert geo_block_crs_id(out) == "<no crs key -- resolves as OGC:CRS84>"
    assert logical_crs_id(out) == "<no native geo type>"
    assert "bbox" in pq.read_schema(out).names


def test_skip_flags_leave_the_bbox_column_off(monkeypatch, tmp_path):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=6)
    out = tmp_path / "plain.parquet"

    convert_arcgis_to_geoparquet(SERVICE, str(out), skip_hilbert=True, skip_bbox=True)

    assert spec_problems(out) == []
    assert "bbox" not in pq.read_schema(out).names


def test_an_existing_output_is_refused_without_overwrite(monkeypatch, tmp_path):
    http = FakeTransport.install(monkeypatch)
    out = tmp_path / "exists.parquet"
    out.write_bytes(b"not parquet")

    with pytest.raises(GeoParquetError, match="already exists"):
        convert_arcgis_to_geoparquet(SERVICE, str(out))
    assert http.requests == []


def test_overwrite_replaces_the_file(monkeypatch, tmp_path):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=3)
    out = tmp_path / "exists.parquet"
    out.write_bytes(b"not parquet")

    convert_arcgis_to_geoparquet(SERVICE, str(out), overwrite=True)

    assert pq.read_metadata(out).num_rows == 3
    assert spec_problems(out) == []


def test_a_where_clause_and_bbox_reach_the_page_requests(monkeypatch, tmp_path):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=5)
    out = tmp_path / "filtered.parquet"

    convert_arcgis_to_geoparquet(
        SERVICE, str(out), where="pop > 1000", bbox=(-123.0, 37.0, -121.0, 38.0)
    )

    page = http.matching(_is_query)[0]
    assert page.params["where"] == "pop > 1000"
    assert page.params["geometry"] == "-123.0,37.0,-121.0,38.0"
    assert spec_problems(out) == []


def test_a_failing_service_leaves_no_output_behind(monkeypatch, tmp_path):
    http = FakeTransport.install(monkeypatch)
    http.respond(lambda r: True, error_reply(500))
    out = tmp_path / "never.parquet"

    with pytest.raises(RemoteAccessError):
        convert_arcgis_to_geoparquet(SERVICE, str(out))
    assert not out.exists()
    assert len(http.requests) == 3


def test_an_unknown_esri_field_type_falls_back_to_string(monkeypatch, caplog):
    http = FakeTransport.install(monkeypatch)
    layer = _layer_json()
    layer["fields"] = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID"},
        {"name": "name", "type": "esriFieldTypeString"},
        {"name": "pop", "type": "esriFieldTypeQuantum"},
    ]
    stub_service(http, total=2, layer=layer)

    with caplog.at_level("WARNING"):
        table = arcgis_to_table(SERVICE)

    assert "Unknown ArcGIS field type 'esriFieldTypeQuantum'" in caplog.text
    assert table.schema.field("pop").type == pa.string()


def test_excluding_the_geometry_column_declares_types_unknown(monkeypatch, caplog):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=3)

    with caplog.at_level("WARNING"):
        table = arcgis_to_table(SERVICE, exclude_cols="geometry")

    assert "geometry column was excluded" in caplog.text
    assert "geometry" not in table.column_names
    assert json.loads(table.schema.metadata[b"geo"])["columns"]["geometry"]["geometry_types"] == []


def test_an_all_null_geometry_column_declares_types_unknown(monkeypatch, caplog):
    http = FakeTransport.install(monkeypatch)
    page = _page(0, 2)
    for feature in page["features"]:
        feature["geometry"] = None
    stub_service(http, total=2, page_replies=(json_reply(page),))

    with caplog.at_level("WARNING"):
        table = arcgis_to_table(SERVICE)

    assert "holds no geometries" in caplog.text
    assert json.loads(table.schema.metadata[b"geo"])["columns"]["geometry"]["geometry_types"] == []


def test_native_output_crs_requires_an_advertised_spatial_reference(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    layer = _layer_json()
    layer["spatialReference"] = {}
    layer["extent"]["spatialReference"] = {}
    stub_service(http, total=2, layer=layer)

    with pytest.raises(GeoParquetError, match="advertises no spatial reference"):
        arcgis_to_table(SERVICE, output_crs="native")


def test_an_unresolvable_wkid_is_written_as_lon_lat(monkeypatch, tmp_path, caplog):
    """An unresolvable WKID relabels projected coordinates as WGS84 lon/lat.

    `_projjson_from_wkid` returns None for a WKID that is neither an EPSG nor
    an ESRI code, and `arcgis_to_table` guards the *whole* geo dict on that
    value, so no CRS is attached. In GeoParquet an absent `crs` key does not
    mean "unknown" — it means OGC:CRS84 — so metres come out labelled as
    degrees — and gpio's own `check spec` then fails the file it just wrote.
    The warning says only that the CRS was "left unset".
    See https://github.com/geoparquet/geoparquet-io/issues/1039
    """
    http = FakeTransport.install(monkeypatch)
    http.respond(
        lambda r: r.path.endswith("/0") and not r.path.endswith("query"),
        json_reply(_layer_json()),
    )
    http.respond(_is_count, json_reply({"count": 1}))
    http.respond(
        _is_query,
        json_reply(
            {
                "geometryType": "esriGeometryPoint",
                "fields": FIELDS,
                "features": [
                    {
                        "geometry": {"x": 441000.0, "y": 4474000.0},
                        "attributes": {"OBJECTID": 1, "name": "a", "pop": 1},
                    }
                ],
            }
        ),
    )
    out = tmp_path / "unresolvable.parquet"

    with caplog.at_level("WARNING"):
        convert_arcgis_to_geoparquet(SERVICE, str(out), output_crs="EPSG:99999")

    assert "not a known EPSG or ESRI code" in caplog.text
    # No `crs` key at all, which GeoParquet resolves as OGC:CRS84 - so these
    # projected easting/northing values are now labelled longitude/latitude.
    assert geo_block_crs_id(out) == "<no crs key -- resolves as OGC:CRS84>"
    assert geo_block(out)["columns"]["geometry"]["bbox"] == [
        441000.0,
        4474000.0,
        441000.0,
        4474000.0,
    ]
    # gpio wrote a file its own validator rejects.
    assert any(
        "coordinates outside valid range for CRS" in problem for problem in spec_problems(out)
    )
