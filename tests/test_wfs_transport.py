"""WFS extraction over a stubbed transport.

Runs offline through ``tests/http_transport.py``: the GetFeature/hits traffic
goes through ``httpx.MockTransport``, and OWSLib is handed a canned
GetCapabilities document so it parses the real thing without a network. The
oracle is the request issued (merged query string, page offsets, retry count,
``time.sleep`` argument) and the file written (``check spec`` clean, both CRS
carriers read separately).
"""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.wfs import (
    LayerNotFoundError,
    WFSAuthenticationError,
    WFSError,
    _fetch_wfs_page,
    _get_feature_count,
    _make_request,
    _probe_startindex_limit,
    convert_wfs_to_geoparquet,
    fetch_all_features_duckdb,
    get_layer_info,
    get_wfs_capabilities,
    list_available_layers,
    negotiate_wfs_version,
)
from tests.http_transport import (
    FakeTransport,
    bytes_reply,
    connect_error,
    error_reply,
    geojson_reply,
    protocol_error,
    timeout_error,
    wfs_capabilities,
    xml_reply,
)
from tests.native_geo_probes import geo_block, geo_block_crs_id, logical_crs_id, spec_problems
from tests.test_wfs import MOCK_CAPABILITIES_XML

SERVICE = "https://geo.example.org/geoserver/wfs"
TYPENAME = "test:cities"

CAPABILITIES = MOCK_CAPABILITIES_XML.encode("utf-8")

SCHEMA = {
    "geometry": "Point",
    "geometry_column": "the_geom",
    "properties": {"the_geom": "Point", "name": "string", "pop": "int"},
}


def _geojson(count: int, start: int = 0, crs: str | None = None) -> dict:
    document = {
        "type": "FeatureCollection",
        "numberMatched": count,
        "features": [
            {
                "type": "Feature",
                "id": f"cities.{index}",
                "geometry": {"type": "Point", "coordinates": [-122.0 + index * 0.01, 37.5]},
                "properties": {"name": f"city-{index}", "pop": 1000 + index},
            }
            for index in range(start, start + count)
        ],
    }
    if crs:
        document["crs"] = {"type": "name", "properties": {"name": crs}}
    return document


def _is_hits(request) -> bool:
    return request.params.get("resultType") == "hits"


def _is_startindex_probe(request) -> bool:
    return request.params.get("startIndex") == "50001"


def _is_getfeature(request) -> bool:
    return not _is_hits(request) and not _is_startindex_probe(request)


def _pages(total: int, page_size: int):
    """Serve the window the ``startIndex``/``count`` pair asks for."""

    def _serve(request):
        import httpx

        start = int(request.url.params.get("startIndex", 0))
        asked = int(request.url.params.get("count", request.url.params.get("maxFeatures", total)))
        remaining = max(0, total - start)
        return httpx.Response(
            200,
            json=_geojson(min(asked, page_size, remaining), start=start),
            headers={"content-type": "application/geo+json"},
        )

    return _serve


def stub_service(http, *, total, page_size=100_000, startindex_limit=None):
    """Route a whole WFS layer: the hits probe, the startIndex probe, the pages."""
    http.respond(_is_hits, xml_reply(f'<wfs:FeatureCollection numberMatched="{total}"/>'.encode()))
    if startindex_limit is None:
        http.respond(_is_startindex_probe, geojson_reply(_geojson(0)))
    else:
        http.respond(
            _is_startindex_probe,
            bytes_reply(
                f"startIndex is limited to {startindex_limit:,} on this server".encode(),
                status=400,
                content_type="text/plain",
            ),
        )
    http.respond(_is_getfeature, _pages(total, page_size))
    return http


# ---------------------------------------------------------------------------
# _make_request - 70 lines of retry logic the fast suite never reached
# ---------------------------------------------------------------------------


def test_a_request_carries_gzip_and_the_accept_header(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, xml_reply(b"<ok/>"))

    body = _make_request(SERVICE, params={"request": "GetFeature"}, accept="application/json")

    assert body == b"<ok/>"
    assert http.last.headers["accept-encoding"] == "gzip, deflate"
    assert http.last.headers["accept"] == "application/json"
    assert http.last.params == {"request": "GetFeature"}


def test_a_request_without_an_accept_header_sends_none(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, xml_reply(b"<ok/>"))

    _make_request(SERVICE)

    assert http.last.headers["accept"] == "*/*"  # httpx's own default, not ours


def test_many_params_are_summarised_for_the_log_not_the_wire(monkeypatch):
    """The >5-param logging branch must not change what is sent."""
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, xml_reply(b"<ok/>"))
    params = {f"p{index}": str(index) for index in range(8)}

    _make_request(SERVICE, params=params)

    assert http.last.params == params


def test_a_dropped_connection_resets_the_pool_then_succeeds(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, protocol_error(), xml_reply(b"<ok/>"))

    assert _make_request(SERVICE) == b"<ok/>"
    assert http.resets == 1
    assert http.sleeps == [1.0]


def test_a_timeout_backs_off_linearly_then_gives_up(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, timeout_error())

    with pytest.raises(WFSError, match="Request failed after 3 attempts"):
        _make_request(SERVICE, timeout=9.0)

    assert len(http.requests) == 3
    assert http.sleeps == [1.0, 2.0]
    assert http.client_timeouts == [9.0, 9.0, 9.0]


def test_a_network_error_is_retried(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, connect_error(), xml_reply(b"<ok/>"))

    assert _make_request(SERVICE) == b"<ok/>"
    assert http.sleeps == [1.0]
    assert http.resets == 0


def test_a_custom_retry_delay_scales_the_backoff(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, timeout_error(), timeout_error(), xml_reply(b"<ok/>"))

    _make_request(SERVICE, retry_delay=0.25)

    assert http.sleeps == [0.25, 0.5]


def test_a_429_honours_an_integer_retry_after(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, error_reply(429, retry_after=11), xml_reply(b"<ok/>"))

    assert _make_request(SERVICE) == b"<ok/>"
    assert http.sleeps == [11.0]


def test_an_http_date_retry_after_degrades_to_linear_backoff(monkeypatch):
    """`Retry-After` is honoured only when `.isdigit()` (wfs.py:263-268).

    RFC 9110 also allows an HTTP-date. gpio ignores that form and uses its own
    backoff instead — graceful degradation, not a defect, but invisible unless
    asserted. Pinning it here makes a future date-parsing change show up as a
    failing test rather than a silent behaviour change.
    """
    http = FakeTransport.install(monkeypatch)
    http.respond(
        SERVICE,
        error_reply(503, retry_after="Wed, 21 Oct 2026 07:28:00 GMT"),
        xml_reply(b"<ok/>"),
    )

    assert _make_request(SERVICE) == b"<ok/>"
    assert http.sleeps == [1.0]


def test_a_5xx_that_never_clears_reports_the_status(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, error_reply(502))

    with pytest.raises(WFSError, match="HTTP error 502"):
        _make_request(SERVICE)
    assert len(http.requests) == 3
    assert http.sleeps == [1.0, 2.0]


@pytest.mark.parametrize("status", [401, 403])
def test_an_auth_status_raises_the_typed_error_without_retrying(monkeypatch, status):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, error_reply(status))

    with pytest.raises(WFSAuthenticationError) as excinfo:
        _make_request(SERVICE)

    assert excinfo.value.status_code == status
    assert len(http.requests) == 1
    assert http.sleeps == []


def test_a_404_names_the_url(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, error_reply(404))

    with pytest.raises(WFSError, match=r"not found \(404\)"):
        _make_request(SERVICE)
    assert len(http.requests) == 1


def test_an_unclassified_status_is_not_retried(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, error_reply(418))

    with pytest.raises(WFSError, match="HTTP error 418"):
        _make_request(SERVICE)
    assert len(http.requests) == 1


# ---------------------------------------------------------------------------
# Capabilities and version negotiation
# ---------------------------------------------------------------------------


def test_capabilities_are_parsed_into_layer_contents(monkeypatch):
    wfs_capabilities(monkeypatch, CAPABILITIES, SCHEMA)

    service = get_wfs_capabilities(SERVICE, "1.1.0")

    assert sorted(service.contents) == ["test:cities", "test:roads"]


@pytest.mark.parametrize(
    ("message", "needle"),
    [
        ("Connection refused by host", "Could not connect to WFS service"),
        ("timeout while reading", "Could not connect to WFS service"),
        ("not well-formed xml at line 3", "Invalid WFS response"),
        ("something else entirely", "Failed to get WFS capabilities"),
    ],
)
def test_capability_failures_are_classified(monkeypatch, message, needle):
    def _boom(*_args, **_kwargs):
        raise RuntimeError(message)

    monkeypatch.setattr("owslib.wfs.WebFeatureService", _boom)

    with pytest.raises(WFSError, match=needle):
        get_wfs_capabilities(SERVICE)


def test_auto_negotiation_walks_down_from_the_newest_version(monkeypatch):
    asked = wfs_capabilities(monkeypatch, {"1.1.0": CAPABILITIES}, SCHEMA)

    version, service = negotiate_wfs_version(SERVICE, "auto")

    assert version == "1.1.0"
    assert asked == ["2.0.0", "1.1.0"]
    assert "test:cities" in service.contents


def test_an_explicit_version_is_not_negotiated(monkeypatch):
    asked = wfs_capabilities(monkeypatch, {"1.1.0": CAPABILITIES}, SCHEMA)

    version, _ = negotiate_wfs_version(SERVICE, "1.1.0")

    assert version == "1.1.0"
    assert asked == ["1.1.0"]  # 2.0.0 was never tried


def test_a_server_speaking_no_supported_version_reports_all_attempts(monkeypatch):
    wfs_capabilities(monkeypatch, {}, SCHEMA)

    with pytest.raises(WFSError) as excinfo:
        negotiate_wfs_version(SERVICE, "auto")

    message = str(excinfo.value)
    assert "Tried: 2.0.0, 1.1.0, 1.0.0" in message
    assert message.count("no capabilities canned") == 3


# ---------------------------------------------------------------------------
# Layer metadata
# ---------------------------------------------------------------------------


def test_layer_info_reads_crs_bbox_and_formats_from_capabilities(monkeypatch):
    wfs_capabilities(monkeypatch, CAPABILITIES, SCHEMA)

    info = get_layer_info(SERVICE, TYPENAME, "1.1.0")

    assert info.typename == "test:cities"
    assert info.title == "Cities"
    assert info.crs_list[0].endswith("4326")
    assert info.bbox == (-180.0, -90.0, 180.0, 90.0)
    assert "application/json" in info.available_formats
    assert info.geometry_column == "the_geom"
    assert info.sortable_attribute == "name"


def test_a_namespace_less_typename_still_matches(monkeypatch):
    wfs_capabilities(monkeypatch, CAPABILITIES, SCHEMA)

    assert get_layer_info(SERVICE, "cities", "1.1.0").typename == "test:cities"


def test_an_unknown_layer_lists_what_is_available(monkeypatch):
    wfs_capabilities(monkeypatch, CAPABILITIES, SCHEMA)

    with pytest.raises(LayerNotFoundError) as excinfo:
        get_layer_info(SERVICE, "test:absent", "1.1.0")

    assert "test:cities" in str(excinfo.value)


def test_an_unavailable_describe_feature_type_degrades_to_defaults(monkeypatch):
    wfs_capabilities(monkeypatch, CAPABILITIES, schema=None)

    info = get_layer_info(SERVICE, TYPENAME, "1.1.0")

    assert info.geometry_column == "geometry"
    assert info.sortable_attribute is None


def test_list_available_layers_reports_every_feature_type(monkeypatch):
    wfs_capabilities(monkeypatch, CAPABILITIES, SCHEMA)

    layers = list_available_layers(SERVICE, "1.1.0")

    assert [layer["typename"] for layer in layers] == ["test:cities", "test:roads"]
    assert layers[1]["bbox"] == (-125.0, 24.0, -66.0, 50.0)
    assert layers[0]["title"] == "Cities"


# ---------------------------------------------------------------------------
# The feature-count probe
# ---------------------------------------------------------------------------


def test_wfs_10_has_no_hits_probe_to_issue(monkeypatch):
    http = FakeTransport.install(monkeypatch)

    assert _get_feature_count(SERVICE, TYPENAME, "1.0.0") is None
    assert http.requests == []


@pytest.mark.parametrize("attribute", ["numberOfFeatures", "numberMatched"])
def test_either_count_attribute_is_read(monkeypatch, attribute):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_hits, xml_reply(f'<wfs:FeatureCollection {attribute}="1234"/>'.encode()))

    assert _get_feature_count(SERVICE, TYPENAME, "1.1.0") == 1234


def test_the_hits_probe_uses_the_20_parameter_names(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_hits, xml_reply(b'<c numberMatched="1"/>'))

    _get_feature_count(SERVICE, TYPENAME, "2.0.0")

    assert http.last.params["typeNames"] == TYPENAME
    assert "typeName" not in http.last.params
    assert http.last.params["version"] == "2.0.0"


def test_a_bbox_reaches_the_hits_probe(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_hits, xml_reply(b'<c numberMatched="1"/>'))

    _get_feature_count(SERVICE, TYPENAME, "1.1.0", bbox=(-1, -2, 3, 4), crs="EPSG:4326")

    assert http.last.params["bbox"].startswith("-1")
    assert http.last.params["bbox"].endswith("EPSG:4326")


def test_a_countless_answer_is_not_a_failure(monkeypatch, caplog):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_hits, xml_reply(b"<wfs:FeatureCollection/>"))

    with caplog.at_level("WARNING"):
        assert _get_feature_count(SERVICE, TYPENAME, "1.1.0") is None
    assert caplog.text == ""


def test_a_failed_probe_says_what_it_costs(monkeypatch, caplog):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_hits, error_reply(500))

    with caplog.at_level("WARNING"):
        assert _get_feature_count(SERVICE, TYPENAME, "1.1.0") is None

    assert "auto-tiling and pagination cannot engage" in caplog.text
    assert len(http.requests) == 3


def test_a_speculative_probe_failure_stays_quiet(monkeypatch, caplog):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_hits, error_reply(400))

    with caplog.at_level("WARNING"):
        assert _get_feature_count(SERVICE, TYPENAME, "2.0.0", warn_on_failure=False) is None
    assert caplog.text == ""


# ---------------------------------------------------------------------------
# The startIndex probe
# ---------------------------------------------------------------------------


def test_the_startindex_probe_reads_the_cap_out_of_the_rejection(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(
        _is_startindex_probe,
        bytes_reply(
            b"Error: startIndex is limited to 50,000 features",
            status=400,
            content_type="text/plain",
        ),
    )

    assert _probe_startindex_limit(SERVICE, TYPENAME, "1.1.0") == 50000
    assert http.last.params["startIndex"] == "50001"
    assert http.last.params["maxFeatures"] == "1"


def test_an_unparseable_cap_falls_back_to_50000(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(
        _is_startindex_probe,
        bytes_reply(b"startIndex too large", status=400, content_type="text/plain"),
    )

    assert _probe_startindex_limit(SERVICE, TYPENAME, "1.1.0") == 50000


def test_a_server_with_no_cap_returns_none(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_startindex_probe, geojson_reply(_geojson(0)))

    assert _probe_startindex_limit(SERVICE, TYPENAME, "1.1.0") is None


def test_a_failed_startindex_probe_warns_about_unbounded_pagination(monkeypatch, caplog):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_startindex_probe, connect_error())

    with caplog.at_level("WARNING"):
        assert _probe_startindex_limit(SERVICE, TYPENAME, "1.1.0") is None

    assert "pagination past a server's limit" in caplog.text


# ---------------------------------------------------------------------------
# _fetch_wfs_page - streaming, content-type dispatch, retries
# ---------------------------------------------------------------------------


def test_a_geojson_page_becomes_an_arrow_table(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, geojson_reply(_geojson(5)))

    table = _fetch_wfs_page(SERVICE, output_format="application/json")

    assert table.num_rows == 5
    assert "geometry" in table.column_names
    assert http.last.headers["accept"] == "application/json"
    assert http.client_timeouts == [600]


def test_a_declared_server_crs_rides_along_on_the_table(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, geojson_reply(_geojson(2, crs="urn:ogc:def:crs:EPSG::3857")))

    table = _fetch_wfs_page(SERVICE)

    # The URN form the server sent is normalised to the EPSG form gpio carries.
    assert table.schema.metadata[b"_wfs_server_crs"] == b"EPSG:3857"


def test_a_startindex_rejection_is_reported_as_such(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(
        SERVICE,
        bytes_reply(
            b"Error: startIndex exceeds the server maximum", status=400, content_type="text/plain"
        ),
    )

    with pytest.raises(WFSError, match="Server rejected paginated request"):
        _fetch_wfs_page(SERVICE)
    assert len(http.requests) == 1


def test_another_400_reports_the_body(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, bytes_reply(b"unknown typeName", status=400, content_type="text/plain"))

    with pytest.raises(WFSError, match="HTTP 400: unknown typeName"):
        _fetch_wfs_page(SERVICE)


def test_a_5xx_page_is_not_retried_by_the_page_fetcher(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, error_reply(503))

    with pytest.raises(WFSError, match="HTTP 503"):
        _fetch_wfs_page(SERVICE)
    assert len(http.requests) == 1


def test_a_transient_page_failure_resets_the_pool_and_retries(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, connect_error(), geojson_reply(_geojson(3)))

    assert _fetch_wfs_page(SERVICE).num_rows == 3
    assert http.resets == 1
    assert http.sleeps == [2.0]  # the page fetcher's own retry_delay default


def test_a_page_that_never_arrives_names_the_attempt_count(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, timeout_error())

    with pytest.raises(WFSError, match="after 3 attempts"):
        _fetch_wfs_page(SERVICE)
    assert len(http.requests) == 3
    assert http.sleeps == [2.0, 4.0]


@pytest.mark.parametrize("content_type", ["text/html", "text/plain", "application/xhtml+xml"])
def test_an_error_page_returned_with_200_is_refused(monkeypatch, content_type):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, bytes_reply(b"<html>login required</html>", content_type=content_type))

    with pytest.raises(WFSError) as excinfo:
        _fetch_wfs_page(SERVICE)
    assert content_type in str(excinfo.value)


def test_the_accept_header_follows_the_negotiated_output_format(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(SERVICE, geojson_reply(_geojson(1)))

    _fetch_wfs_page(SERVICE, output_format="text/xml; subtype=gml/3.1.1")

    assert "gml" in http.last.headers["accept"]


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


def test_a_small_layer_is_fetched_in_one_request(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=40)

    table = fetch_all_features_duckdb(SERVICE, TYPENAME, "1.1.0", page_size=1000)

    assert table.num_rows == 40
    assert len(http.matching(_is_getfeature)) == 1
    assert "startIndex" not in http.matching(_is_getfeature)[0].params


def test_sequential_pagination_walks_the_start_indexes(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=250, page_size=100)

    table = fetch_all_features_duckdb(SERVICE, TYPENAME, "1.1.0", page_size=100)

    assert table.num_rows == 250
    pages = http.matching(_is_getfeature)
    assert [page.params.get("startIndex") for page in pages] == [None, "100", "200"]
    assert [page.params["maxFeatures"] for page in pages] == ["100", "100", "50"]


def test_parallel_pagination_covers_every_window(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=250, page_size=100)

    table = fetch_all_features_duckdb(SERVICE, TYPENAME, "1.1.0", page_size=100, max_workers=3)

    assert table.num_rows == 250
    starts = sorted(int(page.params.get("startIndex", 0)) for page in http.matching(_is_getfeature))
    assert starts == [0, 100, 200]


def test_a_limit_clamps_the_pages_requested(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=1000, page_size=100)

    table = fetch_all_features_duckdb(SERVICE, TYPENAME, "1.1.0", max_features=150, page_size=100)

    assert table.num_rows == 150
    assert [page.params["maxFeatures"] for page in http.matching(_is_getfeature)] == ["100", "50"]


def test_a_server_cap_shrinks_the_page_size_and_keeps_going(monkeypatch):
    """A first page shorter than asked for switches the fetch to adaptive paging."""
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=300, page_size=75)

    table = fetch_all_features_duckdb(SERVICE, TYPENAME, "1.1.0", page_size=1000)

    assert table.num_rows == 300
    sizes = [page.params.get("maxFeatures") for page in http.matching(_is_getfeature)]
    # The first request is the unpaginated single fetch (no maxFeatures at all);
    # the short answer switches the fetch to pages of exactly what the server gives.
    assert sizes[0] is None
    assert sizes[1:] == ["75", "75", "75", "75"]


def test_a_startindex_cap_beyond_reach_is_an_actionable_error(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=500_000, page_size=100, startindex_limit=50000)

    with pytest.raises(WFSError) as excinfo:
        fetch_all_features_duckdb(SERVICE, TYPENAME, "1.1.0", page_size=100)

    message = str(excinfo.value)
    assert "limits startIndex to 50,000" in message
    assert "--auto-tile" in message


def test_a_sortby_attribute_reaches_every_page(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    stub_service(http, total=150, page_size=100)

    fetch_all_features_duckdb(SERVICE, TYPENAME, "1.1.0", page_size=100, sort_by="name")

    assert all(page.params["sortBy"] == "name" for page in http.matching(_is_getfeature))


def test_a_page_failure_names_the_page_and_offset(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_hits, xml_reply(b'<c numberMatched="300"/>'))
    http.respond(_is_startindex_probe, geojson_reply(_geojson(0)))
    http.respond(_is_getfeature, geojson_reply(_geojson(100)), error_reply(500))

    with pytest.raises(WFSError, match=r"Failed to fetch page 2 \(offset 100\)"):
        fetch_all_features_duckdb(SERVICE, TYPENAME, "1.1.0", page_size=100)


def test_a_parallel_page_failure_names_the_page_and_offset(monkeypatch):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_hits, xml_reply(b'<c numberMatched="300"/>'))
    http.respond(_is_startindex_probe, geojson_reply(_geojson(0)))
    http.respond(_is_getfeature, error_reply(500))

    with pytest.raises(WFSError, match=r"Failed to fetch page \d \(offset \d+\)"):
        fetch_all_features_duckdb(SERVICE, TYPENAME, "1.1.0", page_size=100, max_workers=3)


def test_an_unknowable_count_falls_back_to_a_single_request(monkeypatch, caplog):
    http = FakeTransport.install(monkeypatch)
    http.respond(_is_hits, xml_reply(b"<c/>"))
    http.respond(_is_getfeature, geojson_reply(_geojson(7)))

    with caplog.at_level("WARNING"):
        table = fetch_all_features_duckdb(SERVICE, TYPENAME, "1.1.0", page_size=10)

    assert table.num_rows == 7
    assert len(http.matching(_is_getfeature)) == 1


# ---------------------------------------------------------------------------
# convert_wfs_to_geoparquet - the file that gets written
# ---------------------------------------------------------------------------


def test_the_written_file_is_spec_clean(monkeypatch, tmp_path):
    http = FakeTransport.install(monkeypatch)
    wfs_capabilities(monkeypatch, CAPABILITIES, SCHEMA)
    stub_service(http, total=30)
    out = tmp_path / "cities.parquet"

    convert_wfs_to_geoparquet(SERVICE, TYPENAME, str(out), version="1.1.0", verbose=True)

    assert spec_problems(out) == []
    assert pq.read_metadata(out).num_rows == 30
    assert geo_block(out)["primary_column"] == "geometry"
    # Both CRS carriers read separately, never merged through source_crs_string.
    assert geo_block_crs_id(out) == "<no crs key -- resolves as OGC:CRS84>"
    assert logical_crs_id(out) == "<no native geo type>"
    assert "bbox" in pq.read_schema(out).names
    assert geo_block(out)["columns"]["geometry"]["encoding"] == "WKB"


def test_the_negotiated_output_format_reaches_the_getfeature_request(monkeypatch, tmp_path):
    http = FakeTransport.install(monkeypatch)
    wfs_capabilities(monkeypatch, CAPABILITIES, SCHEMA)
    stub_service(http, total=5)

    convert_wfs_to_geoparquet(
        SERVICE, TYPENAME, str(tmp_path / "o.parquet"), version="1.1.0", skip_hilbert=True
    )

    page = http.matching(_is_getfeature)[0]
    assert page.params["outputFormat"] == "application/json"
    assert page.params["typeName"] == TYPENAME
    assert page.params["service"] == "WFS"


def test_a_limit_and_skip_flags_are_honoured_end_to_end(monkeypatch, tmp_path):
    http = FakeTransport.install(monkeypatch)
    wfs_capabilities(monkeypatch, CAPABILITIES, SCHEMA)
    stub_service(http, total=100)
    out = tmp_path / "limited.parquet"

    convert_wfs_to_geoparquet(
        SERVICE,
        TYPENAME,
        str(out),
        version="1.1.0",
        limit=12,
        skip_hilbert=True,
        skip_bbox=True,
    )

    assert pq.read_metadata(out).num_rows == 12
    assert "bbox" not in pq.read_schema(out).names
    assert spec_problems(out) == []


def test_an_existing_output_is_refused_without_overwrite(monkeypatch, tmp_path):
    http = FakeTransport.install(monkeypatch)
    out = tmp_path / "exists.parquet"
    out.write_bytes(b"not parquet")

    with pytest.raises(WFSError, match="Output file exists"):
        convert_wfs_to_geoparquet(SERVICE, TYPENAME, str(out), version="1.1.0")
    assert http.requests == []


def test_overwrite_replaces_the_file(monkeypatch, tmp_path):
    http = FakeTransport.install(monkeypatch)
    wfs_capabilities(monkeypatch, CAPABILITIES, SCHEMA)
    stub_service(http, total=6)
    out = tmp_path / "exists.parquet"
    out.write_bytes(b"not parquet")

    convert_wfs_to_geoparquet(SERVICE, TYPENAME, str(out), version="1.1.0", overwrite=True)

    assert pq.read_metadata(out).num_rows == 6
    assert spec_problems(out) == []


def test_a_service_url_query_parameter_survives_every_request(monkeypatch, tmp_path):
    """httpx `params=` replaces a URL's query; gpio merges instead (#828)."""
    http = FakeTransport.install(monkeypatch)
    wfs_capabilities(monkeypatch, CAPABILITIES, SCHEMA)
    stub_service(http, total=8)
    keyed = SERVICE + "?apikey=secret"

    convert_wfs_to_geoparquet(
        keyed, TYPENAME, str(tmp_path / "keyed.parquet"), version="1.1.0", skip_hilbert=True
    )

    assert http.requests
    assert all(request.params.get("apikey") == "secret" for request in http.requests)
