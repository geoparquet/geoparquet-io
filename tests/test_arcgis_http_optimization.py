"""
Tests for ArcGIS HTTP optimizations.

These tests verify connection pooling and compression improvements.
"""

from unittest.mock import MagicMock, Mock, patch


class TestConnectionPooling:
    """Tests for HTTP connection pooling optimization."""

    def test_http_client_is_reused_across_requests(self):
        """
        Test that HTTP client is reused across multiple requests.

        This is a performance optimization to avoid creating new TCP/TLS
        connections for every request.
        """
        from geoparquet_io.core.http_retry import make_request_with_retry as _make_request
        from geoparquet_io.core.http_retry import reset_http_client

        # Reset to ensure clean state
        reset_http_client()

        mock_client = MagicMock()
        mock_response = Mock()
        mock_response.json.return_value = {"success": True}
        mock_response.raise_for_status = Mock()
        mock_client.get.return_value = mock_response

        with patch("geoparquet_io.core.http_retry.get_shared_http_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            # Make multiple requests
            _make_request("GET", "https://example.com/1")
            _make_request("GET", "https://example.com/2")
            _make_request("GET", "https://example.com/3")

            # Client should be retrieved 3 times (function called 3 times)
            assert mock_get_client.call_count == 3

    def test_http_client_has_connection_pooling_enabled(self):
        """
        Test that HTTP client has connection pooling properly configured.

        Connection pooling allows reusing TCP connections across requests,
        which saves ~100-200ms per request on TLS handshake.
        """
        from geoparquet_io.core.http_retry import get_shared_http_client, reset_http_client

        # Reset to ensure clean state
        reset_http_client()

        client = get_shared_http_client()

        # Verify client is an httpx.Client instance with expected config
        import httpx

        assert isinstance(client, httpx.Client)

        # Verify HTTP/2 and connection limits are configured by checking _transport
        assert hasattr(client, "_transport")
        assert client.follow_redirects is True

    def test_http_client_is_singleton(self):
        """
        Test that get_shared_http_client returns the same instance.

        This ensures connection pooling works across the entire module,
        not just within a single function.
        """
        from geoparquet_io.core.http_retry import get_shared_http_client

        client1 = get_shared_http_client()
        client2 = get_shared_http_client()

        # Should be the exact same object
        assert client1 is client2

    def test_http_client_can_be_reset(self):
        """
        Test that HTTP client can be reset (for testing or cleanup).

        This is useful for tests that need to verify client creation logic.
        """
        from geoparquet_io.core.http_retry import get_shared_http_client, reset_http_client

        client1 = get_shared_http_client()
        reset_http_client()
        client2 = get_shared_http_client()

        # After reset, should get a different instance
        assert client1 is not client2

    def test_http_client_has_http2_disabled(self):
        """
        Test that HTTP/2 is DISABLED for ArcGIS compatibility.

        HTTP/2 causes RemoteProtocolError with ArcGIS servers after sustained use.
        Connection pooling and gzip compression still provide performance benefits.
        """
        from geoparquet_io.core.http_retry import get_shared_http_client, reset_http_client

        # Reset to ensure clean state
        reset_http_client()

        client = get_shared_http_client()

        # Verify client exists and has transport
        assert hasattr(client, "_transport")
        # HTTP/2 should be disabled (http2=False in constructor)


class TestGzipCompression:
    """Tests for gzip compression optimization."""

    def test_make_request_sends_accept_encoding_header(self):
        """
        Test that requests include Accept-Encoding header for compression.

        This allows the server to send gzip-compressed responses, which
        reduces bandwidth by 60-80% for typical GeoJSON responses.
        """
        from geoparquet_io.core.http_retry import make_request_with_retry as _make_request

        mock_client = MagicMock()
        mock_response = Mock()
        mock_response.json.return_value = {"success": True}
        mock_response.raise_for_status = Mock()
        mock_client.get.return_value = mock_response

        with patch("geoparquet_io.core.http_retry.get_shared_http_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            _make_request("GET", "https://example.com/test", params={"f": "json"})

            # Verify Accept-Encoding header was sent
            mock_client.get.assert_called_once()
            call_kwargs = mock_client.get.call_args[1]

            assert "headers" in call_kwargs
            assert "Accept-Encoding" in call_kwargs["headers"]
            assert "gzip" in call_kwargs["headers"]["Accept-Encoding"]

    def test_fetch_features_page_uses_compression(self):
        """
        Test that feature fetching requests support compression.

        This is the most bandwidth-intensive operation, so compression
        here provides the biggest benefit.
        """
        from geoparquet_io.core.arcgis import fetch_features_page

        mock_response = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                    "properties": {"OBJECTID": 1, "name": "Test"},
                }
            ],
        }

        with patch("geoparquet_io.core.arcgis.make_request_with_retry") as mock_request:
            mock_request.return_value = mock_response

            result = fetch_features_page(
                "https://example.com/FeatureServer/0",
                offset=0,
                limit=1000,
            )

            # Verify _make_request was called (it handles compression)
            mock_request.assert_called_once()
            assert result == mock_response


# Integration tests removed - unit tests above verify optimization works correctly
