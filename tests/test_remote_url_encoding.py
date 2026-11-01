"""
An http(s) URL is taken as already percent-encoded (#825).

``resolve_file_url()`` used to percent-encode the path component of every
HTTP(S) URL, with ``%`` absent from its ``safe`` set. A URL the user pasted
from a browser -- ``https://example.com/my%20file.parquet`` -- was therefore
encoded a second time into ``my%2520file.parquet``, and the server answered
404 for a file it holds.

There is no way to tell ``my%20file.parquet`` (an encoded space) from
``my%2520file.parquet`` (a file whose name really contains ``%20``) by looking
at the string, which is the same shape as the SQL-escaping problem #718/#803
solved: transform exactly once, at a boundary where the input's state is known.
Per RFC 3986 a URL *is* the encoded form, so the boundary is the caller: gpio
passes an http(s) URL through verbatim and encodes nothing.

The deliberate cost is the breaking side, pinned below: a URL containing a raw
space or bracket is no longer encoded for the user either.
"""

import functools
import http.server
import shutil
import threading
from pathlib import Path

import pytest

from geoparquet_io.core.duckdb_utils import sql_path
from geoparquet_io.core.file_utils import resolve_file_url

# =============================================================================
# resolve_file_url(): an http(s) URL is passed through
# =============================================================================


class TestHttpUrlIsTakenAsEncoded:
    """A user-supplied http(s) URL reaches the reader byte-for-byte."""

    def test_percent_encoded_space_is_not_re_encoded(self):
        """The regression in #825: ``%20`` must not become ``%2520``."""
        url = "https://example.com/my%20file.parquet"
        assert resolve_file_url(url) == url

    def test_literal_percent_20_in_a_filename_survives(self):
        """A file genuinely named ``my%20file.parquet`` is requested as written.

        Its URL spells the ``%`` as ``%25``; encoding again would ask for
        ``my%252520file.parquet``.
        """
        url = "https://example.com/my%2520file.parquet"
        assert resolve_file_url(url) == url

    def test_plain_url_unchanged(self):
        url = "https://example.com/path/to/file.parquet"
        assert resolve_file_url(url) == url

    def test_glob_and_query_string_unchanged(self):
        """Globs, hive ``=`` and a query string (presigned URLs) survive."""
        url = "https://example.com/data/state=CA/*.parquet?X-Amz-Signature=abc%2Fdef"
        assert resolve_file_url(url) == url

    def test_http_scheme_too(self):
        url = "http://example.com/my%20file.parquet"
        assert resolve_file_url(url) == url

    def test_raw_space_is_now_passed_through_unencoded_breaking_change(self):
        """BREAKING (#825): gpio no longer encodes a raw space for the user.

        A URL with a literal space is not a valid URL per RFC 3986. gpio used
        to paper over that; it now hands the string to the reader as given, so
        this is the case that changes behaviour for existing callers. Encode
        the URL before passing it in.
        """
        url = "https://example.com/my file.parquet"
        assert resolve_file_url(url) == url

    def test_raw_bracket_is_now_passed_through_unencoded_breaking_change(self):
        """BREAKING (#825): the same for any other character gpio used to encode."""
        url = "https://example.com/my<file>.parquet"
        assert resolve_file_url(url) == url

    def test_verbose_logs_and_returns_the_url_unchanged(self):
        """The verbose debug line reports the URL as given, not a re-encoding."""
        url = "https://example.com/my%20file.parquet"
        assert resolve_file_url(url, verbose=True) == url

    def test_non_http_remote_schemes_unchanged(self):
        """S3/GCS URLs were never encoded and still are not.

        Azure is absent on purpose: ``az://`` reads are refused at this boundary
        (write destinations only) -- see tests/test_file_utils.py.
        """
        for url in (
            "s3://bucket/my%20file.parquet",
            "gs://bucket/my%20file.parquet",
        ):
            assert resolve_file_url(url) == url


class TestSqlPathTransformsOnce:
    """``sql_path`` of a resolved URL adds SQL quoting and nothing else."""

    def test_percent_encoded_url_is_neither_re_encoded_nor_escaped(self):
        url = "https://example.com/my%20file.parquet"
        assert sql_path(resolve_file_url(url)) == f"'{url}'"

    def test_literal_percent_20_in_a_filename_survives(self):
        url = "https://example.com/my%2520file.parquet"
        assert sql_path(resolve_file_url(url)) == f"'{url}'"

    def test_raw_space_is_now_passed_through_unencoded_breaking_change(self):
        """BREAKING (#825): the SQL boundary does not encode either."""
        url = "https://example.com/my file.parquet"
        assert sql_path(resolve_file_url(url)) == f"'{url}'"

    def test_only_the_sql_quote_is_transformed(self):
        """The one transform at the SQL boundary is the quote escape."""
        url = "https://example.com/o'brien%20data.parquet"
        assert sql_path(resolve_file_url(url)) == "'https://example.com/o''brien%20data.parquet'"


# =============================================================================
# End to end: the server sees the path the user typed
# =============================================================================


class _RecordingHandler(http.server.SimpleHTTPRequestHandler):
    """Static file handler that records every raw request path it is given."""

    seen: list[str] = []

    def do_GET(self):  # noqa: N802 - http.server API
        type(self).seen.append(self.path)
        super().do_GET()

    def do_HEAD(self):  # noqa: N802 - http.server API
        type(self).seen.append(self.path)
        super().do_HEAD()

    def log_message(self, *args):  # pragma: no cover - silence stderr noise
        pass


@pytest.fixture
def recording_http_server(tmp_path):
    """Serve a directory over loopback HTTP, recording the raw paths requested.

    Yields ``(base_url, served_dir, seen_paths)``. No outside network is
    touched, so this is not a ``network`` test.
    """

    served = tmp_path / "served"
    served.mkdir()

    handler_cls = type("_Handler", (_RecordingHandler,), {"seen": []})
    httpd = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0), functools.partial(handler_cls, directory=str(served))
    )
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}", served, handler_cls.seen
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


@pytest.mark.integration
class TestRemoteReadRequestsTheUrlVerbatim:
    """Prove the fix on a real read, not just on the helper's return value."""

    def test_inspect_meta_requests_the_encoded_path_as_given(
        self, recording_http_server, buildings_test_file
    ):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        base_url, served, seen = recording_http_server
        shutil.copyfile(buildings_test_file, served / "my file.parquet")

        result = CliRunner().invoke(cli, ["inspect", "meta", f"{base_url}/my%20file.parquet"])

        assert seen, "the server was never contacted"
        # Before the fix the server saw /my%2520file.parquet and answered 404.
        assert all(path.startswith("/my%20file.parquet") for path in seen), seen
        assert result.exit_code == 0, result.output


def test_no_module_percent_encodes_a_read_path():
    """Guard the deletion: ``file_utils`` no longer encodes anything (#825)."""
    source = (Path(__file__).parents[1] / "geoparquet_io" / "core" / "file_utils.py").read_text(
        encoding="utf-8"
    )
    assert "quote(" not in source, "file_utils must not percent-encode a path; see #825"
