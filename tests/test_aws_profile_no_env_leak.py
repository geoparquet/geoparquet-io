"""A ``profile=`` / ``aws_profile=`` argument must not leak into ``os.environ`` (#1055).

The old ``setup_aws_profile_if_needed`` wrote ``AWS_PROFILE`` into the process
environment and never put it back, so one Python API call with a profile changed
the credentials of every later call. The profile now travels in the ambient S3
config (:func:`~geoparquet_io.core.duckdb_utils.s3_config_scope`), which DuckDB's
``credential_chain`` secret and the obstore store both read explicitly.
"""

import http.server
import os
import sys
import threading

import duckdb
import pytest

from geoparquet_io.core import duckdb_utils
from geoparquet_io.core.duckdb_utils import (
    _s3_secret_sql,
    get_active_s3_config,
    s3_config_scope,
    uses_aws_profile,
)


class _Stop(Exception):
    """Raised by a stub to end the call once it has recorded what it saw."""


@pytest.fixture(params=["outer", None], ids=["env-set", "env-unset"])
def previous_profile(request, monkeypatch):
    if request.param is None:
        monkeypatch.delenv("AWS_PROFILE", raising=False)
    else:
        monkeypatch.setenv("AWS_PROFILE", request.param)
    return request.param


def _recorder(seen):
    def _record(*args, **kwargs):
        seen.append(
            {
                "env": os.environ.get("AWS_PROFILE"),
                "profile": get_active_s3_config().get("profile"),
            }
        )
        raise _Stop

    return _record


def test_core_fix_keeps_aws_profile_env_and_passes_profile(previous_profile, monkeypatch):
    """A core function called with profile= neither changes nor leaks AWS_PROFILE."""
    from geoparquet_io.core import check_fixes

    seen: list = []
    monkeypatch.setattr(check_fixes, "resolve_file_url", _recorder(seen))

    with pytest.raises(_Stop):
        check_fixes.fix_compression(
            "s3://bucket/in.parquet", "s3://bucket/out.parquet", profile="prod"
        )

    assert seen == [{"env": previous_profile, "profile": "prod"}]
    assert os.environ.get("AWS_PROFILE") == previous_profile
    assert get_active_s3_config().get("profile") is None


def test_api_read_partition_keeps_aws_profile_env(previous_profile, monkeypatch):
    """The Python API's aws_profile= reaches DuckDB without touching the environment."""
    import geoparquet_io as gpio

    seen: list = []
    monkeypatch.setattr(duckdb_utils, "get_duckdb_connection", _recorder(seen))

    with pytest.raises(_Stop):
        gpio.read_partition("s3://bucket/data.parquet", aws_profile="prod")

    assert seen == [{"env": previous_profile, "profile": "prod"}]
    assert os.environ.get("AWS_PROFILE") == previous_profile


def test_uses_aws_profile_scopes_keyword_and_positional_profile():
    @uses_aws_profile
    def probe(path, profile=None):
        return get_active_s3_config().get("profile")

    assert probe("s3://b/k", profile="kw") == "kw"
    assert probe("s3://b/k", "pos") == "pos"
    assert get_active_s3_config().get("profile") is None


def test_uses_aws_profile_without_profile_keeps_outer_scope():
    @uses_aws_profile
    def probe(path, profile=None):
        return get_active_s3_config()

    with s3_config_scope({"profile": "outer", "s3_region": "eu-west-1"}):
        assert probe("s3://b/k") == {"profile": "outer", "s3_region": "eu-west-1"}
        inner = probe("s3://b/k", profile="inner")
        assert inner == {"profile": "inner", "profile_from_env": False, "s3_region": "eu-west-1"}


def test_uses_aws_profile_rejects_function_without_profile_parameter():
    with pytest.raises(TypeError, match="profile"):

        @uses_aws_profile
        def probe(path):
            return path


def _settings_recorder(seen):
    def _fake_sql(settings):
        seen.append(dict(settings))
        return "SELECT 1"

    return _fake_sql


def test_connection_secret_uses_ambient_profile(monkeypatch):
    """get_duckdb_connection(use_s3_auth=True) builds the secret from the scoped profile."""
    seen: list = []
    monkeypatch.setattr(duckdb_utils, "_install_and_load_extension", lambda con, name: None)
    monkeypatch.setattr(duckdb_utils, "_s3_secret_sql", _settings_recorder(seen))

    with s3_config_scope({"profile": "prod"}):
        con = duckdb_utils.get_duckdb_connection(
            load_spatial=False, load_httpfs=True, use_s3_auth=True
        )
    con.close()

    assert [s["profile"] for s in seen] == ["prod"]


@pytest.mark.parametrize(("profile", "expected"), [("prod", ["prod"]), (None, [])])
def test_connection_secret_follows_scoped_profile_without_use_s3_auth(
    monkeypatch, profile, expected
):
    """A scoped profile alone builds the secret. No profile and no use_s3_auth builds none."""
    seen: list = []
    monkeypatch.setattr(duckdb_utils, "_install_and_load_extension", lambda con, name: None)
    monkeypatch.setattr(duckdb_utils, "_s3_secret_sql", _settings_recorder(seen))

    with s3_config_scope({"profile": profile}):
        con = duckdb_utils.get_duckdb_connection(load_spatial=False, load_httpfs=True)
    con.close()

    assert [s["profile"] for s in seen] == expected


def test_connection_secret_gets_the_resolved_settings(monkeypatch):
    """Explicit kwargs beat the ambient scope, and the secret sees the result."""
    seen: list = []
    monkeypatch.setattr(duckdb_utils, "_install_and_load_extension", lambda con, name: None)
    monkeypatch.setattr(duckdb_utils, "_s3_secret_sql", _settings_recorder(seen))

    scope = {"profile": "prod", "s3_endpoint": "ambient:1", "s3_region": "eu-west-1"}
    with s3_config_scope(scope):
        con = duckdb_utils.get_duckdb_connection(
            load_spatial=False, load_httpfs=True, s3_endpoint="minio:9000", s3_use_ssl=False
        )
    con.close()

    assert seen == [
        {"profile": "prod", "endpoint": "minio:9000", "region": "eu-west-1", "use_ssl": False}
    ]


# =============================================================================
# The S3 secret is built from the fully resolved config (review of #1274)
# =============================================================================
#
# A ``credential_chain`` secret carries its own endpoint and region, and DuckDB
# reads those over ``SET s3_endpoint`` / ``SET s3_region``. So the secret has to
# be built from the same resolved values, or a profile sends a MinIO/R2 read to
# AWS and the profile's region beats ``--s3-region``. Everything below uses fake
# credentials in temp AWS files and a loopback server, so no test touches AWS.

_FAKE_STATIC_KEY = "AKIAFAKESTATIC000001"
_FAKE_ENV_KEY = "AKIAFAKEENVKEY000001"


def _aws_extension_available() -> bool:
    con = duckdb.connect()
    try:
        duckdb_utils._install_and_load_extension(con, "aws")
        return True
    except Exception:
        return False
    finally:
        con.close()


needs_aws_extension = pytest.mark.skipif(
    not _aws_extension_available(), reason="DuckDB aws extension is not installable here"
)

# These tests point the AWS SDK at fake files and keys with monkeypatch.setenv.
# On Windows the aws extension has its own C runtime, which keeps the env it saw
# when the DLL loaded (at collection, in _aws_extension_available), so a
# variable set later in the same process never reaches the SDK. A real user sets
# the env before gpio starts, so this only affects the test, not the feature.
needs_runtime_env_in_aws_sdk = pytest.mark.skipif(
    sys.platform == "win32",
    reason="the aws extension does not see env vars set after it loads on Windows",
)


@pytest.fixture
def fake_aws_files(tmp_path, monkeypatch):
    """Point the AWS SDK at temp files holding one fake static profile."""
    config = tmp_path / "config"
    config.write_text("[profile fakep]\nregion = eu-west-3\n")
    credentials = tmp_path / "credentials"
    credentials.write_text(
        f"[fakep]\naws_access_key_id = {_FAKE_STATIC_KEY}\naws_secret_access_key = fakesecret\n"
    )
    for name in (
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_ENDPOINT_URL",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    return tmp_path


class _S3Recorder(http.server.BaseHTTPRequestHandler):
    """A loopback stand-in for S3 that records each request and answers 404."""

    seen: list[dict] = []

    def _record(self):
        type(self).seen.append(
            {"path": self.path, "authorization": self.headers.get("Authorization", "")}
        )
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_GET = _record  # noqa: N815 - http.server API
    do_HEAD = _record  # noqa: N815 - http.server API

    def log_message(self, *args):  # pragma: no cover - silence stderr noise
        pass


@pytest.fixture
def fake_s3_endpoint():
    """Yield ``(host:port, seen)`` for a loopback S3 stand-in."""
    handler = type("_Handler", (_S3Recorder,), {"seen": []})
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"127.0.0.1:{httpd.server_address[1]}", handler.seen
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def _secret_string(con) -> str:
    """The one secret on ``con``, as DuckDB prints it (the secret key is redacted)."""
    rows = con.execute("SELECT secret_string FROM duckdb_secrets()").fetchall()
    assert len(rows) == 1, f"expected one secret, found {len(rows)}"
    return rows[0][0]


def _read_s3(con, url="s3://fakebucket/x.parquet"):
    con.execute("SET http_retries = 0")
    with pytest.raises(duckdb.Error):
        con.execute(f"SELECT 1 FROM read_parquet('{url}')").fetchall()


class TestS3SecretSql:
    def test_no_profile_uses_the_default_chain(self):
        sql = _s3_secret_sql({})
        assert "PROVIDER credential_chain" in sql
        assert "PROFILE" not in sql
        assert "CHAIN '" not in sql

    def test_profile_keeps_sso_and_credential_process_in_the_chain(self):
        sql = _s3_secret_sql({"profile": "o'brien"})
        assert "PROFILE 'o''brien'" in sql
        assert "CHAIN 'config;process;sso'" in sql

    def test_endpoint_region_and_ssl_go_into_the_secret(self):
        sql = _s3_secret_sql(
            {"endpoint": "minio.local:9000", "region": "us-west-2", "use_ssl": False}
        )
        assert "ENDPOINT 'minio.local:9000'" in sql
        assert "URL_STYLE 'path'" in sql
        assert "USE_SSL false" in sql
        assert "REGION 'us-west-2'" in sql

    def test_ssl_defaults_on_for_a_custom_endpoint(self):
        assert "USE_SSL true" in _s3_secret_sql({"endpoint": "r2.example.com"})

    def test_values_are_escaped(self):
        sql = _s3_secret_sql({"endpoint": "a'b", "region": "r'x"})
        assert "ENDPOINT 'a''b'" in sql
        assert "REGION 'r''x'" in sql


@needs_aws_extension
@pytest.mark.usefixtures("fake_aws_files")
class TestProfileSecretKeepsResolvedSettings:
    def test_profile_with_custom_endpoint_keeps_the_endpoint(self, fake_s3_endpoint):
        endpoint, seen = fake_s3_endpoint
        scope = {"profile": "fakep", "s3_endpoint": endpoint, "s3_use_ssl": False}
        with s3_config_scope(scope):
            con = duckdb_utils.get_duckdb_connection(load_spatial=False, load_httpfs=True)
        try:
            secret = _secret_string(con)
            assert f"endpoint={endpoint}" in secret
            assert "url_style=path" in secret
            assert "use_ssl=false" in secret
            _read_s3(con)
        finally:
            con.close()

        assert seen, "the read never reached the custom endpoint"
        assert seen[0]["path"].startswith("/fakebucket/x.parquet")

    @needs_runtime_env_in_aws_sdk
    def test_profile_signs_the_custom_endpoint_read(self, fake_s3_endpoint):
        endpoint, seen = fake_s3_endpoint
        scope = {"profile": "fakep", "s3_endpoint": endpoint, "s3_use_ssl": False}
        with s3_config_scope(scope):
            con = duckdb_utils.get_duckdb_connection(load_spatial=False, load_httpfs=True)
        try:
            _read_s3(con)
        finally:
            con.close()
        assert seen
        assert f"Credential={_FAKE_STATIC_KEY}/" in seen[0]["authorization"]

    def test_explicit_region_beats_the_profile_region(self):
        with s3_config_scope({"profile": "fakep", "s3_region": "us-west-2"}):
            con = duckdb_utils.get_duckdb_connection(load_spatial=False, load_httpfs=True)
        try:
            secret = _secret_string(con)
        finally:
            con.close()
        assert "region=us-west-2" in secret
        assert "eu-west-3" not in secret

    def test_env_region_resolved_by_the_cli_beats_the_profile_region(self, monkeypatch):
        from geoparquet_io.core.remote import resolve_s3_config

        monkeypatch.setenv("AWS_REGION", "ap-southeast-2")
        with s3_config_scope(resolve_s3_config(aws_profile="fakep")):
            con = duckdb_utils.get_duckdb_connection(load_spatial=False, load_httpfs=True)
        try:
            assert "region=ap-southeast-2" in _secret_string(con)
        finally:
            con.close()

    @needs_runtime_env_in_aws_sdk
    def test_profile_region_is_used_when_nothing_else_is_set(self):
        with s3_config_scope({"profile": "fakep"}):
            con = duckdb_utils.get_duckdb_connection(load_spatial=False, load_httpfs=True)
        try:
            assert "region=eu-west-3" in _secret_string(con)
        finally:
            con.close()

    def test_auth_retry_secret_keeps_the_custom_endpoint(self, fake_s3_endpoint):
        """The use_s3_auth path (the 403 retry) builds the same secret as the profile path."""
        endpoint, _seen = fake_s3_endpoint
        with s3_config_scope({"s3_endpoint": endpoint, "s3_use_ssl": False}):
            con = duckdb_utils.get_duckdb_connection(
                load_spatial=False, load_httpfs=True, use_s3_auth=True
            )
        try:
            assert f"endpoint={endpoint}" in _secret_string(con)
        finally:
            con.close()

    @needs_runtime_env_in_aws_sdk
    def test_explicit_env_keys_still_sign_requests(self, fake_s3_endpoint, monkeypatch):
        endpoint, seen = fake_s3_endpoint
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", _FAKE_ENV_KEY)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fakesecret")
        with s3_config_scope({"s3_endpoint": endpoint, "s3_use_ssl": False}):
            con = duckdb_utils.get_duckdb_connection(
                load_spatial=False, load_httpfs=True, use_s3_auth=True
            )
        try:
            _read_s3(con)
        finally:
            con.close()
        assert seen
        assert f"Credential={_FAKE_ENV_KEY}/" in seen[0]["authorization"]


@pytest.mark.usefixtures("fake_aws_files")
def test_anonymous_read_builds_no_secret_and_sends_no_signature(fake_s3_endpoint):
    endpoint, seen = fake_s3_endpoint
    with s3_config_scope({"s3_endpoint": endpoint, "s3_use_ssl": False}):
        con = duckdb_utils.get_duckdb_connection(load_spatial=False, load_httpfs=True)
    try:
        assert con.execute("SELECT count(*) FROM duckdb_secrets()").fetchone()[0] == 0
        _read_s3(con)
    finally:
        con.close()
    assert seen
    assert seen[0]["authorization"] == ""


class TestProfileOnlyAppliesToS3:
    @pytest.mark.parametrize(
        "path", ["gs://b/k.parquet", "https://example.com/k.parquet", "az://c/k.parquet"]
    )
    def test_non_s3_paths_do_not_scope_the_profile(self, path):
        @uses_aws_profile
        def probe(path, profile=None):
            return get_active_s3_config().get("profile")

        assert probe(path, profile="prod") is None

    def test_an_s3_path_in_a_list_scopes_the_profile(self):
        @uses_aws_profile
        def probe(paths, profile=None):
            return get_active_s3_config().get("profile")

        assert probe(["local.parquet", "s3://b/k.parquet"], profile="prod") == "prod"

    def test_scoped_profile_tolerates_a_missing_aws_extension(self, monkeypatch):
        """A gs/az read under an exported AWS_PROFILE must not fail on INSTALL aws."""
        real = duckdb_utils._install_and_load_extension

        def _no_aws(con, name):
            if name == "aws":
                raise duckdb.IOException("offline")
            return real(con, name)

        monkeypatch.setattr(duckdb_utils, "_install_and_load_extension", _no_aws)
        with s3_config_scope({"profile": "prod"}):
            con = duckdb_utils.get_duckdb_connection(load_spatial=False, load_httpfs=True)
        try:
            assert con.execute("SELECT count(*) FROM duckdb_secrets()").fetchone()[0] == 0
        finally:
            con.close()

    def test_use_s3_auth_still_fails_without_the_aws_extension(self, monkeypatch):
        def _no_aws(con, name):
            if name == "aws":
                raise duckdb.IOException("offline")

        monkeypatch.setattr(duckdb_utils, "_install_and_load_extension", _no_aws)
        with pytest.raises(duckdb.IOException, match="offline"):
            duckdb_utils.get_duckdb_connection(
                load_spatial=False, load_httpfs=True, use_s3_auth=True
            )


@pytest.mark.parametrize("dataset_name", ["current", "gaul", "overture"])
def test_public_admin_datasets_read_anonymously(dataset_name):
    """A scoped profile must not sign reads of the public admin datasets."""
    from geoparquet_io.core.admin_datasets import AdminDatasetFactory

    dataset = AdminDatasetFactory.create(dataset_name)
    with s3_config_scope({"profile": "prod"}):
        with s3_config_scope(dataset.get_s3_config()):
            assert not get_active_s3_config().get("profile")


@needs_aws_extension
@needs_runtime_env_in_aws_sdk
@pytest.mark.usefixtures("fake_aws_files")
class TestExportedProfileFollowsSdkPrecedence:
    """Env keys beat an exported ``AWS_PROFILE``. Only an explicit profile beats env keys.

    That is the AWS CLI and SDK order. ``resolve_s3_config`` folds an exported
    ``AWS_PROFILE`` into the scope, so the secret has to tell it apart from
    ``--aws-profile`` or the ``profile=`` argument.
    """

    @staticmethod
    def _signer(endpoint, seen, *, aws_profile=None):
        from geoparquet_io.core.remote import resolve_s3_config

        config = resolve_s3_config(s3_endpoint=f"http://{endpoint}", aws_profile=aws_profile)
        with s3_config_scope(config):
            con = duckdb_utils.get_duckdb_connection(
                load_spatial=False, load_httpfs=True, use_s3_auth=True
            )
        try:
            _read_s3(con)
        finally:
            con.close()
        assert seen, "the read never reached the fake endpoint"
        return seen[0]["authorization"]

    @staticmethod
    def _env_keys(monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", _FAKE_ENV_KEY)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fakesecret")

    def test_env_keys_beat_an_exported_profile(self, fake_s3_endpoint, monkeypatch):
        endpoint, seen = fake_s3_endpoint
        self._env_keys(monkeypatch)
        monkeypatch.setenv("AWS_PROFILE", "fakep")
        assert f"Credential={_FAKE_ENV_KEY}/" in self._signer(endpoint, seen)

    def test_explicit_profile_beats_env_keys(self, fake_s3_endpoint, monkeypatch):
        endpoint, seen = fake_s3_endpoint
        self._env_keys(monkeypatch)
        auth = self._signer(endpoint, seen, aws_profile="fakep")
        assert f"Credential={_FAKE_STATIC_KEY}/" in auth

    def test_explicit_profile_beats_env_keys_and_an_exported_profile(
        self, fake_s3_endpoint, monkeypatch
    ):
        endpoint, seen = fake_s3_endpoint
        self._env_keys(monkeypatch)
        monkeypatch.setenv("AWS_PROFILE", "missing")
        auth = self._signer(endpoint, seen, aws_profile="fakep")
        assert f"Credential={_FAKE_STATIC_KEY}/" in auth

    def test_exported_profile_alone_still_signs(self, fake_s3_endpoint, monkeypatch):
        endpoint, seen = fake_s3_endpoint
        monkeypatch.setenv("AWS_PROFILE", "fakep")
        assert f"Credential={_FAKE_STATIC_KEY}/" in self._signer(endpoint, seen)

    def test_exported_profile_alone_builds_no_secret_like_main(self, monkeypatch):
        """Without use_s3_auth an exported AWS_PROFILE is left to the SDK, as on main."""
        from geoparquet_io.core.remote import resolve_s3_config

        monkeypatch.setenv("AWS_PROFILE", "fakep")
        with s3_config_scope(resolve_s3_config()):
            con = duckdb_utils.get_duckdb_connection(load_spatial=False, load_httpfs=True)
        try:
            assert con.execute("SELECT count(*) FROM duckdb_secrets()").fetchone()[0] == 0
        finally:
            con.close()


def test_resolve_s3_config_marks_an_exported_profile(monkeypatch):
    from geoparquet_io.core.remote import resolve_s3_config

    monkeypatch.setenv("AWS_PROFILE", "outer")
    assert resolve_s3_config()["profile_from_env"] is True
    assert resolve_s3_config(aws_profile="p")["profile_from_env"] is False


def test_decorator_profile_counts_as_explicit_inside_an_exported_profile_scope(monkeypatch):
    """``profile=`` on a decorated call is explicit even inside a scope built from env."""
    from geoparquet_io.core.remote import resolve_s3_config

    monkeypatch.setenv("AWS_PROFILE", "outer")

    @uses_aws_profile
    def probe(path, profile=None):
        return duckdb_utils._resolve_s3_settings(None, None, None)["profile"]

    with s3_config_scope(resolve_s3_config()):
        assert duckdb_utils._resolve_s3_settings(None, None, None)["profile"] is None
        assert probe("s3://b/k.parquet", profile="inner") == "inner"
