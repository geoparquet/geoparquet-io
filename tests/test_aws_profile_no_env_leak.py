"""A ``profile=`` / ``aws_profile=`` argument must not leak into ``os.environ`` (#1055).

The old ``setup_aws_profile_if_needed`` wrote ``AWS_PROFILE`` into the process
environment and never put it back, so one Python API call with a profile changed
the credentials of every later call. The profile now travels in the ambient S3
config (:func:`~geoparquet_io.core.duckdb_utils.s3_config_scope`), which DuckDB's
``credential_chain`` secret and the obstore store both read explicitly.
"""

import os

import pytest

from geoparquet_io.core import duckdb_utils
from geoparquet_io.core.duckdb_utils import (
    _credential_chain_secret_sql,
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
        assert inner == {"profile": "inner", "s3_region": "eu-west-1"}


def test_uses_aws_profile_rejects_function_without_profile_parameter():
    with pytest.raises(TypeError, match="profile"):

        @uses_aws_profile
        def probe(path):
            return path


def test_credential_chain_secret_names_the_profile():
    assert "PROFILE" not in _credential_chain_secret_sql(None)
    sql = _credential_chain_secret_sql("o'brien")
    assert "PROFILE 'o''brien'" in sql
    assert "PROVIDER credential_chain" in sql


def test_connection_secret_uses_ambient_profile(monkeypatch):
    """get_duckdb_connection(use_s3_auth=True) builds the secret from the scoped profile."""
    seen: list = []

    def _fake_sql(profile):
        seen.append(profile)
        return "SELECT 1"

    monkeypatch.setattr(duckdb_utils, "_install_and_load_extension", lambda con, name: None)
    monkeypatch.setattr(duckdb_utils, "_credential_chain_secret_sql", _fake_sql)

    with s3_config_scope({"profile": "prod"}):
        con = duckdb_utils.get_duckdb_connection(
            load_spatial=False, load_httpfs=True, use_s3_auth=True
        )
    con.close()

    assert seen == ["prod"]


@pytest.mark.parametrize(("profile", "expected"), [("prod", ["prod"]), (None, [])])
def test_connection_secret_follows_scoped_profile_without_use_s3_auth(
    monkeypatch, profile, expected
):
    """A scoped profile alone builds the secret. No profile and no use_s3_auth builds none."""
    seen: list = []

    def _fake_sql(scoped_profile):
        seen.append(scoped_profile)
        return "SELECT 1"

    monkeypatch.setattr(duckdb_utils, "_install_and_load_extension", lambda con, name: None)
    monkeypatch.setattr(duckdb_utils, "_credential_chain_secret_sql", _fake_sql)

    with s3_config_scope({"profile": profile}):
        con = duckdb_utils.get_duckdb_connection(load_spatial=False, load_httpfs=True)
    con.close()

    assert seen == expected
