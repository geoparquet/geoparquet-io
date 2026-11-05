"""Tests for the botocore-backed AWS credential chain (#865).

Every test here runs offline: botocore is pointed at temporary ``AWS_CONFIG_FILE``
and ``AWS_SHARED_CREDENTIALS_FILE`` files, instance-metadata lookups are disabled,
and the ``credential_process`` case runs a local Python script rather than any
AWS service. SSO and assume-role are not exercised end to end -- both need STS --
but the ``credential_process`` case proves the resolution is botocore's chain and
not a hand-rolled read of ``~/.aws/credentials``.
"""

import sys
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from geoparquet_io.core.aws_credentials import resolve_aws_credentials, resolve_aws_region


@pytest.fixture
def aws_home(isolated_aws):
    """Use the shared ambient-credential isolation for resolution tests."""
    return isolated_aws


def _write_credentials(aws_home: Path, body: str) -> None:
    (aws_home / "credentials").write_text(textwrap.dedent(body), encoding="utf-8")


def _write_config(aws_home: Path, body: str) -> None:
    (aws_home / "config").write_text(textwrap.dedent(body), encoding="utf-8")


def _credential_process_command(aws_home: Path, token: str | None = "process-token") -> str:
    """Write a script that prints what botocore's ``credential_process`` expects."""
    script = aws_home / "print_credentials.py"
    payload = {
        "Version": 1,
        "AccessKeyId": "PROCESS-KEY",
        "SecretAccessKey": "process-secret",
        "Expiration": "2999-01-01T00:00:00Z",
    }
    if token:
        payload["SessionToken"] = token
    script.write_text(
        textwrap.dedent(f"""
            import json

            print(json.dumps({payload!r}))
            """),
        encoding="utf-8",
    )
    # as_posix() keeps the command shlex-splittable on Windows, where botocore
    # splits credential_process before handing it to subprocess.
    return f"{Path(sys.executable).as_posix()} {script.as_posix()}"


class TestResolveAwsCredentials:
    """The chain botocore walks, exercised source by source."""

    def test_no_credentials_anywhere_returns_none(self, aws_home):
        """Nothing configured means None, so obstore keeps its own resolution."""
        assert resolve_aws_credentials() is None

    def test_environment_keys_are_resolved(self, aws_home, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV-KEY")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret")

        assert resolve_aws_credentials() == {
            "access_key_id": "ENV-KEY",
            "secret_access_key": "env-secret",
        }

    def test_session_token_is_passed_through(self, aws_home, monkeypatch):
        """A temporary session's token has to reach S3Store or every request 403s."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV-KEY")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "env-token")

        assert resolve_aws_credentials()["session_token"] == "env-token"

    def test_static_keys_in_the_default_profile(self, aws_home):
        _write_credentials(
            aws_home,
            """
            [default]
            aws_access_key_id = FILE-KEY
            aws_secret_access_key = file-secret
            """,
        )

        assert resolve_aws_credentials() == {
            "access_key_id": "FILE-KEY",
            "secret_access_key": "file-secret",
        }

    def test_shared_credentials_file_session_token(self, aws_home):
        _write_credentials(
            aws_home,
            """
            [default]
            aws_access_key_id = FILE-KEY
            aws_secret_access_key = file-secret
            aws_session_token = file-token
            """,
        )

        assert resolve_aws_credentials()["session_token"] == "file-token"

    def test_explicit_profile_selection(self, aws_home):
        """--aws-profile picks its own profile, not the default one."""
        _write_credentials(
            aws_home,
            """
            [default]
            aws_access_key_id = DEFAULT-KEY
            aws_secret_access_key = default-secret

            [other]
            aws_access_key_id = OTHER-KEY
            aws_secret_access_key = other-secret
            """,
        )

        assert resolve_aws_credentials("other")["access_key_id"] == "OTHER-KEY"
        assert resolve_aws_credentials()["access_key_id"] == "DEFAULT-KEY"

    def test_explicit_profile_wins_over_environment_keys(self, aws_home, monkeypatch):
        """Naming a profile means that profile, exactly as it did before #865."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENV-KEY")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
        _write_credentials(
            aws_home,
            """
            [other]
            aws_access_key_id = OTHER-KEY
            aws_secret_access_key = other-secret
            """,
        )

        assert resolve_aws_credentials("other")["access_key_id"] == "OTHER-KEY"

    def test_credential_process_profile(self, aws_home):
        """The case the old configparser read could never serve.

        ``credential_process`` lives in the config file and is executed by
        botocore; a profile carrying it has no ``aws_access_key_id`` at all, so
        resolving it proves gpio is walking botocore's chain.
        """
        _write_config(
            aws_home,
            f"""
            [profile viaprocess]
            credential_process = {_credential_process_command(aws_home)}
            """,
        )

        assert resolve_aws_credentials("viaprocess") == {
            "access_key_id": "PROCESS-KEY",
            "secret_access_key": "process-secret",
            "session_token": "process-token",
        }

    def test_credential_process_without_a_token(self, aws_home):
        """A process handing back long-lived keys yields no session_token key."""
        _write_config(
            aws_home,
            f"""
            [profile viaprocess]
            credential_process = {_credential_process_command(aws_home, token=None)}
            """,
        )

        assert "session_token" not in resolve_aws_credentials("viaprocess")

    def test_unknown_profile_returns_none_rather_than_raising(self, aws_home):
        """A typo'd --aws-profile has to reach gpio's own hint, not a traceback."""
        assert resolve_aws_credentials("does-not-exist") is None

    def test_blank_credentials_are_treated_as_none(self, aws_home):
        """A provider can hand back an object whose keys are empty; that is not usable."""
        from unittest.mock import MagicMock

        session = MagicMock()
        frozen = session.get_credentials.return_value.get_frozen_credentials.return_value
        frozen.access_key = ""
        frozen.secret_key = ""

        with patch("geoparquet_io.core.aws_credentials.botocore.session.Session") as mock_session:
            mock_session.return_value = session
            assert resolve_aws_credentials() is None

    def test_a_failing_credential_process_returns_none(self, aws_home):
        """A broken credential_process is 'no credentials', not a crash."""
        _write_config(
            aws_home,
            f"""
            [profile broken]
            credential_process = {Path(sys.executable).as_posix()} -c "raise SystemExit(1)"
            """,
        )

        assert resolve_aws_credentials("broken") is None


class TestResolveAwsRegion:
    """Region comes off the same session, so a profile's region is honoured."""

    def test_region_from_the_profile_config(self, aws_home):
        _write_config(
            aws_home,
            """
            [profile regional]
            region = ap-southeast-2
            """,
        )

        assert resolve_aws_region("regional") == "ap-southeast-2"

    def test_region_from_the_default_profile(self, aws_home):
        _write_config(
            aws_home,
            """
            [default]
            region = eu-north-1
            """,
        )

        assert resolve_aws_region() == "eu-north-1"

    def test_no_region_configured(self, aws_home):
        assert resolve_aws_region() is None

    def test_unknown_profile_returns_none(self, aws_home):
        assert resolve_aws_region("does-not-exist") is None


class TestSetupStorePassesTheChainToS3Store:
    """The upload path and the copy path both build their store from the chain."""

    def _s3store_kwargs(self, mock_s3store):
        return mock_s3store.call_args.kwargs

    def test_upload_path_hands_frozen_credentials_to_s3store(self, aws_home):
        from geoparquet_io.core.upload import _setup_store_and_kwargs

        _write_config(
            aws_home,
            f"""
            [profile viaprocess]
            region = us-west-1
            credential_process = {_credential_process_command(aws_home)}
            """,
        )

        with patch("geoparquet_io.core.upload.S3Store") as mock_s3store:
            _setup_store_and_kwargs(
                bucket_url="s3://my-bucket",
                profile="viaprocess",
                chunk_concurrency=12,
                chunk_size=None,
            )

        kwargs = self._s3store_kwargs(mock_s3store)
        assert mock_s3store.call_args.args[0] == "my-bucket"
        assert kwargs["access_key_id"] == "PROCESS-KEY"
        assert kwargs["secret_access_key"] == "process-secret"
        assert kwargs["session_token"] == "process-token"
        assert kwargs["region"] == "us-west-1"

    def test_explicit_region_flag_still_wins_over_the_profile(self, aws_home):
        from geoparquet_io.core.upload import _setup_store_and_kwargs

        _write_config(
            aws_home,
            """
            [profile regional]
            region = ap-southeast-2
            """,
        )
        _write_credentials(
            aws_home,
            """
            [regional]
            aws_access_key_id = FILE-KEY
            aws_secret_access_key = file-secret
            """,
        )

        with patch("geoparquet_io.core.upload.S3Store") as mock_s3store:
            _setup_store_and_kwargs(
                bucket_url="s3://my-bucket",
                profile="regional",
                chunk_concurrency=12,
                chunk_size=None,
                s3_region="eu-west-1",
            )

        assert self._s3store_kwargs(mock_s3store)["region"] == "eu-west-1"

    def test_no_credentials_leaves_the_store_to_obstore(self, aws_home):
        """With nothing resolved, no credential kwargs are invented."""
        from geoparquet_io.core.upload import _setup_store_and_kwargs

        with patch("geoparquet_io.core.upload.S3Store") as mock_s3store:
            _setup_store_and_kwargs(
                bucket_url="s3://my-bucket",
                profile=None,
                chunk_concurrency=12,
                chunk_size=None,
            )

        kwargs = self._s3store_kwargs(mock_s3store)
        assert "access_key_id" not in kwargs
        assert "secret_access_key" not in kwargs
        assert "session_token" not in kwargs

    def test_copy_path_hands_the_same_credentials_to_s3store(self, aws_home):
        """The #849 copy shares the one choke point, so SSO-style creds reach it too."""
        from geoparquet_io.core.duckdb_utils import s3_config_scope
        from geoparquet_io.core.file_utils import resolve_object_store

        _write_config(
            aws_home,
            f"""
            [profile viaprocess]
            credential_process = {_credential_process_command(aws_home)}
            """,
        )

        with (
            patch("geoparquet_io.core.upload.S3Store") as mock_s3store,
            s3_config_scope({"profile": "viaprocess", "s3_region": "us-east-2"}),
        ):
            store, key = resolve_object_store("s3://bucket/path/to/file.parquet")

        assert key == "path/to/file.parquet"
        assert store is mock_s3store.return_value
        kwargs = self._s3store_kwargs(mock_s3store)
        assert kwargs["access_key_id"] == "PROCESS-KEY"
        assert kwargs["secret_access_key"] == "process-secret"
        assert kwargs["session_token"] == "process-token"


class TestCredentialCheckUsesTheChain:
    """``gpio publish upload`` gates on credentials before it starts; the gate moved too."""

    def test_credential_process_profile_passes_the_check(self, aws_home):
        from geoparquet_io.core.upload import check_credentials

        _write_config(
            aws_home,
            f"""
            [profile viaprocess]
            credential_process = {_credential_process_command(aws_home)}
            """,
        )

        ok, hint = check_credentials("s3://bucket/path", "viaprocess")
        assert ok is True
        assert hint == ""

    def test_no_credentials_still_fails_with_a_hint(self, aws_home):
        from geoparquet_io.core.upload import check_credentials

        ok, hint = check_credentials("s3://bucket/path")
        assert ok is False
        assert "S3 credentials not found" in hint
        assert "aws sso login" in hint
