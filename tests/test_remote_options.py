"""The remote-access options object (#1436): one value carries how a store is reached.

``WriteOptions`` (#761) made the write concern one value, built once per CLI
command by the shared decorators, and accepted whole by the Python API. This is
the same pattern for the second concern: the AWS profile, the S3 endpoint, the
region and the SSL flag, which ``gpio.read_partition``, ``gpio.convert``, every
writer and ``gpio publish upload`` all take.

Before this, each command took ``aws_profile`` (and sometimes the ``s3_*``
trio) by name, forwarded them by name into ``_activate_s3``, and read the
resolved values back out of a dict -- four chances for a credential to be
accepted at one door and dropped on the way to the store (#627, #663). The
guarantee here is construction-time: a command that declares a remote flag
receives one ``RemoteOptions`` and passes it on whole.
"""

from __future__ import annotations

import dataclasses
import inspect
import os
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import get_type_hints

import click
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli import decorators
from geoparquet_io.cli.main import cli
from geoparquet_io.core.parquet_writer import WriteOptions
from geoparquet_io.core.remote import RemoteOptions, remote_options_from_keywords
from tests.conftest import walk_cli_commands

FIELDS = ("profile", "s3_endpoint", "s3_region", "s3_use_ssl")


@pytest.fixture
def places(places_test_file):
    return gpio.read(places_test_file)


@contextmanager
def fresh_deprecations():
    """Record warnings with an empty "warned once" ledger, then put it back.

    The warning fires once per process, so a test that asserts on it has to
    start from a clean ledger -- and a test that asserts on its *absence* has
    to, too, or it passes only because an earlier test warned first.
    """
    from geoparquet_io.core import deprecation as remote_module

    saved = set(remote_module._warned)
    remote_module._warned.clear()
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            yield caught
    finally:
        remote_module._warned.clear()
        remote_module._warned.update(saved)


def deprecations(caught):
    """The ``DeprecationWarning`` messages among *caught*, sorted."""
    return sorted(str(w.message) for w in caught if w.category is DeprecationWarning)


# ---------------------------------------------------------------------------
# The value itself
# ---------------------------------------------------------------------------


def test_the_fields_are_the_remote_access_facts():
    assert tuple(f.name for f in dataclasses.fields(RemoteOptions)) == FIELDS


def test_every_field_defaults_to_not_asked():
    """None is "not asked", so an explicit value stays visible to ``merge``."""
    assert all(getattr(RemoteOptions(), name) is None for name in FIELDS)
    assert not RemoteOptions()
    assert RemoteOptions().asked() == frozenset()


def test_it_is_frozen_and_keyword_only():
    with pytest.raises(dataclasses.FrozenInstanceError):
        RemoteOptions().profile = "p"  # type: ignore[misc]
    with pytest.raises(TypeError):
        RemoteOptions("p")  # type: ignore[misc]


def test_asked_names_only_the_fields_that_were_given():
    assert RemoteOptions(profile="p", s3_use_ssl=False).asked() == {"profile", "s3_use_ssl"}
    assert bool(RemoteOptions(s3_region="us-west-2"))


def test_pick_reads_the_fields_by_name_and_ignores_the_rest():
    values = {
        "profile": "p",
        "s3_endpoint": "minio.example.com:9000",
        "s3_region": "us-east-1",
        "s3_use_ssl": False,
        "verbose": True,
        "input_parquet": "in.parquet",
    }
    assert RemoteOptions.pick(values) == RemoteOptions(**{k: values[k] for k in FIELDS})


def test_pick_leaves_an_absent_field_at_its_default():
    assert RemoteOptions.pick({"s3_region": "eu-west-1"}) == RemoteOptions(s3_region="eu-west-1")


# ---------------------------------------------------------------------------
# from_cli: the CLI spellings, and the flags that are not credentials
# ---------------------------------------------------------------------------


def test_from_cli_maps_the_cli_spellings():
    built = RemoteOptions.from_cli(
        aws_profile="prod",
        s3_endpoint="minio.example.com:9000",
        s3_region="us-east-1",
        s3_no_ssl=True,
    )
    assert built == RemoteOptions(
        profile="prod",
        s3_endpoint="minio.example.com:9000",
        s3_region="us-east-1",
        s3_use_ssl=False,
    )


def test_from_cli_reads_an_unset_no_ssl_flag_as_not_asked():
    """``--s3-no-ssl`` absent is ``False``, which must not *ask* for SSL.

    Asking would make a command's unset flag beat the root group's
    ``--s3-no-ssl``, silently putting the endpoint back on HTTPS.
    """
    assert RemoteOptions.from_cli(s3_no_ssl=False) == RemoteOptions()
    assert RemoteOptions.from_cli(s3_no_ssl=False).asked() == frozenset()


def test_from_cli_never_reads_a_profile_that_is_not_a_credential():
    """``check --profile`` is a row-group band and ``benchmark --profile`` is profiling.

    Reading either here would hand the AWS credential chain a profile named
    ``"balanced"``.
    """
    assert RemoteOptions.from_cli(profile="balanced", threads=4) == RemoteOptions()


def test_check_row_group_really_does_spell_profile_that_way():
    """The premise of the test above, read off the command itself."""
    commands = {" ".join(path): command for path, command in walk_cli_commands(cli)}
    names = {p.name for p in commands["check row-group"].params}
    assert "profile" in names
    assert "aws_profile" not in names


# ---------------------------------------------------------------------------
# merge: the root group's flags are the base, a command's own win
# ---------------------------------------------------------------------------


def test_merge_lets_an_asked_field_beat_the_base():
    root = RemoteOptions(profile="root", s3_region="us-east-1")
    command = RemoteOptions(profile="command")
    assert root.merge(command) == RemoteOptions(profile="command", s3_region="us-east-1")


def test_merge_with_nothing_asked_changes_nothing():
    root = RemoteOptions(profile="root", s3_use_ssl=False)
    assert root.merge(RemoteOptions()) == root
    assert root.merge(None) == root


# ---------------------------------------------------------------------------
# resolved / activate: the one bridge to the ambient S3 config
# ---------------------------------------------------------------------------


def test_resolved_falls_back_to_the_aws_environment(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "eu-central-1")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    config = RemoteOptions().resolved()
    assert config["s3_region"] == "eu-central-1"
    assert config["s3_use_ssl"] is True
    assert config["profile"] is None


def test_resolved_strips_a_scheme_and_reads_http_as_no_ssl(monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    config = RemoteOptions(s3_endpoint="http://minio.example.com:9000").resolved()
    assert config["s3_endpoint"] == "minio.example.com:9000"
    assert config["s3_use_ssl"] is False


def test_an_explicit_profile_is_not_an_env_profile(monkeypatch):
    monkeypatch.setenv("AWS_PROFILE", "ambient")
    assert RemoteOptions().resolved()["profile_from_env"] is True
    assert RemoteOptions(profile="explicit").resolved()["profile_from_env"] is False


def test_activate_makes_it_the_ambient_s3_config(monkeypatch):
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    from geoparquet_io.core.duckdb_utils import get_active_s3_config

    assert get_active_s3_config().get("profile") is None
    with RemoteOptions(profile="prod", s3_region="us-west-1").activate() as config:
        assert config["profile"] == "prod"
        assert get_active_s3_config()["profile"] == "prod"
        assert get_active_s3_config()["s3_region"] == "us-west-1"
    assert get_active_s3_config().get("profile") is None


def test_activate_writes_nothing_to_the_environment():
    """The profile travels in the ambient config, never ``os.environ`` (#1055)."""
    before = os.environ.get("AWS_PROFILE")
    with RemoteOptions(profile="prod").activate():
        assert os.environ.get("AWS_PROFILE") == before


# ---------------------------------------------------------------------------
# The CLI builds one RemoteOptions per command (the construction-time guarantee)
# ---------------------------------------------------------------------------


def test_the_packer_hands_over_one_remote_and_keeps_the_rest():
    @decorators.pack_remote_options
    def command(**kwargs):
        return kwargs

    got = command(aws_profile="prod", s3_no_ssl=True, input_file="in.parquet", verbose=True)
    assert got == {
        "remote": RemoteOptions(profile="prod", s3_use_ssl=False),
        "input_file": "in.parquet",
        "verbose": True,
    }


def test_the_packer_takes_every_remote_flag():
    """``REMOTE_OPTION_PARAMS`` must list every flag the decorators declare.

    A flag the packer does not know stays in ``**kwargs`` and reaches the
    command body under its raw name -- the "accepted and then dropped" shape.
    """
    declared: set[str] = set()
    for decorate in (decorators.aws_profile_option, decorators.s3_endpoint_options):
        declared |= {p.name for p in click.command()(decorate(lambda **_: None)).params}
    assert declared == set(decorators.REMOTE_OPTION_PARAMS)


def test_no_leaf_takes_a_raw_remote_flag_by_name():
    """Every command that declares a remote flag receives ``remote`` instead.

    This is the guarantee the object exists for: a credential cannot be
    declared as a flag and then forgotten in the command body, because the body
    never sees the flag.
    """
    offenders = []
    for leaf, command in walk_cli_commands(cli):  # leaf is a path tuple
        names = {p.name for p in command.params}
        if not names & set(decorators.REMOTE_OPTION_PARAMS):
            continue
        parameters = set(inspect.signature(command.callback).parameters)
        if "remote" not in parameters or parameters & set(decorators.REMOTE_OPTION_PARAMS):
            offenders.append(leaf)
    assert offenders == []


def test_at_least_one_leaf_declares_a_remote_flag():
    """Guards the test above against passing because nothing declares one."""
    with_flags = [
        leaf
        for leaf, command in walk_cli_commands(cli)
        if {p.name for p in command.params} & set(decorators.REMOTE_OPTION_PARAMS)
    ]
    assert len(with_flags) >= 12


def test_the_root_flags_and_a_commands_own_are_layered_in_one_place(monkeypatch):
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    from geoparquet_io.cli._shared import activate_remote

    ctx = click.Context(click.Command("standalone"))
    ctx.obj = {"aws_profile": "root", "s3_region": "us-east-1"}
    with activate_remote(ctx, RemoteOptions(profile="command")) as config:
        assert config["profile"] == "command"
        assert config["s3_region"] == "us-east-1"


def test_a_commands_remote_flags_reach_the_store(monkeypatch, tmp_path):
    """``gpio publish upload`` hands ``upload`` one ``RemoteOptions``, not four keywords."""
    from geoparquet_io.cli.commands import publish

    source = tmp_path / "data.parquet"
    source.write_bytes(b"PAR1")
    seen: dict = {}

    def fake_upload(**kwargs):
        seen.update(kwargs)

    monkeypatch.setattr(publish, "upload_impl", fake_upload)
    monkeypatch.setattr(publish, "check_credentials", lambda *_a, **_k: (True, ""))
    monkeypatch.delenv("AWS_PROFILE", raising=False)

    result = CliRunner().invoke(
        cli,
        [
            "publish",
            "upload",
            str(source),
            "s3://bucket/data.parquet",
            "--aws-profile",
            "prod",
            "--s3-endpoint",
            "minio.example.com:9000",
            "--s3-region",
            "us-west-2",
            "--s3-no-ssl",
        ],
    )
    assert result.exit_code == 0, result.output
    assert seen["remote"] == RemoteOptions(
        profile="prod",
        s3_endpoint="minio.example.com:9000",
        s3_region="us-west-2",
        s3_use_ssl=False,
    )


# ---------------------------------------------------------------------------
# profile leaves WriteOptions, behind a deprecation (#1436, decision 2)
# ---------------------------------------------------------------------------


def test_write_options_carries_a_remote_and_defaults_it_to_nothing_asked():
    assert WriteOptions().remote == RemoteOptions()
    assert not WriteOptions().remote


def test_the_new_spelling_mirrors_into_the_deprecated_field():
    options = WriteOptions(remote=RemoteOptions(profile="prod"))
    assert options.remote.profile == "prod"
    assert options.profile == "prod"  # the ~35 call sites that read it keep working
    assert options.funnel_kwargs()["profile"] == "prod"


def test_the_deprecated_field_forwards_into_the_remote_object_and_warns_once():
    with fresh_deprecations() as caught:
        first = WriteOptions(profile="prod")
        second = WriteOptions(profile="other")
        messages = deprecations(caught)
    assert first.remote == RemoteOptions(profile="prod")
    assert second.remote == RemoteOptions(profile="other")
    assert len(messages) == 1
    assert "next minor release" in messages[0]
    assert "remote=gpio.RemoteOptions(profile=...)" in messages[0]


def test_replacing_a_reconciled_value_does_not_warn_again():
    """``dataclasses.replace`` passes both sides, so it is not the old spelling."""
    base = WriteOptions(remote=RemoteOptions(profile="prod"))
    with fresh_deprecations() as caught:
        later = dataclasses.replace(base, compression="GZIP")
        assert deprecations(caught) == []
    assert later.profile == "prod"
    assert later.remote.profile == "prod"


def test_two_different_profiles_are_refused():
    with pytest.raises(TypeError, match="one place"):
        WriteOptions(profile="a", remote=RemoteOptions(profile="b"))


def test_the_remote_concern_does_not_shape_the_output():
    """``sets_output_layout`` decides whether ``add bbox`` may copy its input through."""
    assert not WriteOptions(remote=RemoteOptions(profile="prod")).sets_output_layout()


def test_a_remote_option_is_not_a_geoparquet_only_option(places, tmp_path):
    """A credential applies to every output format, so ``.csv`` must not refuse it."""
    places.write(tmp_path / "out.csv", remote=RemoteOptions(profile="prod"))
    assert (tmp_path / "out.csv").exists()


# ---------------------------------------------------------------------------
# The API takes the object; the old keywords keep working, with a warning
# ---------------------------------------------------------------------------


def test_remote_options_is_a_root_export():
    assert gpio.RemoteOptions is RemoteOptions
    assert "RemoteOptions" in gpio.__all__


@pytest.mark.parametrize(
    ("entry", "keyword"),
    [
        (gpio.read_partition, "aws_profile"),
        (gpio.convert, "profile"),
    ],
)
def test_every_migrated_entry_point_takes_the_object_and_the_old_keyword(entry, keyword):
    parameters = inspect.signature(entry).parameters
    assert "remote" in parameters
    assert keyword in parameters


def test_table_write_and_upload_take_the_object():
    from geoparquet_io.api.table import Table

    assert "remote" in inspect.signature(Table.write).parameters
    assert "remote" in inspect.signature(Table.upload).parameters


def test_table_write_folds_the_deprecated_profile_into_the_object(places, tmp_path):
    seen: dict = {}

    def capture(_self, path, write, **kwargs):
        seen["write"] = write
        return path

    from geoparquet_io.api.table import Table

    original = Table._write_geoparquet
    try:
        Table._write_geoparquet = capture  # type: ignore[method-assign]
        with fresh_deprecations() as caught:
            places.write(tmp_path / "out.parquet", profile="prod")
            messages = deprecations(caught)
    finally:
        Table._write_geoparquet = original  # type: ignore[method-assign]

    assert seen["write"].remote == RemoteOptions(profile="prod")
    assert messages and messages[0].startswith("Table.write(profile=...)")


def test_naming_the_profile_twice_is_refused(places, tmp_path):
    with pytest.raises(TypeError, match="one place"):
        places.write(
            tmp_path / "out.parquet",
            remote=RemoteOptions(profile="a"),
            profile="b",
        )


def test_the_helper_warns_once_per_entry_point_and_keyword():
    with fresh_deprecations() as caught:
        for _ in range(3):
            built = remote_options_from_keywords("thing", None, profile="p", s3_region="r")
        messages = deprecations(caught)
    assert built == RemoteOptions(profile="p", s3_region="r")
    assert len(messages) == 2
    assert "thing(profile=...)" in messages[0]
    assert "thing(s3_region=...)" in messages[1]


def test_the_helper_reads_the_old_ssl_default_as_not_asked():
    """``s3_use_ssl=True`` is the old signature's default, not a request."""
    with fresh_deprecations() as caught:
        built = remote_options_from_keywords("thing", None, s3_use_ssl=True)
        assert deprecations(caught) == []
    assert built == RemoteOptions()


def test_the_helper_keeps_an_explicit_no_ssl():
    with fresh_deprecations():
        built = remote_options_from_keywords("thing", None, s3_use_ssl=False)
    assert built == RemoteOptions(s3_use_ssl=False)


def test_the_helper_refuses_a_field_set_both_ways():
    with pytest.raises(TypeError, match="one place"):
        remote_options_from_keywords("thing", RemoteOptions(profile="a"), aws_profile="b")


def test_the_new_spelling_alone_never_warns():
    with fresh_deprecations() as caught:
        built = remote_options_from_keywords("thing", RemoteOptions(profile="p"))
        assert deprecations(caught) == []
    assert built == RemoteOptions(profile="p")


# ---------------------------------------------------------------------------
# The whole object reaches the store, not just its profile
# ---------------------------------------------------------------------------


#: An ask no single keyword can carry: every field set, and an endpoint that is
#: not AWS. Dropping any of it sends the object to the wrong store.
MINIO = RemoteOptions(
    profile="prod",
    s3_endpoint="minio.example.com:9000",
    s3_region="us-west-2",
    s3_use_ssl=False,
)


@contextmanager
def upload_seen(monkeypatch):
    """Record what a remote ``Table.write`` hands :func:`core.upload.upload`."""
    from geoparquet_io.core import upload as upload_module

    seen: dict = {}

    def fake_upload(source=None, destination=None, **kwargs):
        seen["source"] = source
        seen["destination"] = destination
        seen.update(kwargs)

    monkeypatch.setattr(upload_module, "upload", fake_upload)
    yield seen


def test_a_remote_geoparquet_write_hands_the_store_every_remote_option(places, monkeypatch):
    """The endpoint, region and SSL flag decide which store is written to, not just who."""
    with upload_seen(monkeypatch) as seen:
        places.write("s3://bucket/out.parquet", remote=MINIO)
    assert seen["remote"] == MINIO


def test_a_remote_gdal_write_hands_the_store_every_remote_option(places, monkeypatch):
    """The other remote destination: a format GDAL writes, staged then uploaded."""
    from geoparquet_io.core import format_writers

    def fake_writer(_source, output, **_kwargs):
        Path(output).write_bytes(b"FGB")

    monkeypatch.setattr(format_writers, "write_flatgeobuf", fake_writer)
    with upload_seen(monkeypatch) as seen:
        places.write("s3://bucket/out.fgb", remote=MINIO)
    assert seen["remote"] == MINIO


def test_the_remote_field_admits_the_none_a_caller_forwards():
    """``__post_init__`` normalizes it, so the annotation must not forbid it."""
    assert WriteOptions(remote=None).remote == RemoteOptions()
    assert get_type_hints(WriteOptions)["remote"] == RemoteOptions | None
    assert WriteOptions(remote=None).remote_access() == RemoteOptions()


# ---------------------------------------------------------------------------
# A deprecated keyword is named, and blamed, the way the caller wrote it
# ---------------------------------------------------------------------------


def test_the_deprecation_points_at_the_caller_not_at_the_generated_init(places, tmp_path):
    """Both doors: ``WriteOptions(profile=)`` is one frame deeper than ``Table.write``.

    ``WriteOptions(profile='p')`` reaches the warning through
    ``__init__ -> __post_init__ -> remote_options_from_keywords``, while
    ``Table.write(profile='p')`` calls the helper itself. A fixed
    ``stacklevel`` is right for only one of them, and the dataclass path blamed
    the generated ``__init__`` for the caller's keyword.
    """
    with fresh_deprecations() as caught:
        WriteOptions(profile="prod")
        places.write(tmp_path / "out.parquet", profile="prod")
        blamed = {w.filename for w in caught if w.category is DeprecationWarning}
    assert blamed == {__file__}


def test_the_table_write_collision_names_the_keyword_the_caller_wrote(places, tmp_path):
    """``Table.write(profile=)`` sets ``remote``; the error quotes what was typed."""
    with fresh_deprecations():
        with pytest.raises(TypeError, match=r"Table\.write\(\) got profile both"):
            places.write(tmp_path / "out.parquet", WriteOptions(profile="a"), profile="b")


def test_the_helper_collision_names_the_keyword_the_caller_wrote():
    """``read_partition(aws_profile=)`` sets ``profile``; the error quotes what was typed."""
    with pytest.raises(TypeError, match=r"read_partition\(\) got aws_profile both"):
        remote_options_from_keywords("read_partition", RemoteOptions(profile="a"), aws_profile="b")
