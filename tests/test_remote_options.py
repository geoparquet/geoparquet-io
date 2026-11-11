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
    from geoparquet_io.core import remote as remote_module

    saved = set(remote_module._warned_spellings)
    remote_module._warned_spellings.clear()
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            yield caught
    finally:
        remote_module._warned_spellings.clear()
        remote_module._warned_spellings.update(saved)


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
# Every leaf that can reach a store activates the config (#1452)
# ---------------------------------------------------------------------------
#
# Declaring a remote flag, or taking a path that may be a URL, is only half of
# it: the command body has to run inside ``activate_remote``, or neither its own
# ``--aws-profile`` nor the root group's ``--s3-*`` ever becomes the ambient S3
# config its DuckDB read and its object-store write look at. Six leaves were
# wired to the flags and never entered the scope (#1452), so the coverage is
# checked structurally here rather than one command at a time.

_BENCHMARK_IS_LOCAL = (
    "Every input is a local path -- `click.Path(exists=True)` on the leaves that read "
    "a file, an explicit `Path(f).exists()` behind `--files` -- and the results are JSON "
    "written with `Path.write_text`. A URL is refused before any read, so the group "
    "reaches no store."
)

#: The CLI leaves whose call path never enters ``activate_remote``, and why each
#: needs none: the leaf reaches no object store at all. Set equality in
#: :func:`test_every_leaf_that_can_reach_a_store_activates_the_config`, so a leaf
#: that grows a remote path fails here until it activates, and an entry that
#: stops being true fails just as loudly.
NO_STORE_TO_REACH: dict[str, str] = {
    "benchmark compare": _BENCHMARK_IS_LOCAL,
    "benchmark explain": _BENCHMARK_IS_LOCAL,
    "benchmark report": _BENCHMARK_IS_LOCAL,
    "benchmark suite": _BENCHMARK_IS_LOCAL,
    "publish stac": (
        "STAC generation refuses a remote input by design: `generate_stac_item` and "
        "`generate_stac_collection` raise `InvalidParameterError` for a URL, because a "
        "catalog's asset hrefs would then point at files the user may not control. The "
        "JSON it writes is local, and the bucket in `--bucket` is only spelled into an "
        "href, never read."
    ),
    "skills": "Lists the skills bundled with the package; it takes no path at all.",
}


def _cli_functions() -> dict[str, list[tuple[set[str], bool]]]:
    """Per function name in ``geoparquet_io/cli``, its calls and whether it activates.

    One AST pass over the package, so the reachability walk below needs no
    imports and no execution. A name defined twice contributes two entries and
    either may satisfy the walk.
    """
    import ast
    from pathlib import Path as _Path

    out: dict[str, list[tuple[set[str], bool]]] = {}
    root = _Path(__file__).resolve().parent.parent / "geoparquet_io" / "cli"
    for path in sorted(root.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.FunctionDef):
                continue
            calls = {
                call.func.id if isinstance(call.func, ast.Name) else call.func.attr
                for call in ast.walk(node)
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Name | ast.Attribute)
            }
            out.setdefault(node.name, []).append((calls, "activate_remote" in calls))
    return out


def _activates(name: str, functions: dict, seen: frozenset[str] = frozenset()) -> bool:
    """Whether *name*, or anything it calls inside ``cli``, enters ``activate_remote``.

    Transitive, because four ``partition`` leaves delegate their whole body to a
    shared helper that opens the scope.
    """
    if name in seen:
        return False
    return any(
        activates or any(_activates(c, functions, seen | {name}) for c in calls)
        for calls, activates in functions.get(name, [])
    )


def _unwrapped(callback):
    """The command body under the option packers, which ``functools.wraps`` hides."""
    while hasattr(callback, "__wrapped__"):
        callback = callback.__wrapped__
    return callback


def test_every_leaf_that_can_reach_a_store_activates_the_config():
    """Each leaf either opens ``activate_remote`` or is a recorded no-store leaf."""
    functions = _cli_functions()
    silent = {
        " ".join(leaf)
        for leaf, command in walk_cli_commands(cli)
        if not _activates(_unwrapped(command.callback).__name__, functions)
    }
    assert silent == set(NO_STORE_TO_REACH), (
        "Leaves with no remote activation drifted from NO_STORE_TO_REACH.\n"
        f"  New -- wrap the body in `activate_remote(ctx, ...)`, or record why it "
        f"reaches no store: {sorted(silent - set(NO_STORE_TO_REACH))}\n"
        f"  Stale -- now activated; delete the entry: "
        f"{sorted(set(NO_STORE_TO_REACH) - silent)}"
    )


def test_the_reachability_walk_sees_a_delegated_activation():
    """Guards the census above: a leaf that activates only through a helper passes."""
    functions = _cli_functions()
    assert _activates("partition_h3", functions)  # delegates to _partition_cell_index
    assert not _activates("benchmark_report", functions)


#: ``(argv, module, attribute)`` per leaf fixed in #1452: the core call the body
#: makes, which must see the flags as the ambient S3 config. The body imports the
#: name when it runs, so patching the attribute on its module is what the command
#: actually calls.
ACTIVATION_CASES = [
    pytest.param(
        ["add", "admin-divisions", "{src}", "{out}.parquet", "--dataset", "gaul"],
        "geoparquet_io.core.add.admin_divisions",
        "add_admin_divisions_multi",
        id="add-admin-divisions",
    ),
    pytest.param(
        ["add", "geometry-metrics", "{src}", "{out}.parquet"],
        "geoparquet_io.core.add.geometry_metrics",
        "add_geometry_metrics",
        id="add-geometry-metrics",
    ),
    pytest.param(
        ["extract", "bigquery", "proj.dataset.table", "{out}.parquet"],
        "geoparquet_io.core.extract_bigquery",
        "extract_bigquery",
        id="extract-bigquery",
    ),
    pytest.param(
        ["extract", "wfs", "https://wfs.example.com/ows", "roads", "{out}.parquet"],
        "geoparquet_io.core.wfs",
        "convert_wfs_to_geoparquet",
        id="extract-wfs",
    ),
    pytest.param(
        ["pmtiles", "create", "{src}", "{out}.pmtiles"],
        "geoparquet_io.core.pmtiles",
        "create_pmtiles_from_geoparquet",
        id="pmtiles-create",
    ),
    pytest.param(
        ["pmtiles", "pyramid", "{src}", "{out}.pmtiles"],
        "geoparquet_io.core.pmtiles_pyramid",
        "create_pmtiles_pyramid",
        id="pmtiles-pyramid",
    ),
]


@pytest.mark.parametrize(("argv", "module", "attribute"), ACTIVATION_CASES)
def test_the_root_remote_flags_reach_a_fixed_leafs_core_call(
    argv, module, attribute, monkeypatch, tmp_path, places_test_file
):
    """``gpio --aws-profile p --s3-region r <leaf>`` is ambient inside the core call."""
    import importlib

    from geoparquet_io.core.duckdb_utils import get_active_s3_config

    seen: list[dict] = []
    monkeypatch.setattr(
        importlib.import_module(module),
        attribute,
        lambda *_a, **_k: seen.append(get_active_s3_config()),
    )
    rendered = [
        a.format(src=places_test_file, out=str(tmp_path / "out")) if "{" in a else a for a in argv
    ]
    result = CliRunner().invoke(
        cli,
        ["--aws-profile", "root-profile", "--s3-region", "us-west-1", *rendered],
    )
    assert result.exit_code == 0, result.output
    assert len(seen) == 1
    assert seen[0]["profile"] == "root-profile"
    assert seen[0]["s3_region"] == "us-west-1"


def test_pmtiles_create_layers_its_own_profile_over_the_root_one(
    monkeypatch, tmp_path, places_test_file
):
    """The one fixed leaf with a flag of its own hands the merged value to core."""
    from geoparquet_io.core import pmtiles as pmtiles_core
    from geoparquet_io.core.duckdb_utils import get_active_s3_config

    seen: dict = {}

    def record(**kwargs):
        seen.update(kwargs)
        seen["ambient"] = get_active_s3_config()

    monkeypatch.setattr(pmtiles_core, "create_pmtiles_from_geoparquet", record)
    result = CliRunner().invoke(
        cli,
        [
            "--aws-profile",
            "root-profile",
            "--s3-endpoint",
            "minio.example.com:9000",
            "pmtiles",
            "create",
            places_test_file,
            str(tmp_path / "out.pmtiles"),
            "--aws-profile",
            "command-profile",
        ],
    )
    assert result.exit_code == 0, result.output
    assert seen["remote"] == RemoteOptions(
        profile="command-profile", s3_endpoint="minio.example.com:9000"
    )
    assert seen["ambient"]["profile"] == "command-profile"
    assert seen["ambient"]["s3_endpoint"] == "minio.example.com:9000"


# ---------------------------------------------------------------------------
# The child `gpio` processes PMTiles pipes through get the same options (#1452)
# ---------------------------------------------------------------------------


def test_the_pmtiles_child_commands_carry_every_remote_option():
    """``create_pmtiles`` pipes through child ``gpio`` runs; they need the flags too.

    The ambient config lives in this process, so a child reads none of it. The
    options are re-spelled as the root group's own flags, which every child
    accepts whatever leaf it is -- the per-command ``--profile`` the chain used
    to pass is not an option any of them declares (#1452).
    """
    from geoparquet_io.core.pmtiles import _build_gpio_commands

    remote = RemoteOptions(
        profile="prod", s3_endpoint="minio.example.com:9000", s3_region="us-west-2"
    )
    commands = _build_gpio_commands(
        "s3://bucket/in.parquet", "0,0,1,1", None, None, 6, False, remote, None
    )
    assert len(commands) > 1
    for command in commands:
        assert command[1:7] == [
            "--aws-profile",
            "prod",
            "--s3-endpoint",
            "minio.example.com:9000",
            "--s3-region",
            "us-west-2",
        ]


def test_nothing_asked_adds_no_child_flags():
    from geoparquet_io.core.pmtiles import _build_gpio_commands

    commands = _build_gpio_commands("in.parquet", None, None, None, 6, False, RemoteOptions(), None)
    assert [c[1] for c in commands] == ["convert"]


def test_the_child_flags_are_flags_the_root_group_declares():
    """``cli_args`` spells the root group's own options, so a child never rejects one."""
    root = {opt for param in cli.params for opt in param.opts}
    asked = RemoteOptions(profile="p", s3_endpoint="e", s3_region="r", s3_use_ssl=False).cli_args()
    assert set(asked[:-1:2]) <= root
    assert asked[-1] == "--s3-no-ssl"


def test_create_pmtiles_takes_the_object_and_folds_the_old_keyword(monkeypatch):
    """``ops.create_pmtiles`` joins the migrated entry points (#1436, #1452)."""
    from geoparquet_io.api import ops
    from geoparquet_io.core import pmtiles as pmtiles_core

    assert "remote" in inspect.signature(ops.create_pmtiles).parameters

    seen: dict = {}
    monkeypatch.setattr(
        pmtiles_core, "create_pmtiles_from_geoparquet", lambda **kwargs: seen.update(kwargs)
    )
    with fresh_deprecations() as caught:
        ops.create_pmtiles("in.parquet", "out.pmtiles", profile="prod")
    assert "create_pmtiles(profile=...)" in deprecations(caught)[0]
    assert seen["remote"] == RemoteOptions(profile="prod")
