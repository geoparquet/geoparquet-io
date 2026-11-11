"""The check-options object (#1436): one value says what a check looks at.

The third per-concern options object, after ``WriteOptions`` (#761, the write
concern) and ``RemoteOptions`` (remote access). The check concern is spread
across three doors that are meant to be one surface -- the nine ``gpio check``
commands, ``gpio.check_*`` and ``Table.fix`` -- and each spelled the size band,
the sample bounds and the repair selection for itself.

The guarantee is construction-time, as it is for the other two: a command that
declares a check flag receives one ``CheckOptions`` and never sees the flag, so
it cannot accept one and forget it in its body.
"""

from __future__ import annotations

import dataclasses
import inspect
import warnings
from contextlib import contextmanager

import click
import pytest

import geoparquet_io as gpio
from geoparquet_io.api import check as check_api
from geoparquet_io.cli import decorators
from geoparquet_io.cli.main import cli
from geoparquet_io.core.check_bbox_values import DEFAULT_BBOX_SAMPLE_ROWS
from geoparquet_io.core.check_options import (
    DEFAULT_SPATIAL_LIMIT_ROWS,
    DEFAULT_SPATIAL_SAMPLE_PAIRS,
    DEFAULT_SPEC_SAMPLE_ROWS,
    FIXABLE_CHECKS,
    CheckOptions,
    check_options_from_keywords,
)
from geoparquet_io.core.check_parquet_structure import CheckProfile
from geoparquet_io.core.remote import RemoteOptions
from tests.conftest import walk_cli_commands

FIELDS = (
    "profile",
    "sample_rows",
    "all_rows",
    "random_sample_size",
    "limit_rows",
    "spec_sample_size",
    "fix",
    "checks",
    "fix_output",
    "no_backup",
    "overwrite",
    "remote",
)


@contextmanager
def fresh_deprecations():
    """Record warnings with an empty "warned once" ledger, then put it back."""
    from geoparquet_io.core import check_options as module

    saved = set(module._warned_spellings)
    module._warned_spellings.clear()
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            yield caught
    finally:
        module._warned_spellings.clear()
        module._warned_spellings.update(saved)


def deprecations(caught):
    """The ``DeprecationWarning`` messages among *caught*, sorted."""
    return sorted(str(w.message) for w in caught if w.category is DeprecationWarning)


def _check_leaves():
    return {" ".join(path): cmd for path, cmd in walk_cli_commands(cli) if path[0] == "check"}


# ---------------------------------------------------------------------------
# The value itself
# ---------------------------------------------------------------------------


def test_the_fields_are_the_shared_check_facts():
    """The band, the sample bounds, the repair selection, and remote access."""
    assert tuple(f.name for f in dataclasses.fields(CheckOptions)) == FIELDS


def test_every_field_defaults_to_not_asked():
    unset = CheckOptions()
    assert all(getattr(unset, name) is None for name in FIELDS if name != "remote")
    assert not unset.remote
    assert not unset
    assert unset.asked() == frozenset()


def test_it_is_frozen_and_keyword_only():
    with pytest.raises(dataclasses.FrozenInstanceError):
        CheckOptions().fix = True  # type: ignore[misc]
    with pytest.raises(TypeError):
        CheckOptions(True)  # type: ignore[misc]


def test_it_does_not_restate_the_remote_concern():
    """Remote access is one object; the check concern holds it, it does not copy it."""
    names = {f.name for f in dataclasses.fields(CheckOptions)}
    assert names & {"s3_endpoint", "s3_region", "s3_use_ssl", "aws_profile"} == set()
    assert CheckOptions(remote=RemoteOptions(profile="prod")).remote.profile == "prod"


def test_profile_here_is_the_use_case_band_not_a_credential():
    assert CheckOptions(profile=CheckProfile.web).profile is CheckProfile.web
    assert CheckOptions(profile=CheckProfile.web).remote.profile is None


def test_it_does_not_carry_the_cli_only_facts():
    """``--verbose``/``--strict`` are output policy and ``--all-files`` selects files.

    Both are recorded as one-sided CLI classes in the parity registry, which is
    the statement that they are CLI-only on purpose.
    """
    names = {f.name for f in dataclasses.fields(CheckOptions)}
    assert names & {"verbose", "strict", "check_all_files", "check_sample"} == set()


def test_checks_given_as_a_string_or_a_list_becomes_a_tuple():
    assert CheckOptions(checks="bbox").checks == ("bbox",)
    assert CheckOptions(checks=["bbox", "spatial"]).checks == ("bbox", "spatial")


def test_pick_reads_the_fields_by_name_and_ignores_the_rest():
    values = {"fix": True, "limit_rows": 10, "verbose": True, "parquet_file": "in.parquet"}
    assert CheckOptions.pick(values) == CheckOptions(fix=True, limit_rows=10)


def test_merge_lets_an_asked_field_beat_the_base():
    base = CheckOptions(limit_rows=10, fix=True)
    assert base.merge(CheckOptions(limit_rows=20)) == CheckOptions(limit_rows=20, fix=True)
    assert base.merge(None) == base
    assert base.merge(CheckOptions()) == base


# ---------------------------------------------------------------------------
# One owner per default
# ---------------------------------------------------------------------------


def test_an_unset_bound_resolves_to_this_modules_default():
    unset = CheckOptions()
    assert unset.resolved_random_sample_size() == DEFAULT_SPATIAL_SAMPLE_PAIRS
    assert unset.resolved_limit_rows() == DEFAULT_SPATIAL_LIMIT_ROWS
    assert unset.resolved_spec_sample_size() == DEFAULT_SPEC_SAMPLE_ROWS
    assert unset.resolved_sample_rows() == DEFAULT_BBOX_SAMPLE_ROWS
    assert unset.resolved_checks() == FIXABLE_CHECKS


def test_an_asked_bound_is_kept_including_zero():
    """``--sample-rows 0`` skips the bbox comparison, so 0 is a request, not "unset"."""
    assert CheckOptions(sample_rows=0).resolved_sample_rows() == 0
    assert CheckOptions(limit_rows=7).resolved_limit_rows() == 7
    assert CheckOptions(checks=("bbox",)).resolved_checks() == ("bbox",)


@pytest.mark.parametrize(
    ("leaf", "flag", "default"),
    [
        ("check all", "random_sample_size", DEFAULT_SPATIAL_SAMPLE_PAIRS),
        ("check all", "limit_rows", DEFAULT_SPATIAL_LIMIT_ROWS),
        ("check spatial", "random_sample_size", DEFAULT_SPATIAL_SAMPLE_PAIRS),
        ("check spatial", "limit_rows", DEFAULT_SPATIAL_LIMIT_ROWS),
        ("check bbox", "sample_rows", DEFAULT_BBOX_SAMPLE_ROWS),
        ("check spec", "sample_size", DEFAULT_SPEC_SAMPLE_ROWS),
    ],
)
def test_the_click_default_is_the_owners_number(leaf, flag, default):
    """The flag's default and the resolved default are one constant, not two copies."""
    param = next(p for p in _check_leaves()[leaf].params if p.name == flag)
    assert param.default == default


# ---------------------------------------------------------------------------
# The CLI builds one per command (the construction-time guarantee)
# ---------------------------------------------------------------------------


def test_from_cli_maps_the_spec_sample_spelling():
    """``check spec --sample-size`` is the spec row count, not the spatial pair count."""
    assert CheckOptions.from_cli(sample_size=7) == CheckOptions(spec_sample_size=7)


def test_from_cli_never_reads_the_cli_only_flags():
    built = CheckOptions.from_cli(
        verbose=True, strict=True, check_all_files=True, check_sample=3, fix=True
    )
    assert built == CheckOptions(fix=True)


def test_the_packer_hands_over_one_check_and_keeps_the_rest():
    @decorators.pack_check_options
    def command(**kwargs):
        return kwargs

    got = command(fix=True, limit_rows=5, parquet_file="in.parquet", strict=True)
    assert got == {
        "check": CheckOptions(fix=True, limit_rows=5),
        "parquet_file": "in.parquet",
        "strict": True,
    }


def test_the_packer_takes_every_check_flag():
    """A flag the packer does not know reaches the body under its raw name."""
    declared: set[str] = set()
    for decorate in (
        decorators.check_fix_options(help_text="x"),
        decorators.check_profile_option,
        decorators.spatial_sample_options,
    ):
        declared |= {p.name for p in click.command()(decorate(lambda **_: None)).params}
    assert declared <= set(decorators.CHECK_OPTION_PARAMS)


def test_no_check_leaf_takes_a_raw_check_flag_by_name():
    """Every check leaf that declares a check flag receives ``check`` instead."""
    offenders = []
    for leaf, command in _check_leaves().items():
        names = {p.name for p in command.params}
        if not names & set(decorators.CHECK_OPTION_PARAMS):
            continue
        parameters = set(inspect.signature(command.callback).parameters)
        if "check" not in parameters or parameters & set(decorators.CHECK_OPTION_PARAMS):
            offenders.append(leaf)
    assert offenders == []


def test_most_check_leaves_declare_a_check_flag():
    """Guards the test above against passing because nothing declares one."""
    with_flags = [
        leaf
        for leaf, command in _check_leaves().items()
        if {p.name for p in command.params} & set(decorators.CHECK_OPTION_PARAMS)
    ]
    assert len(with_flags) >= 6


def test_the_fix_flag_set_is_one_definition_shared():
    """Five commands carried their own copy; only ``--fix``'s own help differs."""
    leaves = _check_leaves()
    carriers = [
        leaf
        for leaf in ("check all", "check spatial", "check compression", "check bbox")
        if {"fix", "fix_output", "no_backup", "overwrite"} <= {p.name for p in leaves[leaf].params}
    ]
    assert len(carriers) == 4
    helps = {
        next(p for p in leaves[leaf].params if p.name == "fix_output").help for leaf in carriers
    }
    assert len(helps) == 1


def test_a_commands_check_flags_reach_the_runner(monkeypatch, tmp_path, buildings_test_file):
    """``gpio check bbox`` hands ``begin_check_run`` one ``CheckOptions``, not six keywords."""
    from click.testing import CliRunner

    from geoparquet_io.cli.commands import check as check_cmd

    seen: dict = {}
    real = check_cmd.begin_check_run

    def capture(parquet_file, check, **kwargs):
        seen["check"] = check
        return real(parquet_file, check, **kwargs)

    monkeypatch.setattr(check_cmd, "begin_check_run", capture)
    out = tmp_path / "fixed.parquet"
    result = CliRunner().invoke(
        cli,
        [
            "check",
            "bbox",
            str(buildings_test_file),
            "--fix",
            "--fix-output",
            str(out),
            "--no-backup",
            "--sample-rows",
            "7",
        ],
    )
    assert result.exit_code in (0, 1), result.output
    assert seen["check"] == CheckOptions(
        fix=True, fix_output=str(out), no_backup=True, sample_rows=7
    )


# ---------------------------------------------------------------------------
# The API takes the object; the old keywords keep working, with a warning
# ---------------------------------------------------------------------------


def test_check_options_is_a_root_export():
    assert gpio.CheckOptions is CheckOptions
    assert "CheckOptions" in gpio.__all__


@pytest.mark.parametrize(
    "entry",
    [
        check_api.check_all,
        check_api.check_row_groups,
        check_api.check_bbox,
        check_api.check_spatial,
        check_api.validate,
    ],
)
def test_every_migrated_check_takes_the_object(entry):
    assert "options" in inspect.signature(entry).parameters


def test_table_fix_takes_the_object():
    from geoparquet_io.api.table import Table

    assert "options" in inspect.signature(Table.fix).parameters


def test_the_declared_defaults_are_unchanged_by_the_migration():
    """The deprecated keywords keep their numbers, so CLI/API default parity holds."""
    params = inspect.signature(check_api.check_all).parameters
    assert params["sample_size"].default == DEFAULT_SPATIAL_SAMPLE_PAIRS
    assert params["limit_rows"].default == DEFAULT_SPATIAL_LIMIT_ROWS
    assert params["options"].default is None
    assert inspect.signature(check_api.validate).parameters["sample_size"].default == (
        DEFAULT_SPEC_SAMPLE_ROWS
    )


def test_the_object_reaches_the_check(buildings_test_file, monkeypatch):
    seen: dict = {}
    real = check_api._check_spatial_order

    def capture(path, sample_size=None, limit_rows=None):
        seen.update(sample_size=sample_size, limit_rows=limit_rows)
        return real(path)

    monkeypatch.setattr(check_api, "_check_spatial_order", capture)
    check_api.check_spatial(buildings_test_file, options=CheckOptions(limit_rows=11))
    assert seen == {"sample_size": DEFAULT_SPATIAL_SAMPLE_PAIRS, "limit_rows": 11}


def test_the_deprecated_keyword_still_reaches_the_check_and_warns(buildings_test_file, monkeypatch):
    seen: dict = {}
    real = check_api._check_spatial_order

    def capture(path, sample_size=None, limit_rows=None):
        seen.update(sample_size=sample_size, limit_rows=limit_rows)
        return real(path)

    monkeypatch.setattr(check_api, "_check_spatial_order", capture)
    with fresh_deprecations() as caught:
        check_api.check_spatial(buildings_test_file, limit_rows=11)
        messages = deprecations(caught)
    assert seen["limit_rows"] == 11
    assert messages and messages[0].startswith("check_spatial(limit_rows=...)")
    assert "options=gpio.CheckOptions(limit_rows=...)" in messages[0]


def test_the_warning_names_the_keyword_and_the_field_it_replaces():
    """``check_all(sample_size=)`` sets ``random_sample_size``; both names appear."""
    with fresh_deprecations() as caught:
        check_options_from_keywords("check_all", None, random_sample_size=7)
        messages = deprecations(caught)
    assert messages == [
        "check_all(sample_size=...) is deprecated and will be removed in the next "
        "minor release; pass options=gpio.CheckOptions(random_sample_size=...) instead"
    ]


def test_the_helper_warns_once_per_entry_point_and_field():
    with fresh_deprecations() as caught:
        for _ in range(3):
            built = check_options_from_keywords("thing", None, fix=True, limit_rows=5)
        messages = deprecations(caught)
    assert built == CheckOptions(fix=True, limit_rows=5)
    assert len(messages) == 2


def test_the_new_spelling_alone_never_warns():
    with fresh_deprecations() as caught:
        built = check_options_from_keywords("thing", CheckOptions(fix=True))
        assert deprecations(caught) == []
    assert built == CheckOptions(fix=True)


def test_the_helper_refuses_a_field_set_both_ways():
    with pytest.raises(TypeError, match="one place"):
        check_options_from_keywords("thing", CheckOptions(limit_rows=5), limit_rows=7)


def test_the_collision_names_the_keyword_the_caller_wrote():
    """``validate(sample_size=)`` sets ``spec_sample_size``; the error quotes what was typed."""
    with pytest.raises(TypeError, match=r"validate\(\) got sample_size both"):
        check_options_from_keywords(
            "validate", CheckOptions(spec_sample_size=5), spec_sample_size=7
        )


def test_the_deprecation_points_at_the_caller_not_at_gpios_wrapper(buildings_test_file):
    """Both doors: a path function (wrapped by ``_one_file_only``) and a ``Table`` method.

    A fixed ``stacklevel`` is right for only one of them -- the wrapper adds a
    frame -- so the five path-level checks blamed ``api/check.py`` for the
    caller's keyword.
    """
    with fresh_deprecations() as caught:
        check_api.check_spatial(buildings_test_file, limit_rows=11)
        gpio.read(buildings_test_file).check_spatial(limit_rows=11)
    blamed = {
        w.filename
        for w in caught
        if w.category is DeprecationWarning and "gpio.CheckOptions" in str(w.message)
    }
    assert blamed == {__file__}


def test_check_all_forwards_the_spec_sample_size(buildings_test_file, monkeypatch):
    """``gpio check all`` honours ``--sample-size``'s owner; so must ``gpio.check_all``."""
    from geoparquet_io.core import validate as validate_core

    seen: dict = {}
    real = validate_core.validate_geoparquet

    def capture(path, **kwargs):
        seen.update(kwargs)
        return real(path, **kwargs)

    monkeypatch.setattr(validate_core, "validate_geoparquet", capture)
    check_api.check_all(buildings_test_file, options=CheckOptions(spec_sample_size=5))
    assert seen["sample_size"] == 5


# ---------------------------------------------------------------------------
# CheckOptions.remote: the check is reached the way the options ask
# ---------------------------------------------------------------------------


@contextmanager
def ambient_s3_seen(monkeypatch):
    """Record the ambient S3 config a structure check runs under."""
    from geoparquet_io.core import check_parquet_structure as core
    from geoparquet_io.core.duckdb_utils import get_active_s3_config

    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    seen: dict = {}
    real = core.check_row_groups

    def capture(path, **kwargs):
        seen.update(get_active_s3_config())
        return real(path, **kwargs)

    monkeypatch.setattr(core, "check_row_groups", capture)
    yield seen
    # The scope is the check's, not the process's.
    assert get_active_s3_config().get("profile") is None


REMOTE_ASK = CheckOptions(remote=RemoteOptions(profile="prod", s3_region="us-west-1"))


def test_a_path_checks_remote_options_are_the_ambient_s3_config(buildings_test_file, monkeypatch):
    """``CheckOptions.remote`` is how the file is reached, not a field that is merely held."""
    with ambient_s3_seen(monkeypatch) as seen:
        check_api.check_row_groups(buildings_test_file, options=REMOTE_ASK)
    assert seen["profile"] == "prod"
    assert seen["s3_region"] == "us-west-1"


def test_a_table_checks_remote_options_are_the_ambient_s3_config(buildings_test_file, monkeypatch):
    """``Table.check_*`` calls ``__wrapped__``, so it activates in its own one place."""
    with ambient_s3_seen(monkeypatch) as seen:
        gpio.read(buildings_test_file).check_row_groups(options=REMOTE_ASK)
    assert seen["profile"] == "prod"
    assert seen["s3_region"] == "us-west-1"


def test_table_fix_reaches_the_file_its_options_ask_for(buildings_test_file, tmp_path, monkeypatch):
    from geoparquet_io.core.duckdb_utils import get_active_s3_config

    monkeypatch.delenv("AWS_PROFILE", raising=False)
    seen: dict = {}
    real = check_api._check_structure

    def capture(path, profile=None):
        seen.update(get_active_s3_config())
        return real(path, profile)

    monkeypatch.setattr(check_api, "_check_structure", capture)
    gpio.read(buildings_test_file).fix(
        options=CheckOptions(
            checks=("spatial",),
            fix_output=str(tmp_path / "fixed.parquet"),
            remote=RemoteOptions(profile="prod"),
        )
    )
    assert seen["profile"] == "prod"


def test_a_keyword_left_at_its_default_is_not_asked_for():
    """So the declared defaults can stay, and default parity with the CLI holds."""
    with fresh_deprecations() as caught:
        built = check_api._asked(DEFAULT_SPATIAL_LIMIT_ROWS, DEFAULT_SPATIAL_LIMIT_ROWS)
        assert deprecations(caught) == []
    assert built is None
    assert check_api._asked(7, DEFAULT_SPATIAL_LIMIT_ROWS) == 7


# ---------------------------------------------------------------------------
# Table.fix: the repair selection is the same value
# ---------------------------------------------------------------------------


def test_table_fix_takes_the_selection_from_the_object(buildings_test_file, tmp_path):
    out = tmp_path / "fixed.parquet"
    fixed = gpio.read(buildings_test_file).fix(
        options=CheckOptions(checks=("spatial",), fix_output=str(out))
    )
    assert out.exists()
    assert fixed.num_rows == gpio.read(buildings_test_file).num_rows


def test_table_fix_keeps_the_deprecated_keywords(buildings_test_file, tmp_path):
    out = tmp_path / "fixed.parquet"
    with fresh_deprecations() as caught:
        gpio.read(buildings_test_file).fix(checks=["spatial"], output=out)
        messages = deprecations(caught)
    assert out.exists()
    assert len(messages) == 2  # checks= and output=
    assert any("Table.fix(output=...)" in m for m in messages)


def test_table_fix_refuses_a_selection_set_both_ways(buildings_test_file, tmp_path):
    with pytest.raises(TypeError, match="one place"):
        gpio.read(buildings_test_file).fix(
            checks=["spatial"], options=CheckOptions(checks=("bbox",))
        )


def test_table_fix_still_refuses_an_unknown_check(buildings_test_file):
    from geoparquet_io.core.exceptions import InvalidParameterError

    with pytest.raises(InvalidParameterError, match="unknown"):
        gpio.read(buildings_test_file).fix(options=CheckOptions(checks=("nope",)))


def test_table_fix_overwrite_travels_in_the_object(buildings_test_file, tmp_path):
    from geoparquet_io.core.exceptions import GeoParquetError

    out = tmp_path / "fixed.parquet"
    out.write_bytes(b"PAR1")
    with pytest.raises(GeoParquetError):
        gpio.read(buildings_test_file).fix(options=CheckOptions(fix_output=str(out)))
    gpio.read(buildings_test_file).fix(options=CheckOptions(fix_output=str(out), overwrite=True))
    assert out.stat().st_size > 4
