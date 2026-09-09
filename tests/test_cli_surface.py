"""Characterization snapshot of the ``gpio`` CLI surface.

This suite freezes the *structure* of the Click command tree - groups,
commands, parameter names, types, defaults and flags - not the help prose.
Help wording may be reflowed freely; adding, removing or re-typing an option
is a user-visible change and must be acknowledged explicitly.

To accept an intentional change::

    GPIO_UPDATE_SNAPSHOT=1 uv run pytest tests/test_cli_surface.py

then review the diff of ``tests/data/cli_surface.json`` before committing. The
variable is honored only for an affirmative value (``1``/``true``/``yes``/``on``)
and is refused outright when ``CI`` is set.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests.conftest import CLICK_HAS_UNSET
from tests.conftest import UNSET as _UNSET

SNAPSHOT_PATH = Path(__file__).parent / "data" / "cli_surface.json"
UPDATE_ENV_VAR = "GPIO_UPDATE_SNAPSHOT"

# Click 8.2 introduced a sentinel distinguishing "no default declared" from an
# explicitly declared ``None``. Its ``repr`` is a click implementation detail,
# so it is recorded as a stable token instead; ``None`` still records as
# ``'None'``, keeping the unset-vs-explicit-None distinction visible.
#
# On click 8.1 that distinction does not exist -- every unset default reads back
# as plain ``None`` -- so the committed snapshot is simply not representable
# there and the comparison is skipped. The help-render cases below still run.
UNSET_TOKEN = "<unset>"
requires_click_unset = pytest.mark.skipif(
    not CLICK_HAS_UNSET,
    reason=(
        "click < 8.2 has no click.core.UNSET sentinel, so every unset default "
        "reads back as None and cannot match the recorded '<unset>' token"
    ),
)

# Commands contributed by third-party plugins (loaded through the
# ``gpio.plugins`` entry point group) are excluded from the snapshot so an
# installed plugin cannot break this test.
BUILTIN_PACKAGE = "geoparquet_io"


def _origin_module(command: click.Command) -> str:
    """Return the module a command was defined in (for plugin filtering)."""
    callback = getattr(command, "callback", None)
    if callback is not None:
        return getattr(callback, "__module__", "") or ""
    return type(command).__module__ or ""


def _is_builtin(command: click.Command) -> bool:
    module = _origin_module(command)
    return module == BUILTIN_PACKAGE or module.startswith(f"{BUILTIN_PACKAGE}.")


def _type_repr(param_type: click.ParamType) -> str:
    """Render a param type as a stable, machine-independent string.

    ``str(click.Path(...))`` embeds the object's memory address, so types are
    described structurally instead of by their default repr.
    """
    if isinstance(param_type, click.Choice):
        choices = "|".join(str(choice) for choice in param_type.choices)
        return f"choice[{choices},case_sensitive={param_type.case_sensitive}]"
    if isinstance(param_type, click.Path):
        return (
            "path["
            f"exists={param_type.exists},"
            f"file_okay={param_type.file_okay},"
            f"dir_okay={param_type.dir_okay},"
            f"writable={param_type.writable},"
            f"readable={param_type.readable},"
            f"resolve_path={param_type.resolve_path}"
            "]"
        )
    if isinstance(param_type, (click.IntRange, click.FloatRange)):
        return (
            f"{param_type.name}["
            f"min={param_type.min},"
            f"max={param_type.max},"
            f"min_open={param_type.min_open},"
            f"max_open={param_type.max_open},"
            f"clamp={param_type.clamp}"
            "]"
        )
    if isinstance(param_type, click.Tuple):
        return "tuple[" + ",".join(_type_repr(inner) for inner in param_type.types) + "]"
    return param_type.name


def _default_repr(param: click.Parameter) -> str:
    """Return ``repr`` of a parameter default, invoking callable defaults.

    Click's "no default declared" sentinel is normalized to ``<unset>`` so the
    snapshot does not churn wholesale when click changes the sentinel's repr.

    A flag with no explicit ``default=`` resolves to ``False`` at runtime under
    every click version, but click 8.5 ("Streamline Option flag handling")
    introspects it as ``UNSET`` where 8.4 materialized ``False``. Resolve the
    sentinel the way click's parser does so the snapshot matches both versions.
    """
    default = param.default
    if default is _UNSET and getattr(param, "is_flag", False):
        return repr(False)
    if default is _UNSET:
        return UNSET_TOKEN
    if callable(default):
        default = default()
    return repr(default)


def describe_param(param: click.Parameter) -> dict:
    """Describe one Click parameter as a JSON-serializable dict.

    ``opts`` and ``secondary_opts`` are recorded separately: concatenating them
    makes ``--warmup/--no-warmup`` (a boolean flag whose off-switch clears the
    value) indistinguishable from ``--warmup``/``--no-warmup`` declared as two
    aliases for the *same* switch, which is a user-visible behavior change.

    ``metavar`` records the *declared* override, not the rendered token. Click
    derives an argument's usage token from ``name.upper()`` when none is
    declared, so ``name``, ``metavar``, ``type``, ``required`` and ``nargs``
    together pin the placeholder a user reads in ``Usage:`` (Click's
    ``make_metavar`` consults all of them) -- the one piece of help text that
    is an addressable part of the CLI surface rather than reflowable prose.
    """
    return {
        "name": param.name,
        "param_type": type(param).__name__,
        "opts": sorted(param.opts),
        "secondary_opts": sorted(param.secondary_opts),
        "type": _type_repr(param.type),
        "required": bool(param.required),
        "default": _default_repr(param),
        "is_flag": bool(getattr(param, "is_flag", False)),
        "multiple": bool(getattr(param, "multiple", False)),
        "hidden": bool(getattr(param, "hidden", False)),
        "metavar": param.metavar,
        "nargs": param.nargs,
    }


def _dispatch_target(ctx: click.Context, group: click.Group) -> str | None:
    """Return the subcommand name ``parse_args`` left on a context, if any.

    Click 8.2 renamed ``Context.protected_args`` to ``_protected_args`` and left
    the old name as a deprecated property, so the private name is preferred when
    present to avoid emitting ``DeprecationWarning`` during collection.

    The result is only reported when it names a real subcommand: a plain
    ``click.Group`` handed ``["in.parquet", "out.gpkg"]`` leaves the *filename*
    in this slot, which is a parse failure waiting to happen at invoke time, not
    a dispatch decision.
    """
    attrs = (
        ("_protected_args", "args")
        if hasattr(ctx, "_protected_args")
        else ("protected_args", "args")
    )
    for attr in attrs:
        found = getattr(ctx, attr, None)
        if found and str(found[0]) in group.commands:
            return str(found[0])
    return None


def _probe_dispatch(group: click.Group, argv: list[str]) -> str | None:
    """Return the subcommand a group rewrites ``argv`` to, or ``None``.

    ``create_default_group`` in ``cli/_shared.py`` builds its groups from a
    factory, so every generated class is named ``_DefaultGroup`` and the
    configured subcommand lives only in a closure. Rather than reaching into
    ``__closure__`` cells, this asks the group what it actually does with an
    argv through the public ``parse_args`` API.

    ``resilient_parsing=True`` is load-bearing twice over. Without it, click 8.1
    answers an empty argv by echoing the group's help to stdout and raising
    ``click.exceptions.Exit`` -- not a ``UsageError``, so it would escape the
    handler below and fail collection of this whole module.

    It also stops click from *enforcing* the group's own parameters: a group
    that later grows a required option would otherwise raise
    ``MissingParameter`` (a ``UsageError``) here and be recorded as a bogus
    ``None`` default rather than its real dispatch target.

    Note it does not stop eager callbacks from running -- click's convention is
    that a callback checks ``ctx.resilient_parsing`` and returns early, which
    the only eager option in the tree (``--version`` on the root group) does.
    """
    ctx = click.Context(group, resilient_parsing=True)
    try:
        group.parse_args(ctx, list(argv))
    except click.UsageError:
        # Defensive: resilient parsing suppresses click's own usage errors, but
        # a future click answering "no subcommand" this way must not turn into a
        # collection-time error for the entire module.
        return None
    return _dispatch_target(ctx, group)


def _probe_default_subcommand(group: click.Group) -> str | None:
    """Return the subcommand a group dispatches to when given no arguments.

    Note this pins the *dispatch target* only. Full behavioral coverage of bare
    default dispatch (``gpio check <file>`` running ``check all``) is deferred
    to the test-consolidation issue, #666.
    """
    return _probe_dispatch(group, [])


# Output extensions probed against each group's argv rewriting. ``gpio convert``
# picks its subcommand from the output file's extension, and that map -- not the
# empty-argv fallback -- is the user-visible behavior of the group. The probe set
# is the union of a fixed baseline (so *dropping* a mapping is caught) and
# whatever a group declares for itself (so *adding* one is discovered rather
# than silently missed).
BASELINE_PROBE_EXTENSIONS = (
    ".csv",
    ".fgb",
    ".geojson",
    ".gpkg",
    ".json",
    ".parquet",
    ".shp",
    ".unrecognized",
)


def _probe_extension_dispatch(group: click.Group, default_subcommand: str | None) -> dict:
    """Map output-file extension -> subcommand, for extensions that change it.

    Extensions that dispatch to the same place as a bare argv are omitted: they
    are behaviorally indistinguishable from the fallback, so recording them
    would bulk up every group in the tree with a copy of the same answer.
    """
    declared = getattr(type(group), "EXTENSION_TO_SUBCOMMAND", None) or {}
    extensions = sorted({*BASELINE_PROBE_EXTENSIONS, *declared})
    dispatch = {}
    for ext in extensions:
        target = _probe_dispatch(group, ["in.parquet", f"out{ext}"])
        if target != default_subcommand:
            dispatch[ext] = target
    return dispatch


def describe_command(command: click.Command) -> dict:
    """Describe a command (or group, recursively) as a JSON-serializable dict."""
    described: dict = {
        "kind": "group" if isinstance(command, click.Group) else "command",
        "class": type(command).__name__,
        "hidden": bool(getattr(command, "hidden", False)),
        "params": sorted(
            (describe_param(param) for param in command.params),
            key=lambda entry: entry["name"] or "",
        ),
    }
    if isinstance(command, click.Group):
        default_subcommand = _probe_default_subcommand(command)
        described["default_subcommand"] = default_subcommand
        described["extension_dispatch"] = _probe_extension_dispatch(command, default_subcommand)
        described["commands"] = {
            name: describe_command(sub)
            for name, sub in sorted(command.commands.items())
            if _is_builtin(sub)
        }
    return described


def build_surface() -> dict:
    """Walk the built-in ``gpio`` command tree into a JSON-serializable dict."""
    return describe_command(cli)


def iter_leaf_paths(described: dict, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    """Yield the argv path of every non-group command in a described tree."""
    if described["kind"] != "group":
        return [prefix]
    leaves: list[tuple[str, ...]] = []
    for name, sub in described["commands"].items():
        leaves.extend(iter_leaf_paths(sub, prefix + (name,)))
    return leaves


def iter_group_paths(described: dict, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    """Yield the argv path of every group in a described tree, root included."""
    if described["kind"] != "group":
        return []
    groups = [prefix]
    for name, sub in described["commands"].items():
        groups.extend(iter_group_paths(sub, prefix + (name,)))
    return groups


def _serialize(surface: dict) -> str:
    return json.dumps(surface, indent=1, sort_keys=True) + "\n"


def _is_name_keyed_list(value) -> bool:
    """True for a list of dicts that each carry a ``name`` (i.e. a params list)."""
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(item, dict) and "name" in item for item in value)
    )


def _describe_subtree(value, prefix: str, verb: str) -> list[str]:
    """Render a wholly added/removed value as readable, path-addressed lines.

    A single ``repr`` of an added or removed *group* is one 18k-character line
    that no line cap can help with, so containers are walked and each branch
    reported at its own path. Recursion stops at the first level with nothing
    nested below it -- a params list is summarized by name, and one parameter's
    field bag is reported whole -- which keeps a deleted group to a few dozen
    lines instead of several hundred.
    """
    if _is_name_keyed_list(value):
        names = ", ".join(sorted(str(item["name"]) for item in value))
        return [f"{prefix}: {verb} ({len(value)} entries: {names})"]
    if isinstance(value, dict) and any(
        isinstance(v, dict) or _is_name_keyed_list(v) for v in value.values()
    ):
        lines = []
        for key in sorted(value):
            lines.extend(_describe_subtree(value[key], f"{prefix}/{key}", verb))
        if lines:
            return lines
    return [f"{prefix}: {verb} ({value!r})"]


def _diff_paths(expected, actual, prefix: str = "") -> list[str]:
    """Return human-readable descriptions of every difference between two trees."""
    where = prefix or "<root>"
    if type(expected) is not type(actual):
        return [f"{where}: type changed {type(expected).__name__} -> {type(actual).__name__}"]
    if isinstance(expected, dict):
        diffs = []
        for key in sorted(set(expected) - set(actual)):
            diffs.extend(_describe_subtree(expected[key], f"{prefix}/{key}", "removed, was"))
        for key in sorted(set(actual) - set(expected)):
            diffs.extend(_describe_subtree(actual[key], f"{prefix}/{key}", "added"))
        for key in sorted(set(expected) & set(actual)):
            diffs.extend(_diff_paths(expected[key], actual[key], f"{prefix}/{key}"))
        return diffs
    if isinstance(expected, list):
        # Params are keyed by name so a reorder is not reported as a change.
        if all(isinstance(item, dict) and "name" in item for item in expected + actual):
            keyed_expected = {item["name"]: item for item in expected}
            keyed_actual = {item["name"]: item for item in actual}
            return _diff_paths(keyed_expected, keyed_actual, prefix)
        if expected != actual:
            return [f"{where}: {expected!r} != {actual!r}"]
        return []
    if expected != actual:
        return [f"{where}: expected {expected!r}, got {actual!r}"]
    return []


TRUTHY_ENV_VALUES = frozenset({"1", "true", "yes", "on"})


def _env_flag(name: str) -> bool:
    """True only for an explicitly affirmative value.

    Plain truthiness would treat ``GPIO_UPDATE_SNAPSHOT=0`` -- the obvious way
    to say "no" -- as a request to rewrite the baseline.
    """
    return os.environ.get(name, "").strip().lower() in TRUTHY_ENV_VALUES


@requires_click_unset
def test_cli_surface_matches_snapshot():
    """The Click command tree matches the committed structural snapshot."""
    surface = build_surface()

    if _env_flag(UPDATE_ENV_VAR):
        # Never let a stray env var turn CI green by rewriting the baseline.
        assert not os.environ.get("CI"), (
            f"{UPDATE_ENV_VAR} must not be set in CI: it would rewrite the "
            "baseline instead of checking against it. Re-record the snapshot "
            "locally and commit the diff."
        )
        # newline="\n" keeps a re-record on Windows from rewriting all 11k lines
        # with CRLF. The read side deliberately keeps universal newlines, so a
        # snapshot that arrives with CRLF still compares clean.
        SNAPSHOT_PATH.write_text(_serialize(surface), encoding="utf-8", newline="\n")
        pytest.skip(f"{UPDATE_ENV_VAR} set: refreshed {SNAPSHOT_PATH.name}")

    assert SNAPSHOT_PATH.exists(), (
        f"Missing CLI surface snapshot {SNAPSHOT_PATH}. "
        f"Create it with {UPDATE_ENV_VAR}=1 uv run pytest {Path(__file__).name}"
    )
    expected = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))

    diffs = _diff_paths(expected, surface)
    assert not diffs, (
        "CLI surface drifted from tests/data/cli_surface.json:\n"
        + "\n".join(f"  - {line}" for line in diffs[:40])
        + (f"\n  ... and {len(diffs) - 40} more" if len(diffs) > 40 else "")
        + f"\n\nIf the change is intentional, re-record the snapshot with:\n"
        f"  {UPDATE_ENV_VAR}=1 uv run pytest tests/{Path(__file__).name}\n"
        "and review the resulting diff before committing."
    )


LEAF_PATHS = iter_leaf_paths(build_surface())


@pytest.mark.parametrize(
    "argv",
    LEAF_PATHS,
    ids=["/".join(path) for path in LEAF_PATHS],
)
def test_leaf_command_help_renders(argv):
    """``--help`` renders successfully for every built-in leaf command."""
    result = CliRunner().invoke(cli, [*argv, "--help"])
    assert result.exit_code == 0, (
        f"gpio {' '.join(argv)} --help exited {result.exit_code}\n{result.output}"
    )
    assert "Usage:" in result.output


GROUP_PATHS = iter_group_paths(build_surface())


@pytest.mark.parametrize(
    "argv",
    GROUP_PATHS,
    ids=["/".join(path) or "gpio" for path in GROUP_PATHS],
)
def test_group_help_renders(argv):
    """``--help`` renders for every group, including default-dispatch groups.

    The groups built by ``create_default_group`` intercept ``--help`` in their
    own ``parse_args`` before falling through to the default subcommand, so
    this covers a branch the leaf cases never reach.
    """
    result = CliRunner().invoke(cli, [*argv, "--help"])
    assert result.exit_code == 0, (
        f"gpio {' '.join(argv)} --help exited {result.exit_code}\n{result.output}"
    )
    assert "Usage:" in result.output


def test_command_inventory_is_non_trivial():
    """Guard against the walker silently collecting nothing."""
    assert len(LEAF_PATHS) > 40
    assert len(GROUP_PATHS) > 10
