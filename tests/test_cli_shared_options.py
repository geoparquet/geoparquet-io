"""Guard G13 of #1367: an option name means the same thing on every command.

Walks the live Click tree (``_iter_options`` from
``tests/test_cli_column_name_guard.py``) and, for every option name declared on
more than one command, compares what each declaration does with a value: the
``click.ParamType`` (its class and its ``choices`` or range), the ``default``,
the ``callback`` (by qualified name), and ``multiple`` / ``is_flag``. Two
``--bbox`` options that parse differently are two copies of a parser that
drift apart (``parse_bbox('nan,0,1,1')`` accepted by two commands and rejected
by one, #1367); a shared option belongs in ``cli/decorators.py``.

The option names that differ today are pinned in scripts/owner_exemptions.toml
under ``[[G13.options]]`` with the fields that differ and the track that unifies
them; the set must match exactly, so a new difference fails and a fixed one
must leave the list.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import click
import pytest

from geoparquet_io.cli.main import cli
from tests.test_cli_column_name_guard import _iter_options

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib  # type: ignore[no-redef]

REPO = Path(__file__).resolve().parents[1]
EXEMPTIONS_FILE = REPO / "scripts" / "owner_exemptions.toml"
FIELDS = ("type", "default", "callback", "multiple", "is_flag")
BUILTIN_PACKAGE = "geoparquet_io"


def _qualified(func) -> str | None:
    if func is None:
        return None
    func = getattr(func, "func", func)  # functools.partial
    return f"{getattr(func, '__module__', '?')}.{getattr(func, '__qualname__', repr(func))}"


def _type_signature(param_type: click.ParamType) -> str:
    parts = [type(param_type).__qualname__]
    for attr in ("choices", "min", "max", "min_open", "max_open", "clamp", "case_sensitive"):
        if hasattr(param_type, attr):
            value = getattr(param_type, attr)
            parts.append(f"{attr}={list(value) if attr == 'choices' else value!r}")
    if hasattr(param_type, "types"):  # click.Tuple
        parts.append("types=" + ",".join(_type_signature(t) for t in param_type.types))
    return " ".join(parts)


def _default(option: click.Option) -> str:
    """The default as compared. Click 8.2's "no default declared" sentinel and
    an explicit ``None`` both hand the command ``None``, so they compare equal."""
    value = option.default
    if (
        value is None
        or type(value).__name__ in {"Sentinel", "_UnsetType"}
        or repr(value).startswith(("Sentinel.", "<UNSET"))
    ):
        return "None"
    if callable(value):
        return f"<callable {_qualified(value)}>"
    return repr(value)


def option_signature(option: click.Option) -> dict[str, str]:
    return {
        "type": _type_signature(option.type),
        "default": _default(option),
        "callback": str(_qualified(option.callback)),
        "multiple": str(option.multiple),
        "is_flag": str(option.is_flag),
    }


def option_name(option: click.Option) -> str:
    """The option's longest long flag (``--bbox``), its identity across commands."""
    longs = [o for o in option.opts if o.startswith("--")] or list(option.opts)
    return max(longs, key=len)


def _from(command: click.Command, package: str | None) -> bool:
    """The command is gpio's own, not a plugin's (``gpio.plugins`` entry points)."""
    callback = command.callback
    return (
        package is None
        or callback is None
        or getattr(callback, "__module__", "").startswith(package)
    )


def _iter_builtin_options(group: click.Command, package: str | None = BUILTIN_PACKAGE):
    def walk(command, path):
        if isinstance(command, click.Group):
            for name, sub in command.commands.items():
                if _from(sub, package) or isinstance(sub, click.Group):
                    yield from walk(sub, (*path, name))
            return
        for _, option in _iter_options(command, path):
            yield path, option

    yield from walk(group, ())


def shared_option_differences(
    group: click.Command, package: str | None = BUILTIN_PACKAGE
) -> dict[str, list[str]]:
    """Option name -> the fields whose values differ across its commands,
    for every option name declared on more than one command."""
    seen: dict[str, list[tuple[tuple[str, ...], dict[str, str]]]] = defaultdict(list)
    for path, option in _iter_builtin_options(group, package):
        seen[option_name(option)].append((path, option_signature(option)))
    differences = {}
    for name, declarations in seen.items():
        if len(declarations) < 2:
            continue
        fields = [f for f in FIELDS if len({sig[f] for _, sig in declarations}) > 1]
        if fields:
            differences[name] = fields
    return differences


def describe(group: click.Command, name: str) -> str:
    rows = [
        f"    {' '.join(path)}: " + ", ".join(f"{k}={v}" for k, v in sig.items())
        for path, option in _iter_builtin_options(group)
        if option_name(option) == name
        for sig in [option_signature(option)]
    ]
    return f"{name}\n" + "\n".join(rows)


def _pinned() -> dict[str, dict]:
    with EXEMPTIONS_FILE.open("rb") as fh:
        entries = tomllib.load(fh).get("G13", {}).get("options", [])
    return {e["option"]: e for e in entries}


def test_shared_options_agree_or_are_pinned():
    live = shared_option_differences(cli)
    pinned = _pinned()
    new = sorted(set(live) - set(pinned))
    gone = sorted(set(pinned) - set(live))
    changed = sorted(n for n in set(live) & set(pinned) if live[n] != pinned[n]["fields"])
    assert not (new or gone or changed), (
        "shared-option consistency (G13) drifted:\n"
        + "".join(f"  new: {describe(cli, n)}\n" for n in new)
        + "".join(f"  fixed (delete its [[G13.options]] entry): {n}\n" for n in gone)
        + "".join(
            f"  fields differ from the pin: {n} pinned {pinned[n]['fields']}, now {live[n]}\n"
            for n in changed
        )
        + "Declare the option once in cli/decorators.py and reuse it."
    )


def test_every_pinned_option_has_a_reason():
    assert all(e["reason"].strip() for e in _pinned().values())


def test_g12_the_walk_reports_two_commands_that_disagree_on_a_default():
    @click.group()
    def group():
        pass

    @group.command()
    @click.option("--x", default=1)
    def one(**_options):
        pass

    @group.command()
    @click.option("--x", default=2)
    def two(**_options):
        pass

    assert shared_option_differences(group, None) == {"--x": ["default"]}


@pytest.mark.parametrize(
    ("left", "right", "field"),
    [
        ({"type": click.Choice(["a", "b"])}, {"type": click.Choice(["a"])}, "type"),
        ({"type": int}, {"type": str}, "type"),
        ({"type": click.IntRange(0, 5)}, {"type": click.IntRange(0, 9)}, "type"),
        ({"callback": lambda c, p, v: v}, {}, "callback"),
        ({"multiple": True}, {}, "multiple"),
        ({"is_flag": True}, {}, "is_flag"),
    ],
)
def test_g12_the_walk_compares_each_field(left, right, field):
    @click.group()
    def group():
        pass

    @group.command()
    @click.option("--x", **left)
    def one(**_options):
        pass

    @group.command()
    @click.option("--x", **right)
    def two(**_options):
        pass

    assert field in shared_option_differences(group, None)["--x"]


def test_g12_the_walk_passes_identical_declarations_and_one_off_options():
    @click.group()
    def group():
        pass

    @group.command()
    @click.option("--x", type=int, default=3)
    @click.option("--only-here", default=1)
    def one(**_options):
        pass

    @group.command()
    @click.option("--x", type=int, default=3)
    def two(**_options):
        pass

    assert shared_option_differences(group, None) == {}
