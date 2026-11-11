"""The one census of CLI/API parity, read from `tests/parity_registry.py` (#1088).

Every CLI leaf is checked on three axes -- an API twin, a functional twin (an
`ops.*` or root `gpio.*` function), and a call-parity case -- and every
parameter one side has and the other lacks is either explained by a derived
class or recorded with a reason (#903). Each check is a set equality, so an
exemption that stops being needed fails as loudly as a new gap.
"""

from __future__ import annotations

import geoparquet_io
from tests import parity_registry as reg
from tests import test_sub_partition
from tests.test_cli_api_call_parity_scaffold import CASES_BY_ID


def _axis_problems(leaf: str, covered: bool, exempt: dict[str, str], axis: str) -> list[str]:
    if covered and axis in exempt:
        return [f"{leaf}: EXEMPT[{axis!r}] is stale, the axis is covered"]
    if not covered and axis not in exempt:
        return [f"{leaf}: no {axis.replace('_', ' ')} and no EXEMPT[{axis!r}] reason"]
    return []


def _leaf_problems(leaf: str) -> list[str]:
    exempt = reg.EXEMPT.get(leaf, {})
    twins = reg.api_twins(leaf)
    problems = _axis_problems(leaf, bool(twins), exempt, "api_twin")
    if not twins:
        # A twinless leaf's api_twin reason covers the other two axes.
        moot = sorted(set(exempt) - {"api_twin"})
        if moot:
            problems.append(f"{leaf}: EXEMPT{moot} is moot on a leaf with no API twin")
        return problems
    functional = any(reg.is_functional(label) for label, _fn in twins)
    problems += _axis_problems(leaf, functional, exempt, "ops_twin")
    problems += _axis_problems(leaf, leaf in CASES_BY_ID, exempt, "call_case")
    return problems


def test_every_leaf_is_covered_or_exempt_on_every_axis():
    """Twin, functional twin and call case per leaf; no unknown or stale keys."""
    leaves = reg.cli_leaves()
    problems = [p for leaf in sorted(leaves) for p in _leaf_problems(leaf)]
    problems += [f"EXEMPT names unknown leaf {leaf!r}" for leaf in sorted(set(reg.EXEMPT) - leaves)]
    problems += [
        f"EXEMPT[{leaf!r}] names unknown axis {axis!r}"
        for leaf, axes in sorted(reg.EXEMPT.items())
        for axis in sorted(set(axes) - set(reg.AXES))
    ]
    problems += [f"CASES_BY_ID names unknown leaf {c!r}" for c in sorted(set(CASES_BY_ID) - leaves)]
    problems += [
        f"TWIN_NAMES names unknown leaf {leaf!r}" for leaf in sorted(set(reg.TWIN_NAMES) - leaves)
    ]
    for leaf, names in sorted(reg.TWIN_NAMES.items()):
        resolved = {label.split(".", 1)[1] for label, _fn in reg.api_twins(leaf)}
        problems += [
            f"TWIN_NAMES[{leaf!r}]: {n!r} resolves to nothing" for n in names if n not in resolved
        ]
    assert not problems, "Parity census:\n" + "\n".join(f"  {p}" for p in problems)


def test_table_and_ops_twins_share_one_name():
    """#1436 decision 4, point 1: a `Table` method and its `ops` function carry one name.

    Strict, and not an allowlist of pairs: for every leaf whose API has both legs,
    the names they resolve under must be the same set, and a name only one leg
    has must be a `TWIN_NAMES` entry carrying its reason. A twin collapsed to one
    canonical name therefore needs no entry at all, a new mismatch fails until it
    is either collapsed or reasoned, and an entry whose name stops resolving
    fails in the census above.
    """
    problems = []
    for leaf in sorted(reg.cli_leaves()):
        reasons = reg.TWIN_NAMES.get(leaf, {})
        problems += [
            f"{leaf}: {name!r} is on one API leg only, with no TWIN_NAMES reason -- "
            f"give both legs one name (with a deprecated alias for the old one), or "
            f"record why they differ"
            for name in sorted(reg.unpaired_twin_names(leaf) - set(reasons))
        ]
    assert not problems, "Twin-name census:\n" + "\n".join(f"  {p}" for p in problems)


def test_deprecated_aliases_are_recorded():
    """Every alias the API marks is in `DEPRECATED_ALIASES`, and nothing else is.

    The resolver skips an alias, so an unrecorded one would quietly remove a name
    from the census. Set equality both ways: an alias removed on schedule has to
    lose its entry, and its canonical name must still be a live callable.
    """
    live = reg.live_deprecated_aliases()
    assert set(live) == set(reg.DEPRECATED_ALIASES), (
        "DEPRECATED_ALIASES drifted from the aliases the API marks.\n"
        f"  unrecorded: {sorted(set(live) - set(reg.DEPRECATED_ALIASES))}\n"
        f"  recorded but gone: {sorted(set(reg.DEPRECATED_ALIASES) - set(live))}"
    )
    assert {name: target for name, (target, _r) in reg.DEPRECATED_ALIASES.items()} == live


def _group_problems(leaves: set[str], cases: set[str], exempt: dict, recorded: dict) -> list[str]:
    """A group with no case must have every leaf exempt, and be recorded as such."""
    groups: dict[str, set[str]] = {}
    for leaf in leaves:
        groups.setdefault(leaf.split()[0], set()).add(leaf)
    all_exempt = {
        group
        for group, members in groups.items()
        if all({"api_twin", "call_case"} & set(exempt.get(leaf, {})) for leaf in members)
    }
    problems = [
        f"group {g!r} has no call-parity case and a leaf that is not exempt from one"
        for g in sorted(set(groups) - {c.split()[0] for c in cases} - all_exempt)
    ]
    problems += [
        f"group {g!r}: every leaf is call_case-exempt; record it in GROUPS_WITHOUT_A_CALL_CASE"
        for g in sorted(all_exempt - set(recorded))
    ]
    problems += [
        f"GROUPS_WITHOUT_A_CALL_CASE[{g!r}] is stale: a leaf in it is not exempt"
        for g in sorted(set(recorded) - all_exempt)
    ]
    return problems


def test_every_command_group_has_a_call_case_or_a_recorded_reason():
    """The WP-7 invariant (#1018): no command group goes entirely uncompared."""
    problems = _group_problems(
        reg.cli_leaves(), set(CASES_BY_ID), reg.EXEMPT, reg.GROUPS_WITHOUT_A_CALL_CASE
    )
    assert not problems, "Group census:\n" + "\n".join(f"  {p}" for p in problems)


def test_root_exported_path_functions_are_twins():
    """#1065: these three twins live at the package root, not on `ops` or `Table`."""
    expected = {
        "inspect layers": geoparquet_io.list_layers,
        "check stac": geoparquet_io.validate_stac,
        "publish stac": geoparquet_io.generate_stac,
    }
    for leaf, fn in expected.items():
        twins = dict(reg.api_twins(leaf))
        assert twins.get(f"gpio.{fn.__name__}") is fn, (leaf, sorted(twins))
        assert reg.is_functional(f"gpio.{fn.__name__}")


def test_every_registry_entry_has_a_reason():
    tables = {
        "EXEMPT": {
            (leaf, axis): r for leaf, axes in reg.EXEMPT.items() for axis, r in axes.items()
        },
        "TWIN_NAMES": {
            (leaf, name): r for leaf, names in reg.TWIN_NAMES.items() for name, r in names.items()
        },
        "DEPRECATED_ALIASES": {
            name: reason for name, (_target, reason) in reg.DEPRECATED_ALIASES.items()
        },
        "ONE_SIDED": reg.ONE_SIDED,
        "ONE_SIDED_CLASSES": reg.ONE_SIDED_CLASSES,
        "API_REMOTE_EXEMPT": reg.API_REMOTE_EXEMPT,
        "KNOWN_DIVERGENCES": reg.KNOWN_DIVERGENCES,
        "KNOWN_PARITY_GAPS": reg.KNOWN_PARITY_GAPS,
        "GROUPS_WITHOUT_A_CALL_CASE": reg.GROUPS_WITHOUT_A_CALL_CASE,
    }
    blank = [
        f"{name}{key}"
        for name, table in tables.items()
        for key, r in table.items()
        if not r.strip()
    ]
    assert not blank, "Registry entries with no reason:\n" + "\n".join(blank)


def _diff_message(side: str, actual: set, recorded: set) -> str:
    new = "\n".join(f"  {k}" for k in sorted(actual - recorded))
    stale = "\n".join(f"  {k}" for k in sorted(recorded - actual))
    return (
        f"{side}-only parameters drifted from ONE_SIDED (tests/parity_registry.py).\n"
        f"New -- add the knob to the other side, or record it with a reason:\n{new}\n"
        f"Stale -- now matched or gone; delete the entry:\n{stale}"
    )


def test_no_unrecorded_cli_only_parameter():
    """#903: a CLI option no twin accepts is a class member or a recorded reason."""
    actual = reg.one_sided("cli")
    recorded = {key for key in reg.ONE_SIDED if key[1] == "cli"}
    assert actual == recorded, _diff_message("CLI", actual, recorded)


def test_a_leaf_with_remote_flags_has_an_api_side_that_takes_them():
    """#1452: the census required nothing of a remote-flagged leaf's API twin.

    `ONE_SIDED_CLASSES["remote_access"]` excuses an API-side `remote`, so a twin
    that takes a bare `profile=` -- or no remote option at all -- while its CLI
    side packs `--aws-profile` into a `RemoteOptions` was invisible here. A twin
    that hands an in-memory Table on is excused by derivation; one whose operand
    is a path does the store I/O itself and must take `remote=`.
    """
    actual = reg.api_twins_missing_remote()
    assert actual == set(reg.API_REMOTE_EXEMPT), (
        "Path-taking API twins of leaves that declare remote flags drifted from "
        "API_REMOTE_EXEMPT (tests/parity_registry.py).\n"
        f"New -- add `remote: gpio.RemoteOptions | None = None`, or record a reason:\n"
        f"{sorted(actual - set(reg.API_REMOTE_EXEMPT))}\n"
        f"Stale -- now takes it; delete the entry:\n"
        f"{sorted(set(reg.API_REMOTE_EXEMPT) - actual)}"
    )


def test_no_unrecorded_api_only_parameter():
    """#903, the other way: a twin keyword no CLI option spells."""
    actual = reg.one_sided("api")
    recorded = {key for key in reg.ONE_SIDED if key[1] == "api"}
    assert actual == recorded, _diff_message("API", actual, recorded)


def test_sub_partition_front_door_case_exercises_every_shared_knob():
    """`partition <idx> <dir> --min-size` forwards its knobs through `**run`, which
    no signature shows; the front-door case must set every knob the twin shares."""
    exercised: dict[str, set[str]] = {}
    for index, invocations in test_sub_partition.TestSubPartitionFrontDoorParity.CASES:
        exercised[index] = {k for _args, kwargs in invocations for k in kwargs}
    expected = reg.sub_partition_knobs()
    assert set(exercised) == set(expected)
    missing = {index: sorted(expected[index] - exercised[index]) for index in expected}
    assert not any(missing.values()), f"Knobs no front-door case sets: {missing}"
