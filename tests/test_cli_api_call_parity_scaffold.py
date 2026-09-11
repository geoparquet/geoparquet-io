"""SCAFFOLDING -- delete or rewrite when the write facade lands (#664, suite 5).

This module deliberately knows *call structure*: it patches the core function
each front end imports and compares the keyword arguments the front ends hand
down. That is normally the wrong thing to test -- but right now there is no
single seam where `gpio add h3 in.parquet out.parquet`, `ops.add_h3(table)` and
`Table.add_h3()` meet, so there is nothing behavioural to compare. The CLI calls
the file-centric `add_h3_column()`; both API front ends call the table-centric
`add_h3_table()`. Two implementations, two option sets, one advertised
operation.

Until the write-path unification gives those three doors a shared facade, this
harness is the only thing that notices when one door starts passing a different
resolution, column name or compression codec than the others. **When the facade
lands, throw this file away** and assert on the facade's inputs instead.

How it works
------------
Each `ParityCase` names, per front end:

* the *patch site* -- the namespace the front end resolved the core function
  into, as a ``(module_or_class, attribute)`` pair. `ops` binds its core imports
  at module import time, so `ops` is patched on `ops`; `Table` imports inside
  each method body, so `Table` is patched on the *core* module; the CLI imports
  under an ``_impl`` alias, so the CLI is patched on the module that owns the
  command. That is `cli.main` for groups still living there, and
  `cli.commands.<group>` for groups already extracted (#3.2) -- pass
  ``module=`` to `_cli` for those.
* the *reference callable* -- the real function, used only for its signature.
  Captured calls are bound through it with ``apply_defaults()``, so a value
  passed positionally by one front end and by keyword (or not at all) by
  another still compares equal. What is compared is the **effective** value the
  core sees, which is the contract users actually feel.
* how to *invoke* it with nothing but defaults.

`ParityCase.normalize` then maps a canonical parameter name onto the parameter
name each side spells it with, e.g. ``"resolution": ("h3_resolution",
"resolution")``. **These maps are the contract.** They record which CLI option
and which API argument are meant to be the same knob; every entry is a claim
that those two names must always carry the same value. A key absent from the
map is a knob one side does not have (the table-centric `add_*_table` functions
take no compression or row-group options at all -- they do not write).

Known gaps live in `KNOWN_PARITY_GAPS`, keyed by ``(case id, front end,
canonical name, repr(CLI value), repr(API value))`` with a written
justification, following `collect_divergences()` in
`tests/test_cli_api_default_parity.py`. The two reprs are part of the key on
purpose: an allowlist recording only "these differ" would stay green if
`ops.sort_str` changed `tile_size` from 50000 to 40000 -- still a divergence, but a
different one nobody reviewed.

A per-key allowlist is used rather than `xfail` on the parametrized case because
`xfail` is case-granular: xfailing `partition h3` over its known auto-resolution
gap would also stop `hive`, `overwrite` and `keep_h3_column` from being checked.
`test_known_parity_gap_still_diverges_the_same_way` re-surfaces every entry as
its own named test, so the findings stay visible in the pytest report and a gap
that gets *fixed* fails until its entry is deleted.

Complements (does not duplicate) `tests/test_cli_api_default_parity.py`, which
diffs *declared signature defaults* across the whole CLI by introspection. This
module diffs *values actually delivered to core* for the ~13 commands the
refactors touch -- which catches the cases introspection cannot see, such as the
CLI deriving `iterations` from `--partitions` or upper-casing `--compression`
on the way down.

Patching note (project memory): dotted-string `mock.patch("...cli.main.X")`
targets fail on Python 3.10. Every patch here is `patch.object(module, name)`,
which is unaffected -- the trap is about string targets only.

Running note: this file mocks core out, so it covers very little of the package
and running it *alone* trips the coverage floor. Use `--no-cov` when running
it in isolation; in the full suite it is a non-issue.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from geoparquet_io.api import ops
from geoparquet_io.api import table as table_module
from geoparquet_io.cli import main as cli_main
from geoparquet_io.cli.commands import add as cli_add
from geoparquet_io.cli.commands import convert as cli_convert
from geoparquet_io.cli.commands import extract as cli_extract
from geoparquet_io.cli.commands import partition as cli_partition
from geoparquet_io.cli.commands import sort as cli_sort
from geoparquet_io.core import extract as core_extract
from geoparquet_io.core import hilbert_order as core_hilbert
from geoparquet_io.core import sort_by_column as core_sort_column
from geoparquet_io.core import sort_quadkey as core_sort_quadkey
from geoparquet_io.core import str_order as core_str
from geoparquet_io.core import write_strategies as core_write_strategies
from geoparquet_io.core.add import a5 as core_a5
from geoparquet_io.core.add import bbox as core_bbox
from geoparquet_io.core.add import h3 as core_h3
from geoparquet_io.core.add import kdtree as core_kdtree
from geoparquet_io.core.add import quadkey as core_quadkey
from geoparquet_io.core.add import s2 as core_s2
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.parquet_writer import DEFAULT_ROW_GROUP_ROWS
from geoparquet_io.core.partition import by_h3 as core_part_h3
from geoparquet_io.core.partition import by_quadkey as core_part_quadkey

# Sentinel: the recorder returns its first positional argument. The table-centric
# core functions return a table that the API front ends immediately re-wrap, so a
# stub returning `None` would blow up before we could inspect anything.
ECHO_FIRST_ARG = object()

MISSING = object()


class _Recorder:
    """Stand-in for a core function that records the arguments it was handed.

    Calls are normalized through `reference`'s signature with
    ``apply_defaults()``, so `self.calls[0]` is always a full
    ``{parameter: effective value}`` mapping regardless of how the caller split
    its positional and keyword arguments.
    """

    def __init__(self, reference: Callable | None, result: Any = None):
        self._signature = inspect.signature(reference) if reference is not None else None
        self._result = result
        self.calls: list[dict[str, Any]] = []

    def __call__(self, *args, **kwargs):
        if self._signature is None:
            self.calls.append(dict(kwargs))
        else:
            bound = self._signature.bind(*args, **kwargs)
            bound.apply_defaults()
            self.calls.append(dict(bound.arguments))
        if self._result is ECHO_FIRST_ARG:
            return args[0] if args else next(iter(kwargs.values()))
        return self._result


class _StrategyStub:
    """Minimal write-strategy object whose `write_from_table` records its kwargs."""

    def __init__(self, recorder: _Recorder):
        self._recorder = recorder

    def write_from_table(self, **kwargs):
        return self._recorder(**kwargs)


@dataclass(frozen=True)
class Frontend:
    """One door into an operation: where to patch it, and how to call it."""

    patch_site: tuple[Any, str]
    reference: Callable | None
    invoke: Callable[[Ctx], Any]
    result: Any = None
    # Some front ends do not resolve a core function by name (Table.write picks a
    # write strategy through a factory), so they install their own recorder.
    install: Callable[[Any, _Recorder], Any] | None = None


@dataclass(frozen=True)
class ParityCase:
    id: str
    cli: Frontend
    table: Frontend
    normalize: dict[str, tuple[str, str]]
    ops: Frontend | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)


@dataclass
class Ctx:
    """Inputs the front ends need: a real file for the CLI, a real table for the API."""

    input_file: str
    convert_input: str
    output_file: str
    output_dir: str
    table: Any
    gpio_table: Any
    runner: CliRunner
    cli_result: Any = None

    def run_cli(self, args: list[str]):
        self.cli_result = self.runner.invoke(cli_main.cli, args)
        return self.cli_result


# --------------------------------------------------------------------------
# Front-end constructors
# --------------------------------------------------------------------------


def _cli(
    alias: str,
    reference: Callable,
    args: Callable[[Ctx], list[str]],
    module: Any = cli_main,
) -> Frontend:
    return Frontend(
        patch_site=(module, alias),
        reference=reference,
        invoke=lambda ctx: ctx.run_cli(args(ctx)),
    )


def _ops(attr: str, reference: Callable, call: Callable[[Ctx], Any]) -> Frontend:
    return Frontend(
        patch_site=(ops, attr),
        reference=reference,
        invoke=call,
        result=ECHO_FIRST_ARG,
    )


def _table(module: Any, attr: str, reference: Callable, call: Callable[[Ctx], Any]) -> Frontend:
    return Frontend(
        patch_site=(module, attr),
        reference=reference,
        invoke=call,
        result=ECHO_FIRST_ARG,
    )


# An `ops` front end that reaches core through `Table`.
#
# `ops.partition_by_*` delegates to the matching `Table` method rather than keeping a
# second copy of the temp-file plumbing, and `Table` imports its core function inside
# the method body -- so this one is patched on the *core* module, like a `Table` front
# end, not on `ops`. That makes it constructed exactly like `_table`; the alias keeps
# the call sites at the case list reading as "ops, via Table" without a duplicate body.
_ops_via_table = _table


def _install_write_strategy(_frontend, recorder: _Recorder):
    """Patch the strategy factory so Table.write's core write call is recorded.

    `get_strategy` is a `classmethod`; `patch.object` restores the descriptor
    itself rather than the bound method `getattr` would hand back, so the class
    is left exactly as it was found.
    """
    return patch.object(
        core_write_strategies.WriteStrategyFactory,
        "get_strategy",
        lambda *_args, **_kwargs: _StrategyStub(recorder),
    )


# --------------------------------------------------------------------------
# The case table
# --------------------------------------------------------------------------

SORT_COLUMN = "name"  # a real column of tests/data/places_test.parquet


CASES: list[ParityCase] = [
    ParityCase(
        id="add bbox",
        cli=_cli(
            "add_bbox_column_impl",
            core_bbox.add_bbox_column,
            lambda c: ["add", "bbox", c.input_file, c.output_file],
            module=cli_add,
        ),
        ops=_ops("add_bbox_table", core_bbox.add_bbox_table, lambda c: ops.add_bbox(c.table)),
        table=_table(
            core_bbox,
            "add_bbox_table",
            core_bbox.add_bbox_table,
            lambda c: c.gpio_table.add_bbox(),
        ),
        normalize={"column_name": ("bbox_column_name", "bbox_column_name")},
    ),
    ParityCase(
        id="add h3",
        cli=_cli(
            "add_h3_column_impl",
            core_h3.add_h3_column,
            lambda c: ["add", "h3", c.input_file, c.output_file],
            module=cli_add,
        ),
        ops=_ops("add_h3_table", core_h3.add_h3_table, lambda c: ops.add_h3(c.table)),
        table=_table(
            core_h3, "add_h3_table", core_h3.add_h3_table, lambda c: c.gpio_table.add_h3()
        ),
        normalize={
            "column_name": ("h3_column_name", "h3_column_name"),
            "resolution": ("h3_resolution", "resolution"),
        },
    ),
    ParityCase(
        id="add s2",
        cli=_cli(
            "add_s2_column_impl",
            core_s2.add_s2_column,
            lambda c: ["add", "s2", c.input_file, c.output_file],
            module=cli_add,
        ),
        ops=_ops("add_s2_table", core_s2.add_s2_table, lambda c: ops.add_s2(c.table)),
        table=_table(
            core_s2, "add_s2_table", core_s2.add_s2_table, lambda c: c.gpio_table.add_s2()
        ),
        normalize={
            "column_name": ("s2_column_name", "s2_column_name"),
            "level": ("s2_level", "level"),
        },
    ),
    ParityCase(
        id="add a5",
        cli=_cli(
            "add_a5_column_impl",
            core_a5.add_a5_column,
            lambda c: ["add", "a5", c.input_file, c.output_file],
            module=cli_add,
        ),
        ops=_ops("add_a5_table", core_a5.add_a5_table, lambda c: ops.add_a5(c.table)),
        table=_table(
            core_a5, "add_a5_table", core_a5.add_a5_table, lambda c: c.gpio_table.add_a5()
        ),
        normalize={
            "column_name": ("a5_column_name", "a5_column_name"),
            "resolution": ("a5_resolution", "resolution"),
        },
    ),
    ParityCase(
        id="add quadkey",
        cli=_cli(
            "add_quadkey_column_impl",
            core_quadkey.add_quadkey_column,
            lambda c: ["add", "quadkey", c.input_file, c.output_file],
            module=cli_add,
        ),
        ops=_ops(
            "add_quadkey_table",
            core_quadkey.add_quadkey_table,
            lambda c: ops.add_quadkey(c.table),
        ),
        table=_table(
            core_quadkey,
            "add_quadkey_table",
            core_quadkey.add_quadkey_table,
            lambda c: c.gpio_table.add_quadkey(),
        ),
        normalize={
            "column_name": ("quadkey_column_name", "quadkey_column_name"),
            "resolution": ("resolution", "resolution"),
            "use_centroid": ("use_centroid", "use_centroid"),
        },
    ),
    ParityCase(
        id="add kdtree",
        cli=_cli(
            "add_kdtree_column_impl",
            core_kdtree.add_kdtree_column,
            lambda c: ["add", "kdtree", c.input_file, c.output_file],
            module=cli_add,
        ),
        ops=_ops(
            "add_kdtree_table",
            core_kdtree.add_kdtree_table,
            lambda c: ops.add_kdtree(c.table, auto=True),
        ),
        table=_table(
            core_kdtree,
            "add_kdtree_table",
            core_kdtree.add_kdtree_table,
            lambda c: c.gpio_table.add_kdtree(auto=True),
        ),
        notes=(
            "`auto=True` is supplied on the API side because that is what the bare CLI "
            "call selects: `gpio add kdtree in out` with no flags falls back to auto mode "
            "(iterations=None, auto_target_rows=('rows', 120000)). The API no longer "
            "guesses 512 partitions when neither is named -- it refuses, like the "
            "resolution-bearing indices after #800 (#813).",
        ),
        normalize={
            "column_name": ("kdtree_column_name", "kdtree_column_name"),
            "iterations": ("iterations", "iterations"),
            "auto_target_rows": ("auto_target_rows", "auto_target_rows"),
            "sample_size": ("sample_size", "sample_size"),
        },
    ),
    ParityCase(
        id="sort hilbert",
        cli=_cli(
            "hilbert_impl",
            core_hilbert.hilbert_order,
            lambda c: ["sort", "hilbert", c.input_file, c.output_file],
            module=cli_sort,
        ),
        ops=_ops(
            "hilbert_order_table",
            core_hilbert.hilbert_order_table,
            lambda c: ops.sort_hilbert(c.table),
        ),
        table=_table(
            core_hilbert,
            "hilbert_order_table",
            core_hilbert.hilbert_order_table,
            lambda c: c.gpio_table.sort_hilbert(),
        ),
        normalize={"geometry_column": ("geometry_column", "geometry_column")},
    ),
    ParityCase(
        id="sort str",
        cli=_cli(
            "str_impl",
            core_str.str_order,
            lambda c: ["sort", "str", c.input_file, c.output_file],
            module=cli_sort,
        ),
        ops=_ops(
            "str_order_table",
            core_str.str_order_table,
            lambda c: ops.sort_str(c.table),
        ),
        table=_table(
            core_str,
            "str_order_table",
            core_str.str_order_table,
            lambda c: c.gpio_table.sort_str(),
        ),
        normalize={
            "geometry_column": ("geometry_column", "geometry_column"),
            "tile_size": ("row_group_rows", "tile_size"),
        },
    ),
    ParityCase(
        id="sort column",
        cli=_cli(
            "sort_by_column_impl",
            core_sort_column.sort_by_column,
            lambda c: ["sort", "column", c.input_file, c.output_file, SORT_COLUMN],
            module=cli_sort,
        ),
        ops=_ops(
            "sort_by_column_table",
            core_sort_column.sort_by_column_table,
            lambda c: ops.sort_column(c.table, column=SORT_COLUMN),
        ),
        table=_table(
            core_sort_column,
            "sort_by_column_table",
            core_sort_column.sort_by_column_table,
            lambda c: c.gpio_table.sort_column(column_name=SORT_COLUMN),
        ),
        normalize={
            "columns": ("columns", "columns"),
            "descending": ("descending", "descending"),
        },
    ),
    ParityCase(
        id="sort quadkey",
        cli=_cli(
            "sort_by_quadkey_impl",
            core_sort_quadkey.sort_by_quadkey,
            lambda c: ["sort", "quadkey", c.input_file, c.output_file],
            module=cli_sort,
        ),
        ops=_ops(
            "sort_by_quadkey_table",
            core_sort_quadkey.sort_by_quadkey_table,
            lambda c: ops.sort_quadkey(c.table),
        ),
        table=_table(
            core_sort_quadkey,
            "sort_by_quadkey_table",
            core_sort_quadkey.sort_by_quadkey_table,
            lambda c: c.gpio_table.sort_quadkey(),
        ),
        normalize={
            "column_name": ("quadkey_column_name", "quadkey_column_name"),
            "resolution": ("resolution", "resolution"),
            "use_centroid": ("use_centroid", "use_centroid"),
            "remove_column": ("remove_quadkey_column", "remove_quadkey_column"),
        },
    ),
    ParityCase(
        id="extract geoparquet",
        cli=_cli(
            "extract_impl",
            core_extract.extract,
            lambda c: ["extract", "geoparquet", c.input_file, c.output_file],
            module=cli_extract,
        ),
        ops=_ops("extract_table", core_extract.extract_table, lambda c: ops.extract(c.table)),
        table=_table(
            core_extract,
            "extract_table",
            core_extract.extract_table,
            lambda c: c.gpio_table.extract(),
        ),
        normalize={
            # The CLI takes comma-joined strings and the API takes lists, but the
            # *default* on both sides means "every column", so the values are
            # comparable as long as neither side invents a non-empty default.
            "columns": ("include_cols", "columns"),
            "exclude_columns": ("exclude_cols", "exclude_columns"),
            "bbox": ("bbox", "bbox"),
            "where": ("where", "where"),
            "limit": ("limit", "limit"),
            "repair_geometry": ("repair_geometry", "repair_geometry"),
        },
    ),
    ParityCase(
        id="convert geoparquet",
        cli=_cli(
            "convert_to_geoparquet",
            convert_to_geoparquet,
            lambda c: ["convert", "geoparquet", c.convert_input, c.output_file],
            module=cli_convert,
        ),
        # `Table.write` has no core twin of `convert_to_geoparquet`: it resolves a
        # write strategy and calls `strategy.write_from_table`. That call *is* the
        # API's core write boundary, and is what the write facade will replace.
        table=Frontend(
            patch_site=(core_write_strategies.WriteStrategyFactory, "get_strategy"),
            reference=None,
            invoke=lambda c: c.gpio_table.write(c.output_file),
            install=_install_write_strategy,
        ),
        normalize={
            "compression": ("compression", "compression"),
            "compression_level": ("compression_level", "compression_level"),
            "row_group_size_mb": ("row_group_size_mb", "row_group_size_mb"),
            "row_group_rows": ("row_group_rows", "row_group_rows"),
        },
        notes=(
            "geoparquet_version is deliberately not compared: the CLI forwards the "
            "unresolved None ('auto') and lets convert_to_geoparquet decide, while "
            "Table.write resolves auto to a concrete version before it reaches the "
            "write boundary. Both mean 'auto'; the values cannot be equal here.",
        ),
    ),
    ParityCase(
        id="partition h3",
        cli=_cli(
            "partition_by_h3_impl",
            core_part_h3.partition_by_h3,
            lambda c: ["partition", "h3", c.input_file, c.output_dir, "--resolution", "6"],
            module=cli_partition,
        ),
        ops=_ops_via_table(
            core_part_h3,
            "partition_by_h3",
            core_part_h3.partition_by_h3,
            lambda c: ops.partition_by_h3(c.table, c.output_dir, resolution=6),
        ),
        table=_table(
            core_part_h3,
            "partition_by_h3",
            core_part_h3.partition_by_h3,
            lambda c: c.gpio_table.partition_by_h3(c.output_dir, resolution=6),
        ),
        notes=(
            "A resolution is supplied on all three sides because no front end accepts a "
            "call without one: the CLI errors from core, and the API now errors in front "
            "of the temp-file write. `ops.partition_by_h3` inherits that gate by "
            "delegating to `Table.partition_by_h3`, so it has to be called with a "
            "resolution too. Every other knob is still left at its default.",
        ),
        normalize={
            "column_name": ("h3_column_name", "h3_column_name"),
            "resolution": ("resolution", "resolution"),
            "auto": ("auto", "auto"),
            "target_rows": ("target_rows", "target_rows"),
            "max_partitions": ("max_partitions", "max_partitions"),
            "hive": ("hive", "hive"),
            "keep_column": ("keep_h3_column", "keep_h3_column"),
            "overwrite": ("overwrite", "overwrite"),
            "compression": ("compression", "compression"),
        },
    ),
    ParityCase(
        id="partition quadkey",
        cli=_cli(
            "partition_by_quadkey_impl",
            core_part_quadkey.partition_by_quadkey,
            lambda c: [
                "partition",
                "quadkey",
                c.input_file,
                c.output_dir,
                "--resolution",
                "13",
                "--partition-resolution",
                "6",
            ],
            module=cli_partition,
        ),
        ops=_ops_via_table(
            core_part_quadkey,
            "partition_by_quadkey",
            core_part_quadkey.partition_by_quadkey,
            lambda c: ops.partition_by_quadkey(
                c.table, c.output_dir, resolution=13, partition_resolution=6
            ),
        ),
        table=_table(
            core_part_quadkey,
            "partition_by_quadkey",
            core_part_quadkey.partition_by_quadkey,
            lambda c: c.gpio_table.partition_by_quadkey(
                c.output_dir, resolution=13, partition_resolution=6
            ),
        ),
        notes=(
            "Both resolutions are supplied on all three sides because no front end accepts "
            "a call without them: the CLI errors from core, and the API now errors in front "
            "of the temp-file write. `ops.partition_by_quadkey` inherits that gate by "
            "delegating to `Table.partition_by_quadkey`, so it has to be called with both "
            "resolutions too. Every other knob is still left at its default.",
        ),
        normalize={
            "column_name": ("quadkey_column_name", "quadkey_column_name"),
            "resolution": ("resolution", "resolution"),
            "partition_resolution": ("partition_resolution", "partition_resolution"),
            "auto": ("auto", "auto"),
            "target_rows": ("target_rows", "target_rows"),
            "max_partitions": ("max_partitions", "max_partitions"),
            "hive": ("hive", "hive"),
            "keep_column": ("keep_quadkey_column", "keep_quadkey_column"),
            "overwrite": ("overwrite", "overwrite"),
            "compression": ("compression", "compression"),
        },
    ),
]

CASES_BY_ID = {case.id: case for case in CASES}


# --------------------------------------------------------------------------
# Known gaps -- findings, not pinned behaviour. Every entry needs a reason.
# --------------------------------------------------------------------------

# Keys are (case id, front end, canonical name, repr(CLI value), repr(API value)),
# following `collect_divergences()` in tests/test_cli_api_default_parity.py. The two
# reprs are load-bearing: an allowlist that recorded only "these differ" would stay
# green if `ops.sort_str` changed `tile_size` from 50000 to 40000 -- still a divergence,
# but a *different* one that nobody reviewed. Pinning the pair means any change to
# either side has to come back through this table.
KNOWN_PARITY_GAPS: dict[tuple[str, str, str, str, str], str] = {
    (
        "sort hilbert",
        "ops",
        "geometry_column",
        "'geometry'",
        "None",
    ): (
        "CLI --geometry-column defaults to the conventional name 'geometry'; ops.sort_hilbert "
        "passes None so core auto-detects. Table.sort_hilbert does not have this gap because a "
        "Table already knows its geometry column. Also listed in the #661 allowlist."
    ),
    (
        "sort str",
        "ops",
        "geometry_column",
        "'geometry'",
        "None",
    ): (
        "CLI --geometry-column defaults to the conventional name 'geometry'; ops.sort_str "
        "passes None so core auto-detects. Table.sort_str already knows its geometry column."
    ),
    (
        "sort str",
        "ops",
        "tile_size",
        "None",
        str(DEFAULT_ROW_GROUP_ROWS),
    ): (
        f"The file core resolves row_group_rows=None to the sort default of "
        f"{DEFAULT_ROW_GROUP_ROWS:,} rows (DEFAULT_ROW_GROUP_ROWS); the in-memory API "
        f"spells that same effective default explicitly as tile_size={DEFAULT_ROW_GROUP_ROWS}."
    ),
    (
        "sort str",
        "table",
        "tile_size",
        "None",
        str(DEFAULT_ROW_GROUP_ROWS),
    ): (
        f"The file core resolves row_group_rows=None to the sort default of "
        f"{DEFAULT_ROW_GROUP_ROWS:,} rows (DEFAULT_ROW_GROUP_ROWS); the fluent API "
        f"spells that same effective default explicitly as tile_size={DEFAULT_ROW_GROUP_ROWS}."
    ),
    (
        "convert geoparquet",
        "table",
        "row_group_rows",
        "None",
        str(DEFAULT_ROW_GROUP_ROWS),
    ): (
        f"Measurement depth, not behaviour -- the same gap this case's notes already "
        f"concede for geoparquet_version. Both front ends resolve row_group_rows through "
        f"parquet_writer.resolve_row_group_rows and both land on {DEFAULT_ROW_GROUP_ROWS:,}, "
        f"but the two boundaries this case patches sit on opposite sides of that call: the "
        f"CLI's is convert_to_geoparquet, which runs *before* write_parquet_with_metadata "
        f"resolves, and the table's is WriteStrategyFactory.get_strategy, which runs *after* "
        f"Table.write resolves. tests/test_write_facade_row_groups.py asserts the thing this "
        f"case cannot see: that the two front ends write the same row-group layout."
    ),
}


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


@pytest.fixture
def parity_ctx(places_test_file, geojson_input, tmp_path):
    """A real input file, a real table, and somewhere to write."""
    import pyarrow.parquet as pq

    return Ctx(
        input_file=places_test_file,
        convert_input=geojson_input,
        output_file=str(tmp_path / "out.parquet"),
        output_dir=str(tmp_path / "out_dir"),
        table=pq.read_table(places_test_file),
        gpio_table=table_module.read(places_test_file),
        runner=CliRunner(),
    )


# --------------------------------------------------------------------------
# Harness
# --------------------------------------------------------------------------


def _capture(frontend: Frontend, ctx: Ctx) -> dict[str, Any]:
    """Invoke one front end with defaults and return the call it made to core."""
    ctx.cli_result = None
    recorder = _Recorder(frontend.reference, frontend.result)
    if frontend.install is not None:
        patcher = frontend.install(frontend, recorder)
    else:
        target, attribute = frontend.patch_site
        patcher = patch.object(target, attribute, recorder)

    with patcher:
        frontend.invoke(ctx)

    if len(recorder.calls) != 1:
        target, attribute = frontend.patch_site
        detail = ""
        if ctx.cli_result is not None:
            detail = f"\nCLI exit={ctx.cli_result.exit_code} output={ctx.cli_result.output!r}"
        raise AssertionError(
            f"expected exactly one call to {target.__name__ if hasattr(target, '__name__') else target}"
            f".{attribute}, recorded {len(recorder.calls)}{detail}"
        )
    return recorder.calls[0]


def _mismatches(
    case: ParityCase, side: str, cli_call: dict, api_call: dict
) -> dict[str, tuple[str, str]]:
    """Diff one front end against the CLI over the case's canonical parameters.

    Returns ``{canonical name: (repr(CLI value), repr(API value))}`` for the knobs
    that disagree. The values -- not just the fact of disagreement -- are what the
    allowlist matches on.
    """
    out: dict[str, tuple[str, str]] = {}
    for canonical, (cli_name, api_name) in sorted(case.normalize.items()):
        cli_value = cli_call.get(cli_name, MISSING)
        api_value = api_call.get(api_name, MISSING)
        assert cli_value is not MISSING, (
            f"{case.id}: normalize maps {canonical!r} to CLI parameter {cli_name!r}, "
            f"which the CLI never passed (got {sorted(cli_call)})"
        )
        assert api_value is not MISSING, (
            f"{case.id}: normalize maps {canonical!r} to API parameter {api_name!r}, "
            f"which {side} never passed (got {sorted(api_call)})"
        )
        if cli_value != api_value or isinstance(cli_value, bool) != isinstance(api_value, bool):
            out[canonical] = (repr(cli_value), repr(api_value))
    return out


def _render(case: ParityCase, side: str, canonical: str, pair: tuple[str, str]) -> str:
    cli_name, api_name = case.normalize[canonical]
    return f"CLI {cli_name}={pair[0]} vs {side} {api_name}={pair[1]}"


def _known_pairs(case_id: str, side: str) -> dict[str, tuple[str, str]]:
    """The value pairs the allowlist expects for one (case, front end)."""
    return {
        key[2]: (key[3], key[4])
        for key in KNOWN_PARITY_GAPS
        if key[0] == case_id and key[1] == side
    }


def _assert_parity(case: ParityCase, side: str, ctx: Ctx) -> None:
    cli_call = _capture(case.cli, ctx)
    frontend = getattr(case, side)
    api_call = _capture(frontend, ctx)

    found = _mismatches(case, side, cli_call, api_call)
    known = _known_pairs(case.id, side)

    # A knob is acceptable only if it diverges *exactly* the way the allowlist
    # recorded. A gap whose values shifted is a new, unreviewed divergence.
    unexpected = {name: pair for name, pair in found.items() if known.get(name) != pair}
    lines = []
    for name, pair in sorted(unexpected.items()):
        rendered = _render(case, side, name, pair)
        if name in known:
            was = known[name]
            lines.append(
                f"  {name}: {rendered}\n"
                f"    (KNOWN_PARITY_GAPS still records CLI={was[0]} vs {side} {was[1]}; "
                f"the divergence changed -- re-review it and update the entry)"
            )
        else:
            lines.append(f"  {name}: {rendered}")
    # Surface the case's caveats: they usually explain why a knob looks unequal.
    caveats = "".join(f"\nNote: {note}" for note in case.notes)
    assert not unexpected, (
        f"`gpio {case.id}` and its {side} twin now hand core different values.\n"
        f"Either align them, or add/update an entry in KNOWN_PARITY_GAPS with a "
        f"justification:\n" + "\n".join(lines) + caveats
    )


OPS_CASES = [case.id for case in CASES if case.ops is not None]
TABLE_CASES = [case.id for case in CASES]


@pytest.mark.parametrize("case_id", OPS_CASES)
def test_cli_and_ops_pass_the_same_values_to_core(case_id, parity_ctx):
    """`gpio <cmd>` and `ops.<fn>` must hand core the same option values."""
    _assert_parity(CASES_BY_ID[case_id], "ops", parity_ctx)


@pytest.mark.parametrize("case_id", TABLE_CASES)
def test_cli_and_table_pass_the_same_values_to_core(case_id, parity_ctx):
    """`gpio <cmd>` and `Table.<method>` must hand core the same option values."""
    _assert_parity(CASES_BY_ID[case_id], "table", parity_ctx)


@pytest.mark.parametrize("gap", sorted(KNOWN_PARITY_GAPS), ids=lambda g: " / ".join(g[:3]))
def test_known_parity_gap_still_diverges_the_same_way(gap, parity_ctx):
    """Each recorded gap must still diverge, and diverge with the recorded values.

    Two ways this earns its keep. A gap fixed in the code would otherwise sit in
    the allowlist forever, silently re-permitting the divergence later. And a gap
    whose values merely *shifted* -- `ops.sort_str` going from `tile_size=50000`
    to `40000` -- is a different divergence that nobody reviewed, so pinning only
    "these differ" would let it through unnoticed.
    """
    case_id, side, canonical, cli_repr, api_repr = gap
    case = CASES_BY_ID[case_id]
    cli_call = _capture(case.cli, parity_ctx)
    api_call = _capture(getattr(case, side), parity_ctx)
    found = _mismatches(case, side, cli_call, api_call)

    assert canonical in found, (
        f"`gpio {case_id}` and its {side} twin now agree on {canonical!r}. "
        f"Remove this entry from KNOWN_PARITY_GAPS so the parity is enforced."
    )
    assert found[canonical] == (cli_repr, api_repr), (
        f"`gpio {case_id}` / {side} still diverges on {canonical!r}, but not the way "
        f"the allowlist records.\n"
        f"  recorded: CLI={cli_repr} vs {side}={api_repr}\n"
        f"  actual:   {_render(case, side, canonical, found[canonical])}\n"
        f"Re-review the divergence and update the KNOWN_PARITY_GAPS entry."
    )


def test_every_known_gap_has_a_justification():
    for gap, reason in KNOWN_PARITY_GAPS.items():
        assert reason and reason.strip(), f"No justification recorded for {gap}"


def test_every_known_gap_names_a_real_case_and_parameter():
    """A typo in the allowlist would silently disable a comparison."""
    for case_id, side, canonical, _cli_repr, _api_repr in KNOWN_PARITY_GAPS:
        assert case_id in CASES_BY_ID, f"{case_id!r} is not a parity case"
        case = CASES_BY_ID[case_id]
        assert side in ("ops", "table"), f"{side!r} is not a front end"
        assert getattr(case, side) is not None, f"{case_id!r} has no {side} front end"
        assert canonical in case.normalize, (
            f"{canonical!r} is not a canonical parameter of {case_id!r}"
        )


def test_case_table_covers_the_commands_the_refactors_touch():
    """Guard against a case being dropped while the harness still looks healthy."""
    assert set(CASES_BY_ID) == {
        "add bbox",
        "add h3",
        "add s2",
        "add a5",
        "add quadkey",
        "add kdtree",
        "sort hilbert",
        "sort str",
        "sort column",
        "sort quadkey",
        "extract geoparquet",
        "convert geoparquet",
        "partition h3",
        "partition quadkey",
    }
