"""Call parity: every front end hands core the same values for the same knob.

This module deliberately knows *call structure*: it patches the core function
each front end imports and compares the keyword arguments the front ends hand
down. It began as scaffolding for the write facade (#664); the facade landed in
`core/parquet_writer.py` and did not make it redundant, because the three doors
of most commands (`gpio add h3`, `ops.add_h3`, `Table.add_h3`) still meet only
inside core. This harness is what notices when one door starts passing a
different resolution, column name or codec than the others; the *answers* the
doors give are compared in `tests/test_api_cli_behaviour_parity.py`.

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
module diffs *values actually delivered to core* -- which catches the cases
introspection cannot see, such as the CLI deriving `iterations` from
`--partitions` or upper-casing `--compression` on the way down.

The census
----------
Every CLI leaf is either a `ParityCase` or an entry in `NO_CALL_PARITY_CASE`
with a written reason; `test_every_cli_leaf_has_a_parity_case_or_a_recorded_reason`
fails on a new command that is neither.

Patching note (project memory): dotted-string `mock.patch("...cli.main.X")`
targets fail on Python 3.10. Every patch here is `patch.object(module, name)`,
which is unaffected -- the trap is about string targets only.

Running note: this file mocks core out, so it covers very little of the package
and running it *alone* trips the coverage floor. Use `--no-cov` when running
it in isolation; in the full suite it is a non-issue.
"""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from geoparquet_io.api import ops
from geoparquet_io.api import stac as api_stac
from geoparquet_io.api import table as table_module
from geoparquet_io.cli import main as cli_main
from geoparquet_io.cli.commands import add as cli_add
from geoparquet_io.cli.commands import check as cli_check
from geoparquet_io.cli.commands import convert as cli_convert
from geoparquet_io.cli.commands import extract as cli_extract
from geoparquet_io.cli.commands import partition as cli_partition
from geoparquet_io.cli.commands import process as cli_process
from geoparquet_io.cli.commands import publish as cli_publish
from geoparquet_io.cli.commands import sort as cli_sort
from geoparquet_io.core import benchmark as core_benchmark
from geoparquet_io.core import check_optimization as core_check_optimization
from geoparquet_io.core import check_parquet_structure as core_check_structure
from geoparquet_io.core import check_spatial_order as core_check_spatial
from geoparquet_io.core import extract as core_extract
from geoparquet_io.core import hilbert_order as core_hilbert
from geoparquet_io.core import pmtiles as core_pmtiles
from geoparquet_io.core import pmtiles_pyramid as core_pmtiles_pyramid
from geoparquet_io.core import sort_by_column as core_sort_column
from geoparquet_io.core import sort_quadkey as core_sort_quadkey
from geoparquet_io.core import stac as core_stac
from geoparquet_io.core import str_order as core_str
from geoparquet_io.core import upload as core_upload
from geoparquet_io.core import validate as core_validate
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
from geoparquet_io.core.process import overview as core_overview
from geoparquet_io.core.process.aggregate import by_a5 as core_agg_a5
from geoparquet_io.core.process.aggregate import by_admin as core_agg_admin
from geoparquet_io.core.process.aggregate import by_h3 as core_agg_h3

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
    normalize: dict[str, tuple[str, str]]
    table: Frontend | None = None
    ops: Frontend | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.table is None and self.ops is None:
            raise ValueError(f"{self.id}: a parity case needs at least one API front end")


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
    tmp_path: Path | None = None
    cli_result: Any = None

    def run_cli(self, args: list[str]):
        self.cli_result = self.runner.invoke(cli_main.cli, args)
        return self.cli_result

    def scratch(self, name: str) -> str:
        """A per-front-end output path.

        Several commands refuse to overwrite an existing output, and both front
        ends of a case run against the same `Ctx`. Without a distinct path the
        second invocation would be rejected by that guard rather than reaching
        core.
        """
        return str(self.tmp_path / name)


# --------------------------------------------------------------------------
# Front-end constructors
# --------------------------------------------------------------------------


def _cli(
    alias: str,
    reference: Callable,
    args: Callable[[Ctx], list[str]],
    module: Any = cli_main,
    wrap: Callable[[], AbstractContextManager[Any]] = contextlib.nullcontext,
) -> Frontend:
    """A CLI front end; `wrap` holds a context manager open around the invocation."""

    def _invoke(ctx: Ctx):
        with wrap():
            return ctx.run_cli(args(ctx))

    return Frontend(patch_site=(module, alias), reference=reference, invoke=_invoke)


def _with_credentials():
    """`gpio publish upload` runs `check_credentials` before `upload_impl` (even under
    `--dry-run`); it fails on any machine without AWS credentials, CI included."""
    return patch.object(cli_publish, "check_credentials", lambda *_a, **_k: (True, ""))


def _no_network():
    """Fail closed: if the `upload` patch ever misses, the store is never built."""
    return patch.object(
        core_upload,
        "_setup_store_and_kwargs",
        side_effect=AssertionError("Table.upload reached the network"),
    )


def _ops(attr: str, reference: Callable, call: Callable[[Ctx], Any]) -> Frontend:
    return Frontend(
        patch_site=(ops, attr),
        reference=reference,
        invoke=call,
        result=ECHO_FIRST_ARG,
    )


def _table(
    module: Any,
    attr: str,
    reference: Callable,
    call: Callable[[Ctx], Any],
    result: Any = ECHO_FIRST_ARG,
) -> Frontend:
    return Frontend(
        patch_site=(module, attr),
        reference=reference,
        invoke=call,
        result=result,
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
STAC_BUCKET = "s3://example-bucket/data/"


# An empty but real `ValidationResult`, so `Table.validate` can unpack what the
# recorder hands back; the call is what the case compares.
_STUB_VALIDATION_RESULT = core_validate.ValidationResult(
    file_path="", detected_version=None, target_version=None
)


def _fail_closed(guard: Callable[[], AbstractContextManager[Any]], call: Callable[[], Any]) -> Any:
    with guard():
        return call()


def _swallow(call: Callable[[], Any]) -> Any:
    """Run an API front end whose read-back of the (recorded, never written) core
    output raises; `_capture` still asserts the call happened exactly once."""
    try:
        return call()
    except FileNotFoundError:
        return None


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
    # ----------------------------------------------------------------------
    # WP-7 (#1018): the seven command groups that had no call-parity case.
    #
    # `check`, `benchmark`, `process`, `pmtiles` and `publish` are added here.
    # `inspect` and `skills` cannot be: see `NO_CALL_PARITY_CASE` below.
    #
    # A recurring shape in this batch: `verbose` and `quiet` are deliberately
    # absent from every `normalize` map. They are rendering knobs, and the API
    # is *required* to differ -- a library call must not print a check report to
    # stdout. What these cases pin is the knobs that change the answer.
    # ----------------------------------------------------------------------
    ParityCase(
        id="check all",
        cli=_cli(
            "check_structure_impl",
            core_check_structure.check_all,
            lambda c: ["check", "all", c.input_file],
            module=cli_check,
        ),
        table=_table(
            core_check_structure,
            "check_all",
            core_check_structure.check_all,
            lambda c: c.gpio_table.check(),
        ),
        normalize={
            "return_results": ("return_results", "return_results"),
            "profile": ("profile", "profile"),
        },
        notes=(
            "`Table.check()` takes no `profile` argument at all, so it rides the core "
            "default. That matches `gpio check all` only because the CLI's --profile also "
            "defaults to None; if either default moves, this case fails.",
        ),
    ),
    ParityCase(
        id="check spatial",
        cli=_cli(
            "check_spatial_impl",
            core_check_spatial.check_spatial_order,
            lambda c: ["check", "spatial", c.input_file],
            module=cli_check,
        ),
        table=_table(
            core_check_spatial,
            "check_spatial_order",
            core_check_spatial.check_spatial_order,
            lambda c: c.gpio_table.check_spatial(),
        ),
        normalize={
            "sample_size": ("random_sample_size", "random_sample_size"),
            "limit_rows": ("limit_rows", "limit_rows"),
            "return_results": ("return_results", "return_results"),
        },
    ),
    ParityCase(
        id="check compression",
        cli=_cli(
            "check_compression",
            core_check_structure.check_compression,
            lambda c: ["check", "compression", c.input_file],
            module=core_check_structure,
        ),
        table=_table(
            core_check_structure,
            "check_compression",
            core_check_structure.check_compression,
            lambda c: c.gpio_table.check_compression(),
        ),
        normalize={"return_results": ("return_results", "return_results")},
        notes=(
            "`gpio check compression` imports its core function inside the command body, "
            "so the CLI's patch site is the core module -- the same object the Table "
            "front end patches. The two are still captured in separate `patch.object` "
            "scopes, one per invocation.",
        ),
    ),
    ParityCase(
        id="check bbox",
        cli=_cli(
            "check_metadata_and_bbox",
            core_check_structure.check_metadata_and_bbox,
            lambda c: ["check", "bbox", c.input_file],
            module=core_check_structure,
        ),
        table=_table(
            core_check_structure,
            "check_metadata_and_bbox",
            core_check_structure.check_metadata_and_bbox,
            lambda c: c.gpio_table.check_bbox(),
        ),
        normalize={"return_results": ("return_results", "return_results")},
    ),
    ParityCase(
        id="check row-group",
        cli=_cli(
            "check_row_groups",
            core_check_structure.check_row_groups,
            lambda c: ["check", "row-group", c.input_file],
            module=core_check_structure,
        ),
        table=_table(
            core_check_structure,
            "check_row_groups",
            core_check_structure.check_row_groups,
            lambda c: c.gpio_table.check_row_groups(),
        ),
        normalize={
            "return_results": ("return_results", "return_results"),
            "profile": ("profile", "profile"),
        },
    ),
    ParityCase(
        id="check optimization",
        cli=_cli(
            "check_optimization",
            core_check_optimization.check_optimization,
            lambda c: ["check", "optimization", c.input_file],
            module=core_check_optimization,
        ),
        table=_table(
            core_check_optimization,
            "check_optimization",
            core_check_optimization.check_optimization,
            lambda c: c.gpio_table.check_optimization(),
        ),
        normalize={"return_results": ("return_results", "return_results")},
    ),
    ParityCase(
        id="check spec",
        cli=_cli(
            "validate_geoparquet",
            core_validate.validate_geoparquet,
            lambda c: ["check", "spec", c.input_file],
            module=core_validate,
        ),
        table=_table(
            core_validate,
            "validate_geoparquet",
            core_validate.validate_geoparquet,
            lambda c: c.gpio_table.validate(),
            result=_STUB_VALIDATION_RESULT,
        ),
        normalize={
            "target_version": ("target_version", "target_version"),
            "validate_data": ("validate_data", "validate_data"),
            "sample_size": ("sample_size", "sample_size"),
        },
        notes=(
            "`--sample-size` (1000) and `--skip-data-validation` (off) are the two knobs "
            "that change which rows are inspected; `Table.validate()` hard-codes both. "
            "A move on either side changes what the API is willing to call valid.",
        ),
    ),
    ParityCase(
        id="benchmark explain",
        cli=_cli(
            "explain_analyze",
            core_benchmark.explain_analyze,
            lambda c: ["benchmark", "explain", c.input_file],
            module=core_benchmark,
        ),
        ops=Frontend(
            patch_site=(core_benchmark, "explain_analyze"),
            reference=core_benchmark.explain_analyze,
            invoke=lambda c: ops.explain_analyze(c.input_file),
            result={},
        ),
        table=Frontend(
            patch_site=(core_benchmark, "explain_analyze"),
            reference=core_benchmark.explain_analyze,
            invoke=lambda c: table_module.Table.explain_analyze(c.input_file),
            result={},
        ),
        normalize={"query": ("query", "query")},
        notes=(
            "`benchmark` is file-centric on all three sides -- `Table.explain_analyze` is "
            "a static method taking a path, not a table -- so both API front ends are "
            "spelled out rather than built with `_ops`/`_table`.",
        ),
    ),
    ParityCase(
        id="process overview",
        cli=_cli(
            "create_overviews_impl",
            core_overview.create_overviews,
            lambda c: ["process", "overview", c.input_file, "--output-dir", c.scratch("ov")],
            module=cli_process,
        ),
        ops=Frontend(
            patch_site=(core_overview, "create_overviews"),
            reference=core_overview.create_overviews,
            invoke=lambda c: ops.create_overviews(c.input_file, output_dir=c.scratch("ov")),
            result={},
        ),
        normalize={
            "levels": ("levels", "levels"),
            "max_tile_kb": ("max_tile_kb", "max_tile_kb"),
            "bytes_per_cell": ("bytes_per_cell", "bytes_per_cell"),
            "cell_column": ("cell_column", "cell_column"),
            "scheme": ("scheme", "scheme"),
            "output_dir": ("output_dir", "output_dir"),
            "compression": ("compression", "compression"),
            "compression_level": ("compression_level", "compression_level"),
        },
        notes=(
            "`Table.overview()` rolls a table up to *one* level and is a different "
            "operation; `ops.create_overviews` is the command's twin.",
        ),
    ),
    ParityCase(
        id="process aggregate h3",
        cli=_cli(
            "aggregate_by_h3_impl",
            core_agg_h3.aggregate_by_h3,
            lambda c: [
                "process",
                "aggregate",
                "h3",
                c.input_file,
                c.scratch("agg_h3.parquet"),
                "--resolution",
                "6",
            ],
            module=cli_process,
        ),
        ops=_ops_via_table(
            core_agg_h3,
            "aggregate_h3_table",
            core_agg_h3.aggregate_h3_table,
            lambda c: ops.aggregate_h3(c.table, resolution=6),
        ),
        table=_table(
            core_agg_h3,
            "aggregate_h3_table",
            core_agg_h3.aggregate_h3_table,
            lambda c: c.gpio_table.aggregate_h3(resolution=6),
        ),
        normalize={
            "resolution": ("resolution", "resolution"),
            "metric": ("metric", "metric"),
            "metric_nodata": ("metric_nodata", "metric_nodata"),
            "breakdown": ("breakdown", "breakdown"),
            "breakdown_limit": ("breakdown_limit", "breakdown_limit"),
            "out_geometry": ("out_geometry", "out_geometry"),
            "column_name": ("h3_column_name", "h3_column_name"),
            "where": ("where", "where"),
            "bucket_point": ("bucket_point", "bucket_point"),
        },
        notes=(
            "A resolution is supplied on all three sides: the command's --resolution "
            "defaults to None (meaning 'use --auto'), while `aggregate_h3_table` requires "
            "a concrete int. Naming it on both sides keeps the comparison about the "
            "remaining knobs rather than about that one asymmetry.",
        ),
    ),
    ParityCase(
        id="process aggregate a5",
        cli=_cli(
            "aggregate_by_a5_impl",
            core_agg_a5.aggregate_by_a5,
            lambda c: [
                "process",
                "aggregate",
                "a5",
                c.input_file,
                c.scratch("agg_a5.parquet"),
                "--resolution",
                "6",
            ],
            module=cli_process,
        ),
        ops=_ops_via_table(
            core_agg_a5,
            "aggregate_a5_table",
            core_agg_a5.aggregate_a5_table,
            lambda c: ops.aggregate_a5(c.table, resolution=6),
        ),
        table=_table(
            core_agg_a5,
            "aggregate_a5_table",
            core_agg_a5.aggregate_a5_table,
            lambda c: c.gpio_table.aggregate_a5(resolution=6),
        ),
        normalize={
            "resolution": ("resolution", "resolution"),
            "metric": ("metric", "metric"),
            "metric_nodata": ("metric_nodata", "metric_nodata"),
            "breakdown": ("breakdown", "breakdown"),
            "breakdown_limit": ("breakdown_limit", "breakdown_limit"),
            "out_geometry": ("out_geometry", "out_geometry"),
            "column_name": ("a5_column_name", "a5_column_name"),
            "where": ("where", "where"),
            "bucket_point": ("bucket_point", "bucket_point"),
        },
    ),
    ParityCase(
        id="process aggregate admin",
        cli=_cli(
            "aggregate_by_admin_impl",
            core_agg_admin.aggregate_by_admin,
            lambda c: [
                "process",
                "aggregate",
                "admin",
                c.input_file,
                c.scratch("agg_admin.parquet"),
            ],
            module=cli_process,
        ),
        ops=Frontend(
            patch_site=(core_agg_admin, "aggregate_by_admin"),
            reference=core_agg_admin.aggregate_by_admin,
            invoke=lambda c: _swallow(lambda: ops.aggregate_admin(c.table)),
        ),
        table=Frontend(
            patch_site=(core_agg_admin, "aggregate_by_admin"),
            reference=core_agg_admin.aggregate_by_admin,
            invoke=lambda c: _swallow(lambda: c.gpio_table.aggregate_admin()),
        ),
        normalize={
            "level": ("level", "level"),
            "metric": ("metric", "metric"),
            "metric_nodata": ("metric_nodata", "metric_nodata"),
            "breakdown": ("breakdown", "breakdown"),
            "breakdown_limit": ("breakdown_limit", "breakdown_limit"),
            "out_geometry": ("out_geometry", "out_geometry"),
            "where": ("where", "where"),
            "dataset": ("dataset", "dataset"),
        },
        notes=(
            "`ops.aggregate_admin` is file-centric under the hood: it writes the table to "
            "a temp file, calls the same `aggregate_by_admin` the command does, and reads "
            "the result back. With core recorded rather than run there is no result to "
            "read, so the read-back is swallowed -- the recorded call is what this case "
            "is about. No network: the admin boundary download lives inside the core "
            "function that never runs here.",
        ),
    ),
    ParityCase(
        id="pmtiles create",
        cli=_cli(
            "create_pmtiles_from_geoparquet",
            core_pmtiles.create_pmtiles_from_geoparquet,
            lambda c: ["pmtiles", "create", c.input_file, c.scratch("cli.pmtiles")],
            module=core_pmtiles,
        ),
        ops=Frontend(
            patch_site=(core_pmtiles, "create_pmtiles_from_geoparquet"),
            reference=core_pmtiles.create_pmtiles_from_geoparquet,
            invoke=lambda c: ops.create_pmtiles(c.input_file, c.scratch("api.pmtiles")),
        ),
        normalize={
            "layer": ("layer", "layer"),
            "min_zoom": ("min_zoom", "min_zoom"),
            "max_zoom": ("max_zoom", "max_zoom"),
            "bbox": ("bbox", "bbox"),
            "where": ("where", "where"),
            "include_cols": ("include_cols", "include_cols"),
            "precision": ("precision", "precision"),
            "src_crs": ("src_crs", "src_crs"),
            "attribution": ("attribution", "attribution"),
            "layer_by_column": ("layer_by_column", "layer_by_column"),
            "repair_geometry": ("repair_geometry", "repair_geometry"),
            "maximum_tile_bytes": ("maximum_tile_bytes", "maximum_tile_bytes"),
            "simplify_only_low_zooms": ("simplify_only_low_zooms", "simplify_only_low_zooms"),
            "no_simplification_of_shared_nodes": (
                "no_simplification_of_shared_nodes",
                "no_simplification_of_shared_nodes",
            ),
            "no_tile_size_limit": ("no_tile_size_limit", "no_tile_size_limit"),
            "drop_densest_as_needed": ("drop_densest_as_needed", "drop_densest_as_needed"),
            "force": ("force", "force"),
        },
        notes=("A PMTiles archive is not a table, so there is no `Table` door.",),
    ),
    ParityCase(
        id="pmtiles pyramid",
        cli=_cli(
            "create_pmtiles_pyramid",
            core_pmtiles_pyramid.create_pmtiles_pyramid,
            lambda c: ["pmtiles", "pyramid", c.input_file, c.scratch("cli_pyr.pmtiles")],
            module=core_pmtiles_pyramid,
        ),
        ops=Frontend(
            patch_site=(core_pmtiles_pyramid, "create_pmtiles_pyramid"),
            reference=core_pmtiles_pyramid.create_pmtiles_pyramid,
            invoke=lambda c: ops.create_pmtiles_pyramid(c.input_file, c.scratch("api_pyr.pmtiles")),
        ),
        normalize={
            "levels": ("levels", "levels"),
            "max_tile_kb": ("max_tile_kb", "max_tile_kb"),
            "bytes_per_cell": ("bytes_per_cell", "bytes_per_cell"),
            "layer_mode": ("layer_mode", "layer_mode"),
            "include_features": ("include_features", "include_features"),
            "features_source": ("features_source", "features_source"),
            "features_min_zoom": ("features_min_zoom", "features_min_zoom"),
            "max_zoom": ("max_zoom", "max_zoom"),
            "attribution": ("attribution", "attribution"),
            "force": ("force", "force"),
        },
    ),
    ParityCase(
        id="publish upload",
        cli=_cli(
            "upload_impl",
            core_upload.upload,
            lambda c: ["publish", "upload", c.input_file, "s3://bucket/prefix/"],
            module=cli_publish,
            wrap=_with_credentials,
        ),
        table=Frontend(
            patch_site=(core_upload, "upload"),
            reference=core_upload.upload,
            invoke=lambda c: _fail_closed(
                _no_network, lambda: c.gpio_table.upload("s3://bucket/prefix/")
            ),
        ),
        normalize={
            "destination": ("destination", "destination"),
            "profile": ("profile", "profile"),
            "chunk_concurrency": ("chunk_concurrency", "chunk_concurrency"),
            "s3_endpoint": ("s3_endpoint", "s3_endpoint"),
            "s3_region": ("s3_region", "s3_region"),
            "s3_use_ssl": ("s3_use_ssl", "s3_use_ssl"),
        },
        notes=(
            "`dry_run` is not pinned: `Table.upload` has no such keyword (#1063, "
            "`test_table_upload_cannot_be_rehearsed`). `pattern`, `max_files`, "
            "`chunk_size` and `fail_fast` describe walking a directory and have no "
            "analogue on a Table.",
        ),
    ),
    ParityCase(
        id="publish stac",
        cli=_cli(
            "generate_stac_item",
            core_stac.generate_stac_item,
            lambda c: [
                "publish",
                "stac",
                c.input_file,
                c.scratch("cli_stac.json"),
                "--bucket",
                STAC_BUCKET,
            ],
            module=core_stac,
        ),
        ops=Frontend(
            patch_site=(core_stac, "generate_stac_item"),
            reference=core_stac.generate_stac_item,
            invoke=lambda c: api_stac.generate_stac(
                c.input_file, c.scratch("api_stac.json"), bucket=STAC_BUCKET
            ),
            result={},
        ),
        normalize={
            "bucket": ("bucket_prefix", "bucket_prefix"),
            "public_url": ("public_url", "public_url"),
            "item_id": ("item_id", "item_id"),
        },
        notes=("The twin is `geoparquet_io.generate_stac` in `api/stac.py` (#1065).",),
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
def parity_ctx(places_test_file, geojson_input, tmp_path, monkeypatch):
    """A real input file, a real table, and somewhere to write.

    The CLI's S3 activation reads the ambient AWS environment (#1055) and
    `Table.upload` does not, so a developer's exported profile would read as a
    parity failure; the variables are cleared for the case.
    """
    import pyarrow.parquet as pq

    for name in ("AWS_PROFILE", "AWS_ENDPOINT_URL", "AWS_REGION", "AWS_DEFAULT_REGION"):
        monkeypatch.delenv(name, raising=False)
    return Ctx(
        input_file=places_test_file,
        convert_input=geojson_input,
        output_file=str(tmp_path / "out.parquet"),
        output_dir=str(tmp_path / "out_dir"),
        table=pq.read_table(places_test_file),
        gpio_table=table_module.read(places_test_file),
        runner=CliRunner(),
        tmp_path=tmp_path,
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
TABLE_CASES = [case.id for case in CASES if case.table is not None]


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


# --------------------------------------------------------------------------
# Census -- a new CLI leaf must arrive with a parity case or an excuse
# --------------------------------------------------------------------------

_NO_SEAM_INSPECT = (
    "No shared seam: the CLI goes through `core.inspect`, the Table method is inline "
    "pyarrow/DuckDB and calls no core function. Compared behaviourally in "
    "tests/test_api_cli_behaviour_parity.py::TestInspectGroupAgreesWithTable."
)
_ONE_DOOR = (
    "The CLI and the API call the same core function with just the path (#1065): one "
    "door, nothing to diff."
)
_NETWORK = "Reaches a live service; the network lane covers it."
_DEFERRED = "Worth a case; not written yet (#1085)."

NO_CALL_PARITY_CASE: dict[str, str] = {
    # --- no API twin at all (`NO_API_TWIN` minus the three #1065 mislabels) --
    "benchmark compare": "No API twin: diffs two benchmark JSON runs for a human.",
    "benchmark report": "No API twin: renders collected benchmark results for a human.",
    "benchmark suite": "No API twin: orchestrates a multi-command benchmark run.",
    "skills": "No API twin: prints the bundled LLM skill documents.",
    # --- a twin exists, but the two front ends share no call seam ----------
    **dict.fromkeys(
        ["inspect summary", "inspect head", "inspect tail", "inspect stats", "inspect meta"],
        _NO_SEAM_INSPECT,
    ),
    "inspect layers": _ONE_DOOR,
    "check stac": _ONE_DOOR,
    "add geometry-metrics": (
        "Compared behaviourally in tests/test_api_cli_behaviour_parity.py::"
        "TestAddGeometryMetricsParity, on the computed values rather than the call."
    ),
    # --- a twin and a seam, but not reachable offline --------------------
    **dict.fromkeys(["extract arcgis", "extract wfs"], _NETWORK),
    # --- deferred ---------------------------------------------------------
    **dict.fromkeys(
        [
            "extract carto",
            "extract bigquery",
            "add admin-divisions",
            "partition admin",
            "convert reproject",
            "convert geojson",
            "convert geopackage",
            "convert flatgeobuf",
            "convert csv",
            "convert shapefile",
            "partition string",
            "partition kdtree",
            "partition s2",
            "partition a5",
            "add bbox-metadata",
        ],
        _DEFERRED,
    ),
}


def _cli_leaves() -> set[str]:
    from tests.conftest import walk_cli_commands

    return {" ".join(path) for path, _cmd in walk_cli_commands(cli_main.cli)}


def test_every_cli_leaf_has_a_parity_case_or_a_recorded_reason():
    """Left: leaves with neither a case nor an excuse. Right: excuses that are stale
    (no such command) or dead (the command has a case)."""
    assert _cli_leaves() - set(CASES_BY_ID) == set(NO_CALL_PARITY_CASE)


def test_every_command_group_is_represented():
    """A group with no case at all is the state WP-7 set out to fix."""
    groups_without_a_case = {"inspect", "skills"}  # no shared seam / no twin

    covered = {case_id.split()[0] for case_id in CASES_BY_ID}
    all_groups = {name.split()[0] for name in _cli_leaves()}
    assert all_groups - covered - groups_without_a_case == set()


@pytest.mark.xfail(strict=True, reason="gpio #1063: Table.upload has no dry_run")
def test_table_upload_cannot_be_rehearsed():
    assert "dry_run" in inspect.signature(table_module.Table.upload).parameters
