"""The one CLI/API parity registry (#1088): data plus the twin resolver.

Every exception to "the CLI and the Python API are one surface" (ADR-0004)
lives here, with its reason. `tests/test_parity_registry.py` is the census that
reads it; `tests/test_cli_api_default_parity.py` (declared defaults) and
`tests/test_cli_api_call_parity_scaffold.py` (values handed to core) read
`KNOWN_DIVERGENCES` and `KNOWN_PARITY_GAPS` from here.

To register a new twin, case or exemption:

* a twin whose name the resolver cannot derive -> `TWIN_NAMES`
* a leaf that lacks a twin, a functional twin or a call case -> `EXEMPT`
* a parameter only one side has, outside `ONE_SIDED_CLASSES` -> `ONE_SIDED`

Every check is a set equality, so an entry made stale by a fix fails too.
Not a test module: nothing here is collected.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from functools import cache

import click

import geoparquet_io
from geoparquet_io.api import ops
from geoparquet_io.api.table import Table
from geoparquet_io.cli import decorators
from geoparquet_io.cli.main import cli
from geoparquet_io.core.parquet_writer import DEFAULT_ROW_GROUP_ROWS
from tests.conftest import walk_cli_commands

# --------------------------------------------------------------------------
# Twin resolution
# --------------------------------------------------------------------------

#: Twins whose names the resolver cannot derive from the command path.
TWIN_NAMES: dict[str, list[str]] = {
    "extract wfs": ["from_wfs", "from_wfs_layers"],
    "extract arcgis": ["from_arcgis"],
    "extract carto": ["from_carto"],
    "extract bigquery": ["read_bigquery", "from_bigquery"],
    "extract geoparquet": ["extract"],
    "pmtiles create": ["create_pmtiles"],
    "pmtiles pyramid": ["create_pmtiles_pyramid"],
    "process overview": ["create_overviews", "overview"],
    "publish upload": ["upload"],
    "publish stac": ["generate_stac"],
    "inspect meta": ["metadata"],
    "inspect layers": ["list_layers"],
    "benchmark explain": ["explain_analyze"],
    "convert reproject": ["reproject"],
    # `Table.fix` applies the fixes `--fix` does, selected by its `checks=` (#1078).
    # Not `Table.check`: it runs the structure checks only; `gpio.check_all` is the twin.
    "check all": ["fix"],
    "check bbox": ["fix"],
    "check compression": ["fix"],
    # `Table.check_spatial` still resolves by name, but it is spatial order alone;
    # `gpio.check_spatial` adds the pushdown block `check spatial` judges, and is the twin.
    "check spatial": ["fix"],
    "check spec": ["validate"],
    "check stac": ["validate_stac"],
    "check row-group": ["check_row_groups", "fix"],
    "check optimization": ["check_optimization"],
    "inspect summary": ["info"],
    "convert geoparquet": ["write", "convert"],
    # `gpio partition <index>` is two operations behind one command: a single
    # file, and a *directory* walked with --min-size, which has its own `ops`
    # twin (#811).
    "partition a5": ["partition_by_a5", "sub_partition_by_a5"],
    "partition h3": ["partition_by_h3", "sub_partition_by_h3"],
    "partition s2": ["partition_by_s2", "sub_partition_by_s2"],
    "partition quadkey": ["partition_by_quadkey", "sub_partition_by_quadkey"],
}


@cache
def _commands() -> dict[str, click.Command]:
    return {" ".join(path): cmd for path, cmd in walk_cli_commands(cli)}


def cli_leaves() -> set[str]:
    """Every leaf of the live command tree, spelled ``"group cmd"``."""
    return set(_commands())


def _candidate_names(leaf: str) -> list[str]:
    parts = [p.replace("-", "_") for p in leaf.split()]
    group = parts[0] if len(parts) > 1 else ""
    name = parts[-1]
    out = [
        *TWIN_NAMES.get(leaf, []),
        "_".join(parts),
        name,
        f"{group}_by_{name}" if group else "",
        f"convert_to_{name}",
        f"to_{name}",
        f"{name}_{group}" if group else "",
        f"check_{name}" if group == "check" else "",
        "_".join(parts[1:]),
        "_".join(parts[-2:]),
    ]
    return [c for c in out if c]


def _root(name: str):
    # Only names the package exports: its lazy `__getattr__` imports on access.
    return getattr(geoparquet_io, name) if name in geoparquet_io.__all__ else None


_CONTAINERS: tuple[tuple[str, Callable[[str], object]], ...] = (
    ("ops", lambda name: getattr(ops, name, None)),
    ("Table", lambda name: getattr(Table, name, None)),
    ("gpio", _root),
)


def api_twins(leaf: str) -> list[tuple[str, Callable]]:
    """``[(label, callable)]`` for a leaf's `ops.*`, `Table.*` and root `gpio.*` twins."""
    found: list[tuple[str, Callable]] = []
    seen: set[int] = set()
    for cand in _candidate_names(leaf):
        for prefix, lookup in _CONTAINERS:
            fn = lookup(cand)
            identity = id(getattr(fn, "__func__", fn))
            if inspect.isroutine(fn) and identity not in seen:
                seen.add(identity)
                found.append((f"{prefix}.{cand}", fn))
    return found


def is_functional(label: str) -> bool:
    """The functional leg of the API: an `ops.*` function or a root `gpio.*` one."""
    return label.startswith(("ops.", "gpio."))


# --------------------------------------------------------------------------
# EXEMPT[leaf][axis] -- a leaf without a twin, a functional twin, or a case
# --------------------------------------------------------------------------

AXES = ("api_twin", "ops_twin", "call_case")

_INSPECTION_NOT_A_TABLE = (
    "Inspection: renders or returns a description of a table (metadata, a preview, "
    "column statistics) rather than transforming one, so an `ops.<fn>(table) -> table` "
    "twin would have nothing to return. `Table` is the reviewed home for it."
)
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
_BENCHMARK = (
    "Benchmarking is a CLI reporting workflow, not a data operation: there is no "
    "GeoParquet input/output to hang a Table method or ops function off."
)

#: A leaf with no API twin carries only `api_twin`; the other two axes are moot.
EXEMPT: dict[str, dict[str, str]] = {
    "benchmark compare": {"api_twin": _BENCHMARK + " `compare` diffs two runs for a human."},
    "benchmark report": {"api_twin": _BENCHMARK + " `report` renders collected results."},
    "benchmark suite": {
        "api_twin": _BENCHMARK + " `suite` orchestrates a run; an API caller composes "
        "the operations directly."
    },
    "skills": {"api_twin": "Prints the bundled LLM skill documents; no data operation."},
    **{
        f"inspect {c}": {"ops_twin": _INSPECTION_NOT_A_TABLE, "call_case": _NO_SEAM_INSPECT}
        for c in ("head", "meta", "summary", "tail")
    },
    "inspect stats": {
        "ops_twin": _INSPECTION_NOT_A_TABLE,
        "call_case": "Both doors call `core.inspect_utils.compute_column_statistics` (#1087), "
        "but on different sources (the CLI's file, the Table's registered Arrow table), so "
        "there is no one call to compare. Both outputs are pinned in "
        "tests/test_column_statistics_snapshot.py.",
    },
    "publish upload": {
        "ops_twin": "Uploads a file already on disk; the unit of work is a path, not the "
        "in-memory table an `ops` function receives. `Table.upload` writes first."
    },
    "inspect layers": {"call_case": _ONE_DOOR},
    "check stac": {"call_case": _ONE_DOOR},
    "add geometry-metrics": {
        "call_case": "Compared behaviourally in tests/test_api_cli_behaviour_parity.py::"
        "TestAddGeometryMetricsParity, on the computed values rather than the call."
    },
    **{leaf: {"call_case": _NETWORK} for leaf in ("extract arcgis", "extract wfs")},
}

#: A command group with no call-parity case at all, which is allowed only when
#: every leaf in it is exempt from `call_case` (directly, or through `api_twin`).
#: Recorded here as well, so losing a group's last case is a decision, not a drift.
GROUPS_WITHOUT_A_CALL_CASE: dict[str, str] = {
    "inspect": _NO_SEAM_INSPECT,
    "skills": "The group is one leaf with no API twin.",
}


# --------------------------------------------------------------------------
# KNOWN_DIVERGENCES -- declared-default mismatches (test_cli_api_default_parity.py)
# --------------------------------------------------------------------------
#
# What is left here is deliberate, not deferred work: two groups where the CLI
# and the API legitimately spell the same runtime behaviour with different
# defaults, because one front end is handed a path and the other a live Table.
# Do not "fix" them by aligning the literals -- that would change behaviour.
#
# A divergence that is a *bug* does not belong here. The partition
# auto-resolution entries used to live in this dict claiming the CLI
# auto-calculated a resolution when left unset; it never did -- it refused, and
# it was the API that silently guessed (H3 9, quadkey 13/6, S2 13, A5 15). That
# was closed in #762 by giving the API the same `auto=True` the CLI has, not by
# writing a better excuse.

_KEEP_TRISTATE = (
    "Intentional. CLI flag False and API None both mean 'follow hive' at runtime; the API spells "
    "it as a tri-state so it can also express an explicit drop, which a bare Click flag cannot."
)
_KDTREE_AUTO_SPELLING = (
    "Intentional. The KD-tree commands spell auto mode as `--auto N` -- one int option carrying "
    "the target row count, unset meaning 'not asked for'. The API splits that into the `auto` "
    "bool and `target_rows` the other index methods already take (#800), so `auto=True` reads "
    "the same across every partition method. `auto=True` and `--auto` deliver the identical "
    "auto_target_rows=('rows', 120000) to core, which is what "
    "tests/test_cli_api_call_parity_scaffold.py's 'add kdtree' case compares."
)

_ENCODING_IS_TWO_KNOBS = (
    "Intentional. The two `encoding` parameters are homonyms, not one default spelled twice. "
    "`gpio convert geoparquet --encoding` names the *source* text encoding (a shapefile DBF "
    "without .cpg, a Latin-1 CSV), unset meaning 'trust the driver'; its API twin is "
    "`gpio.convert(encoding=)`, where the default is the same None. `Table.write(encoding=)` "
    "is the GDAL *output* encoding for Shapefile/GeoPackage writes, which Parquet output "
    "never uses, so `convert geoparquet` has no output-encoding knob at all."
)

_SIMPLIFY_TRISTATE = (
    "The CLI declares None so it can reject --preserve-topology/--simplify-boundary "
    "in the mode they do not apply to; it maps None to the API's True before core."
)

KNOWN_DIVERGENCES: dict[tuple[str, str, str, str, str], str] = {
    (
        "process simplify",
        "ops.simplify",
        "preserve_topology",
        "'<unset>'",
        "True",
    ): _SIMPLIFY_TRISTATE,
    (
        "process simplify",
        "ops.simplify",
        "simplify_boundary",
        "'<unset>'",
        "True",
    ): _SIMPLIFY_TRISTATE,
    (
        "process simplify",
        "Table.simplify",
        "preserve_topology",
        "'<unset>'",
        "True",
    ): _SIMPLIFY_TRISTATE,
    (
        "process simplify",
        "Table.simplify",
        "simplify_boundary",
        "'<unset>'",
        "True",
    ): _SIMPLIFY_TRISTATE,
    (
        "convert geoparquet",
        "Table.write",
        "encoding",
        "'<unset>'",
        "'UTF-8'",
    ): _ENCODING_IS_TWO_KNOBS,
    ("add kdtree", "ops.add_kdtree", "auto", "'<unset>'", "False"): _KDTREE_AUTO_SPELLING,
    ("add kdtree", "Table.add_kdtree", "auto", "'<unset>'", "False"): _KDTREE_AUTO_SPELLING,
    (
        "partition kdtree",
        "ops.partition_by_kdtree",
        "auto",
        "'<unset>'",
        "False",
    ): _KDTREE_AUTO_SPELLING,
    (
        "partition kdtree",
        "Table.partition_by_kdtree",
        "auto",
        "'<unset>'",
        "False",
    ): _KDTREE_AUTO_SPELLING,
    (
        "partition a5",
        "ops.partition_by_a5",
        "keep_a5_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
    (
        "partition h3",
        "ops.partition_by_h3",
        "keep_h3_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
    (
        "partition kdtree",
        "ops.partition_by_kdtree",
        "keep_kdtree_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
    (
        "partition quadkey",
        "ops.partition_by_quadkey",
        "keep_quadkey_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
    (
        "partition s2",
        "ops.partition_by_s2",
        "keep_s2_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
    (
        "partition a5",
        "Table.partition_by_a5",
        "keep_a5_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
    (
        "partition h3",
        "Table.partition_by_h3",
        "keep_h3_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
    (
        "partition kdtree",
        "Table.partition_by_kdtree",
        "keep_kdtree_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
    (
        "partition quadkey",
        "Table.partition_by_quadkey",
        "keep_quadkey_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
    (
        "partition s2",
        "Table.partition_by_s2",
        "keep_s2_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
}


# --------------------------------------------------------------------------
# KNOWN_PARITY_GAPS -- values handed to core (test_cli_api_call_parity_scaffold.py)
# --------------------------------------------------------------------------

# Keys are (case id, front end, canonical name, repr(CLI value), repr(API value)),
# following `KNOWN_DIVERGENCES` above. The two
# reprs are load-bearing: an allowlist that recorded only "these differ" would stay
# green if `ops.sort_str` changed `tile_size` from 50000 to 40000 -- still a divergence,
# but a *different* one that nobody reviewed. Pinning the pair means any change to
# either side has to come back through this table.
KNOWN_PARITY_GAPS: dict[tuple[str, str, str, str, str], str] = {
    (
        "publish upload",
        "table",
        "destination",
        "'s3://bucket/prefix/'",
        "'s3://bucket/prefix/places_test.parquet'",
    ): (
        "A prefix destination names the object after the source file. The CLI uploads that "
        "file, so core names it; Table.upload uploads a scratch file whose name is generated "
        "(a source basename may hold ':' or '*', or a URL query), so it resolves the key "
        "itself and hands core the full one. Same object key: TestTableUpload pins it."
    ),
    (
        "sort hilbert",
        "table",
        "geometry_column",
        "None",
        "'geometry'",
    ): (
        "CLI --geometry-column defaults to None, the file's primary geometry column "
        "(geometry_detection.primary_geometry_column, #1367 T3c); Table.sort_hilbert passes the "
        "column the Table already tracks. Same column, resolved where each front end knows it."
    ),
    (
        "sort str",
        "table",
        "geometry_column",
        "None",
        "'geometry'",
    ): (
        "CLI --geometry-column defaults to None, the file's primary geometry column "
        "(geometry_detection.primary_geometry_column, #1367 T3c); Table.sort_str passes the "
        "column the Table already tracks. Same column, resolved where each front end knows it."
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
    (
        "process simplify",
        "table",
        "geometry_column",
        "None",
        "'geometry'",
    ): (
        "CLI --geometry-column defaults to None, the file's primary geometry column; "
        "Table.simplify passes the column the Table already tracks. Same column, resolved "
        "where each front end knows it -- the `sort hilbert` gap, one command over."
    ),
}


# --------------------------------------------------------------------------
# One-sided parameters (#903) -- classes first, reasons for the remainder
# --------------------------------------------------------------------------

#: CLI-only plumbing (I/O paths, output formatting) with no API analogue.
CLI_PLUMBING = frozenset(
    {
        "help",
        "verbose",
        "quiet",
        "output",
        "input_file",
        "output_file",
        "dry_run",
        "show_sql",
        "json",
        "json_output",
        "yes",
        "input",
        "url",
        "no_color",
    }
)

#: One knob, two names: the CLI option name -> the API keyword. A `no_*` CLI
#: flag here is the negation of its API keyword.
API_SPELLING = {
    "row_group_size": "row_group_rows",
    "write_memory": "memory_limit",
    "include_cols": "columns",
    "exclude_cols": "exclude_columns",
    "geoparquet_version": "version",
    "stac_geoparquet_version": "version",
    "wfs_version": "version",
    "count": "n",
    "workers": "max_workers",
    "random_sample_size": "sample_size",
    "src_crs": "source_crs",
    "dst_crs": "target_crs",
    "remove_quadkey_column": "remove_column",
    "bbox_name": "bbox_column",
    "no_mask": "use_mask",
    "no_bbox": "include_bbox",
    "no_wkt": "include_wkt",
    "no_vecorel": "vecorel",
    "no_rs": "rs",
    "s3_no_ssl": "s3_use_ssl",
    "fix_output": "output",
}


def _declared(*decorated: Callable) -> frozenset[str]:
    """The parameter names a set of Click option decorators declares."""
    names: set[str] = set()
    for decorate in decorated:
        names |= {p.name for p in click.command()(decorate(lambda **_: None)).params}
    return frozenset(names)


def _params(fn: Callable) -> set[str]:
    return set(inspect.signature(fn).parameters) - {"self", "cls", "table"}


_WRITE_OPTIONS = _declared(
    decorators.output_format_options,
    decorators.geoparquet_version_option,
    decorators.write_strategy_option,
    decorators.overwrite_option,
)
_TABLE_WRITE = _params(Table.write)
# `--prefix` shapes the output's file names; it is a gap, recorded below.
_PARTITION_RUN = _declared(decorators.partition_options) - _declared(decorators.prefix_option)
_CLI_GUARDS = _declared(decorators.any_extension_option, decorators.strict_option)
_MULTI_FILE_CHECK = _declared(decorators.check_partition_options)
_READ_PARTITION = _params(geoparquet_io.read_partition)

ONE_SIDED_CLASSES: dict[str, str] = {
    "cli_plumbing": (
        "CLI plumbing: an operand passed as a Click argument, a hidden alias of a global "
        "option, output formatting or exit-status policy, or the --any-extension output "
        "guard. The API receives paths and tables as arguments and returns values."
    ),
    "multi_file_check": (
        "Selects which files of a partitioned dataset a check reads. A Table is one table."
    ),
    "lives_on_read": (
        "Reads a directory or glob of files. The API reads a dataset with "
        "`gpio.read_partition(...)` before the chain starts."
    ),
    "partition_run": (
        "Partition run control from `partition_options` (preview, preview limit, analysis "
        "override, in-place, size threshold): the CLI plans around the write; each "
        "`partition_by_*` exposes the subset its core honours."
    ),
    "lives_on_write": (
        "A standard write option. The API twin returns a Table and the knob lives on "
        "`Table.write`, the chain's terminal step."
    ),
    "operand": "A required API argument: the operand the CLI takes as a Click argument.",
    "write_on_api": (
        "A `Table.write` parameter: the API twin writes a Table itself (`Table.write` "
        "serves every format; `Table.upload` writes before it uploads)."
    ),
}


def _returns_table(fn: Callable) -> bool:
    return "Table" in str(inspect.signature(fn).return_annotation)


def _spellings(param: click.Parameter) -> set[str]:
    names = {param.name, API_SPELLING.get(param.name, param.name)}
    if getattr(param, "callback", None) is decorators._validate_column_name:
        names.add("column_name")  # every `--<index>-name` option
    return names


def _cli_class(leaf: str, param: click.Parameter, twins: list[tuple[str, Callable]]) -> str | None:
    if (
        param.name in CLI_PLUMBING
        or isinstance(param, click.Argument)
        or getattr(param, "hidden", False)
        or param.name in _CLI_GUARDS
    ):
        return "cli_plumbing"
    if param.name in _MULTI_FILE_CHECK:
        return "multi_file_check"
    if param.name in _READ_PARTITION:
        return "lives_on_read"
    if leaf.startswith("partition ") and param.name in _PARTITION_RUN:
        return "partition_run"
    if (
        param.name in _WRITE_OPTIONS
        and _spellings(param) & _TABLE_WRITE
        and any(_returns_table(fn) for _label, fn in twins)
    ):
        return "lives_on_write"
    return None


def _api_class(name: str, fn: Callable) -> str | None:
    if inspect.signature(fn).parameters[name].default is inspect.Parameter.empty:
        return "operand"
    if name in _TABLE_WRITE:
        return "write_on_api"
    return None


def one_sided(side: str) -> set[tuple[str, str, str]]:
    """``(leaf, side, param)`` for every parameter only that side has, minus the classes."""
    out: set[tuple[str, str, str]] = set()
    for leaf, cmd in _commands().items():
        twins = api_twins(leaf)
        if not twins:
            continue
        api = {name: fn for _label, fn in reversed(twins) for name in _params(fn)}
        if side == "cli":
            out |= {
                (leaf, side, p.name)
                for p in cmd.params
                if not _spellings(p) & api.keys() and _cli_class(leaf, p, twins) is None
            }
            continue
        cli_names = {name for p in cmd.params for name in _spellings(p)}
        out |= {
            (leaf, side, name)
            for name, fn in api.items()
            if name not in cli_names and _api_class(name, fn) is None
        }
    return out


def sub_partition_knobs() -> dict[str, set[str]]:
    """Per index, the `partition <idx>` options that `ops.sub_partition_by_<idx>` shares.

    The CLI forwards them to core through ``**run``, which no signature shows, so
    `TestSubPartitionFrontDoorParity` must set each one. `min_size` selects the mode
    and `preview` takes another branch; neither reaches the patched core call.
    """
    out: dict[str, set[str]] = {}
    for leaf, cmd in _commands().items():
        twin = dict(api_twins(leaf)).get(f"ops.sub_partition_by_{leaf.split()[-1]}")
        if twin is not None:
            shared = {p.name for p in cmd.params if p.name in _params(twin)}
            out[leaf.split()[-1]] = shared - {"min_size", "preview", "verbose"}
    return out


_PARTITIONS = tuple(
    f"partition {i}" for i in ("a5", "admin", "h3", "kdtree", "quadkey", "s2", "string")
)
_CHECKS_WITH_FIX = tuple(
    f"check {c}" for c in ("all", "bbox", "compression", "row-group", "spatial")
)

_PARTITION_WRITE_OPTIONS = (
    "The partition API writes its directory itself and exposes compression only; the "
    "GeoParquet version, row-group sizing and memory limit take the defaults. Taking a "
    "`WriteOptions` there is follow-up work after #761, which gave it to `Table.write`."
)
_CHECK_FIX = (
    "--fix selects the repair and --no-backup guards an in-place one. The API calls "
    "`Table.fix`, which returns a new Table and never rewrites its input (#1078)."
)
_CHECK_PMTILES = (
    "A second operation `check all` runs on request; the API runs it on the file: "
    "`ops.create_pmtiles(...)`."
)
_FIX_CHECKS = (
    "`Table.fix` serves every `check` leaf with --fix; `checks=` picks which checks' fixes "
    "run, where the CLI picks by command."
)
_RENDERING = (
    "Presentation: how the CLI prints the answer (markdown, truncation, which metadata "
    "sections, a report format). The API returns the data itself."
)
_CHAIN_STEP = (
    "A second operation the CLI runs on request (--add-bbox) or by default (bbox, Hilbert "
    "sort); the API leaves it to the chain: `.add_bbox()`, `.sort_hilbert()`."
)
_KDTREE_SPELLING = (
    "KD-tree depth is spelled two ways: --partitions/--approx/--exact on the CLI, "
    "iterations/sample_size/target_rows on the API (#800). The call-parity 'add kdtree' "
    "case pins what reaches core."
)
_PROMPT = "Answers or triggers an interactive prompt (confirmation, cache purge); a library call never prompts."
_DIRECTORY_UPLOAD = (
    "Directory upload controls: the CLI uploads a file or a tree; `Table.upload` uploads the "
    "one file it writes."
)
_GEOMETRY_COLUMN = (
    "The CLI reads the file's primary geometry column (`primary_geometry_column`); a Table "
    "can come from anywhere, so the API takes an explicit override."
)
_OPTIONAL_OUTPUT = (
    "The API returns the result when no output path is given, so the path is an optional "
    "keyword; the CLI takes it as an argument."
)
_GAP = (
    "#903 finding: the twin does not take this knob yet. Add the keyword to the twin and "
    "delete this entry."
)


def _each(side: str, reason: str, leaves, params) -> dict[tuple[str, str, str], str]:
    leaves = (leaves,) if isinstance(leaves, str) else leaves
    params = (params,) if isinstance(params, str) else params
    return {(leaf, side, param): reason for leaf in leaves for param in params}


#: The remainder: one-sided parameters no class explains, each with its reason.
ONE_SIDED: dict[tuple[str, str, str], str] = {
    **_each(
        "cli",
        _PARTITION_WRITE_OPTIONS,
        _PARTITIONS,
        ("geoparquet_version", "row_group_size", "row_group_size_mb", "write_memory"),
    ),
    **_each("cli", _GAP, _PARTITIONS, "prefix"),
    **_each("cli", _CHECK_FIX, _CHECKS_WITH_FIX, ("fix", "no_backup")),
    **_each("cli", _CHECK_PMTILES, "check all", "pmtiles"),
    **_each("api", _FIX_CHECKS, _CHECKS_WITH_FIX, "checks"),
    **_each("cli", _RENDERING, ("inspect head", "inspect tail"), ("max_columns", "no_truncate")),
    **_each(
        "cli",
        _RENDERING,
        ("inspect head", "inspect stats", "inspect summary", "inspect tail"),
        "markdown_output",
    ),
    **_each(
        "cli",
        _RENDERING,
        "inspect meta",
        (
            "meta_geo_stats",
            "meta_geoparquet",
            "meta_parquet",
            "meta_parquet_geo",
            "meta_row_groups",
        ),
    ),
    **_each("cli", _RENDERING, "check all", "spec_details"),
    **_each("cli", _RENDERING, "benchmark explain", "output_format"),
    **_each("api", _RENDERING, "inspect meta", "include_parquet_metadata"),
    **_each("cli", _CHAIN_STEP, ("add admin-divisions", "sort hilbert", "sort str"), "add_bbox"),
    **_each(
        "cli",
        _CHAIN_STEP,
        ("convert geoparquet", "extract arcgis", "extract carto"),
        "skip_hilbert",
    ),
    **_each("cli", _CHAIN_STEP, ("extract arcgis", "extract carto"), "skip_bbox"),
    **_each(
        "cli",
        _KDTREE_SPELLING,
        ("add kdtree", "partition kdtree"),
        ("approx", "exact", "partitions"),
    ),
    **_each(
        "api", _KDTREE_SPELLING, ("add kdtree", "partition kdtree"), ("iterations", "target_rows")
    ),
    **_each("api", _KDTREE_SPELLING, "add kdtree", "sample_size"),
    **_each("cli", _PROMPT, "add kdtree", "force"),
    **_each("cli", _PROMPT, "add admin-divisions", "clear_cache"),
    **_each(
        "cli",
        _DIRECTORY_UPLOAD,
        "publish upload",
        ("chunk_size", "fail_fast", "max_files", "pattern"),
    ),
    **_each(
        "api",
        _GEOMETRY_COLUMN,
        (
            *(f"add {c}" for c in ("a5", "bbox", "bbox-metadata", "h3", "kdtree", "quadkey", "s2")),
            "convert geoparquet",
            "convert reproject",
            "extract geoparquet",
            *_PARTITIONS,
            "process aggregate a5",
            "process aggregate h3",
        ),
        "geometry_column",
    ),
    **_each("api", _OPTIONAL_OUTPUT, ("convert geojson", "publish stac"), "output_path"),
    **_each("api", _OPTIONAL_OUTPUT, "add stac-metadata", "output_file"),
    **_each(
        "api",
        "Carto's API key: the CLI reads CARTO_API_KEY from the environment, as the API does "
        "when `api_key` is left unset.",
        "extract carto",
        "api_key",
    ),
    **_each(
        "api",
        "`Table.sort_str` spells the CLI's --row-group-size as `tile_size`; KNOWN_PARITY_GAPS "
        "pins the effective default on both sides.",
        "sort str",
        "tile_size",
    ),
    **_each("cli", _GAP, ("convert csv", "convert flatgeobuf", "convert geojson"), "overwrite"),
    **_each("cli", _GAP, "convert geojson", ("description", "keep_crs", "no_seq", "pretty")),
    **_each("cli", _GAP, "convert geoparquet", ("allow_no_geometry", "crs", "csv_max_line_size")),
    **_each("cli", _GAP, "extract geoparquet", ("geometry", "skip_count", "use_first_geometry")),
    **_each("cli", _GAP, "extract wfs", ("bbox_mode", "output_crs", "sort_by")),
    **_each("cli", _GAP, "add bbox", "force"),
    **_each("cli", _GAP, "add admin-divisions", "no_cache"),
    **_each(
        "cli",
        _GAP,
        ("process aggregate a5", "process aggregate h3"),
        ("auto", "max_cells", "target_per_cell"),
    ),
    **_each("cli", _GAP, "process overview", ("cell_detail", "explicit_gsd", "overview_out")),
    **_each("cli", _GAP, "process simplify", "refresh_metrics"),
    **_each("cli", _GAP, "partition kdtree", "kdtree_name"),
}
