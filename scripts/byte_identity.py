#!/usr/bin/env python3
"""Run one command matrix in two worktrees and compare the outputs byte for byte.

The per-track proof harness of #1367 (Method 3): output stays byte-identical to
the base except for the approved changes, and a track shows it by running the
same fixture set through every write path on the base and on its branch.

Each worktree runs with its own virtualenv, so the base's package and the
branch's never mix::

    UV_PROJECT_ENVIRONMENT=<wt>/.venv uv run --no-sync --project <wt> gpio ...
    UV_PROJECT_ENVIRONMENT=<wt>/.venv uv run --no-sync --project <wt> python -c ...

(``--no-sync`` so a comparison never edits either environment.) Both sides read
the same copies of the inputs and write into their own output directory; then
every output is compared. A Parquet output that differs gets a footer-level
diff -- per-column codec, row groups and their row counts, the KV key set, the
``geo`` block as a JSON diff, the schema, the geometry column's compressed size
and whether the data tables are equal -- so an approved change can be shown to
be the only difference.

Run it with a Python that has pyarrow (the footer diff reads both files), for
example the branch's own environment::

    uv run python scripts/byte_identity.py --base ../base --branch . --out report.md
    uv run python scripts/byte_identity.py --base ../base --branch . --out r.md --only convert_places_default,sort_hilbert
    uv run python scripts/byte_identity.py --base ../base --branch ../base --out self.md   # determinism check

Exit status: 0 when no case differs, 1 when at least one does, 2 on bad usage.

Determinism: gpio exposes no thread option on most commands, so nothing is
pinned. A base-against-itself run is the check: a case that differs there is
non-deterministic on one tree and is listed in ``NONDETERMINISTIC`` below with
the reason, which excludes it from the verdict (it is still run and reported).

Test hook: with ``GPIO_BYTE_IDENTITY_FAULT=compression_level:<n>`` set *and*
running under pytest (``PYTEST_CURRENT_TEST`` present), the branch side gets
``--compression-level <n>`` (``compression_level=<n>`` for API cases) on every
case that takes a level. It exists only so ``tests/test_byte_identity_script.py``
can show an injected change being reported; outside pytest it is ignored.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import filecmp
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_TIMEOUT = 600

#: Inputs the matrix reads, relative to ``--fixtures-dir`` (default
#: ``<base>/tests/data``). A case whose input is missing is skipped.
FIXTURES = (
    "places_test.parquet",
    "buildings_test.parquet",
    "buildings_test.geojson",
    "buildings_test.gpkg",
    "buildings_test.shp",
    "buildings_test.shx",
    "buildings_test.dbf",
    "buildings_test.prj",
    "fields_geom_type_only_5070_30.parquet",
    "fields_gpq2_5070_brotli.parquet",
    "fields_gpq2_crs84_zstd.parquet",
    "fields_pgo_5070_snappy.parquet",
    "fields_pgo_crs84_bbox_snappy.parquet",
    "fields_pgo_crs84_zstd.parquet",
    "data-linestring-encoding_native.parquet",
    "data-multilinestring-encoding_native.parquet",
    "data-multipoint-encoding_native.parquet",
    "data-multipolygon-encoding_native.parquet",
    "data-point-encoding_native.parquet",
    "data-polygon-encoding_native.parquet",
    "crs-projjson.parquet",
    "crs-srid.parquet",
    "idecor_carlospaz_invalid_wkb.parquet",
    "points_wkt.csv",
    "points_wkt.tsv",
    "points_latlon.csv",
    "points_latitude_longitude.csv",
    "points_geometry.csv",
    "mixed_geometries.csv",
)

#: Inputs built once, by the base side, from the fixtures above. A case that
#: needs one whose preparation failed is skipped with the reason.
PREPARED: dict[str, tuple[str, ...]] = {
    "buildings_test.fgb": ("convert", "flatgeobuf", "{fixtures}/buildings_test.parquet", "{out}"),
    "places_cells.parquet": (
        "process",
        "aggregate",
        "h3",
        "{fixtures}/places_test.parquet",
        "{out}",
        "--resolution",
        "6",
    ),
    # `add bbox-metadata` needs a 1.1 file whose bbox column is not declared;
    # every 1.x fixture in tests/data is 1.0.0, which the command refuses.
    "places_v11_undeclared_bbox.parquet": (
        "python",
        "-c",
        "import json, sys, pyarrow.parquet as pq\n"
        "t = pq.read_table(sys.argv[1]); md = dict(t.schema.metadata)\n"
        "geo = json.loads(md[b'geo']); geo['version'] = '1.1.0'\n"
        "md[b'geo'] = json.dumps(geo).encode()\n"
        "pq.write_table(t.replace_schema_metadata(md), sys.argv[2], compression='zstd')\n",
        "{fixtures}/places_test.parquet",
        "{out}",
    ),
}

#: GDAL stamps ``gpkg_contents.last_change`` with the wall clock; this config
#: option (read from the environment) pins it, so a GeoPackage is reproducible.
DETERMINISTIC_ENV = {"OGR_CURRENT_DATE": "2000-01-01T00:00:00.000Z"}

#: Cases that differ base-against-base, with the reason. Excluded from the
#: verdict; still run and shown. Measured on 3185269f with a ``--base X
#: --branch X`` run: none, once the cases below run single-threaded.
NONDETERMINISTIC: dict[str, str] = {}

#: Cases whose output row order depends on DuckDB's thread schedule: the
#: H3 rollups are hash aggregates with no ``ORDER BY`` (see #1121), so with
#: several threads two runs of the same tree write the same rows in a different
#: order (measured: tables equal only after sorting, every column chunk a few
#: bytes apart). They run with every DuckDB connection pinned to one thread
#: (``SINGLE_THREAD_SITECUSTOMIZE`` on ``PYTHONPATH``), on both sides alike.
SINGLE_THREAD_CASES = frozenset({"aggregate_h3_none", "aggregate_h3_polygon", "process_overview"})

SINGLE_THREAD_SITECUSTOMIZE = """\
import duckdb

_connect = duckdb.connect


def _single_thread(*args, **kwargs):
    config = dict(kwargs.pop("config", None) or {})
    config["threads"] = 1
    return _connect(*args, config=config, **kwargs)


duckdb.connect = _single_thread
"""

TABLE_WRITE_CODE = (
    "import sys, geoparquet_io as gpio\n"
    "kwargs = {'write_strategy': sys.argv[3]}\n"
    "if len(sys.argv) > 4: kwargs['compression_level'] = int(sys.argv[4])\n"
    "gpio.read(sys.argv[1]).write(sys.argv[2], **kwargs)\n"
)


@dataclass(frozen=True)
class Case:
    """One command run on both sides.

    ``argv`` is the command after ``gpio`` (or, for ``kind == "api"``, the
    arguments after ``python -c CODE``); ``{in}`` is the first input,
    ``{in2}`` the second, ``{out}`` the output path. ``inplace`` copies the
    first input to ``{out}`` and passes ``{out}`` as ``{in}``, for commands
    that rewrite their input. ``command`` names the CLI path whose presence is
    checked in each worktree; ``takes_level`` says whether the case accepts
    ``--compression-level`` (used only by the test hook).
    """

    name: str
    argv: tuple[str, ...]
    inputs: tuple[str, ...]
    output: str
    kind: str = "cli"
    inplace: bool = False
    takes_level: bool = True

    @property
    def command(self) -> tuple[str, ...]:
        if self.kind != "cli":
            return ()
        words = []
        for word in self.argv:
            if word.startswith("{") or word.startswith("-"):
                break
            words.append(word)
        return tuple(words)


def _convert(name, src, *extra, out="out.parquet"):
    return Case(name, ("convert", "geoparquet", "{in}", "{out}", *extra), (src,), out)


#: ``convert geoparquet`` runs from each input format with each option set.
CONVERT_SOURCES = {
    "parquet": "places_test.parquet",
    "geojson": "buildings_test.geojson",
    "csv": "points_wkt.csv",
    "gpkg": "buildings_test.gpkg",
    "fgb": "buildings_test.fgb",
}
CONVERT_VARIANTS = {
    "level15": ("--compression-level", "15"),
    "rg1000": ("--row-group-size", "1000"),
    "v1.0": ("--geoparquet-version", "1.0"),
    "v1.1": ("--geoparquet-version", "1.1"),
    "v2.0": ("--geoparquet-version", "2.0"),
}


def _matrix() -> list[Case]:
    places = "places_test.parquet"
    buildings = "buildings_test.parquet"
    cases = [
        _convert("convert_buildings_shp", "buildings_test.shp"),
        _convert("convert_buildings_parquet", buildings),
    ]
    for label, src in CONVERT_SOURCES.items():
        cases.append(_convert(f"convert_{label}_default", src))
        for variant, extra in CONVERT_VARIANTS.items():
            cases.append(_convert(f"convert_{label}_{variant}", src, *extra))
    for fixture in FIXTURES:
        stem = fixture.rsplit(".", 1)[0]
        if fixture.startswith(("fields_", "data-", "crs-", "idecor_")):
            cases.append(_convert(f"convert_{stem}", fixture))
        elif fixture.endswith((".csv", ".tsv")):
            cases.append(_convert(f"convert_{stem}_{fixture.rsplit('.', 1)[1]}", fixture))
    cases += [
        Case(
            "convert_reproject_3857",
            ("convert", "reproject", "{in}", "{out}", "--dst-crs", "EPSG:3857"),
            (places,),
            "out.parquet",
        ),
        Case(
            "convert_reproject_srid_4326",
            ("convert", "reproject", "{in}", "{out}", "--dst-crs", "EPSG:4326"),
            ("crs-srid.parquet",),
            "out.parquet",
        ),
    ]
    for fmt, ext in (
        ("geojson", "geojson"),
        ("geopackage", "gpkg"),
        ("flatgeobuf", "fgb"),
        ("csv", "csv"),
    ):
        for src in (places, buildings):
            cases.append(
                Case(
                    f"to_{fmt}_{src.split('_')[0]}",
                    ("convert", fmt, "{in}", "{out}"),
                    (src,),
                    f"out.{ext}",
                    takes_level=False,
                )
            )
    cases += [
        Case("sort_hilbert", ("sort", "hilbert", "{in}", "{out}"), (places,), "out.parquet"),
        Case(
            "sort_hilbert_buildings",
            ("sort", "hilbert", "{in}", "{out}"),
            (buildings,),
            "out.parquet",
        ),
        Case("sort_quadkey", ("sort", "quadkey", "{in}", "{out}"), (places,), "out.parquet"),
        Case("sort_column", ("sort", "column", "{in}", "{out}", "name"), (places,), "out.parquet"),
        Case(
            "extract_bbox",
            ("extract", "{in}", "{out}", "--bbox", "-0.5,9.5,0.5,10.5"),
            (places,),
            "out.parquet",
        ),
        Case(
            "extract_columns",
            ("extract", "{in}", "{out}", "--include-cols", "name"),
            (places,),
            "out.parquet",
        ),
        Case(
            "extract_exclude",
            ("extract", "{in}", "{out}", "--exclude-cols", "bbox"),
            (places,),
            "out.parquet",
        ),
        Case(
            "extract_where",
            ("extract", "{in}", "{out}", "--where", "address IS NOT NULL"),
            (places,),
            "out.parquet",
        ),
        Case(
            "extract_where_buildings",
            ("extract", "{in}", "{out}", "--where", "1 = 1"),
            (buildings,),
            "out.parquet",
        ),
        Case("add_bbox", ("add", "bbox", "{in}", "{out}"), (buildings,), "out.parquet"),
        Case(
            "add_bbox_force", ("add", "bbox", "{in}", "{out}", "--force"), (places,), "out.parquet"
        ),
        Case(
            "add_bbox_metadata",
            ("add", "bbox-metadata", "{in}"),
            ("places_v11_undeclared_bbox.parquet",),
            "out.parquet",
            inplace=True,
            takes_level=False,
        ),
        Case(
            "add_h3", ("add", "h3", "{in}", "{out}", "--resolution", "7"), (places,), "out.parquet"
        ),
        Case("add_quadkey", ("add", "quadkey", "{in}", "{out}"), (places,), "out.parquet"),
        Case("add_s2", ("add", "s2", "{in}", "{out}", "--level", "10"), (places,), "out.parquet"),
        Case(
            "add_geometry_metrics",
            ("add", "geometry-metrics", "{in}", "{out}"),
            (buildings,),
            "out.parquet",
        ),
        Case(
            "check_bbox_fix",
            ("check", "bbox", "{in}", "--fix", "--fix-output", "{out}"),
            (places,),
            "out.parquet",
            takes_level=False,
        ),
        Case(
            "aggregate_h3_none",
            (
                "process",
                "aggregate",
                "h3",
                "{in}",
                "{out}",
                "--resolution",
                "8",
                "--out-geometry",
                "none",
            ),
            (places,),
            "out.parquet",
        ),
        Case(
            "aggregate_h3_polygon",
            ("process", "aggregate", "h3", "{in}", "{out}", "--resolution", "8"),
            (places,),
            "out.parquet",
        ),
        Case(
            "process_overview",
            ("process", "overview", "{in}", "--levels", "4,5", "--output-dir", "{out}"),
            ("places_cells.parquet",),
            "out_dir",
            takes_level=False,
        ),
        Case(
            "process_simplify",
            ("process", "simplify", "{in}", "{out}", "--tolerance", "0.0001"),
            (buildings,),
            "out.parquet",
            takes_level=False,
        ),
        Case(
            "partition_h3",
            ("partition", "h3", "{in}", "{out}", "--resolution", "4", "--force", "--skip-analysis"),
            (places,),
            "out_dir",
        ),
        Case(
            "partition_quadkey",
            (
                "partition",
                "quadkey",
                "{in}",
                "{out}",
                "--resolution",
                "13",
                "--partition-resolution",
                "6",
                "--force",
                "--skip-analysis",
            ),
            (places,),
            "out_dir",
        ),
    ]
    for strategy in ("duckdb-kv", "in-memory", "streaming", "disk-rewrite"):
        for src in (places, buildings):
            cases.append(
                Case(
                    f"table_write_{strategy}_{src.split('_')[0]}",
                    ("{in}", "{out}", strategy),
                    (src,),
                    "out.parquet",
                    kind="api",
                )
            )
    return cases


# ---------------------------------------------------------------------------
# Running one side
# ---------------------------------------------------------------------------


@dataclass
class Run:
    status: str  # "ok", "error", "skipped"
    detail: str = ""
    output: Path | None = None


@dataclass
class Side:
    label: str
    worktree: Path
    out_root: Path
    inputs: Path
    fault_level: int | None = None
    _commands: dict[tuple[str, ...], bool] = field(default_factory=dict)

    def env(self, single_thread: bool = False) -> dict[str, str]:
        env = {k: v for k, v in os.environ.items() if not k.startswith("VIRTUAL_ENV")}
        env["UV_PROJECT_ENVIRONMENT"] = str(self.worktree / ".venv")
        env.update(DETERMINISTIC_ENV)
        env.pop("GPIO_BYTE_IDENTITY_FAULT", None)
        if single_thread:
            shim = self.out_root.parent / "single_thread"
            shim.mkdir(parents=True, exist_ok=True)
            (shim / "sitecustomize.py").write_text(SINGLE_THREAD_SITECUSTOMIZE)
            env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(shim), env.get("PYTHONPATH")]))
        return env

    def prefix(self) -> list[str]:
        return ["uv", "run", "--no-sync", "--project", str(self.worktree)]

    def has_command(self, command: tuple[str, ...]) -> bool:
        if not command:
            return True
        if command not in self._commands:
            proc = subprocess.run(
                [*self.prefix(), "gpio", *command, "--help"],
                env=self.env(),
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
            self._commands[command] = proc.returncode == 0
        return self._commands[command]

    def run(self, case: Case, timeout: int) -> Run:
        missing = [name for name in case.inputs if not (self.inputs / name).exists()]
        if missing:
            return Run("skipped", f"input missing: {', '.join(missing)}")
        if not self.has_command(case.command):
            return Run("skipped", f"`gpio {' '.join(case.command)}` not in {self.label}")
        case_dir = self.out_root / case.name
        shutil.rmtree(case_dir, ignore_errors=True)
        case_dir.mkdir(parents=True)
        out = case_dir / case.output
        first = self.inputs / case.inputs[0]
        if case.inplace:
            shutil.copy(first, out)
            first = out
        values = {"in": str(first), "out": str(out)}
        if len(case.inputs) > 1:
            values["in2"] = str(self.inputs / case.inputs[1])
        argv = [word.format(**values) for word in case.argv]
        if self.fault_level is not None and case.takes_level:
            argv += (
                [str(self.fault_level)]
                if case.kind == "api"
                else ["--compression-level", str(self.fault_level)]
            )
        command = [
            *self.prefix(),
            *(["python", "-c", TABLE_WRITE_CODE] if case.kind == "api" else ["gpio"]),
            *argv,
        ]
        try:
            env = self.env(single_thread=case.name in SINGLE_THREAD_CASES)
            proc = subprocess.run(
                command,
                env=env,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=case_dir,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return Run("error", f"timed out after {timeout}s")
        if proc.returncode != 0:
            lines = [line.strip() for line in (proc.stderr or proc.stdout).splitlines()]
            errors = [line for line in lines if line.startswith("Error")][:1]
            tail = errors + [line for line in lines if line][-2:]
            message = " | ".join(tail).replace(str(case_dir), "<out>")
            return Run("error", f"exit {proc.returncode}: {message}")
        if not out.exists():
            return Run("error", "command succeeded but wrote no output")
        return Run("ok", output=out)


# ---------------------------------------------------------------------------
# Comparing
# ---------------------------------------------------------------------------


def _files(path: Path) -> dict[str, Path]:
    if path.is_file():
        return {"": path}
    return {str(p.relative_to(path)): p for p in sorted(path.rglob("*")) if p.is_file()}


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


def compare_outputs(base: Path, branch: Path) -> tuple[bool, list[str]]:
    """``(identical, detail lines)`` for two output files or directory trees."""
    left, right = _files(base), _files(branch)
    detail: list[str] = []
    if set(left) != set(right):
        detail.append(
            f"- file sets differ: only base {sorted(set(left) - set(right))}, "
            f"only branch {sorted(set(right) - set(left))}"
        )
    differing = [
        name
        for name in sorted(set(left) & set(right))
        if not filecmp.cmp(left[name], right[name], shallow=False)
    ]
    for name in differing:
        label = name or base.name
        detail.append(
            f"- `{label}`: {left[name].stat().st_size} B vs "
            f"{right[name].stat().st_size} B "
            f"(sha {_digest(left[name])} vs {_digest(right[name])})"
        )
        if left[name].suffix == ".parquet":
            detail.extend("  " + line for line in footer_diff(left[name], right[name]))
    return not detail, detail


def _json_diff(a, b, path: str = "") -> list[str]:
    if isinstance(a, dict) and isinstance(b, dict):
        lines = []
        for key in sorted(set(a) | set(b), key=str):
            where = f"{path}.{key}" if path else str(key)
            if key not in a:
                lines.append(f"+ {where} = {json.dumps(b[key])[:200]}")
            elif key not in b:
                lines.append(f"- {where} = {json.dumps(a[key])[:200]}")
            else:
                lines.extend(_json_diff(a[key], b[key], where))
        return lines
    if a != b:
        return [f"~ {path or '(root)'}: {json.dumps(a)[:200]} -> {json.dumps(b)[:200]}"]
    return []


def _footer(path: Path) -> dict:
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(str(path))
    md = pf.metadata
    codecs: dict[str, set[str]] = {}
    sizes: dict[str, int] = {}
    for r in range(md.num_row_groups):
        group = md.row_group(r)
        for c in range(group.num_columns):
            chunk = group.column(c)
            codecs.setdefault(chunk.path_in_schema, set()).add(chunk.compression)
            sizes[chunk.path_in_schema] = sizes.get(chunk.path_in_schema, 0) + (
                chunk.total_compressed_size
            )
    kv = dict(md.metadata or {})
    geo = None
    if b"geo" in kv:
        try:
            geo = json.loads(kv[b"geo"])
        except ValueError:
            geo = {"<unparsable>": kv[b"geo"][:200].decode("utf-8", "replace")}
    return {
        "codecs": {k: sorted(v) for k, v in codecs.items()},
        "sizes": sizes,
        "row_groups": [md.row_group(r).num_rows for r in range(md.num_row_groups)],
        "kv_keys": sorted(k.decode("utf-8", "replace") for k in kv),
        "geo": geo,
        "schema": str(pf.schema_arrow.remove_metadata()),
    }


def _tables_equal(base: Path, branch: Path) -> str:
    import pyarrow as pa
    import pyarrow.parquet as pq

    left = pq.read_table(str(base)).replace_schema_metadata(None)
    right = pq.read_table(str(branch)).replace_schema_metadata(None)
    if left.schema != right.schema:
        return "no (schemas differ)"
    if left.equals(right):
        return "yes"
    keys = [
        (f.name, "ascending")
        for f in left.schema
        if not (pa.types.is_nested(f.type) or isinstance(f.type, pa.ExtensionType))
    ]
    if keys and left.sort_by(keys).equals(right.sort_by(keys)):
        return "yes after sorting (row order differs)"
    return "no"


def footer_diff(base: Path, branch: Path) -> list[str]:
    """Footer-level differences between two Parquet files, one line each."""
    try:
        a, b = _footer(base), _footer(branch)
    except Exception as exc:  # an unreadable output is itself the finding
        return [f"footer unreadable: {exc}"]
    lines = []
    for column in sorted(set(a["codecs"]) | set(b["codecs"])):
        if a["codecs"].get(column) != b["codecs"].get(column):
            lines.append(
                f"codec `{column}`: {a['codecs'].get(column)} -> {b['codecs'].get(column)}"
            )
    for column in sorted(set(a["sizes"]) | set(b["sizes"])):
        if a["sizes"].get(column) != b["sizes"].get(column):
            lines.append(
                f"compressed size `{column}`: {a['sizes'].get(column)} -> "
                f"{b['sizes'].get(column)} B"
            )
    if a["row_groups"] != b["row_groups"]:
        lines.append(
            f"row groups: {len(a['row_groups'])} {a['row_groups'][:6]} -> "
            f"{len(b['row_groups'])} {b['row_groups'][:6]}"
        )
    if a["kv_keys"] != b["kv_keys"]:
        lines.append(f"KV keys: {a['kv_keys']} -> {b['kv_keys']}")
    lines.extend(f"geo {line}" for line in _json_diff(a["geo"], b["geo"]))
    if a["schema"] != b["schema"]:
        lines.append(f"schema: {a['schema']!r} -> {b['schema']!r}")
    try:
        lines.append(f"data tables equal: {_tables_equal(base, branch)}")
    except Exception as exc:
        lines.append(f"data tables equal: could not compare ({exc})")
    return lines


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------


#: Verdicts that mean "the two sides behaved the same": the same bytes, the
#: same error, or nothing to compare.
AGREEING = frozenset({"identical", "same-error", "skipped"})


@dataclass
class Result:
    case: Case
    verdict: str  # identical / differs / skipped / both-failed / base-failed / branch-failed
    reason: str
    detail: list[str]


def judge(case: Case, base: Run, branch: Run) -> Result:
    if "skipped" in (base.status, branch.status):
        reason = base.detail if base.status == "skipped" else branch.detail
        return Result(case, "skipped", reason, [])
    if base.status == "error" and branch.status == "error":
        verdict = "same-error" if base.detail == branch.detail else "different-errors"
        return Result(
            case,
            verdict,
            base.detail[:160],
            [f"- base: {base.detail}", f"- branch: {branch.detail}"],
        )
    if base.status == "error" or branch.status == "error":
        who = "base" if base.status == "error" else "branch"
        return Result(case, f"{who}-failed", (base if who == "base" else branch).detail, [])
    identical, detail = compare_outputs(base.output, branch.output)
    if identical:
        return Result(case, "identical", "", [])
    first = next(
        (
            line.strip()
            for line in detail
            if line.strip().startswith(("codec", "compressed", "row groups", "KV", "geo", "schema"))
        ),
        detail[0],
    )
    return Result(case, "differs", first, detail)


def prepare_inputs(fixtures: Path, base: Side, inputs: Path, timeout: int) -> dict[str, str]:
    """Copy the fixtures, then build the prepared inputs with the base side."""
    inputs.mkdir(parents=True, exist_ok=True)
    for name in FIXTURES:
        if (fixtures / name).exists():
            shutil.copy(fixtures / name, inputs / name)
    failures = {}
    for name, template in PREPARED.items():
        target = inputs / name
        argv = [w.format(fixtures=inputs, out=target) for w in template]
        program = [] if argv[0] == "python" else ["gpio"]
        proc = subprocess.run(
            [*base.prefix(), *program, *argv],
            env=base.env(),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        if proc.returncode != 0 or not target.exists():
            failures[name] = (proc.stderr or proc.stdout).strip().splitlines()[-1:]
    return failures


def _fault_level() -> int | None:
    fault = os.environ.get("GPIO_BYTE_IDENTITY_FAULT", "")
    if not fault or "PYTEST_CURRENT_TEST" not in os.environ:
        return None
    kind, _, value = fault.partition(":")
    if kind != "compression_level":
        raise SystemExit(f"unknown GPIO_BYTE_IDENTITY_FAULT {fault!r}")
    return int(value)


def write_report(path: Path, results: list[Result], meta: dict[str, str]) -> None:
    counts: dict[str, int] = {}
    for result in results:
        counts[result.verdict] = counts.get(result.verdict, 0) + 1
    lines = ["# Byte-identity report", ""]
    lines += [f"- {key}: `{value}`" for key, value in meta.items()]
    lines += ["- verdicts: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())), ""]
    lines += ["| case | verdict | reason |", "|---|---|---|"]
    for result in results:
        note = NONDETERMINISTIC.get(result.case.name)
        reason = result.reason + (f" (non-deterministic: {note})" if note else "")
        lines.append(f"| `{result.case.name}` | {result.verdict} | {reason.replace('|', '/')} |")
    lines.append("")
    for result in results:
        if result.detail:
            lines += [
                f"## `{result.case.name}` -- {result.verdict}",
                "",
                f"`{' '.join(result.case.argv)}` on `{', '.join(result.case.inputs)}`",
                "",
            ]
            lines += result.detail + [""]
    path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", type=Path)
    parser.add_argument("--branch", type=Path)
    parser.add_argument("--out", type=Path, help="markdown report path")
    parser.add_argument("--only", default="", help="comma-separated case names")
    parser.add_argument("--fixtures-dir", type=Path, default=None)
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=None,
        help="keep inputs and outputs here (default: a temp dir, removed)",
    )
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--list", action="store_true", help="print the case names and exit")
    args = parser.parse_args(argv)

    try:
        import pyarrow  # noqa: F401  (the footer diff needs it)
    except ImportError:
        parser.error("pyarrow is required: run with `uv run python scripts/byte_identity.py`")
    cases = _matrix()
    if args.list:
        print("\n".join(case.name for case in cases))
        return 0
    if not (args.base and args.branch and args.out):
        parser.error("--base, --branch and --out are required")
    if args.only:
        wanted = {name.strip() for name in args.only.split(",") if name.strip()}
        unknown = wanted - {case.name for case in cases}
        if unknown:
            parser.error(f"unknown case(s): {', '.join(sorted(unknown))}")
        cases = [case for case in cases if case.name in wanted]
    base_wt, branch_wt = args.base.resolve(), args.branch.resolve()
    for wt in (base_wt, branch_wt):
        if not (wt / ".venv").is_dir():
            parser.error(f"{wt} has no .venv")
    fixtures = (args.fixtures_dir or base_wt / "tests" / "data").resolve()

    work = args.work_dir.resolve() if args.work_dir else Path(tempfile.mkdtemp(prefix="byteid-"))
    try:
        inputs = work / "inputs"
        base = Side("base", base_wt, work / "base", inputs)
        branch = Side("branch", branch_wt, work / "branch", inputs, fault_level=_fault_level())
        prep_failures = prepare_inputs(fixtures, base, inputs, args.timeout)

        def _one(case: Case) -> Result:
            return judge(case, base.run(case, args.timeout), branch.run(case, args.timeout))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
            results = list(pool.map(_one, cases))
        meta = {
            "base": str(base_wt),
            "branch": str(branch_wt),
            "fixtures": str(fixtures),
            "cases": str(len(cases)),
        }
        if prep_failures:
            meta["prepared inputs that failed"] = json.dumps(prep_failures)
        if branch.fault_level is not None:
            meta["injected fault (branch side)"] = f"--compression-level {branch.fault_level}"
        write_report(args.out, results, meta)
    finally:
        if not args.work_dir:
            shutil.rmtree(work, ignore_errors=True)

    differing = [
        r for r in results if r.verdict not in AGREEING and r.case.name not in NONDETERMINISTIC
    ]
    for result in results:
        print(f"{result.verdict:13} {result.case.name}  {result.reason}")
    print(f"report: {args.out}")
    return 1 if differing else 0


if __name__ == "__main__":
    sys.exit(main())
