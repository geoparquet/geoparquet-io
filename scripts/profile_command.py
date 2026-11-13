#!/usr/bin/env python3
"""Fresh-process command measurements; diagnostics are separate from timing runs.

Run from the repository with ``uv run python -m scripts.profile_command``.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from collections import Counter
from pathlib import Path
from unittest.mock import patch


class CallSpies:
    """Observe Python/C chokepoints without changing DuckDB connection identity."""

    def __init__(self):
        self.counts = Counter()
        self.previous = None

    def _event(self, frame, event, arg):
        if event == "call":
            name = frame.f_code.co_name
            module = frame.f_globals.get("__name__", "")
            if name == "get_duckdb_connection" and module.endswith("duckdb_utils"):
                self.counts["connections"] += 1
            if module.startswith("pyarrow.parquet"):
                if (
                    name == "__init__"
                    and type(frame.f_locals.get("self")).__name__ == "ParquetFile"
                    and frame.f_locals.get("metadata") is None
                ):
                    self.counts["footer_reads"] += 1
                if (
                    name == "write_table"
                    and type(frame.f_locals.get("self")).__name__ == "ParquetWriter"
                ):
                    self.counts["parquet_writes"] += 1

    def __enter__(self):
        import duckdb
        import pyarrow.ipc as ipc

        self.patches = contextlib.ExitStack()
        counts = self.counts

        class ReaderSpy:
            def __init__(self, reader):
                self.reader = reader

            def read_all(self):
                counts["arrow_read_all"] += 1
                table = self.reader.read_all()
                counts["arrow_nonempty_read_all"] += bool(table.num_rows)
                return table

            def __getattr__(self, name):
                return getattr(self.reader, name)

            def __iter__(self):
                return iter(self.reader)

            def __enter__(self):
                self.reader.__enter__()
                return self

            def __exit__(self, *exc):
                return self.reader.__exit__(*exc)

        original_read = ipc.RecordBatchStreamReader.read_all

        def read_all(owner, *args, **kwargs):
            counts["arrow_read_all"] += 1
            table = original_read(owner, *args, **kwargs)
            counts["arrow_nonempty_read_all"] += bool(table.num_rows)
            return table

        self.patches.enter_context(patch.object(ipc.RecordBatchStreamReader, "read_all", read_all))
        for cls in (duckdb.DuckDBPyConnection, duckdb.DuckDBPyRelation):
            if not hasattr(cls, "arrow"):
                continue
            original = cls.arrow

            def arrow(owner, *args, _original=original, **kwargs):
                return ReaderSpy(_original(owner, *args, **kwargs))

            self.patches.enter_context(patch.object(cls, "arrow", arrow))
        for name in ("execute", "executemany"):
            original = getattr(duckdb.DuckDBPyConnection, name)

            def execute(owner, *args, _original=original, **kwargs):
                query = args[0] if args else kwargs.get("query", "")
                instrumentation = (
                    str(query)
                    .lstrip()
                    .upper()
                    .startswith(
                        (
                            "SET PROFILING_OUTPUT",
                            "SET ENABLE_PROFILING",
                            "PRAGMA DISABLE_PROFILING",
                        )
                    )
                )
                if not instrumentation:
                    counts["execute"] += 1
                if re.search(r"\bFORMAT\s*(?:=|:=)?\s*['\"]?PARQUET\b", str(query), re.I):
                    counts["parquet_writes"] += 1
                return _original(owner, *args, **kwargs)

            self.patches.enter_context(patch.object(duckdb.DuckDBPyConnection, name, execute))
        original_write = duckdb.DuckDBPyRelation.write_parquet

        def write_parquet(owner, *args, **kwargs):
            counts["parquet_writes"] += 1
            return original_write(owner, *args, **kwargs)

        self.patches.enter_context(
            patch.object(duckdb.DuckDBPyRelation, "write_parquet", write_parquet)
        )
        import pyarrow.parquet as pq

        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        self.monitoring = getattr(sys, "monitoring", None)
        if self.monitoring is not None and self.monitoring.get_tool(5) is None:
            # Local events keep per-row serialization out of the counter callback.
            self.codes = {
                get_duckdb_connection.__code__: "connections",
                pq.ParquetFile.__init__.__code__: "footer_reads",
                pq.ParquetWriter.write_table.__code__: "parquet_writes",
            }
            self.monitoring.use_tool_id(5, "gpio-call-spies")

            def entered(code, _offset):
                counter = self.codes[code]
                if (
                    counter == "footer_reads"
                    and sys._getframe(1).f_locals.get("metadata") is not None
                ):
                    return
                counts[counter] += 1

            self.monitoring.register_callback(5, self.monitoring.events.PY_START, entered)
            for code in self.codes:
                self.monitoring.set_local_events(5, code, self.monitoring.events.PY_START)
        else:
            self.monitoring = None
            self.previous_threads = threading.getprofile()
            threading.setprofile(self._event)
            self.previous = sys.getprofile()
            sys.setprofile(self._event)
        return self

    def __exit__(self, *exc):
        if self.monitoring is not None:
            for code in self.codes:
                self.monitoring.set_local_events(5, code, 0)
            self.monitoring.register_callback(5, self.monitoring.events.PY_START, None)
            self.monitoring.free_tool_id(5)
        else:
            sys.setprofile(self.previous)
            threading.setprofile(self.previous_threads)
        self.patches.close()


def _cli(argv):
    from click.exceptions import Exit

    from geoparquet_io.cli.main import cli

    try:
        with cli.make_context("gpio", argv) as context:
            return cli.invoke(context)
    except Exit as exc:
        if exc.exit_code:
            raise RuntimeError(f"CLI exited with status {exc.exit_code}") from exc


def invoke(spec, input_path, output, url, *, prepare=False, prepared=None):
    """Invoke the actual CLI/API; setup artifacts are excluded from measurements."""
    from pathlib import Path

    kind = spec.get("kind", "cli")
    if prepare:
        if kind == "stdin":
            from contextlib import redirect_stdout

            stream = output.parent / "input.arrow"
            with stream.open("w") as stdout, redirect_stdout(stdout):
                _cli(["extract", "geoparquet", str(input_path), "-"])
            return stream
        if spec.get("prepared_input"):
            path = output.parent / "bbox.parquet"
            unsorted = output.parent / "bbox-unsorted.parquet"
            _cli(["add", "bbox", str(input_path), str(unsorted), "--overwrite"])
            _cli(["sort", "hilbert", str(unsorted), str(path), "--overwrite"])
            return path
        if kind.startswith("admin-"):
            _admin_fixture(output.parent / "admin.parquet")
            if kind == "admin-native":
                _cli(
                    [
                        "convert",
                        "geoparquet",
                        str(input_path),
                        str(output.parent / "input.parquet"),
                        "--geoparquet-version",
                        "2.0",
                        "--overwrite",
                    ]
                )
            elif kind == "admin-reprojected":
                _cli(
                    [
                        "convert",
                        "reproject",
                        str(input_path),
                        str(output.parent / "input.parquet"),
                        "-d",
                        "EPSG:3857",
                        "--overwrite",
                    ]
                )
            return output.parent
        if kind == "overview":
            base = output.parent / "aggregate.parquet"
            _cli(["process", "aggregate", "h3", str(input_path), str(base), "--resolution", "6"])
            return base
        return None
    if kind == "overview":
        return _cli(
            [
                "process",
                "overview",
                str(prepared),
                "--levels",
                "2,3,4,5",
                "--output-dir",
                str(output),
            ]
        )
    if kind.startswith("api-"):
        from geoparquet_io.api.table import convert, read

        operation = kind.removeprefix("api-")
        table = convert(input_path) if operation == "convert" else read(input_path)
        if operation == "h3":
            table = table.add_h3(resolution=5)
        elif operation == "kdtree":
            table = table.add_kdtree(iterations=3)
        elif operation == "sort-column":
            table = table.sort_column("value")
        elif operation == "reproject":
            table = table.reproject("EPSG:3857")
        elif operation == "extract":
            table = table.extract(bbox=(-60, -30, 60, 30))
        return table.write(output / "result.parquet")
    if kind in {"stream", "stdin"}:
        import sys
        from contextlib import redirect_stdout
        from unittest.mock import patch

        if kind == "stream":
            with (output / "stream.arrow").open("w") as stdout, redirect_stdout(stdout):
                return _cli(["extract", "geoparquet", str(input_path), "-"])
        with Path(prepared).open("r") as stdin, patch.object(sys, "stdin", stdin):
            return _cli(["convert", "geojson", "-", str(output / "result.geojson")])
    if kind.startswith("admin-"):
        fixture = Path(prepared)
        prepared_input = fixture / "input.parquet"
        return _admin(
            prepared_input if prepared_input.exists() else input_path,
            output,
            boundary=fixture / "admin.parquet",
            operation=kind.removeprefix("admin-"),
        )
    values = {
        "input": str(input_path),
        "output": str(output / "result.parquet"),
        "directory": str(output / "parts"),
        "geojson": str(output / "result.geojson"),
        "report": str(output / "report.json"),
        "fgb": str(output / "result.fgb"),
        "gpkg": str(output / "result.gpkg"),
        "shp": str(output / "result.shp"),
        "glob": str(Path(input_path) / "*.parquet"),
        "url": url,
    }
    argv = [arg if "{file}" in arg else arg.format(**values) for arg in spec["argv"]]
    if spec.get("capture_stdout"):
        from contextlib import redirect_stdout

        with (output / "stdout.txt").open("w") as stdout, redirect_stdout(stdout):
            return _cli(argv)
    return _cli(argv)


def _admin_fixture(boundary):
    """Write a small synthetic Overture boundary pair for offline spatial joins."""
    import json
    import struct

    import pyarrow as pa
    import pyarrow.parquet as pq

    def polygon(xmin, xmax):
        ring = [(xmin, -90), (xmax, -90), (xmax, 90), (xmin, 90), (xmin, -90)]
        return struct.pack("<BIII", 1, 3, 1, 5) + b"".join(struct.pack("<dd", *p) for p in ring)

    table = pa.table(
        {
            "country": ["west", "east"],
            "region": ["west", "east"],
            "subtype": ["country", "country"],
            "bbox": [
                {"xmin": -180.0, "xmax": 10.0, "ymin": -90.0, "ymax": 90.0},
                {"xmin": -10.0, "xmax": 180.0, "ymin": -90.0, "ymax": 90.0},
            ],
            "geometry": [polygon(-180, 10), polygon(-10, 180)],
        }
    ).replace_schema_metadata(
        {
            b"geo": json.dumps(
                {
                    "version": "1.1.0",
                    "primary_column": "geometry",
                    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon"]}},
                }
            ).encode()
        }
    )
    pq.write_table(table, boundary)


def _admin(input_path, output, *, boundary, operation="aggregate"):
    """Keep the real admin command paths, substituting only local source data."""
    from unittest.mock import patch

    from geoparquet_io.core.admin_datasets import AdminDatasetFactory

    factory = AdminDatasetFactory.create

    def local(dataset_name, source_path=None, verbose=False):
        return factory(dataset_name, str(boundary), verbose)

    with patch.object(AdminDatasetFactory, "create", local):
        if operation == "add":
            return _cli(
                [
                    "add",
                    "admin-divisions",
                    str(input_path),
                    str(output / "result.parquet"),
                    "--dataset",
                    "overture",
                    "--levels",
                    "country",
                    "--no-cache",
                ]
            )
        if operation == "partition":
            return _cli(
                [
                    "partition",
                    "admin",
                    str(input_path),
                    str(output / "parts"),
                    "--dataset",
                    "overture",
                    "--levels",
                    "country",
                    "--skip-analysis",
                ]
            )
        return _cli(
            [
                "process",
                "aggregate",
                "admin",
                str(input_path),
                str(output / "result.parquet"),
                "--breakdown",
                "category",
            ]
        )


def write_json(path, value):
    target = Path(path)
    temporary = target.with_name(target.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(target)


def output_hashes(directory, command=None):
    """Hash output bytes, independent of the temporary directory's name."""
    hashes = {}
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            if command is None:
                with path.open("rb") as stream:
                    digest = hashlib.file_digest(stream, "sha256").hexdigest()
            else:
                from scripts.profile_matrix.output_comparison import comparable_bytes

                digest = hashlib.sha256(comparable_bytes(path, command)).hexdigest()
            hashes[str(path.relative_to(directory))] = digest
    return hashes


def peak_rss_bytes():
    if sys.platform == "win32":
        import psutil

        return psutil.Process().memory_info().peak_wset

    if sys.platform.startswith("linux"):
        # rusage can retain a parent's pre-exec peak; procfs tracks this image.
        try:
            for line in Path("/proc/self/status").read_text().splitlines():
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
        except (OSError, ValueError, IndexError):
            pass

    import resource

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (
        1 if sys.platform == "darwin" else 1024
    )


def run_child(args):
    from scripts.profile_matrix.matrix import COMMANDS
    from scripts.profile_matrix.workloads import generate_workload, serve_workload

    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    spec = next(c for c in COMMANDS if c["id"] == args.command)
    root = Path(args.work_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    input_path = (
        Path(args.input_path)
        if args.input_path
        else generate_workload(spec["workload"], root / "inputs", args.size)
    )
    if spec.get("prepared_input") and args.prepared_path and not args.prepare_only:
        input_path = Path(args.prepared_path)
    output = root / "output"
    output.mkdir(exist_ok=True)
    # Prepare fixtures and start local servers outside the timed region.
    with serve_workload(input_path) as served:
        if args.prepare_only:
            prepared = invoke(spec, input_path, output, served, prepare=True)
            write_json(
                args.output,
                {
                    "prepared": str(prepared),
                    "revision": revision,
                    "workload": spec["workload"],
                    "size": args.size,
                    "input_path": str(input_path.resolve()),
                },
            )
            return
        prepared = args.prepared_path
        spies = CallSpies()
        profiler = contextlib.nullcontext()
        if args.memray:
            import memray

            profiler = memray.Tracker(args.memray, native_traces=True)
        instrument = None
        if args.pyinstrument:
            from pyinstrument import Profiler

            instrument = Profiler()
            instrument.start()
        start_cpu = time.process_time()
        start = time.perf_counter()
        with profiler, spies if args.spies else contextlib.nullcontext():
            invoke(spec, input_path, output, served, prepared=prepared)
        row = {
            "command": args.command,
            "revision": revision,
            "workload": spec["workload"],
            "size": args.size,
            "wall_seconds": time.perf_counter() - start,
            "cpu_seconds": time.process_time() - start_cpu,
            "peak_rss_bytes": peak_rss_bytes(),
            "counters": dict(spies.counts),
            "output_sha256": output_hashes(output, args.command),
            "raw_output_sha256": output_hashes(output),
            "diagnostic": bool(
                args.spies or args.memray or args.pyinstrument or os.getenv("GPIO_DUCKDB_PROFILE")
            ),
        }
    if instrument is not None:
        instrument.stop()
        instrument.write_html(args.pyinstrument)
    write_json(args.output, row)


def run_matrix(args):
    from scripts.profile_matrix.matrix import COMMANDS
    from scripts.profile_matrix.workloads import generate_workload

    target = Path(args.output).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    rows = []
    commands = [c for c in COMMANDS if not args.command or c["id"] == args.command]
    for spec in commands:
        input_path = generate_workload(spec["workload"], target.parent / "inputs", args.size)
        prepared_flags = []
        if spec.get("prepare") or spec.get("kind") == "overview":
            fixture = target.parent / "fixtures" / spec["id"]
            fixture.mkdir(parents=True, exist_ok=True)
            prepared_result = fixture / "prepared.json"
            if not prepared_result.exists():
                subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "scripts.profile_command",
                        "--child",
                        "--prepare-only",
                        "--command",
                        spec["id"],
                        "--size",
                        args.size,
                        "--input-path",
                        str(input_path),
                        "--work-dir",
                        str(fixture),
                        "--output",
                        str(prepared_result),
                    ],
                    check=True,
                    timeout=args.timeout,
                )
            prepared_metadata = json.loads(prepared_result.read_text())
            if (
                prepared_metadata.get("revision"),
                prepared_metadata.get("workload"),
                prepared_metadata.get("size"),
                prepared_metadata.get("input_path"),
            ) != (revision, spec["workload"], args.size, str(input_path.resolve())):
                raise RuntimeError(f"Stale fixture {prepared_result}; use a fresh output directory")
            prepared_flags = ["--prepared-path", prepared_metadata["prepared"]]
        for repeat in range(max(3, args.runs)):
            run_dir = target.parent / "measurements" / spec["id"] / str(repeat)
            result = run_dir / "result.json"
            run_dir.mkdir(parents=True, exist_ok=True)
            if not result.exists():
                shutil.rmtree(run_dir / "output", ignore_errors=True)
                command = [
                    sys.executable,
                    "-m",
                    "scripts.profile_command",
                    "--child",
                    "--command",
                    spec["id"],
                    "--size",
                    args.size,
                    "--input-path",
                    str(input_path),
                    "--work-dir",
                    str(run_dir),
                    "--output",
                    str(result),
                    *prepared_flags,
                ]
                env = os.environ.copy()
                env.pop("GPIO_DUCKDB_PROFILE", None)
                with (run_dir / "stdout.log").open("w") as stdout:
                    completed = subprocess.run(
                        command,
                        env=env,
                        stdout=stdout,
                        stderr=subprocess.STDOUT,
                        timeout=args.timeout,
                    )
                if completed.returncode:
                    raise RuntimeError(f"{spec['id']} failed; see {run_dir / 'stdout.log'}")
            row = json.loads(result.read_text())
            if (row.get("revision"), row["size"], row["command"], row["workload"]) != (
                revision,
                args.size,
                spec["id"],
                spec["workload"],
            ) or row["diagnostic"]:
                raise RuntimeError(f"Stale measurement {result}; use a fresh output directory")
            rows.append(row)
            write_json(target.with_suffix(".partial.json"), rows)
            print(f"{spec['id']} {repeat + 1}: {rows[-1]['wall_seconds']:.3f}s", flush=True)
    write_json(target, rows)


def main():
    from scripts.profile_matrix.matrix import COMMANDS

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--command", choices=[spec["id"] for spec in COMMANDS])
    parser.add_argument("--size", choices=["small", "full"], default="full")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--output", required=True)
    parser.add_argument("--work-dir")
    parser.add_argument("--input-path")
    parser.add_argument("--prepared-path")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--spies", action="store_true")
    parser.add_argument("--memray")
    parser.add_argument("--pyinstrument")
    args = parser.parse_args()
    if not args.child and (args.spies or args.memray or args.pyinstrument):
        parser.error("diagnostic flags require --child")
    if args.child:
        if not args.command or not args.work_dir:
            parser.error("--child requires --command and --work-dir")
        run_child(args)
    else:
        run_matrix(args)


if __name__ == "__main__":
    main()
