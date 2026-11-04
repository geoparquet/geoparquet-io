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
                return self.reader.read_all()

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
            return original_read(owner, *args, **kwargs)

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
                counts["execute"] += 1
                query = args[0] if args else kwargs.get("query", "")
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
        self.previous_threads = threading.getprofile()
        threading.setprofile(self._event)
        self.previous = sys.getprofile()
        sys.setprofile(self._event)
        return self

    def __exit__(self, *exc):
        sys.setprofile(self.previous)
        threading.setprofile(self.previous_threads)
        self.patches.close()


def write_json(path, value):
    target = Path(path)
    temporary = target.with_name(target.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(target)


def output_hashes(directory):
    """Hash output bytes, independent of the temporary directory's name."""
    hashes = {}
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            with path.open("rb") as stream:
                hashes[str(path.relative_to(directory))] = hashlib.file_digest(
                    stream, "sha256"
                ).hexdigest()
    return hashes


def run_child(args):
    import resource

    from geoparquet_io.benchmarks.matrix import COMMANDS, invoke
    from geoparquet_io.benchmarks.workloads import generate_workload, serve_workload

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
            write_json(args.output, {"prepared": str(prepared), "revision": revision})
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
            "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            * (1 if sys.platform == "darwin" else 1024),
            "counters": dict(spies.counts),
            "output_sha256": output_hashes(output),
            "diagnostic": bool(
                args.spies or args.memray or args.pyinstrument or os.getenv("GPIO_DUCKDB_PROFILE")
            ),
        }
    if instrument is not None:
        instrument.stop()
        instrument.write_html(args.pyinstrument)
    write_json(args.output, row)


def run_matrix(args):
    from geoparquet_io.benchmarks.matrix import COMMANDS
    from geoparquet_io.benchmarks.workloads import generate_workload

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
            if prepared_metadata.get("revision") != revision:
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
    from geoparquet_io.benchmarks.matrix import COMMANDS

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
