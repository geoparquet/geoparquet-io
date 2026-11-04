#!/usr/bin/env python3
"""Collect diagnostic runs separately, then rank measured command costs."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


def operators(directory):
    from geoparquet_io.core.benchmark import parse_query_plan

    totals = defaultdict(float)
    peaks = []

    def peak_memory(node):
        if isinstance(node, dict):
            if "system_peak_buffer_memory" in node:
                peaks.append(node["system_peak_buffer_memory"])
            for value in node.values():
                peak_memory(value)
        elif isinstance(node, list):
            for value in node:
                peak_memory(value)

    for path in directory.glob("*.json"):
        plan = json.loads(path.read_text())
        for operator in parse_query_plan(plan)["operators"]:
            if operator["name"] != "UNKNOWN":
                totals[operator["name"]] += operator["timing"]
        peak_memory(plan)
    return sorted(totals.items(), key=lambda item: -item[1]), max(peaks, default=0)


def sampled_symbol(path):
    data = json.loads(path.read_text())
    frames = data["shared"]["frames"]
    counts = defaultdict(float)
    total = 0
    for profile in data["profiles"]:
        for stack, weight in zip(
            profile.get("samples", []), profile.get("weights", []), strict=True
        ):
            if not stack:
                continue
            frame = frames[stack[-1]]
            key = f"{frame['name']} ({frame.get('file', '')}:{frame.get('line', '')})"
            counts[key] += weight
            total += weight
    if not total:
        return "no samples", 0
    symbol, count = max(counts.items(), key=lambda item: item[1])
    return symbol, count / total * 100


def allocation_site(path):
    import memray

    records = list(memray.FileReader(path).get_high_watermark_allocation_records())
    totals = defaultdict(int)
    for record in records:
        stack = record.hybrid_stack_trace()
        frame = next(iter(stack), ("unknown", "", 0))
        totals[f"{frame[0]} ({frame[1]}:{frame[2]})"] += record.size
    if not totals:
        return "no allocations", 0, 0
    site, size = max(totals.items(), key=lambda item: item[1])
    return site, size, sum(totals.values())


def diagnose(matrix):
    from geoparquet_io.benchmarks.matrix import COMMANDS
    from geoparquet_io.benchmarks.workloads import generate_workload

    rows = json.loads(matrix.read_text())
    groups = defaultdict(list)
    for row in rows:
        groups[row["command"]].append(row)
    ranked = sorted(groups, key=lambda name: -min(r["wall_seconds"] for r in groups[name]))
    specs = {s["id"]: s for s in COMMANDS}
    summaries = []
    for name in ranked:
        samples = groups[name]
        wall = min(r["wall_seconds"] for r in samples)
        rss = max(r["peak_rss_bytes"] for r in samples)
        spec = specs[name]
        root = matrix.parent / "diagnostics" / name
        root.mkdir(parents=True, exist_ok=True)
        input_path = generate_workload(
            spec["workload"], matrix.parent / "inputs", samples[0]["size"]
        )

        prepared_flags = []
        if spec.get("prepare") or spec.get("kind") == "overview":
            prepared = json.loads((matrix.parent / "fixtures" / name / "prepared.json").read_text())
            prepared_flags = ["--prepared-path", prepared["prepared"]]

        def child(
            label,
            *flags,
            name=name,
            size=samples[0]["size"],
            input_path=input_path,
            root=root,
            prepared_flags=prepared_flags,
        ):
            return [
                sys.executable,
                "-m",
                "scripts.profile_command",
                "--child",
                "--command",
                name,
                "--size",
                size,
                "--input-path",
                str(input_path),
                "--work-dir",
                str(root / label),
                "--output",
                str(root / f"{label}.json"),
                *prepared_flags,
                *flags,
            ]

        if not (root / "spies.json").exists():
            shutil.rmtree(root / "spies", ignore_errors=True)
            env = os.environ.copy()
            env["GPIO_DUCKDB_PROFILE"] = str(root / "duckdb")
            with (root / "spies.log").open("w") as log:
                subprocess.run(
                    child("spies", "--spies"),
                    check=True,
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
        op, buffer_peak = operators(root / "duckdb")
        summary = {
            "command": name,
            "size": samples[0]["size"],
            "wall_seconds": wall,
            "peak_rss_bytes": rss,
            "tickets": spec["tickets"],
            "top_operator": op[0] if op else None,
            "duckdb_operator_cpu_seconds": sum(v for _, v in op),
            "duckdb_peak_buffer_bytes": buffer_peak,
            "counters": json.loads((root / "spies.json").read_text())["counters"],
        }
        if name in ranked[:20]:
            speedscope = root / "py-spy.json"
            if not speedscope.exists():
                shutil.rmtree(root / "sampling", ignore_errors=True)
                with (root / "py-spy.log").open("w") as log:
                    subprocess.run(
                        [
                            str(Path(sys.executable).parent / "py-spy"),
                            "record",
                            "--native",
                            "--format",
                            "speedscope",
                            "--output",
                            str(speedscope),
                            "--",
                            *child("sampling"),
                        ],
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=True,
                    )
            summary["top_symbol"], summary["symbol_sample_pct"] = sampled_symbol(speedscope)
            capture = root / "memray.bin"
            if not (root / "memory.json").exists():
                shutil.rmtree(root / "memory", ignore_errors=True)
                capture.unlink(missing_ok=True)
                with (root / "memray.log").open("w") as log:
                    subprocess.run(
                        child("memory", "--memray", str(capture)),
                        check=True,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                    )
            site, size, peak = allocation_site(capture)
            summary.update(
                allocation_site=site,
                allocation_bytes=size,
                memray_peak_bytes=peak,
                allocation_rss_pct=size / rss * 100,
            )
        summaries.append(summary)
        (matrix.parent / "costs.json").write_text(json.dumps(summaries, indent=2) + "\n")
        print(f"diagnosed {name}", flush=True)
    render_costs(matrix.parent, summaries)


def render_costs(directory, summaries):
    lines = [
        "# Baseline costs",
        "",
        f"Timing/RSS: three fresh, uninstrumented processes, {', '.join(sorted({r['size'] for r in summaries}))} size, cores 0–15.",
        "Wall is the minimum; RSS is the maximum Linux ru_maxrss (includes process startup).",
        "Diagnostics are separate runs. Operator seconds sum thread CPU and can exceed wall time;",
        "operator share below is of profiled operator CPU, not a wall-time attribution.",
        "py-spy shares are leaf samples of the full child lifetime, including startup; native waiting frames can dominate.",
        "Memray tracks native allocation high water, not resident pages: allocation/RSS is a ratio, not a measured resident share.",
        "No tool here can reliably assign RSS or parallel wall time to individual symbols; those shares are unavailable.",
        "",
        "| Command | wall s | peak MiB | largest sampled symbol (% samples) | largest operator (% operator CPU) | largest allocation (bytes; % process RSS) | seed ticket |",
        "|---|---:|---:|---|---|---|---|",
    ]
    for row in summaries:
        operator = row["top_operator"]
        op = (
            f"{operator[0]} ({operator[1] / row['duckdb_operator_cpu_seconds'] * 100:.1f}%)"
            if operator
            else "unavailable"
        )
        symbol = (
            f"{row['top_symbol']} ({row['symbol_sample_pct']:.1f}%)"
            if "top_symbol" in row
            else "outside top 20"
        )
        allocation = (
            f"{row['allocation_site']} ({row['allocation_bytes']}; {row['allocation_rss_pct']:.1f}%)"
            if "allocation_site" in row
            else "outside top 20"
        )
        tickets = ", ".join(f"#{n}" for n in row["tickets"]) or "unseeded"
        cells = [
            row["command"],
            f"{row['wall_seconds']:.3f}",
            f"{row['peak_rss_bytes'] / 2**20:.1f}",
            symbol,
            op,
            allocation,
            tickets,
        ]
        lines.append(
            "| " + " | ".join(str(c).replace("|", "/").replace("\n", " ") for c in cells) + " |"
        )
    (directory / "costs.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("matrix", type=Path)
    args = parser.parse_args()
    diagnose(args.matrix.resolve())


if __name__ == "__main__":
    main()
