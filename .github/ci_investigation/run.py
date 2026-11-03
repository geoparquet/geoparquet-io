"""Bounded experiment runner for the temporary CI investigation branch."""

import argparse
import importlib.metadata
import json
import os
import platform
import signal
import subprocess
import sys
import time
from pathlib import Path

import psutil  # noqa: TID251 -- Diagnostic process sampling, outside application memory policy.

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "ci-evidence" / platform.system()
TARGET = (
    "tests/test_carto.py::TestFailureClassificationIgnoresTheUsersSql::"
    "test_a_real_dead_port_fetch_retries_end_to_end"
)


def run_case(name, arguments, limit, *, profile=False, env_extra=None, instrument=True):
    directory = OUT / name
    directory.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(GPIO_CI_PROBE_DIR=str(directory), PYTHONUNBUFFERED="1")
    env["PYTHONPATH"] = os.pathsep.join(
        [str(Path(__file__).parent), str(ROOT), env.get("PYTHONPATH", "")]
    )
    env.update(env_extra or {})
    command = [sys.executable]
    if profile:
        command += ["-m", "cProfile", "-o", str(directory / "python.prof")]
    command += [
        "-m",
        "pytest",
        *(["-p", "probe_plugin"] if instrument else []),
        "--no-cov",
        "--tb=short",
        "--durations=0",
        "--durations-min=0",
        "-vv",
        "--junitxml=" + str(directory / "junit.xml"),
        *arguments,
    ]
    started = time.monotonic()
    expired = False
    with (
        (directory / "stdout.log").open("w", encoding="utf-8") as log,
        (directory / "processes.jsonl").open("w", encoding="utf-8") as samples,
    ):
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=os.name != "nt",
        )
        while process.poll() is None:
            try:
                parent = psutil.Process(process.pid)
                for child in [parent, *parent.children(recursive=True)]:
                    try:
                        cpu = child.cpu_times()
                        samples.write(
                            json.dumps(
                                {
                                    "t": time.monotonic() - started,
                                    "pid": child.pid,
                                    "ppid": child.ppid(),
                                    "name": child.name(),
                                    "threads": child.num_threads(),
                                    "rss": child.memory_info().rss,
                                    "user": cpu.user,
                                    "system": cpu.system,
                                }
                            )
                            + "\n"
                        )
                    except psutil.Error:
                        pass
                samples.flush()
            except psutil.Error:
                pass
            if time.monotonic() - started > limit:
                expired = True
                if os.name == "nt":
                    subprocess.run(
                        ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=False,
                    )
                else:
                    os.killpg(process.pid, signal.SIGKILL)
                break
            time.sleep(1)
        process.wait(timeout=20)
    result = {
        "case": name,
        "command": command,
        "wall_seconds": time.monotonic() - started,
        "returncode": process.returncode,
        "outer_timeout": expired,
    }
    (directory / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result), flush=True)
    for line in (
        (directory / "stdout.log").read_text(encoding="utf-8", errors="replace").splitlines()[-12:]
    ):
        print(line, flush=True)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=[
            "narrow",
            "suite",
            "suite-load",
            "plain-loadfile",
            "plain-load",
            "native",
            "http",
            "database",
            "carto-retry-control",
        ],
        default="narrow",
    )
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    # Workflow-contract tests must inspect the historical production workflow,
    # not this branch's manually dispatched measurement wrapper.
    workflow = ROOT / ".github/workflows/tests.yml"
    original = subprocess.run(
        ["git", "show", "552703312773cd525c0b74ce6bcb1b8737b44d00:.github/workflows/tests.yml"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    ).stdout
    (OUT / "dispatch-workflow.txt").write_text(
        workflow.read_text(encoding="utf-8"), encoding="utf-8"
    )
    workflow.write_text(original, encoding="utf-8")
    metadata = {
        "platform": platform.platform(),
        "python": sys.version,
        "cpu_count": os.cpu_count(),
        "versions": {
            name: importlib.metadata.version(name)
            for name in [
                "duckdb",
                "pytest",
                "pytest-xdist",
                "pytest-timeout",
                "pytest-rerunfailures",
                "pyarrow",
                "psutil",
            ]
        },
    }
    (OUT / "environment.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(json.dumps(metadata), flush=True)
    if args.mode == "carto-retry-control":
        run_case(
            "carto-no-native-retries-60",
            [TARGET, "-n", "0", "--timeout=60"],
            90,
            env_extra={"GPIO_CI_NATIVE_RETRIES": "0"},
        )
        return
    if args.mode in {"native", "http"}:
        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("native.py")),
                *(["--http-logs"] if args.mode == "http" else []),
            ],
            check=True,
        )
        return
    if args.mode == "database":
        selected = [
            "tests/test_check.py",
            "tests/test_sort.py",
            "tests/test_api.py",
            "-n",
            "4",
            "--dist=loadfile" if os.name == "nt" else "--dist=load",
            "-m",
            "not slow and not network and not meta",
            "--timeout=120",
        ]
        for repeat in range(3):
            for mode in ["shared", "fresh"] if repeat % 2 == 0 else ["fresh", "shared"]:
                run_case(
                    f"database-{mode}-{repeat}",
                    selected,
                    240,
                    env_extra={
                        "GPIO_CI_FORCE_FRESH": "1" if mode == "fresh" else "0",
                        "GPIO_CI_COUNT_CONNECTIONS": "1",
                    },
                )
        return
    if args.mode in {"suite", "suite-load", "plain-loadfile", "plain-load"}:
        run_case(
            f"full-fast-{args.mode}-120",
            [
                "tests",
                "-n",
                "4",
                "--dist=loadfile"
                if os.name == "nt" and args.mode in {"suite", "plain-loadfile"}
                else "--dist=load",
                "-m",
                "not slow and not network and not meta",
                "--timeout=120",
            ],
            1200,
            instrument=not args.mode.startswith("plain-"),
        )
        return
    run_case("carto-serial-120", [TARGET, "-n", "0", "--timeout=120"], 170)
    run_case("carto-profile-120", [TARGET, "-n", "0", "--timeout=120"], 170, profile=True)
    selection = ["tests/test_carto.py", "tests/test_suite_budget.py", "-n", "4", "--dist=loadfile"]
    run_case("loadfile-60", [*selection, "--timeout=60"], 200)
    run_case("loadfile-120", [*selection, "--timeout=120"], 200)
    run_case("loadfile-60-no-restart", [*selection, "--timeout=60", "--max-worker-restart=0"], 130)
    run_case(
        "loadfile-60-no-rerun-plugin", [*selection, "--timeout=60", "-p", "no:rerunfailures"], 200
    )


if __name__ == "__main__":
    main()
