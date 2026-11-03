"""Native-network and extension-load controls, each in a bounded subprocess."""

import argparse
import json
import os
import platform
import socket
import subprocess
import sys
import time
from pathlib import Path

URL = "http://127.0.0.1:1/api/v2/sql?q=SELECT%201&format=GeoJSON"
OUT = Path(__file__).resolve().parents[2] / "ci-evidence" / platform.system() / "native"


def timed(label, function):
    start, cpu = time.perf_counter(), time.process_time()
    try:
        function()
        outcome = "ok"
    except Exception as error:
        outcome = f"{type(error).__name__}: {str(error)[:250]}"
    print(
        json.dumps(
            {
                "label": label,
                "wall": time.perf_counter() - start,
                "cpu": time.process_time() - cpu,
                "outcome": outcome,
            }
        ),
        flush=True,
    )


def worker(case):
    if case == "socket":
        for i in range(3):
            timed(f"socket-{i}", lambda: socket.create_connection(("127.0.0.1", 1), timeout=2))
        return
    if case == "httpx":
        import httpx  # noqa: TID251 -- Isolated transport control, deliberately bypasses application retries.

        for i in range(3):
            timed(f"httpx-{i}", lambda: httpx.get(URL, timeout=2, trust_env=False))
        return
    import duckdb

    if case == "connections":
        for repeat in range(3):

            def fresh():
                for _ in range(20):
                    connection = duckdb.connect(config={"threads": 2})
                    connection.execute("LOAD spatial")
                    connection.execute("SELECT ST_AsText(ST_Point(1, 2))").fetchall()
                    connection.close()

            timed(f"fresh-20-{repeat}", fresh)
            root = duckdb.connect(config={"threads": 2})
            root.execute("LOAD spatial")

            def cursors(root=root):
                for _ in range(20):
                    cursor = root.cursor()
                    cursor.execute("SELECT ST_AsText(ST_Point(1, 2))").fetchall()
                    cursor.close()

            timed(f"cursors-20-{repeat}", cursors)
            root.close()
        return
    connection = duckdb.connect(config={"threads": 2})
    timed("load-spatial", lambda: connection.execute("LOAD spatial"))
    connection.execute("SET allow_asterisks_in_http_paths = true")
    connection.execute("SET http_retry_wait_ms = 0")
    connection.execute(
        "SET http_timeout = 1" if case == "st-read-one-second" else "SET http_timeout = 30000"
    )
    if case == "st-read-no-retries":
        connection.execute("SET http_retries = 0")
    print(
        json.dumps(
            {
                "settings": connection.sql(
                    "SELECT name,value,description FROM duckdb_settings() WHERE name IN "
                    "('http_timeout','http_retries','http_retry_wait_ms')"
                ).fetchall()
            }
        ),
        flush=True,
    )
    timed(case, lambda: connection.sql("SELECT * FROM ST_Read(?)", params=[URL]).arrow().read_all())
    connection.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--case")
    args = parser.parse_args()
    if args.case:
        worker(args.case)
        return
    OUT.mkdir(parents=True, exist_ok=True)
    recording = False
    if os.name == "nt":
        with (OUT / "wpr.log").open("w") as log:
            try:
                result = subprocess.run(
                    ["wpr", "-start", "GeneralProfile", "-filemode"],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    timeout=30,
                    check=False,
                )
                recording = result.returncode == 0
            except (OSError, subprocess.TimeoutExpired) as error:
                log.write(str(error))
    try:
        for case in [
            "socket",
            "httpx",
            "st-read-default",
            "st-read-one-second",
            "st-read-no-retries",
            "st-read-gdal-env",
            "connections",
        ]:
            env = os.environ.copy()
            if case == "st-read-gdal-env":
                env.update(
                    GDAL_HTTP_CONNECTTIMEOUT="1", GDAL_HTTP_TIMEOUT="1", GDAL_HTTP_MAX_RETRY="0"
                )
            env.update(CPL_CURL_VERBOSE="YES", CPL_TIMESTAMP="ON")
            command = [
                sys.executable,
                "-m",
                "cProfile",
                "-o",
                str(OUT / f"{case}.prof"),
                __file__,
                "--case",
                case,
            ]
            with (OUT / f"{case}.log").open("w", encoding="utf-8") as log:
                start = time.monotonic()
                try:
                    result = subprocess.run(
                        command,
                        env=env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        timeout=100,
                        check=False,
                    )
                    status = result.returncode
                except subprocess.TimeoutExpired:
                    status = "outer-timeout"
            print(
                json.dumps({"case": case, "status": status, "wall": time.monotonic() - start}),
                flush=True,
            )
            print(
                (OUT / f"{case}.log").read_text(encoding="utf-8", errors="replace")[-4000:],
                flush=True,
            )
    finally:
        if recording:
            with (OUT / "wpr.log").open("a") as log:
                subprocess.run(
                    ["wpr", "-stop", str(OUT / "native.etl")],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    timeout=60,
                    check=False,
                )


if __name__ == "__main__":
    main()
