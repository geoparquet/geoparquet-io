"""Temporary diagnostic plugin; never imported by ordinary test runs."""

import faulthandler
import json
import os
import sys
import time
from pathlib import Path

import pytest

_out = None
_stacks = None
_started = time.perf_counter()


def record(event, **values):
    if _out is not None:
        _out.write(
            json.dumps(
                {"event": event, "pid": os.getpid(), "t": time.perf_counter() - _started, **values}
            )
            + "\n"
        )
        _out.flush()


def pytest_configure(config):
    global _out, _stacks
    directory = Path(os.environ["GPIO_CI_PROBE_DIR"])
    directory.mkdir(parents=True, exist_ok=True)
    _out = (directory / f"events-{os.getpid()}.jsonl").open("w", encoding="utf-8")
    _stacks = (directory / f"stacks-{os.getpid()}.txt").open("w", encoding="utf-8")
    faulthandler.enable(file=_stacks)
    faulthandler.dump_traceback_later(25, repeat=True, file=_stacks)
    record("configure", worker=getattr(config, "workerinput", {}).get("workerid", "controller"))


def pytest_collection_finish(session):
    record("collection_finish", count=len(session.items))


def pytest_runtest_logstart(nodeid, location):
    record("logstart", nodeid=nodeid)


def pytest_runtest_logreport(report):
    record(
        "report",
        nodeid=report.nodeid,
        phase=report.when,
        outcome=report.outcome,
        duration=report.duration,
    )


def _trace(frame, event, arg):
    filename = frame.f_code.co_filename.replace("\\", "/")
    function = frame.f_code.co_name
    if filename.endswith(("/core/carto.py", "/core/duckdb_utils.py", "/test_carto.py")):
        if event in {"call", "return"} and function in {
            "attempt",
            "classify",
            "_carto_status_of",
            "_probe_request_status",
            "get_duckdb_connection",
            "immediate_connection",
            "execute",
            "sql",
            "_install_and_load_extension",
            "load_spatial_extension",
        }:
            record(event, function=function, filename=filename.rsplit("/", 1)[-1])


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item):
    previous = sys.getprofile()
    traced = "test_a_real_dead_port_fetch_retries_end_to_end" in item.nodeid and previous is None
    if traced:
        sys.setprofile(_trace)
    try:
        return (yield)
    finally:
        if traced:
            sys.setprofile(previous)


def pytest_sessionfinish(session, exitstatus):
    record("sessionfinish", exitstatus=int(exitstatus))
    faulthandler.cancel_dump_traceback_later()
