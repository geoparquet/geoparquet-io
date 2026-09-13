# ADR-0007: The Console Script Leaves via `os._exit`

## Status

Accepted

## Context

A `gpio` run loads DuckDB, its spatial extension, Arrow, GEOS and PROJ. When
the command has finished and printed its last line, CPython's interpreter
finalization unloads all of them and destroys their process-global state while
their worker threads are torn down. That code is not gpio's and can fail after
gpio has succeeded. Measured on Linux under CPU contention (eight concurrent
loops on four CPUs), roughly one run in twenty of `gpio add geometry-metrics`
printed its success line, wrote a correct file, and then died with
`terminate called without an active exception` and status 134 (#1053). A
shell's `set -e` and CI both read that as a failed command.

The abort has no Python frame and DuckDB is absent from the extension module
list at the point of failure, which places it in module teardown or C++ static
destruction, after the atexit handlers have run. gpio has no connection
registry to close first (every `get_duckdb_connection` is a fresh connection,
closed by its caller), and neither DuckDB nor pyarrow offers a shutdown API for
their thread pools. There is nothing narrower to fix in gpio.

## Decision

The `gpio` console script is `geoparquet_io.cli.main:main`, which runs the root
group as Click would and then ends the process itself: it runs the atexit
handlers, flushes `stdout` and `stderr`, and calls `os._exit` with the status
Click chose. Interpreter finalization, the part that aborted, never runs.

Three limits keep the blast radius small:

1. **Only the console script.** `cli` stays a plain Click group. `CliRunner`,
   `cli(standalone_mode=False)` and the Python API return as before; a program
   that embeds gpio is never `os._exit`ed.
2. **Atexit handlers run.** `logging.shutdown`, coverage's data write, and
   pyarrow's S3 finalizer all run. What is skipped is module teardown and
   static destructors, which is where the evidence puts the abort.
3. **An escape hatch.** `GPIO_INTERPRETER_EXIT=1` leaves through `sys.exit`
   instead, for anyone debugging the teardown itself.

A flush that fails because the reader went away (`| head`) or the stream was
closed keeps the command's status. Any other `OSError` on the flush, a full
disk for instance, exits 120, which is what CPython does on the same failure.

## Consequences

### Positive
- A run that wrote its output is reported as a success on every platform.
- The exit status is the one Click chose: 0, 1 for a reported failure, 2 for
  a usage error, unchanged from before.

### Negative
- Module-level `__del__` methods and C++ static destructors never run for a
  `gpio` process. gpio registers none and relies on none; every temporary
  file is removed in a `finally` before the command returns, and DuckDB spill
  directories orphaned by any hard exit are swept at the next start.
- A future subprocess-coverage setup must rely on coverage's atexit hook,
  which does run, not on interpreter finalization.

### Neutral
- The mechanism lives in one function next to the entry point, with a test
  that probes both an atexit handler (must run) and a module `__del__` (must
  not).

## Alternatives Considered

### Close DuckDB connections explicitly before exit
There is nothing to close: instrumenting `duckdb.connect` on the failing
command showed four connections created and none open at atexit. The abort is
in native teardown, not in a leaked handle.

### Change import order (geoarrow before or after duckdb)
Changes Python-level teardown order, not the C++ static destruction that
`std::terminate` comes from. Not reproducible enough locally to prove either
way, and fragile even if it helped.

### `os._exit` on the Click group's `__call__`
The first shape of this change. It also fired for `cli(standalone_mode=False)`
callers, which are documented Click embedding, and it put process-lifecycle
plumbing on a `cls=` class in `cli/decorators.py`. Moved to the entry point.

### Skip the atexit handlers too
Cheaper, but it forfeits `logging.shutdown` and coverage's subprocess data
write for every run, to avoid a crash the evidence places after them.

## References

- Issue #1053, PR #1073
- `geoparquet_io/cli/main.py`: `main`, `_leave_process`, `_process_status`
- `tests/test_cli_process_exit.py`
- `docs/troubleshooting.md`, "A Command Succeeded but the Process Exited 134"
