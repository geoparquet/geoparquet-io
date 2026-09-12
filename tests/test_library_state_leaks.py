"""Regression tests for library-mode state leaks.

Global side effects that make gpio unsafe to embed in a host application:

- F1: ``write_parquet_with_metadata`` mutated the caller-supplied
  ``extra_kv_metadata`` dict in place, so a dict reused across writes
  accumulated preserved keys from prior files.
- F2: the S3 write/upload paths set ``AWS_PROFILE`` in the process
  environment and never restored it, so a single ``.write(profile=...)``
  bled the profile into every later, unrelated call. The profile is passed
  explicitly to :func:`geoparquet_io.core.upload.upload` instead, so no
  process-wide environment mutation is needed at all.
- F3: ``configure_verbose()`` stamped a level onto the shared
  ``geoparquet_io`` logger, clobbering a level the host application chose
  and truncating an outer ``--verbose`` run from nested default calls.
- F4: the first library call bootstrapped ``setup_cli_logging()``, attaching
  a stream handler to a logger that still propagates, so every gpio message
  appeared twice in a host application that had configured logging itself.
"""

import io
import logging
import os
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.logging_config import _bootstrap_default_handler
from geoparquet_io.core.logging_config import logger as gpio_logger

S3_DEST = "s3://fake-bucket/out.parquet"


@pytest.fixture(autouse=True)
def restore_logging_state():
    """Snapshot and restore every logger this file touches.

    Several tests here call ``setup_cli_logging()``, which clears and replaces
    the handlers on the shared ``geoparquet_io`` logger. Without this fixture
    the state-leak test file would itself leak state into later tests.
    """
    root = logging.getLogger()
    saved_level = gpio_logger.level
    saved_handlers = list(gpio_logger.handlers)
    saved_propagate = gpio_logger.propagate
    saved_root_level = root.level
    saved_root_handlers = list(root.handlers)
    try:
        yield
    finally:
        gpio_logger.setLevel(saved_level)
        gpio_logger.handlers[:] = saved_handlers
        gpio_logger.propagate = saved_propagate
        root.setLevel(saved_root_level)
        root.handlers[:] = saved_root_handlers


# ---------------------------------------------------------------------------
# F1 - write_parquet_with_metadata must not mutate the caller's dict
# ---------------------------------------------------------------------------


def test_write_does_not_mutate_caller_extra_kv_dict(buildings_test_file, tmp_path):
    """Reusing one extra_kv_metadata dict across writes must not accumulate keys."""
    from geoparquet_io.core.common import write_parquet_with_metadata
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection(load_spatial=True)
    query = f"SELECT * FROM read_parquet('{buildings_test_file}')"
    caller_dict: dict[str, str] = {}
    out_a = tmp_path / "out_a.parquet"
    out_b = tmp_path / "out_b.parquet"

    try:
        write_parquet_with_metadata(
            con,
            query,
            str(out_a),
            original_metadata={"custom_a": '{"x": 1}'},
            extra_kv_metadata=caller_dict,
            geoparquet_version="1.1",
        )
        assert caller_dict == {}, "caller dict mutated after first write"

        write_parquet_with_metadata(
            con,
            query,
            str(out_b),
            original_metadata={"custom_b": '{"y": 2}'},
            extra_kv_metadata=caller_dict,
            geoparquet_version="1.1",
        )
        assert caller_dict == {}, "caller dict mutated after second write"
    finally:
        con.close()

    # Each output still carries its own preserved key, with no cross-file leak.
    meta_a = pq.read_metadata(str(out_a)).metadata
    meta_b = pq.read_metadata(str(out_b)).metadata
    assert b"custom_a" in meta_a
    assert b"custom_b" in meta_b
    assert b"custom_b" not in meta_a, "second file's key leaked into the first"
    assert b"custom_a" not in meta_b, "first file's key leaked into the second"


def test_caller_extra_kv_wins_over_preserved_key(buildings_test_file, tmp_path):
    """A caller-supplied key must beat a same-named key preserved from the input.

    This pins the merge *precedence*, not just the absence of mutation: with the
    merge order flipped, the preserved input value would overwrite the caller's.
    """
    from geoparquet_io.core.common import write_parquet_with_metadata
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection(load_spatial=True)
    query = f"SELECT * FROM read_parquet('{buildings_test_file}')"
    caller_dict = {"shared": "from-caller", "caller_only": "kept"}
    out = tmp_path / "out.parquet"

    try:
        write_parquet_with_metadata(
            con,
            query,
            str(out),
            original_metadata={"shared": "from-input", "input_only": "preserved"},
            extra_kv_metadata=caller_dict,
            geoparquet_version="1.1",
        )
    finally:
        con.close()

    assert caller_dict == {
        "shared": "from-caller",
        "caller_only": "kept",
    }, "caller dict mutated"

    meta = pq.read_metadata(str(out)).metadata
    assert meta[b"shared"] == b"from-caller", "preserved input key overwrote the caller's"
    assert meta[b"caller_only"] == b"kept"
    assert meta[b"input_only"] == b"preserved"


# ---------------------------------------------------------------------------
# F2 - the S3 write/upload paths must not touch AWS_PROFILE at all
# ---------------------------------------------------------------------------


def _recording_upload(seen: list):
    """Patch stand-in for upload() that records the env it actually ran under."""

    def _record(*args, **kwargs):
        seen.append({"env_profile": os.environ.get("AWS_PROFILE"), "kwargs": kwargs})

    return _record


def test_api_write_to_s3_passes_profile_without_touching_env(buildings_test_file, monkeypatch):
    """A profile= write to S3 credentials the upload explicitly, not via env."""
    import geoparquet_io as gpio

    monkeypatch.setenv("AWS_PROFILE", "original-profile")
    seen: list = []
    with patch("geoparquet_io.core.upload.upload", side_effect=_recording_upload(seen)):
        gpio.read(buildings_test_file).write(S3_DEST, profile="write-profile")

    assert len(seen) == 1, "upload should have been invoked for the remote write"
    assert seen[0]["kwargs"].get("profile") == "write-profile", (
        "the profile must reach upload() as an explicit argument"
    )
    assert seen[0]["env_profile"] == "original-profile", (
        "the write must not rewrite AWS_PROFILE in the host process"
    )
    assert os.environ.get("AWS_PROFILE") == "original-profile"


def test_api_write_to_s3_leaves_unset_aws_profile_unset(buildings_test_file, monkeypatch):
    """A profile= write to S3 must not invent an AWS_PROFILE that was never set."""
    import geoparquet_io as gpio

    monkeypatch.delenv("AWS_PROFILE", raising=False)
    seen: list = []
    with patch("geoparquet_io.core.upload.upload", side_effect=_recording_upload(seen)):
        gpio.read(buildings_test_file).write(S3_DEST, profile="write-profile")

    assert len(seen) == 1
    assert seen[0]["kwargs"].get("profile") == "write-profile"
    assert seen[0]["env_profile"] is None
    assert "AWS_PROFILE" not in os.environ


def test_api_upload_to_s3_passes_profile_without_touching_env(buildings_test_file, monkeypatch):
    """Table.upload() must credential explicitly too - no AWS_PROFILE leak."""
    import geoparquet_io as gpio

    monkeypatch.setenv("AWS_PROFILE", "original-profile")
    seen: list = []
    with patch("geoparquet_io.core.upload.upload", side_effect=_recording_upload(seen)):
        gpio.read(buildings_test_file).upload(S3_DEST, profile="upload-profile")

    assert len(seen) == 1
    assert seen[0]["kwargs"].get("profile") == "upload-profile"
    assert seen[0]["env_profile"] == "original-profile"
    assert os.environ.get("AWS_PROFILE") == "original-profile"


def test_api_write_non_parquet_to_s3_does_not_leak_profile(buildings_test_file, monkeypatch):
    """The non-parquet .write() branch (.fgb/.gpkg/...) must not leak either."""
    import geoparquet_io as gpio

    monkeypatch.setenv("AWS_PROFILE", "original-profile")
    seen: list = []
    with patch("geoparquet_io.core.upload.upload", side_effect=_recording_upload(seen)):
        gpio.read(buildings_test_file).write("s3://fake-bucket/out.fgb", profile="fgb-profile")

    assert len(seen) == 1
    assert seen[0]["kwargs"].get("profile") == "fgb-profile"
    assert seen[0]["env_profile"] == "original-profile"
    assert os.environ.get("AWS_PROFILE") == "original-profile"


def test_no_opt_in_aws_profile_scope_helper():
    """There must be one profile mechanism, not a safe/unsafe pair to choose from."""
    import geoparquet_io.core.remote as remote

    assert not hasattr(remote, "aws_profile_scope"), (
        "aws_profile_scope was replaced by explicit profile= arguments"
    )


# ---------------------------------------------------------------------------
# F3 - configure_verbose must not stamp a level on the shared logger
# ---------------------------------------------------------------------------


def test_configure_verbose_false_keeps_host_chosen_level():
    """A default (verbose=False) library call must not clobber the host's level."""
    from geoparquet_io.core.logging_config import (
        configure_verbose,
        get_logger,
        setup_cli_logging,
    )

    setup_cli_logging(verbose=False)
    get_logger().setLevel(logging.WARNING)

    configure_verbose(False)

    assert get_logger().level == logging.WARNING, (
        "configure_verbose(False) force-stamped a level over the host's choice"
    )


def test_configure_verbose_false_does_not_truncate_an_outer_verbose_run():
    """A nested default call must not drop the level of an outer --verbose run."""
    from geoparquet_io.core.logging_config import (
        configure_verbose,
        get_logger,
        setup_cli_logging,
    )

    setup_cli_logging(verbose=False)
    configure_verbose(True)  # the CLI's --verbose entry point
    assert get_logger().level == logging.DEBUG

    configure_verbose(False)  # a nested core call with its default verbosity
    assert get_logger().level == logging.DEBUG, (
        "a nested verbose=False call silently truncated the verbose run"
    )


def test_first_library_call_keeps_host_chosen_level():
    """Bootstrapping a handler must not override a level the host already set."""
    from geoparquet_io.core.logging_config import configure_verbose, get_logger

    logger = get_logger()
    logger.handlers.clear()
    logger.setLevel(logging.WARNING)

    configure_verbose(False)

    assert logger.handlers, "a handler should still be bootstrapped for non-CLI usage"
    assert logger.level == logging.WARNING, (
        "the first gpio call overrode the host application's logger level"
    )


def test_first_library_call_does_not_duplicate_host_configured_output():
    """When the host has configured logging, gpio must not add a second handler.

    ``setup_cli_logging()`` attaches a stream handler and leaves ``propagate``
    True, so bootstrapping it inside a host application that already called
    ``logging.basicConfig()`` emitted every gpio message twice: once through
    gpio's handler and once through the host's. Library convention is a
    ``NullHandler`` plus propagation.
    """
    from geoparquet_io.core.logging_config import configure_verbose, get_logger, info

    stream = io.StringIO()
    host_handler = logging.StreamHandler(stream)
    host_handler.setFormatter(logging.Formatter("HOST:%(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [host_handler]
    root.setLevel(logging.INFO)

    logger = get_logger()
    logger.handlers.clear()
    logger.setLevel(logging.NOTSET)

    configure_verbose(False)

    assert logger.handlers, "the bootstrap must still mark the logger as handled"
    assert all(isinstance(h, logging.NullHandler) for h in logger.handlers), (
        "gpio installed a stream handler on top of the host's logging config"
    )
    assert logger.level == logging.NOTSET, (
        "gpio stamped a level onto a logger the host left inheriting from root"
    )

    info("one message")
    assert stream.getvalue().count("one message") == 1, "the host saw the message twice"


def test_gpio_call_does_not_reconfigure_the_root_logger(buildings_test_file, tmp_path):
    """A gpio call must never touch the root logger (e.g. via logging.basicConfig)."""
    import geoparquet_io as gpio

    root = logging.getLogger()
    before_level = root.level
    before_handlers = list(root.handlers)

    gpio.read(buildings_test_file).write(tmp_path / "out.parquet")

    assert root.level == before_level, "gpio changed the root logger level"
    assert list(root.handlers) == before_handlers, "gpio added/removed a root handler"


def test_cli_verbose_output_survives_nested_default_calls(buildings_test_file, tmp_path):
    """An end-to-end --verbose run must keep emitting debug after spec validation.

    ``gpio check all --fix`` runs the spec validator (a nested verbose=False
    call) before the fix stage. When the shared logger's level was lowered by
    that nested call, every later debug() line in the command vanished.
    """
    import shutil

    target = tmp_path / "b.parquet"
    shutil.copy(buildings_test_file, target)

    result = CliRunner().invoke(cli, ["check", "all", str(target), "--verbose", "--fix"])

    assert result.exit_code == 0, result.output
    combined = result.stdout + result.stderr
    for expected in ("Adding column 'bbox'...", "Creating column 'bbox'...", "Schema fields:"):
        assert expected in combined, f"verbose line lost after spec validation: {expected!r}"

    # This test is about the logger, but it ran a real `check all --fix` over a
    # real file, so it is also a place the WP-1 oracle (#1018) costs nothing.
    from tests.fix_output_oracle import CRS84, assert_fix_output_is_sound

    # 1.1, not the 1.0 it came in as: adding a bbox column means declaring it in
    # a `covering`, and `covering` is a 1.1-only key (#686).
    assert_fix_output_is_sound(
        target,
        expected_rows=42,
        expected_crs=CRS84,
        expects_covering=True,
        expected_version_prefix="1.1",
    )


# ---------------------------------------------------------------------------
# Bootstrap when NOTHING has configured logging (a bare script importing gpio)
# ---------------------------------------------------------------------------


def test_bootstrap_installs_a_real_handler_when_nothing_configured_logging():
    """With an unconfigured root logger, gpio must still emit output.

    The duplicate-output fix short-circuits to a NullHandler whenever the host
    already has root handlers. Under pytest that is always true, so the other
    branch -- a bare script that imports gpio and configures nothing -- needs
    the root handlers cleared to be reachable at all.
    """
    root = logging.getLogger()
    root.handlers[:] = []
    gpio_logger.handlers[:] = []
    gpio_logger.setLevel(logging.NOTSET)

    _bootstrap_default_handler(verbose=False)

    assert gpio_logger.handlers, "gpio installed no handler; output would be dropped"
    assert not all(isinstance(h, logging.NullHandler) for h in gpio_logger.handlers), (
        "gpio installed only a NullHandler with no host handlers to propagate to"
    )


def test_bootstrap_restores_a_level_the_caller_chose_explicitly():
    """setup_cli_logging() sets a level; an explicit caller choice must win.

    Covers the `chosen_level != NOTSET` restore -- without it, a script that
    silences gpio before its first call would have that choice overwritten by
    the bootstrap.
    """
    root = logging.getLogger()
    root.handlers[:] = []
    gpio_logger.handlers[:] = []
    gpio_logger.setLevel(logging.WARNING)

    _bootstrap_default_handler(verbose=False)

    assert gpio_logger.level == logging.WARNING, (
        "bootstrap overwrote a level the caller set before the first gpio call"
    )


def test_bootstrap_leaves_notset_level_to_setup_cli_logging():
    """A caller who chose nothing gets the CLI default, not a forced NOTSET."""
    root = logging.getLogger()
    root.handlers[:] = []
    gpio_logger.handlers[:] = []
    gpio_logger.setLevel(logging.NOTSET)

    _bootstrap_default_handler(verbose=False)

    assert gpio_logger.level != logging.NOTSET
