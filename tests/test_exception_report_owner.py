"""Caught-exception text reaches the user only through the sanitizing owners (#1367, #1034).

An ``except ... as e`` handler (or a function handed an exception) that formats
the exception straight into ``click.echo``, ``print``, a ``ClickException`` or
``could_not_run`` prints text nobody cleaned: httpx, OWSLib and DuckDB all embed
the request URL, query string and credentials included. The owners are
``core.exceptions.clean_error_text`` (inside ``GeoParquetError`` and
``logging_config.warn``/``error``) and ``cli.exception_handler.error_text`` /
``click_error_for``

A site is flagged when it calls a reporter with an argument that mentions the
exception and is not wrapped in one of those. ``warn``/``error`` are not
reporters here: the logging layer cleans once. The AST guard and its exemptions
live in tests/test_owner_guards.py and scripts/owner_exemptions.toml.
"""

from __future__ import annotations

import pathlib

PACKAGE = pathlib.Path(__file__).resolve().parent.parent / "geoparquet_io"


def test_logging_redacts_credentials_once(caplog):
    """``warn``/``error`` are the single point for logged exception text."""
    import logging

    from geoparquet_io.core.logging_config import error, warn

    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        warn("could not read s3://k:secret@bucket/x.parquet (api_key=SECRETKEY)")
        error("failed with token: SECRETTOKEN on s3://bucket/y.parquet")
    assert "secret@" not in caplog.text and "SECRETKEY" not in caplog.text
    assert "SECRETTOKEN" not in caplog.text
    # URLs are otherwise intact: the line still names the file it is about.
    assert "s3://k:***@bucket/x.parquet" in caplog.text
    assert "s3://bucket/y.parquet" in caplog.text


def test_logging_applies_the_one_error_policy(caplog):
    """An Azure SAS ``sig=`` and an ANSI escape do not survive ``warn()``."""
    import logging

    from geoparquet_io.core.logging_config import warn

    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        warn(
            "could not read https://acct.blob.core.windows.net/c/x.parquet?sv=2020&sig=SASSECRET \x1b[2Kdone"
        )
    assert "SASSECRET" not in caplog.text
    assert "\x1b" not in caplog.text
    assert "https://acct.blob.core.windows.net/c/x.parquet" in caplog.text
