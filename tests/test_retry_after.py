"""The one ``Retry-After`` parser shared by the ArcGIS, WFS and Carto retry loops (#1052)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from geoparquet_io.core.http_retry import MAX_RETRY_AFTER, parse_retry_after

NOW = datetime(2026, 10, 21, 7, 28, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("7", 7.0),
        (" 7 ", 7.0),
        ("0", 0.0),
        ("1.5", 1.5),
        ("60", 60.0),
    ],
)
def test_delay_seconds_are_honoured_as_given(raw, expected):
    assert parse_retry_after(raw) == expected


@pytest.mark.parametrize(
    "raw", [None, "", "   ", "soon", "-5", "inf", "nan", "1,5", "Wed, 99 Foo 2026"]
)
def test_an_absent_or_unreadable_value_is_none(raw):
    assert parse_retry_after(raw) is None


def test_a_long_delay_is_capped_and_says_so(caplog):
    with caplog.at_level("WARNING"):
        assert parse_retry_after("86400") == MAX_RETRY_AFTER

    assert "86400" in caplog.text
    assert f"{MAX_RETRY_AFTER:g}" in caplog.text


def test_a_delay_at_the_cap_is_not_reported_as_clamped(caplog):
    with caplog.at_level("WARNING"):
        assert parse_retry_after(str(int(MAX_RETRY_AFTER))) == MAX_RETRY_AFTER

    assert caplog.text == ""


def test_an_http_date_is_the_seconds_until_that_date():
    assert parse_retry_after("Wed, 21 Oct 2026 07:28:12 GMT", now=NOW) == 12.0


def test_an_http_date_far_ahead_is_capped(caplog):
    with caplog.at_level("WARNING"):
        assert parse_retry_after("Thu, 22 Oct 2026 07:28:00 GMT", now=NOW) == MAX_RETRY_AFTER

    assert "capping" in caplog.text.lower()


def test_an_http_date_in_the_past_means_retry_now():
    assert parse_retry_after("Tue, 20 Oct 2026 07:28:00 GMT", now=NOW) == 0.0


def test_an_http_date_without_a_zone_is_read_as_utc():
    # parsedate_to_datetime gives a naive datetime for the "-0000" zone.
    assert parse_retry_after("Wed, 21 Oct 2026 07:28:05 -0000", now=NOW) == 5.0


def test_an_http_date_defaults_to_the_current_clock():
    soon = datetime.now(timezone.utc) + timedelta(seconds=30)
    delay = parse_retry_after(soon.strftime("%a, %d %b %Y %H:%M:%S GMT"))

    assert delay is not None
    assert 0.0 <= delay <= 30.0
