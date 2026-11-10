"""``--timeout`` is one declaration across the service extracts (#1367 G13).

``extract arcgis`` took a positive float and ``extract carto`` an int from 1 to
3600; both are seconds handed to an HTTP client that takes a float. Each keeps
its own default (a per-service budget).
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli

COMMANDS = {
    "arcgis": ["extract", "arcgis", "https://gpio.invalid/arcgis/rest/services/x/FeatureServer/0"],
    "carto": ["extract", "carto", "https://gpio-test.carto.com", "table"],
}


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    import socket

    def refuse(*args, **kwargs):
        raise OSError("no network in this test")

    monkeypatch.setattr(socket, "getaddrinfo", refuse)


@pytest.mark.parametrize("command", sorted(COMMANDS))
@pytest.mark.parametrize("value", ["0", "-1", "soon"])
def test_a_non_positive_timeout_is_a_usage_error(command, value, tmp_path):
    args = [*COMMANDS[command], str(tmp_path / "o.parquet"), "--timeout", value]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 2, result.output
    assert "Invalid value for '--timeout'" in result.output


@pytest.mark.parametrize("command", sorted(COMMANDS))
@pytest.mark.parametrize("value", ["2.5", "7200"])
def test_a_positive_number_of_seconds_is_accepted(command, value, tmp_path):
    args = [*COMMANDS[command], str(tmp_path / "o.parquet"), "--timeout", value]
    result = CliRunner().invoke(cli, args)
    assert "Invalid value for '--timeout'" not in result.output
    assert result.exit_code != 2, result.output
