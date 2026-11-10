"""``--limit`` is one declaration: a non-negative row count on every extract command (#1367 G13)."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli

COMMANDS = {
    "geoparquet": ["extract", "{f}", "{o}"],
    "arcgis": [
        "extract",
        "arcgis",
        "https://gpio.invalid/arcgis/rest/services/x/FeatureServer/0",
        "{o}",
    ],
    "bigquery": ["extract", "bigquery", "project.dataset.table", "{o}"],
    "wfs": ["extract", "wfs", "https://gpio.invalid/wfs", "layer", "{o}"],
    "carto": ["extract", "carto", "https://gpio.invalid/carto", "table", "{o}"],
}


@pytest.mark.parametrize("command", sorted(COMMANDS))
def test_a_negative_limit_is_a_usage_error(command, buildings_test_file, tmp_path, monkeypatch):
    import socket

    def no_network(*args, **kwargs):
        raise AssertionError("refused before any request")

    monkeypatch.setattr(socket, "getaddrinfo", no_network)
    out = str(tmp_path / "out.parquet")
    argv = [a.format(f=buildings_test_file, o=out) for a in COMMANDS[command]]
    result = CliRunner().invoke(cli, [*argv, "--limit", "-1"])
    assert result.exit_code == 2, result.output
    assert "Invalid value for '--limit'" in result.output


def test_zero_and_positive_limits_still_extract(buildings_test_file, tmp_path):
    import pyarrow.parquet as pq

    for n in (0, 3):
        out = tmp_path / f"out{n}.parquet"
        result = CliRunner().invoke(
            cli, ["extract", buildings_test_file, str(out), "--limit", str(n)]
        )
        assert result.exit_code == 0, result.output
        assert pq.read_metadata(out).num_rows == n
