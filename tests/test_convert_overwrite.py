"""``gpio convert`` refuses an existing output unless --overwrite (#1367, T2e review F9).

Every other writing command refused an existing output through
``file_utils.handle_output_overwrite``; ``gpio convert`` (and the
``gpio convert geoparquet`` it defaults to) silently replaced it.
"""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli


@pytest.fixture
def existing_output(tmp_path):
    out = tmp_path / "out.parquet"
    out.write_bytes(b"keep me")
    return out


@pytest.mark.parametrize("prefix", [["convert"], ["convert", "geoparquet"]])
def test_an_existing_output_is_refused(buildings_test_file, existing_output, prefix):
    result = CliRunner().invoke(cli, [*prefix, buildings_test_file, str(existing_output)])
    assert result.exit_code != 0
    assert "--overwrite" in result.output, result.output
    assert existing_output.read_bytes() == b"keep me"


@pytest.mark.parametrize("prefix", [["convert"], ["convert", "geoparquet"]])
def test_overwrite_replaces_it(buildings_test_file, existing_output, prefix):
    result = CliRunner().invoke(
        cli, [*prefix, buildings_test_file, str(existing_output), "--overwrite"]
    )
    assert result.exit_code == 0, result.output
    assert pq.read_metadata(existing_output).num_rows > 0


def test_a_new_output_needs_no_flag(buildings_test_file, tmp_path):
    out = tmp_path / "new.parquet"
    result = CliRunner().invoke(cli, ["convert", buildings_test_file, str(out)])
    assert result.exit_code == 0, result.output
    assert pq.read_metadata(out).num_rows > 0


def test_the_input_itself_is_refused_even_with_overwrite(buildings_test_file, tmp_path):
    src = tmp_path / "in.parquet"
    src.write_bytes(open(buildings_test_file, "rb").read())
    before = src.read_bytes()
    result = CliRunner().invoke(cli, ["convert", str(src), str(src), "--overwrite"])
    assert result.exit_code != 0
    assert src.read_bytes() == before
