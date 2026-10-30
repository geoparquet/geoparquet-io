"""One path classification and one overwrite policy across commands (#1367 T2e).

``core/remote`` owns the scheme table, ``core/file_utils`` the glob/partition
classification and the overwrite check. These tests pin that every command
reads the same answer from them: a remote scheme is remote everywhere, a file
whose name holds ``[`` is one file everywhere (#1243), and an existing output
is refused everywhere with the one message unless the command's switch is
given.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.file_utils import (
    get_first_parquet_file,
    has_glob_pattern,
    is_partition_path,
    validate_parquet_extension,
)
from geoparquet_io.core.remote import (
    display_path,
    is_azure_url,
    is_gcs_url,
    is_remote_url,
    is_s3_url,
    needs_httpfs,
    remote_family,
    url_scheme,
)

DATA = Path(__file__).parent / "data"
PLACES = DATA / "places_test.parquet"


def _run(*args):
    return CliRunner().invoke(cli, [str(a) for a in args], catch_exceptions=True)


def _explain(result) -> str:
    return f"exit {result.exit_code}; {result.exception!r}; output:\n{result.output[-1500:]}"


# ---------------------------------------------------------------------------
# The scheme table: one answer per path, from every test over it
# ---------------------------------------------------------------------------

#: path -> (url_scheme, family)
SCHEMES = {
    "/data/a.parquet": (None, None),
    "a.parquet": (None, None),
    "file:///data/a.parquet": (None, None),
    "ftp://host/a.parquet": (None, None),
    "S3://bucket/a.parquet": (None, None),
    "http://host/a.parquet": ("http", "http"),
    "https://host/a.parquet": ("https", "http"),
    "s3://bucket/a.parquet": ("s3", "s3"),
    "s3a://bucket/a.parquet": ("s3a", "s3"),
    "gs://bucket/a.parquet": ("gs", "gcs"),
    "gcs://bucket/a.parquet": ("gcs", "gcs"),
    "az://account/container/a.parquet": ("az", "azure"),
    "azure://container/a.parquet": ("azure", "azure"),
    "abfs://c@a.dfs.core.windows.net/a.parquet": ("abfs", "azure"),
    "abfss://c@a.dfs.core.windows.net/a.parquet": ("abfss", "azure"),
    "s3://bucket/part-*.parquet": ("s3", "s3"),
}


@pytest.mark.parametrize(("path", "expected"), SCHEMES.items(), ids=list(SCHEMES))
def test_every_scheme_test_reads_the_one_table(path, expected):
    scheme, family = expected
    assert url_scheme(path) == scheme
    assert remote_family(path) == family
    assert is_remote_url(path) is (family is not None)
    assert needs_httpfs(path) is (family in ("s3", "gcs", "azure"))
    assert is_s3_url(path) is (family == "s3")
    assert is_gcs_url(path) is (family == "gcs")
    assert is_azure_url(path) is (family == "azure")


@pytest.mark.parametrize("value", [None, 3, Path("s3:/bucket/a.parquet")])
def test_a_non_string_is_never_remote(value):
    assert url_scheme(value) is None
    assert is_remote_url(value) is False


def test_display_path_strips_secrets_from_remote_urls_only():
    signed = "https://host/a.parquet?X-Amz-Signature=secret"
    assert "secret" not in display_path(signed)
    assert display_path("/data/o'brien?.parquet") == "/data/o'brien?.parquet"


# ---------------------------------------------------------------------------
# Glob classification: a regular file is one file, whatever its name (#1243)
# ---------------------------------------------------------------------------


@pytest.fixture
def bracket_file(tmp_path) -> Path:
    path = tmp_path / "l[c.parquet"
    shutil.copy(PLACES, path)
    return path


def test_an_existing_file_named_with_glob_characters_is_one_file(tmp_path):
    for name in ("l[c.parquet", "a*b.parquet", "q?.parquet"):
        path = tmp_path / name
        shutil.copy(PLACES, path)
        assert not has_glob_pattern(str(path)), name
        assert not is_partition_path(str(path)), name
        assert get_first_parquet_file(str(path)) == str(path)


def test_a_bracket_that_names_no_file_is_still_a_glob(tmp_path):
    (tmp_path / "part1.parquet").write_bytes(b"")
    pattern = str(tmp_path / "part[0-9].parquet")
    assert has_glob_pattern(pattern)
    assert is_partition_path(pattern)
    assert get_first_parquet_file(pattern) == str(tmp_path / "part1.parquet")


def test_a_remote_bracket_is_classified_by_its_characters():
    assert has_glob_pattern("s3://bucket/l[c.parquet")


def test_a_remote_extension_is_read_from_the_url_path():
    validate_parquet_extension("s3a://bucket/dir.v2/a.parquet")
    with pytest.raises(Exception, match="does not have .parquet extension"):
        validate_parquet_extension("gcs://bucket/a.parquet/x.json")


#: Commands that read one parquet file: each must take ``l[c.parquet`` as one.
BRACKET_INPUT_COMMANDS = {
    "extract": lambda src, out: ("extract", "geoparquet", src, out / "o.parquet"),
    "add bbox": lambda src, out: ("add", "bbox", src, out / "o.parquet", "--force"),
    "sort hilbert": lambda src, out: ("sort", "hilbert", src, out / "o.parquet"),
    "sort str": lambda src, out: ("sort", "str", src, out / "o.parquet"),
    "convert csv": lambda src, out: ("convert", "csv", src, out / "o.csv"),
    "partition h3": lambda src, out: ("partition", "h3", src, out / "parts", "--resolution", "2"),
    "inspect summary": lambda src, out: ("inspect", "summary", src),
    "publish upload": lambda src, out: ("publish", "upload", src, "s3://b/o.parquet", "--dry-run"),
}


@pytest.mark.parametrize("command", sorted(BRACKET_INPUT_COMMANDS))
def test_every_command_reads_a_bracket_named_file_as_one_file(
    command, bracket_file, tmp_path, monkeypatch
):
    # publish upload's dry run checks that credentials exist; it opens no connection
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    out = tmp_path / "out"
    out.mkdir()
    result = _run(*BRACKET_INPUT_COMMANDS[command](bracket_file, out))
    assert result.exit_code == 0, _explain(result)
    assert "Partitioned input detected" not in result.output


# ---------------------------------------------------------------------------
# The one overwrite policy: same refusal, same switch semantics, everywhere
# ---------------------------------------------------------------------------

#: command -> (argv builder for an existing output, its suffix, the switch that replaces it)
OVERWRITE_COMMANDS = {
    "extract": (lambda src, out: ("extract", "geoparquet", src, out), ".parquet", "--overwrite"),
    "add bbox": (lambda src, out: ("add", "bbox", src, out, "--force"), ".parquet", "--overwrite"),
    "sort hilbert": (lambda src, out: ("sort", "hilbert", src, out), ".parquet", "--overwrite"),
    "convert csv": (lambda src, out: ("convert", "csv", src, out), ".csv", "--overwrite"),
    "pmtiles create": (lambda src, out: ("pmtiles", "create", src, out), ".pmtiles", "--force"),
    "pmtiles pyramid": (lambda src, out: ("pmtiles", "pyramid", src, out), ".pmtiles", "--force"),
}


@pytest.mark.parametrize("command", sorted(OVERWRITE_COMMANDS))
def test_an_existing_output_is_refused_with_the_one_message(command, tmp_path):
    build, suffix, switch = OVERWRITE_COMMANDS[command]
    out = tmp_path / f"existing{suffix}"
    out.write_bytes(b"precious")

    result = _run(*build(PLACES, out))

    assert result.exit_code == 1, _explain(result)
    assert f"Output file already exists: {out}" in result.output, _explain(result)
    assert f"Use {switch} to replace it." in result.output, _explain(result)
    assert out.read_bytes() == b"precious"


@pytest.mark.parametrize("command", ["extract", "add bbox", "sort hilbert", "convert csv"])
def test_the_switch_replaces_an_existing_output(command, tmp_path):
    build, suffix, switch = OVERWRITE_COMMANDS[command]
    out = tmp_path / f"existing{suffix}"
    out.write_bytes(b"precious")

    result = _run(*build(PLACES, out), switch)

    assert result.exit_code == 0, _explain(result)
    assert out.read_bytes() != b"precious"
    if suffix == ".parquet":
        assert pq.read_metadata(str(out)).num_rows == pq.read_metadata(str(PLACES)).num_rows


@pytest.mark.parametrize("command", ["pmtiles create", "pmtiles pyramid"])
def test_the_tile_switch_gets_past_the_overwrite_check(command, tmp_path, monkeypatch):
    """With --force the archive is the tiler's to replace: the run goes on to
    look for tippecanoe (faked absent here) instead of refusing."""
    import geoparquet_io.core.pmtiles as pmtiles
    import geoparquet_io.core.pmtiles_pyramid as pyramid

    monkeypatch.setattr(pmtiles, "_check_tippecanoe", lambda: False)
    monkeypatch.setattr(pyramid, "_check_tippecanoe", lambda: False)
    build, _suffix, switch = OVERWRITE_COMMANDS[command]
    out = tmp_path / "existing.pmtiles"
    out.write_bytes(b"precious")

    result = _run(*build(PLACES, out), switch)

    assert "already exists" not in result.output, _explain(result)
    assert "tippecanoe" in result.output.lower(), _explain(result)
    assert out.read_bytes() == b"precious"


def test_the_python_api_names_its_own_switch(tmp_path):
    from geoparquet_io.api.stac import generate_stac

    out = tmp_path / "item.json"
    out.write_text("{}")
    with pytest.raises(ValueError, match=r"already exists: .*item\.json\nUse overwrite=True"):
        generate_stac(PLACES, out, bucket="s3://bucket/")
