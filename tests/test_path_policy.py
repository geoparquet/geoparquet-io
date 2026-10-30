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


# ---------------------------------------------------------------------------
# One store spelling per family: upload and copy_file read the same map
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("s3://b/k/x.parquet", ("s3://b", "k/x.parquet")),
        ("s3a://b/k/x.parquet", ("s3://b", "k/x.parquet")),
        ("gs://b/k/x.parquet", ("gs://b", "k/x.parquet")),
        ("gcs://b/k/x.parquet", ("gs://b", "k/x.parquet")),
        ("az://acct/cont/x.parquet", ("az://acct/cont", "x.parquet")),
    ],
)
def test_upload_parses_every_spelling_of_a_store(url, expected):
    from geoparquet_io.core.upload import parse_object_store_url

    assert parse_object_store_url(url) == expected


@pytest.mark.parametrize(
    ("url", "checker"),
    [
        ("s3a://b/x.parquet", "_check_s3_credentials"),
        ("gcs://b/x.parquet", "_check_gcs_credentials"),
    ],
)
def test_upload_checks_the_credentials_of_the_family(url, checker, monkeypatch):
    from geoparquet_io.core import upload

    monkeypatch.setattr(upload, checker, lambda *a, **k: (False, f"{checker} ran"))
    assert upload.check_credentials(url) == (False, f"{checker} ran")


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("s3a://b/k", "s3://b/k"),
        ("gcs://b/k", "gs://b/k"),
        ("s3://b/k", "s3://b/k"),
        ("az://a/c/k", "az://a/c/k"),
        ("abfs://c@a.dfs.core.windows.net/k", "abfs://c@a.dfs.core.windows.net/k"),
        ("https://h/k", "https://h/k"),
        ("/local/k", "/local/k"),
    ],
)
def test_canonical_store_url_respells_only_aliases(url, expected):
    from geoparquet_io.core.remote import canonical_store_url

    assert canonical_store_url(url) == expected


# ---------------------------------------------------------------------------
# Direct pins for the small owner helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "label"),
    [
        ("s3://b/x", "S3"),
        ("s3a://b/x", "S3A"),
        ("gs://b/x", "GS"),
        ("az://a/c/x", "AZ"),
        ("https://h/x", "HTTPS"),
        ("/local/x", "HTTP"),
    ],
)
def test_protocol_label_names_the_scheme_as_written(path, label):
    from geoparquet_io.core.remote import protocol_label

    assert protocol_label(path) == label


def test_protocol_label_falls_back_to_the_given_default():
    from geoparquet_io.core.remote import protocol_label

    assert protocol_label("/local/x", default="local") == "local"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("http://h/api", True),
        ("https://h/api", True),
        ("HTTPS://h/api", True),
        ("s3://b/x", False),
        ("gs://b/x", False),
        ("file:///x", False),
        ("/local/x", False),
        ("", False),
    ],
)
def test_has_http_scheme_accepts_only_http(url, expected):
    from geoparquet_io.core.remote import has_http_scheme

    assert has_http_scheme(url) is expected


def test_output_is_kept_only_when_it_exists_without_the_switch(tmp_path):
    from geoparquet_io.core.file_utils import output_is_kept

    out = tmp_path / "o.parquet"
    assert output_is_kept(str(out), overwrite=False) is False
    out.write_bytes(b"x")
    assert output_is_kept(str(out), overwrite=False) is True
    assert output_is_kept(str(out), overwrite=True) is False


def test_unlink_released_file_deletes_and_ignores_a_missing_file(tmp_path):
    from geoparquet_io.core.file_utils import unlink_released_file

    f = tmp_path / "t.parquet"
    f.write_bytes(b"x")
    unlink_released_file(f)
    assert not f.exists()
    unlink_released_file(f)  # missing: silent


def test_unlink_released_file_retries_then_gives_up_quietly(tmp_path, monkeypatch):
    from geoparquet_io.core import file_utils

    f = tmp_path / "t.parquet"
    f.write_bytes(b"x")
    calls = []

    def locked(self, *a, **k):
        calls.append(self)
        raise PermissionError("held")

    monkeypatch.setattr(Path, "unlink", locked)
    monkeypatch.setattr("time.sleep", lambda s: None)
    file_utils.unlink_released_file(f)
    assert len(calls) == len(file_utils._UNLINK_WAITS)


# ---------------------------------------------------------------------------
# One remote listing: partition info, the all-files read and the GeoJSON sweep
# ---------------------------------------------------------------------------

REMOTE_GLOB = "s3://bkt/parts/*.parquet"
REMOTE_FILES = ["s3://bkt/parts/a.parquet", "s3://bkt/parts/b.parquet"]


def test_a_remote_glob_lists_every_file(remote_parts):
    from geoparquet_io.core.file_utils import get_all_parquet_files

    assert get_all_parquet_files(REMOTE_GLOB) == REMOTE_FILES


def test_partition_info_of_a_remote_glob_names_its_files(remote_parts):
    from geoparquet_io.core.partition.reader import get_partition_info

    info = get_partition_info(REMOTE_GLOB)
    assert info["first_file"] == REMOTE_FILES[0]
    assert info["all_files"] == REMOTE_FILES
    assert info["file_count"] == 2


def test_the_geojson_sweep_reads_the_one_listing(remote_parts):
    from geoparquet_io.core.geojson_stream import _geojson_input_files

    assert _geojson_input_files(REMOTE_GLOB) == REMOTE_FILES


def test_check_of_a_remote_glob_checks_its_first_file(remote_parts):
    endpoint, _requests = remote_parts
    result = _run(
        "--s3-endpoint", endpoint, "--s3-no-ssl", "check", "bbox", REMOTE_GLOB,
    )  # fmt: skip
    assert "Checking first file (of 2 total)" in result.output, _explain(result)


def test_stac_lists_a_partition_like_every_other_reader(tmp_path):
    from geoparquet_io.core.file_utils import get_all_parquet_files
    from geoparquet_io.core.stac import _find_partition_files

    for rel in ("a.parquet", "k=1/b.parquet", "k=1/j=2/c.parquet", "._a.parquet"):
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_bytes(b"x")
    found = [str(p) for p in _find_partition_files(str(tmp_path))]
    assert found == get_all_parquet_files(str(tmp_path))
    assert len(found) == 3


# ---------------------------------------------------------------------------
# A remote in-place rewrite is the owner's call too
# ---------------------------------------------------------------------------


def test_the_owner_refuses_a_remote_in_place_rewrite_without_the_switch():
    from geoparquet_io.core.exceptions import GeoParquetError
    from geoparquet_io.core.file_utils import handle_output_overwrite

    url = "s3://b/x.parquet"
    with pytest.raises(GeoParquetError, match="Cannot modify remote file without --force flag"):
        handle_output_overwrite(url, False, url, flag="--force")
    handle_output_overwrite(url, True, url)  # the switch allows it
    handle_output_overwrite("s3://b/y.parquet", False, url)  # another output passes


def test_a_remote_fix_reads_the_owner_refusal(monkeypatch):
    from geoparquet_io.cli import fix_helpers
    from geoparquet_io.core.exceptions import GeoParquetError

    def refuse(*a, **k):
        raise GeoParquetError("owner says no")

    monkeypatch.setattr(fix_helpers, "handle_output_overwrite", refuse)
    with pytest.raises(GeoParquetError, match="owner says no"):
        fix_helpers.validate_remote_file_modification("s3://b/x.parquet", None, False)


def test_overwrite_option_spells_the_tile_switch():
    import click

    from geoparquet_io.cli.decorators import overwrite_option

    @click.command()
    @overwrite_option(flag="--force", short="-f", help_text="Replace it")
    def cmd(force):
        click.echo(f"force={force}")

    (param,) = cmd.params
    assert param.opts == ["--force", "-f"] and param.help == "Replace it"
    assert CliRunner().invoke(cmd, ["-f"]).output == "force=True\n"


@pytest.mark.parametrize(
    ("overwrite", "expected"), [(False, "Use --overwrite"), (True, "Failed to create")]
)
def test_a_writer_already_exists_error_is_the_refusal_only_without_the_switch(
    tmp_path, monkeypatch, overwrite, expected
):
    from geoparquet_io.core import format_writers
    from geoparquet_io.core.exceptions import GeoParquetError

    def boom(*a, **k):
        raise RuntimeError("GDAL Error: layer already exists")

    monkeypatch.setattr(format_writers, "atomic_write", boom)
    with pytest.raises(GeoParquetError, match=expected):
        format_writers.write_gdal_format(
            str(PLACES), str(tmp_path / "o.gpkg"), "geopackage", overwrite=overwrite
        )
