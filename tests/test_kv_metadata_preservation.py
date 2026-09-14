"""
Regression tests for issue #690 — non-geo key/value metadata must survive a write.

A GeoParquet file's file-level KV metadata carries sidecar payloads (``fiboa``,
``vecorel``, STAC fragments, collection records). ``write_parquet_with_metadata``
already merges those keys into the output, but the two highest-level entry
points did not reach that merge:

1. ``gpio convert geoparquet`` hardcoded ``original_metadata=None`` and passed
   no KV metadata at all, so every strategy dropped the keys.
2. ``api.Table.write`` calls the strategy's ``write_from_table`` directly. The
   Arrow strategies preserved the table's schema metadata incidentally, while
   the DuckDB COPY paths (``duckdb-kv`` — the default — and ``disk-rewrite``)
   rebuilt the KV block from scratch and dropped them.

These tests assert preservation on the DEFAULT path for both entry points, and
consistency across all four write strategies.
"""

import json
import logging
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io.api as api
from geoparquet_io.cli.main import cli
from geoparquet_io.core.write_funnels import (
    extract_preserved_kv_metadata,
    read_preserved_kv_metadata,
)

WRITE_STRATEGIES = ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"]

FIBOA_VALUE = '{"schemas": ["https://fiboa.org/specification/v0.2.0/schema.yaml"]}'
CUSTOM_VALUE = '{"hello": "world"}'


def _geo_metadata() -> dict:
    return {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
            }
        },
    }


def _point_wkb(x: float, y: float) -> bytes:
    return struct.pack("<BI2d", 1, 1, x, y)


@pytest.fixture
def kv_source(tmp_path):
    """A tiny GeoParquet file carrying two non-geo KV keys next to ``geo``."""
    src = tmp_path / "kv_source.parquet"
    table = pa.table(
        {
            "id": [1, 2, 3],
            "geometry": [_point_wkb(1.0, 2.0), _point_wkb(3.0, 4.0), _point_wkb(5.0, 6.0)],
        }
    )
    metadata = {
        b"geo": json.dumps(_geo_metadata()).encode("utf-8"),
        b"fiboa": FIBOA_VALUE.encode("utf-8"),
        b"custom_note": CUSTOM_VALUE.encode("utf-8"),
    }
    pq.write_table(table.replace_schema_metadata(metadata), src)
    return src


def kv_keys(path) -> set[str]:
    metadata = pq.ParquetFile(str(path)).schema_arrow.metadata or {}
    return {k.decode("utf-8") if isinstance(k, bytes) else k for k in metadata}


def kv_value(path, key: str) -> str:
    metadata = pq.ParquetFile(str(path)).schema_arrow.metadata or {}
    return metadata[key.encode("utf-8")].decode("utf-8")


def assert_single_valid_geo(path):
    """The geo key is regenerated exactly once and is still parseable."""
    metadata = pq.ParquetFile(str(path)).schema_arrow.metadata or {}
    geo_keys = [k for k in kv_keys(path) if k == "geo"]
    assert geo_keys == ["geo"], f"expected exactly one geo key, got {sorted(kv_keys(path))}"
    geo = json.loads(metadata[b"geo"].decode("utf-8"))
    assert geo["primary_column"] == "geometry"
    assert "geometry" in geo["columns"]


class TestExtractPreservedKvMetadata:
    """Unit tests for the shared preserved-keys filter."""

    def test_none_and_empty(self):
        assert extract_preserved_kv_metadata(None) == {}
        assert extract_preserved_kv_metadata({}) == {}

    def test_excludes_geo_and_arrow_noise(self):
        preserved = extract_preserved_kv_metadata(
            {
                b"geo": b"{}",
                b"ARROW:schema": b"base64noise",
                b"pandas": b"{}",
                b"fiboa": FIBOA_VALUE.encode("utf-8"),
            }
        )
        assert preserved == {"fiboa": FIBOA_VALUE}

    def test_decodes_str_and_bytes_keys(self):
        preserved = extract_preserved_kv_metadata({"custom_note": CUSTOM_VALUE, b"fiboa": b"{}"})
        assert preserved == {"custom_note": CUSTOM_VALUE, "fiboa": "{}"}

    def test_skips_undecodable_values(self):
        """Binary payloads that are not UTF-8 are dropped, not fatal."""
        preserved = extract_preserved_kv_metadata({b"binary_blob": b"\xff\xfe", b"fiboa": b"{}"})
        assert preserved == {"fiboa": "{}"}

    def test_skips_undecodable_keys(self):
        """Parquet KV keys are arbitrary bytes; a non-UTF-8 key must not raise."""
        preserved = extract_preserved_kv_metadata({b"\xff\xfe": b"{}", b"fiboa": b"{}"})
        assert preserved == {"fiboa": "{}"}

    def test_undecodable_key_survives_the_read_path(self, tmp_path):
        """The same skip applies when the key comes off a real file."""
        src = tmp_path / "binary_key.parquet"
        table = pa.table({"id": [1], "geometry": [_point_wkb(1.0, 2.0)]})
        pq.write_table(
            table.replace_schema_metadata(
                {
                    b"geo": json.dumps(_geo_metadata()).encode("utf-8"),
                    b"\xff\xfe": b"{}",
                    b"fiboa": FIBOA_VALUE.encode("utf-8"),
                }
            ),
            src,
        )
        assert read_preserved_kv_metadata(str(src)) == {"fiboa": FIBOA_VALUE}


class TestReadPreservedKvMetadata:
    """Reading an input's preservable keys must degrade, never abort a write."""

    @pytest.mark.parametrize("verbose", [False, True])
    def test_reads_keys_from_file(self, kv_source, verbose):
        preserved = read_preserved_kv_metadata(str(kv_source), verbose=verbose)
        assert preserved == {"fiboa": FIBOA_VALUE, "custom_note": CUSTOM_VALUE}

    def test_unreadable_input_warns_and_returns_empty(self, caplog, monkeypatch):
        from geoparquet_io.core import duckdb_metadata

        def _boom(*args, **kwargs):
            raise OSError("simulated metadata read failure")

        monkeypatch.setattr(duckdb_metadata, "get_kv_metadata", _boom)

        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            assert read_preserved_kv_metadata("s3://bucket/unreachable.parquet") == {}

        assert any(
            "metadata" in rec.message.lower() and rec.levelno == logging.WARNING
            for rec in caplog.records
        ), f"expected a warning; got {[r.message for r in caplog.records]}"


class TestConvertPreservesKv:
    """``gpio convert geoparquet`` — parquet input, default write strategy."""

    def test_default_path_preserves_non_geo_keys(self, kv_source, tmp_path):
        out = tmp_path / "converted.parquet"
        result = CliRunner().invoke(
            cli,
            [
                "convert",
                "geoparquet",
                str(kv_source),
                str(out),
                "--skip-hilbert",
                "--geoparquet-version",
                "1.1",
            ],
        )
        assert result.exit_code == 0, result.output
        assert {"fiboa", "custom_note"} <= kv_keys(out)
        assert kv_value(out, "fiboa") == FIBOA_VALUE
        assert kv_value(out, "custom_note") == CUSTOM_VALUE
        assert_single_valid_geo(out)

    def test_auto_version_path_preserves_non_geo_keys(self, kv_source, tmp_path):
        """No --geoparquet-version: the version is auto-resolved from the input."""
        out = tmp_path / "converted_auto.parquet"
        result = CliRunner().invoke(
            cli, ["convert", "geoparquet", str(kv_source), str(out), "--skip-hilbert"]
        )
        assert result.exit_code == 0, result.output
        assert {"fiboa", "custom_note"} <= kv_keys(out)
        assert_single_valid_geo(out)

    def test_hilbert_sorted_path_preserves_non_geo_keys(self, kv_source, tmp_path):
        out = tmp_path / "converted_hilbert.parquet"
        result = CliRunner().invoke(
            cli,
            ["convert", "geoparquet", str(kv_source), str(out), "--geoparquet-version", "1.1"],
        )
        assert result.exit_code == 0, result.output
        assert {"fiboa", "custom_note"} <= kv_keys(out)

    @pytest.mark.parametrize(
        "fixture_name",
        ["fields_v2_file", "austria_bbox_covering_file"],
        ids=["v2_fast_path", "bbox_covering"],
    )
    def test_sidecar_keys_do_not_change_geo_metadata(self, request, tmp_path, fixture_name):
        """KV keys force the metadata-rewrite path; geo output must be identical.

        Two inputs, two things being pinned:

        - ``fields_v2_file`` (GeoParquet 2.0, no bbox column) normally takes the
          no-rewrite fast path. Sidecar keys send it down the rewrite path
          instead, so the geo metadata that path produces has to match what the
          fast path wrote — otherwise preservation would silently trade one kind
          of metadata for another.
        - ``austria_bbox_covering_file`` carries a bbox column and a ``covering``
          block, so the rewrite path runs ``_add_bbox_covering_if_present``. That
          is the assignment #694 is about; this pins that carrying sidecar keys
          does not cost the output its covering.
        """
        source_file = request.getfixturevalue(fixture_name)
        with_kv = tmp_path / "source_with_kv.parquet"
        source = pq.read_table(source_file)
        metadata = dict(source.schema.metadata or {})
        metadata[b"fiboa"] = FIBOA_VALUE.encode("utf-8")
        pq.write_table(source.replace_schema_metadata(metadata), with_kv)

        outputs = {}
        for label, inp in (("plain", source_file), ("with_kv", with_kv)):
            out = tmp_path / f"converted_{label}.parquet"
            result = CliRunner().invoke(
                cli, ["convert", "geoparquet", str(inp), str(out), "--skip-hilbert"]
            )
            assert result.exit_code == 0, result.output
            outputs[label] = json.loads(kv_value(out, "geo"))

        assert outputs["with_kv"] == outputs["plain"]
        assert "fiboa" in kv_keys(tmp_path / "converted_with_kv.parquet")

        # The covering fixture must actually exercise the covering assignment,
        # otherwise this test would pass vacuously if the fixture ever changed.
        if fixture_name == "austria_bbox_covering_file":
            primary = outputs["with_kv"]["primary_column"]
            assert "covering" in outputs["with_kv"]["columns"][primary]

    def test_non_parquet_input_still_converts(self, geojson_input, tmp_path):
        """A non-parquet input has no KV metadata to preserve — must not break."""
        out = tmp_path / "from_geojson.parquet"
        result = CliRunner().invoke(
            cli, ["convert", "geoparquet", str(geojson_input), str(out), "--skip-hilbert"]
        )
        assert result.exit_code == 0, result.output
        assert "geo" in kv_keys(out)


class TestTableWritePreservesKv:
    """``api.read(...).write(...)`` — default and explicit strategies."""

    def test_default_strategy_preserves_non_geo_keys(self, kv_source, tmp_path):
        out = tmp_path / "api_default.parquet"
        api.read(str(kv_source)).write(str(out), geoparquet_version="1.1")
        assert {"fiboa", "custom_note"} <= kv_keys(out)
        assert kv_value(out, "fiboa") == FIBOA_VALUE
        assert kv_value(out, "custom_note") == CUSTOM_VALUE
        assert_single_valid_geo(out)

    @pytest.mark.parametrize("strategy", WRITE_STRATEGIES)
    def test_every_strategy_preserves_non_geo_keys(self, kv_source, tmp_path, strategy):
        out = tmp_path / f"api_{strategy}.parquet"
        api.read(str(kv_source)).write(str(out), geoparquet_version="1.1", write_strategy=strategy)
        assert {"fiboa", "custom_note"} <= kv_keys(out), f"{strategy} dropped keys"
        assert kv_value(out, "fiboa") == FIBOA_VALUE
        assert_single_valid_geo(out)

    def test_written_geo_metadata_is_not_the_stale_input_copy(self, kv_source, tmp_path):
        """Preserving KV must not smuggle the input's geo key through verbatim."""
        out = tmp_path / "api_geo.parquet"
        api.read(str(kv_source)).write(str(out), geoparquet_version="1.0")
        metadata = pq.ParquetFile(str(out)).schema_arrow.metadata
        geo = json.loads(metadata[b"geo"].decode("utf-8"))
        assert geo["version"].startswith("1.0")


class TestBuildKvMetadataClause:
    """The shared clause builder escapes exactly once and quotes keys."""

    def test_empty_returns_none(self):
        from geoparquet_io.core.duckdb_utils import build_kv_metadata_clause

        assert build_kv_metadata_clause(None) is None
        assert build_kv_metadata_clause({}) is None

    def test_escapes_values_once(self):
        from geoparquet_io.core.duckdb_utils import build_kv_metadata_clause

        assert build_kv_metadata_clause({"k": "o'brien"}) == "KV_METADATA {'k': 'o''brien'}"

    def test_quotes_keys_so_a_colon_survives(self):
        """An unquoted `ARROW:schema` key makes DuckDB's parser reject the COPY."""
        from geoparquet_io.core.duckdb_utils import build_kv_metadata_clause

        clause = build_kv_metadata_clause({"ARROW:schema": "x"})
        assert clause == "KV_METADATA {'ARROW:schema': 'x'}"


@pytest.fixture
def nongeo_kv_source(tmp_path):
    """Plain Parquet -- no geometry column at all -- carrying a sidecar key."""
    src = tmp_path / "nongeo.parquet"
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    metadata = {
        b"fiboa": FIBOA_VALUE.encode("utf-8"),
        b"custom_note": CUSTOM_VALUE.encode("utf-8"),
    }
    pq.write_table(table.replace_schema_metadata(metadata), src)
    return src


class TestNonGeoTablePreservesKv:
    """#708: preservation must not depend on there being a geometry column.

    #690 fixed this for GeoParquet writes. ``duckdb-kv`` -- the default -- routes
    a table with no geometry column to ``_write_plain_parquet_from_table``, which
    built its COPY options inline with no ``KV_METADATA`` and never received the
    keys #690 plumbed in. The other three strategies write through PyArrow, which
    carries schema metadata along for free.
    """

    @pytest.mark.parametrize("strategy", WRITE_STRATEGIES)
    def test_every_strategy_preserves_keys_without_geometry(
        self, nongeo_kv_source, tmp_path, strategy
    ):
        out = tmp_path / f"nongeo_{strategy}.parquet"
        api.read(str(nongeo_kv_source)).write(str(out), write_strategy=strategy)

        assert {"fiboa", "custom_note"} <= kv_keys(out), f"{strategy} dropped keys"
        assert kv_value(out, "fiboa") == FIBOA_VALUE
        assert kv_value(out, "custom_note") == CUSTOM_VALUE

    def test_default_strategy_preserves_keys_without_geometry(self, nongeo_kv_source, tmp_path):
        """The default strategy is the one that was broken."""
        out = tmp_path / "nongeo_default.parquet"
        api.read(str(nongeo_kv_source)).write(str(out))

        assert {"fiboa", "custom_note"} <= kv_keys(out)

    def test_no_geo_key_is_invented(self, nongeo_kv_source, tmp_path):
        """A table with no geometry is still not GeoParquet afterwards."""
        out = tmp_path / "nongeo_nogeo.parquet"
        api.read(str(nongeo_kv_source)).write(str(out))

        assert "geo" not in kv_keys(out)

    def test_rows_survive(self, nongeo_kv_source, tmp_path):
        out = tmp_path / "nongeo_rows.parquet"
        api.read(str(nongeo_kv_source)).write(str(out))

        assert pq.read_table(out).column("name").to_pylist() == ["a", "b"]

    def test_values_needing_escaping_round_trip(self, tmp_path):
        """A sidecar value with an apostrophe must be escaped exactly once."""
        src = tmp_path / "quoted.parquet"
        awkward = '{"note": "o\'brien said \'hi\'"}'
        pq.write_table(
            pa.table({"id": [1]}).replace_schema_metadata({b"custom_note": awkward.encode()}),
            src,
        )
        out = tmp_path / "quoted_out.parquet"
        api.read(str(src)).write(str(out))

        assert kv_value(out, "custom_note") == awkward

    def test_query_entry_point_preserves_keys(self, tmp_path):
        """``write_from_query`` with no geometry column carries the keys too."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.write_strategies.duckdb_kv import DuckDBKVStrategy

        out = tmp_path / "nongeo_query.parquet"
        con = get_duckdb_connection(load_spatial=False)
        try:
            DuckDBKVStrategy().write_from_query(
                con=con,
                query="SELECT 1 AS id",
                output_path=str(out),
                geometry_column=None,
                geoparquet_version="1.1",
                original_metadata=None,
                input_crs=None,
                compression="ZSTD",
                compression_level=None,
                row_group_size_mb=None,
                row_group_rows=None,
                verbose=False,
                extra_kv_metadata={"fiboa": FIBOA_VALUE},
            )
        finally:
            con.close()

        assert kv_value(out, "fiboa") == FIBOA_VALUE


class TestSidecarKeysRideTheFastPath:
    """#709: an unrelated sidecar key must not cost a full metadata rewrite.

    ``extra_kv_metadata`` used to set ``rewrite_needed = True`` unconditionally,
    so a 2.0 -> 2.0 copy that would otherwise take the no-rewrite ``_plain_copy_to``
    path paid for an extra scan of the geometry column purely because the file
    carried a fiboa or vecorel payload.
    """

    FAST_PATH_MARKER = "using plain COPY TO"

    def _convert(self, inp, out):
        result = CliRunner().invoke(
            cli,
            ["convert", "geoparquet", str(inp), str(out), "--skip-hilbert", "--verbose"],
        )
        assert result.exit_code == 0, result.output
        return result.output

    @pytest.fixture
    def v2_with_sidecar(self, fields_v2_file, tmp_path):
        source = pq.read_table(fields_v2_file)
        metadata = dict(source.schema.metadata or {})
        metadata[b"fiboa"] = FIBOA_VALUE.encode("utf-8")
        path = tmp_path / "v2_sidecar.parquet"
        pq.write_table(source.replace_schema_metadata(metadata), path)
        return path

    def test_plain_v2_input_takes_the_fast_path(self, fields_v2_file, tmp_path):
        """Non-vacuity: this input takes the fast path with no sidecar key."""
        output = self._convert(fields_v2_file, tmp_path / "plain.parquet")

        assert self.FAST_PATH_MARKER in output

    def test_sidecar_key_still_takes_the_fast_path(self, v2_with_sidecar, tmp_path):
        output = self._convert(v2_with_sidecar, tmp_path / "sidecar.parquet")

        assert self.FAST_PATH_MARKER in output, (
            "a sidecar key still forces the metadata-rewrite path"
        )

    def test_fast_path_writes_the_sidecar_key(self, v2_with_sidecar, tmp_path):
        out = tmp_path / "sidecar_keys.parquet"
        self._convert(v2_with_sidecar, out)

        assert kv_value(out, "fiboa") == FIBOA_VALUE
        assert "geo" in kv_keys(out)

    def test_fast_path_geo_matches_the_no_sidecar_output(
        self, fields_v2_file, v2_with_sidecar, tmp_path
    ):
        """Carrying a sidecar key must not change the geo block at all."""
        plain_out = tmp_path / "geo_plain.parquet"
        sidecar_out = tmp_path / "geo_sidecar.parquet"
        self._convert(fields_v2_file, plain_out)
        self._convert(v2_with_sidecar, sidecar_out)

        assert json.loads(kv_value(sidecar_out, "geo")) == json.loads(kv_value(plain_out, "geo"))

    def test_covering_input_still_rewrites(self, austria_bbox_covering_file, tmp_path):
        """A covering is a real reason to rewrite; that gate is untouched."""
        source = pq.read_table(austria_bbox_covering_file)
        metadata = dict(source.schema.metadata or {})
        metadata[b"fiboa"] = FIBOA_VALUE.encode("utf-8")
        inp = tmp_path / "covering_sidecar.parquet"
        pq.write_table(source.replace_schema_metadata(metadata), inp)

        out = tmp_path / "covering_out.parquet"
        self._convert(inp, out)

        geo = json.loads(kv_value(out, "geo"))
        assert "covering" in geo["columns"][geo["primary_column"]]
        assert kv_value(out, "fiboa") == FIBOA_VALUE
