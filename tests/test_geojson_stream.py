"""
Tests for GeoJSON conversion.

Tests verify that gpio convert geojson:
- Outputs valid GeoJSON Features in streaming mode
- Includes RFC 8142 record separators by default
- Supports --no-rs flag to disable separators
- Works with both file input and stdin (pipeline)
- Writes valid GeoJSON FeatureCollection to file
- Supports --precision, --write-bbox, --id-field options
"""

import io
import json
import shutil
import sys
import tempfile
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import quote_identifier
from geoparquet_io.core.geojson_stream import (
    RS,
    WGS84_CRS,
    _build_feature_query,
    _encodes_utf8,
    _get_property_columns,
    _needs_reprojection,
    convert_to_geojson,
    convert_to_geojson_stream,
)

# Test data
TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES_PARQUET = TEST_DATA_DIR / "places_test.parquet"
BUILDINGS_PARQUET = TEST_DATA_DIR / "buildings_test.parquet"


class TestQuoteIdentifier:
    """Tests for SQL identifier quoting."""

    def test_simple_name(self):
        """Test quoting a simple identifier."""
        assert quote_identifier("name") == '"name"'

    def test_name_with_spaces(self):
        """Test quoting identifier with spaces."""
        assert quote_identifier("my column") == '"my column"'

    def test_name_with_quotes(self):
        """Test quoting identifier with embedded quotes."""
        assert quote_identifier('foo"bar') == '"foo""bar"'


class TestBuildFeatureQuery:
    """Tests for SQL query construction."""

    def test_basic_query(self):
        """Test basic feature query generation."""
        query = _build_feature_query("test_table", "geometry", ["name", "population"])
        assert "ST_AsGeoJSON" in query
        assert "Feature" in query
        assert "name" in query
        assert "population" in query

    def test_empty_properties(self):
        """Test query with no properties."""
        query = _build_feature_query("test_table", "geometry", [])
        assert "ST_AsGeoJSON" in query
        assert "'{}'" in query  # Empty properties object

    def test_special_column_names(self):
        """Test query handles special column names."""
        query = _build_feature_query("test_table", "geometry", ["my column", "type"])
        assert '"my column"' in query
        assert '"type"' in query

    def test_with_write_bbox(self):
        """Test query includes bbox when requested."""
        query = _build_feature_query("test_table", "geometry", ["name"], write_bbox=True)
        assert "ST_XMin" in query
        assert "ST_YMin" in query
        assert "ST_XMax" in query
        assert "ST_YMax" in query
        assert "bbox" in query

    def test_with_id_field(self):
        """Test query includes id when requested."""
        query = _build_feature_query("test_table", "geometry", ["name"], id_field="osm_id")
        assert '"osm_id"' in query
        assert '"id":' in query


class TestGetPropertyColumns:
    """Tests for property column selection."""

    @pytest.fixture
    def mock_duckdb_connection(self):
        """Create a mock DuckDB connection."""
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("""
            CREATE TABLE test_data AS
            SELECT
                1 as id,
                'test' as name,
                100 as population,
                ST_GeomFromText('POINT(0 0)') as geometry,
                STRUCT_PACK(xmin := 0, ymin := 0, xmax := 1, ymax := 1) as bbox
        """)
        yield con
        con.close()

    def test_excludes_geometry_column(self, mock_duckdb_connection):
        """Test that geometry column is excluded from properties."""
        cols = _get_property_columns(mock_duckdb_connection, "test_data", "geometry")
        assert "geometry" not in cols
        assert "id" in cols
        assert "name" in cols

    def test_excludes_bbox_column(self, mock_duckdb_connection):
        """Test that bbox column is excluded by default."""
        cols = _get_property_columns(mock_duckdb_connection, "test_data", "geometry")
        assert "bbox" not in cols


@pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
class TestConvertToGeoJSONStream:
    """Tests for streaming GeoJSON output."""

    def test_basic_output(self, capsys):
        """Test basic GeoJSON output."""
        count = convert_to_geojson_stream(str(PLACES_PARQUET), rs=False)

        captured = capsys.readouterr()
        lines = [line for line in captured.out.strip().split("\n") if line]

        assert count > 0
        assert len(lines) == count

        # Verify first line is valid GeoJSON Feature
        feature = json.loads(lines[0])
        assert feature["type"] == "Feature"
        assert "geometry" in feature
        assert "properties" in feature

    def test_rs_separators_default(self, capsys):
        """Test RFC 8142 record separators are included by default."""
        convert_to_geojson_stream(str(PLACES_PARQUET), rs=True)

        captured = capsys.readouterr()
        # Check for record separator character
        assert "\x1e" in captured.out

    def test_no_rs_separators(self, capsys):
        """Test disabling RS separators."""
        convert_to_geojson_stream(str(PLACES_PARQUET), rs=False)

        captured = capsys.readouterr()
        # No record separator character
        assert "\x1e" not in captured.out

    def test_valid_geojson_features(self, capsys):
        """Test that all output lines are valid GeoJSON Features."""
        convert_to_geojson_stream(str(PLACES_PARQUET), rs=False)

        captured = capsys.readouterr()
        for line in captured.out.strip().split("\n"):
            if line:
                feature = json.loads(line)
                assert feature["type"] == "Feature"
                assert "geometry" in feature
                assert feature["geometry"] is not None
                assert "type" in feature["geometry"]
                assert "coordinates" in feature["geometry"]
                assert "properties" in feature


@pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
class TestConvertGeoJSONCLI:
    """Tests for gpio convert geojson CLI."""

    def test_basic_geojson_output(self):
        """Test gpio convert geojson command."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", "geojson", str(PLACES_PARQUET)])

        assert result.exit_code == 0

        # Parse first feature (skip RS char if present)
        output = result.output
        if output.startswith("\x1e"):
            output = output[1:]
        first_line = output.split("\n")[0]
        feature = json.loads(first_line)
        assert feature["type"] == "Feature"

    def test_no_rs_flag(self):
        """Test --no-rs flag disables record separators."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", "geojson", str(PLACES_PARQUET), "--no-rs"])

        assert result.exit_code == 0
        assert "\x1e" not in result.output

    def test_rs_enabled_by_default(self):
        """Test RS separators are enabled by default."""
        runner = CliRunner()
        result = runner.invoke(cli, ["convert", "geojson", str(PLACES_PARQUET)])

        assert result.exit_code == 0
        assert "\x1e" in result.output

    def test_precision_option(self):
        """Test --precision flag is accepted."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["convert", "geojson", str(PLACES_PARQUET), "--no-rs", "--precision", "5"]
        )

        assert result.exit_code == 0
        # Verify output is valid GeoJSON
        lines = [line for line in result.output.strip().split("\n") if line]
        if lines:
            feature = json.loads(lines[0])
            assert feature["type"] == "Feature"

    def test_verbose_flag_works(self):
        """Test that --verbose flag is accepted and command succeeds."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["convert", "geojson", str(PLACES_PARQUET), "--verbose", "--no-rs"]
        )

        # Command should succeed with verbose flag
        assert result.exit_code == 0

        # Output should contain GeoJSON features (may also have debug output mixed in)
        assert '"type":"Feature"' in result.output
        assert '"geometry"' in result.output

    def test_write_to_file(self):
        """Test writing to a GeoJSON file."""
        runner = CliRunner()
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.geojson"

        try:
            result = runner.invoke(
                cli, ["convert", "geojson", str(PLACES_PARQUET), str(output_path)]
            )

            assert result.exit_code == 0
            assert output_path.exists()

            # Verify the file contains valid GeoJSON
            with open(output_path, encoding="utf-8") as f:
                geojson = json.load(f)
            assert geojson["type"] == "FeatureCollection"
            assert "features" in geojson
            assert len(geojson["features"]) > 0
            assert geojson["features"][0]["type"] == "Feature"
        finally:
            if output_path.exists():
                output_path.unlink()


@pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
class TestPipelineIntegration:
    """Tests for pipeline integration."""

    def test_geometry_column_detection_from_arrow_table(self):
        """Test geometry column detection from Arrow table schema."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.streaming import find_geometry_column_from_table

        # Read test data
        table = pq.read_table(str(PLACES_PARQUET))

        # Find geometry column from the table
        geom_col = find_geometry_column_from_table(table)
        assert geom_col is not None

    def test_no_geometry_column_raises_error(self):
        """Test that missing geometry column raises ValueError with helpful message."""
        from unittest.mock import patch

        import pyarrow as pa

        from geoparquet_io.core.geojson_stream import _convert_from_stream

        # Create a table without geometry column
        table_no_geom = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        # Mock read_arrow_stream at the source module
        with patch("geoparquet_io.core.streaming.read_arrow_stream", return_value=table_no_geom):
            with pytest.raises(ValueError) as exc_info:
                _convert_from_stream()

            # Check error message contains helpful information
            error_msg = str(exc_info.value)
            assert "No geometry column found" in error_msg
            assert "id" in error_msg  # Available columns listed
            assert "name" in error_msg

    def test_arrow_stream_to_geojson_conversion(self):
        """Test full conversion from Arrow table to GeoJSON."""
        from unittest.mock import patch

        import pyarrow.parquet as pq

        from geoparquet_io.core.geojson_stream import _convert_from_stream

        # Read test data
        table = pq.read_table(str(PLACES_PARQUET))
        table = table.slice(0, 3)  # Small subset for testing

        # Create temp output file
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.geojson"

        try:
            # Mock read_arrow_stream at the source module
            with patch("geoparquet_io.core.streaming.read_arrow_stream", return_value=table):
                count = _convert_from_stream(output_path=str(output_path))

            assert count == 3
            assert output_path.exists()

            # Verify valid GeoJSON FeatureCollection
            with open(output_path, encoding="utf-8") as f:
                geojson = json.load(f)

            assert geojson["type"] == "FeatureCollection"
            assert len(geojson["features"]) == 3
            assert geojson["features"][0]["type"] == "Feature"
            assert "geometry" in geojson["features"][0]
        finally:
            if output_path.exists():
                output_path.unlink()

    @pytest.mark.integration
    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    @pytest.mark.skipif(
        sys.platform == "win32" or shutil.which("head") is None,
        reason="needs POSIX platform with `head` binary (Git-for-Windows head triggers cp1252 encoding bug, unrelated to #421)",
    )
    def test_streaming_handles_broken_pipe(self):
        """Stream survives downstream closing pipe early (e.g. tippecanoe done).

        Regression for issue #421: piping `gpio convert geojson` to a consumer
        that closes early caused a silent BrokenPipeError exit 1, surfaced to
        users as `gpio failed with exit code 1` with no logs.
        """
        import subprocess

        producer = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "from geoparquet_io.cli.main import main; main()",
                "convert",
                "geojson",
                str(PLACES_PARQUET),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        consumer = subprocess.Popen(
            ["head", "-n", "1"],
            stdin=producer.stdout,
            stdout=subprocess.DEVNULL,
        )
        producer.stdout.close()
        consumer.wait(timeout=30)
        stderr = producer.stderr.read()
        producer.stderr.close()
        producer.wait(timeout=30)

        assert producer.returncode == 0, (
            f"gpio convert geojson exited {producer.returncode} after downstream "
            f"closed pipe. stderr: {stderr.decode(errors='replace')!r}"
        )


@pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
class TestNonUtf8Stdout:
    """GeoJSON stays writable when the console codec cannot spell the data.

    Regression for the Windows slow-tests failure on `docs/guide/geojson.md`:
    GeoJSON is UTF-8 by definition (RFC 7946 s11), but Python gives
    ``sys.stdout`` the console's codec - cp1252 on a default Windows console -
    and only the CLI group callback reconfigured it. A Python API caller kept
    the console codec, so ``Table.to_geojson()`` died on the first place name
    outside Latin-1 (``UnicodeEncodeError`` on ``\u016b``, "Reicelas Buris").
    """

    #: A name in the fixture that cp1252 cannot encode; 125 of the 766 rows
    #: carry one, so the first flush is enough to trip the old behavior.
    NON_LATIN1_NAME = "Reicelas B\u016bris"

    @staticmethod
    def _cp1252_stdout() -> io.TextIOWrapper:
        """A stand-in for a default Windows console."""
        return io.TextIOWrapper(io.BytesIO(), encoding="cp1252", newline="")

    def _written(self, stream: io.TextIOWrapper) -> str:
        stream.flush()
        return stream.buffer.getvalue().decode("utf-8")

    def test_geojsonseq_survives_a_cp1252_stdout(self, monkeypatch):
        """The streaming (GeoJSONSeq) path is the one the docs example hits."""
        stream = self._cp1252_stdout()
        monkeypatch.setattr(sys, "stdout", stream)

        count = convert_to_geojson_stream(str(PLACES_PARQUET))

        assert count == 766
        assert self.NON_LATIN1_NAME in self._written(stream)

    def test_feature_collection_survives_a_cp1252_stdout(self, monkeypatch):
        """`seq=False` writes a FeatureCollection to stdout, and must too."""
        stream = self._cp1252_stdout()
        monkeypatch.setattr(sys, "stdout", stream)

        count = convert_to_geojson_stream(str(PLACES_PARQUET), seq=False)

        assert count == 766
        assert self.NON_LATIN1_NAME in json.dumps(
            json.loads(self._written(stream)), ensure_ascii=False
        )

    def test_a_pipe_that_broke_before_the_first_write_is_still_handled(self, monkeypatch):
        """Entering the wrapper flushes, and that flush is inside the handler.

        Switching stdout to UTF-8 has to push out whatever the caller already
        buffered under the old codec first. If the reader is gone by then that
        flush raises, and it must reach the BrokenPipeError handler below rather
        than escaping as the exit-1 regression of issue #421.
        """

        class _BrokenPipeStdout(io.TextIOWrapper):
            def flush(self):
                raise BrokenPipeError(32, "Broken pipe")

            def close(self):
                """No-op: closing flushes, and this fake's flush always raises.

                Without it the destructor raises during collection and pytest
                reports an unraisable exception for a test that passed.
                """

        monkeypatch.setattr(
            sys, "stdout", _BrokenPipeStdout(io.BytesIO(), encoding="cp1252", newline="")
        )

        assert convert_to_geojson_stream(str(PLACES_PARQUET)) == 0

    def test_pretty_geojsonseq_survives_a_cp1252_stdout(self, monkeypatch):
        """`--pretty` re-serializes each feature through a separate write.

        `json.dumps` escapes non-ASCII by default, so this path could not have
        hit the encoding bug - but it is the other half of the branch and it
        must still come out as 766 parseable features with the names intact.
        """
        stream = self._cp1252_stdout()
        monkeypatch.setattr(sys, "stdout", stream)

        assert convert_to_geojson_stream(str(PLACES_PARQUET), pretty=True) == 766

        features = [json.loads(chunk) for chunk in self._written(stream).split(RS) if chunk.strip()]
        assert len(features) == 766
        assert any(f["properties"]["name"] == self.NON_LATIN1_NAME for f in features)

    def test_pretty_feature_collection_survives_a_cp1252_stdout(self, monkeypatch):
        """The indented FeatureCollection takes the same stdout path."""
        stream = self._cp1252_stdout()
        monkeypatch.setattr(sys, "stdout", stream)

        count = convert_to_geojson_stream(str(PLACES_PARQUET), seq=False, pretty=True)

        assert count == 766
        assert self.NON_LATIN1_NAME in json.dumps(
            json.loads(self._written(stream)), ensure_ascii=False
        )

    @pytest.mark.parametrize(
        ("encoding", "expected"),
        [
            ("utf-8", True),
            # The reason for codecs.lookup rather than a string compare: Python
            # reports the same codec under several names.
            ("UTF8", True),
            ("utf_8", True),
            ("cp1252", False),
            # A stream can name a codec Python does not have, and an unknown
            # name must read as "not UTF-8" rather than raising mid-stream.
            ("definitely-not-a-codec", False),
        ],
    )
    def test_encodes_utf8_reads_the_codec_not_the_spelling(self, encoding, expected):
        assert _encodes_utf8(SimpleNamespace(encoding=encoding)) is expected

    @pytest.mark.parametrize("stream", [io.StringIO(), SimpleNamespace(encoding=None)])
    def test_a_stream_with_no_codec_is_not_assumed_to_be_utf8(self, stream):
        """A StringIO takes str directly and names no encoding; assume nothing."""
        assert _encodes_utf8(stream) is False

    def test_a_utf8_stdout_is_left_alone(self, monkeypatch):
        """Nothing is reconfigured when the stream already speaks UTF-8."""
        stream = io.TextIOWrapper(io.BytesIO(), encoding="utf-8", newline="")
        monkeypatch.setattr(sys, "stdout", stream)

        convert_to_geojson_stream(str(PLACES_PARQUET))

        assert stream.encoding.lower().replace("-", "") == "utf8"
        assert self.NON_LATIN1_NAME in self._written(stream)


@pytest.mark.skipif(not BUILDINGS_PARQUET.exists(), reason="Test data not available")
class TestPolygonGeometries:
    """Tests with polygon geometries (buildings)."""

    def test_polygon_output(self, capsys):
        """Test GeoJSON output for polygon geometries."""
        convert_to_geojson_stream(str(BUILDINGS_PARQUET), rs=False)

        captured = capsys.readouterr()
        lines = [line for line in captured.out.strip().split("\n") if line]

        assert len(lines) > 0

        # Check first feature has polygon geometry
        feature = json.loads(lines[0])
        assert feature["type"] == "Feature"
        assert feature["geometry"]["type"] in ["Polygon", "MultiPolygon"]


@pytest.fixture
def output_file():
    """Create a temp output file path."""
    tmp_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.geojson"
    yield str(tmp_path)
    if tmp_path.exists():
        tmp_path.unlink()


@pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
class TestConvertToGeoJSONFile:
    """Tests for file output mode."""

    def test_writes_valid_geojson_file(self, output_file):
        """Test that file output produces valid GeoJSON FeatureCollection."""
        count = convert_to_geojson(str(PLACES_PARQUET), output_path=output_file)

        assert count > 0
        assert Path(output_file).exists()

        with open(output_file, encoding="utf-8") as f:
            geojson = json.load(f)

        assert geojson["type"] == "FeatureCollection"
        assert "features" in geojson
        assert len(geojson["features"]) == count

    @pytest.mark.parametrize("pretty", [False, True])
    def test_file_output_uses_lf_on_every_platform(self, output_file, pretty):
        """The same input must produce the same bytes on Windows as everywhere else.

        Python text mode translates every "\\n" to ``os.linesep``, so without an
        explicit ``newline="\\n"`` the writer emitted CRLF GeoJSON on Windows.
        That is a different file for the same input: checksums stop matching
        across platforms, byte offsets shift, and anything reading the stream as
        bytes sees separators it did not ask for. This assertion is load-bearing
        on the Windows CI lane; elsewhere it just documents the guarantee.
        """
        convert_to_geojson(str(PLACES_PARQUET), output_path=output_file, pretty=pretty)

        raw = Path(output_file).read_bytes()
        assert b"\r\n" not in raw, "GeoJSON output contains CRLF line endings"
        assert raw.endswith(b"\n")
        # Still parses, i.e. the newline argument did not cost us anything.
        assert json.loads(raw.decode("utf-8"))["type"] == "FeatureCollection"

    def test_routing_based_on_output(self, output_file, capsys):
        """Test that convert_to_geojson routes correctly based on output."""
        # Without output - streams to stdout
        count_stream = convert_to_geojson(str(PLACES_PARQUET), rs=False)
        captured = capsys.readouterr()
        assert len(captured.out.strip().split("\n")) == count_stream

        # With output - writes to file
        count_file = convert_to_geojson(str(PLACES_PARQUET), output_path=output_file)
        assert Path(output_file).exists()
        assert count_file > 0


class TestNeedsReprojection:
    """Tests for CRS detection and reprojection logic."""

    def test_wgs84_epsg4326_no_reprojection(self):
        """WGS84 (EPSG:4326) should not need reprojection."""
        assert _needs_reprojection("EPSG:4326") is False

    def test_wgs84_ogc_crs84_no_reprojection(self):
        """OGC:CRS84 is WGS84 and should not need reprojection."""
        assert _needs_reprojection("OGC:CRS84") is False

    def test_wgs84_crs84_no_reprojection(self):
        """CRS84 variant should not need reprojection."""
        assert _needs_reprojection("CRS84") is False

    def test_wgs84_case_insensitive(self):
        """WGS84 detection should be case insensitive."""
        assert _needs_reprojection("epsg:4326") is False
        assert _needs_reprojection("EPSG:4326") is False

    def test_other_crs_needs_reprojection(self):
        """Non-WGS84 CRS should need reprojection."""
        assert _needs_reprojection("EPSG:32610") is True
        assert _needs_reprojection("EPSG:3857") is True
        assert _needs_reprojection("EPSG:28992") is True

    def test_none_crs_no_reprojection(self):
        """None CRS should not need reprojection (assume WGS84)."""
        assert _needs_reprojection(None) is False


class TestBuildFeatureQueryWithReprojection:
    """Tests for query building with CRS reprojection."""

    def test_query_without_reprojection(self):
        """Test query without source_crs produces no ST_Transform."""
        query = _build_feature_query("test_table", "geometry", ["name"], source_crs=None)
        assert "ST_Transform" not in query
        assert "ST_AsGeoJSON" in query

    def test_query_with_reprojection(self):
        """Test query with source_crs includes ST_Transform."""
        query = _build_feature_query("test_table", "geometry", ["name"], source_crs="EPSG:32610")
        assert "ST_Transform" in query
        assert "EPSG:32610" in query
        assert WGS84_CRS in query

    def test_query_with_bbox_and_reprojection(self):
        """Test query with both bbox and reprojection."""
        query = _build_feature_query(
            "test_table",
            "geometry",
            ["name"],
            write_bbox=True,
            source_crs="EPSG:3857",
        )
        assert "ST_Transform" in query
        assert "ST_Envelope" in query or "ST_XMin" in query


class TestKeepCrsFlag:
    """Tests for --keep-crs flag behavior."""

    def test_keep_crs_in_streaming_mode(self, capsys):
        """Test keep_crs parameter works in streaming mode."""
        # Should not raise with keep_crs=True
        count = convert_to_geojson_stream(
            str(PLACES_PARQUET),
            rs=False,
            keep_crs=True,
        )
        assert count > 0

    def test_keep_crs_in_file_mode(self, output_file):
        """Test keep_crs parameter works in file mode."""
        count = convert_to_geojson(
            str(PLACES_PARQUET),
            output_path=output_file,
            keep_crs=True,
        )
        assert count > 0
        assert Path(output_file).exists()


class TestWgs84Constant:
    """Tests for WGS84_CRS constant."""

    def test_wgs84_crs_value(self):
        """Test WGS84_CRS is correctly defined."""
        assert WGS84_CRS == "EPSG:4326"


class TestRepairAfterReprojection:
    """Geometry must be repaired in the CRS it is written in, not the source CRS.

    Reprojection is not topology-preserving, so repairing first leaves
    ST_ReducePrecision to raise TopologyException on geometry the transform has
    just invalidated.
    """

    # A sliver from the SITEX Extremadura "mancomunidades" layer: valid in
    # EPSG:25830 (two of its vertices differ only in the 13th decimal), invalid
    # once transformed to WGS84. Repairing before the transform makes
    # ST_ReducePrecision raise "TopologyException: unable to assign free hole to
    # a shell" on it.
    SLIVER_25830 = (
        "POLYGON (("
        "217334.43251087563 4419513.990336159, "
        "217334.2614384655 4419513.996670783, "
        "217334.2527995768 4419513.996990671, "
        "217334.5221580651 4419514.098189879, "
        "217334.60769426968 4419514.09502257, "
        "217334.60769426887 4419514.09502257, "
        "217334.43251087563 4419513.990336159))"
    )

    def test_repair_wraps_the_reprojected_geometry(self):
        """The repair must be applied after the transform, not before it."""
        query = _build_feature_query("t", "geometry", ["name"], source_crs="EPSG:25830")
        # The transform happens in the subquery that feeds the repair, so the
        # repaired expression is a plain column reference.
        assert "ST_MakeValid(ST_Transform(" not in query.replace(" ", "")
        transform_pos = query.index("ST_Transform")
        repair_pos = query.index("ST_MakeValid")
        assert transform_pos > repair_pos, "repair must consume the reprojected column"

    def test_transform_is_evaluated_once_per_row(self):
        """repair_geometry_sql repeats its argument; ST_Transform must not be."""
        query = _build_feature_query(
            "t", "geometry", ["name"], source_crs="EPSG:25830", write_bbox=True
        )
        assert query.count("ST_Transform") == 1

    def test_reprojected_sliver_survives_precision_reduction(self):
        """The real-world regression: this raised before the reorder."""
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(
            f"CREATE TABLE t AS SELECT ST_GeomFromText('{self.SLIVER_25830}') "
            "AS geometry, 'sliver' AS name"
        )
        query = _build_feature_query(
            "t", "geometry", ["name"], precision=6, source_crs="EPSG:25830"
        )
        rows = con.execute(query).fetchall()
        assert len(rows) == 1
        assert json.loads(rows[0][0])["type"] == "Feature"

    def test_null_geometry_still_filtered_with_reprojection(self):
        """The NULL filter must survive the subquery rewrite."""
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(
            "CREATE TABLE t AS SELECT * FROM (VALUES "
            f"(ST_GeomFromText('{self.SLIVER_25830}'), 'kept'), "
            "(NULL::GEOMETRY, 'dropped')) AS v(geometry, name)"
        )
        query = _build_feature_query(
            "t", "geometry", ["name"], precision=6, source_crs="EPSG:25830"
        )
        rows = con.execute(query).fetchall()
        assert len(rows) == 1
        assert json.loads(rows[0][0])["properties"]["name"] == "kept"


class TestFeatureMembersEmitValidJson:
    """#726: --write-bbox and --id-field appended a comma *outside* the SQL
    string literal, so it split the feature SELECT into two columns instead of
    landing in the JSON. Every Feature came out truncated after the option's
    own member, and the writer failed to parse its own output."""

    @staticmethod
    def _convert(tmp_path, places_test_file, *extra_args):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        out = tmp_path / "out.geojson"
        result = CliRunner().invoke(
            cli, ["convert", "geojson", places_test_file, str(out), *extra_args]
        )
        assert result.exit_code == 0, result.output
        return json.loads(out.read_text())

    def test_write_bbox_emits_parseable_features(self, tmp_path, places_test_file):
        data = self._convert(tmp_path, places_test_file, "--write-bbox")

        assert data["type"] == "FeatureCollection"
        assert data["features"]
        for feature in data["features"]:
            assert len(feature["bbox"]) == 4
            assert feature["geometry"] is not None
            assert "properties" in feature

    def test_id_field_emits_parseable_features(self, tmp_path, places_test_file):
        data = self._convert(tmp_path, places_test_file, "--id-field", "name")

        assert data["features"]
        for feature in data["features"]:
            assert feature["id"] == feature["properties"]["name"]
            assert feature["geometry"] is not None

    def test_bbox_and_id_field_together(self, tmp_path, places_test_file):
        data = self._convert(tmp_path, places_test_file, "--write-bbox", "--id-field", "name")

        assert data["features"]
        for feature in data["features"]:
            assert "id" in feature
            assert len(feature["bbox"]) == 4
            assert feature["geometry"] is not None

    def test_feature_query_is_one_column_in_every_option_combination(self):
        """The regression itself: a stray comma makes this several columns."""
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("CREATE TABLE t AS SELECT ST_Point(1, 2) AS geometry, 'a' AS name")

        for options in (
            {},
            {"write_bbox": True},
            {"id_field": "name"},
            {"write_bbox": True, "id_field": "name"},
        ):
            query = _build_feature_query("t", "geometry", ["name"], **options)
            assert len(con.execute(query).description) == 1, options

    def test_null_id_value_omits_the_member(self):
        """A NULL id must not null the whole Feature (`||` propagates NULL).

        An optional key with a missing value is ordinary data, and the failure
        was not a bad `id` -- it aborted the entire conversion, because the
        writer got `None` where it expected a Feature string.
        """
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(
            "CREATE TABLE t AS SELECT * FROM (VALUES "
            "(ST_Point(1, 2), 'has-id'), "
            "(ST_Point(3, 4), NULL)) AS v(geometry, name)"
        )
        query = _build_feature_query("t", "geometry", ["name"], id_field="name")

        rows = [row[0] for row in con.execute(query).fetchall()]

        assert len(rows) == 2
        assert all(row is not None for row in rows), "a NULL id nulled the Feature"
        features = [json.loads(row) for row in rows]
        assert features[0]["id"] == "has-id"
        # RFC 7946 wants `id` to be a string or a number, so omit rather than null.
        assert "id" not in features[1]
        assert features[1]["geometry"] is not None

    def test_empty_geometry_omits_the_bbox(self):
        """ST_XMin of an EMPTY geometry is NULL, which nulled the whole Feature.

        EMPTY is not NULL, so it passes the query's `IS NOT NULL` filter, and
        --precision or geometry repair can produce it from valid input.
        """
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(
            "CREATE TABLE t AS SELECT * FROM (VALUES "
            "(ST_Point(1, 2), 'point'), "
            "(ST_GeomFromText('POLYGON EMPTY'), 'empty')) AS v(geometry, name)"
        )
        query = _build_feature_query("t", "geometry", ["name"], write_bbox=True)

        rows = [row[0] for row in con.execute(query).fetchall()]

        assert len(rows) == 2
        assert all(row is not None for row in rows), "an EMPTY geometry nulled the Feature"
        features = [json.loads(row) for row in rows]
        assert features[0]["bbox"] == [1, 2, 1, 2]
        assert "bbox" not in features[1]
