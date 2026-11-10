"""
Integration tests for Arrow IPC piping between CLI commands.

Tests multi-stage pipelines like:
    gpio add bbox input.parquet | gpio sort hilbert - output.parquet
    gpio extract input.parquet | gpio add bbox - | gpio add quadkey - output.parquet
"""

from __future__ import annotations

import subprocess
import tempfile
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.conftest import safe_rmtree, safe_unlink

# Whole-pipeline subprocess checks belong in the slow lane (#1402).
pytestmark = pytest.mark.slow

TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES_PARQUET = TEST_DATA_DIR / "places_test.parquet"


def run_pipeline(commands: list[str], timeout: int = 60) -> subprocess.CompletedProcess:
    """Run a shell pipeline and return the result."""
    pipeline = " | ".join(commands)
    return subprocess.run(
        pipeline,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


@pytest.fixture
def output_file():
    """Create a temporary output file path."""
    tmp_path = Path(tempfile.gettempdir()) / f"test_pipe_{uuid.uuid4()}.parquet"
    yield str(tmp_path)
    safe_unlink(tmp_path)


@pytest.fixture
def output_dir():
    """Create a temporary output directory."""
    tmp_path = Path(tempfile.gettempdir()) / f"test_pipe_dir_{uuid.uuid4()}"
    tmp_path.mkdir(exist_ok=True)
    yield str(tmp_path)
    safe_rmtree(tmp_path)


class TestTwoStagePipelines:
    """Tests for two-stage command pipelines."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_add_bbox_to_sort_hilbert(self, output_file):
        """Test: gpio add bbox input | gpio sort hilbert - output."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio add bbox --bbox-name bbox_test {PLACES_PARQUET} -",
                f"gpio sort hilbert - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        # Verify output has bbox_test and is sorted
        table = pq.read_table(output_file)
        assert "bbox_test" in table.column_names
        assert table.num_rows == 766

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_extract_to_add_bbox(self, output_file):
        """Test: gpio extract --limit 100 input | gpio add bbox - output."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio extract --limit 100 {PLACES_PARQUET} -",
                f"gpio add bbox --bbox-name bbox_test - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox_test" in table.column_names
        assert table.num_rows == 100

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_add_bbox_to_add_quadkey(self, output_file):
        """Test: gpio add bbox input | gpio add quadkey - output."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio add bbox --bbox-name bbox_test {PLACES_PARQUET} -",
                f"gpio add quadkey - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox_test" in table.column_names
        assert "quadkey" in table.column_names
        assert table.num_rows == 766


class TestThreeStagePipelines:
    """Tests for three-stage command pipelines."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_extract_add_bbox_add_quadkey(self, output_file):
        """Test: extract | add bbox | add quadkey."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio extract --limit 50 {PLACES_PARQUET} -",
                "gpio add bbox --bbox-name bbox_test - -",
                f"gpio add quadkey - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox_test" in table.column_names
        assert "quadkey" in table.column_names
        assert table.num_rows == 50

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_add_bbox_add_quadkey_sort_hilbert(self, output_file):
        """Test: add bbox | add quadkey | sort hilbert."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio add bbox --bbox-name bbox_test {PLACES_PARQUET} -",
                "gpio add quadkey - -",
                f"gpio sort hilbert - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox_test" in table.column_names
        assert "quadkey" in table.column_names
        assert table.num_rows == 766


class TestPartitionWithPipes:
    """Tests for partition command with stdin input."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_add_quadkey_to_partition(self, output_dir):
        """Test: add quadkey | partition string (stdin to directory)."""
        result = run_pipeline(
            [
                f"gpio add quadkey {PLACES_PARQUET} -",
                f"gpio partition string --column quadkey --chars 2 - {output_dir}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        # Check that partitioned files were created
        output_path = Path(output_dir)
        parquet_files = list(output_path.glob("**/*.parquet"))
        assert len(parquet_files) > 0, "No partitioned files created"


class TestFullPipeline:
    """Tests for full multi-stage pipelines."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_full_transform_pipeline(self, output_file):
        """Test: extract | add bbox | add quadkey | sort hilbert."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio extract --limit 100 {PLACES_PARQUET} -",
                "gpio add bbox --bbox-name bbox_test - -",
                "gpio add quadkey - -",
                f"gpio sort hilbert - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox_test" in table.column_names
        assert "quadkey" in table.column_names
        assert table.num_rows == 100


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_single_row_pipeline(self, output_file):
        """Test pipeline with single row extract."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio extract --limit 1 {PLACES_PARQUET} -",
                f"gpio add bbox --bbox-name bbox_test - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        table = pq.read_table(output_file)
        assert table.num_rows == 1
        assert "bbox_test" in table.column_names

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_column_selection_through_pipe(self, output_file):
        """Test that column selection works through pipe."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio extract --include-cols name,address {PLACES_PARQUET} -",
                f"gpio add bbox --bbox-name bbox_test - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        table = pq.read_table(output_file)
        # Should have: name, address, geometry (auto-included), bbox_test (added)
        assert "name" in table.column_names
        assert "address" in table.column_names
        assert "geometry" in table.column_names
        assert "bbox_test" in table.column_names


class TestStdinToNamedGeoJsonOutput:
    """#723: `gpio convert geojson - out.geojson` failed with "File not found: -".

    The message was wrong about what happened -- `-` is understood a moment
    earlier in the redirect form, and the named path is the *output*. The
    streaming converter already writes a FeatureCollection to a named path, so
    the pipeline works once the CLI stops routing stdin through the file-mode
    writer that can only open a path.
    """

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_stdin_to_named_geojson_file(self, tmp_path):
        import json

        output = tmp_path / "out.geojson"
        result = run_pipeline(
            [
                f"gpio extract --limit 5 {PLACES_PARQUET} -",
                f"gpio convert geojson - {output}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        data = json.loads(output.read_text())
        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 5
        assert all(f["geometry"] is not None for f in data["features"])

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_stdin_redirect_form_still_works(self, tmp_path):
        """The documented form must keep working."""
        import json

        output = tmp_path / "out.geojson"
        result = subprocess.run(
            f"gpio extract --limit 5 {PLACES_PARQUET} - | "
            f"gpio convert geojson - --feature-collection --no-rs > {output}",
            shell=True,
            capture_output=True,
            text=True,
            timeout=60,
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert json.loads(output.read_text())["type"] == "FeatureCollection"

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_stdin_to_named_file_with_bbox_and_id_field(self, tmp_path):
        """The combination neither #723 nor #726 covered on its own.

        Routing stdin into file mode sends it through `_build_feature_query`,
        the function #726 fixed. Before that fix this pipeline did not fail --
        it silently wrote truncated, unparsable GeoJSON, which is worse than
        the `File not found: -` it replaced. Guard the intersection.
        """
        import json

        output = tmp_path / "out.geojson"
        result = run_pipeline(
            [
                f"gpio extract --limit 5 {PLACES_PARQUET} -",
                f"gpio convert geojson - {output} --write-bbox --id-field name",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        data = json.loads(output.read_text())
        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 5
        for feature in data["features"]:
            assert len(feature["bbox"]) == 4
            assert feature["geometry"] is not None
            assert "properties" in feature


class TestExtractIntoPartition:
    """#722: `extract` -> ... -> `partition` died with a raw DuckDB traceback.

    A filtered `extract` invalidates the input's `bbox` and `geometry_types`,
    so it dropped them; the file-writing path recomputed them but the Arrow IPC
    stream did not. `partition` spools its stdin to a temp Parquet, and DuckDB
    refuses to read a Parquet whose `geo` metadata declares a geometry column
    with no `geometry_types` -- so only the piped-into-partition case broke.
    Both chains below are from docs/guide/piping.md.
    """

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_extract_stream_carries_geometry_types(self):
        """The stream itself, read straight off stdout, must declare the key."""
        import json

        import pyarrow.ipc as ipc

        result = subprocess.run(
            f"gpio extract --bbox=-0.5,9.8,0.5,11.0 {PLACES_PARQUET} -",
            shell=True,
            capture_output=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stderr.decode()

        table = ipc.RecordBatchStreamReader(result.stdout).read_all()
        assert table.num_rows > 0
        geo = json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
        col = geo["columns"][geo["primary_column"]]
        assert col["geometry_types"] == ["Point"]

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_spatial_filter_and_partition_chain(self, output_dir):
        """piping.md "Spatial Filter and Partition", verbatim."""
        result = run_pipeline(
            [
                f"gpio extract --bbox=-4,4,4,12 {PLACES_PARQUET} -",
                "gpio add quadkey -",
                f"gpio partition string --column quadkey --chars 4 - {output_dir}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert "Traceback (most recent call last)" not in result.stderr

        files = list(Path(output_dir).glob("**/*.parquet"))
        assert files, "no partitions written"
        assert sum(pq.read_table(f).num_rows for f in files) > 0

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_full_processing_pipeline_chain(self, output_dir):
        """piping.md "Full Processing Pipeline" (lines 126-132), the same shape.

        Three deviations from the printed example, none of them about #722:
        `--force` on `add bbox` because this fixture already has a bbox column;
        a wider `--bbox` and H3 resolution 2 instead of 8/4, because 766 points
        cut to a handful per cell trip `partition h3`'s tiny-partition guard --
        a data-size verdict, reached only once the chain is readable at all.
        """
        result = run_pipeline(
            [
                f"gpio extract --bbox=-4,4,4,12 {PLACES_PARQUET} -",
                "gpio add bbox --force -",
                "gpio add h3 --resolution 2 -",
                "gpio sort hilbert -",
                f"gpio partition h3 --resolution 2 - {output_dir}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert "Traceback (most recent call last)" not in result.stderr
        assert list(Path(output_dir).glob("**/*.parquet")), "no partitions written"


def _two_geometry_file(path: Path) -> Path:
    """Write a GeoParquet with a primary ``geometry`` and a secondary ``centroid``."""
    import json
    import struct

    import pyarrow as pa

    def wkb_point(x, y):
        return struct.pack("<BIdd", 1, 1, x, y)

    col_meta = {"encoding": "WKB", "geometry_types": ["Point"], "bbox": [0.0, 0.0, 5.0, 5.0]}
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": dict(col_meta), "centroid": dict(col_meta)},
    }
    n = 12
    table = pa.table(
        {
            "id": list(range(n)),
            "name": [f"{chr(ord('a') + i % 4)}-{i}" for i in range(n)],
            "geometry": [wkb_point(i % 5, i % 5) for i in range(n)],
            "centroid": [wkb_point(i % 5, i % 5) for i in range(n)],
        }
    )
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")}), path)
    return path


def _stream_without_geometry_types(dst: Path) -> Path:
    """Write an Arrow IPC stream file whose ``geo`` omits ``geometry_types``.

    A stream can come from any producer, not just gpio, and DuckDB refuses to
    open a Parquet file whose ``geo`` metadata declares a geometry column
    without the key GeoParquet 1.1 requires -- so every command that spools its
    stdin to a temp Parquet has to fill the gap in the file it owns (#722).
    """
    import json

    import pyarrow.ipc as ipc

    table = pq.read_table(PLACES_PARQUET)
    metadata = dict(table.schema.metadata or {})
    geo = json.loads(metadata[b"geo"].decode("utf-8"))
    for col_meta in geo["columns"].values():
        col_meta.pop("geometry_types", None)
        col_meta.pop("bbox", None)
    metadata[b"geo"] = json.dumps(geo).encode("utf-8")
    table = table.replace_schema_metadata(metadata)

    with open(dst, "wb") as fh:
        with ipc.new_stream(fh, table.schema) as writer:
            writer.write_table(table)
    return dst


class TestProjectionDropsSecondaryGeometry:
    """#722, the projection half: pruning, not just backfilling.

    ``extract --include-cols`` can drop a secondary geometry column. The file
    path prunes the ``geo`` metadata to the columns it writes; the stream path
    did not, so the next stage got a ``geo`` block naming an absent column with
    no ``geometry_types`` -- and DuckDB refused to read the temp Parquet.
    """

    def test_stream_geo_metadata_names_only_present_columns(self, tmp_path):
        import json

        import pyarrow.ipc as ipc

        src = _two_geometry_file(tmp_path / "two_geom.parquet")
        result = subprocess.run(
            [
                "gpio",
                "extract",
                "--include-cols",
                "id,name,geometry",
                "--where",
                "id < 8",
                str(src),
                "-",
            ],
            capture_output=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stderr.decode()

        table = ipc.RecordBatchStreamReader(result.stdout).read_all()
        geo = json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
        assert "centroid" not in table.column_names
        assert set(geo["columns"]) <= set(table.column_names), sorted(geo["columns"])

    def test_projected_stream_pipes_into_partition(self, tmp_path, output_dir):
        src = _two_geometry_file(tmp_path / "two_geom.parquet")
        result = run_pipeline(
            [
                f'gpio extract --include-cols id,name,geometry --where "id < 8" {src} -',
                f"gpio partition string --column name --chars 1 --force --skip-analysis - {output_dir}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert "Traceback (most recent call last)" not in result.stderr
        assert list(Path(output_dir).glob("**/*.parquet")), "no partitions written"


class TestStdinWithoutGeometryTypes:
    """Commands that spool stdin to a temp Parquet must fill in the missing key.

    ``reproject`` and ``sort quadkey`` hand-rolled the bridge instead of using
    ``read_stdin_to_temp_file``, so a stream whose ``geo`` omits
    ``geometry_types`` reached DuckDB as an unreadable file and the command died
    with a raw ``InvalidInputException`` (#722).
    """

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_reproject_from_stdin(self, tmp_path, output_file):
        stream = _stream_without_geometry_types(tmp_path / "nogt.arrows")
        with open(stream, "rb") as fh:
            result = subprocess.run(
                ["gpio", "convert", "reproject", "--dst-crs", "EPSG:3857", "-", output_file],
                stdin=fh,
                capture_output=True,
                text=True,
                timeout=120,
            )

        assert result.returncode == 0, result.stderr
        assert "Traceback (most recent call last)" not in result.stderr
        assert pq.read_table(output_file).num_rows > 0

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_sort_quadkey_from_stdin(self, tmp_path, output_file):
        stream = _stream_without_geometry_types(tmp_path / "nogt.arrows")
        with open(stream, "rb") as fh:
            result = subprocess.run(
                ["gpio", "sort", "quadkey", "-", output_file],
                stdin=fh,
                capture_output=True,
                text=True,
                timeout=120,
            )

        assert result.returncode == 0, result.stderr
        assert "Traceback (most recent call last)" not in result.stderr
        assert pq.read_table(output_file).num_rows > 0


class TestZeroRowStreams:
    """#804: streaming a zero-row result aborted the process with SIGABRT.

    An empty result is an ordinary outcome of a spatial filter, not an error.
    A DuckDB result with no rows exports an Arrow table whose columns are
    ChunkedArrays with *zero* chunks; ``geoarrow.pyarrow.as_wkb`` then rebuilds
    a ChunkedArray from an empty chunk list with no explicit type, which trips
    an Arrow C++ ``Check failed`` and aborts the interpreter (exit 134). The
    abort is not a Python exception, so these tests must shell out.

    Note the ``--bbox=VALUE`` form: Windows ``cmd.exe`` does not strip single
    quotes, so the quoted form only fails on the Windows CI matrix.
    """

    # Far outside the fixture's extent (Ghana/Burkina Faso), so zero rows match.
    EMPTY_BBOX = "--bbox=170,-80,171,-79"

    @staticmethod
    def _read_stream(raw: bytes):
        import pyarrow.ipc as ipc

        return ipc.RecordBatchStreamReader(pa.BufferReader(pa.py_buffer(raw))).read_all()

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_extract_zero_rows_to_stdout(self):
        """`gpio extract <empty bbox> in.parquet -` must emit a valid empty stream."""
        result = subprocess.run(
            f"gpio extract {self.EMPTY_BBOX} {PLACES_PARQUET} -",
            shell=True,
            capture_output=True,
            timeout=60,
        )

        assert result.returncode == 0, (
            f"exit={result.returncode} (134 == SIGABRT): {result.stderr.decode(errors='replace')}"
        )
        assert result.stdout, "zero-row stream must still carry a schema, not zero bytes"

        table = self._read_stream(result.stdout)
        assert table.num_rows == 0
        assert "geometry" in table.column_names
        assert "name" in table.column_names

    @staticmethod
    def _narrow(type_: pa.DataType) -> pa.DataType:
        """Reduce a type to the shape that is stable across readers.

        Two differences here are real but orthogonal to this fix:

        * DuckDB exports strings and blobs as ``large_string``/``large_binary``
          while the Parquet writer stores the 32-bit forms -- for *every*
          result, empty or not, so it is not a zero-row artifact.
        * Whether ``geoarrow.pyarrow`` happens to be imported in *this* process
          decides if the stream's ``ARROW:extension:name`` resolves into a
          registered ``WkbType`` or stays a bare ``binary`` field carrying
          metadata. Under ``pytest -n auto`` that depends on which other tests
          share the worker, so comparing the raw types is order-dependent.

        Unwrapping to storage type and narrowing the offsets leaves the part
        the assertion is actually about: which columns there are, and what
        they hold.
        """
        if isinstance(type_, pa.ExtensionType):
            type_ = type_.storage_type
        if type_ == pa.large_string():
            return pa.string()
        if type_ == pa.large_binary():
            return pa.binary()
        return type_

    @staticmethod
    def _extension_name(field: pa.Field) -> str:
        """The geoarrow extension name, however this process chose to expose it.

        A registered extension type carries it as ``extension_name``; an
        unregistered one leaves it in the field metadata. See ``_narrow``.
        """
        if isinstance(field.type, pa.ExtensionType):
            return field.type.extension_name
        return (field.metadata or {}).get(b"ARROW:extension:name", b"").decode()

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_zero_row_stream_matches_file_output_schema(self, output_file):
        """The stream's schema must match what the file path writes for the same filter."""
        file_result = subprocess.run(
            f"gpio extract {self.EMPTY_BBOX} {PLACES_PARQUET} {output_file}",
            shell=True,
            capture_output=True,
            timeout=60,
        )
        assert file_result.returncode == 0, file_result.stderr.decode(errors="replace")
        file_table = pq.read_table(output_file)
        assert file_table.num_rows == 0

        stream_result = subprocess.run(
            f"gpio extract {self.EMPTY_BBOX} {PLACES_PARQUET} -",
            shell=True,
            capture_output=True,
            timeout=60,
        )
        assert stream_result.returncode == 0, stream_result.stderr.decode(errors="replace")

        stream_table = self._read_stream(stream_result.stdout)
        assert stream_table.column_names == file_table.column_names
        # Types, not fields: the geometry field carries an extra
        # ARROW:extension:name entry on the stream side only.
        assert [self._narrow(f.type) for f in stream_table.schema] == [
            self._narrow(f.type) for f in file_table.schema
        ]

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_zero_row_stream_schema_matches_a_non_empty_stream(self):
        """The empty stream must be the same stream, minus the rows.

        The bug this guards was a *typing* failure -- geoarrow rebuilding the
        geometry column with no type at all -- so an empty result that merely
        parses is not enough. Compared stream-to-stream, with no normalizing,
        every field including the geoarrow extension metadata must be identical
        to what the same command emits when rows do match.
        """
        empty = subprocess.run(
            f"gpio extract {self.EMPTY_BBOX} {PLACES_PARQUET} -",
            shell=True,
            capture_output=True,
            timeout=60,
        )
        assert empty.returncode == 0, empty.stderr.decode(errors="replace")

        populated = subprocess.run(
            f"gpio extract --limit 5 {PLACES_PARQUET} -",
            shell=True,
            capture_output=True,
            timeout=60,
        )
        assert populated.returncode == 0, populated.stderr.decode(errors="replace")

        empty_table = self._read_stream(empty.stdout)
        populated_table = self._read_stream(populated.stdout)
        assert empty_table.num_rows == 0
        assert populated_table.num_rows == 5

        empty_geometry = empty_table.schema.field("geometry")
        assert empty_geometry == populated_table.schema.field("geometry")
        assert self._extension_name(empty_geometry) == "geoarrow.wkb"
        assert list(empty_table.schema) == list(populated_table.schema)

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_zero_row_pipe_to_add_bbox(self, output_file):
        """A zero-row stream must be readable by the next stage of a pipe."""
        result = run_pipeline(
            [
                f"gpio extract {self.EMPTY_BBOX} {PLACES_PARQUET} -",
                f"gpio add bbox --bbox-name bbox_test - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        table = pq.read_table(output_file)
        assert table.num_rows == 0
        assert "bbox_test" in table.column_names

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_zero_row_pipe_stays_a_stream_through_two_stages(self):
        """Two streaming stages in a row, ending on stdout, still produce a stream."""
        result = subprocess.run(
            f"gpio extract {self.EMPTY_BBOX} {PLACES_PARQUET} - | "
            "gpio add bbox --bbox-name bbox_test - -",
            shell=True,
            capture_output=True,
            timeout=60,
        )

        assert result.returncode == 0, result.stderr.decode(errors="replace")
        table = self._read_stream(result.stdout)
        assert table.num_rows == 0
        assert "bbox_test" in table.column_names

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_zero_row_sort_hilbert_to_stdout(self, tmp_path):
        """`sort` shares the streaming writer, so it must survive zero rows too."""
        empty = tmp_path / "empty.parquet"
        prep = subprocess.run(
            f"gpio extract {self.EMPTY_BBOX} {PLACES_PARQUET} {empty}",
            shell=True,
            capture_output=True,
            timeout=60,
        )
        assert prep.returncode == 0, prep.stderr.decode(errors="replace")

        result = subprocess.run(
            f"gpio sort hilbert {empty} -",
            shell=True,
            capture_output=True,
            timeout=60,
        )

        assert result.returncode == 0, result.stderr.decode(errors="replace")
        assert self._read_stream(result.stdout).num_rows == 0

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_zero_row_reproject_to_stdout(self, tmp_path):
        """`convert reproject` shares the streaming writer as well."""
        empty = tmp_path / "empty.parquet"
        prep = subprocess.run(
            f"gpio extract {self.EMPTY_BBOX} {PLACES_PARQUET} {empty}",
            shell=True,
            capture_output=True,
            timeout=60,
        )
        assert prep.returncode == 0, prep.stderr.decode(errors="replace")

        result = subprocess.run(
            f"gpio convert reproject {empty} - --dst-crs EPSG:3857",
            shell=True,
            capture_output=True,
            timeout=60,
        )

        assert result.returncode == 0, result.stderr.decode(errors="replace")
        assert self._read_stream(result.stdout).num_rows == 0
