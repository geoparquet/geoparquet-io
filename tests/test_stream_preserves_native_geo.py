"""The streaming path must not demote a native geometry column to plain binary.

Two sites on the pipe path are a plain-pyarrow read or write of a table that
carries a Parquet ``GEOMETRY``/``GEOGRAPHY`` column:

* ``cli/commands/convert.py`` -- ``gpio convert geoparquet in.parquet -`` writes
  its result to a scratch file, reads it back with ``pq.read_table`` and puts
  that table on stdout as an Arrow IPC stream.
* ``core/streaming.read_stdin_to_temp_file`` -- the other end of the same pipe,
  materialising an incoming IPC stream to a scratch Parquet file with
  ``pq.write_table``.

Whether either keeps the geometry column's type depends on nothing in the data
and everything in the *process*: importing ``geoarrow.pyarrow`` registers the
extension types that make pyarrow materialise a Parquet ``GEOMETRY`` column as
``geoarrow.wkb`` and write it back as one, and that registration is global.
Without it the column round-trips as plain ``binary`` -- the logical type, and
the CRS stored inside it, gone from the Arrow schema.

End to end this is lossless anyway, because the ``geo`` key travels alongside
and carries the CRS, which is why it is a latent shape rather than a live
defect (#1006). It stops being lossless for a consumer that reads the Arrow
field type rather than the KV, and it is one import away from being the #993
defect again if anything downstream starts trusting the schema.

**Every test of the registration here runs in a subprocess, and has to.** This
module's own imports register the extension types process-wide, so an
in-process test of a process-global registration is green whatever the code
under test does -- which is exactly how #993's first version shipped with the
bug in it. The one in-process test is about handle discipline, not the
registration, and says so.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1006
Refs: https://github.com/geoparquet/geoparquet-io/issues/993
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest

from tests.native_geo_probes import EPSG_5070

#: A wedge cap rather than a performance budget, matching
#: ``tests/test_add_preserves_native_geo.py``: each of these pays a cold
#: interpreter's full CLI import (pyarrow, geoarrow, duckdb) with no warm
#: process to share it.
_SUBPROCESS_TIMEOUT = 300


def _run_cli_in_a_fresh_process(*args) -> bytes:
    """Run one gpio command in a subprocess and return its raw stdout.

    Raw bytes, not text: what this module measures is an Arrow IPC stream.
    """
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "from geoparquet_io.cli.main import cli; cli()",
            *(str(a) for a in args),
        ],
        capture_output=True,
        timeout=_SUBPROCESS_TIMEOUT,
    )
    assert completed.returncode == 0, completed.stderr.decode("utf-8", "replace")
    return completed.stdout


def _geometry_field_of_stream(payload: bytes) -> pa.Field:
    """The ``geometry`` field of an Arrow IPC stream, as its producer described it."""
    return ipc.RecordBatchStreamReader(pa.BufferReader(payload)).read_all().schema.field("geometry")


def _is_geoarrow(field: pa.Field) -> bool:
    """Did the producer describe this field as a geometry, by either carrier?

    Read in a process that *has* registered the extension types, a field the
    producer emitted as ``geoarrow.wkb`` comes back as an extension type; read
    in one that has not, the same field is plain ``binary`` carrying
    ``ARROW:extension:name``. Both are the producer saying "geometry", and this
    accepts either so the assertion is about the producer rather than about
    this process's own imports.
    """
    if getattr(field.type, "extension_name", None):
        return True
    metadata = field.metadata or {}
    return b"ARROW:extension:name" in metadata


@pytest.fixture(scope="module")
def stream_of(projected_conus, tmp_path_factory) -> Path:
    """The EPSG:5070 fixture as an Arrow IPC stream file, geometry typed.

    Built here rather than by a gpio command on purpose: the consumer test below
    has to be handed a stream that is known-good, or a failure could not be told
    apart from the producer's.
    """
    table = pq.read_table(str(projected_conus))
    assert table.schema.field("geometry").type.extension_name == "geoarrow.wkb", (
        "fixture stream is not geometry-typed; the consumer test would be vacuous"
    )

    path = tmp_path_factory.mktemp("stream") / "conus.arrows"
    with path.open("wb") as handle:
        writer = ipc.RecordBatchStreamWriter(handle, table.schema)
        writer.write_table(table)
        writer.close()
    return path


def test_convert_to_stdout_keeps_the_geometry_field_typed(projected_conus):
    """``gpio convert geoparquet in -`` must not put plain binary on the pipe."""
    payload = _run_cli_in_a_fresh_process("convert", "geoparquet", projected_conus, "-")

    field = _geometry_field_of_stream(payload)
    assert _is_geoarrow(field), f"geometry streamed as {field.type}, not a geometry type"


def test_convert_to_stdout_keeps_the_geo_key(projected_conus):
    """The control: the KV carrier is what makes this latent rather than lossy."""
    payload = _run_cli_in_a_fresh_process("convert", "geoparquet", projected_conus, "-")

    table = ipc.RecordBatchStreamReader(pa.BufferReader(payload)).read_all()
    geo = json.loads((table.schema.metadata or {})[b"geo"].decode("utf-8"))
    assert geo["columns"]["geometry"]["crs"]["id"] == EPSG_5070


def test_convert_to_stdout_closes_the_scratch_file_before_deleting_it(projected_conus, monkeypatch):
    """The handle-closed invariant behind a Windows-only failure, asserted on every OS.

    With the extension types registered, the table ``pq.read_table`` returned
    kept its reader reachable past the frame, and ``temp_path.unlink()`` in the
    ``finally`` raised ``PermissionError: [WinError 32]`` on all three Windows
    legs -- POSIX unlinks an open file, so nothing else saw it. The rule (see
    ``windows-os-replace-open-handle``) is that a handle on a file the code
    later deletes is closed first.

    The leaked reader is not observable from POSIX -- no descriptor stays open,
    the difference is in how Windows treats a reader the collector has not yet
    reached -- so what can be pinned on every OS is the discipline that keeps
    the invariant: the scratch file is read through a ``ParquetFile`` that is
    closed before the stream is written, and never through ``pq.read_table``,
    whose reader is closed whenever it is collected. In-process on purpose: it
    is about handle discipline, not the registration.
    """
    import pyarrow.parquet as pq
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli
    from geoparquet_io.core import streaming

    events: list[tuple[str, str]] = []
    real_parquet_file = pq.ParquetFile
    real_read_table = pq.read_table

    def recording_read_table(source, *args, **kwargs):
        events.append(("read_table", str(source)))
        return real_read_table(source, *args, **kwargs)

    monkeypatch.setattr(pq, "read_table", recording_read_table)

    class RecordingParquetFile(real_parquet_file):
        """Records which file each closed reader was on: the conversion itself
        opens and closes readers on the *input*, which are not the point."""

        def __init__(self, source, *args, **kwargs):
            self._recorded_source = str(source)
            super().__init__(source, *args, **kwargs)

        def close(self, *args, **kwargs):
            events.append(("closed", self._recorded_source))
            return super().close(*args, **kwargs)

    monkeypatch.setattr(pq, "ParquetFile", RecordingParquetFile)
    monkeypatch.setattr(
        streaming, "write_arrow_stream", lambda table: events.append(("streamed", ""))
    )

    result = CliRunner().invoke(cli, ["convert", "geoparquet", str(projected_conus), "-"])

    assert result.exit_code == 0, result.output
    scratch_read_table = [s for kind, s in events if kind == "read_table" and "gpio_convert_" in s]
    assert not scratch_read_table, (
        "the scratch file was read through pq.read_table, whose reader is closed "
        f"at the collector's leisure -- after the unlink, on Windows: {scratch_read_table}"
    )
    streamed_at = events.index(("streamed", ""))
    scratch_closed_at = [
        i
        for i, (kind, source) in enumerate(events)
        if kind == "closed" and "gpio_convert_" in source
    ]
    assert scratch_closed_at, f"the scratch file was never read through a closable reader: {events}"
    assert scratch_closed_at[0] < streamed_at, events


def test_read_stdin_to_temp_file_keeps_the_native_logical_type(stream_of):
    """The scratch Parquet file a piped-into command reads must stay native.

    ``read_stdin_to_temp_file`` is what every ``gpio <cmd> -`` runs first, and
    its output is the file the command then treats as the user's input -- the
    witness ``write_parquet_with_metadata`` reads the version and the CRS from
    (#993). A scratch file that has dropped the Parquet ``GEOMETRY`` logical
    type is a witness that has forgotten half of what it is asked.
    """
    driver = (
        "import json;"
        "import pyarrow.parquet as pq;"
        "from geoparquet_io.core.streaming import read_stdin_to_temp_file;"
        "schema = pq.ParquetFile(read_stdin_to_temp_file()).schema;"
        "print(json.dumps({schema.column(i).name: "
        "json.loads(schema.column(i).logical_type.to_json()) for i in range(len(schema))}))"
    )
    with stream_of.open("rb") as handle:
        completed = subprocess.run(
            [sys.executable, "-c", driver],
            stdin=handle,
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT,
        )
    assert completed.returncode == 0, completed.stderr

    described = json.loads(completed.stdout)["geometry"]
    assert described.get("Type") == "Geometry", (
        f"geometry written to the scratch file as {described}, not a Parquet GEOMETRY"
    )
    assert json.loads(described["crs"])["id"] == EPSG_5070
