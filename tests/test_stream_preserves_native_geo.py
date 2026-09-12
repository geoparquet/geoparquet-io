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

**Every test here runs in a subprocess, and has to.** This module's own imports
register the extension types process-wide, so an in-process test of a
process-global registration is green whatever the code under test does -- which
is exactly how #993's first version shipped with the bug in it.

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
