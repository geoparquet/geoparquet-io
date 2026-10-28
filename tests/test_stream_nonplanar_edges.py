"""IPC must carry the geometry model before the next command decides to repair."""

from __future__ import annotations

import json
import struct
import subprocess
import sys

import geoarrow.pyarrow as ga
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest

from tests.native_geo_probes import write_native_geo_only

_CAP_POINTS = ((0, 80), (90, 80), (180, 80), (-90, 80), (0, 80))
_CAP = struct.pack("<BIII", 1, 3, 1, len(_CAP_POINTS)) + b"".join(
    struct.pack("<dd", *point) for point in _CAP_POINTS
)
_POINT = struct.pack("<BIdd", 1, 1, 10, 20)


def _extract(*args, payload=None):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from geoparquet_io.cli.main import main; main()",
            "extract",
            "geoparquet",
            *(str(arg) for arg in args),
        ],
        input=payload,
        capture_output=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", "replace")
    return result


def _read_stream(payload):
    return ipc.open_stream(pa.BufferReader(payload)).read_all()


def _storage(column):
    return [value for chunk in column.chunks for value in chunk.storage.to_pylist()]


@pytest.mark.parametrize("geometry_column", ["geometry", "footprint"])
def test_native_geography_survives_two_extract_hops(tmp_path, geometry_column):
    source = tmp_path / "source.parquet"
    destination = tmp_path / "result.parquet"
    geo_type = ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL).with_crs("EPSG:4269")
    write_native_geo_only(source, [(1, "cap", _CAP)], {geometry_column: (2, geo_type)})

    first = _extract(source, "-")
    streamed = _read_stream(first.stdout)
    field = streamed.schema.field(geometry_column)
    assert field.type.edge_type == ga.EdgeType.SPHERICAL
    assert field.type.crs.to_json_dict()["id"] == {"authority": "EPSG", "code": 4269}
    assert _storage(streamed.column(geometry_column)) == [_CAP]

    second = _extract("-", destination, payload=first.stdout)
    with pq.ParquetFile(destination) as reader:
        output = reader.read()
        assert "Geography" in str(reader.schema.column(2).logical_type)
    assert output.schema.field(geometry_column).type.edge_type == ga.EdgeType.SPHERICAL
    assert _storage(output.column(geometry_column)) == [_CAP]
    assert b"Repaired" not in second.stderr


@pytest.mark.parametrize("edges", ["planar", "spherical", "karney"])
def test_geo_block_controls_the_stream_field(tmp_path, edges):
    source = tmp_path / "source.parquet"
    native_type = ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL)
    write_native_geo_only(source, [(1, "point", _POINT)], {"geometry": (2, native_type)})
    geo = {
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "edges": edges}},
    }
    from geoparquet_io.core.parquet_footer import patch_footer_kv

    patch_footer_kv(source, {"geo": json.dumps(geo)})

    output = _read_stream(_extract(source, "-").stdout)
    assert output.schema.field("geometry").type.edge_type.name.lower() == edges


def test_stdin_type_without_geo_block_skips_planar_repair(tmp_path):
    geo_type = ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL)
    geometry = pa.ExtensionArray.from_storage(geo_type, pa.array([_CAP], type=pa.binary()))
    table = pa.table({"geometry": geometry})
    buffer = pa.BufferOutputStream()
    with ipc.new_stream(buffer, table.schema) as writer:
        writer.write_table(table)

    result = _extract("-", "-", payload=buffer.getvalue().to_pybytes())
    output = _read_stream(result.stdout)
    assert _storage(output.column("geometry")) == [_CAP]
    assert output.schema.field("geometry").type.edge_type == ga.EdgeType.SPHERICAL
    assert b"Repaired" not in result.stderr


def test_secondary_geography_keeps_its_own_crs_and_edges(tmp_path):
    source = tmp_path / "source.parquet"
    primary_type = ga.wkb().with_crs("EPSG:3857")
    secondary_type = ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL).with_crs("EPSG:4269")
    write_native_geo_only(
        source,
        [(1, "mixed", _POINT, _CAP)],
        {"geometry": (2, primary_type), "boundary": (3, secondary_type)},
    )

    first = _extract(source, "-")
    table = _read_stream(first.stdout)
    secondary = table.schema.field("boundary").type
    assert secondary.edge_type == ga.EdgeType.SPHERICAL
    assert secondary.crs.to_json_dict()["id"] == {"authority": "EPSG", "code": 4269}
    assert _storage(table.column("boundary")) == [_CAP]

    second = _read_stream(_extract("-", "-", payload=first.stdout).stdout)
    assert second.schema.field("boundary").type == secondary
    assert _storage(second.column("boundary")) == [_CAP]


def test_opening_native_file_for_file_output_keeps_its_format_metadata(tmp_path):
    from geoparquet_io.core.stream_io import open_input

    path = tmp_path / "native_only.parquet"
    write_native_geo_only(path, [(1, "point", _POINT)], {"geometry": (2, ga.wkb())})
    with pq.ParquetFile(path) as reader:
        original = reader.metadata.metadata
    with open_input(str(path)) as (_, metadata, _, _):
        assert metadata == original


def test_explicit_null_crs_survives_in_both_ipc_carriers():
    field_type = ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL).with_crs("EPSG:4269")
    array = pa.ExtensionArray.from_storage(field_type, pa.array([_CAP], type=pa.binary()))
    geo = {
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon"], "crs": None}},
    }
    table = pa.table({"geometry": array}).replace_schema_metadata(
        {b"geo": json.dumps(geo).encode()}
    )
    buffer = pa.BufferOutputStream()
    with ipc.new_stream(buffer, table.schema) as writer:
        writer.write_table(table)
    output = _read_stream(_extract("-", "-", payload=buffer.getvalue().to_pybytes()).stdout)
    assert json.loads(output.schema.metadata[b"geo"])["columns"]["geometry"]["crs"] is None
    assert output.schema.field("geometry").type.crs is not None
    assert "srid:0" in output.schema.field("geometry").type.__arrow_ext_serialize__().decode()


def test_remote_schema_adapter_keeps_native_crs_and_edges(tmp_path, monkeypatch):
    from geoparquet_io.core import stream_io

    source = tmp_path / "source.parquet"
    geo_type = ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL).with_crs("EPSG:4269")
    write_native_geo_only(source, [(1, "cap", _CAP)], {"geometry": (2, geo_type)})
    with pq.ParquetFile(source) as reader:
        metadata = reader.metadata.metadata
    # Exercise the remote schema adapter without network access or credentials.
    monkeypatch.setattr(stream_io, "get_parquet_metadata", lambda *a, **kw: (metadata, object()))
    with stream_io.open_input(str(source), stream_output=True) as (_, described, _, _):
        geometry = json.loads(described[b"geo"])["columns"]["geometry"]
    assert geometry["edges"] == "spherical"
    assert geometry["crs"]["id"] == {"authority": "EPSG", "code": 4269}


def test_empty_and_projected_streams_keep_only_surviving_geometry(tmp_path):
    source = tmp_path / "source.parquet"
    geo_type = ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL)
    write_native_geo_only(
        source,
        [(1, "cap", _CAP, _POINT)],
        {"geometry": (2, geo_type), "boundary": (3, ga.wkb())},
    )
    first = _extract(source, "-", "--exclude-cols", "boundary", "--where", "id < 0")
    output = _read_stream(first.stdout)
    assert output.num_rows == 0
    assert output.schema.field("geometry").type.edge_type == ga.EdgeType.SPHERICAL
    assert "boundary" not in output.column_names
    assert set(json.loads(output.schema.metadata[b"geo"])["columns"]) == {"geometry"}
    next_hop = _read_stream(_extract("-", "-", payload=first.stdout).stdout)
    assert next_hop.num_rows == 0
    assert next_hop.schema.field("geometry").type.edge_type == ga.EdgeType.SPHERICAL


@pytest.mark.parametrize(
    ("declared", "expected"), [("planar", "planar"), ("sphercal", "spherical"), (7, "spherical")]
)
def test_stdin_edge_authority_uses_only_supported_algorithms(declared, expected):
    from geoparquet_io.core.stream_io import _stream_input_metadata

    geo_type = ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL)
    array = pa.ExtensionArray.from_storage(geo_type, pa.array([_POINT], type=pa.binary()))
    geo = {
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["Point"], "edges": declared}
        },
    }
    table = pa.table({"geometry": array}).replace_schema_metadata(
        {b"geo": json.dumps(geo).encode()}
    )
    described = _stream_input_metadata(table)
    assert json.loads(described[b"geo"])["columns"]["geometry"]["edges"] == expected


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0"])
def test_planar_1x_pipe_keeps_legacy_auto_output(tmp_path, version):
    source = tmp_path / "source.parquet"
    destination = tmp_path / "output.parquet"
    geo = {
        "version": version,
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
    }
    table = pa.table({"geometry": [_POINT]}).replace_schema_metadata(
        {b"geo": json.dumps(geo).encode()}
    )
    pq.write_table(table, source)
    first = _extract(source, "-")
    _extract("-", destination, payload=first.stdout)
    with pq.ParquetFile(destination) as reader:
        # Auto mode has always normalized 1.0 inputs to 1.1 on the pipe path.
        assert json.loads(reader.metadata.metadata[b"geo"])["version"] == "1.1.0"
        assert json.loads(reader.schema.column(0).logical_type.to_json())["Type"] == "None"


def test_in_process_hops_keep_geometry_and_apply_an_explicit_unknown_crs(tmp_path, monkeypatch):
    from geoparquet_io.core import stream_io
    from geoparquet_io.core.extract import extract

    source = tmp_path / "source.parquet"
    geo_type = ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL).with_crs("EPSG:4269")
    write_native_geo_only(
        source,
        [(1, "cap", _CAP, _POINT)],
        {"footprint": (2, geo_type), "boundary": (3, ga.wkb())},
    )
    emitted = []
    monkeypatch.setattr(stream_io, "write_arrow_stream", emitted.append)
    extract(str(source), "-", exclude_cols="boundary")
    first = emitted.pop()
    assert first.schema.field("footprint").type.edge_type == ga.EdgeType.SPHERICAL
    assert _storage(first.column("footprint")) == [_CAP]
    geo = json.loads(first.schema.metadata[b"geo"])
    assert set(geo["columns"]) == {"footprint"}

    # A receiving command must honor an explicit unknown CRS even when the
    # sender's field still names a known one. Edge semantics stay spherical.
    geo["columns"]["footprint"]["crs"] = None
    incoming = first.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    monkeypatch.setattr(stream_io, "read_arrow_stream", lambda: incoming)
    extract("-", "-")
    output = emitted.pop()
    assert _storage(output.column("footprint")) == [_CAP]
    assert output.schema.field("footprint").type.edge_type == ga.EdgeType.SPHERICAL
    assert "srid:0" in output.schema.field("footprint").type.__arrow_ext_serialize__().decode()
    assert json.loads(output.schema.metadata[b"geo"])["columns"]["footprint"]["crs"] is None


def test_plain_input_does_not_acquire_a_geo_block():
    from geoparquet_io.core.stream_io import _stream_input_metadata

    table = pa.table({"count": [1]}).replace_schema_metadata({b"author": b"example"})
    assert _stream_input_metadata(table) == {b"author": b"example"}
