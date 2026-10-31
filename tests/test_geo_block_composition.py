"""The composition table: one ``geo`` block per input, whichever path writes it (#1367 concept 4).

``geo_metadata.compose_geo_block`` is the only composer of a ``geo`` block, so
for one input and one requested version every write path declares the same
block: ``convert geoparquet``, ``extract``, ``sort hilbert``, ``process
simplify`` (tolerance 0, so the data is unchanged), the Arrow IPC stream
(``extract ... -`` into ``convert geoparquet -``) and ``Table.write`` with each
of the four strategies. Each cell compares the decoded block (key order is not
part of the comparison) against ``convert geoparquet``'s.

Source kinds: a 1.1 file declaring a covering over its bbox struct and a
projected CRS; a 1.0 file with an undeclared conventional ``bbox`` column; a
1.1 file declaring ``edges: spherical``; a GeoJSON file (no carried block).
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from tests._defrag_fixtures import geo_block, grid_points_table, write_with_geo
from tests.native_geo_probes import projjson

N_ROWS = 400
VERSIONS = ("auto", "1.0", "1.1", "2.0")
STRATEGIES = ("duckdb-kv", "in-memory", "streaming", "disk-rewrite")
PATHS = (
    "convert_geoparquet",
    "sort_hilbert",
    "simplify",
    "stream",
    *(f"table_write_{s}" for s in STRATEGIES),
)
SOURCES = ("covered_projected", "bare_bbox_1_0", "spherical", "geojson")
REFERENCE = "extract"


def _write_source(kind: str, path: Path) -> Path:
    if kind == "covered_projected":
        covering = {"bbox": {a: ["bbox", a] for a in ("xmin", "ymin", "xmax", "ymax")}}
        geo = geo_block(crs=json.loads(projjson(32630)), covering=covering)
        return write_with_geo(path, grid_points_table(N_ROWS), geo)
    if kind == "bare_bbox_1_0":
        return write_with_geo(path, grid_points_table(N_ROWS), geo_block(version="1.0.0"))
    if kind == "spherical":
        geo = geo_block(edges="spherical")
        return write_with_geo(path, grid_points_table(N_ROWS, with_bbox=False), geo)
    out = path.with_suffix(".geojson")
    features = [
        {
            "type": "Feature",
            "properties": {"id": i},
            "geometry": {"type": "Point", "coordinates": [-10 + i * 0.01, 5 + i * 0.01]},
        }
        for i in range(N_ROWS)
    ]
    out.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
    return out


def _invoke(*args, input=None):
    result = CliRunner().invoke(cli, [str(a) for a in args], input=input)
    assert result.exit_code == 0, f"gpio {' '.join(map(str, args))}: {result.output[-1500:]}"
    return result


def _version_args(version: str) -> list[str]:
    return [] if version == "auto" else ["--geoparquet-version", version]


def _run(path_name: str, src: Path, out: Path, version: str) -> Path:
    v = _version_args(version)
    parquet_src = src
    if path_name == REFERENCE:
        _invoke("extract", "geoparquet", parquet_src, out, *v)
    elif path_name == "convert_geoparquet":
        _invoke("convert", "geoparquet", src, out, *v)
    elif path_name == "sort_hilbert":
        _invoke("sort", "hilbert", parquet_src, out, *v)
    elif path_name == "simplify":
        _invoke("process", "simplify", parquet_src, out, "--tolerance", "0", *v)
    elif path_name == "stream":
        # A stream becomes a file by persisting it as it is (``pq.write_table``),
        # so the block the stream carries is the block it writes. The stream
        # takes no version: its block is the input's, gated.
        streamed = _invoke("extract", "geoparquet", parquet_src, "-").stdout_bytes
        pq.write_table(pa.ipc.open_stream(streamed).read_all(), str(out))
    else:
        strategy = path_name.removeprefix("table_write_")
        gpio.read(str(parquet_src)).write(
            str(out),
            write_strategy=strategy,
            geoparquet_version=None if version == "auto" else version,
        )
    return out


def _geo(path: Path) -> dict | None:
    raw = (pq.ParquetFile(str(path)).metadata.metadata or {}).get(b"geo")
    return None if raw is None else json.loads(raw)


@pytest.fixture(scope="module")
def sources(tmp_path_factory) -> dict[str, Path]:
    root = tmp_path_factory.mktemp("composition_src")
    parquet = {}
    for kind in SOURCES:
        src = _write_source(kind, root / f"{kind}.parquet")
        if src.suffix != ".parquet":
            # Every path but convert reads GeoParquet: the GeoJSON source goes
            # through convert once (no carried block), then each path.
            converted = root / f"{kind}_converted.parquet"
            _invoke("convert", "geoparquet", src, converted)
            src = converted
        parquet[kind] = src
    return parquet


@pytest.fixture(scope="module")
def references(sources, tmp_path_factory) -> dict:
    root = tmp_path_factory.mktemp("composition_ref")
    return {
        (kind, version): _geo(_run(REFERENCE, src, root / f"{kind}_{version}.parquet", version))
        for kind, src in sources.items()
        for version in VERSIONS
    }


#: ``convert`` adds a bbox column when the input has none (a data change, so
#: a different block); it is compared only on sources that already carry one.
#: The stream takes no ``--geoparquet-version``: it carries the input's block.
SKIP = {
    **{("spherical", v, "convert_geoparquet"): "convert adds a bbox column" for v in VERSIONS},
    **{(k, v, "stream"): "the stream takes no version" for k in SOURCES for v in VERSIONS[1:]},
}

_STREAMING_BBOX = (
    "#1367: Table.write with the streaming strategy declares no bbox (the table "
    "entry point never measures one); kept byte-identical, recorded for the lead"
)
_SIMPLIFY_SPHERICAL_BBOX = (
    "#1367: process simplify drops the bbox of a spherical-edges column the other "
    "paths declare; kept byte-identical, recorded for the lead"
)
_STREAM_UPGRADE = (
    "#1367: the Arrow IPC stream carries the input block as it is, so a 1.0 input's "
    "conventional bbox column is not declared as the other paths declare it"
)
_CONVERT_V2 = (
    "#1367: convert --geoparquet-version 2.0 declares no covering over the bbox "
    "column the other paths declare (and re-resolves a projected CRS through DuckDB)"
)
#: (source, version, path) -> reason: every cell that disagrees with ``extract``
#: on the integration tip c4d9aeb2. Each is a disagreement between composers'
#: callers that is not on the approved-change list, so T3e keeps the bytes.
XFAIL: dict[tuple[str, str, str], str] = {
    **{
        (k, v, "table_write_streaming"): _STREAMING_BBOX
        for k in SOURCES
        if k != "spherical"
        for v in VERSIONS
    },
    **{("spherical", v, "simplify"): _SIMPLIFY_SPHERICAL_BBOX for v in ("auto", "1.0", "1.1")},
    ("bare_bbox_1_0", "auto", "stream"): _STREAM_UPGRADE,
    **{
        (k, "2.0", "convert_geoparquet"): _CONVERT_V2
        for k in ("covered_projected", "bare_bbox_1_0", "geojson")
    },
}


def _cases():
    for kind in SOURCES:
        for version in VERSIONS:
            for path_name in PATHS:
                if (kind, version, path_name) in SKIP:
                    continue
                marks = []
                reason = XFAIL.get((kind, version, path_name))
                if reason:
                    marks.append(pytest.mark.xfail(strict=True, reason=reason))
                yield pytest.param(
                    kind, version, path_name, marks=marks, id=f"{kind}-{version}-{path_name}"
                )


@pytest.mark.parametrize(("kind", "version", "path_name"), list(_cases()))
def test_every_path_declares_the_same_block(
    kind, version, path_name, sources, references, tmp_path
):
    out = _run(path_name, sources[kind], tmp_path / "out.parquet", version)
    got, want = _geo(out), references[(kind, version)]
    assert got == want, _diff(got, want)


def _diff(got, want, prefix="") -> str:
    """The differing leaves, one per line (``crs`` bodies abbreviated to their id)."""
    if isinstance(got, dict) and isinstance(want, dict):
        lines = []
        for key in sorted(set(got) | set(want), key=str):
            lines.append(
                _diff(got.get(key, "<absent>"), want.get(key, "<absent>"), f"{prefix}.{key}")
            )
        return "\n".join(line for line in lines if line)
    if got == want:
        return ""
    short = lambda v: v.get("id", "projjson") if isinstance(v, dict) and "type" in v else v  # noqa: E731
    return f"{prefix}: {short(got)!r} != {short(want)!r}"
