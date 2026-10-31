"""The composition table: one ``geo`` block per input, whichever path writes it (#1367 concept 4).

``geo_metadata.compose_geo_block`` is the only composer of a ``geo`` block, so
for one input and one requested version every write path declares the same
block: ``convert geoparquet``, ``extract``, ``sort hilbert``, ``process
simplify`` (tolerance 0, so the data is unchanged), the Arrow IPC stream
(``extract ... -`` persisted with ``pq.write_table``) and ``Table.write`` with
each of the four strategies. Each cell compares the decoded block (key order is
not part of the comparison) against ``extract``'s. On the integration tip
c4d9aeb2 nineteen cells disagreed (streaming ``Table.write`` declared no bbox,
streamed ``simplify`` dropped a spherical column's bbox, the stream kept a 1.0
input's block un-upgraded); T3e routed each to the composer's rule.

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

#: ``convert`` at 2.0 drops the input's bbox column ("not needed for native geo
#: types": a data decision, not the block's), and re-resolves the CRS from the
#: native type, so its PROJJSON is the same CRS spelled by another PROJ.
#: The block is compared on what the composer's rule says for those columns.
CONVERT_DROPS_BBOX = {(k, "2.0", "convert_geoparquet") for k in SOURCES}


def _cases():
    for kind in SOURCES:
        for version in VERSIONS:
            for path_name in PATHS:
                if (kind, version, path_name) in SKIP:
                    continue
                yield pytest.param(kind, version, path_name, id=f"{kind}-{version}-{path_name}")


@pytest.mark.parametrize(("kind", "version", "path_name"), list(_cases()))
def test_every_path_declares_the_same_block(
    kind, version, path_name, sources, references, tmp_path
):
    out = _run(path_name, sources[kind], tmp_path / "out.parquet", version)
    got, want = _geo(out), references[(kind, version)]
    if (kind, version, path_name) in CONVERT_DROPS_BBOX:
        got, want = _without_dropped_bbox(out, got, want)
    assert got == want, _diff(got, want)


def _without_dropped_bbox(out: Path, got: dict, want: dict) -> tuple[dict, dict]:
    """``(got, want)`` with what convert's 2.0 data decision changes taken out.

    The output has no bbox column, so the composer declares no covering over
    one; and the CRS is compared as the CRS it names, not its PROJJSON text.
    """
    from geoparquet_io.core.crs_utils import crs_authority_code

    assert "bbox" not in pq.read_schema(str(out)).names
    got, want = json.loads(json.dumps(got)), json.loads(json.dumps(want))
    for block in (got, want):
        entry = block["columns"][block["primary_column"]]
        entry.pop("covering", None)
        if "crs" in entry:
            entry["crs"] = crs_authority_code(entry["crs"])
    return got, want


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
