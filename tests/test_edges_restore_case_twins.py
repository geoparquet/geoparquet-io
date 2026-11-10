"""Strip and restore of non-planar edges agree about case-twin geometry columns (audit of #1395).

``edges.strip_edges`` drops a declaration by the exact ``geo``-block key, so
``_edges_to_restore`` must hold back exactly those keys and no others. Matching
folded would stop ``GEOM``'s edges coming back when ``geom`` was reprojected.
"""

from geoparquet_io.core.edges import strip_edges
from geoparquet_io.core.write_funnels import _edges_to_restore

BLOCK = {
    "columns": {
        "geom": {"encoding": "WKB", "edges": "spherical"},
        "GEOM": {"encoding": "WKB", "edges": "spherical"},
    }
}


def _metadata():
    import json

    return {b"geo": json.dumps(BLOCK).encode()}


def test_restore_holds_back_exactly_what_strip_dropped():
    stripped = strip_edges(__import__("copy").deepcopy(BLOCK), {"geom"})
    still_declared = {k for k, v in stripped["columns"].items() if "edges" in v}
    assert still_declared == {"GEOM"}

    restored = _edges_to_restore(None, _metadata(), invalidated_columns={"geom"})
    assert restored == {"GEOM": "spherical"}


def test_restore_without_invalidated_columns_restores_every_declaration():
    assert _edges_to_restore(None, _metadata()) == {"geom": "spherical", "GEOM": "spherical"}
