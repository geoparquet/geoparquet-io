"""The primary geometry's ``covering`` never names another column's bbox struct.

Regression tests for #953: a multi-geometry file whose SECONDARY column
``boundary`` travels with a ``boundary_bbox`` struct came out of the in-memory
and streaming write strategies with that struct declared as the PRIMARY
column's ``covering.bbox`` — a Point column advertising a Polygon column's
extents, so a reader pruning row groups for the primary consults the wrong
column entirely.

The gate is :func:`geoparquet_io.core.geo_metadata.bbox_column_to_declare`.
Its no-provenance fallback name-matched ANY conventional bbox name
(``bounds``, ``extent``, ``*_bbox``), contradicting the #738 policy the module
itself documents on ``SELF_EVIDENT_BBOX_COLUMN``: only the exact name ``bbox``
is self-evidently the *primary geometry's* envelope; every broader name
requires explicit provenance (a declared ``covering`` on the primary's own
entry). gpio's own writers never emit ``{primary}_bbox`` — ``add bbox``
defaults to ``bbox`` and declares any ``--bbox-name`` explicitly through
``custom_metadata`` — so no gpio-written file loses its covering to this
tightening.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.geo_metadata import bbox_column_to_declare
from tests.fixtures.multi_geometry import create_multi_geometry_with_secondary_bbox

BBOX_STRUCT = pa.struct(
    [
        ("xmin", pa.float64()),
        ("ymin", pa.float64()),
        ("xmax", pa.float64()),
        ("ymax", pa.float64()),
    ]
)

#: Every write strategy `gpio extract geoparquet` exposes, plus the default.
STRATEGIES = [None, "in-memory", "streaming", "disk-rewrite"]


def _geo(path) -> dict:
    kv = pq.ParquetFile(str(path)).metadata.metadata or {}
    assert b"geo" in kv, f"{path} has no geo key"
    return json.loads(kv[b"geo"].decode("utf-8"))


def _rewrite(input_file, output_file, strategy):
    args = ["extract", "geoparquet", str(input_file), str(output_file)]
    if strategy is not None:
        args += ["--write-strategy", strategy]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output


def _covering_columns(col_meta) -> set[str]:
    """Every column a ``covering.bbox`` entry references."""
    bbox = (col_meta.get("covering") or {}).get("bbox") or {}
    return {ref[0] for ref in bbox.values() if isinstance(ref, list) and ref}


class TestBboxColumnToDeclare:
    """Unit contract of the one gate, `bbox_column_to_declare` (#953/#738)."""

    def _schema(self, *extra_fields) -> pa.Schema:
        return pa.schema(
            [
                pa.field("geometry", pa.binary()),
                pa.field("boundary", pa.binary()),
                *extra_fields,
            ]
        )

    def _geo_meta(self, primary_covering=None, boundary_covering=None) -> dict:
        geometry = {"encoding": "WKB"}
        boundary = {"encoding": "WKB"}
        if primary_covering:
            geometry["covering"] = primary_covering
        if boundary_covering:
            boundary["covering"] = boundary_covering
        return {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": geometry, "boundary": boundary},
        }

    def test_a_secondary_named_bbox_struct_is_not_declared(self):
        """`boundary_bbox` with no provenance is another column's envelope."""
        schema = self._schema(pa.field("boundary_bbox", BBOX_STRUCT))
        assert bbox_column_to_declare(schema, self._geo_meta()) is None

    def test_the_exact_conventional_name_is_still_declared(self):
        schema = self._schema(pa.field("bbox", BBOX_STRUCT))
        assert bbox_column_to_declare(schema, self._geo_meta()) == "bbox"

    def test_the_exact_name_wins_beside_a_suffixed_one(self):
        schema = self._schema(pa.field("bbox", BBOX_STRUCT), pa.field("boundary_bbox", BBOX_STRUCT))
        assert bbox_column_to_declare(schema, self._geo_meta()) == "bbox"

    def test_broad_conventional_names_require_provenance(self):
        """`bounds`/`extent` matching is read-side only; declaring needs provenance (#738)."""
        for name in ("bounds", "extent"):
            schema = self._schema(pa.field(name, BBOX_STRUCT))
            assert bbox_column_to_declare(schema, self._geo_meta()) is None, name

    def test_provenance_on_the_primary_is_honoured_whatever_the_name(self):
        """A covering the PRIMARY's own entry declares is carried, name and all."""
        covering = {"bbox": {ax: ["boundary_bbox", ax] for ax in ("xmin", "ymin", "xmax", "ymax")}}
        schema = self._schema(pa.field("boundary_bbox", BBOX_STRUCT))
        assert (
            bbox_column_to_declare(schema, self._geo_meta(primary_covering=covering))
            == "boundary_bbox"
        )

    def test_provenance_on_the_secondary_is_not_the_primarys(self):
        """The SECONDARY's declared covering must not leak onto the primary."""
        covering = {"bbox": {ax: ["boundary_bbox", ax] for ax in ("xmin", "ymin", "xmax", "ymax")}}
        schema = self._schema(pa.field("boundary_bbox", BBOX_STRUCT))
        assert bbox_column_to_declare(schema, self._geo_meta(boundary_covering=covering)) is None

    def test_no_geo_metadata_still_declares_only_the_exact_name(self):
        schema = self._schema(pa.field("boundary_bbox", BBOX_STRUCT))
        assert bbox_column_to_declare(schema, None) is None
        schema = self._schema(pa.field("bbox", BBOX_STRUCT))
        assert bbox_column_to_declare(schema, None) == "bbox"

    def test_a_non_struct_bbox_column_is_not_declared(self):
        schema = self._schema(pa.field("bbox", pa.string()))
        assert bbox_column_to_declare(schema, self._geo_meta()) is None


@pytest.mark.parametrize("strategy", STRATEGIES)
class TestRewriteNeverCrossesColumns:
    """End-to-end (#953): every strategy, same rule."""

    def test_primary_covering_never_names_the_secondarys_bbox(self, strategy, tmp_path):
        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        create_multi_geometry_with_secondary_bbox(str(input_file))

        _rewrite(input_file, output_file, strategy)

        geo = _geo(output_file)
        primary_meta = geo["columns"][geo["primary_column"]]
        assert "boundary_bbox" not in _covering_columns(primary_meta), primary_meta["covering"]

    def test_secondarys_own_declared_covering_survives_on_its_own_entry(self, strategy, tmp_path):
        """The input's provenance for the SECONDARY is carried where it belongs."""
        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        create_multi_geometry_with_secondary_bbox(str(input_file), declare_boundary_covering=True)

        _rewrite(input_file, output_file, strategy)

        geo = _geo(output_file)
        assert _covering_columns(geo["columns"]["boundary"]) == {"boundary_bbox"}
        # And even with that provenance in the file, it is still not the primary's.
        primary_meta = geo["columns"][geo["primary_column"]]
        assert "boundary_bbox" not in _covering_columns(primary_meta)
