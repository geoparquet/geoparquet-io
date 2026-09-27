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


@pytest.mark.parametrize("strategy", STRATEGIES)
class TestTheExactNameIsNotExempt:
    """#953 through the conventional name: `bbox` may be the SECONDARY's envelope."""

    def test_a_bbox_the_secondary_declares_is_not_the_primarys(self, strategy, tmp_path):
        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        create_multi_geometry_with_secondary_bbox(
            str(input_file), declare_boundary_covering=True, bbox_name="bbox"
        )

        _rewrite(input_file, output_file, strategy)

        geo = _geo(output_file)
        assert "bbox" not in _covering_columns(geo["columns"][geo["primary_column"]])
        assert _covering_columns(geo["columns"]["boundary"]) == {"bbox"}

    def test_an_undeclared_exact_bbox_is_still_declared(self, strategy, tmp_path):
        """The positive half, on every strategy (disk-rewrite included)."""
        source = pq.read_table(str(_places_like(tmp_path)))
        output_file = tmp_path / "out.parquet"

        _rewrite(_places_like(tmp_path), output_file, strategy)

        assert "bbox" in source.column_names
        geo = _geo(output_file)
        assert _covering_columns(geo["columns"][geo["primary_column"]]) == {"bbox"}


def _places_like(tmp_path):
    """A 1.1 copy of the places fixture: an exact `bbox` struct, no covering."""
    from pathlib import Path

    path = tmp_path / "places_v11.parquet"
    if not path.exists():
        source = Path(__file__).parent / "data" / "places_test.parquet"
        table = pq.read_table(str(source))
        geo = json.loads(table.schema.metadata[b"geo"])
        geo["version"] = "1.1.0"
        for col_meta in geo["columns"].values():
            col_meta.pop("covering", None)
        table = table.replace_schema_metadata(
            {**table.schema.metadata, b"geo": json.dumps(geo).encode("utf-8")}
        )
        pq.write_table(table, str(path))
    return path


def test_the_primarys_own_covering_survives_in_memory(tmp_path):
    """The in-memory gate reads the input's block, not the empty DuckDB table's.

    It used to see no provenance at all, so the secondary's `bbox` replaced the
    covering the primary itself declared.
    """
    input_file = tmp_path / "in.parquet"
    output_file = tmp_path / "out.parquet"
    create_multi_geometry_with_secondary_bbox(
        str(input_file),
        declare_boundary_covering=True,
        bbox_name="bbox",
        primary_covering_column="geometry_bbox",
    )

    _rewrite(input_file, output_file, "in-memory")

    geo = _geo(output_file)
    assert _covering_columns(geo["columns"]["geometry"]) == {"geometry_bbox"}


class TestComputedColumnsKeepTheirProvenance:
    """A bbox gpio computed under another name is declared through its provenance."""

    def test_add_bbox_custom_name_on_the_streaming_route(self, buildings_test_file, tmp_path):
        """1.1-geoarrow auto-routes to the streaming strategy, which dropped custom_metadata."""
        output = tmp_path / "out.parquet"
        result = CliRunner().invoke(
            cli,
            [
                "add",
                "bbox",
                buildings_test_file,
                str(output),
                "--bbox-name",
                "bounds",
                "--geoparquet-version",
                "1.1-geoarrow",
            ],
        )
        assert result.exit_code == 0, result.output

        geo = _geo(output)
        assert _covering_columns(geo["columns"][geo["primary_column"]]) == {"bounds"}

    def test_the_python_add_bbox_custom_name(self, buildings_test_file, tmp_path):
        """Table.add_bbox records the covering for the column it computed."""
        import geoparquet_io as gpio
        from geoparquet_io.core.add.bbox import add_bbox_table
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        output = tmp_path / "in_memory.parquet"
        gpio.read(buildings_test_file).add_bbox(column_name="bounds").write(
            str(output), write_strategy="in-memory"
        )
        geo = _geo(output)
        assert _covering_columns(geo["columns"][geo["primary_column"]]) == {"bounds"}

        table = add_bbox_table(pq.read_table(buildings_test_file), bbox_column_name="bounds")
        output = tmp_path / "table_funnel.parquet"
        write_geoparquet_table(table, str(output), geometry_column="geometry")
        geo = _geo(output)
        assert _covering_columns(geo["columns"][geo["primary_column"]]) == {"bounds"}


class TestGateDetails:
    def test_verbose_says_why_a_claimed_bbox_is_not_the_primarys(self, caplog):
        import logging

        schema = pa.schema(
            [
                pa.field("geometry", pa.binary()),
                pa.field("boundary", pa.binary()),
                pa.field("bbox", BBOX_STRUCT),
            ]
        )
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {"encoding": "WKB"},
                "boundary": {
                    "encoding": "WKB",
                    "covering": {
                        "bbox": {ax: ["bbox", ax] for ax in ("xmin", "ymin", "xmax", "ymax")}
                    },
                },
            },
        }
        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            assert bbox_column_to_declare(schema, geo, verbose=True) is None
            assert bbox_column_to_declare(schema, None, verbose=True) == "bbox"
        assert "another column's covering names it" in caplog.text
        assert "Found conventional bbox column" in caplog.text


class TestDeclareComputedBbox:
    """add_bbox_table records the covering only where there is a block to record it in."""

    @staticmethod
    def _geo_kv(columns):
        geo = {"version": "1.1.0", "primary_column": "geometry", "columns": columns}
        return {b"geo": json.dumps(geo).encode("utf-8"), b"other": b"kept"}

    def test_records_the_covering_and_keeps_index_entries(self):
        from geoparquet_io.core.add.bbox import _declare_computed_bbox

        h3 = {"column": "h3", "resolution": 9}
        kv = self._geo_kv({"geometry": {"encoding": "WKB", "covering": {"h3": h3}}})

        result = _declare_computed_bbox(kv, "geometry", "bounds")

        covering = json.loads(result[b"geo"])["columns"]["geometry"]["covering"]
        assert covering["h3"] == h3
        assert covering["bbox"]["xmin"] == ["bounds", "xmin"]
        assert result[b"other"] == b"kept"
        assert "covering" in json.loads(kv[b"geo"])["columns"]["geometry"]  # input untouched
        assert "bbox" not in json.loads(kv[b"geo"])["columns"]["geometry"]["covering"]

    @pytest.mark.parametrize(
        "kv",
        [
            None,
            {b"other": b"x"},
            {b"geo": b"{not json"},
            {b"geo": json.dumps({"version": "1.1.0", "columns": {}}).encode("utf-8")},
        ],
        ids=["no-metadata", "no-geo-key", "malformed", "no-entry-for-the-column"],
    )
    def test_leaves_metadata_it_cannot_record_into_alone(self, kv):
        from geoparquet_io.core.add.bbox import _declare_computed_bbox

        assert _declare_computed_bbox(kv, "geometry", "bounds") is kv
