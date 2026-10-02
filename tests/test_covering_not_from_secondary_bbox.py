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


# ---------------------------------------------------------------------------
# #1171: the same mistake in the second detector, `check_bbox_structure`
# ---------------------------------------------------------------------------


def _check_bbox_structure(path):
    from geoparquet_io.core.bbox_structure import check_bbox_structure

    return check_bbox_structure(str(path))


def _secondary_bbox_file(tmp_path, name, **kwargs):
    path = tmp_path / f"{name}.parquet"
    create_multi_geometry_with_secondary_bbox(str(path), **kwargs)
    return path


class TestCheckBboxStructureAnswersForThePrimary:
    """`bbox_structure.check_bbox_structure` reports the PRIMARY's bbox or none (#1171).

    Its covering lookup took the first `covering.bbox` of ANY column and its
    name fallback matched any name ending in `bbox`, so a `boundary_bbox` — the
    SECONDARY `boundary` column's envelope — was handed to every caller as the
    primary's bbox column.
    """

    @pytest.mark.parametrize(
        ("case", "kwargs"),
        [
            ("undeclared", {}),
            ("declared-by-the-secondary", {"declare_boundary_covering": True}),
            (
                "declared-under-the-exact-name",
                {"declare_boundary_covering": True, "bbox_name": "bbox"},
            ),
        ],
    )
    def test_a_secondarys_bbox_is_not_the_primarys(self, case, kwargs, tmp_path):
        info = _check_bbox_structure(_secondary_bbox_file(tmp_path, case, **kwargs))

        assert info["bbox_column_name"] is None
        assert info["has_bbox_column"] is False
        assert info["has_bbox_metadata"] is False

    def test_the_primarys_own_covering_is_still_found(self, tmp_path):
        """Provenance on the primary wins, whatever the column is called."""
        path = _secondary_bbox_file(
            tmp_path,
            "primary_declares",
            declare_boundary_covering=True,
            primary_covering_column="geometry_bbox",
        )

        info = _check_bbox_structure(path)

        assert info["bbox_column_name"] == "geometry_bbox"
        assert info["has_bbox_column"] is True
        assert info["has_bbox_metadata"] is True
        assert info["status"] == "optimal"


class TestEveryConsumerGetsThePrimarysAnswer:
    """`check spatial` and `inspect` read the primary's bbox column, not any name.

    `duckdb_metadata.has_bbox_column` matched any column whose name *ends* in
    `bbox`/`bounds`/`extent`, so these consumers still took a SECONDARY
    geometry's `boundary_bbox` as the primary's: `check spatial` measured row
    group overlap on the Polygon column's extents while `check bbox` reported
    no bbox column at all, and `inspect meta` printed those extents as the
    primary's bounds (#1171).
    """

    def test_check_spatial_refuses_what_check_bbox_does_not_see(self, tmp_path):
        from geoparquet_io.core.check_spatial_order import check_spatial_order_bbox_stats

        path = _secondary_bbox_file(tmp_path, "in")
        assert _check_bbox_structure(path)["bbox_column_name"] is None

        with pytest.raises(ValueError, match="does not have a bbox column"):
            check_spatial_order_bbox_stats(str(path))

    def test_pushdown_readiness_reports_no_bbox_column(self, tmp_path):
        from geoparquet_io.core.check_spatial_order import check_spatial_pushdown_readiness

        result = check_spatial_pushdown_readiness(str(_secondary_bbox_file(tmp_path, "in")))

        assert result["has_geo_bbox"] is False

    def test_row_group_bounds_are_not_the_secondarys_extents(self, tmp_path):
        """The boundary polygons reach (-0.5, -0.5)-(2.5, 2.5); the points do not."""
        from geoparquet_io.core.metadata_utils import extract_bbox_from_row_group_stats

        path = _secondary_bbox_file(tmp_path, "in")

        assert extract_bbox_from_row_group_stats(str(path), "geometry") is None

    def test_the_primarys_own_covering_is_still_read(self, tmp_path):
        """The positive half: declared provenance gives the POINTS' extents."""
        from geoparquet_io.core.metadata_utils import extract_bbox_from_row_group_stats

        path = _secondary_bbox_file(
            tmp_path,
            "primary_declares",
            declare_boundary_covering=True,
            primary_covering_column="geometry_bbox",
        )

        assert extract_bbox_from_row_group_stats(str(path), "geometry") == [0.0, 0.0, 2.0, 2.0]


class TestAMalformedPrimaryColumnIsNotALookupKey:
    """`primary_column` can be any JSON value in someone else's file (#947/#1171).

    The primary-only lookup indexes `columns` by it, so a list or an object
    raised `unhashable type: 'list'` where the truthful answer is that the block
    names no primary entry and so declares no covering.
    """

    BLOCKS = [["geometry"], {"name": "geometry"}, 42, None]
    IDS = ["list", "object", "number", "null"]

    def _file(self, tmp_path, primary, name):
        geo = {
            "version": "1.1.0",
            "primary_column": primary,
            "columns": {"geometry": {"encoding": "WKB"}},
        }
        table = pa.table(
            {
                "geometry": pa.array([b"\x00"], type=pa.binary()),
                "bbox": pa.array(
                    [{"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}], BBOX_STRUCT
                ),
            }
        ).replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")})
        path = tmp_path / f"{name}.parquet"
        pq.write_table(table, str(path))
        return path, geo

    @pytest.mark.parametrize("primary", BLOCKS, ids=IDS)
    def test_the_file_detector_reports_the_conventional_column(self, primary, tmp_path):
        path, _ = self._file(tmp_path, primary, "malformed_primary")

        info = _check_bbox_structure(path)

        assert info["bbox_column_name"] == "bbox"
        assert info["has_bbox_metadata"] is False

    @pytest.mark.parametrize("primary", BLOCKS, ids=IDS)
    def test_the_write_side_gate_agrees(self, primary, tmp_path):
        _, geo = self._file(tmp_path, primary, "malformed_primary")
        schema = pa.schema([pa.field("geometry", pa.binary()), pa.field("bbox", BBOX_STRUCT)])

        assert bbox_column_to_declare(schema, geo) == "bbox"


class TestGoodFilesKeepTheirBboxAdvice:
    """The tightening must not report "no bbox column" on a sound file (#1171)."""

    def test_a_covering_the_primary_declares_stays_optimal(self, austria_bbox_covering_file):
        """A non-conventional name the primary itself declares is the whole point of #738."""
        info = _check_bbox_structure(austria_bbox_covering_file)

        assert info["bbox_column_name"] == "geometry_bbox"
        assert info["has_bbox_metadata"] is True
        assert info["status"] == "optimal"

    def test_an_undeclared_exact_bbox_is_suboptimal_not_absent(self, places_test_file):
        """The 1.0 -> 1.1 upgrade path: the conventional column is still found."""
        info = _check_bbox_structure(places_test_file)

        assert info["bbox_column_name"] == "bbox"
        assert info["has_bbox_metadata"] is False
        assert info["status"] == "suboptimal"

    def test_check_bbox_advice_is_unchanged_for_a_sound_file(self, austria_bbox_covering_file):
        """`gpio check bbox` still names the column and reports it as optimal."""
        result = CliRunner().invoke(cli, ["check", "bbox", austria_bbox_covering_file])

        assert result.exit_code == 0, result.output
        assert "geometry_bbox" in result.output


class TestConvertDoesNotCrossColumns:
    """`gpio convert geoparquet` of the #1171 shape (the #953 output, second route)."""

    def test_the_primary_does_not_get_the_secondarys_bbox(self, tmp_path):
        input_file = _secondary_bbox_file(tmp_path, "in")
        output_file = tmp_path / "out.parquet"

        result = CliRunner().invoke(
            cli,
            [
                "convert",
                "geoparquet",
                str(input_file),
                str(output_file),
                "--geoparquet-version",
                "1.1",
            ],
        )
        assert result.exit_code == 0, result.output

        geo = _geo(output_file)
        primary_meta = geo["columns"][geo["primary_column"]]
        assert "boundary_bbox" not in _covering_columns(primary_meta), primary_meta

    def test_geoarrow_does_not_drop_the_secondarys_declared_bbox(self, tmp_path):
        """1.1-geoarrow drops "the" bbox column; the secondary's is not it."""
        input_file = _secondary_bbox_file(tmp_path, "in", declare_boundary_covering=True)
        output_file = tmp_path / "out.parquet"

        result = CliRunner().invoke(
            cli,
            [
                "convert",
                "geoparquet",
                str(input_file),
                str(output_file),
                "--geoparquet-version",
                "1.1-geoarrow",
            ],
        )
        assert result.exit_code == 0, result.output

        columns = pq.ParquetFile(str(output_file)).schema_arrow.names
        assert "boundary_bbox" in columns, columns


class TestAddBboxMetadataDoesNotCrossColumns:
    """`gpio add bbox-metadata` declared the SECONDARY's struct on the primary (#1171)."""

    def test_it_refuses_rather_than_declare_a_secondarys_bbox(self, tmp_path):
        input_file = _secondary_bbox_file(tmp_path, "in")

        result = CliRunner().invoke(cli, ["add", "bbox-metadata", str(input_file)])

        assert result.exit_code != 0, result.output
        assert "boundary_bbox" not in result.output
        assert "No valid bbox column" in result.output
        assert _covering_columns(_geo(input_file)["columns"]["geometry"]) == set()

    def test_it_still_declares_an_undeclared_exact_bbox(self, tmp_path):
        """The positive half: the conventional column on a sound file is declared."""
        target = _places_like(tmp_path)

        result = CliRunner().invoke(cli, ["add", "bbox-metadata", str(target)])

        assert result.exit_code == 0, result.output
        assert _covering_columns(_geo(target)["columns"]["geometry"]) == {"bbox"}

    # --- the escape hatch: --bbox-name asserts a column the policy will not ---

    def _add(self, target, *args):
        return CliRunner().invoke(cli, ["add", "bbox-metadata", str(target), *args])

    def test_bbox_name_declares_the_column_the_user_vouches_for(self, tmp_path):
        """The GDAL shape: a `geometry_bbox` struct no metadata declares."""
        target = _unclaimed_bbox_struct_file(tmp_path, "geometry_bbox")

        result = self._add(target, "--bbox-name", "geometry_bbox")

        assert result.exit_code == 0, result.output
        assert _covering_columns(_geo(target)["columns"]["geometry"]) == {"geometry_bbox"}

    def test_bbox_name_cannot_claim_a_secondarys_declared_bbox(self, tmp_path):
        """The flag is an escape hatch from the naming policy, not from #1171."""
        target = _secondary_bbox_file(tmp_path, "in", declare_boundary_covering=True)

        result = self._add(target, "--bbox-name", "boundary_bbox")

        assert result.exit_code != 0, result.output
        assert "boundary_bbox" in result.output
        assert _covering_columns(_geo(target)["columns"]["geometry"]) == set()

    def test_bbox_name_over_an_overture_order_struct_is_refused(self, tmp_path):
        """`xmin, xmax, ymin, ymax` is a struct no 1.1 covering may point at."""
        target = _unclaimed_bbox_struct_file(
            tmp_path, "bounds", field_order=("xmin", "xmax", "ymin", "ymax")
        )

        result = self._add(target, "--bbox-name", "bounds")

        assert result.exit_code != 0, result.output
        assert "Cannot add bbox covering metadata" in result.output
        assert _covering_columns(_geo(target)["columns"]["geometry"]) == set()

    def test_bbox_name_for_a_column_that_is_not_there_is_refused(self, tmp_path):
        target = _unclaimed_bbox_struct_file(tmp_path, "geometry_bbox")

        result = self._add(target, "--bbox-name", "nosuch")

        assert result.exit_code != 0, result.output
        assert "nosuch" in result.output
        assert _covering_columns(_geo(target)["columns"]["geometry"]) == set()


def _unclaimed_bbox_struct_file(tmp_path, column, field_order=("xmin", "ymin", "xmax", "ymax")):
    """A 1.1 file whose only bbox struct is called ``column`` and is undeclared.

    What OGR and friends write: the struct is named after the geometry column
    (``geometry_bbox``) or something descriptive (``bounds``), which the
    #738/#1171 policy will not vouch for on its own. ``--bbox-name`` is how the
    user vouches for it.
    """
    import struct

    bbox_type = pa.struct([(axis, pa.float64()) for axis in field_order])
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
    }
    table = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "geometry": pa.array([struct.pack("<BIdd", 1, 1, 1.0, 1.0)], type=pa.binary()),
            column: pa.array([dict.fromkeys(field_order, 1.0)], type=bbox_type),
        }
    )
    path = tmp_path / f"{column}_{'_'.join(field_order)}.parquet"
    pq.write_table(
        table.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")}), str(path)
    )
    return path


class TestExtractBboxFiltersOnThePrimary:
    """`gpio extract geoparquet --bbox` pre-filtered on the SECONDARY's extents (#1171)."""

    def _extract(self, input_file, output_file, bbox):
        result = CliRunner().invoke(
            cli,
            ["extract", "geoparquet", str(input_file), str(output_file), "--bbox", bbox],
        )
        assert result.exit_code == 0, result.output
        return pq.read_table(str(output_file))

    def test_a_box_that_holds_no_point_returns_no_rows(self, tmp_path):
        """1.6,1.6,1.7,1.7 falls inside the polygon around POINT(2 2), not on the point."""
        input_file = _secondary_bbox_file(tmp_path, "in")

        table = self._extract(input_file, tmp_path / "out.parquet", "1.6,1.6,1.7,1.7")

        assert table.num_rows == 0, table.to_pydict()

    def test_a_box_that_holds_a_point_still_returns_it(self, tmp_path):
        input_file = _secondary_bbox_file(tmp_path, "in")

        table = self._extract(input_file, tmp_path / "out.parquet", "1.9,1.9,2.1,2.1")

        assert table.num_rows == 1
        assert table.column("id").to_pylist() == [3]


class TestAnIncompleteCoveringIsNotAPointer:
    """The primary's own ``covering.bbox`` must name all four axes with
    well-formed ``[column, field]`` paths. ``_covering_column`` accepts the
    first list-shaped ref it sees, so these reach the stricter check above it."""

    @staticmethod
    def _block(bbox_refs):
        return {
            "primary_column": "geometry",
            "columns": {"geometry": {"covering": {"bbox": bbox_refs}}},
        }

    def test_only_some_axes_is_not_a_pointer(self):
        from geoparquet_io.core.bbox_structure import _bbox_column_from_covering

        partial = {"xmin": ["my_box", "xmin"], "ymin": ["my_box", "ymin"]}
        assert _bbox_column_from_covering(self._block(partial)) is None

    def test_one_malformed_axis_spoils_it(self):
        from geoparquet_io.core.bbox_structure import _bbox_column_from_covering

        refs = {k: ["my_box", k] for k in ("xmin", "ymin", "xmax")}
        refs["ymax"] = "my_box.ymax"  # a string, not a [column, field] path
        assert _bbox_column_from_covering(self._block(refs)) is None

    def test_all_four_well_formed_is_a_pointer(self):
        from geoparquet_io.core.bbox_structure import _bbox_column_from_covering

        refs = {k: ["my_box", k] for k in ("xmin", "ymin", "xmax", "ymax")}
        assert _bbox_column_from_covering(self._block(refs)) == "my_box"


class TestTheClaimedNameIsExplainedWhenAsked:
    """``--verbose`` says why the conventional name was not read as the
    primary's bbox, so a user whose `bbox` struct is another column's covering
    can tell that from a file that simply has none."""

    @staticmethod
    def _schema_info_of_a_file_with_a_bbox_struct(tmp_path):
        """Real `parquet_schema()` output, so the struct walk is the real one."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from geoparquet_io.core.duckdb_metadata import get_schema_info

        bbox_type = pa.struct([(axis, pa.float64()) for axis in ("xmin", "ymin", "xmax", "ymax")])
        table = pa.table(
            {
                "geometry": pa.array([None], type=pa.binary()),
                "bbox": pa.array([{"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}], bbox_type),
            }
        )
        path = tmp_path / "with_bbox_struct.parquet"
        pq.write_table(table, str(path))
        return get_schema_info(str(path))

    @staticmethod
    def _refs(column):
        return {axis: [column, axis] for axis in ("xmin", "ymin", "xmax", "ymax")}

    def test_it_says_which_covering_claimed_the_name(self, tmp_path, caplog):
        import logging

        from geoparquet_io.core.bbox_structure import _find_bbox_column_in_schema

        schema_info = self._schema_info_of_a_file_with_a_bbox_struct(tmp_path)
        claimed = {
            "primary_column": "geometry",
            "columns": {
                "geometry": {},
                "boundary": {"covering": {"bbox": self._refs("bbox")}},
            },
        }
        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            found = _find_bbox_column_in_schema(schema_info, True, claimed)

        assert found is None
        assert "another column's covering names it" in caplog.text

    def test_an_unclaimed_conventional_name_is_still_read(self, tmp_path):
        from geoparquet_io.core.bbox_structure import _find_bbox_column_in_schema

        schema_info = self._schema_info_of_a_file_with_a_bbox_struct(tmp_path)
        unclaimed = {"primary_column": "geometry", "columns": {"geometry": {}}}
        assert _find_bbox_column_in_schema(schema_info, False, unclaimed) == "bbox"
