"""Invariants the vectorized simplify path must not change (#1300).

``_simplify_values`` and ``_accumulate_stats`` used to walk every geometry in
Python -- to build ``idx``, to scatter the output WKB back into a list, to fold
the geometry-type set, and to invert the empty mask. On inputs with many small
geometries those loops, not the Rust kernel, dominated: ``coarsen._core`` was
7% of a 1M-building profile.

Replacing them is a pure refactor, so these tests characterize the behaviour
rather than the implementation. The oracle for the geometry itself is shapely
(the same oracle the #1195 benchmark used to confirm byte-identity across
1,091,914 geometries), and the rest pins the footer stats, the null and empty
handling, and the chunk shapes that the Python loops happened to flatten.

``tests/test_process_simplify.py`` already covers the single-chunk happy paths;
what is here is the edge of the fold -- empty and all-null columns, several
chunks, Z geometries, mixed types, and the interaction between ``drop_empty``
and nulls.
"""

from importlib.util import find_spec

import pyarrow as pa
import pytest

requires_coarsen = pytest.mark.skipif(
    find_spec("coarsen") is None, reason="requires the simplify extra (coarsen)"
)


def _shapely():
    return pytest.importorskip("shapely")


def _np():
    return pytest.importorskip("numpy")


def _geo_block(**extras):
    import json

    return {
        b"geo": json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB", "geometry_types": [], **extras}},
            }
        ).encode()
    }


def _table(wkb_values, chunks=1):
    """A WKB table whose geometry column has exactly ``chunks`` chunks."""
    schema = pa.schema(
        [pa.field("name", pa.string()), pa.field("geometry", pa.binary())],
        metadata=_geo_block(),
    )
    if chunks == 1:
        return pa.table(
            {
                "name": [f"f{i}" for i in range(len(wkb_values))],
                "geometry": pa.array(wkb_values, type=pa.binary()),
            },
            schema=schema,
        )
    size = max(1, -(-len(wkb_values) // chunks))
    slices = [wkb_values[i : i + size] for i in range(0, len(wkb_values), size)] or [[]]
    return pa.table(
        {
            "name": pa.chunked_array(
                [[f"f{i}" for i in range(len(s))] for s in slices], type=pa.string()
            ),
            "geometry": pa.chunked_array([pa.array(s, type=pa.binary()) for s in slices]),
        },
        schema=schema,
    )


def _jagged(x, y, n=12):
    """A polygon with ``n`` vertices along each edge, so simplification bites."""
    shapely = _shapely()
    pts = [(x + i / n, y + (0.02 if i % 2 else 0.0)) for i in range(n)]
    pts += [(x + 1, y + 1), (x, y + 1)]
    return shapely.Polygon(pts)


@requires_coarsen
class TestByteIdenticalToShapely:
    """The geometry itself: gpio's output WKB must equal shapely's, exactly."""

    def _assert_matches_shapely(self, geoms, tolerance=0.05):
        shapely = _shapely()
        from geoparquet_io.core.process.simplify import simplify_table

        wkb = [None if g is None else shapely.to_wkb(g) for g in geoms]
        out = simplify_table(_table(wkb), tolerance)
        got = out.column("geometry").to_pylist()

        assert len(got) == len(geoms)
        for i, g in enumerate(geoms):
            if g is None:
                assert got[i] is None, f"row {i}: a null geometry did not stay null"
                continue
            expected = shapely.to_wkb(shapely.simplify(g, tolerance, preserve_topology=True))
            assert got[i] == expected, f"row {i} diverged from shapely"

    def test_polygons(self):
        self._assert_matches_shapely([_jagged(x, 0) for x in range(8)])

    def test_mixed_geometry_types(self):
        shapely = _shapely()
        self._assert_matches_shapely(
            [
                shapely.Point(1, 2),
                shapely.LineString([(0, 0), (0.5, 0.01), (1, 0)]),
                _jagged(0, 0),
                shapely.MultiPoint([(0, 0), (1, 1)]),
                shapely.MultiLineString([[(0, 0), (0.5, 0.01), (1, 0)]]),
                shapely.MultiPolygon([_jagged(0, 0), _jagged(3, 3)]),
            ]
        )

    def test_nulls_interleaved_with_geometry(self):
        # The null positions are what `idx` exists for: the scatter back into
        # the output column has to land every value at its own row.
        geoms = [_jagged(x, 0) if x % 3 else None for x in range(10)]
        self._assert_matches_shapely(geoms)

    def test_leading_and_trailing_nulls(self):
        self._assert_matches_shapely([None, None, _jagged(0, 0), None])

    def test_all_null_column(self):
        self._assert_matches_shapely([None, None, None])

    def test_empty_table(self):
        self._assert_matches_shapely([])

    def test_a_single_row(self):
        self._assert_matches_shapely([_jagged(0, 0)])


@requires_coarsen
class TestFooterStats:
    """The ``geo`` block the streaming fold produces: the type set and the bbox.

    These go through ``simplify_file``, not ``simplify_table``: the in-memory
    path hands the stats to the write funnel, and ``_accumulate_stats`` -- the
    loop this change rewrites -- only runs while streaming. ``simplify_table``
    strips the stale stats and returns a block with neither key.
    """

    def _geo_of_file(self, tmp_path, wkb, tolerance=0.05, **kwargs):
        import json

        import pyarrow.parquet as pq

        from geoparquet_io.core.process.simplify import simplify_file
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        src = tmp_path / "in.parquet"
        out = tmp_path / "out.parquet"
        write_geoparquet_table(_table(wkb), str(src))
        simplify_file(str(src), str(out), tolerance, **kwargs)
        meta = pq.ParquetFile(str(out)).schema_arrow.metadata
        block = json.loads(meta[b"geo"])
        return block["columns"][block["primary_column"]], out

    def test_geometry_types_are_deduplicated(self, tmp_path):
        shapely = _shapely()

        geoms = [_jagged(x, 0) for x in range(40)] + [shapely.Point(0, 0)] * 40
        wkb = [shapely.to_wkb(g) for g in geoms]
        geo, _ = self._geo_of_file(tmp_path, wkb)
        assert sorted(geo["geometry_types"]) == ["Point", "Polygon"]

    def test_z_geometries_are_named_with_z(self, tmp_path):
        shapely = _shapely()

        z = shapely.Polygon([(0, 0, 1), (1, 0, 1), (1, 1, 1), (0, 0, 1)])
        wkb = [shapely.to_wkb(_jagged(0, 0)), shapely.to_wkb(z)]
        geo, _ = self._geo_of_file(tmp_path, wkb)
        assert sorted(geo["geometry_types"]) == ["Polygon", "Polygon Z"]

    def test_a_z_only_column_is_only_z(self, tmp_path):
        shapely = _shapely()

        z = shapely.Polygon([(0, 0, 1), (1, 0, 1), (1, 1, 1), (0, 0, 1)])
        geo, _ = self._geo_of_file(tmp_path, [shapely.to_wkb(z)] * 5)
        assert geo["geometry_types"] == ["Polygon Z"]

    def test_bbox_covers_every_geometry(self, tmp_path):
        shapely = _shapely()
        import pyarrow.parquet as pq

        wkb = [shapely.to_wkb(_jagged(x, x)) for x in range(6)]
        geo, out = self._geo_of_file(tmp_path, wkb)
        written = pq.read_table(str(out)).column("geometry").to_pylist()
        bounds = shapely.bounds(shapely.from_wkb(written))
        assert geo["bbox"] == pytest.approx(
            [
                float(bounds[:, 0].min()),
                float(bounds[:, 1].min()),
                float(bounds[:, 2].max()),
                float(bounds[:, 3].max()),
            ]
        )

    def test_nulls_do_not_reach_the_bbox(self, tmp_path):
        shapely = _shapely()

        wkb = [None, shapely.to_wkb(_jagged(10, 10)), None]
        geo, _ = self._geo_of_file(tmp_path, wkb)
        assert geo["bbox"][0] >= 10.0
        assert geo["geometry_types"] == ["Polygon"]

    def test_an_all_null_column_gets_no_bbox(self, tmp_path):
        geo, _ = self._geo_of_file(tmp_path, [None, None])
        assert "bbox" not in geo or geo["bbox"] is None
        assert geo["geometry_types"] == []


@requires_coarsen
class TestChunkedInput:
    """Several chunks must fold to the same answer as one."""

    @pytest.mark.parametrize("chunks", [1, 2, 3, 7])
    def test_chunk_count_does_not_change_the_output(self, chunks):
        shapely = _shapely()
        from geoparquet_io.core.process.simplify import simplify_table

        geoms = [_jagged(x, 0) if x % 4 else None for x in range(14)]
        wkb = [None if g is None else shapely.to_wkb(g) for g in geoms]
        out = simplify_table(_table(wkb, chunks=chunks), 0.05)
        reference = simplify_table(_table(wkb, chunks=1), 0.05)
        assert out.column("geometry").to_pylist() == reference.column("geometry").to_pylist()
        assert out.schema.metadata[b"geo"] == reference.schema.metadata[b"geo"]

    def test_a_chunk_of_only_nulls_in_the_middle(self):
        shapely = _shapely()
        from geoparquet_io.core.process.simplify import simplify_table

        schema = pa.schema(
            [pa.field("name", pa.string()), pa.field("geometry", pa.binary())],
            metadata=_geo_block(),
        )
        good = shapely.to_wkb(_jagged(0, 0))
        table = pa.table(
            {
                "name": pa.chunked_array([["a"], ["b", "c"], ["d"]], type=pa.string()),
                "geometry": pa.chunked_array(
                    [
                        pa.array([good], type=pa.binary()),
                        pa.array([None, None], type=pa.binary()),
                        pa.array([good], type=pa.binary()),
                    ]
                ),
            },
            schema=schema,
        )
        got = simplify_table(table, 0.05).column("geometry").to_pylist()
        assert got[1] is None and got[2] is None
        assert got[0] is not None and got[3] is not None


@requires_coarsen
class TestRowBoundsLandOnTheirOwnRow:
    """The per-row bounds scatter, which a declared covering column exposes.

    ``_row_bounds`` writes into ``bounds[idx]``, so a bug here does not raise
    and loses no rows -- it silently files row *k*'s box against row *j*. A
    covering column that no longer bounds its own geometry is a reader-visible
    data error (a query pruning on it skips matching rows), and nulls are what
    make ``idx`` differ from ``arange``, so they have to be interleaved for the
    difference to show.
    """

    def _covering_table(self, wkb):
        import json

        np = _np()
        shapely = _shapely()
        bounds = shapely.bounds(shapely.from_wkb(np.array(wkb, dtype=object)))
        bbox = pa.StructArray.from_arrays(
            [pa.array(bounds[:, i], type=pa.float32()) for i in range(4)],
            names=["xmin", "ymin", "xmax", "ymax"],
        )
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Polygon"],
                    "covering": {
                        "bbox": {
                            "xmin": ["bbox", "xmin"],
                            "ymin": ["bbox", "ymin"],
                            "xmax": ["bbox", "xmax"],
                            "ymax": ["bbox", "ymax"],
                        }
                    },
                }
            },
        }
        return pa.table(
            {"geometry": pa.array(wkb, type=pa.binary()), "bbox": bbox},
            schema=pa.schema(
                [pa.field("geometry", pa.binary()), pa.field("bbox", bbox.type)],
                metadata={b"geo": json.dumps(geo).encode()},
            ),
        )

    def test_each_covering_row_still_bounds_its_own_geometry(self):
        shapely = _shapely()
        from geoparquet_io.core.process.simplify import simplify_table

        # Spread far apart, so a box filed against the wrong row cannot
        # accidentally still contain it.
        wkb = [
            None if i % 3 == 1 else shapely.to_wkb(_jagged(i * 100.0, i * 50.0)) for i in range(9)
        ]
        out = simplify_table(self._covering_table(wkb), 0.05)
        boxes = out.column("bbox").to_pylist()
        geoms = out.column("geometry").to_pylist()
        for row, raw in zip(boxes, geoms, strict=True):
            if raw is None:
                continue
            xmin, ymin, xmax, ymax = shapely.from_wkb(raw).bounds
            assert row["xmin"] <= xmin and row["ymin"] <= ymin
            assert row["xmax"] >= xmax and row["ymax"] >= ymax
            assert row["xmin"] == pytest.approx(xmin, abs=0.01)
            assert row["ymax"] == pytest.approx(ymax, abs=0.01)

    def test_a_null_row_gets_no_box(self):
        import math

        shapely = _shapely()
        from geoparquet_io.core.process.simplify import simplify_table

        wkb = [None, shapely.to_wkb(_jagged(0, 0)), None]
        out = simplify_table(self._covering_table(wkb), 0.05)
        boxes = out.column("bbox").to_pylist()
        for i in (0, 2):
            # A null geometry gets a null struct, not a struct of NaNs.
            assert boxes[i] is None or math.isnan(boxes[i]["xmin"])
        assert boxes[1]["xmin"] == pytest.approx(0.0, abs=0.01)


@requires_coarsen
class TestCoverageMode:
    """Coverage mode needs every geometry at once, chunked or not.

    It is the only caller that concatenates the column, so a chunked input is
    the case where "just take the first chunk" would quietly drop rows.
    """

    def _coverage_grid(self, cells=9):
        """Cells sharing jagged edges -- a valid polygonal coverage."""
        shapely = _shapely()
        return [
            shapely.Polygon(
                [
                    (float(i), 0),
                    (i + 0.5, 0.02),
                    (float(i) + 1, 0),
                    (float(i) + 1, 1),
                    (i + 0.5, 0.98),
                    (float(i), 1),
                ]
            )
            for i in range(cells)
        ]

    @pytest.mark.parametrize("chunks", [1, 2, 4])
    def test_chunked_coverage_keeps_every_row(self, chunks):
        shapely = _shapely()
        from geoparquet_io.core.process.simplify import simplify_table

        wkb = [shapely.to_wkb(g) for g in self._coverage_grid()]
        out = simplify_table(_table(wkb, chunks=chunks), 0.05, coverage=True)
        assert out.num_rows == len(wkb)
        assert out.column("geometry").null_count == 0
        reference = simplify_table(_table(wkb, chunks=1), 0.05, coverage=True)
        assert out.column("geometry").to_pylist() == reference.column("geometry").to_pylist()

    def test_chunked_coverage_with_nulls(self):
        shapely = _shapely()
        from geoparquet_io.core.process.simplify import simplify_table

        wkb = [shapely.to_wkb(g) for g in self._coverage_grid(6)]
        wkb.insert(2, None)
        wkb.append(None)
        out = simplify_table(_table(wkb, chunks=3), 0.05, coverage=True)
        got = out.column("geometry").to_pylist()
        assert len(got) == len(wkb)
        assert got[2] is None and got[-1] is None
        assert all(v is not None for i, v in enumerate(got) if wkb[i] is not None)

    def test_an_empty_coverage_column(self):
        from geoparquet_io.core.process.simplify import simplify_table

        out = simplify_table(_table([]), 0.05, coverage=True)
        assert out.num_rows == 0


@requires_coarsen
class TestDropEmpty:
    """``drop_empty`` inverts the empty mask; nulls are not empties."""

    def _collapsing(self, x=0.0):
        """A sliver simplify empties at a coarse tolerance.

        Only with ``preserve_topology=False``: with it on, GEOS refuses to
        collapse the ring and the row is never marked empty, so every test
        here passes that off explicitly.
        """
        shapely = _shapely()
        return shapely.Polygon([(x, 0), (x + 0.001, 0), (x + 0.001, 0.001), (x, 0)])

    def test_nulls_survive_drop_empty(self):
        shapely = _shapely()
        from geoparquet_io.core.process.simplify import simplify_table

        wkb = [
            shapely.to_wkb(_jagged(5, 5)),
            None,
            shapely.to_wkb(self._collapsing()),
            None,
            shapely.to_wkb(_jagged(7, 7)),
        ]
        out = simplify_table(_table(wkb), 0.01, drop_empty=True, preserve_topology=False)
        got = out.column("geometry").to_pylist()
        names = out.column("name").to_pylist()
        # The two nulls stay, the collapsed sliver goes.
        assert got.count(None) == 2
        assert "f2" not in names
        assert len(got) == 4

    def test_the_bbox_excludes_dropped_rows(self, tmp_path):
        """Through the file path, where the bbox is actually computed."""
        import json

        import pyarrow.parquet as pq

        shapely = _shapely()
        from geoparquet_io.core.process.simplify import simplify_file
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        wkb = [shapely.to_wkb(self._collapsing()), shapely.to_wkb(_jagged(20, 20))]
        src, out = tmp_path / "in.parquet", tmp_path / "out.parquet"
        write_geoparquet_table(_table(wkb), str(src))
        simplify_file(str(src), str(out), 0.01, drop_empty=True, preserve_topology=False)
        block = json.loads(pq.ParquetFile(str(out)).schema_arrow.metadata[b"geo"])
        geo = block["columns"][block["primary_column"]]
        assert pq.read_table(str(out)).num_rows == 1
        assert geo["bbox"][0] >= 20.0

    def test_dropping_every_row(self):
        shapely = _shapely()
        from geoparquet_io.core.process.simplify import simplify_table

        wkb = [shapely.to_wkb(self._collapsing(x)) for x in (0.0, 5.0, 9.0)]
        out = simplify_table(_table(wkb), 0.01, drop_empty=True, preserve_topology=False)
        assert out.num_rows == 0


class TestAccumulateStats:
    """The type fold on its own, at a scale where a Python loop would show."""

    def test_many_rows_few_types(self):
        shapely = _shapely()
        np = _np()
        from geoparquet_io.core.process.simplify import _accumulate_stats

        geoms = shapely.from_wkb(
            [shapely.to_wkb(shapely.Point(i, i)) for i in range(5000)]
            + [shapely.to_wkb(_jagged(0, 0))]
        )
        bounds = shapely.bounds(geoms)
        types: set = set()
        bbox = [float("inf"), float("inf"), float("-inf"), float("-inf")]
        _accumulate_stats(shapely, np, geoms, bounds, types, bbox)
        assert types == {"Point", "Polygon"}
        assert bbox[0] == 0.0

    def test_an_unknown_type_id_is_skipped_not_raised(self):
        """A geometry shapely types as -1 (none) must not land in the set."""
        shapely = _shapely()
        np = _np()
        from geoparquet_io.core.process.simplify import _accumulate_stats

        geoms = np.array([None, shapely.Point(1, 1)], dtype=object)
        bounds = np.array([[np.nan] * 4, [1.0, 1.0, 1.0, 1.0]])
        types: set = set()
        bbox = [float("inf"), float("inf"), float("-inf"), float("-inf")]
        _accumulate_stats(shapely, np, geoms, bounds, types, bbox)
        assert types == {"Point"}

    def test_empty_batch_is_a_noop(self):
        shapely = _shapely()
        np = _np()
        from geoparquet_io.core.process.simplify import _accumulate_stats

        types: set = set()
        bbox = [float("inf"), float("inf"), float("-inf"), float("-inf")]
        _accumulate_stats(shapely, np, np.empty(0, dtype=object), np.empty((0, 4)), types, bbox)
        assert types == set()
        assert bbox == [float("inf"), float("inf"), float("-inf"), float("-inf")]

    def test_all_nan_bounds_leave_the_bbox_alone(self):
        shapely = _shapely()
        np = _np()
        from geoparquet_io.core.process.simplify import _accumulate_stats

        geoms = shapely.from_wkb([shapely.to_wkb(shapely.Point(1, 1))])
        types: set = set()
        bbox = [1.0, 2.0, 3.0, 4.0]
        _accumulate_stats(shapely, np, geoms, np.full((1, 4), np.nan), types, bbox)
        assert bbox == [1.0, 2.0, 3.0, 4.0]
        assert types == {"Point"}


class TestArrowHelpers:
    """``_first_wkb_of`` and ``_one_array``, which replaced the to_pylist scans.

    Unit-tested because their branches are reached from only two callers --
    the ``--simplify-crs`` sample scan and coverage mode -- so going through
    the commands leaves the shapes that matter (no chunks, one chunk, several,
    an empty leading chunk) unexercised.
    """

    def test_first_wkb_of_a_plain_array(self):
        from geoparquet_io.core.process.simplify import _first_wkb_of

        assert _first_wkb_of(pa.array([b"a", b"b"], type=pa.binary())) == b"a"

    def test_first_wkb_of_skips_leading_nulls(self):
        from geoparquet_io.core.process.simplify import _first_wkb_of

        assert _first_wkb_of(pa.array([None, None, b"c"], type=pa.binary())) == b"c"

    def test_first_wkb_of_an_all_null_array(self):
        from geoparquet_io.core.process.simplify import _first_wkb_of

        assert _first_wkb_of(pa.array([None, None], type=pa.binary())) is None

    def test_first_wkb_of_an_empty_array(self):
        from geoparquet_io.core.process.simplify import _first_wkb_of

        assert _first_wkb_of(pa.array([], type=pa.binary())) is None

    def test_first_wkb_of_scans_past_an_empty_and_an_all_null_chunk(self):
        from geoparquet_io.core.process.simplify import _first_wkb_of

        column = pa.chunked_array(
            [
                pa.array([], type=pa.binary()),
                pa.array([None], type=pa.binary()),
                pa.array([b"found"], type=pa.binary()),
            ]
        )
        assert _first_wkb_of(column) == b"found"

    def test_first_wkb_of_a_chunked_array_with_no_chunks(self):
        from geoparquet_io.core.process.simplify import _first_wkb_of

        assert _first_wkb_of(pa.chunked_array([], type=pa.binary())) is None

    def test_one_array_passes_a_plain_array_through(self):
        from geoparquet_io.core.process.simplify import _one_array

        arr = pa.array([b"a"], type=pa.binary())
        assert _one_array(arr) is arr

    def test_one_array_unwraps_a_single_chunk(self):
        from geoparquet_io.core.process.simplify import _one_array

        column = pa.chunked_array([pa.array([b"a", b"b"], type=pa.binary())])
        out = _one_array(column)
        assert isinstance(out, pa.Array)
        assert out.to_pylist() == [b"a", b"b"]

    def test_one_array_concatenates_several_chunks_in_order(self):
        from geoparquet_io.core.process.simplify import _one_array

        column = pa.chunked_array(
            [
                pa.array([b"a"], type=pa.binary()),
                pa.array([None], type=pa.binary()),
                pa.array([b"c"], type=pa.binary()),
            ]
        )
        out = _one_array(column)
        assert isinstance(out, pa.Array)
        assert out.to_pylist() == [b"a", None, b"c"]

    def test_one_array_of_a_chunked_array_with_no_chunks(self):
        """``pa.concat_arrays([])`` raises, and geoarrow aborts on zero chunks."""
        from geoparquet_io.core.process.simplify import _one_array

        out = _one_array(pa.chunked_array([], type=pa.binary()))
        assert len(out) == 0
        assert out.type == pa.binary()
