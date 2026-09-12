"""Tests for core/check_fixes.py module."""

import os
import shutil
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core import check_fixes
from geoparquet_io.core.check_fixes import (
    fix_bbox_column,
    fix_bbox_metadata,
    fix_bbox_removal,
    fix_compression,
    fix_spatial_ordering,
)
from geoparquet_io.core.check_parquet_structure import (
    check_compression,
    check_metadata_and_bbox,
)


class TestFixCompression:
    """Tests for fix_compression function."""

    def test_fixes_snappy_to_zstd(self, places_test_file, temp_output_dir):
        """Test fixing SNAPPY compression to ZSTD."""
        # Create a file with SNAPPY compression
        snappy_file = os.path.join(temp_output_dir, "snappy.parquet")
        output_file = os.path.join(temp_output_dir, "fixed.parquet")

        table = pq.read_table(places_test_file)
        pq.write_table(table, snappy_file, compression="SNAPPY")

        # Verify it has SNAPPY compression
        result = check_compression(snappy_file, verbose=False, return_results=True)
        assert result["current_compression"] == "SNAPPY"

        # Apply fix
        fix_result = fix_compression(snappy_file, output_file, verbose=False)

        assert fix_result["success"] is True
        assert "ZSTD" in fix_result["fix_applied"]
        assert os.path.exists(output_file)

        # Verify compression is now ZSTD
        final_result = check_compression(output_file, verbose=False, return_results=True)
        assert final_result["current_compression"] == "ZSTD"

    def test_fixes_with_verbose(self, places_test_file, temp_output_dir):
        """Test fix_compression with verbose flag."""
        snappy_file = os.path.join(temp_output_dir, "snappy.parquet")
        output_file = os.path.join(temp_output_dir, "fixed.parquet")

        table = pq.read_table(places_test_file)
        pq.write_table(table, snappy_file, compression="SNAPPY")

        fix_result = fix_compression(snappy_file, output_file, verbose=True)
        assert fix_result["success"] is True


class TestFixBboxColumn:
    """Tests for fix_bbox_column function."""

    def test_adds_missing_bbox_column(self, places_test_file, temp_output_dir):
        """Test adding bbox column to file without one."""
        # Create file without bbox column
        no_bbox_file = os.path.join(temp_output_dir, "no_bbox.parquet")
        output_file = os.path.join(temp_output_dir, "fixed.parquet")

        table = pq.read_table(places_test_file)
        if "bbox" in table.column_names:
            table = table.drop(["bbox"])
        pq.write_table(table, no_bbox_file)

        # Verify no bbox column
        result = check_metadata_and_bbox(no_bbox_file, verbose=False, return_results=True)
        assert result["has_bbox_column"] is False

        # Apply fix
        fix_result = fix_bbox_column(no_bbox_file, output_file, verbose=False)

        assert fix_result["success"] is True
        assert os.path.exists(output_file)

        # Verify bbox column now exists
        final_result = check_metadata_and_bbox(output_file, verbose=False, return_results=True)
        assert final_result["has_bbox_column"] is True

    def test_with_verbose(self, buildings_test_file, temp_output_dir):
        """Test fix_bbox_column with verbose flag."""
        output_file = os.path.join(temp_output_dir, "fixed.parquet")

        # Buildings file doesn't have bbox
        fix_result = fix_bbox_column(buildings_test_file, output_file, verbose=True)
        assert fix_result["success"] is True


class TestFixBboxMetadata:
    """Tests for fix_bbox_metadata function."""

    def test_adds_bbox_metadata(self, places_v11_file, temp_output_dir):
        """Test adding bbox metadata to file with bbox column.

        Uses the 1.1 places fixture: covering is a 1.1-only key, so adding it to
        the 1.0 original is now refused outright (gpio #686).
        """
        test_file = os.path.join(temp_output_dir, "test.parquet")
        shutil.copy2(places_v11_file, test_file)

        fix_result = fix_bbox_metadata(test_file, test_file, verbose=False)

        assert fix_result["success"] is True
        assert "bbox" in fix_result["fix_applied"].lower()

    def test_copies_file_if_output_different(self, places_v11_file, temp_output_dir):
        """Test that file is copied when output differs from input."""
        output_file = os.path.join(temp_output_dir, "output.parquet")

        fix_result = fix_bbox_metadata(places_v11_file, output_file, verbose=False)

        assert fix_result["success"] is True
        assert os.path.exists(output_file)

    def test_with_verbose(self, places_v11_file, temp_output_dir):
        """Test fix_bbox_metadata with verbose flag."""
        test_file = os.path.join(temp_output_dir, "test.parquet")
        shutil.copy2(places_v11_file, test_file)

        fix_result = fix_bbox_metadata(test_file, test_file, verbose=True)
        assert fix_result["success"] is True


class TestFixRowGroups:
    """Tests for fix_row_groups function."""

    def test_optimizes_row_groups(self, places_test_file, temp_output_dir):
        """Test row group optimization."""
        from geoparquet_io.core.check_fixes import fix_row_groups

        output_file = os.path.join(temp_output_dir, "optimized.parquet")

        fix_result = fix_row_groups(places_test_file, output_file, verbose=False)

        assert fix_result["success"] is True
        assert "Optimized row groups" in fix_result["fix_applied"]
        assert os.path.exists(output_file)

    def test_with_geoparquet_version(self, places_test_file, temp_output_dir):
        """Test row group optimization preserving version."""
        from geoparquet_io.core.check_fixes import fix_row_groups

        output_file = os.path.join(temp_output_dir, "optimized.parquet")

        fix_result = fix_row_groups(
            places_test_file, output_file, verbose=False, geoparquet_version="1.1"
        )

        assert fix_result["success"] is True

    def test_with_verbose(self, places_test_file, temp_output_dir):
        """Test row group optimization with verbose output."""
        from geoparquet_io.core.check_fixes import fix_row_groups

        output_file = os.path.join(temp_output_dir, "optimized.parquet")

        fix_result = fix_row_groups(places_test_file, output_file, verbose=True)

        assert fix_result["success"] is True


class TestFixBboxRemoval:
    """Tests for fix_bbox_removal function."""

    def test_removes_bbox_column(self, places_test_file, temp_output_dir):
        """Test removing bbox column from file."""
        from geoparquet_io.core.check_fixes import fix_bbox_removal

        output_file = os.path.join(temp_output_dir, "no_bbox.parquet")

        # First ensure test file has bbox column
        table = pq.read_table(places_test_file)
        if "bbox" not in table.column_names:
            # Skip if no bbox column
            return

        fix_result = fix_bbox_removal(
            places_test_file, output_file, bbox_column_name="bbox", verbose=False
        )

        assert fix_result["success"] is True
        assert "Removed bbox column" in fix_result["fix_applied"]

        # Verify column removed
        result_table = pq.read_table(output_file)
        assert "bbox" not in result_table.column_names

    def test_with_verbose(self, places_test_file, temp_output_dir):
        """Test bbox removal with verbose output."""
        from geoparquet_io.core.check_fixes import fix_bbox_removal

        output_file = os.path.join(temp_output_dir, "no_bbox.parquet")

        table = pq.read_table(places_test_file)
        if "bbox" not in table.column_names:
            return

        fix_result = fix_bbox_removal(
            places_test_file, output_file, bbox_column_name="bbox", verbose=True
        )

        assert fix_result["success"] is True


class TestFixBboxRemovalSqlInjection:
    """The bbox column name reaches the ``EXCLUDE`` rewrite from the file's own
    schema (``_detect_bbox_column_from_table``), so it is attacker-controlled.
    A bare interpolation let a crafted column name inject arbitrary SQL into the
    projection of ``gpio check --fix`` (#918)."""

    # A struct column NAME (pyarrow allows any name) that breaks out of the
    # ``SELECT * EXCLUDE (<name>) FROM ...`` template: it closes the EXCLUDE
    # list, appends an attacker-chosen projection column, and opens a final
    # scalar subquery that the template's own trailing ``)`` closes.
    _PAYLOAD = "filler) , (SELECT 42) AS pwn, (SELECT 1 LIMIT 0"

    def _write_malicious_file(self, path, places_test_file):
        """A GeoParquet file whose bbox struct column is named with an injection
        payload, plus a benign ``filler`` column the payload references so the
        injected SQL is valid on the vulnerable code path."""
        places = pq.read_table(places_test_file)
        n = places.num_rows
        struct_arr = pa.StructArray.from_arrays(
            [
                pa.array([0.0] * n),
                pa.array([0.0] * n),
                pa.array([1.0] * n),
                pa.array([1.0] * n),
            ],
            names=["xmin", "ymin", "xmax", "ymax"],
        )
        table = pa.table(
            {
                "geometry": places.column("geometry"),
                "filler": pa.array(list(range(n))),
                self._PAYLOAD: struct_arr,
            }
        ).replace_schema_metadata(places.schema.metadata or {})
        pq.write_table(table, path)

    def test_injection_payload_in_bbox_name_does_not_execute(
        self, places_test_file, temp_output_dir
    ):
        src = os.path.join(temp_output_dir, "malicious.parquet")
        out = os.path.join(temp_output_dir, "fixed.parquet")
        self._write_malicious_file(src, places_test_file)

        fix_bbox_removal(src, out, bbox_column_name=self._PAYLOAD, verbose=False)

        result_cols = pq.read_table(out).column_names
        # The injected projection must never materialise.
        assert "pwn" not in result_cols
        assert "(SELECT 1 LIMIT 0)" not in result_cols
        # The named column is treated as a single identifier and removed cleanly;
        # the legitimate columns survive untouched.
        assert self._PAYLOAD not in result_cols
        assert result_cols == ["geometry", "filler"]

    def test_bbox_name_with_embedded_quote_is_escaped(self, places_test_file, temp_output_dir):
        """A payload carrying a literal double quote must have it doubled, not
        interpreted as an identifier delimiter."""
        payload = 'weird") , (SELECT 99) AS pwn2 FROM (SELECT 1) t("x'
        src = os.path.join(temp_output_dir, "malicious_quote.parquet")
        out = os.path.join(temp_output_dir, "fixed_quote.parquet")

        places = pq.read_table(places_test_file)
        n = places.num_rows
        struct_arr = pa.StructArray.from_arrays(
            [
                pa.array([0.0] * n),
                pa.array([0.0] * n),
                pa.array([1.0] * n),
                pa.array([1.0] * n),
            ],
            names=["xmin", "ymin", "xmax", "ymax"],
        )
        table = pa.table(
            {"geometry": places.column("geometry"), payload: struct_arr}
        ).replace_schema_metadata(places.schema.metadata or {})
        pq.write_table(table, src)

        fix_bbox_removal(src, out, bbox_column_name=payload, verbose=False)

        result_cols = pq.read_table(out).column_names
        assert "pwn2" not in result_cols
        assert payload not in result_cols
        assert result_cols == ["geometry"]


class TestGetGeoparquetVersionFromCheckResults:
    """Tests for get_geoparquet_version_from_check_results function."""

    def test_v2_detection(self):
        """Test detecting GeoParquet v2."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "geoparquet_v2", "version": "2.0.0"}}

        version = get_geoparquet_version_from_check_results(check_results)

        assert version == "2.0"

    def test_parquet_geo_only_detection(self):
        """Test detecting parquet-geo-only files."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "parquet_geo_only"}}

        version = get_geoparquet_version_from_check_results(check_results)

        assert version == "parquet-geo-only"

    def test_v1_0_detection(self):
        """Test detecting GeoParquet v1.0."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "geoparquet_v1", "version": "1.0.0"}}

        version = get_geoparquet_version_from_check_results(check_results)

        assert version == "1.0"

    def test_v1_1_detection(self):
        """Test detecting GeoParquet v1.1."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "geoparquet_v1", "version": "1.1.0"}}

        version = get_geoparquet_version_from_check_results(check_results)

        assert version == "1.1"

    def test_unknown_defaults_to_none(self):
        """Test unknown file type defaults to None."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {"bbox": {"file_type": "unknown"}}

        version = get_geoparquet_version_from_check_results(check_results)

        assert version is None

    def test_missing_bbox_result(self):
        """Test handling missing bbox result."""
        from geoparquet_io.core.check_fixes import get_geoparquet_version_from_check_results

        check_results = {}

        version = get_geoparquet_version_from_check_results(check_results)

        assert version is None


class TestFixSpatialOrdering:
    """Tests for fix_spatial_ordering function."""

    def test_fix_spatial_ordering_with_existing_temp_file(self, places_test_file, temp_output_dir):
        """Test that fix_spatial_ordering works even when output file exists.

        This tests the bug fix for issue #278 where sequential fixes in `check --fix`
        would fail with "Output file already exists" when a temp file path was reused.
        """
        output_file = os.path.join(temp_output_dir, "fixed.parquet")

        # Create the output file to simulate it already existing
        # (simulating a temp file collision scenario)
        shutil.copy2(places_test_file, output_file)

        # This should not raise "Output file already exists" error
        # because fix_spatial_ordering should pass overwrite=True to hilbert_order
        fix_result = fix_spatial_ordering(places_test_file, output_file, verbose=False)

        assert fix_result["success"] is True
        assert "Hilbert" in fix_result["fix_applied"]
        assert os.path.exists(output_file)

    def test_inplace_fix_reorders_the_input(self, places_test_file, temp_output_dir):
        """#941: input == output is routed through a temp file, not refused.

        ``hilbert_order`` reads the whole input while writing, so it refuses an
        output that resolves to its own input. Every other ``fix_*`` already
        routes an in-place rewrite through a temp file; this one used to hand
        the path straight through and die with "Cannot overwrite input file".
        """
        target = os.path.join(temp_output_dir, "inplace_spatial.parquet")
        table = pq.read_table(places_test_file)
        shuffled = table.take(pa.array(list(reversed(range(table.num_rows)))))
        pq.write_table(shuffled, target)
        before = pq.read_table(target).column("fsq_place_id").to_pylist()

        fix_result = fix_spatial_ordering(target, target, verbose=False)

        assert fix_result["success"] is True
        after = pq.read_table(target).column("fsq_place_id").to_pylist()
        assert sorted(after) == sorted(before)
        assert after != before

    def test_temp_file_is_removed_when_the_sort_fails(self, places_test_file, temp_output_dir):
        """A failed in-place sort leaves no stray temp file behind."""
        target = os.path.join(temp_output_dir, "doomed.parquet")
        shutil.copy2(places_test_file, target)
        seen = []

        def explode(*_args, output_parquet, **_kwargs):
            seen.append(output_parquet)
            Path(output_parquet).write_bytes(b"partial")
            raise RuntimeError("sort blew up")

        with mock.patch.object(check_fixes, "hilbert_order", side_effect=explode):
            with pytest.raises(RuntimeError, match="sort blew up"):
                fix_spatial_ordering(target, target, verbose=False)

        assert seen and seen[0] != target
        assert not os.path.exists(seen[0])
        # The original is untouched: nothing was moved over it.
        assert pq.read_table(target).num_rows == pq.read_table(places_test_file).num_rows

    def test_temp_output_is_written_beside_the_target(self, places_test_file, temp_output_dir):
        """The rewrite stages next to the target, not in ``$TMPDIR``.

        ``$TMPDIR`` is routinely on another filesystem (a Linux ``/tmp`` tmpfs,
        a container, an NFS home, an external volume). Staging there makes a
        multi-GB in-place fix exhaust a tmpfs the destination would have
        accommodated, and turns the move back into a copy+unlink -- which is
        also what makes it non-atomic.
        """
        target = os.path.join(temp_output_dir, "beside.parquet")
        shutil.copy2(places_test_file, target)
        seen = []

        def capture(*_args, output_parquet, **_kwargs):
            seen.append(output_parquet)
            shutil.copy2(places_test_file, output_parquet)

        with mock.patch.object(check_fixes, "hilbert_order", side_effect=capture):
            fix_spatial_ordering(target, target, verbose=False)

        assert seen, "hilbert_order was never called"
        assert os.path.dirname(os.path.abspath(seen[0])) == os.path.abspath(temp_output_dir)

    def test_an_aliased_output_path_is_still_an_in_place_fix(
        self, places_test_file, temp_output_dir
    ):
        """``./a.parquet`` and ``a.parquet`` name one file, so route through a temp.

        ``handle_output_overwrite`` compares ``Path.resolve()``, so it sees the
        two spellings as the same file and refuses the write. Comparing the raw
        strings here missed that and reproduced the original #941 failure for
        any ``./`` prefix, relative-vs-absolute mix or symlink alias.
        """
        target = os.path.join(temp_output_dir, "aliased.parquet")
        shutil.copy2(places_test_file, target)
        alias = os.path.join(temp_output_dir, ".", "aliased.parquet")
        before = pq.read_table(target).column("fsq_place_id").to_pylist()

        fix_result = fix_spatial_ordering(alias, target, verbose=False)

        assert fix_result["success"] is True
        after = pq.read_table(target).column("fsq_place_id").to_pylist()
        assert sorted(after) == sorted(before)

    def test_a_failed_move_leaves_the_original_intact(self, places_test_file, temp_output_dir):
        """A move that fails must not take the only good copy of the data with it.

        The whole destructive step is the ``os.replace()`` inside
        ``_staged_output``, so if it raises, the original -- never unlinked --
        is exactly as it was. Under ``--fix --no-backup`` there is no ``.bak``
        to fall back on, and a direct caller of this function has no ``.bak``
        concept at all.
        """
        target = os.path.join(temp_output_dir, "precious.parquet")
        shutil.copy2(places_test_file, target)
        original_bytes = Path(target).read_bytes()

        def copy_into_place(*_args, output_parquet, **_kwargs):
            shutil.copy2(places_test_file, output_parquet)

        def no_space(*_args, **_kwargs):
            raise OSError(28, "No space left on device")

        with (
            mock.patch.object(check_fixes, "hilbert_order", side_effect=copy_into_place),
            mock.patch.object(os, "replace", side_effect=no_space),
            pytest.raises(OSError, match="No space left on device"),
        ):
            fix_spatial_ordering(target, target, verbose=False)

        assert os.path.exists(target), "the in-place fix destroyed the original file"
        assert Path(target).read_bytes() == original_bytes
        assert list(Path(temp_output_dir).glob(".gpio-fix-*")) == []


class TestInPlaceFixOperations:
    """Tests for in-place fix operations (input == output)."""

    def test_fix_compression_inplace(self, places_test_file, temp_output_dir):
        """Test that in-place compression fix uses temp file and succeeds."""
        # Create a copy to modify in-place
        test_file = os.path.join(temp_output_dir, "inplace.parquet")
        shutil.copy2(places_test_file, test_file)

        # Apply compression fix in-place (input == output)
        fix_result = fix_compression(
            parquet_file=test_file,
            output_file=test_file,  # Same path - in-place operation
            verbose=False,
            geoparquet_version="1.1",
        )

        assert fix_result["success"] is True
        assert os.path.exists(test_file)

        # Verify file was actually modified (ZSTD compression)
        from geoparquet_io.core.check_parquet_structure import check_compression

        result = check_compression(test_file, verbose=False, return_results=True)
        assert result["current_compression"] == "ZSTD"

    def test_fix_compression_inplace_preserves_data(self, places_test_file, temp_output_dir):
        """Test that in-place fix preserves all data."""
        import pyarrow.parquet as pq

        test_file = os.path.join(temp_output_dir, "inplace_data.parquet")
        shutil.copy2(places_test_file, test_file)

        # Read original data
        original_table = pq.read_table(test_file)
        original_row_count = len(original_table)

        # Apply in-place fix
        fix_compression(
            parquet_file=test_file, output_file=test_file, verbose=False, geoparquet_version="1.1"
        )

        # Verify data is preserved
        fixed_table = pq.read_table(test_file)
        assert len(fixed_table) == original_row_count
        assert fixed_table.schema.names == original_table.schema.names


class TestHilbertDetectionAndWarnings:
    """Tests for Hilbert-ordered file detection and spatial warnings."""

    def test_spatial_check_passes_for_hilbert_files(self, temp_output_dir):
        """Test that files with ~100k rows/group are detected as Hilbert-ordered."""
        from geoparquet_io.core.check_spatial_order import check_spatial_order_bbox_stats
        from geoparquet_io.core.hilbert_order import hilbert_order

        # Create a test file and apply Hilbert ordering
        test_file = "tests/data/austria_bbox_covering.parquet"
        output_file = os.path.join(temp_output_dir, "hilbert_ordered.parquet")

        hilbert_order(
            input_parquet=test_file,
            output_parquet=output_file,
            add_bbox_flag=False,
            verbose=False,
            row_group_rows=100000,  # Standard Hilbert row group size
        )

        # Check spatial ordering - should pass despite high overlap
        result = check_spatial_order_bbox_stats(
            output_file, verbose=False, return_results=True, quiet=True
        )

        # Should be marked as passed (Hilbert-ordered with large groups)
        assert result["passed"] is True
        assert result["fix_available"] is False

    def test_spatial_check_fails_for_non_hilbert_files(self, places_test_file, temp_output_dir):
        """Test that randomly shuffled data gets spatial warnings."""
        import random

        import pyarrow.parquet as pq

        from geoparquet_io.core.check_spatial_order import check_spatial_order_bbox_stats

        output_file = os.path.join(temp_output_dir, "non_hilbert.parquet")

        # Shuffle rows to destroy spatial order before writing with small row groups
        table = pq.read_table(places_test_file)
        indices = list(range(table.num_rows))
        random.seed(42)
        random.shuffle(indices)
        table = table.take(indices)
        pq.write_table(table, output_file, compression="ZSTD", row_group_size=50)

        result = check_spatial_order_bbox_stats(
            output_file, verbose=False, return_results=True, quiet=True
        )

        assert result["passed"] is False
        assert result["fix_available"] is True


class TestRowGroupFixIdempotency:
    """Tests that row group fixes don't loop infinitely."""

    def test_row_group_fix_not_offered_when_optimal(self, temp_output_dir):
        """Test that files with optimal row count don't get row group fix."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.check_parquet_structure import check_row_groups

        test_file = "tests/data/austria_bbox_covering.parquet"
        output_file = os.path.join(temp_output_dir, "optimal_rows.parquet")

        # Write with optimal row count (100k)
        table = pq.read_table(test_file)
        pq.write_table(table, output_file, compression="ZSTD", row_group_size=100000)

        result = check_row_groups(output_file, verbose=False, return_results=True, quiet=True)

        # Should pass row count check (even if size is small)
        assert result["row_status"] == "optimal"
        # Should NOT offer fix (prevents infinite loop)
        assert result["fix_available"] is False

    def test_row_group_fix_offered_when_poor_row_count(self, places_test_file, temp_output_dir):
        """Test that files with poor row count get row group fix."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.check_parquet_structure import check_row_groups

        # Use places_test_file which has ~1000 rows
        output_file = os.path.join(temp_output_dir, "poor_rows.parquet")

        # Write with very small row groups (50 rows per group = too small)
        table = pq.read_table(places_test_file)
        pq.write_table(table, output_file, compression="ZSTD", row_group_size=50)

        result = check_row_groups(output_file, verbose=False, return_results=True, quiet=True)

        # Should fail row count check (50 rows/group is below 100k minimum)
        assert result["row_status"] != "optimal"
        # Should offer fix
        assert result["fix_available"] is True
