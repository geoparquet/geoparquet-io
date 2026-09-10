"""
Tests for admin dataset abstraction layer.
"""

import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from geoparquet_io.core.admin_datasets import (
    AdminDatasetFactory,
    CurrentAdminDataset,
    GAULAdminDataset,
    OvertureAdminDataset,
    check_cache_age,
    clear_cache,
    get_cache_dir,
    get_cached_path,
    get_or_cache_dataset,
)
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

TEST_DATA_DIR = Path(__file__).parent / "data"


def _dead_pid() -> int:
    """A pid that has certainly exited and been reaped."""
    import subprocess
    import sys

    proc = subprocess.Popen([sys.executable, "-c", ""])
    proc.wait()
    return proc.pid


def _spatial_connection(duckdb):
    """An in-memory DuckDB with spatial loaded, configured as gpio configures it."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("SET geometry_always_xy=true;")
    return con


def _write_divisions_fixture(con, tmp_path, rows, name="division_area.parquet"):
    """Write an Overture-divisions-shaped fixture and return its path.

    ``rows`` are ``(wkt, (xmin, xmax, ymin, ymax), country, subtype, is_land)``.
    The geometry column is written as a real ``GEOMETRY`` (as Overture's own
    files read back), so the emitted queries — ``ST_SimplifyPreserveTopology``
    included — can be run verbatim instead of being rewritten by the test.
    """
    src = tmp_path / name
    selects = []
    for wkt, (xmin, xmax, ymin, ymax), country, subtype, is_land in rows:
        land = "NULL" if is_land is None else str(is_land).lower()
        selects.append(
            f"SELECT ST_GeomFromText('{wkt}') AS geometry, "
            f"{{'xmin':{xmin},'xmax':{xmax},'ymin':{ymin},'ymax':{ymax}}} AS bbox, "
            f"'{country}' AS country, '{subtype}' AS subtype, {land}::BOOLEAN AS is_land"
        )
    con.execute(f"COPY ({' UNION ALL '.join(selects)}) TO '{src}' (FORMAT PARQUET)")
    return src


class TestCurrentAdminDataset:
    """Test CurrentAdminDataset implementation."""

    def test_get_dataset_name(self):
        dataset = CurrentAdminDataset()
        assert dataset.get_dataset_name() == "Current (source.coop countries)"

    def test_get_default_source(self):
        dataset = CurrentAdminDataset()
        source = dataset.get_default_source()
        assert source.startswith("https://data.source.coop/")
        assert "countries.parquet" in source

    def test_get_available_levels(self):
        dataset = CurrentAdminDataset()
        levels = dataset.get_available_levels()
        assert levels == ["country"]

    def test_get_level_column_mapping(self):
        dataset = CurrentAdminDataset()
        mapping = dataset.get_level_column_mapping()
        assert mapping == {"country": "country"}

    def test_get_geometry_column(self):
        dataset = CurrentAdminDataset()
        assert dataset.get_geometry_column() == "geometry"

    def test_get_bbox_column(self):
        dataset = CurrentAdminDataset()
        assert dataset.get_bbox_column() == "bbox"

    def test_is_remote_with_default(self):
        dataset = CurrentAdminDataset()
        assert dataset.is_remote() is True

    def test_is_remote_with_local_path(self):
        dataset = CurrentAdminDataset(source_path="/local/path/data.parquet")
        assert dataset.is_remote() is False

    def test_get_partition_columns(self):
        dataset = CurrentAdminDataset()
        columns = dataset.get_partition_columns(["country"])
        assert columns == ["country"]

    def test_validate_levels_valid(self):
        dataset = CurrentAdminDataset()
        # Should not raise
        dataset.validate_levels(["country"])

    def test_validate_levels_invalid(self):
        from geoparquet_io.core.exceptions import InvalidParameterError

        dataset = CurrentAdminDataset()
        with pytest.raises(InvalidParameterError) as exc_info:
            dataset.validate_levels(["continent"])
        assert "invalid" in str(exc_info.value).lower()
        assert "continent" in str(exc_info.value)


class TestGAULAdminDataset:
    """Test GAULAdminDataset implementation."""

    def test_get_dataset_name(self):
        dataset = GAULAdminDataset()
        assert dataset.get_dataset_name() == "GAUL L2 Admin Boundaries"

    def test_get_default_source(self):
        dataset = GAULAdminDataset()
        source = dataset.get_default_source()
        assert "gaul-l2-admin" in source.lower()
        assert source.endswith("*.parquet")

    def test_get_available_levels(self):
        dataset = GAULAdminDataset()
        levels = dataset.get_available_levels()
        assert levels == ["continent", "country", "department"]

    def test_get_level_column_mapping(self):
        dataset = GAULAdminDataset()
        mapping = dataset.get_level_column_mapping()
        assert mapping["continent"] == "continent"
        assert mapping["country"] == "gaul0_name"
        assert mapping["department"] == "gaul2_name"

    def test_get_geometry_column(self):
        dataset = GAULAdminDataset()
        assert dataset.get_geometry_column() == "geometry"

    def test_get_bbox_column(self):
        dataset = GAULAdminDataset()
        assert dataset.get_bbox_column() == "geometry_bbox"

    def test_get_partition_columns_single_level(self):
        dataset = GAULAdminDataset()
        columns = dataset.get_partition_columns(["continent"])
        assert columns == ["continent"]

    def test_get_partition_columns_multi_level(self):
        dataset = GAULAdminDataset()
        columns = dataset.get_partition_columns(["continent", "country", "department"])
        assert columns == ["continent", "gaul0_name", "gaul2_name"]

    def test_validate_levels_all_valid(self):
        dataset = GAULAdminDataset()
        # Should not raise
        dataset.validate_levels(["continent", "country", "department"])

    def test_validate_levels_partial_valid(self):
        dataset = GAULAdminDataset()
        # Should not raise
        dataset.validate_levels(["continent", "country"])

    def test_validate_levels_invalid(self):
        from geoparquet_io.core.exceptions import InvalidParameterError

        dataset = GAULAdminDataset()
        with pytest.raises(InvalidParameterError) as exc_info:
            dataset.validate_levels(["region"])
        assert "invalid" in str(exc_info.value).lower()


class TestOvertureAdminDataset:
    """Test OvertureAdminDataset implementation."""

    def test_get_dataset_name(self):
        dataset = OvertureAdminDataset()
        assert dataset.get_dataset_name() == "Overture Maps Divisions"

    def test_get_default_source(self):
        dataset = OvertureAdminDataset()
        source = dataset.get_default_source()
        assert source.startswith("s3://")
        assert "overturemaps" in source.lower()
        assert "divisions" in source.lower()

    def test_get_available_levels(self):
        dataset = OvertureAdminDataset()
        levels = dataset.get_available_levels()
        # At minimum should have some levels defined
        assert len(levels) > 0
        assert isinstance(levels, list)

    def test_get_geometry_column(self):
        dataset = OvertureAdminDataset()
        assert dataset.get_geometry_column() == "geometry"

    def test_get_bbox_column(self):
        dataset = OvertureAdminDataset()
        assert dataset.get_bbox_column() == "bbox"

    def test_get_read_parquet_options(self):
        dataset = OvertureAdminDataset()
        options = dataset.get_read_parquet_options()
        assert "hive_partitioning" in options
        assert options["hive_partitioning"] == 1

    def test_get_subtype_filter(self):
        dataset = OvertureAdminDataset()
        # The country level spans both of Overture's sovereign classes, so
        # dependent territories are attributed rather than left NULL (#819).
        filter_country = dataset.get_subtype_filter(["country"])
        assert "subtype IN ('country', 'dependency')" in filter_country

        # Test with both levels
        filter_both = dataset.get_subtype_filter(["country", "region"])
        assert "country" in filter_both
        assert "dependency" in filter_both
        assert "region" in filter_both
        assert "subtype IN" in filter_both

    def test_get_subtype_filter_carries_the_whole_level_predicate(self):
        """``--no-cache`` must admit exactly the rows the cache holds (#819).

        The cache bakes ``is_land IS NOT FALSE`` and the country level's
        placeholder/Antarctica filters into the file. A ``--no-cache`` run reads
        the raw Overture release instead, so the same filters have to travel in
        the subtype predicate or a feature matches both a territory's land
        polygon and its maritime (EEZ) polygon and the LEFT JOIN emits it twice.
        """
        dataset = OvertureAdminDataset()
        filter_country = dataset.get_subtype_filter(["country"])
        assert "is_land IS NOT FALSE" in filter_country
        assert "NOT LIKE 'X%'" in filter_country
        assert "'AQ'" in filter_country

        # The region level has no extra filters but still needs the land filter.
        filter_region = dataset.get_subtype_filter(["region"])
        assert "is_land IS NOT FALSE" in filter_region
        assert "NOT LIKE 'X%'" not in filter_region

    def test_get_subtype_filter_drops_the_extras_for_a_prefiltered_source(self):
        """A per-level cache already holds exactly those rows — and does not
        project ``is_land`` at all — so only the subtype clause is emitted.

        Same for a user-supplied ``--admin-source``: gpio does not control its
        schema, so it keeps the pre-#819 clause.
        """
        dataset = OvertureAdminDataset()
        cached = dataset.get_subtype_filter(
            ["country"], source=str(get_cache_dir() / "overture-x-country-dependency-land.parquet")
        )
        assert cached == "subtype IN ('country', 'dependency')"

        custom = OvertureAdminDataset(source_path="/data/my-divisions.parquet")
        assert custom.get_subtype_filter(["country"], source="/data/my-divisions.parquet") == (
            "subtype IN ('country', 'dependency')"
        )

    def test_get_subtype_filter_ors_levels_with_different_filters(self):
        """A multi-level request keeps each level's own filters.

        The country level's placeholder/Antarctica filters must not leak onto
        the region level, so the levels are OR'd rather than AND'd, and the
        whole predicate is parenthesised because callers AND it with an extent
        filter.
        """
        dataset = OvertureAdminDataset()
        filter_both = dataset.get_subtype_filter(["country", "region"])
        assert filter_both.startswith("(") and filter_both.endswith(")")
        assert ") OR (" in filter_both

    def test_get_subtype_filter_does_not_duplicate_levels(self):
        """A repeated level must not emit its predicate twice."""
        dataset = OvertureAdminDataset()
        assert dataset.get_subtype_filter(["country", "country"]) == dataset.get_subtype_filter(
            ["country"]
        )

    def test_get_subtype_filter_no_cache_matches_each_feature_once(self, tmp_path):
        """Behavioral (#819): a dependency's feature matches ONE polygon on the
        ``--no-cache`` path.

        Overture stores a land polygon and a maritime (EEZ) polygon per
        territory; the EEZ polygon covers the landmass too. Filtering on
        ``subtype`` alone matched both, so the plain LEFT JOIN duplicated every
        row in French Guiana (and in the 50+ other dependencies this level now
        attributes).
        """
        duckdb = pytest.importorskip("duckdb")
        con = _spatial_connection(duckdb)
        src = _write_divisions_fixture(
            con,
            tmp_path,
            [
                ("POLYGON((0 0,0 2,2 2,2 0,0 0))", (0.0, 2.0, 0.0, 2.0), "GF", "dependency", True),
                # French Guiana's maritime polygon spans the landmass as well.
                (
                    "POLYGON((-5 -5,-5 5,5 5,5 -5,-5 -5))",
                    (-5.0, 5.0, -5.0, 5.0),
                    "GF",
                    "dependency",
                    False,
                ),
                ("POLYGON((2 0,2 2,4 2,4 0,2 0))", (2.0, 4.0, 0.0, 2.0), "BR", "country", True),
            ],
        )

        predicate = OvertureAdminDataset().get_subtype_filter(["country"])
        matches = con.execute(
            f"""
            SELECT country FROM read_parquet('{src}', hive_partitioning=1)
            WHERE {predicate}
              AND ST_Intersects(geometry, ST_GeomFromText('POINT(1 1)'))
            """
        ).fetchall()
        assert matches == [("GF",)], "no-cache predicate must match each feature exactly once"

    def test_placeholder_country_codes_map_to_their_iso_codes(self, tmp_path):
        """Saba, Sint Eustatius and Jan Mayen carry Overture placeholder X*
        codes but are not disputed: their ISO 3166-1 codes are BQ, BQ and SJ.

        Without the remap the disputed-territory filter dropped them to NULL ->
        'ZZ' under ``--vecorel``. Genuinely disputed X* codes (XK) stay out.
        """
        duckdb = pytest.importorskip("duckdb")
        con = _spatial_connection(duckdb)
        src = _write_divisions_fixture(
            con,
            tmp_path,
            [
                ("POLYGON((0 0,0 1,1 1,1 0,0 0))", (0.0, 1.0, 0.0, 1.0), "XS", "dependency", True),
                ("POLYGON((2 0,2 1,3 1,3 0,2 0))", (2.0, 3.0, 0.0, 1.0), "XE", "dependency", True),
                ("POLYGON((4 0,4 1,5 1,5 0,4 0))", (4.0, 5.0, 0.0, 1.0), "XJ", "dependency", True),
                ("POLYGON((6 0,6 1,7 1,7 0,6 0))", (6.0, 7.0, 0.0, 1.0), "XK", "country", True),
                ("POLYGON((8 0,8 1,9 1,9 0,8 0))", (8.0, 9.0, 0.0, 1.0), "AQ", "country", True),
            ],
        )

        dataset = OvertureAdminDataset()
        cached = con.execute(
            f"SELECT country FROM ({dataset._build_level_cache_query('country', str(src))}) "
            "ORDER BY country"
        ).fetchall()
        assert [r[0] for r in cached] == ["BQ", "BQ", "SJ"]

        # The --no-cache path reads the raw release, so the same codes have to
        # survive its predicate and be remapped by the join's column transform.
        no_cache = con.execute(
            f"""
            SELECT {dataset.get_column_transform("country")} AS code
            FROM read_parquet('{src}', hive_partitioning=1) b
            WHERE {dataset.get_subtype_filter(["country"])}
            ORDER BY code
            """
        ).fetchall()
        assert [r[0] for r in no_cache] == ["BQ", "BQ", "SJ"]

    def test_country_transform_leaves_real_codes_alone(self):
        """The remap is a no-op on already-correct codes, so re-applying it to a
        cache that already stores BQ/SJ cannot corrupt them."""
        duckdb = pytest.importorskip("duckdb")
        con = _spatial_connection(duckdb)
        expr = OvertureAdminDataset().get_column_transform("country").replace('b."country"', "c")
        rows = con.execute(
            f"SELECT {expr} FROM (VALUES ('US'), ('BQ'), ('SJ'), (NULL)) t(c)"
        ).fetchall()
        assert [r[0] for r in rows] == ["US", "BQ", "SJ", None]

    def test_get_column_transform_region(self):
        """Test that region level returns SQL transformation for Vecorel compliance."""
        dataset = OvertureAdminDataset()
        transform = dataset.get_column_transform("region")

        # Should return transformation SQL to strip country prefix. Column refs
        # are qualified with the admin alias `b.` (and quoted) so the transform
        # is unambiguous when the input already has a `region` column (todo 015).
        assert transform is not None
        assert "CASE WHEN b.\"region\" LIKE '%-%'" in transform
        assert "split_part(b.\"region\", '-', 2)" in transform
        assert 'ELSE b."region" END' in transform

    def test_get_column_transform_country(self):
        """The country level remaps Overture's placeholder X* codes to the ISO
        codes those territories actually hold (XS/XE -> BQ, XJ -> SJ).

        Column refs are qualified with the admin alias ``b.`` (and quoted) so
        the transform stays unambiguous when the input carries its own
        ``country`` column.
        """
        dataset = OvertureAdminDataset()
        transform = dataset.get_column_transform("country")

        assert transform is not None
        assert "'XS' THEN 'BQ'" in transform
        assert "'XE' THEN 'BQ'" in transform
        assert "'XJ' THEN 'SJ'" in transform
        assert 'ELSE b."country" END' in transform
        assert "country" not in transform.replace('b."country"', "").replace("'BQ'", "")

    def test_get_column_transform_unknown_level(self):
        """Test that unknown levels return None."""
        dataset = OvertureAdminDataset()
        transform = dataset.get_column_transform("unknown_level")

        assert transform is None

    def test_get_output_column_name_country(self):
        """Test default country column name (dataset prefix)."""
        dataset = OvertureAdminDataset()
        col_name = dataset.get_output_column_name("country")

        # Default prefix should be "overture"
        assert col_name == "overture_country"

    def test_get_output_column_name_region(self):
        """Test default region column name (dataset prefix)."""
        dataset = OvertureAdminDataset()
        col_name = dataset.get_output_column_name("region")

        # Default prefix should be "overture"
        assert col_name == "overture_region"

    def test_get_output_column_name_fallback(self):
        """Test fallback to default pattern for unknown levels."""
        dataset = OvertureAdminDataset()
        col_name = dataset.get_output_column_name("unknown_level")

        # Should use dataset prefix
        assert col_name == "overture_unknown_level"

    def test_per_level_cache_query_filters_to_land(self):
        """Per-level cache keeps only land polygons (see _build_level_cache_query
        for the maritime-EEZ-overlap rationale).

        The predicate is ``IS NOT FALSE`` (not ``= true``) so land polygons with
        a NULL flag are kept rather than silently dropped by SQL three-valued
        logic.
        """
        dataset = OvertureAdminDataset()
        for level in ("country", "region"):
            query = dataset._build_level_cache_query(level, "s3://example/*")
            assert "is_land IS NOT FALSE" in query, f"{level} cache must filter to land"
            assert "is_land = true" not in query, f"{level} must not drop NULL is_land"
            assert f"'{level}'" in query and "subtype IN (" in query

    def test_per_level_cache_query_excludes_disputed_countries(self):
        """Country cache still excludes disputed (X*) territories and Antarctica."""
        dataset = OvertureAdminDataset()
        query = dataset._build_level_cache_query("country", "s3://example/*")
        assert "X%" in query
        assert "AQ" in query

    def test_per_level_cache_query_rejects_unknown_level(self):
        """An unconfigured level must raise rather than be silently mis-projected
        as a region (which would produce a wrong-schema cache)."""
        dataset = OvertureAdminDataset()
        with pytest.raises(ValueError, match="cache config"):
            dataset._build_level_cache_query("locality", "s3://example/*")

    def test_country_cache_query_includes_dependencies(self):
        """Overture files dependent territories (French Guiana, Puerto Rico,
        Reunion, Greenland, Hong Kong, ...) under ``subtype = 'dependency'``,
        not ``'country'``. They carry ISO 3166-1 alpha-2 codes, so the country
        level must take both classes or those territories get NULL -> 'ZZ'
        (#819).
        """
        dataset = OvertureAdminDataset()
        query = dataset._build_level_cache_query("country", "s3://example/*")
        assert "'dependency'" in query
        assert "'country'" in query
        assert "subtype IN (" in query

    def test_region_cache_query_excludes_dependencies(self):
        """Only the country level widens: a dependency is not a region, and
        pulling it in would put a country-shaped polygon in the region cache.
        """
        dataset = OvertureAdminDataset()
        query = dataset._build_level_cache_query("region", "s3://example/*")
        assert "'dependency'" not in query
        assert "'region'" in query

    def test_cached_path_distinguishes_dependency_aware_country_cache(self):
        """A country cache built before #819 holds no dependency polygons, so
        the cache key must change or the fix is invisible to existing users.
        The region cache is unaffected and must keep its key (no needless
        ~1GB re-download).
        """
        dataset = OvertureAdminDataset()
        country = dataset.get_cached_path_for_level("country")
        region = dataset.get_cached_path_for_level("region")
        assert "dependency" in country.name
        assert country.name.endswith("-land.parquet")
        assert "dependency" not in region.name
        assert region.name.endswith("-region-land.parquet")

    def test_dependency_territory_gets_its_own_country_code(self, tmp_path):
        """Behavioral: a feature inside a dependency polygon is attributed to
        that dependency's ISO code, exactly once.

        The fixture mirrors the real Overture shape around French Guiana: a
        ``dependency`` polygon coded GF, and the neighbouring ``country``
        polygon for Brazil that shares a border with it. Before #819 the GF
        point matched nothing (-> NULL -> 'ZZ'); after, it matches GF only.
        Matching *once* is the invariant the memory-safe plain LEFT JOIN
        depends on, so the border-sharing BR polygon is here on purpose.

        The probe sits a hair inside GF *at the shared border* (x=2), where a
        double match would actually show up, and the cache query is run exactly
        as emitted — simplification included. Simplification is why the
        invariant is approximate rather than exact: two independently simplified
        borders leave sub-hectare slivers, so an interior probe is the honest
        assertion and the sliver caveat is documented on
        ``_OVERTURE_LEVEL_CACHE_CONFIG``.
        """
        duckdb = pytest.importorskip("duckdb")
        con = _spatial_connection(duckdb)
        src = _write_divisions_fixture(
            con,
            tmp_path,
            [
                ("POLYGON((0 0,0 2,2 2,2 0,0 0))", (0.0, 2.0, 0.0, 2.0), "GF", "dependency", True),
                ("POLYGON((2 0,2 2,4 2,4 0,2 0))", (2.0, 4.0, 0.0, 2.0), "BR", "country", True),
            ],
        )

        query = OvertureAdminDataset()._build_level_cache_query("country", str(src))
        con.execute(f"CREATE TABLE land AS SELECT * FROM ({query})")

        assert [
            r[0] for r in con.execute("SELECT country FROM land ORDER BY country").fetchall()
        ] == [
            "BR",
            "GF",
        ]

        # Just inside GF, right on the GF/BR border — where a duplicate match
        # would surface if the two polygons overlapped.
        pt = "ST_GeomFromText('POINT(1.999999 1)')"
        matches = con.execute(
            f"SELECT country FROM land WHERE ST_Intersects(geometry, {pt})"
        ).fetchall()
        assert matches == [("GF",)], "dependency feature must match its own code, exactly once"

    def test_cached_path_rejects_unknown_level(self):
        """An unconfigured level must fail the same way the cache producer does,
        instead of inventing a plausible-looking filename for a cache that
        ``_build_level_cache_query`` will refuse to build."""
        dataset = OvertureAdminDataset()
        with pytest.raises(ValueError, match="cache config"):
            dataset.get_cached_path_for_level("locality")

    def test_stale_level_caches_are_removed(self, tmp_path):
        """Every superseded cache for this level/version is unlinked, not just
        the legacy unsuffixed one.

        The country cache key gained a "-dependency" segment in #819, so the
        ~40MB ``-country-land.parquet`` file it supersedes would otherwise sit
        in the cache dir forever.
        """
        from geoparquet_io.core.admin_datasets import _remove_stale_level_caches

        version = "2026-07-22.0"
        current = tmp_path / f"overture-{version}-country-dependency-land.parquet"
        superseded = tmp_path / f"overture-{version}-country-land.parquet"
        legacy = tmp_path / f"overture-{version}-country.parquet"
        other_level = tmp_path / f"overture-{version}-region-land.parquet"
        other_version = tmp_path / "overture-2026-01-01.0-country-land.parquet"
        for path in (current, superseded, legacy, other_level, other_version):
            path.write_bytes(b"stub")

        _remove_stale_level_caches(tmp_path, version, "country", current)

        assert current.exists(), "the current cache must survive"
        assert other_level.exists(), "another level's cache must survive"
        assert other_version.exists(), "another version's cache must survive"
        assert not superseded.exists()
        assert not legacy.exists()

    def test_cached_path_distinguishes_land_only_caches(self):
        """Cache filename encodes the land-only filter so stale (maritime-
        contaminated) caches are not silently reused after the fix.

        The segment between the level and the "-land" marker records the extra
        subtypes the level draws on (#819), so this pins the level and the
        marker rather than their adjacency.
        """
        dataset = OvertureAdminDataset()
        path = dataset.get_cached_path_for_level("country")
        assert "-country" in path.name
        assert path.name.endswith("-land.parquet")

    def test_land_filter_removes_maritime_double_match(self, tmp_path):
        """Behavioral: the land-only filter makes a feature match exactly one
        polygon per level instead of two (land + maritime EEZ) — the root cause
        of the ~2.6x row multiplication (PR #474).

        Runs the ``_build_level_cache_query`` output verbatim.
        """
        duckdb = pytest.importorskip("duckdb")
        con = _spatial_connection(duckdb)
        # US: a land polygon AND a maritime (EEZ) polygon spanning the whole
        # territory (incl. the landmass). CA: a land polygon with NULL is_land.
        src = _write_divisions_fixture(
            con,
            tmp_path,
            [
                ("POLYGON((0 0,0 1,1 1,1 0,0 0))", (0.0, 1.0, 0.0, 1.0), "US", "country", True),
                (
                    "POLYGON((-5 -5,-5 5,5 5,5 -5,-5 -5))",
                    (-5.0, 5.0, -5.0, 5.0),
                    "US",
                    "country",
                    False,
                ),
                (
                    "POLYGON((10 10,10 11,11 11,11 10,10 10))",
                    (10.0, 11.0, 10.0, 11.0),
                    "CA",
                    "country",
                    None,
                ),
            ],
        )

        query = OvertureAdminDataset()._build_level_cache_query("country", str(src))
        con.execute(f"CREATE TABLE land AS SELECT * FROM ({query})")

        # Maritime US polygon dropped; NULL-is_land CA land polygon kept.
        countries = [
            r[0] for r in con.execute("SELECT country FROM land ORDER BY country").fetchall()
        ]
        assert countries == ["CA", "US"]

        pt = "ST_GeomFromText('POINT(0.5 0.5)')"
        n_land = con.execute(
            f"SELECT count(*) FROM land WHERE ST_Intersects(geometry, {pt})"
        ).fetchone()[0]
        assert n_land == 1, "land-only cache must match each feature exactly once"

        # Sanity: against the unfiltered source the point matches both polygons.
        n_all = con.execute(
            f"""
            SELECT count(*) FROM read_parquet('{src}')
            WHERE subtype='country' AND ST_Intersects(geometry, {pt})
            """
        ).fetchone()[0]
        assert n_all == 2


class TestBaseAdminDatasetDefaults:
    """Test base AdminDataset default implementations for backwards compatibility."""

    def test_get_column_transform_default(self):
        """Test that base class returns None by default (no transformation)."""
        # Use CurrentAdminDataset as a concrete implementation
        dataset = CurrentAdminDataset()
        transform = dataset.get_column_transform("country")

        # Default implementation should return None
        assert transform is None

    def test_get_output_column_name_default(self):
        """Test that base class uses dataset prefix by default."""
        # Use CurrentAdminDataset as a concrete implementation
        dataset = CurrentAdminDataset()
        col_name = dataset.get_output_column_name("country")

        # Default implementation should use dataset prefix
        assert col_name == "current_country"

    def test_gaul_uses_default_column_names(self):
        """Test that GAUL dataset uses dataset prefix by default."""
        dataset = GAULAdminDataset()

        # GAUL should use dataset prefix
        assert dataset.get_output_column_name("continent") == "gaul_continent"
        assert dataset.get_output_column_name("country") == "gaul_country"
        assert dataset.get_output_column_name("department") == "gaul_department"

    def test_gaul_no_column_transforms(self):
        """Test that GAUL dataset has no column transformations."""
        dataset = GAULAdminDataset()

        # GAUL should not transform columns
        assert dataset.get_column_transform("continent") is None
        assert dataset.get_column_transform("country") is None
        assert dataset.get_column_transform("department") is None


class TestAdminDatasetFactory:
    """Test AdminDatasetFactory."""

    def test_get_available_datasets(self):
        datasets = AdminDatasetFactory.get_available_datasets()
        assert "current" in datasets
        assert "gaul" in datasets
        assert "overture" in datasets

    def test_create_current_dataset(self):
        dataset = AdminDatasetFactory.create("current")
        assert isinstance(dataset, CurrentAdminDataset)

    def test_create_gaul_dataset(self):
        dataset = AdminDatasetFactory.create("gaul")
        assert isinstance(dataset, GAULAdminDataset)

    def test_create_overture_dataset(self):
        dataset = AdminDatasetFactory.create("overture")
        assert isinstance(dataset, OvertureAdminDataset)

    def test_create_with_custom_source(self):
        dataset = AdminDatasetFactory.create("current", source_path="/custom/path.parquet")
        assert dataset.source_path == "/custom/path.parquet"
        assert dataset.get_source() == "/custom/path.parquet"

    def test_create_with_verbose(self):
        dataset = AdminDatasetFactory.create("current", verbose=True)
        assert dataset.verbose is True

    def test_create_invalid_dataset(self):
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError) as exc_info:
            AdminDatasetFactory.create("invalid_dataset")
        assert "unknown" in str(exc_info.value).lower()
        assert "invalid_dataset" in str(exc_info.value)


class TestAdminDatasetIntegration:
    """Integration tests for admin datasets."""

    def test_current_dataset_full_workflow(self):
        """Test typical workflow with current dataset."""
        dataset = AdminDatasetFactory.create("current")

        # Validate levels
        dataset.validate_levels(["country"])

        # Get partition columns
        columns = dataset.get_partition_columns(["country"])
        assert columns == ["country"]

        # Check remote status
        assert dataset.is_remote() is True

    def test_gaul_dataset_hierarchical_workflow(self):
        """Test hierarchical workflow with GAUL dataset."""
        dataset = AdminDatasetFactory.create("gaul")

        # Test single level
        dataset.validate_levels(["continent"])
        columns = dataset.get_partition_columns(["continent"])
        assert columns == ["continent"]

        # Test two levels
        dataset.validate_levels(["continent", "country"])
        columns = dataset.get_partition_columns(["continent", "country"])
        assert columns == ["continent", "gaul0_name"]

        # Test all three levels
        dataset.validate_levels(["continent", "country", "department"])
        columns = dataset.get_partition_columns(["continent", "country", "department"])
        assert columns == ["continent", "gaul0_name", "gaul2_name"]

    def test_custom_source_override(self):
        """Test using custom source instead of default."""
        custom_path = "/my/custom/gaul.parquet"
        dataset = AdminDatasetFactory.create("gaul", source_path=custom_path)

        assert dataset.get_source() == custom_path
        assert dataset.is_remote() is False

    def test_remote_url_detection(self):
        """Test detection of remote vs local sources."""
        # HTTP URL
        dataset = AdminDatasetFactory.create("gaul", source_path="http://example.com/data.parquet")
        assert dataset.is_remote() is True

        # HTTPS URL
        dataset = AdminDatasetFactory.create("gaul", source_path="https://example.com/data.parquet")
        assert dataset.is_remote() is True

        # S3 URL
        dataset = AdminDatasetFactory.create("gaul", source_path="s3://bucket/data.parquet")
        assert dataset.is_remote() is True

        # Local path
        dataset = AdminDatasetFactory.create("gaul", source_path="/local/path/data.parquet")
        assert dataset.is_remote() is False


class TestAdminDatasetPrefixFunctionality:
    """Test prefix functionality for column naming."""

    def test_get_default_prefix_gaul(self):
        """Test that GAUL dataset returns 'gaul' as default prefix."""
        dataset = GAULAdminDataset()
        prefix = dataset.get_default_prefix()
        assert prefix == "gaul"

    def test_get_default_prefix_overture(self):
        """Test that Overture dataset returns 'overture' as default prefix."""
        dataset = OvertureAdminDataset()
        prefix = dataset.get_default_prefix()
        assert prefix == "overture"

    def test_get_default_prefix_current(self):
        """Test that Current dataset returns 'current' as default prefix."""
        dataset = CurrentAdminDataset()
        prefix = dataset.get_default_prefix()
        assert prefix == "current"

    def test_get_output_column_name_with_default_prefix_gaul(self):
        """Test GAUL column naming with default prefix (None)."""
        dataset = GAULAdminDataset()

        # With no prefix specified, should use default prefix (gaul)
        assert dataset.get_output_column_name("continent", prefix=None) == "gaul_continent"
        assert dataset.get_output_column_name("country", prefix=None) == "gaul_country"
        assert dataset.get_output_column_name("department", prefix=None) == "gaul_department"

    def test_get_output_column_name_with_default_prefix_overture(self):
        """Test Overture column naming with default prefix (None)."""
        dataset = OvertureAdminDataset()

        # With no prefix specified, should use default prefix (overture)
        assert dataset.get_output_column_name("country", prefix=None) == "overture_country"
        assert dataset.get_output_column_name("region", prefix=None) == "overture_region"

    def test_get_output_column_name_with_admin_prefix(self):
        """Test column naming with admin prefix (colon format)."""
        dataset = GAULAdminDataset()

        # With admin prefix, should use colon format
        assert dataset.get_output_column_name("continent", prefix="admin") == "admin:continent"
        assert dataset.get_output_column_name("country", prefix="admin") == "admin:country"
        assert dataset.get_output_column_name("department", prefix="admin") == "admin:department"

    def test_get_output_column_name_with_custom_prefix(self):
        """Test column naming with custom prefix."""
        dataset = GAULAdminDataset()

        # With custom prefix, should use underscore format
        assert (
            dataset.get_output_column_name("continent", prefix="mycustom") == "mycustom_continent"
        )
        assert dataset.get_output_column_name("country", prefix="mycustom") == "mycustom_country"
        assert (
            dataset.get_output_column_name("department", prefix="mycustom") == "mycustom_department"
        )

    def test_prefix_prevents_duplicate_columns(self):
        """Test that different prefixes create unique column names."""
        gaul_dataset = GAULAdminDataset()
        overture_dataset = OvertureAdminDataset()

        # Same level, different datasets, default prefixes
        gaul_country = gaul_dataset.get_output_column_name("country", prefix=None)
        overture_country = overture_dataset.get_output_column_name("country", prefix=None)

        # Should be different
        assert gaul_country == "gaul_country"
        assert overture_country == "overture_country"
        assert gaul_country != overture_country

    def test_multiple_custom_prefixes(self):
        """Test using multiple custom prefixes on same dataset."""
        dataset = GAULAdminDataset()

        # Different prefixes should create different column names
        prefix1_col = dataset.get_output_column_name("country", prefix="source1")
        prefix2_col = dataset.get_output_column_name("country", prefix="source2")

        assert prefix1_col == "source1_country"
        assert prefix2_col == "source2_country"
        assert prefix1_col != prefix2_col


# =============================================================================
# CACHING TESTS - Issue #43
# =============================================================================


class TestAdminDatasetVersion:
    """Test VERSION attribute and get_version() method on datasets."""

    def test_gaul_dataset_has_version(self):
        """Test that GAUL dataset has a VERSION class attribute."""
        assert hasattr(GAULAdminDataset, "VERSION")
        assert isinstance(GAULAdminDataset.VERSION, str)
        # Version should be in date format like "2024-12-19"
        assert len(GAULAdminDataset.VERSION.split("-")) >= 2

    def test_overture_dataset_has_version(self):
        """Test that Overture dataset has a VERSION class attribute."""
        assert hasattr(OvertureAdminDataset, "VERSION")
        assert isinstance(OvertureAdminDataset.VERSION, str)
        # VERSION is the fallback release; should match date format like "2026-05-20.0"
        assert "." in OvertureAdminDataset.VERSION and "-" in OvertureAdminDataset.VERSION

    def test_gaul_get_version(self):
        """Test get_version() method on GAUL dataset."""
        dataset = GAULAdminDataset()
        version = dataset.get_version()
        assert version == GAULAdminDataset.VERSION

    def test_overture_get_version(self):
        """Test get_version() method on Overture dataset resolves dynamically."""
        from unittest.mock import patch

        import geoparquet_io.core.overture as overture_mod

        original_cached = overture_mod._cached_release
        try:
            dataset = OvertureAdminDataset()
            with patch("geoparquet_io.core.overture._fetch_latest_release") as mock:
                mock.return_value = "2099-01-01.0"
                overture_mod._cached_release = None
                version = dataset.get_version()
                assert version == "2099-01-01.0"
        finally:
            overture_mod._cached_release = original_cached

    def test_overture_get_version_rejects_malformed(self):
        """Test get_version() falls back when release format is malformed."""
        from unittest.mock import patch

        import geoparquet_io.core.overture as overture_mod

        original_cached = overture_mod._cached_release
        try:
            dataset = OvertureAdminDataset()
            with patch("geoparquet_io.core.overture._fetch_latest_release") as mock:
                mock.return_value = "malicious-input; DROP TABLE"
                overture_mod._cached_release = None
                version = dataset.get_version()
                assert version == OvertureAdminDataset.VERSION
        finally:
            overture_mod._cached_release = original_cached

    def test_current_dataset_has_version(self):
        """Test that Current dataset has a VERSION attribute."""
        assert hasattr(CurrentAdminDataset, "VERSION")
        dataset = CurrentAdminDataset()
        version = dataset.get_version()
        assert isinstance(version, str)


class TestGetCacheDir:
    """Test get_cache_dir() function."""

    def test_returns_path_object(self):
        """Test that get_cache_dir returns a Path object."""
        cache_dir = get_cache_dir()
        assert isinstance(cache_dir, Path)

    def test_cache_dir_in_user_home(self):
        """Test that cache directory is in user's home directory."""
        cache_dir = get_cache_dir()
        home = Path.home()
        assert str(cache_dir).startswith(str(home))

    def test_cache_dir_path_structure(self):
        """Test expected cache directory path structure."""
        cache_dir = get_cache_dir()
        # Should be ~/.geoparquet-io/cache/admin/
        assert cache_dir.parts[-1] == "admin"
        assert cache_dir.parts[-2] == "cache"
        assert cache_dir.parts[-3] == ".geoparquet-io"

    def test_cache_dir_respects_xdg_cache_home(self):
        """Test that XDG_CACHE_HOME is respected if set."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"XDG_CACHE_HOME": tmpdir}):
                # Note: This tests if we decide to support XDG - for now we use ~/.geoparquet-io
                cache_dir = get_cache_dir()
                # Default implementation uses ~/.geoparquet-io regardless of XDG
                # This test documents the current behavior
                assert ".geoparquet-io" in str(cache_dir)


class TestGetCachedPath:
    """Test get_cached_path() function for generating cache file paths."""

    def test_gaul_cached_path(self):
        """Test cached path for GAUL dataset."""
        dataset = GAULAdminDataset()
        cached_path = get_cached_path(dataset)

        assert isinstance(cached_path, Path)
        assert cached_path.suffix == ".parquet"
        assert "gaul" in cached_path.name.lower()
        assert dataset.get_version() in cached_path.name

    def test_overture_cached_path(self):
        """Test cached path for Overture dataset."""
        dataset = OvertureAdminDataset()
        cached_path = get_cached_path(dataset)

        assert isinstance(cached_path, Path)
        assert cached_path.suffix == ".parquet"
        assert "overture" in cached_path.name.lower()
        assert dataset.get_version() in cached_path.name

    def test_cached_path_format(self):
        """Test that cached path follows the expected format: {dataset}-{version}.parquet"""
        dataset = GAULAdminDataset()
        cached_path = get_cached_path(dataset)

        # Path should be in format: gaul-{version}.parquet
        expected_name = f"gaul-{dataset.get_version()}.parquet"
        assert cached_path.name == expected_name

    def test_cached_path_is_in_cache_dir(self):
        """Test that cached path is inside the cache directory."""
        dataset = GAULAdminDataset()
        cached_path = get_cached_path(dataset)
        cache_dir = get_cache_dir()

        assert cached_path.parent == cache_dir


class TestCheckCacheAge:
    """Test check_cache_age() function for age warnings."""

    def test_new_cache_no_warning(self):
        """Test that new cache files don't trigger warning."""
        # Create temp file and close it immediately (Windows compatibility)
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        cache_file = Path(f.name)
        f.close()
        try:
            # File was just created, should be recent
            warning = check_cache_age(cache_file)
            assert warning is None
        finally:
            cache_file.unlink()

    def test_old_cache_triggers_warning(self):
        """Test that cache files older than 6 months trigger warning."""
        # Create temp file and close it immediately (Windows compatibility)
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        cache_file = Path(f.name)
        f.close()
        try:
            # Set file modification time to 7 months ago
            seven_months_ago = time.time() - (7 * 30 * 24 * 60 * 60)
            os.utime(cache_file, (seven_months_ago, seven_months_ago))

            warning = check_cache_age(cache_file)
            assert warning is not None
            assert "6 months" in warning or "old" in warning.lower()
        finally:
            cache_file.unlink()

    def test_six_month_boundary(self):
        """Test behavior at exactly 6 months."""
        # Create temp file and close it immediately (Windows compatibility)
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        cache_file = Path(f.name)
        f.close()
        try:
            # Set file modification time to exactly 6 months ago (should trigger)
            six_months_ago = time.time() - (6 * 30 * 24 * 60 * 60)
            os.utime(cache_file, (six_months_ago, six_months_ago))

            warning = check_cache_age(cache_file)
            assert warning is not None
        finally:
            cache_file.unlink()

    def test_nonexistent_file_returns_none(self):
        """Test that non-existent file returns None (no warning)."""
        fake_path = Path("/nonexistent/path/to/cache.parquet")
        warning = check_cache_age(fake_path)
        assert warning is None


class TestClearCache:
    """Test clear_cache() function."""

    def test_clear_cache_empty_directory(self):
        """Test clearing an empty cache directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = Path(tmpdir)
                result = clear_cache(confirm=True)

                # Should return info about clearing
                assert result is not None
                assert result["files_deleted"] == 0
                assert result["bytes_freed"] == 0

    def test_clear_cache_with_files(self):
        """Test clearing cache with actual files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            # Create some fake cache files
            (cache_dir / "gaul-2024-12-19.parquet").write_bytes(b"fake data 1")
            (cache_dir / "overture-2025-10-22.0.parquet").write_bytes(b"fake data 2" * 100)

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                result = clear_cache(confirm=True)

                assert result["files_deleted"] == 2
                assert result["bytes_freed"] > 0
                # Check files are actually deleted
                assert not (cache_dir / "gaul-2024-12-19.parquet").exists()
                assert not (cache_dir / "overture-2025-10-22.0.parquet").exists()

    def test_clear_cache_without_confirm_does_nothing(self):
        """Test that clear_cache without confirm does nothing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            (cache_dir / "gaul-2024-12-19.parquet").write_bytes(b"fake data")

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                result = clear_cache(confirm=False)

                # Should return without deleting
                assert result is None or result.get("cancelled", False)
                # File should still exist
                assert (cache_dir / "gaul-2024-12-19.parquet").exists()

    def test_clear_cache_reports_size(self):
        """Test that clear_cache reports the total size freed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            # Create file with known size
            data = b"x" * 1024  # 1KB
            (cache_dir / "test.parquet").write_bytes(data)

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                result = clear_cache(confirm=True)

                assert result["bytes_freed"] == 1024

    def test_clear_cache_only_deletes_parquet_files(self):
        """Test that clear_cache only deletes .parquet files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            (cache_dir / "gaul-2024.parquet").write_bytes(b"parquet")
            (cache_dir / "readme.txt").write_text("do not delete")
            (cache_dir / ".gitkeep").write_text("")

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                result = clear_cache(confirm=True)

                assert result["files_deleted"] == 1
                # Non-parquet files should remain
                assert (cache_dir / "readme.txt").exists()
                assert (cache_dir / ".gitkeep").exists()

    def test_clear_cache_removes_orphaned_spill_directories(self):
        """The worst orphan lands here, in ``$HOME``, where no tmp-reaper runs.

        ``gpio add admin-divisions`` spills onto the admin cache volume, so an
        interrupted Overture run leaves a multi-GB ``gpio-spill-*`` directory in
        ``~/.geoparquet-io/cache/admin/``. Globbing ``*.parquet`` reported it as
        nothing and deleted nothing.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            orphan = cache_dir / f"gpio-spill-{_dead_pid()}-abcdef123456"
            orphan.mkdir()
            (orphan / "duckdb_temp_storage_S192K-0.tmp").write_bytes(b"x" * 2048)

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                result = clear_cache(confirm=True)

            assert not orphan.exists()
            assert result["spill_dirs_deleted"] == 1
            assert result["spill_bytes_freed"] == 2048
            # Cached-dataset accounting is unchanged: a spill directory is not a file.
            assert result["files_deleted"] == 0
            assert result["bytes_freed"] == 0

    def test_clear_cache_on_an_absent_directory_reports_the_same_shape(self):
        """Callers read four keys off the result; a no-op must still carry them."""
        with tempfile.TemporaryDirectory() as tmpdir:
            missing = Path(tmpdir) / "never-created"
            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = missing
                result = clear_cache(confirm=True)

            assert result == {
                "files_deleted": 0,
                "bytes_freed": 0,
                "spill_dirs_deleted": 0,
                "spill_bytes_freed": 0,
            }

    def test_clear_cache_leaves_a_running_process_spill_directory(self):
        """A concurrent run's scratch space is not ours to delete."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            live = cache_dir / f"gpio-spill-{os.getpid()}-abcdef123456"
            live.mkdir()

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                result = clear_cache(confirm=True)

            assert live.is_dir()
            assert result["spill_dirs_deleted"] == 0


class TestGetOrCacheDataset:
    """Test get_or_cache_dataset() function - the main caching logic."""

    def test_returns_cached_path_when_exists(self):
        """Test that existing cache is used without re-download."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()
            expected_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"
            expected_path.write_bytes(b"cached data")

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                result = get_or_cache_dataset(dataset)

                # Result is a string path, expected_path is Path
                assert result == str(expected_path)
                # Content should be unchanged (not re-downloaded)
                assert expected_path.read_bytes() == b"cached data"

    def test_cache_miss_triggers_download(self):
        """Test that cache miss triggers download and caching."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch.object(dataset, "_download_to_cache") as mock_download:
                    expected_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"
                    mock_download.return_value = expected_path

                    get_or_cache_dataset(dataset)

                    mock_download.assert_called_once()

    def test_creates_cache_directory_if_missing(self):
        """Test that cache directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache" / "admin"  # Doesn't exist yet
            dataset = GAULAdminDataset()

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch.object(dataset, "_download_to_cache") as mock_download:
                    expected_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"

                    # Simulate download creating the file
                    def create_file():
                        cache_dir.mkdir(parents=True, exist_ok=True)
                        expected_path.write_bytes(b"data")
                        return expected_path

                    mock_download.side_effect = create_file

                    get_or_cache_dataset(dataset)

                    # Directory should be created
                    assert cache_dir.exists()

    def test_no_cache_flag_skips_cache(self):
        """Test that no_cache=True skips caching and returns remote URL."""
        dataset = GAULAdminDataset()

        result = get_or_cache_dataset(dataset, no_cache=True)

        # Should return the remote source URL directly
        assert result == dataset.get_default_source()

    def test_custom_source_not_cached(self):
        """Test that custom source files are not cached."""
        custom_path = "/my/custom/data.parquet"
        dataset = GAULAdminDataset(source_path=custom_path)

        result = get_or_cache_dataset(dataset)

        # Should return custom path as-is
        assert result == custom_path

    def test_local_source_not_cached(self):
        """Test that local files are not cached."""
        # Create temp file and close it immediately (Windows compatibility)
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        local_path = f.name
        f.close()
        try:
            dataset = GAULAdminDataset(source_path=local_path)
            result = get_or_cache_dataset(dataset)

            # Should return local path as-is
            assert result == local_path
        finally:
            os.unlink(local_path)

    def test_age_warning_on_old_cache(self):
        """Test that old cache triggers age warning."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()
            cached_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"
            cached_path.write_bytes(b"old cached data")

            # Set file to 7 months old
            seven_months_ago = time.time() - (7 * 30 * 24 * 60 * 60)
            os.utime(cached_path, (seven_months_ago, seven_months_ago))

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch("geoparquet_io.core.admin_datasets.warn") as mock_warn:
                    get_or_cache_dataset(dataset)

                    # Warning should have been issued
                    mock_warn.assert_called()
                    warning_msg = str(mock_warn.call_args)
                    assert "old" in warning_msg.lower() or "month" in warning_msg.lower()


class TestCacheMessaging:
    """Test user messaging during cache operations."""

    def test_first_run_message_on_cache_miss(self):
        """Test that first-run notification is shown when caching."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch.object(dataset, "_download_to_cache") as mock_download:
                    expected_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"
                    mock_download.return_value = expected_path

                    with patch("geoparquet_io.core.admin_datasets.info") as mock_info:
                        get_or_cache_dataset(dataset)

                        # Should show message about caching
                        mock_info.assert_called()
                        info_msg = " ".join(str(c) for c in mock_info.call_args_list)
                        assert "cache" in info_msg.lower() or "download" in info_msg.lower()

    def test_cache_hit_message(self):
        """Test that cache hit shows appropriate message."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()
            cached_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"
            cached_path.write_bytes(b"cached data")

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch("geoparquet_io.core.admin_datasets.debug") as mock_debug:
                    get_or_cache_dataset(dataset, verbose=True)

                    # Should show cache hit message
                    mock_debug.assert_called()


class TestCacheCLIIntegration:
    """Test CLI flag integration for caching."""

    def test_no_cache_flag_exists(self):
        """Test that --no-cache flag will be available on add admin-divisions."""
        # This test documents the expected CLI interface
        # Implementation will add the flag to main.py
        pass  # Placeholder - actual CLI test in test_cli.py

    def test_clear_cache_flag_exists(self):
        """Test that --clear-cache flag will be available on add admin-divisions."""
        # This test documents the expected CLI interface
        pass  # Placeholder - actual CLI test in test_cli.py


class TestCacheEdgeCases:
    """Test edge cases and error handling in caching."""

    def test_handles_permission_error_on_cache_dir(self):
        """Test graceful handling when cache directory cannot be created."""
        dataset = GAULAdminDataset()

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            # Don't create the cached file - simulate cache miss

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir

                # Make mkdir raise PermissionError
                def mock_mkdir_error(self, *args, **kwargs):
                    raise PermissionError("Cannot create directory")

                with patch.object(Path, "mkdir", mock_mkdir_error):
                    # Should fall back to remote source
                    result = get_or_cache_dataset(dataset)
                    assert result == dataset.get_default_source()

    def test_handles_download_failure(self):
        """Test graceful handling when download fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch.object(dataset, "_download_to_cache") as mock_download:
                    mock_download.side_effect = Exception("Network error")

                    # Should fall back to remote source
                    result = get_or_cache_dataset(dataset)
                    assert result == dataset.get_default_source()

    def test_handles_corrupted_cache_file(self):
        """Test handling of corrupted cache file (0 bytes)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()
            cached_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"
            cached_path.write_bytes(b"")  # Empty/corrupted file

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch.object(dataset, "_download_to_cache") as mock_download:
                    fresh_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"

                    def redownload():
                        fresh_path.write_bytes(b"fresh data")
                        return fresh_path

                    mock_download.side_effect = redownload

                    get_or_cache_dataset(dataset)

                    # Should re-download when cache is corrupted
                    mock_download.assert_called_once()

    def test_concurrent_cache_access(self):
        """Test that concurrent access to cache is handled safely."""
        # This is a placeholder for thread-safety testing
        # The implementation should use atomic file operations
        pass


class TestDownloadToCache:
    """Test AdminDataset._download_to_cache() -- the default DuckDB download path.

    ``get_default_source`` is monkeypatched to a local test fixture, so this
    exercises the read_parquet/COPY SQL (built via sql_path (#802)) without any
    network access.
    """

    def test_download_to_cache_no_read_options(self, tmp_path):
        """No per-dataset read_parquet options -> the plain read_parquet branch."""
        dataset = GAULAdminDataset()
        local_source = str(TEST_DATA_DIR / "buildings_test.parquet")
        cache_path = tmp_path / "cache" / "gaul-test.parquet"

        with patch.object(dataset, "get_default_source", return_value=local_source):
            result = dataset._download_to_cache(cache_path)

        assert result == cache_path
        assert cache_path.exists()

        con = get_duckdb_connection(load_spatial=True)
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {sql_path(str(cache_path))}").fetchone()[0]
        finally:
            con.close()
        assert count > 0

    def test_download_to_cache_with_read_options(self, tmp_path):
        """Non-empty read_parquet options (e.g. Overture's hive_partitioning) build the options branch."""
        dataset = GAULAdminDataset()
        local_source = str(TEST_DATA_DIR / "buildings_test.parquet")
        cache_path = tmp_path / "cache2" / "gaul-test2.parquet"

        with (
            patch.object(dataset, "get_default_source", return_value=local_source),
            patch.object(
                dataset, "get_read_parquet_options", return_value={"hive_partitioning": 1}
            ),
        ):
            result = dataset._download_to_cache(cache_path)

        assert result == cache_path
        assert cache_path.exists()


class TestPrepareDataSource:
    """Test AdminDataset.prepare_data_source() -- returns a SQL table reference (#802)."""

    def test_remote_source_returns_sql_path_literal(self):
        """A remote (s3://) source is quoted/escaped via sql_path, not verified on disk."""
        dataset = GAULAdminDataset(verbose=True)
        con = get_duckdb_connection()
        try:
            result = dataset.prepare_data_source(con)
        finally:
            con.close()

        assert result == sql_path(dataset.get_default_source())

    def test_local_source_returns_sql_path_literal(self, tmp_path):
        """A local source is verified to exist, then also returned via sql_path."""
        local_file = tmp_path / "local_admin.parquet"
        local_file.write_bytes(b"not a real parquet file, just needs to exist")
        dataset = GAULAdminDataset(source_path=str(local_file))
        con = get_duckdb_connection()
        try:
            result = dataset.prepare_data_source(con)
        finally:
            con.close()

        assert result == sql_path(str(local_file))

    def test_local_source_missing_raises(self):
        """A local source that does not exist raises FileNotFoundGeoParquetError."""
        from geoparquet_io.core.exceptions import FileNotFoundGeoParquetError

        dataset = GAULAdminDataset(source_path="/nonexistent/admin-source.parquet")
        con = get_duckdb_connection()
        try:
            with pytest.raises(FileNotFoundGeoParquetError):
                dataset.prepare_data_source(con)
        finally:
            con.close()


class TestPerLevelJoinRowCountGuard:
    """Runtime guard for the join-multiplication class of bugs.

    The per-level caches are built to be non-overlapping so a plain LEFT JOIN
    preserves the row count (see ``_build_level_cache_query``). When that
    invariant breaks — a maritime polygon slipping back in, an overlapping
    dependency, a custom ``--admin-source`` — the failure is silent duplication.
    A cheap count comparison after the join names it instead.
    """

    def test_warns_when_the_join_changed_the_row_count(self, monkeypatch):
        from geoparquet_io.core.add import admin_divisions

        messages = []
        monkeypatch.setattr(admin_divisions, "warn", messages.append)

        admin_divisions._warn_if_row_count_changed(1000, 2600)

        assert len(messages) == 1
        assert "1000" in messages[0] and "2600" in messages[0]

    def test_silent_when_the_row_count_is_preserved(self, monkeypatch):
        from geoparquet_io.core.add import admin_divisions

        messages = []
        monkeypatch.setattr(admin_divisions, "warn", messages.append)

        admin_divisions._warn_if_row_count_changed(1000, 1000)
        admin_divisions._warn_if_row_count_changed(None, 1000)
        admin_divisions._warn_if_row_count_changed(1000, None)

        assert messages == []

    def test_count_rows_returns_the_count_for_a_readable_source(self, tmp_path):
        """The happy path: a metadata-only ``COUNT(*)`` against a real file."""
        duckdb = pytest.importorskip("duckdb")
        from geoparquet_io.core.add.admin_divisions import _count_rows

        con = duckdb.connect()
        src = tmp_path / "rows.parquet"
        con.execute(f"COPY (SELECT * FROM range(7) AS t(i)) TO '{src}' (FORMAT PARQUET)")

        assert _count_rows(con, str(src)) == 7

    def test_count_rows_returns_the_count_for_a_table_ref(self, tmp_path):
        """``is_table_ref=True`` emits the source bare, not single-quoted, so a
        chained per-level temp table (not a file path) can be counted too."""
        duckdb = pytest.importorskip("duckdb")
        from geoparquet_io.core.add.admin_divisions import _count_rows

        con = duckdb.connect()
        con.execute("CREATE TEMP TABLE _gpio_admin_step_0 AS SELECT * FROM range(3) AS t(i)")

        assert _count_rows(con, "_gpio_admin_step_0", is_table_ref=True) == 3

    def test_count_rows_returns_none_when_the_source_cannot_be_read(self, tmp_path):
        """A missing/unreadable source must not raise — the row-count guard is
        a best-effort diagnostic, not something that should break the join."""
        duckdb = pytest.importorskip("duckdb")
        from geoparquet_io.core.add.admin_divisions import _count_rows

        con = duckdb.connect()

        assert _count_rows(con, str(tmp_path / "does-not-exist.parquet")) is None


def _write_country_admin_fixture(con, tmp_path, polygons, name="admin.parquet"):
    """A minimal Overture-country-shaped admin fixture: subtype/country/bbox.

    ``polygons`` are ``(wkt, country_code)`` pairs.
    """
    selects = [
        f"SELECT 'country' AS subtype, '{country}' AS country, ST_GeomFromText('{wkt}') AS geometry"
        for wkt, country in polygons
    ]
    src = tmp_path / name
    con.execute(
        f"""
        COPY (
            SELECT subtype, country, geometry,
                {{'xmin': ST_XMin(geometry), 'xmax': ST_XMax(geometry),
                  'ymin': ST_YMin(geometry), 'ymax': ST_YMax(geometry)}} AS bbox
            FROM ({" UNION ALL ".join(selects)})
        ) TO '{src}' (FORMAT PARQUET)
        """
    )
    return src


def _write_points_fixture(con, tmp_path, points, name="input.parquet"):
    """A plain points input fixture: one ``geometry`` column, no bbox/metadata."""
    selects = [f"SELECT ST_GeomFromText('POINT({x} {y})') AS geometry" for x, y in points]
    src = tmp_path / name
    con.execute(f"COPY ({' UNION ALL '.join(selects)}) TO '{src}' (FORMAT PARQUET)")
    return src


class TestPerLevelJoinRowCountGuardEndToEnd:
    """``add admin-divisions`` runs the row-count guard on the real per-level
    join path (``_execute_per_level_joins``), not just the standalone helper.

    Uses a local, offline ``--admin-source`` (Overture-shaped, per #819) so no
    network access or real cache is involved.
    """

    def test_row_count_preserved_stays_silent(self, tmp_path, monkeypatch):
        import duckdb

        from geoparquet_io.core.add import admin_divisions
        from geoparquet_io.core.add.admin_divisions import add_admin_divisions_multi

        messages = []
        monkeypatch.setattr(admin_divisions, "warn", messages.append)

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        admin_file = _write_country_admin_fixture(
            con,
            tmp_path,
            [
                ("POLYGON((0 0,0 1,1 1,1 0,0 0))", "AA"),
                ("POLYGON((10 10,10 11,11 11,11 10,10 10))", "BB"),
            ],
        )
        input_file = _write_points_fixture(con, tmp_path, [(0.5, 0.5), (10.5, 10.5)])
        con.close()

        out = str(tmp_path / "out.parquet")
        add_admin_divisions_multi(
            input_parquet=str(input_file),
            output_parquet=out,
            dataset_name="overture",
            levels=["country"],
            dataset_source=str(admin_file),
            verbose=False,
        )

        con = duckdb.connect()
        assert con.execute(f"SELECT COUNT(*) FROM '{out}'").fetchone()[0] == 2
        # Other warnings (e.g. bbox-optimization advice) may fire; the row-count
        # guard itself must stay silent.
        assert [m for m in messages if "Admin join" in m] == []

    def test_overlapping_admin_polygons_duplicate_rows_and_warn(self, tmp_path, monkeypatch):
        """Two overlapping country polygons make one input point match twice —
        exactly the join-multiplication bug the guard exists to name.
        """
        import duckdb

        from geoparquet_io.core.add import admin_divisions
        from geoparquet_io.core.add.admin_divisions import add_admin_divisions_multi

        messages = []
        monkeypatch.setattr(admin_divisions, "warn", messages.append)

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        admin_file = _write_country_admin_fixture(
            con,
            tmp_path,
            [
                # Both polygons cover (0.5, 0.5) - an overlapping-territory bug.
                ("POLYGON((0 0,0 2,2 2,2 0,0 0))", "AA"),
                ("POLYGON((0 0,0 2,2 2,2 0,0 0))", "BB"),
                ("POLYGON((10 10,10 11,11 11,11 10,10 10))", "CC"),
            ],
        )
        input_file = _write_points_fixture(con, tmp_path, [(0.5, 0.5), (10.5, 10.5)])
        con.close()

        out = str(tmp_path / "out.parquet")
        add_admin_divisions_multi(
            input_parquet=str(input_file),
            output_parquet=out,
            dataset_name="overture",
            levels=["country"],
            dataset_source=str(admin_file),
            verbose=False,
        )

        con = duckdb.connect()
        # Duplicated: 1 input point matched 2 polygons, the other matched 1.
        assert con.execute(f"SELECT COUNT(*) FROM '{out}'").fetchone()[0] == 3
        guard_messages = [m for m in messages if "Admin join" in m]
        assert len(guard_messages) == 1
        assert "duplicated" in guard_messages[0]
        assert "2 input features" in guard_messages[0]
        assert "3 in the" in guard_messages[0]


class TestOvertureLevelPredicates:
    """The predicate helpers' "unconfigured level" branches return None, so the
    ``--no-cache`` SQL builders fall back cleanly instead of emitting a bogus
    ``WHERE``."""

    def test_level_predicate_is_none_for_an_unconfigured_level(self):
        from geoparquet_io.core.admin_datasets import _overture_level_predicate

        assert _overture_level_predicate("locality") is None

    def test_levels_predicate_is_none_when_no_level_is_configured(self):
        from geoparquet_io.core.admin_datasets import _overture_levels_predicate

        assert _overture_levels_predicate(["locality"]) is None
        assert _overture_levels_predicate([]) is None


class TestStaleCacheCleanupResilience:
    """Cache pruning is best-effort housekeeping: a file we cannot delete (say,
    held open on Windows) must not fail the run."""

    def test_unlink_oserror_is_swallowed(self, tmp_path, monkeypatch):
        from geoparquet_io.core.admin_datasets import _remove_stale_level_caches

        version = "2026-07-22.0"
        current = tmp_path / f"overture-{version}-country-dependency-land.parquet"
        stubborn = tmp_path / f"overture-{version}-country-land.parquet"
        current.write_bytes(b"stub")
        stubborn.write_bytes(b"stub")

        original_unlink = Path.unlink

        def failing_unlink(self, *args, **kwargs):
            if self == stubborn:
                raise OSError("file is locked")
            return original_unlink(self, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", failing_unlink)

        # Must not raise.
        _remove_stale_level_caches(tmp_path, version, "country", current)

        assert current.exists()
        assert stubborn.exists(), "the undeletable file is simply left behind"


class TestDownloadPerLevelCachesHousekeeping:
    """``_download_per_level_caches`` prunes superseded caches for every level
    before checking freshness, and skips the download when each level's cache
    is already present — fully offline via a mocked connection."""

    def test_prunes_stale_caches_and_skips_fresh_levels(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock

        from geoparquet_io.core import admin_datasets
        from geoparquet_io.core.admin_datasets import OvertureAdminDataset

        version = "2026-07-22.0"
        monkeypatch.setattr(admin_datasets, "get_cache_dir", lambda: tmp_path)
        mock_con = MagicMock()
        monkeypatch.setattr(admin_datasets, "get_duckdb_connection", lambda **kwargs: mock_con)

        dataset = OvertureAdminDataset()
        # Keep version/source resolution offline and deterministic.
        monkeypatch.setattr(dataset, "get_version", lambda: version)
        monkeypatch.setattr(dataset, "get_default_source", lambda: "s3://stub/divisions")

        fresh = {
            level: dataset.get_cached_path_for_level(level)
            for level in dataset.get_available_levels()
        }
        for path in fresh.values():
            path.write_bytes(b"stub")
        # Superseded by the "-dependency" cache key (#819); must be pruned.
        stale = tmp_path / f"overture-{version}-country-land.parquet"
        stale.write_bytes(b"stale")

        dataset._download_per_level_caches()

        assert not stale.exists()
        for path in fresh.values():
            assert path.exists()
        mock_con.execute.assert_not_called()
        mock_con.close.assert_called_once()
