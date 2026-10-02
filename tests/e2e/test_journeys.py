"""Ten end-to-end user journeys, run through the installed ``gpio`` binary.

Issue #667, deliverable 4. Each journey chains several real commands the way a
user (or the guides) would, then finishes with ``gpio check all`` plus concrete
assertions -- row conservation, expected columns, CRS, spatial-order metrics --
rather than a bare exit-code check. See ``journey_support`` for why the exit
code alone would be a vacuous oracle.

Lanes (issue table): journeys 1-7 fast, 8 and 10 slow, 9 network. Journey 6
skips without tippecanoe, which also covers the Windows CI leg. The module is
``integration``-marked; see ``test_integration_lane.py`` for how that marker is
kept meaningful in CI.
"""

from __future__ import annotations

import json
import subprocess

import pyarrow.parquet as pq
import pytest

from tests.conftest import safe_rmtree
from tests.e2e.journey_support import (
    BUILDINGS_GEOJSON,
    BUILDINGS_ROWS,
    FIELDS_5070,
    FIELDS_ROWS,
    PLACES,
    PLACES_ROWS,
    assert_check_all,
    bbox_extent,
    columns,
    leaf_files,
    q,
    requires_gpio,
    requires_tippecanoe,
    rows,
    run_gpio,
    run_pipeline,
    spatial_skip_rate,
    total_rows,
)

pytestmark = [pytest.mark.integration, requires_gpio]

# Public GeoParquet used by docs/guide/extract.md; journey 9 reads it remotely.
REMOTE_PARQUET_URL = "https://data.source.coop/fiboa/data/si/si-2024.parquet"


def geo_metadata(path) -> dict | None:
    """Return the parsed GeoParquet ``geo`` key, or None for parquet-geo-only files."""
    metadata = pq.read_schema(path).metadata or {}
    raw = metadata.get(b"geo")
    return json.loads(raw) if raw else None


# ---------------------------------------------------------------------------
# Journey 1 -- GeoJSON in, GeoParquet 2.0 out, GeoJSON back (budget 5s, fast)
# ---------------------------------------------------------------------------
def test_journey_01_geojson_to_geoparquet_and_back(tmp_path):
    """geojson -> convert -> sort hilbert (2.0) -> add bbox -> check all -> geojson."""
    imported = tmp_path / "buildings.parquet"
    sorted_v2 = tmp_path / "buildings_sorted.parquet"
    with_bbox = tmp_path / "buildings_bbox.parquet"
    exported = tmp_path / "buildings_out.geojson"

    run_gpio(["convert", "geoparquet", BUILDINGS_GEOJSON, imported], 1)
    run_gpio(["sort", "hilbert", "--geoparquet-version", "2.0", imported, sorted_v2], 1)
    # convert already wrote a bbox column, so re-adding needs --force. That is
    # documented behaviour ("will inform you and exit successfully"), and it is
    # exactly why the oracle cannot trust exit codes alone.
    run_gpio(["add", "bbox", "--force", sorted_v2, with_bbox], 1)

    report = assert_check_all(with_bbox, 1)
    assert "Version 2.0.0" in report
    assert "native Parquet GEOMETRY/GEOGRAPHY types" in report

    run_gpio(["convert", "geojson", with_bbox, exported], 1)

    # Row and geometry parity across the whole round trip.
    assert rows(imported) == rows(with_bbox) == BUILDINGS_ROWS
    assert geo_metadata(with_bbox)["version"] == "2.0.0"
    # Explicit encoding: gpio writes UTF-8, and these fixtures carry non-ASCII
    # place names that the Windows locale codec (cp1252) cannot decode.
    features = json.loads(exported.read_text(encoding="utf-8"))["features"]
    assert len(features) == BUILDINGS_ROWS
    assert {f["geometry"]["type"] for f in features} == {"Polygon"}


# ---------------------------------------------------------------------------
# Journey 2 -- H3 index then H3 partition (budget 8s, fast)
# ---------------------------------------------------------------------------
def test_journey_02_h3_index_then_partition(tmp_path):
    """parquet -> add h3 -> partition h3 -> check all on every leaf; rows conserved."""
    indexed = tmp_path / "places_h3.parquet"
    partitions = tmp_path / "h3_partitions"

    # Resolution 2 keeps the 766 West-African points in a handful of cells;
    # finer resolutions trip the command's tiny-partition guard. `partition h3`
    # reuses an existing h3_cell column, so index and partition resolutions
    # have to agree.
    run_gpio(["add", "h3", "--resolution", "2", PLACES, indexed], 2)
    assert "h3_cell" in columns(indexed)
    assert rows(indexed) == PLACES_ROWS

    run_gpio(["partition", "h3", "--resolution", "2", indexed, partitions], 2)

    leaves = leaf_files(partitions)
    assert len(leaves) > 1, f"expected several H3 partitions, got {leaves}"
    assert total_rows(partitions) == PLACES_ROWS
    # Every leaf is named after a 15-character H3 cell id.
    assert all(len(leaf.stem) == 15 for leaf in leaves), [leaf.name for leaf in leaves]

    assert_check_all(partitions, 2, all_files=True)
    safe_rmtree(partitions)


# ---------------------------------------------------------------------------
# Journey 3 -- documented piping chains (budget 6s, fast)
# ---------------------------------------------------------------------------
def test_journey_03_documented_piping_chains(tmp_path):
    """Two piping chains from docs/guide/piping.md, run through a real shell.

    The second chain is the documented one, near-verbatim; the first is derived
    rather than quoted -- see the comments on each.
    """
    partitions = tmp_path / "quadkey_partitions"
    multi_index = tmp_path / "multi_index.parquet"

    # DERIVED, not quoted: piping.md has no such example. It documents that
    # `partition string` can read from stdin but only writes to a directory (the
    # "Supported Commands" table, and the "Partition commands" bullet under
    # "Limitations"). This is the smallest chain that exercises that claim, and
    # it is the piped half of the broken chain pinned below, which makes the
    # break attributable to `extract` rather than to `partition string`.
    run_pipeline(
        [
            f"gpio add quadkey {q(PLACES)} -",
            f"gpio partition string --column quadkey --chars 2 - {q(partitions)}",
        ],
        3,
    )
    leaves = leaf_files(partitions)
    assert {leaf.stem for leaf in leaves} == {"03", "12"}
    assert total_rows(partitions) == PLACES_ROWS
    assert_check_all(partitions, 3, all_files=True)

    # piping.md "Add Multiple Spatial Indices" (lines 105-109), reproduced with
    # one change: `--force` on the first stage, because places_test.parquet
    # already carries a bbox column and `add bbox` otherwise exits 0 without
    # emitting anything. Four stages, three of them reading Arrow IPC from stdin.
    run_pipeline(
        [
            f"gpio add bbox --force {q(PLACES)}",
            "gpio add h3 --resolution 9 -",
            "gpio add quadkey -",
            f"gpio sort hilbert - {q(multi_index)}",
        ],
        3,
    )
    assert rows(multi_index) == PLACES_ROWS
    for expected in ("bbox", "h3_cell", "quadkey"):
        assert expected in columns(multi_index)
    assert_check_all(multi_index, 3)
    safe_rmtree(partitions)


def test_journey_03b_extract_into_partition_chain(tmp_path):
    """piping.md 'Spatial Filter and Partition' -- filter, index, partition.

    Was a strict xfail pinning #722: a filtered `extract` invalidates the
    input's `geometry_types`, and only the file-writing path recomputed them, so
    the Arrow IPC stream reached `partition` without the key GeoParquet requires
    and DuckDB refuses to read a file without. The chain died on an unhandled
    `_duckdb.InvalidInputException`. The stream now carries the recomputed stats.
    """
    partitions = tmp_path / "bbox_partitions"
    result = run_pipeline(
        [
            f'gpio extract --bbox "-0.5,10.0,1.0,12.0" {q(PLACES)} -',
            "gpio add quadkey -",
            f"gpio partition string --column quadkey --chars 4 - {q(partitions)}",
        ],
        3,
    )
    assert result.returncode == 0, result.stderr
    assert "Traceback (most recent call last)" not in result.stderr
    assert total_rows(partitions) > 0
    assert_check_all(partitions, 3, all_files=True)


# ---------------------------------------------------------------------------
# Journey 4 -- stdout format bridge (budget 4s, fast)
# ---------------------------------------------------------------------------
def test_journey_04_stdout_format_bridge(tmp_path):
    """Arrow IPC -> `convert geojson` on stdout -> back to GeoParquet; parity."""
    bridge = tmp_path / "bridge.geojson"
    round_tripped = tmp_path / "round_tripped.parquet"

    # Shape taken from piping.md's "Supported Commands" table, which records
    # `convert geojson` as stdin-capable but stdout-only ("No (outputs GeoJSON
    # to stdout)"); the redirect is this test's own, not a quoted example.
    # The redirect is what makes this a *streaming* bridge: it is the GeoJSONSeq
    # record-separator stream that the assertions below check. `convert geojson
    # - out.geojson` also works now (#723) but writes a FeatureCollection, which
    # would not exercise this path; tests/test_pipe_integration.py covers it.
    run_pipeline(
        [
            f"gpio add bbox --force {q(PLACES)} -",
            f"gpio convert geojson - > {q(bridge)}",
        ],
        4,
    )
    stream = bridge.read_text(encoding="utf-8")
    assert "\x1e" in stream, "expected RFC 8142 record separators in the GeoJSONSeq stream"
    assert len([line for line in stream.splitlines() if line.strip()]) == PLACES_ROWS

    run_gpio(["convert", "geoparquet", bridge, round_tripped], 4)
    assert rows(round_tripped) == PLACES_ROWS
    # Attribute columns survive the GeoJSON hop (geometry is renamed by GDAL).
    for expected in ("fsq_place_id", "name", "address"):
        assert expected in columns(round_tripped)
    assert_check_all(round_tripped, 4)


# ---------------------------------------------------------------------------
# Journey 5 -- reprojection and CRS metadata (budget 4s, fast)
# ---------------------------------------------------------------------------
def test_journey_05_reproject_crs_metadata(tmp_path):
    """reproject -> add bbox -> check all, with CRS metadata and round-trip parity."""
    wgs84 = tmp_path / "fields_4326.parquet"
    wgs84_bbox = tmp_path / "fields_4326_bbox.parquet"
    back_to_5070 = tmp_path / "fields_5070.parquet"
    back_bbox = tmp_path / "fields_5070_bbox.parquet"

    source_meta = run_gpio(["inspect", "meta", "--parquet-geo", FIELDS_5070], 5).stdout
    assert "NAD83 / Conus Albers" in source_meta  # the file's declared CRS

    run_gpio(["convert", "reproject", "--dst-crs", "EPSG:4326", FIELDS_5070, wgs84], 5)
    run_gpio(["add", "bbox", wgs84, wgs84_bbox], 5)
    assert_check_all(wgs84_bbox, 5)
    assert rows(wgs84_bbox) == FIELDS_ROWS

    # An absent/null `crs` in GeoParquet 1.1 means the OGC:CRS84 default -- that
    # is the CRS assertion, not a bbox check: this fixture is LABELLED EPSG:5070 but
    # its data actually lies in Europe (~18E, 47N), so a CONUS bbox assertion
    # would be wrong. Assert the real, degree-valued extent instead.
    assert geo_metadata(wgs84_bbox)["columns"]["geometry"].get("crs") is None
    xmin, ymin, xmax, ymax = bbox_extent(wgs84_bbox)
    assert (18.0 < xmin < 18.1) and (18.0 < xmax < 18.1)
    assert (47.1 < ymin < 47.2) and (47.1 < ymax < 47.2)

    # Reprojection consistency: 5070 -> 4326 -> 5070 returns the source extent.
    run_gpio(["convert", "reproject", "--dst-crs", "EPSG:5070", wgs84, back_to_5070], 5)
    run_gpio(["add", "bbox", back_to_5070, back_bbox], 5)
    projected = bbox_extent(back_bbox)
    assert projected[0] == pytest.approx(6753224.77, abs=1.0)
    assert projected[1] == pytest.approx(7301719.76, abs=1.0)
    assert projected[2] == pytest.approx(6758125.10, abs=1.0)
    assert projected[3] == pytest.approx(7306052.75, abs=1.0)


# ---------------------------------------------------------------------------
# Journey 6 -- aggregate, overview, PMTiles pyramid (budget 20s, fast)
# ---------------------------------------------------------------------------
@requires_tippecanoe
def test_journey_06_aggregate_overview_pyramid(tmp_path):
    """process aggregate h3 -> process overview -> pmtiles pyramid; header sanity."""
    cells = tmp_path / "cells.parquet"
    pmtiles = tmp_path / "cells.pmtiles"

    run_gpio(["process", "aggregate", "h3", PLACES, cells, "--resolution", "6"], 6)
    assert "h3_cell" in columns(cells)
    assert "count" in columns(cells)
    cell_count = rows(cells)
    assert 0 < cell_count < PLACES_ROWS  # 766 points collapse into fewer cells
    counts = pq.read_table(cells, columns=["count"]).column("count").to_pylist()
    assert sum(counts) == PLACES_ROWS  # aggregation conserves the input rows
    assert_check_all(cells, 6)

    # The base level fits the tile budget here, so ask for a coarser level
    # explicitly rather than relying on auto-selection.
    run_gpio(["process", "overview", cells, "--levels", "4"], 6)
    overview = tmp_path / "cells_r4.parquet"
    assert overview.exists()
    assert 0 < rows(overview) < cell_count
    overview_counts = pq.read_table(overview, columns=["count"]).column("count").to_pylist()
    assert sum(overview_counts) == PLACES_ROWS  # roll-up is exact

    run_gpio(["pmtiles", "pyramid", cells, pmtiles], 6)
    header = pmtiles.read_bytes()[:8]
    assert header[:7] == b"PMTiles", header
    assert header[7] == 3, "expected a PMTiles v3 archive"


# ---------------------------------------------------------------------------
# Journey 7 -- CSV round trip (budget 4s, fast)
# ---------------------------------------------------------------------------
def test_journey_07_csv_roundtrip(tmp_path):
    """CSV -> convert geoparquet -> add bbox -> sort hilbert -> check all."""
    exported_csv = tmp_path / "places.csv"
    imported = tmp_path / "from_csv.parquet"
    with_bbox = tmp_path / "from_csv_bbox.parquet"
    sorted_out = tmp_path / "from_csv_sorted.parquet"

    # --no-bbox: a re-imported JSON-encoded bbox string cannot back covering
    # metadata, and the import then fails GeoParquet metadata validation.
    run_gpio(["convert", "csv", "--no-bbox", PLACES, exported_csv], 7)
    header = exported_csv.read_text(encoding="utf-8").splitlines()[0]
    assert "wkt" in header

    run_gpio(["convert", "geoparquet", exported_csv, imported], 7)
    run_gpio(["add", "bbox", "--force", imported, with_bbox], 7)
    run_gpio(["sort", "hilbert", with_bbox, sorted_out], 7)

    assert rows(sorted_out) == PLACES_ROWS
    assert "bbox" in columns(sorted_out)
    report = assert_check_all(sorted_out, 7)
    # 766 rows sort into one row group, below the footer check's floor: the
    # report carries the numbers and withholds the verdict rather than pass it.
    assert "not judged below 8 row groups" in report
    assert "may not be optimally spatially ordered" not in report


# ---------------------------------------------------------------------------
# Journey 8 -- admin divisions then admin partition (budget 60s, slow)
# ---------------------------------------------------------------------------
# Substrings that mean "the boundaries dataset could not be obtained", as
# opposed to "gpio is broken". Matched case-insensitively against the failed
# command's output.
_BOUNDARIES_UNAVAILABLE = (
    # A concurrent gpio process (another checkout, another agent, the
    # developer's own shell) downloading the same release holds a DuckDB lock on
    # its temp file. Observed verbatim:
    #   IO Error: Could not set lock on file
    #   "~/.geoparquet-io/cache/admin/tmp_overture-2026-07-22.0-country-land.parquet":
    #   Conflicting lock is held in ... (PID 63436)
    "could not set lock",
    "conflicting lock",
    # ... and anything meaning the download itself did not happen. Observed:
    #   IO Error: Timeout was reached error for HTTP GET to
    #   'https://overturemaps-us-west-2.s3.../release/2026-07-22.0/...'
    #
    # Deliberately NOT in this list, because each would swallow a real bug and
    # report it as a skip: bare "io error"; the cache path itself (the SUCCESS
    # path prints it, so a later genuine failure in the same run would match);
    # and bare "connection", which is a live DuckDB error prefix
    # ("Connection Error: ...").
    "could not resolve",
    # Not bare "network": any real bug whose message merely mentions the word
    # would be reported as an environmental skip.
    "network error",
    "network is unreachable",
    "timed out",
    "timeout",
    "failed to download",
    "http error",
    "urlopen",
    "temporary failure",
)


def _skip_if_boundaries_unavailable(result) -> None:
    """Skip (never fail) when the admin cache cannot be downloaded or is locked."""
    if result.returncode == 0:
        return
    output = f"{result.stdout}\n{result.stderr}".lower()
    for needle in _BOUNDARIES_UNAVAILABLE:
        if needle in output:
            pytest.skip(
                "Overture admin boundaries unavailable (offline, download failed, or "
                f"~/.geoparquet-io/cache/admin busy): matched {needle!r}"
            )


def _run_boundary_command(args: list, description: str):
    """Run a cache-dependent journey-8 step, turning unavailability into a skip.

    A cold cache downloads the dataset, and a fresh Overture release can make
    that take longer than the journey's timeout -- measured: ~1.6s warm, minutes
    cold. A timeout here therefore means "the boundaries were not available in
    reasonable time", which is a skip; only a real, prompt failure fails.
    """
    try:
        result = run_gpio(args, 8, expect_success=False)
    except subprocess.TimeoutExpired:
        pytest.skip(
            f"{description} exceeded its timeout -- the Overture cache is cold and the "
            "download did not finish in time. Warm it once with "
            "`gpio add admin-divisions --dataset overture --levels country <file> <out>`."
        )
    _skip_if_boundaries_unavailable(result)
    assert result.returncode == 0, (
        f"{description} failed for a non-cache reason:\n{result.stdout}\n{result.stderr}"
    )
    return result


@pytest.mark.slow
def test_journey_08_admin_divisions_then_partition(tmp_path):
    """add admin-divisions -> partition admin -> check every leaf.

    Lane: ``slow`` ONLY, deliberately not ``network``. The boundaries do need
    fetching on a cold machine, but marking this ``network`` too would move it
    out of the blocking slow job and into the network job, which never runs on
    PRs and is ``continue-on-error`` -- demoting a gate to an advisory signal.
    Instead the fetch is attempted and the test SKIPS with a clear reason when
    the download or the cache is unavailable. In practice that means warm
    developer machines gate on it, while CI's ephemeral runners start cold and
    will usually skip (the cold download exceeds the timeout) until the slow
    job caches ``~/.geoparquet-io/cache/admin`` -- a follow-up, keyed on the
    pinned Overture release.

    Not hermetic, and cannot be: ``gpio add admin-divisions`` caches datasets in
    the machine-global ``~/.geoparquet-io/cache/admin`` (~34MB for Overture
    country level; GAUL would be ~724MB, which is why this uses Overture). That
    directory is shared with the developer's own runs and with any concurrent
    gpio process, so a first run costs a download and a concurrent run can hit a
    cache-lock conflict -- both are skips, not failures, via
    :func:`_skip_if_boundaries_unavailable`.
    """
    enriched = tmp_path / "places_admin.parquet"
    partitions = tmp_path / "admin_partitions"

    _run_boundary_command(
        [
            "add",
            "admin-divisions",
            "--dataset",
            "overture",
            "--levels",
            "country",
            PLACES,
            enriched,
        ],
        "add admin-divisions",
    )
    assert "overture_country" in columns(enriched)
    assert rows(enriched) == PLACES_ROWS

    # `partition admin` re-reads the same cache, so it needs the same guard.
    _run_boundary_command(
        [
            "partition",
            "admin",
            "--dataset",
            "overture",
            "--levels",
            "country",
            enriched,
            partitions,
        ],
        "partition admin",
    )
    # The 766 places straddle Burkina Faso, Benin, Ghana and Togo.
    assert {leaf.stem for leaf in leaf_files(partitions)} == {"BF", "BJ", "GH", "TG"}
    assert total_rows(partitions) == PLACES_ROWS
    assert_check_all(partitions, 8, all_files=True)
    safe_rmtree(partitions)


# ---------------------------------------------------------------------------
# Journey 9 -- remote extract (budget 30s, network)
# ---------------------------------------------------------------------------
@pytest.mark.network
def test_journey_09_remote_extract_then_sort(tmp_path):
    """extract --limit from a public URL -> sort hilbert -> check all; meta over HTTP."""
    subset = tmp_path / "remote_subset.parquet"
    sorted_out = tmp_path / "remote_sorted.parquet"

    run_gpio(["extract", REMOTE_PARQUET_URL, subset, "--limit", "500"], 9)
    assert rows(subset) == 500

    run_gpio(["sort", "hilbert", subset, sorted_out], 9)
    assert rows(sorted_out) == 500
    assert_check_all(sorted_out, 9)

    remote_meta = run_gpio(["inspect", "meta", REMOTE_PARQUET_URL], 9).stdout
    assert "GeoParquet" in remote_meta or "Geo" in remote_meta


# ---------------------------------------------------------------------------
# Journey 10 -- 200k synthetic points, sort, partition (budget 120s, slow)
# ---------------------------------------------------------------------------
@pytest.mark.slow
def test_journey_10_sorting_improves_spatial_pushdown(tmp_path):
    """200k random points -> sort hilbert -> partition quadkey; skip rate improves."""
    import csv
    import random

    source_csv = tmp_path / "points.csv"
    unsorted = tmp_path / "unsorted.parquet"
    hilbert = tmp_path / "hilbert.parquet"
    partitions = tmp_path / "quadkey_partitions"
    point_count = 200_000

    random.seed(667)
    with open(source_csv, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "wkt"])
        for i in range(point_count):
            lon = random.uniform(-180, 180)
            lat = random.uniform(-85, 85)
            writer.writerow([i, f"POINT ({lon:.5f} {lat:.5f})"])

    # --skip-hilbert keeps the input in random order; small row groups give the
    # spatial-order check enough row groups to produce a meaningful metric.
    run_gpio(
        [
            "convert",
            "geoparquet",
            "--skip-hilbert",
            "--row-group-size",
            "10000",
            source_csv,
            unsorted,
        ],
        10,
    )
    assert rows(unsorted) == point_count
    before = spatial_skip_rate(unsorted, 10)

    run_gpio(["sort", "hilbert", "--row-group-size", "10000", unsorted, hilbert], 10)
    assert rows(hilbert) == point_count
    after = spatial_skip_rate(hilbert, 10)

    assert before < 10.0, f"randomly ordered input should barely skip, got {before}%"
    assert after > 50.0, f"Hilbert order should skip most row groups, got {after}%"
    assert after > before + 40.0

    run_gpio(["partition", "quadkey", "--auto", "--target-rows", "50000", hilbert, partitions], 10)
    leaves = leaf_files(partitions)
    assert len(leaves) > 1
    assert total_rows(partitions) == point_count
    assert_check_all(leaves[0], 10)
    safe_rmtree(partitions)


# ---------------------------------------------------------------------------
# Oracle non-vacuity guard
# ---------------------------------------------------------------------------
def test_oracle_rejects_a_damaged_journey_output(tmp_path):
    """The oracle must FAIL on a damaged intermediate, not just on a non-zero exit.

    `gpio check all` exits 0 on the file below (it reports "✗ 3 failed, 25
    passed" and still returns 0), so this guards the assertion that actually
    does the work in :func:`assert_check_all`. Without it every journey could
    pass against corrupt output.
    """
    good = tmp_path / "good.parquet"
    damaged = tmp_path / "damaged.parquet"
    run_gpio(["convert", "geoparquet", BUILDINGS_GEOJSON, good], 1)
    assert_check_all(good, 1)

    payload = bytearray(good.read_bytes())
    del payload[len(payload) // 2 : len(payload) // 2 + 1500]
    damaged.write_bytes(bytes(payload))

    assert run_gpio(["check", "all", damaged], 1, expect_success=False).returncode == 0
    with pytest.raises(AssertionError):
        assert_check_all(damaged, 1)
