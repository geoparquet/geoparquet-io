"""Unit tests for :func:`geoparquet_io.cli.decorators.resolve_kdtree_options`.

``gpio add kdtree`` and ``gpio partition kdtree`` accept the same four sampling
and partition-count options and resolved them with their own copy of the same
~20 lines, in two different files. These tests pin the resolution so the two
commands cannot drift.
"""

from __future__ import annotations

import click
import pytest
from click.core import ParameterSource

from geoparquet_io.cli.decorators import resolve_kdtree_options


class TestExplicitPartitions:
    @pytest.mark.parametrize(
        ("partitions", "iterations"),
        [(2, 1), (4, 2), (8, 3), (1024, 10)],
    )
    def test_partition_count_becomes_log2_iterations(self, partitions, iterations):
        assert resolve_kdtree_options(
            partitions, None, 100_000, False, ParameterSource.DEFAULT
        ) == (
            iterations,
            100_000,
            None,
        )

    @pytest.mark.parametrize("partitions", [0, 1, 3, 6, 100, -4])
    def test_rejects_a_count_that_is_not_a_power_of_two(self, partitions):
        with pytest.raises(click.UsageError) as excinfo:
            resolve_kdtree_options(partitions, None, 100_000, False, ParameterSource.DEFAULT)
        assert str(excinfo.value) == (
            f"Partitions must be a power of 2 (2, 4, 8, ...), got {partitions}"
        )


class TestAutoMode:
    def test_defaults_to_120k_rows_when_neither_option_is_given(self):
        assert resolve_kdtree_options(None, None, 100_000, False, ParameterSource.DEFAULT) == (
            None,
            100_000,
            ("rows", 120_000),
        )

    def test_explicit_target_rows(self):
        assert resolve_kdtree_options(None, 50_000, 100_000, False, ParameterSource.DEFAULT) == (
            None,
            100_000,
            ("rows", 50_000),
        )

    @pytest.mark.parametrize("auto", [0, -1])
    def test_a_non_positive_target_falls_back_to_120k(self, auto):
        assert resolve_kdtree_options(None, auto, 100_000, False, ParameterSource.DEFAULT)[2] == (
            "rows",
            120_000,
        )

    def test_partitions_and_auto_are_mutually_exclusive(self):
        with pytest.raises(click.UsageError) as excinfo:
            resolve_kdtree_options(8, 50_000, 100_000, False, ParameterSource.DEFAULT)
        assert str(excinfo.value) == "--partitions and --auto are mutually exclusive"


class TestSampling:
    def test_exact_disables_sampling(self):
        assert resolve_kdtree_options(8, None, 100_000, True, ParameterSource.DEFAULT) == (
            3,
            None,
            None,
        )

    def test_approx_sets_the_sample_size(self):
        assert resolve_kdtree_options(8, None, 25_000, False, ParameterSource.COMMANDLINE) == (
            3,
            25_000,
            None,
        )

    def test_exact_with_a_non_default_approx_is_rejected(self):
        with pytest.raises(click.UsageError) as excinfo:
            resolve_kdtree_options(8, None, 25_000, True, ParameterSource.COMMANDLINE)
        assert str(excinfo.value) == "--approx and --exact are mutually exclusive"

    def test_exact_with_a_typed_approx_equal_to_the_default_is_rejected(self):
        # `--exact --approx 100000` used to pass silently because the conflict
        # was detected by comparing --approx against its default (#951).
        with pytest.raises(click.UsageError) as excinfo:
            resolve_kdtree_options(8, None, 100_000, True, ParameterSource.COMMANDLINE)
        assert str(excinfo.value) == "--approx and --exact are mutually exclusive"


def test_exclusivity_is_checked_before_the_power_of_two_rule():
    # Both are wrong; the pair error is the one both commands raise today.
    with pytest.raises(click.UsageError) as excinfo:
        resolve_kdtree_options(3, 50_000, 100_000, False, ParameterSource.DEFAULT)
    assert str(excinfo.value) == "--partitions and --auto are mutually exclusive"
