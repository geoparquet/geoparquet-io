"""Tests for version_benchmark.py trend analysis."""

import json
import tempfile
from pathlib import Path

import pytest

# Import via the scripts package (not a sys.path hack): a relative sys.path
# insertion resolves against the CWD, not this file, and breaks when the suite
# runs from anywhere but the repo root.
from scripts.version_benchmark import analyze_trends


@pytest.fixture
def sample_baselines(tmp_path):
    """Create sample baseline files for testing."""
    # Baseline 1: v0.7.0 (oldest)
    baseline1 = {
        "version": "v0.7.0",
        "timestamp": "2024-01-01T00:00:00",
        "benchmarks": [
            {
                "file_size": "small",
                "operation": "extract-limit",
                "avg_time": 1.0,
                "min_time": 0.9,
                "max_time": 1.1,
            },
            {
                "file_size": "small",
                "operation": "add-bbox",
                "avg_time": 2.0,
                "min_time": 1.9,
                "max_time": 2.1,
            },
            {
                "file_size": "medium",
                "operation": "sort-hilbert",
                "avg_time": 5.0,
                "min_time": 4.8,
                "max_time": 5.2,
            },
        ],
    }

    # Baseline 2: v0.8.0 (middle) - some operations got 10% slower
    baseline2 = {
        "version": "v0.8.0",
        "timestamp": "2024-02-01T00:00:00",
        "benchmarks": [
            {
                "file_size": "small",
                "operation": "extract-limit",
                "avg_time": 1.1,  # +10%
                "min_time": 1.0,
                "max_time": 1.2,
            },
            {
                "file_size": "small",
                "operation": "add-bbox",
                "avg_time": 1.8,  # -10% (improvement)
                "min_time": 1.7,
                "max_time": 1.9,
            },
            {
                "file_size": "medium",
                "operation": "sort-hilbert",
                "avg_time": 5.5,  # +10%
                "min_time": 5.3,
                "max_time": 5.7,
            },
        ],
    }

    # Baseline 3: v0.9.0 (newest) - degradation continues for some
    baseline3 = {
        "version": "v0.9.0",
        "timestamp": "2024-03-01T00:00:00",
        "benchmarks": [
            {
                "file_size": "small",
                "operation": "extract-limit",
                "avg_time": 1.21,  # +10% again (gradual degradation)
                "min_time": 1.1,
                "max_time": 1.3,
            },
            {
                "file_size": "small",
                "operation": "add-bbox",
                "avg_time": 1.62,  # -10% again (consistent improvement)
                "min_time": 1.5,
                "max_time": 1.7,
            },
            {
                "file_size": "medium",
                "operation": "sort-hilbert",
                "avg_time": 5.4,  # -2% (slight improvement, not consistent)
                "min_time": 5.2,
                "max_time": 5.6,
            },
        ],
    }

    # Write baseline files
    files = []
    for i, baseline in enumerate([baseline1, baseline2, baseline3], 1):
        filepath = tmp_path / f"baseline{i}.json"
        with open(filepath, "w") as f:
            json.dump(baseline, f)
        files.append(str(filepath))

    return files


def test_analyze_trends_detects_degradation(sample_baselines):
    """Test that gradual degradation is detected."""
    result = analyze_trends(sample_baselines, degradation_threshold=0.05)

    # Should have no error
    assert "error" not in result

    # Should have tracked the operations
    assert result["baselines_count"] == 3
    assert result["versions"] == ["v0.7.0", "v0.8.0", "v0.9.0"]

    # Should detect gradual degradation for extract-limit
    # (it degraded by 10% in both transitions, which is > 5% threshold)
    warnings = result["warnings"]
    assert len(warnings) > 0

    # Find the extract-limit warning
    extract_warning = next(
        (w for w in warnings if w["operation"] == "extract-limit" and w["file_size"] == "small"),
        None,
    )
    assert extract_warning is not None
    assert extract_warning["type"] == "gradual_degradation"
    assert extract_warning["avg_degradation_pct"] > 5  # Should be around 10%


def test_analyze_trends_detects_improvements(sample_baselines):
    """Test that consistent improvements are detected."""
    result = analyze_trends(sample_baselines, degradation_threshold=0.05)

    # Should detect consistent improvement for add-bbox
    improvements = result["improvements"]
    assert len(improvements) > 0

    # Find the add-bbox improvement
    bbox_improvement = next(
        (
            imp
            for imp in improvements
            if imp["operation"] == "add-bbox" and imp["file_size"] == "small"
        ),
        None,
    )
    assert bbox_improvement is not None
    assert bbox_improvement["avg_improvement_pct"] > 5  # Should be around 10%


def test_analyze_trends_ignores_single_regression(sample_baselines):
    """Test that single regressions don't trigger gradual degradation warning."""
    result = analyze_trends(sample_baselines, degradation_threshold=0.05)

    # sort-hilbert had one regression followed by an improvement
    # Should NOT appear in warnings
    warnings = result["warnings"]
    hilbert_warning = next(
        (w for w in warnings if w["operation"] == "sort-hilbert" and w["file_size"] == "medium"),
        None,
    )
    assert hilbert_warning is None


def test_analyze_trends_calculates_statistics(sample_baselines):
    """Test that overall statistics are calculated correctly."""
    result = analyze_trends(sample_baselines, degradation_threshold=0.05)

    stats = result["statistics"]

    # Should have calculated statistics
    assert "avg_change_pct" in stats
    assert "max_regression_pct" in stats
    assert "max_improvement_pct" in stats

    # Max regression should be around 10%
    assert stats["max_regression_pct"] > 5
    assert stats["max_regression_pct"] < 15

    # Max improvement should be negative and around -10%
    assert stats["max_improvement_pct"] < -5
    assert stats["max_improvement_pct"] > -15


def test_analyze_trends_insufficient_baselines():
    """Test error handling when not enough baselines provided."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"version": "v1.0.0", "benchmarks": []}, f)
        single_file = f.name

    try:
        result = analyze_trends([single_file])
        assert "error" in result
        assert "at least 2 baselines" in result["error"].lower()
    finally:
        Path(single_file).unlink()


def test_analyze_trends_custom_threshold(sample_baselines):
    """Test that custom degradation threshold works."""
    # With a 15% threshold, the 10% degradation should NOT trigger warnings
    result = analyze_trends(sample_baselines, degradation_threshold=0.15)

    warnings = result["warnings"]
    assert len(warnings) == 0  # No warnings with higher threshold


def test_analyze_trends_skips_failed_operations(tmp_path):
    """Test that operations with None/0 times are skipped."""
    baseline1 = {
        "version": "v1.0.0",
        "benchmarks": [
            {"file_size": "small", "operation": "test", "avg_time": 1.0},
            {"file_size": "small", "operation": "failed", "avg_time": None},
        ],
    }
    baseline2 = {
        "version": "v2.0.0",
        "benchmarks": [
            {"file_size": "small", "operation": "test", "avg_time": 1.2},
            {"file_size": "small", "operation": "failed", "avg_time": 0},
        ],
    }

    files = []
    for i, baseline in enumerate([baseline1, baseline2], 1):
        filepath = tmp_path / f"baseline{i}.json"
        with open(filepath, "w") as f:
            json.dump(baseline, f)
        files.append(str(filepath))

    result = analyze_trends(files)

    # Should process the valid operation and skip the failed one
    assert "error" not in result
    assert result["operations_tracked"] == 1  # Only "test" operation tracked
