"""
Checks on one GeoParquet file, and the CheckResult they return.

``check_*`` and ``validate`` take a path (#1090); ``Table.check*`` hand them
the file a Table was read from. CheckResult has helper methods for
pass/fail status and for the warnings and failures.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from geoparquet_io.core.check_bbox_values import DEFAULT_BBOX_SAMPLE_ROWS

if TYPE_CHECKING:
    from geoparquet_io.core.check_parquet_structure import CheckProfile


class CheckResult:
    """
    Result of a check operation.

    Provides helper methods for inspecting check results including
    pass/fail status, warnings, and failures.

    Attributes:
        results: Raw results dictionary from the check operation
        check_type: Type of check that was performed
        source_path: The file whose bytes were measured, or None
        prospective: True when no file was measured: the verdict describes the
            bytes ``write()`` would produce from an in-memory table

    Example:
        >>> table = gpio.read('data.parquet')
        >>> result = table.check()
        >>> if result.verdict() == "failure":
        ...     print("Failures:", result.failures())
        >>> else:
        ...     print("Advice:", result.warnings())
    """

    def __init__(
        self,
        results: dict,
        check_type: str = "check",
        *,
        source_path: str | None = None,
    ) -> None:
        """
        Initialize a CheckResult.

        Args:
            results: Raw results dictionary from check operation
            check_type: Type of check (e.g., "spatial", "compression", "all")
            source_path: The file the verdict measured, when there is one.
                Row groups, compression and bloom filters are facts about
                written bytes; a result with no file behind it describes what
                ``write()`` would produce and is ``prospective``.
        """
        self._results = results
        self._check_type = check_type
        self._source_path = source_path

    def passed(self) -> bool:
        """
        Check if all checks passed.

        Returns:
            True if all checks passed, False otherwise
        """
        # Handle nested results (from check_all)
        if self._check_type == "all":
            for _category, cat_results in self._results.items():
                if isinstance(cat_results, dict) and not cat_results.get("passed", True):
                    return False
            return True

        # Handle single check results
        return self._results.get("passed", False)

    def verdict(self) -> str:
        """
        How bad the result is: ``"pass"``, ``"warning"`` or ``"failure"``.

        The classification ``gpio check`` uses for its exit code: a failure
        exits 1, a warning exits 0 (1 with ``--strict``). Unlike :meth:`passed`,
        which is False for either, this tells advice apart from a defect. The
        strict equivalent is ``verdict() != "pass"``.

        It covers the checks this result ran. :meth:`Table.check` runs the
        structure checks (row groups, bbox, covering, compression, bloom
        filters) -- not spatial order or spec validation, which ``gpio check
        all`` adds -- so a CI gate wanting ``check all``'s answer takes the
        worst of ``check()``, ``check_spatial()`` and ``validate()``.

        Returns:
            The verdict's label
        """
        from geoparquet_io.core.check_verdict import result_verdict, worst

        return worst(result_verdict(r) for _, r in self._categories()).label

    def _categories(self) -> list[tuple[str | None, dict]]:
        """``(category, results)`` pairs: one per check for ``"all"``, else the one result."""
        if self._check_type == "all":
            return [(c, r) for c, r in self._results.items() if isinstance(r, dict)]
        return [(None, self._results)]

    def _bucket(self, failures: bool) -> list[str]:
        """Issues labelled failure (``failures``), or labelled warning or less."""
        from geoparquet_io.core.check_verdict import Verdict, labelled_issues, result_verdict

        out: list[str] = []
        for category, results in self._categories():

            def tag(message: str, category=category) -> str:
                return f"[{category}] {message}" if category else message

            labelled = labelled_issues(results)
            if failures:
                found = [tag(i) for level, i in labelled if level is Verdict.FAILURE]
                if not found and result_verdict(results) is Verdict.FAILURE and category:
                    found = [tag("Check failed")]
                out.extend(found)
            else:
                out.extend(tag(w) for w in results.get("warnings", []))
                out.extend(tag(i) for level, i in labelled if level is not Verdict.FAILURE)
        return out

    def warnings(self) -> list[str]:
        """
        Get list of warning messages: advice, by the same severity as :meth:`verdict`.

        Returns:
            List of warning strings
        """
        return self._bucket(failures=False)

    def failures(self) -> list[str]:
        """
        Get list of failure messages: defects, by the same severity as :meth:`verdict`.

        An issue a check reports as advice (a non-ZSTD codec, say) is a
        warning, listed by :meth:`warnings`, not here.

        Returns:
            List of failure/issue strings
        """
        return self._bucket(failures=True)

    def recommendations(self) -> list[str]:
        """
        Get list of recommendations for improving the file.

        Returns:
            List of recommendation strings
        """
        recommendations = []

        if self._check_type == "all":
            for category, cat_results in self._results.items():
                if isinstance(cat_results, dict):
                    recs = cat_results.get("recommendations", [])
                    if recs:
                        recommendations.extend([f"[{category}] {r}" for r in recs])
        else:
            recommendations = self._results.get("recommendations", [])

        return recommendations

    def to_dict(self) -> dict:
        """
        Get the raw results dictionary.

        Returns:
            Raw results dictionary
        """
        return self._results

    @property
    def check_type(self) -> str:
        """Get the type of check that was performed."""
        return self._check_type

    @property
    def source_path(self) -> str | None:
        """The file this verdict measured, or None when it is prospective."""
        return self._source_path

    @property
    def prospective(self) -> bool:
        """True when no file was measured: the verdict describes what ``write()`` would produce."""
        return self._source_path is None

    def __repr__(self) -> str:
        """String representation of the CheckResult."""
        status = "passed" if self.passed() else "failed"
        num_failures = len(self.failures())
        num_warnings = len(self.warnings())
        scope = ", prospective" if self.prospective else ""
        return (
            f"CheckResult({self._check_type}: {status}, failures={num_failures}, "
            f"warnings={num_warnings}{scope})"
        )

    def __bool__(self) -> bool:
        """Boolean representation - True if passed."""
        return self.passed()


# --------------------------------------------------------------------------
# Path-level checks (#1090): one file in, a CheckResult out. ``Table.check*``
# hands these its source file, so both doors share one call path.
# --------------------------------------------------------------------------


def _one_file(path, command: str) -> str:
    """*path* as a string, refused when it names a directory or glob of files."""
    from geoparquet_io.core.exceptions import InvalidParameterError
    from geoparquet_io.core.file_utils import is_partition_path

    path = os.fspath(path)
    if is_partition_path(path):
        raise InvalidParameterError(
            "path",
            f"{path} is a directory or glob; a path-level check reads one file. "
            f"Check each file, or run `gpio check {command}` for a multi-file run.",
        )
    return path


def _structure_check(path, command: str, check_type: str, func, **kwargs) -> CheckResult:
    path = _one_file(path, command)
    results = func(path, verbose=False, return_results=True, quiet=True, **kwargs)
    return CheckResult(results, check_type=check_type, source_path=path)


def check_all(path: str | os.PathLike, profile: CheckProfile | None = None) -> CheckResult:
    """Run the structure checks on a file: row groups, bbox, covering, compression, bloom filters.

    The checks :meth:`Table.check` runs -- not spatial order or spec
    validation, which ``gpio check all`` adds; take the worst of this,
    :func:`check_spatial` and :func:`validate` for its answer.

    Args:
        path: A local file or remote URL (not a directory or glob)
        profile: Row-group size band to judge against, as ``--profile``

    Example:
        >>> result = gpio.check_all("data.parquet")
        >>> print(result.verdict(), result.failures())
    """
    from geoparquet_io.core import check_parquet_structure as core

    return _structure_check(path, "all", "all", core.check_all, profile=profile)


def check_compression(path: str | os.PathLike) -> CheckResult:
    """Check the geometry column's compression; ZSTD is recommended. See :func:`check_all`."""
    from geoparquet_io.core import check_parquet_structure as core

    return _structure_check(path, "compression", "compression", core.check_compression)


def check_row_groups(path: str | os.PathLike, profile: CheckProfile | None = None) -> CheckResult:
    """Check row-group sizes for cloud-native access. See :func:`check_all`."""
    from geoparquet_io.core import check_parquet_structure as core

    return _structure_check(path, "row-group", "row_groups", core.check_row_groups, profile=profile)


def check_bbox(
    path: str | os.PathLike, sample_rows: int = DEFAULT_BBOX_SAMPLE_ROWS, all_rows: bool = False
) -> CheckResult:
    """Check bbox structure, covering metadata, and that each bbox covers its geometry.

    Args:
        path: A local file or remote URL (not a directory or glob)
        sample_rows: Rows compared against their geometry, as
            ``--sample-rows``; 0 skips it
        all_rows: Compare every row, as ``--all-rows``
    """
    from geoparquet_io.core import check_parquet_structure as core

    return _structure_check(
        path, "bbox", "bbox", core.check_bbox, verify_rows=sample_rows, all_rows=all_rows
    )


def check_bloom_filters(path: str | os.PathLike) -> CheckResult:
    """Check which columns carry bloom filters for point lookups. See :func:`check_all`."""
    from geoparquet_io.core import check_parquet_structure as core

    return _structure_check(path, "all", "bloom_filters", core.check_bloom_filters)


def check_optimization(path: str | os.PathLike) -> CheckResult:
    """Score a file's spatial-query optimization (geo types, stats, order, row groups, codec)."""
    from geoparquet_io.core import check_optimization as core

    return _structure_check(path, "optimization", "optimization", core.check_optimization)


def check_spatial(
    path: str | os.PathLike, sample_size: int = 100, limit_rows: int = 500000
) -> CheckResult:
    """Check spatial order: consecutive-feature distance against random pairs (< 0.5 passes).

    Args:
        path: A local file or remote URL (not a directory or glob)
        sample_size: Random pairs to sample, as ``--random-sample-size``
        limit_rows: Maximum rows to analyze, as ``--limit-rows``
    """
    from geoparquet_io.core import check_spatial_order as core

    return _structure_check(
        path,
        "spatial",
        "spatial",
        core.check_spatial_order,
        random_sample_size=sample_size,
        limit_rows=limit_rows,
    )


def check_spatial_pushdown(path: str | os.PathLike) -> CheckResult:
    """Estimate how many row groups a regional query can skip, from per-row-group bbox stats."""
    from geoparquet_io.core import check_spatial_order as core

    path = _one_file(path, "spatial")
    results = core.check_spatial_pushdown_readiness(path, verbose=False)
    return CheckResult(results, check_type="spatial_pushdown", source_path=path)


def validate(
    path: str | os.PathLike,
    version: str | None = None,
    sample_size: int = 1000,
    skip_data_validation: bool = False,
) -> CheckResult:
    """Validate a file against the GeoParquet specification, as ``gpio check spec``.

    Args:
        path: A local file or remote URL (not a directory or glob)
        version: Target version ("1.0", "1.1", "2.0"), or None to use the declared one
        sample_size: Rows sampled for data validation, as ``--sample-size``
        skip_data_validation: Check metadata only, as ``--skip-data-validation``
    """
    from geoparquet_io.core.check_verdict import SEVERITY_KEY, validation_verdict
    from geoparquet_io.core.validate import validate_geoparquet

    path = _one_file(path, "spec")
    outcome = validate_geoparquet(
        path,
        target_version=version,
        validate_data=not skip_data_validation,
        sample_size=sample_size,
        verbose=False,
    )
    results = {
        "passed": outcome.is_valid,
        SEVERITY_KEY: validation_verdict(
            failed=outcome.failed_count, warnings=outcome.warning_count
        ).label,
        "file_path": outcome.file_path,
        "detected_version": outcome.detected_version,
        "target_version": outcome.target_version,
        "passed_count": outcome.passed_count,
        "failed_count": outcome.failed_count,
        "warning_count": outcome.warning_count,
        "issues": [f"{c.name}: {c.message}" for c in outcome.checks if c.status.value == "failed"],
    }
    return CheckResult(results, check_type="validate", source_path=path)
