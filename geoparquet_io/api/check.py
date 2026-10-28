"""
CheckResult class for representing validation and check results.

Provides a structured way to inspect check results with helper methods
for determining pass/fail status and extracting warnings and failures.
"""

from __future__ import annotations


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
