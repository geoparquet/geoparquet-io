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
        source: The file whose bytes were measured, or None
        prospective: True when no such file existed and the verdict describes
            bytes gpio wrote from an in-memory table instead

    Example:
        >>> table = gpio.read('data.parquet')
        >>> result = table.check()
        >>> if result.passed():
        ...     print("All checks passed!")
        >>> else:
        ...     print("Failures:", result.failures())
    """

    def __init__(
        self,
        results: dict,
        check_type: str = "check",
        *,
        source: str | None = None,
        prospective: bool = False,
    ):
        """
        Initialize a CheckResult.

        Args:
            results: Raw results dictionary from check operation
            check_type: Type of check (e.g., "spatial", "compression", "all")
            source: Path of the file the verdict describes, when there is one.
            prospective: True when the verdict describes a file gpio wrote from
                an in-memory table rather than one the caller has. Row groups,
                compression and bloom filters are properties of *written*
                bytes, so a Table with no file behind it can only answer the
                "how would this be laid out if I wrote it now?" question --
                and must not let the answer pass for the other one (#1060).
        """
        self._results = results
        self._check_type = check_type
        self._source = source
        self._prospective = prospective

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

    def warnings(self) -> list[str]:
        """
        Get list of warning messages.

        Returns:
            List of warning strings
        """
        warnings = []

        if self._check_type == "all":
            # Aggregate warnings from all categories
            for category, cat_results in self._results.items():
                if isinstance(cat_results, dict):
                    cat_warnings = cat_results.get("warnings", [])
                    if cat_warnings:
                        warnings.extend([f"[{category}] {w}" for w in cat_warnings])
                    # Some checks use "issues" as warnings when passed
                    if cat_results.get("passed", True):
                        issues = cat_results.get("issues", [])
                        if issues:
                            warnings.extend([f"[{category}] {i}" for i in issues])
        else:
            warnings = self._results.get("warnings", [])

        return warnings

    def failures(self) -> list[str]:
        """
        Get list of failure messages.

        Returns:
            List of failure/issue strings
        """
        failures = []

        if self._check_type == "all":
            # Aggregate failures from all categories
            for category, cat_results in self._results.items():
                if isinstance(cat_results, dict) and not cat_results.get("passed", True):
                    issues = cat_results.get("issues", [])
                    if issues:
                        failures.extend([f"[{category}] {i}" for i in issues])
                    else:
                        failures.append(f"[{category}] Check failed")
        else:
            if not self._results.get("passed", True):
                failures = self._results.get("issues", [])

        return failures

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

        A labelled result (one that knows which bytes it measured) carries two
        extra keys, ``source`` and ``prospective``, so that a caller reading the
        dict rather than the object still learns what the numbers are about.
        An unlabelled result is handed back exactly as the check produced it.

        Returns:
            Raw results dictionary
        """
        if self._source is None and not self._prospective:
            return self._results
        return {**self._results, "source": self._source, "prospective": self._prospective}

    @property
    def check_type(self) -> str:
        """Get the type of check that was performed."""
        return self._check_type

    @property
    def source(self) -> str | None:
        """The file whose bytes this verdict describes, or None."""
        return self._source

    @property
    def prospective(self) -> bool:
        """True when the verdict describes what gpio *would* write, not a file you have."""
        return self._prospective

    def __repr__(self) -> str:
        """String representation of the CheckResult."""
        status = "passed" if self.passed() else "failed"
        num_failures = len(self.failures())
        num_warnings = len(self.warnings())
        scope = ", prospective" if self._prospective else ""
        return (
            f"CheckResult({self._check_type}: {status}, failures={num_failures}, "
            f"warnings={num_warnings}{scope})"
        )

    def __bool__(self) -> bool:
        """Boolean representation - True if passed."""
        return self.passed()
