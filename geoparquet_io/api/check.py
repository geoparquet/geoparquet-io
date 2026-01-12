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

    Example:
        >>> table = gpio.read('data.parquet')
        >>> result = table.check()
        >>> if result.passed():
        ...     print("All checks passed!")
        >>> else:
        ...     print("Failures:", result.failures())
    """

    def __init__(self, results: dict, check_type: str = "check"):
        """
        Initialize a CheckResult.

        Args:
            results: Raw results dictionary from check operation
            check_type: Type of check (e.g., "spatial", "compression", "all")
        """
        self._results = results
        self._check_type = check_type

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

        Returns:
            Raw results dictionary
        """
        return self._results

    @property
    def check_type(self) -> str:
        """Get the type of check that was performed."""
        return self._check_type

    def __repr__(self) -> str:
        """String representation of the CheckResult."""
        status = "passed" if self.passed() else "failed"
        num_failures = len(self.failures())
        num_warnings = len(self.warnings())
        return f"CheckResult({self._check_type}: {status}, failures={num_failures}, warnings={num_warnings})"

    def __bool__(self) -> bool:
        """Boolean representation - True if passed."""
        return self.passed()
