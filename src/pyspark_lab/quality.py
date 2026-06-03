from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QualityResult:
    check_name: str
    passed: bool
    observed_value: int
    threshold: int


def require_minimum_rows(
    *,
    check_name: str,
    observed_value: int,
    threshold: int,
) -> QualityResult:
    """Return a small, serializable data quality result."""

    return QualityResult(
        check_name=check_name,
        passed=observed_value >= threshold,
        observed_value=observed_value,
        threshold=threshold,
    )
