from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QualityResult:
    """품질검사 결과를 Spark JSON 출력으로 저장하기 쉬운 형태로 표현한다."""

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
    """실행일 데이터가 최소 건수 이상 있는지 검사한다.

    이 예제의 품질검사는 단순하지만, 실무에서는 이 함수가 null 비율,
    중복 주문, 지연 도착 데이터 같은 검사를 추가하는 출발점이 된다.
    """

    return QualityResult(
        check_name=check_name,
        passed=observed_value >= threshold,
        observed_value=observed_value,
        threshold=threshold,
    )
