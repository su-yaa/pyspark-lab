from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class DailySalesJobConfig:
    """일매출 Spark 작업의 실행 계약을 담는 설정 객체.

    Airflow는 CLI 인자로 run_date/output_uri 같은 운영 파라미터를 넘기고,
    Spark 코드는 이 객체를 기준으로 입력, 품질검사, 결과 저장 경로를 계산한다.
    """

    run_date: date
    output_uri: str
    input_uri: Optional[str] = None
    min_orders: int = 1

    @property
    def output_path(self) -> str:
        # 결과는 날짜별 파티션에 저장한다. 같은 run_date를 재실행하면 해당 날짜
        # 결과만 덮어쓰므로 장애 재처리와 결과 비교가 단순해진다.
        return f"{self.output_uri.rstrip('/')}/run_date={self.run_date.isoformat()}"

    @property
    def quality_path(self) -> str:
        # 품질검사 결과는 지표와 분리해 저장한다. 지표 생성이 실패하더라도
        # 어떤 검사에서 막혔는지 MinIO에서 바로 확인할 수 있다.
        return f"{self.output_uri.rstrip('/')}/_quality/run_date={self.run_date.isoformat()}"
