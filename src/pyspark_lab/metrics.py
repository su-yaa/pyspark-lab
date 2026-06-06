from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable


def build_daily_sales_metrics(
    orders: Iterable[dict],
    run_date: date,
) -> list[dict]:
    """주문 이벤트를 지역/채널별 일매출 지표로 집계한다.

    실제 운영 경로는 `jobs/daily_sales_metrics.py`의 Spark DataFrame 집계다.
    이 순수 Python 함수는 같은 비즈니스 규칙을 빠르게 테스트하기 위한 기준점이며,
    Jenkins에서 Spark 클러스터 없이도 지표 정의가 깨졌는지 확인할 수 있게 해준다.
    """

    # key=(region, channel) 단위로 주문 수, 매출, 고객 집합을 누적한다.
    # Spark 코드의 groupBy("region", "channel")와 같은 역할이다.
    groups: dict[tuple[str, str], dict] = defaultdict(
        lambda: {
            "order_count": 0,
            "gross_sales": 0,
            "customers": set(),
        }
    )

    for order in orders:
        # Airflow가 넘긴 run_date만 처리한다. 과거/미래 데이터가 섞여 있어도
        # 하나의 DAG run은 하나의 영업일 결과만 만든다.
        if order["order_date"] != run_date:
            continue

        key = (order["region"], order["channel"])
        groups[key]["order_count"] += 1
        groups[key]["gross_sales"] += int(order["amount"])
        groups[key]["customers"].add(order["customer_id"])

    rows = []
    for (region, channel), values in sorted(groups.items()):
        order_count = values["order_count"]
        gross_sales = values["gross_sales"]
        # Decimal을 사용해 평균 주문금액 반올림을 명시적으로 고정한다.
        average_order_value = Decimal(gross_sales / order_count).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )

        rows.append(
            {
                "run_date": run_date.isoformat(),
                "region": region,
                "channel": channel,
                "order_count": order_count,
                "customer_count": len(values["customers"]),
                "gross_sales": gross_sales,
                "average_order_value": str(average_order_value),
            }
        )

    return rows
