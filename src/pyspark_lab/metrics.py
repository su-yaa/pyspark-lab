from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable


def build_daily_sales_metrics(
    orders: Iterable[dict],
    run_date: date,
) -> list[dict]:
    """Aggregate order rows into daily metrics.

    The Spark job implements the same business rules with DataFrame APIs. Keeping
    this small pure-Python version makes Jenkins tests fast and keeps the metric
    definition readable for operators.
    """

    groups: dict[tuple[str, str], dict] = defaultdict(
        lambda: {
            "order_count": 0,
            "gross_sales": 0,
            "customers": set(),
        }
    )

    for order in orders:
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

