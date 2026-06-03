from __future__ import annotations

from datetime import date

from pyspark_lab.metrics import build_daily_sales_metrics
from pyspark_lab.sample_data import SAMPLE_ORDERS


def test_build_daily_sales_metrics_groups_by_region_and_channel() -> None:
    rows = build_daily_sales_metrics(SAMPLE_ORDERS, date(2026, 6, 3))

    assert rows == [
        {
            "run_date": "2026-06-03",
            "region": "busan",
            "channel": "app",
            "order_count": 1,
            "customer_count": 1,
            "gross_sales": 51000,
            "average_order_value": "51000.00",
        },
        {
            "run_date": "2026-06-03",
            "region": "busan",
            "channel": "web",
            "order_count": 1,
            "customer_count": 1,
            "gross_sales": 28000,
            "average_order_value": "28000.00",
        },
        {
            "run_date": "2026-06-03",
            "region": "seoul",
            "channel": "app",
            "order_count": 1,
            "customer_count": 1,
            "gross_sales": 31000,
            "average_order_value": "31000.00",
        },
        {
            "run_date": "2026-06-03",
            "region": "seoul",
            "channel": "web",
            "order_count": 1,
            "customer_count": 1,
            "gross_sales": 42000,
            "average_order_value": "42000.00",
        },
    ]


def test_build_daily_sales_metrics_ignores_other_dates() -> None:
    rows = build_daily_sales_metrics(SAMPLE_ORDERS, date(2026, 6, 4))

    assert rows == []

