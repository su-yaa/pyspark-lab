from __future__ import annotations

from datetime import date


SAMPLE_ORDERS = [
    {
        "order_id": "ORD-1001",
        "order_date": date(2026, 6, 3),
        "region": "seoul",
        "channel": "web",
        "customer_id": "C-001",
        "amount": 42_000,
    },
    {
        "order_id": "ORD-1002",
        "order_date": date(2026, 6, 3),
        "region": "seoul",
        "channel": "app",
        "customer_id": "C-002",
        "amount": 31_000,
    },
    {
        "order_id": "ORD-1003",
        "order_date": date(2026, 6, 3),
        "region": "busan",
        "channel": "web",
        "customer_id": "C-003",
        "amount": 28_000,
    },
    {
        "order_id": "ORD-1004",
        "order_date": date(2026, 6, 2),
        "region": "seoul",
        "channel": "web",
        "customer_id": "C-001",
        "amount": 19_000,
    },
    {
        "order_id": "ORD-1005",
        "order_date": date(2026, 6, 3),
        "region": "busan",
        "channel": "app",
        "customer_id": "C-004",
        "amount": 51_000,
    },
]

