from __future__ import annotations

from datetime import date

JOIN_FLOW_ORDERS = [
    {
        "order_id": "O-1001",
        "order_date": date(2026, 6, 3),
        "customer_id": "C-001",
        "region": "seoul",
        "amount": 42000,
    },
    {
        "order_id": "O-1002",
        "order_date": date(2026, 6, 3),
        "customer_id": "C-002",
        "region": "busan",
        "amount": 28000,
    },
    {
        "order_id": "O-1003",
        "order_date": date(2026, 6, 3),
        "customer_id": "C-003",
        "region": "seoul",
        "amount": 31000,
    },
    {
        "order_id": "O-1004",
        "order_date": date(2026, 6, 4),
        "customer_id": "C-001",
        "region": "seoul",
        "amount": 19000,
    },
]

JOIN_FLOW_CUSTOMERS = [
    {
        "customer_id": "C-001",
        "customer_name": "Hansu",
        "segment": "vip",
        "signup_channel": "web",
    },
    {
        "customer_id": "C-002",
        "customer_name": "Mina",
        "segment": "standard",
        "signup_channel": "app",
    },
    {
        "customer_id": "C-003",
        "customer_name": "Jisoo",
        "segment": "new",
        "signup_channel": "app",
    },
]
