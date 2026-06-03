from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class DailySalesJobConfig:
    """Runtime settings for the daily sales metrics Spark job."""

    run_date: date
    output_uri: str
    input_uri: Optional[str] = None
    min_orders: int = 1

    @property
    def output_path(self) -> str:
        # Partition output by run date so repeated DAG runs are easy to compare.
        return f"{self.output_uri.rstrip('/')}/run_date={self.run_date.isoformat()}"

    @property
    def quality_path(self) -> str:
        return f"{self.output_uri.rstrip('/')}/_quality/run_date={self.run_date.isoformat()}"
