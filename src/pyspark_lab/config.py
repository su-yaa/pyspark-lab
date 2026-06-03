from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class DailySalesJobConfig:
    """Runtime settings for the daily sales metrics Spark job."""

    run_date: date
    output_uri: str

    @property
    def output_path(self) -> str:
        # Partition output by run date so repeated DAG runs are easy to compare.
        return f"{self.output_uri.rstrip('/')}/run_date={self.run_date.isoformat()}"

