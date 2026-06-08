from __future__ import annotations

from typing import Any

from airflow.sdk import Asset


def minio_asset(*, name: str, uri: str) -> Asset:
    """Create an Airflow asset for a MinIO/S3 output location."""

    return Asset(name=name, uri=to_airflow_asset_uri(uri))


def emit_output_asset_event(
    *,
    outlet_events: Any,
    asset: Asset,
    run_date: str,
    output_uri: str,
    spark_application: str,
    spark_state: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Attach metadata to the asset event emitted when the task succeeds."""

    output_path = partitioned_output_path(output_uri=output_uri, run_date=run_date)
    event_extra: dict[str, Any] = {
        "run_date": run_date,
        "output_uri": to_airflow_asset_uri(output_uri),
        "output_path": to_airflow_asset_uri(output_path),
        "storage_output_uri": output_uri,
        "storage_output_path": output_path,
        "spark_application": spark_application,
        "spark_state": spark_state,
    }
    if extra:
        event_extra.update(extra)

    outlet_events[asset].extra = event_extra
    print(
        "[pyspark-lab-dag] Asset event metadata was attached | "
        f"asset={asset.uri}, output_path={event_extra['output_path']}, run_date={run_date}",
        flush=True,
    )
    return event_extra


def partitioned_output_path(*, output_uri: str, run_date: str) -> str:
    return f"{output_uri.rstrip('/')}/run_date={run_date}"


def to_airflow_asset_uri(uri: str) -> str:
    if uri.startswith("s3a://"):
        return f"s3://{uri.removeprefix('s3a://')}"
    return uri
