from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from common.asset_events import emit_output_asset_event, minio_asset, partitioned_output_path
from common.spark_application_factory import SparkJobSpec
from common.spark_application_runner import build_spark_api, submit_and_wait_for_spark_application

NAMESPACE = "data-lab"
SPARK_IMAGE = "ghcr.io/su-yaa/pyspark-lab:main"
DEFAULT_SAMPLE_RUN_DATE = "2026-06-03"

CUSTOMER_DIM_APP_NAME = "pyspark-lab-join-flow-customers"
JOIN_APP_NAME = "pyspark-lab-join-flow-orders-customers"

CUSTOMER_DIM_MAIN_FILE = "local:///opt/spark/work-dir/spark/jobs/join_flow/prepare_customers.py"
JOIN_MAIN_FILE = "local:///opt/spark/work-dir/spark/jobs/join_flow/join_orders_customers.py"

CUSTOMER_DIM_URI = "s3a://pyspark-lab/join-flow/customer-dim"
JOIN_OUTPUT_URI = "s3a://pyspark-lab/join-flow/enriched-orders"

CUSTOMER_DIM_ASSET = minio_asset(
    name="pyspark_lab_join_flow_customer_dim",
    uri=CUSTOMER_DIM_URI,
)
ENRICHED_ORDERS_ASSET = minio_asset(
    name="pyspark_lab_join_flow_enriched_orders",
    uri=JOIN_OUTPUT_URI,
)


def resolve_run_date(context: dict) -> str:
    """수동 실행은 샘플 데이터 기준일을 기본값으로 사용한다."""

    dag_run = context.get("dag_run")
    dag_conf = getattr(dag_run, "conf", {}) or {}
    configured_run_date = dag_conf.get("run_date")
    if configured_run_date:
        return str(configured_run_date)

    run_type = str(getattr(dag_run, "run_type", "")).lower()
    if "manual" in run_type:
        return DEFAULT_SAMPLE_RUN_DATE

    return context.get("ds") or DEFAULT_SAMPLE_RUN_DATE


@dag(
    dag_id="pyspark_lab_join_flow",
    description="Run two SparkApplications in order: prepare customer dim, then join orders with customers.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["data-lab", "pyspark", "spark-operator", "join"],
)
def pyspark_lab_join_flow():
    @task(outlets=[CUSTOMER_DIM_ASSET])
    def execution_1_prepare_customer_dim(*, outlet_events) -> dict[str, str]:
        context = get_current_context()
        run_date = resolve_run_date(context)
        spark_api = build_spark_api()
        spark_job = SparkJobSpec(
            name=CUSTOMER_DIM_APP_NAME,
            namespace=NAMESPACE,
            image=SPARK_IMAGE,
            main_application_file=CUSTOMER_DIM_MAIN_FILE,
            arguments=[
                "--run-date",
                run_date,
                "--output-uri",
                CUSTOMER_DIM_URI,
            ],
            run_date=run_date,
        )

        print(
            "[pyspark-lab-join-dag] 실행 1을 시작합니다 | "
            f"run_date={run_date}, output_uri={CUSTOMER_DIM_URI}",
            flush=True,
        )
        result = submit_and_wait_for_spark_application(
            spark_api=spark_api,
            spark_job=spark_job,
            log_prefix="[pyspark-lab-join-dag][execution-1]",
        )
        customer_dim_path = partitioned_output_path(
            output_uri=CUSTOMER_DIM_URI,
            run_date=run_date,
        )
        emit_output_asset_event(
            outlet_events=outlet_events,
            asset=CUSTOMER_DIM_ASSET,
            run_date=run_date,
            output_uri=CUSTOMER_DIM_URI,
            spark_application=result.app_name,
            spark_state=result.state,
        )
        return {
            "run_date": run_date,
            "customer_dim_path": customer_dim_path,
            "state": result.state,
        }

    @task(outlets=[ENRICHED_ORDERS_ASSET])
    def execution_2_join_orders_with_customers(
        execution_1: dict[str, str],
        *,
        outlet_events,
    ) -> str:
        run_date = execution_1["run_date"]
        customer_dim_path = execution_1["customer_dim_path"]
        spark_api = build_spark_api()
        spark_job = SparkJobSpec(
            name=JOIN_APP_NAME,
            namespace=NAMESPACE,
            image=SPARK_IMAGE,
            main_application_file=JOIN_MAIN_FILE,
            arguments=[
                "--run-date",
                run_date,
                "--customer-dim-uri",
                customer_dim_path,
                "--output-uri",
                JOIN_OUTPUT_URI,
            ],
            run_date=run_date,
        )

        print(
            "[pyspark-lab-join-dag] 실행 2를 시작합니다 | "
            f"run_date={run_date}, customer_dim_uri={customer_dim_path}, output_uri={JOIN_OUTPUT_URI}",
            flush=True,
        )
        result = submit_and_wait_for_spark_application(
            spark_api=spark_api,
            spark_job=spark_job,
            log_prefix="[pyspark-lab-join-dag][execution-2]",
        )
        emit_output_asset_event(
            outlet_events=outlet_events,
            asset=ENRICHED_ORDERS_ASSET,
            run_date=run_date,
            output_uri=JOIN_OUTPUT_URI,
            spark_application=result.app_name,
            spark_state=result.state,
            extra={"input_customer_dim_path": customer_dim_path},
        )
        return result.state

    execution_2_join_orders_with_customers(execution_1_prepare_customer_dim())


pyspark_lab_join_flow()
