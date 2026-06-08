from __future__ import annotations

import time
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from common.asset_events import emit_output_asset_event, minio_asset
from common.kubernetes_log_relay import PodLogRelay, build_core_api
from common.spark_application_factory import (
    SPARK_API_GROUP,
    SPARK_API_VERSION,
    SPARK_PLURAL,
    SparkJobSpec,
    build_pyspark_application,
)
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

NAMESPACE = "data-lab"
SPARK_APP_NAME = "pyspark-lab-daily-sales"
SPARK_IMAGE = "ghcr.io/su-yaa/pyspark-lab:main"
MAIN_APPLICATION_FILE = "local:///opt/spark/work-dir/spark/jobs/daily_sales/metrics.py"
OUTPUT_URI = "s3a://pyspark-lab/daily-sales"
QUALITY_URI = f"{OUTPUT_URI}/_quality"
DEFAULT_SAMPLE_RUN_DATE = "2026-06-03"
SPARK_DRIVER_CONTAINER = "spark-kubernetes-driver"
DRIVER_LOG_TAIL_LINES = 200
DAILY_SALES_ASSET = minio_asset(
    name="pyspark_lab_daily_sales",
    uri=OUTPUT_URI,
)
DAILY_SALES_QUALITY_ASSET = minio_asset(
    name="pyspark_lab_daily_sales_quality",
    uri=QUALITY_URI,
)


def log_step(message: str, **details: object) -> None:
    """Airflow task 로그에서 Spark 제출 흐름을 한 줄씩 추적하기 위한 함수."""

    suffix = ""
    if details:
        rendered_details = ", ".join(
            f"{key}={value}" for key, value in details.items() if value is not None
        )
        if rendered_details:
            suffix = f" | {rendered_details}"

    print(f"[pyspark-lab-dag] {message}{suffix}", flush=True)


def spark_api() -> client.CustomObjectsApi:
    """Airflow pod의 ServiceAccount 권한으로 SparkApplication API에 접근한다."""

    config.load_incluster_config()
    return client.CustomObjectsApi()


def resolve_run_date(context: dict) -> str:
    """DAG 실행일을 Spark job 파라미터로 변환한다.

    운영 스케줄 실행은 Airflow의 `ds`를 그대로 쓰면 된다. 다만 이 저장소의
    내장 샘플 데이터는 2026-06-03 기준이라, 수동 실행에서 run_date를 따로
    넘기지 않으면 성공 예제를 바로 볼 수 있도록 샘플 날짜를 기본값으로 쓴다.
    """

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
    dag_id="pyspark_lab_daily_sales",
    description="Submit a production-shaped PySpark sales metrics job through Spark Operator.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    tags=["data-lab", "pyspark", "spark-operator"],
)
def pyspark_lab_daily_sales():
    @task(outlets=[DAILY_SALES_ASSET, DAILY_SALES_QUALITY_ASSET])
    def submit_and_wait(*, outlet_events) -> str:
        context = get_current_context()
        run_date = resolve_run_date(context)
        api = spark_api()
        pod_api = build_core_api()
        driver_log_relay = PodLogRelay(
            namespace=NAMESPACE,
            container=SPARK_DRIVER_CONTAINER,
            prefix="[spark-driver]",
            tail_lines=DRIVER_LOG_TAIL_LINES,
        )
        spark_job = SparkJobSpec(
            name=SPARK_APP_NAME,
            namespace=NAMESPACE,
            image=SPARK_IMAGE,
            main_application_file=MAIN_APPLICATION_FILE,
            arguments=[
                "--run-date",
                run_date,
                "--output-uri",
                OUTPUT_URI,
                "--min-orders",
                "1",
            ],
            run_date=run_date,
        )
        manifest = build_pyspark_application(spark_job)
        spec = manifest["spec"]

        log_step(
            "Airflow DAG 실행을 시작합니다",
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
            run_date=run_date,
        )
        log_step(
            "SparkApplication manifest를 구성했습니다",
            namespace=NAMESPACE,
            app_name=SPARK_APP_NAME,
            image=spec["image"],
            main_file=spec["mainApplicationFile"],
            output_uri=OUTPUT_URI,
            driver_memory=spec["driver"]["memory"],
            executor_instances=spec["executor"]["instances"],
            executor_memory=spec["executor"]["memory"],
        )

        # 같은 이름의 SparkApplication은 Kubernetes에 하나만 존재할 수 있다.
        # 이전 수동 실행 리소스가 남아 있으면 삭제한 뒤 새 실행을 제출한다.
        try:
            log_step("이전 SparkApplication 정리를 시도합니다", app_name=SPARK_APP_NAME)
            api.delete_namespaced_custom_object(
                group=SPARK_API_GROUP,
                version=SPARK_API_VERSION,
                namespace=NAMESPACE,
                plural=SPARK_PLURAL,
                name=SPARK_APP_NAME,
            )
            time.sleep(5)
            log_step("이전 SparkApplication을 정리했습니다", app_name=SPARK_APP_NAME)
        except ApiException as exc:
            if exc.status != 404:
                raise
            log_step("정리할 이전 SparkApplication이 없습니다", app_name=SPARK_APP_NAME)

        api.create_namespaced_custom_object(
            group=SPARK_API_GROUP,
            version=SPARK_API_VERSION,
            namespace=NAMESPACE,
            plural=SPARK_PLURAL,
            body=manifest,
        )
        log_step("SparkApplication을 제출했습니다", app_name=SPARK_APP_NAME)

        terminal_states = {"COMPLETED", "FAILED", "FAILING", "SUBMISSION_FAILED"}
        last_state = None
        last_driver_pod = None
        while True:
            spark_app = api.get_namespaced_custom_object(
                group=SPARK_API_GROUP,
                version=SPARK_API_VERSION,
                namespace=NAMESPACE,
                plural=SPARK_PLURAL,
                name=SPARK_APP_NAME,
            )
            status = spark_app.get("status", {})
            state = status.get("applicationState", {}).get("state", "UNKNOWN")
            driver_info = status.get("driverInfo", {})
            driver_pod = driver_info.get("podName")

            if state != last_state:
                log_step(
                    "SparkApplication 상태가 변경되었습니다",
                    app_name=SPARK_APP_NAME,
                    state=state,
                    driver_pod=driver_pod,
                    web_ui=driver_info.get("webUIAddress"),
                )
                last_state = state

            if driver_pod:
                if driver_pod != last_driver_pod:
                    driver_log_relay.reset()
                    log_step("Spark driver pod 로그 중계를 시작합니다", driver_pod=driver_pod)
                    last_driver_pod = driver_pod
                driver_log_relay.relay(api=pod_api, pod_name=driver_pod)

            if state in terminal_states:
                if driver_pod:
                    driver_log_relay.relay(api=pod_api, pod_name=driver_pod)
                if state != "COMPLETED":
                    log_step("SparkApplication이 실패했습니다", app_name=SPARK_APP_NAME, state=state)
                    raise RuntimeError(f"SparkApplication failed: {status}")
                log_step("SparkApplication이 정상 완료되었습니다", app_name=SPARK_APP_NAME, state=state)
                emit_output_asset_event(
                    outlet_events=outlet_events,
                    asset=DAILY_SALES_ASSET,
                    run_date=run_date,
                    output_uri=OUTPUT_URI,
                    spark_application=SPARK_APP_NAME,
                    spark_state=state,
                )
                emit_output_asset_event(
                    outlet_events=outlet_events,
                    asset=DAILY_SALES_QUALITY_ASSET,
                    run_date=run_date,
                    output_uri=QUALITY_URI,
                    spark_application=SPARK_APP_NAME,
                    spark_state=state,
                    extra={"quality_for_asset": DAILY_SALES_ASSET.uri},
                )
                return state

            time.sleep(10)

    submit_and_wait()


pyspark_lab_daily_sales()
