from __future__ import annotations

import time
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
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
SPARK_APP_NAME = "pyspark-lab-legacy-sales"
SPARK_IMAGE = "ghcr.io/su-yaa/pyspark-lab:main"
MAIN_APPLICATION_FILE = "local:///opt/spark/work-dir/spark/jobs/daily_sales/metrics.py"
OUTPUT_URI = "s3a://pyspark-lab/daily-sales"
DEFAULT_SAMPLE_RUN_DATE = "2026-06-03"
SPARK_DRIVER_CONTAINER = "spark-kubernetes-driver"
DRIVER_LOG_TAIL_LINES = 200


def log_step(message: str, **details: object) -> None:
    """Airflow task 로그에서 Spark 제출 흐름을 한 줄씩 추적하기 위한 함수."""
    suffix = ""
    if details:
        rendered_details = ", ".join(
            f"{key}={value}" for key, value in details.items() if value is not None
        )
        if rendered_details:
            suffix = f" | {rendered_details}"
    print(f"[pyspark-lab-legacy-dag] {message}{suffix}", flush=True)


def resolve_run_date(**context) -> str:
    """DAG 실행일을 Spark job 파라미터로 변환한다."""
    dag_run = context.get("dag_run")
    dag_conf = getattr(dag_run, "conf", {}) or {}
    configured_run_date = dag_conf.get("run_date")
    if configured_run_date:
        return str(configured_run_date)

    run_type = str(getattr(dag_run, "run_type", "")).lower()
    if "manual" in run_type:
        return DEFAULT_SAMPLE_RUN_DATE

    return context.get("ds") or DEFAULT_SAMPLE_RUN_DATE


def submit_spark_job(**context) -> None:
    """PythonOperator에서 호출되어 SparkApplication 리소스를 생성하고 폴링하여 기다리는 함수."""
    run_date = resolve_run_date(**context)
    config.load_incluster_config()
    api = client.CustomObjectsApi()
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

    # 1. 이전 실행 정리
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
    except ApiException as exc:
        if exc.status != 404:
            raise
        log_step("정리할 이전 SparkApplication이 없습니다", app_name=SPARK_APP_NAME)

    # 2. SparkApplication 제출
    api.create_namespaced_custom_object(
        group=SPARK_API_GROUP,
        version=SPARK_API_VERSION,
        namespace=NAMESPACE,
        plural=SPARK_PLURAL,
        body=manifest,
    )
    log_step("SparkApplication을 제출했습니다", app_name=SPARK_APP_NAME)

    # 3. 완료 상태 폴링 및 로그 릴레이
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
        driver_pod = status.get("driverInfo", {}).get("podName")

        if state != last_state:
            log_step(
                "SparkApplication 상태가 변경되었습니다",
                app_name=SPARK_APP_NAME,
                state=state,
                driver_pod=driver_pod,
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
                raise RuntimeError(f"SparkApplication failed with state: {state}")
            log_step("SparkApplication이 성공적으로 완료되었습니다", app_name=SPARK_APP_NAME)
            break

        time.sleep(10)


# 클래식(레거시) DAG 설정 및 >> 사용법 예시
with DAG(
    dag_id="pyspark_lab_legacy",
    description="A legacy Airflow DAG running PySpark job using >> operator",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["legacy", "pyspark", "spark-operator"],
) as dag:

    start_task = EmptyOperator(task_id="start")

    # PythonOperator를 사용해 Spark Job 실행 태스크 정의
    spark_job_task = PythonOperator(
        task_id="run_spark_job",
        python_callable=submit_spark_job,
    )

    end_task = EmptyOperator(task_id="end")

    # >> 문법을 사용한 태스크 의존성(Flow) 구성
    start_task >> spark_job_task >> end_task
