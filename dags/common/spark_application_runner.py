from __future__ import annotations

import time
from dataclasses import dataclass

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


SPARK_DRIVER_CONTAINER = "spark-kubernetes-driver"
DRIVER_LOG_TAIL_LINES = 200
TERMINAL_STATES = {"COMPLETED", "FAILED", "FAILING", "SUBMISSION_FAILED"}


@dataclass(frozen=True)
class SparkRunResult:
    app_name: str
    state: str
    driver_pod: str | None


def build_spark_api() -> client.CustomObjectsApi:
    """Airflow pod의 ServiceAccount 권한으로 SparkApplication API에 접근한다."""

    config.load_incluster_config()
    return client.CustomObjectsApi()


def submit_and_wait_for_spark_application(
    spark_api: client.CustomObjectsApi,
    spark_job: SparkJobSpec,
    log_prefix: str,
    poll_seconds: int = 10,
) -> SparkRunResult:
    """SparkApplication을 제출하고 driver 로그를 Airflow 로그로 중계하며 완료까지 기다린다."""

    pod_api = build_core_api()
    driver_log_relay = PodLogRelay(
        namespace=spark_job.namespace,
        container=SPARK_DRIVER_CONTAINER,
        prefix="[spark-driver]",
        tail_lines=DRIVER_LOG_TAIL_LINES,
    )
    manifest = build_pyspark_application(spark_job)

    _log(log_prefix, "이전 SparkApplication 정리를 시도합니다", app_name=spark_job.name)
    try:
        spark_api.delete_namespaced_custom_object(
            group=SPARK_API_GROUP,
            version=SPARK_API_VERSION,
            namespace=spark_job.namespace,
            plural=SPARK_PLURAL,
            name=spark_job.name,
        )
        time.sleep(5)
        _log(log_prefix, "이전 SparkApplication을 정리했습니다", app_name=spark_job.name)
    except ApiException as exc:
        if exc.status != 404:
            raise
        _log(log_prefix, "정리할 이전 SparkApplication이 없습니다", app_name=spark_job.name)

    spark_api.create_namespaced_custom_object(
        group=SPARK_API_GROUP,
        version=SPARK_API_VERSION,
        namespace=spark_job.namespace,
        plural=SPARK_PLURAL,
        body=manifest,
    )
    _log(
        log_prefix,
        "SparkApplication을 제출했습니다",
        app_name=spark_job.name,
        main_file=spark_job.main_application_file,
        image=spark_job.image,
    )

    last_state = None
    last_driver_pod = None
    while True:
        spark_app = spark_api.get_namespaced_custom_object(
            group=SPARK_API_GROUP,
            version=SPARK_API_VERSION,
            namespace=spark_job.namespace,
            plural=SPARK_PLURAL,
            name=spark_job.name,
        )
        status = spark_app.get("status", {})
        state = status.get("applicationState", {}).get("state", "UNKNOWN")
        driver_info = status.get("driverInfo", {})
        driver_pod = driver_info.get("podName")

        if state != last_state:
            _log(
                log_prefix,
                "SparkApplication 상태가 변경되었습니다",
                app_name=spark_job.name,
                state=state,
                driver_pod=driver_pod,
                web_ui=driver_info.get("webUIAddress"),
            )
            last_state = state

        if driver_pod:
            if driver_pod != last_driver_pod:
                driver_log_relay.reset()
                _log(log_prefix, "Spark driver pod 로그 중계를 시작합니다", driver_pod=driver_pod)
                last_driver_pod = driver_pod
            driver_log_relay.relay(api=pod_api, pod_name=driver_pod)

        if state in TERMINAL_STATES:
            if driver_pod:
                driver_log_relay.relay(api=pod_api, pod_name=driver_pod)
            if state != "COMPLETED":
                _log(log_prefix, "SparkApplication이 실패했습니다", app_name=spark_job.name, state=state)
                raise RuntimeError(f"SparkApplication failed: {status}")
            _log(log_prefix, "SparkApplication이 정상 완료되었습니다", app_name=spark_job.name, state=state)
            return SparkRunResult(app_name=spark_job.name, state=state, driver_pod=driver_pod)

        time.sleep(poll_seconds)


def _log(prefix: str, message: str, **details: object) -> None:
    suffix = ""
    if details:
        rendered_details = ", ".join(
            f"{key}={value}" for key, value in details.items() if value is not None
        )
        if rendered_details:
            suffix = f" | {rendered_details}"
    print(f"{prefix} {message}{suffix}", flush=True)
