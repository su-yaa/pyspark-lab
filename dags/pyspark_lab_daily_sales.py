from __future__ import annotations

import time
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from kubernetes_log_relay import PodLogRelay, build_core_api

NAMESPACE = "data-lab"
SPARK_APP_NAME = "pyspark-lab-daily-sales"
SPARK_API_GROUP = "sparkoperator.k8s.io"
SPARK_API_VERSION = "v1beta2"
SPARK_PLURAL = "sparkapplications"
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

    print(f"[pyspark-lab-dag] {message}{suffix}", flush=True)


def build_spark_application(run_date: str, image: str) -> dict:
    """Spark Operator에 제출할 실행 계약을 만든다.

    실무에서는 DAG가 Spark 코드를 직접 import해서 실행하기보다, 검증된
    컨테이너 이미지와 실행 파라미터를 SparkApplication으로 넘기는 편이
    운영 경계가 명확하다.
    """

    output_uri = "s3a://pyspark-lab/daily-sales"

    return {
        "apiVersion": f"{SPARK_API_GROUP}/{SPARK_API_VERSION}",
        "kind": "SparkApplication",
        "metadata": {
            "name": SPARK_APP_NAME,
            "namespace": NAMESPACE,
            "labels": {
                "app.kubernetes.io/name": "pyspark-lab",
                "app.kubernetes.io/managed-by": "airflow",
                "pyspark-lab/run-date": run_date,
            },
        },
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": image,
            "imagePullPolicy": "Always",
            "imagePullSecrets": ["ghcr-pyspark-lab"],
            "mainApplicationFile": "local:///opt/spark/work-dir/jobs/daily_sales_metrics.py",
            "arguments": [
                "--run-date",
                run_date,
                "--output-uri",
                output_uri,
                "--min-orders",
                "1",
            ],
            "sparkVersion": "3.5.1",
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 3600,
            "nodeSelector": {
                "kubernetes.io/hostname": "ubuntu",
            },
            "sparkConf": {
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": "file:/opt/spark-events",
                # S3A connector는 이미지에 굽지 않고 Spark submit 시점에 받아서
                # 이미지 빌드 속도와 실행 의존성 관리를 분리한다.
                "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
                "spark.jars.ivy": "/tmp/.ivy2",
                # Spark 결과 파일은 MinIO의 S3 호환 API로 저장한다.
                # 인증값은 Secret에서 환경변수로 주입하므로 로그에 남기지 않는다.
                "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
                "spark.kubernetes.container.image.pullSecrets": "ghcr-pyspark-lab",
                "spark.kubernetes.driverEnv.PYTHONPATH": "/opt/spark/work-dir/src",
                "spark.executorEnv.PYTHONPATH": "/opt/spark/work-dir/src",
            },
            "volumes": [
                {
                    "name": "event-logs",
                    "persistentVolumeClaim": {
                        "claimName": "spark-event-logs",
                    },
                },
            ],
            "driver": {
                "serviceAccount": "spark",
                "cores": 1,
                "coreRequest": "250m",
                "memory": "512m",
                "labels": {"app.kubernetes.io/name": "pyspark-lab"},
                "envSecretKeyRefs": {
                    "AWS_ACCESS_KEY_ID": {
                        "name": "spark-minio-credentials",
                        "key": "AWS_ACCESS_KEY_ID",
                    },
                    "AWS_SECRET_ACCESS_KEY": {
                        "name": "spark-minio-credentials",
                        "key": "AWS_SECRET_ACCESS_KEY",
                    },
                },
                "volumeMounts": [
                    {
                        "name": "event-logs",
                        "mountPath": "/opt/spark-events",
                    },
                ],
            },
            "executor": {
                "instances": 1,
                "cores": 1,
                "coreRequest": "250m",
                "memory": "512m",
                "labels": {"app.kubernetes.io/name": "pyspark-lab"},
                "envSecretKeyRefs": {
                    "AWS_ACCESS_KEY_ID": {
                        "name": "spark-minio-credentials",
                        "key": "AWS_ACCESS_KEY_ID",
                    },
                    "AWS_SECRET_ACCESS_KEY": {
                        "name": "spark-minio-credentials",
                        "key": "AWS_SECRET_ACCESS_KEY",
                    },
                },
                "volumeMounts": [
                    {
                        "name": "event-logs",
                        "mountPath": "/opt/spark-events",
                    },
                ],
            },
        },
    }


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
    @task
    def submit_and_wait() -> str:
        context = get_current_context()
        run_date = resolve_run_date(context)
        image = "ghcr.io/su-yaa/pyspark-lab:main"
        api = spark_api()
        pod_api = build_core_api()
        driver_log_relay = PodLogRelay(
            namespace=NAMESPACE,
            container=SPARK_DRIVER_CONTAINER,
            prefix="[spark-driver]",
            tail_lines=DRIVER_LOG_TAIL_LINES,
        )
        manifest = build_spark_application(run_date=run_date, image=image)
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
            output_uri=spec["arguments"][spec["arguments"].index("--output-uri") + 1],
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
                return state

            time.sleep(10)

    submit_and_wait()


pyspark_lab_daily_sales()
