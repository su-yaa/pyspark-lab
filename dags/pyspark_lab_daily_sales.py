from __future__ import annotations

import time
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

NAMESPACE = "data-lab"
SPARK_APP_NAME = "pyspark-lab-daily-sales"
SPARK_API_GROUP = "sparkoperator.k8s.io"
SPARK_API_VERSION = "v1beta2"
SPARK_PLURAL = "sparkapplications"


def build_spark_application(run_date: str, image: str) -> dict:
    """Spark Operator에 제출할 실행 계약을 만든다.

    실무에서는 DAG가 Spark 코드를 직접 import해서 실행하기보다, 검증된
    컨테이너 이미지와 실행 파라미터를 SparkApplication으로 넘기는 편이
    운영 경계가 명확하다.
    """

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
                "s3a://pyspark-lab/daily-sales",
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
    config.load_incluster_config()
    return client.CustomObjectsApi()


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
        run_date = context.get("ds") or pendulum.now("UTC").to_date_string()
        image = "ghcr.io/su-yaa/pyspark-lab:main"
        api = spark_api()
        manifest = build_spark_application(run_date=run_date, image=image)

        try:
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

        api.create_namespaced_custom_object(
            group=SPARK_API_GROUP,
            version=SPARK_API_VERSION,
            namespace=NAMESPACE,
            plural=SPARK_PLURAL,
            body=manifest,
        )

        terminal_states = {"COMPLETED", "FAILED", "FAILING", "SUBMISSION_FAILED"}
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
            print(f"SparkApplication {SPARK_APP_NAME} state={state}")

            if state in terminal_states:
                if state != "COMPLETED":
                    raise RuntimeError(f"SparkApplication failed: {status}")
                return state

            time.sleep(10)

    submit_and_wait()


pyspark_lab_daily_sales()
