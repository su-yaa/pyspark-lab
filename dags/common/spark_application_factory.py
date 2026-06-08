from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

SPARK_API_GROUP = "sparkoperator.k8s.io"
SPARK_API_VERSION = "v1beta2"
SPARK_PLURAL = "sparkapplications"


@dataclass(frozen=True)
class SparkJobSpec:
    """Airflow DAG가 Spark Operator에 넘길 PySpark job 실행 계약."""

    name: str
    namespace: str
    image: str
    main_application_file: str
    arguments: list[str]
    run_date: str
    app_label: str = "pyspark-lab"
    image_pull_secret: str = "ghcr-pyspark-lab"
    service_account: str = "spark"
    driver_memory: str = "512m"
    executor_memory: str = "512m"
    executor_instances: int = 1
    node_selector: dict[str, str] = field(
        default_factory=lambda: {"kubernetes.io/hostname": "ubuntu"}
    )


def build_pyspark_application(spec: SparkJobSpec) -> dict[str, Any]:
    """공통 SparkApplication manifest를 만든다.

    DAG별로 달라지는 값은 `SparkJobSpec`으로 받고, MinIO/S3A, event log,
    Secret, worker node 고정 같은 클러스터 공통 실행값은 이 함수에 모은다.
    """

    return {
        "apiVersion": f"{SPARK_API_GROUP}/{SPARK_API_VERSION}",
        "kind": "SparkApplication",
        "metadata": {
            "name": spec.name,
            "namespace": spec.namespace,
            "labels": {
                "app.kubernetes.io/name": spec.app_label,
                "app.kubernetes.io/managed-by": "airflow",
                "pyspark-lab/run-date": spec.run_date,
            },
        },
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": spec.image,
            "imagePullPolicy": "Always",
            "imagePullSecrets": [spec.image_pull_secret],
            "mainApplicationFile": spec.main_application_file,
            "arguments": spec.arguments,
            "sparkVersion": "3.5.1",
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 3600,
            "nodeSelector": spec.node_selector,
            "sparkConf": _spark_conf(spec.image_pull_secret),
            "volumes": [
                {
                    "name": "event-logs",
                    "persistentVolumeClaim": {
                        "claimName": "spark-event-logs",
                    },
                },
            ],
            "driver": _pod_spec(
                app_label=spec.app_label,
                service_account=spec.service_account,
                memory=spec.driver_memory,
            ),
            "executor": _pod_spec(
                app_label=spec.app_label,
                service_account=None,
                memory=spec.executor_memory,
                instances=spec.executor_instances,
            ),
        },
    }


def _spark_conf(image_pull_secret: str) -> dict[str, str]:
    return {
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "file:/opt/spark-events",
        "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        "spark.kubernetes.container.image.pullSecrets": image_pull_secret,
        "spark.kubernetes.driverEnv.PYTHONPATH": "/opt/spark/work-dir/src",
        "spark.executorEnv.PYTHONPATH": "/opt/spark/work-dir/src",
    }


def _pod_spec(
    app_label: str,
    service_account: str | None,
    memory: str,
    instances: int | None = None,
) -> dict[str, Any]:
    pod_spec: dict[str, Any] = {
        "cores": 1,
        "coreRequest": "250m",
        "memory": memory,
        "labels": {"app.kubernetes.io/name": app_label},
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
    }

    if service_account:
        pod_spec["serviceAccount"] = service_account
    if instances is not None:
        pod_spec["instances"] = instances

    return pod_spec
