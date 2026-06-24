from __future__ import annotations

import os
import shutil
import tempfile
from datetime import timedelta

import pendulum
from airflow.models import Variable
from airflow.sdk import dag, get_current_context, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# 기본 환경값 및 변수 기본값 정의
DEFAULT_AWS_CONN_ID = "aws_emr_test"
DEFAULT_RUN_DATE = "2026-06-03"

def log_step(message: str, **details: object) -> None:
    """Airflow task 로그에서 EMR 제출 흐름을 한 줄씩 추적하기 위한 함수."""
    suffix = ""
    if details:
        rendered_details = ", ".join(
            f"{key}={value}" for key, value in details.items() if value is not None
        )
        if rendered_details:
            suffix = f" | {rendered_details}"
    print(f"[pyspark-lab-emr] {message}{suffix}", flush=True)


def resolve_s3_bucket(dag_conf: dict) -> str:
    """DAG conf, Airflow Variable, AWS Connection Extra 순으로 S3 버킷명을 안전하게 탐색합니다."""
    # 1. DAG 실행 시 넘겨준 conf 확인
    s3_bucket = dag_conf.get("s3_bucket")
    
    # 2. Airflow Variable 확인
    if not s3_bucket:
        try:
            s3_bucket = Variable.get("pyspark_lab_s3_bucket", default_var=None)
        except Exception:
            s3_bucket = None
            
    # 3. AWS Connection ('aws_emr_test')의 Extra JSON 필드 확인
    if not s3_bucket:
        try:
            from airflow.hooks.base import BaseHook
            conn = BaseHook.get_connection(DEFAULT_AWS_CONN_ID)
            s3_bucket = (conn.extra_dejson or {}).get("s3_bucket")
        except Exception:
            s3_bucket = None
            
    if not s3_bucket:
        raise ValueError(
            "S3 bucket name must be provided via: "
            "1) DAG conf ('s3_bucket'), "
            "2) Airflow Variable ('pyspark_lab_s3_bucket'), "
            "or 3) AWS Connection extra ('s3_bucket')."
        )
        
    return str(s3_bucket)


@dag(
    dag_id="pyspark_lab_aws_emr",
    description="Provision an EMR on EC2 cluster (Primary, Core, Task nodes) and run Spark step with monitoring options.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    tags=["data-lab", "pyspark", "aws", "emr-ec2"],
)
def pyspark_lab_aws_emr():
    
    @task
    def prepare_and_upload_assets() -> dict[str, str]:
        """분산 실행 환경(KubernetesExecutor 등)에서 로컬 파일 유실을 막기 위해,
        단일 태스크 내부에서 코드 ZIP 패키징과 S3 업로드(boto3 리전 명시)를 한 번에 수행합니다.
        """
        log_step("1. 로컬 소스 코드 패키징 및 S3 업로드를 시작합니다.")
        
        # [1] 소스 코드 압축 (ZIP)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, ".."))
        src_dir = os.path.join(project_root, "src")
        
        if not os.path.exists(src_dir):
            raise FileNotFoundError(f"Source directory not found: {src_dir}")
            
        temp_dir = tempfile.gettempdir()
        zip_output_name = os.path.join(temp_dir, "pyspark_lab")
        
        archive_path = shutil.make_archive(
            base_name=zip_output_name,
            format="zip",
            root_dir=src_dir
        )
        log_step("로컬 소스 코드를 ZIP으로 패키징했습니다.", archive_path=archive_path)

        # [2] 버킷명 확인
        context = get_current_context()
        dag_run = context.get("dag_run")
        dag_conf = getattr(dag_run, "conf", {}) or {}
        s3_bucket = resolve_s3_bucket(dag_conf)

        # [3] Airflow Connection에서 자격 증명 획득 및 boto3 클라이언트 생성 (명시적 서울 리전 지정)
        from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
        import boto3
        
        aws_hook = AwsGenericHook(aws_conn_id=DEFAULT_AWS_CONN_ID, client_type="s3")
        credentials = aws_hook.get_credentials()
        
        # 보안 마스킹 처리하여 가져온 Access Key ID의 앞부분 5글자 출력 테스트
        access_key_preview = credentials.access_key[:5] + "..." if credentials and credentials.access_key else "None"
        log_step(f"사용하려는 Access Key ID 앞부분: {access_key_preview}")
        
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            region_name="ap-northeast-2"
        )

        # [4] ZIP 파일 업로드
        zip_s3_key = "pyspark-lab/src/pyspark_lab.zip"
        log_step(f"ZIP 업로드 시작: {archive_path} -> s3://{s3_bucket}/{zip_s3_key}")
        s3_client.upload_file(archive_path, s3_bucket, zip_s3_key)
        
        # 임시 생성된 ZIP 파일 삭제하여 용량 정리
        if os.path.exists(archive_path):
            os.remove(archive_path)

        # [5] metrics.py 스크립트 파일 업로드
        local_script_path = os.path.join(project_root, "spark/jobs/daily_sales/metrics.py")
        script_s3_key = "pyspark-lab/jobs/metrics.py"
        log_step(f"스크립트 업로드 시작: {local_script_path} -> s3://{s3_bucket}/{script_s3_key}")
        s3_client.upload_file(local_script_path, s3_bucket, script_s3_key)

        log_step("모든 리소스 파일이 S3에 정상적으로 업로드 완료되었습니다.")
        return {
            "zip_s3_key": zip_s3_key,
            "script_s3_key": script_s3_key
        }

    @task
    def prepare_job_flow_overrides() -> dict:
        """EMR on EC2 클러스터 생성(Primary, Core, Task 노드 구성)을 위한 명세를 동적으로 생성합니다."""
        context = get_current_context()
        dag_run = context.get("dag_run")
        dag_conf = getattr(dag_run, "conf", {}) or {}
        
        s3_bucket = resolve_s3_bucket(dag_conf)
            
        # 모니터링 실습을 위해 기본적으로 완료 후에도 수동 종료시까지 유지(True)하도록 디폴트 설정
        keep_alive = dag_conf.get("keep_job_flow_alive", True)
        
        master_type = dag_conf.get("master_instance_type", "m5.xlarge")
        core_type = dag_conf.get("core_instance_type", "m5.xlarge")
        core_count = int(dag_conf.get("core_instance_count", 2))
        task_type = dag_conf.get("task_instance_type", "m5.xlarge")
        task_count = int(dag_conf.get("task_instance_count", 1))

        release_label = dag_conf.get("emr_release_label", "emr-6.13.0")
        job_flow_role = dag_conf.get("emr_ec2_role", "EMR_EC2_DefaultRole")
        service_role = dag_conf.get("emr_service_role", "EMR_DefaultRole")

        overrides = {
            "Name": f"pyspark-lab-emr-{context['ds']}",
            "ReleaseLabel": release_label,
            "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
            "Instances": {
                "InstanceGroups": [
                    {
                        "Name": "Primary node (Master)",
                        "Market": "SPOT",
                        "InstanceRole": "MASTER",
                        "InstanceType": master_type,
                        "InstanceCount": 1,
                    },
                    {
                        "Name": "Core nodes",
                        "Market": "SPOT",
                        "InstanceRole": "CORE",
                        "InstanceType": core_type,
                        "InstanceCount": core_count,
                    },
                    {
                        "Name": "Task nodes",
                        "Market": "SPOT",
                        "InstanceRole": "TASK",
                        "InstanceType": task_type,
                        "InstanceCount": task_count,
                    },
                ],
                "KeepJobFlowAliveWhenNoSteps": keep_alive,
                "TerminationProtected": False,
                "Ec2SubnetId": dag_conf.get("ec2_subnet_id", ""),
            },
            "JobFlowRole": job_flow_role,
            "ServiceRole": service_role,
            "LogUri": f"s3://{s3_bucket}/pyspark-lab/emr-logs/",
        }

        # Subnet ID가 정의되지 않았다면 삭제하여 AWS 디폴트 VPC 서브넷을 쓰게 함
        if not overrides["Instances"]["Ec2SubnetId"]:
            overrides["Instances"].pop("Ec2SubnetId")

        log_step(
            "EMR 클러스터 생성 명세를 구성했습니다",
            release_label=release_label,
            master_type=master_type,
            core_type=core_type,
            core_count=core_count,
            task_type=task_type,
            task_count=task_count,
            keep_job_flow_alive=keep_alive
        )
        return overrides

    # EMR on EC2 클러스터 생성
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides="{{ task_instance.xcom_pull(task_ids='prepare_job_flow_overrides') }}",
        aws_conn_id=DEFAULT_AWS_CONN_ID,
    )

    @task
    def prepare_spark_step() -> list[dict]:
        """EMR Spark Step 명세를 구성합니다."""
        context = get_current_context()
        dag_run = context.get("dag_run")
        dag_conf = getattr(dag_run, "conf", {}) or {}
        
        s3_bucket = resolve_s3_bucket(dag_conf)
        run_date = dag_conf.get("run_date") or DEFAULT_RUN_DATE

        entrypoint = f"s3://{s3_bucket}/pyspark-lab/jobs/metrics.py"
        py_files = f"s3://{s3_bucket}/pyspark-lab/src/pyspark_lab.zip"
        output_uri = f"s3://{s3_bucket}/pyspark-lab/output/daily-sales"

        return [
            {
                "Name": "Run daily sales metrics job",
                "ActionOnFailure": "CONTINUE",  # 모니터링 실습을 위해 실패해도 즉시 클러스터가 깨지지 않게 CONTINUE
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode", "client",
                        "--py-files", py_files,
                        "--conf", "spark.sql.session.timeZone=UTC",
                        entrypoint,
                        "--run-date", run_date,
                        "--output-uri", output_uri,
                        "--min-orders", "1",
                    ],
                },
            }
        ]

    # EMR 클러스터에 Step 추가
    add_spark_step = EmrAddStepsOperator(
        task_id="add_spark_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        steps="{{ task_instance.xcom_pull(task_ids='prepare_spark_step') }}",
        aws_conn_id=DEFAULT_AWS_CONN_ID,
    )

    # Step 상태 센싱 및 완료 대기
    watch_spark_step = EmrStepSensor(
        task_id="watch_spark_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_step')[0] }}",
        aws_conn_id=DEFAULT_AWS_CONN_ID,
    )

    @task.branch
    def check_keep_alive() -> str:
        """keep_job_flow_alive 설정에 따라 클러스터를 종료할지, 유지할지 분기합니다."""
        context = get_current_context()
        dag_run = context.get("dag_run")
        dag_conf = getattr(dag_run, "conf", {}) or {}
        
        # 수동으로 종료할 때까지 클러스터를 유지하고 모니터링하는 것이 실습 목적이므로 기본값을 True로 제공
        keep_alive = dag_conf.get("keep_job_flow_alive", True)
        
        if keep_alive:
            log_step("keep_job_flow_alive 가 True입니다. EMR 클러스터를 종료하지 않고 수동 분석용으로 유지합니다.")
            return "keep_alive_skip"
        else:
            log_step("keep_job_flow_alive 가 False입니다. EMR 클러스터를 즉시 반납합니다.")
            return "terminate_emr_cluster"

    keep_alive_skip = EmptyOperator(
        task_id="keep_alive_skip"
    )

    # EMR 클러스터 반납 태스크
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        aws_conn_id=DEFAULT_AWS_CONN_ID,
        trigger_rule="one_success",  # 분기 혹은 에러가 발생해도 명시적으로 False면 반납하도록 룰 지정
    )

    # 태스크 흐름 정의
    upload_assets = prepare_and_upload_assets()
    overrides_task = prepare_job_flow_overrides()
    step_spec_task = prepare_spark_step()

    # 의존 파일 업로드 후 클러스터에 Step 추가 가능
    upload_assets >> add_spark_step

    # 클러스터 라이프사이클 관리
    overrides_task >> create_emr_cluster >> add_spark_step >> watch_spark_step
    step_spec_task >> add_spark_step

    # Step 센싱 완료 후 분기 판단
    branch_task = check_keep_alive()
    watch_spark_step >> branch_task
    branch_task >> [keep_alive_skip, terminate_emr_cluster]


pyspark_lab_aws_emr()
