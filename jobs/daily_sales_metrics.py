from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from pyspark_lab.config import DailySalesJobConfig
from pyspark_lab.quality import require_minimum_rows
from pyspark_lab.sample_data import SAMPLE_ORDERS


def log_step(message: str, **details: object) -> None:
    """Airflow/Spark 로그에서 실행 흐름을 한 줄씩 추적하기 위한 공통 로그 함수."""

    suffix = ""
    if details:
        rendered_details = ", ".join(
            f"{key}={value}" for key, value in details.items() if value is not None
        )
        if rendered_details:
            suffix = f" | {rendered_details}"

    print(f"[pyspark-lab] {message}{suffix}", flush=True)


def parse_args() -> DailySalesJobConfig:
    """SparkApplication에서 전달한 CLI 인자를 작업 설정 객체로 변환한다."""

    parser = argparse.ArgumentParser(
        description="Build daily sales metrics from sample order events.",
    )
    parser.add_argument(
        "--run-date",
        required=True,
        help="Business date to aggregate, formatted as YYYY-MM-DD.",
    )
    parser.add_argument(
        "--output-uri",
        default="file:/tmp/pyspark-lab/daily-sales",
        help="Output base URI. Airflow sets this to the MinIO/S3 daily sales path.",
    )
    parser.add_argument(
        "--input-uri",
        default=None,
        help="Optional JSON input URI. If omitted, the built-in sample orders are used.",
    )
    parser.add_argument(
        "--min-orders",
        type=int,
        default=1,
        help="Minimum source rows required for the run date before metrics are written.",
    )
    args = parser.parse_args()

    return DailySalesJobConfig(
        run_date=date.fromisoformat(args.run_date),
        output_uri=args.output_uri,
        input_uri=args.input_uri,
        min_orders=args.min_orders,
    )


def main() -> None:
    config = parse_args()
    log_step(
        "1/8 실행 파라미터를 해석했습니다",
        run_date=config.run_date.isoformat(),
        input_uri=config.input_uri or "내장 샘플 데이터",
        output_uri=config.output_uri,
        min_orders=config.min_orders,
    )

    # SparkSession은 이 작업의 진입점이다. 여기부터 DataFrame 연산이 Spark driver와
    # executor에서 실행되며, Airflow는 이 프로세스의 완료 여부만 감시한다.
    spark = (
        SparkSession.builder.appName("pyspark-lab-daily-sales-metrics")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    log_step(
        "2/8 SparkSession을 생성했습니다",
        app_name=spark.sparkContext.appName,
        spark_version=spark.version,
        timezone="UTC",
    )

    try:
        # 실무에서는 입력 데이터 스키마를 명시해 타입 흔들림을 줄인다.
        # 이 예제도 JSON 입력 또는 샘플 데이터를 같은 스키마로 읽는다.
        schema = StructType(
            [
                StructField("order_id", StringType(), nullable=False),
                StructField("order_date", DateType(), nullable=False),
                StructField("region", StringType(), nullable=False),
                StructField("channel", StringType(), nullable=False),
                StructField("customer_id", StringType(), nullable=False),
                StructField("amount", IntegerType(), nullable=False),
            ]
        )

        if config.input_uri:
            orders_df = spark.read.schema(schema).json(config.input_uri)
            input_source = config.input_uri
        else:
            orders_df = spark.createDataFrame(SAMPLE_ORDERS, schema=schema)
            input_source = "내장 샘플 데이터"
        log_step("3/8 주문 데이터를 읽었습니다", source=input_source)

        # Airflow가 결정한 run_date만 처리한다. 재실행 시 같은 날짜 파티션만 덮어써서
        # 결과 비교와 장애 재처리가 쉬운 구조를 만든다.
        scoped_orders_df = orders_df.where(
            F.col("order_date") == F.lit(config.run_date.isoformat())
        )

        source_row_count = scoped_orders_df.count()
        log_step(
            "4/8 실행일 기준 주문 데이터를 선별했습니다",
            run_date=config.run_date.isoformat(),
            source_row_count=source_row_count,
        )

        # 품질검사는 지표 저장 전에 실행한다. 실패해도 검사 결과 파일은 먼저 남겨
        # Airflow 실패 원인을 MinIO에서 확인할 수 있게 한다.
        quality_result = require_minimum_rows(
            check_name="orders_available_for_run_date",
            observed_value=source_row_count,
            threshold=config.min_orders,
        )

        quality_df = spark.createDataFrame(
            [
                {
                    "run_date": config.run_date.isoformat(),
                    "check_name": quality_result.check_name,
                    "passed": quality_result.passed,
                    "observed_value": quality_result.observed_value,
                    "threshold": quality_result.threshold,
                }
            ]
        )
        quality_df.write.mode("overwrite").json(config.quality_path)
        log_step(
            "5/8 품질검사 결과를 저장했습니다",
            check_name=quality_result.check_name,
            passed=quality_result.passed,
            observed_value=quality_result.observed_value,
            threshold=quality_result.threshold,
            quality_path=config.quality_path,
        )

        if not quality_result.passed:
            raise RuntimeError(
                "Data quality failed: "
                f"{quality_result.check_name} observed={quality_result.observed_value} "
                f"threshold={quality_result.threshold}"
            )

        # 지표 계산은 Spark DataFrame API로 수행한다. groupBy/agg 단계가 실제
        # 분산 처리의 핵심이며, pure Python 테스트 코드는 같은 비즈니스 규칙을
        # 빠르게 검증하기 위한 보조 수단이다.
        log_step("6/8 지역/채널별 일매출 지표 집계를 시작합니다")
        metrics_df = (
            scoped_orders_df
            .groupBy("order_date", "region", "channel")
            .agg(
                F.count("*").alias("order_count"),
                F.countDistinct("customer_id").alias("customer_count"),
                F.sum("amount").alias("gross_sales"),
                F.round(F.avg("amount"), 2).alias("average_order_value"),
            )
            .withColumnRenamed("order_date", "run_date")
            .withColumn("pipeline_name", F.lit("pyspark_lab_daily_sales"))
            .withColumn("processed_at_utc", F.current_timestamp())
            .orderBy("region", "channel")
        )

        metric_row_count = metrics_df.count()
        log_step("7/8 지표 집계를 완료했습니다", metric_row_count=metric_row_count)

        # coalesce(1)은 연습 환경에서 결과 파일을 확인하기 쉽게 만들기 위한 선택이다.
        # 대용량 실무 환경에서는 병렬성을 유지하도록 파티션 수를 별도로 설계한다.
        metrics_df.show(truncate=False)
        metrics_df.coalesce(1).write.mode("overwrite").json(config.output_path)
        log_step("8/8 매출 지표를 MinIO/S3 경로에 저장했습니다", output_path=config.output_path)
    finally:
        log_step("SparkSession을 종료합니다")
        spark.stop()


if __name__ == "__main__":
    main()
