from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pyspark_lab.pipelines.join_flow.sample_data import JOIN_FLOW_CUSTOMERS


def log_step(message: str, **details: object) -> None:
    suffix = ""
    if details:
        rendered_details = ", ".join(
            f"{key}={value}" for key, value in details.items() if value is not None
        )
        if rendered_details:
            suffix = f" | {rendered_details}"
    print(f"[pyspark-lab][join-flow][execution-1] {message}{suffix}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare customer dimension for join flow.")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--output-uri", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_date = date.fromisoformat(args.run_date)
    output_path = f"{args.output_uri}/run_date={run_date.isoformat()}"

    log_step("1/4 실행 1 파라미터를 해석했습니다", run_date=run_date, output_path=output_path)
    spark = (
        SparkSession.builder.appName("pyspark-lab-join-flow-prepare-customers")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        schema = StructType(
            [
                StructField("customer_id", StringType(), nullable=False),
                StructField("customer_name", StringType(), nullable=False),
                StructField("segment", StringType(), nullable=False),
                StructField("signup_channel", StringType(), nullable=False),
            ]
        )
        customers_df = spark.createDataFrame(JOIN_FLOW_CUSTOMERS, schema=schema)
        log_step("2/4 고객 dimension 데이터를 만들었습니다", row_count=customers_df.count())

        customer_dim_df = customers_df.withColumn("snapshot_date", F.lit(run_date.isoformat()))
        customer_dim_df.show(truncate=False)
        log_step("3/4 고객 dimension 저장을 시작합니다", output_path=output_path)
        customer_dim_df.coalesce(1).write.mode("overwrite").json(output_path)
        log_step("4/4 고객 dimension 저장을 완료했습니다", output_path=output_path)
    finally:
        log_step("SparkSession을 종료합니다")
        spark.stop()


if __name__ == "__main__":
    main()
