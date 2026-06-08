from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from pyspark_lab.pipelines.join_flow.sample_data import JOIN_FLOW_ORDERS


def log_step(message: str, **details: object) -> None:
    suffix = ""
    if details:
        rendered_details = ", ".join(
            f"{key}={value}" for key, value in details.items() if value is not None
        )
        if rendered_details:
            suffix = f" | {rendered_details}"
    print(f"[pyspark-lab][join-flow][execution-2] {message}{suffix}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Join orders with customer dimension.")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--customer-dim-uri", required=True)
    parser.add_argument("--output-uri", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_date = date.fromisoformat(args.run_date)
    output_path = f"{args.output_uri}/run_date={run_date.isoformat()}"

    log_step(
        "1/6 실행 2 파라미터를 해석했습니다",
        run_date=run_date,
        customer_dim_uri=args.customer_dim_uri,
        output_path=output_path,
    )
    spark = (
        SparkSession.builder.appName("pyspark-lab-join-flow-orders-customers")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        order_schema = StructType(
            [
                StructField("order_id", StringType(), nullable=False),
                StructField("order_date", DateType(), nullable=False),
                StructField("customer_id", StringType(), nullable=False),
                StructField("region", StringType(), nullable=False),
                StructField("amount", IntegerType(), nullable=False),
            ]
        )
        orders_df = spark.createDataFrame(JOIN_FLOW_ORDERS, schema=order_schema)
        scoped_orders_df = orders_df.where(F.col("order_date") == F.lit(run_date))
        log_step("2/6 실행일 주문 데이터를 만들었습니다", order_count=scoped_orders_df.count())

        customer_dim_df = spark.read.json(args.customer_dim_uri)
        log_step("3/6 실행 1 결과 customer dimension을 읽었습니다", customer_count=customer_dim_df.count())
        
        log_step("### join 전 데이터 시각화 ###")
        
        log_step("orders_df preview")
        for row in scoped_orders_df.toJSON().collect():
            log_step("orders_df row", row=row)

        log_step("customer_dim_df preview")
        for row in customer_dim_df.toJSON().collect():
            log_step("customer_dim_df row", row=row)

        enriched_df = (
            scoped_orders_df.alias("orders")
            .join(customer_dim_df.alias("customers"), on="customer_id", how="left")
            .select(
                F.col("orders.order_id"),
                F.col("orders.order_date").alias("run_date"),
                F.col("orders.customer_id"),
                F.col("customers.customer_name"),
                F.col("customers.segment"),
                F.col("customers.signup_channel"),
                F.col("orders.region"),
                F.col("orders.amount"),
            )
            .withColumn("pipeline_name", F.lit("pyspark_lab_join_flow"))
            .withColumn("processed_at_utc", F.current_timestamp())
            .orderBy("order_id")
        )
        enriched_count = enriched_df.count()
        log_step("4/6 주문과 고객 dimension join을 완료했습니다", enriched_count=enriched_count)

        if enriched_df.where(F.col("customer_name").isNull()).count() > 0:
            raise RuntimeError("Join quality failed: customer dimension has unmatched orders")

        enriched_df.show(truncate=False)
        log_step("5/6 join 결과 저장을 시작합니다", output_path=output_path)
        enriched_df.coalesce(1).write.mode("overwrite").json(output_path)
        log_step("6/6 join 결과 저장을 완료했습니다", output_path=output_path)
    finally:
        log_step("SparkSession을 종료합니다")
        spark.stop()


if __name__ == "__main__":
    main()
