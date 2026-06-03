from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from pyspark_lab.config import DailySalesJobConfig
from pyspark_lab.quality import require_minimum_rows
from pyspark_lab.sample_data import SAMPLE_ORDERS


def parse_args() -> DailySalesJobConfig:
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
        help="Output base URI. Airflow sets this to the shared Spark event PVC path.",
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
    spark = (
        SparkSession.builder.appName("pyspark-lab-daily-sales-metrics")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
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
        else:
            orders_df = spark.createDataFrame(SAMPLE_ORDERS, schema=schema)

        scoped_orders_df = orders_df.where(
            F.col("order_date") == F.lit(config.run_date.isoformat())
        )

        source_row_count = scoped_orders_df.count()
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

        if not quality_result.passed:
            raise RuntimeError(
                "Data quality failed: "
                f"{quality_result.check_name} observed={quality_result.observed_value} "
                f"threshold={quality_result.threshold}"
            )

        # The filters and grouping below are the production-shape part of this
        # example: Airflow controls the run date, Spark performs the distributed
        # aggregation, and the output path is partitioned for repeated runs.
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

        metrics_df.show(truncate=False)
        metrics_df.coalesce(1).write.mode("overwrite").json(config.output_path)
        print(f"Wrote daily sales metrics to {config.output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
