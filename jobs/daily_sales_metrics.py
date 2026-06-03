from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from pyspark_lab.config import DailySalesJobConfig
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
    args = parser.parse_args()

    return DailySalesJobConfig(
        run_date=date.fromisoformat(args.run_date),
        output_uri=args.output_uri,
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

        orders_df = spark.createDataFrame(SAMPLE_ORDERS, schema=schema)

        # The filters and grouping below are the production-shape part of this
        # example: Airflow controls the run date, Spark performs the distributed
        # aggregation, and the output path is partitioned for repeated runs.
        metrics_df = (
            orders_df.where(F.col("order_date") == F.lit(config.run_date.isoformat()))
            .groupBy("order_date", "region", "channel")
            .agg(
                F.count("*").alias("order_count"),
                F.countDistinct("customer_id").alias("customer_count"),
                F.sum("amount").alias("gross_sales"),
                F.round(F.avg("amount"), 2).alias("average_order_value"),
            )
            .withColumnRenamed("order_date", "run_date")
            .orderBy("region", "channel")
        )

        metrics_df.show(truncate=False)
        metrics_df.coalesce(1).write.mode("overwrite").json(config.output_path)
        print(f"Wrote daily sales metrics to {config.output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

