"""Run the shuffle or skew experiment used by the Spark lab."""

import argparse
import random

from pyspark.sql import SparkSession
from pyspark.sql import functions as functions


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["uniform", "skew"], default="uniform")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--partitions", type=int, default=20)
    parser.add_argument("--hold", action="store_true", help="keep the UI alive until Enter")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(f"academy-{args.mode}")
        .config("spark.sql.shuffle.partitions", str(args.partitions))
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    rows = []
    for index in range(args.rows):
        if args.mode == "skew":
            customer = (
                "cust_0042"
                if random.random() < 0.8
                else f"cust_{random.randint(0, 999):04d}"
            )
        else:
            customer = f"cust_{index % 1000:04d}"
        rows.append((customer, f"service_{index % 10}", random.randint(10, 500)))

    events = spark.createDataFrame(rows, ["customer_id", "service", "latency_ms"])
    result = events.groupBy("customer_id").agg(
        functions.count("*").alias("records"),
        functions.avg("latency_ms").alias("average_latency"),
    )
    result.write.mode("overwrite").parquet("/tmp/dea_spark_output")
    result.orderBy(functions.desc("records")).show(10, truncate=False)
    print("PASS: completed aggregation; Spark UI is http://localhost:4040")
    if args.hold:
        input("Inspect the completed stage, then press Enter to stop Spark: ")
    spark.stop()


if __name__ == "__main__":
    main()
