"""Verify the shuffle/skew prediction objectively instead of eyeballing the Spark UI.

Runs the same groupBy aggregation used by run_lab.py in both 'uniform' and
'skew' mode, reads back the per-key row counts from the Parquet output, and
asserts the property the lab asks you to predict: in skew mode one key
(cust_0042) dominates the aggregation output; in uniform mode no key does.

This does not inspect the Spark UI's task timings (that part still requires
looking at http://localhost:4040 yourself) — it verifies the *data-shape*
half of the prediction, which is what actually causes the skewed task.

Usage: python check_skew.py [--rows 200000]
"""

import argparse
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_mode(spark, mode, rows, out_path):
    data = []
    import random

    for index in range(rows):
        if mode == "skew":
            customer = "cust_0042" if random.random() < 0.8 else f"cust_{random.randint(0, 999):04d}"
        else:
            customer = f"cust_{index % 1000:04d}"
        data.append((customer, random.randint(10, 500)))

    events = spark.createDataFrame(data, ["customer_id", "latency_ms"])
    result = events.groupBy("customer_id").agg(F.count("*").alias("n"))
    shutil.rmtree(out_path, ignore_errors=True)
    result.write.mode("overwrite").parquet(out_path)
    return spark.read.parquet(out_path)


def dominance_ratio(df, rows):
    counts = [row["n"] for row in df.collect()]
    if not counts:
        return 0.0
    return max(counts) / rows


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--uniform-ceiling", type=float, default=0.05,
                         help="max share of rows one key may hold in uniform mode to PASS")
    parser.add_argument("--skew-floor", type=float, default=0.5,
                         help="min share of rows the hot key must hold in skew mode to PASS")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("academy-check-skew")
        .config("spark.sql.shuffle.partitions", "20")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    uniform_df = run_mode(spark, "uniform", args.rows, "/tmp/dea_check_uniform")
    uniform_share = dominance_ratio(uniform_df, args.rows)
    print(f"uniform mode: busiest key holds {uniform_share:.2%} of rows")

    skew_df = run_mode(spark, "skew", args.rows, "/tmp/dea_check_skew")
    skew_share = dominance_ratio(skew_df, args.rows)
    print(f"skew mode: busiest key (cust_0042) holds {skew_share:.2%} of rows")

    spark.stop()

    if uniform_share > args.uniform_ceiling:
        raise AssertionError(
            f"uniform mode was not actually uniform: busiest key held {uniform_share:.2%} "
            f"of rows (expected <= {args.uniform_ceiling:.0%})"
        )
    if skew_share < args.skew_floor:
        raise AssertionError(
            f"skew mode did not reproduce skew: busiest key held only {skew_share:.2%} "
            f"of rows (expected >= {args.skew_floor:.0%})"
        )

    print(
        "PASS: uniform keys spread evenly across the shuffle; the 80%-hot key "
        "in skew mode lands almost entirely on one reduce task regardless of "
        "how many shuffle partitions you configure — that task is the one "
        "you'll see dominate task duration in the Spark UI."
    )


if __name__ == "__main__":
    main()
