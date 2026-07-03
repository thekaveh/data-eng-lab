#!/usr/bin/env python3
"""Bronze smoke: load a landing dataset into an Iceberg bronze table via Spark Connect."""
from __future__ import annotations

import sys


def bronze_table(name: str) -> str:
    return f"lakehouse.bronze.{name}"


def build_bronze(spark, landing_uri: str, table: str) -> int:
    df = spark.read.parquet(landing_uri)
    (df.writeTo(table).using("iceberg").createOrReplace())
    return spark.table(table).count()


def main(argv=None) -> int:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
    n = build_bronze(spark, "s3a://landing/nyc_taxi/", bronze_table("nyc_taxi_trips"))
    print(f"bronze lakehouse.bronze.nyc_taxi_trips: {n} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
