"""Probe: Spark Connect -> MinIO (s3a) + Iceberg round-trip. Exit 0 on success."""
import sys
import uuid

from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
tbl = f"lakehouse.bronze._preflight_{uuid.uuid4().hex[:8]}"
try:
    spark.sql(f"CREATE TABLE {tbl} (id INT) USING iceberg")
    spark.sql(f"INSERT INTO {tbl} VALUES (1)")
    n = spark.sql(f"SELECT count(*) c FROM {tbl}").collect()[0]["c"]
    assert n == 1, f"expected 1 row, got {n}"
    print("spark->iceberg OK")
finally:
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
sys.exit(0)
