import importlib.util
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("bronze_smoke", ROOT / "scripts" / "bronze_smoke.py")
bs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bs)


def test_bronze_table_name_default():
    # pure helper: derives the target table for a landing prefix
    assert bs.bronze_table("nyc_taxi") == "lakehouse.bronze.nyc_taxi"


@pytest.mark.infra
@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1", reason="needs a live enhanced-Atlas stack")
def test_build_bronze_end_to_end():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
    n = bs.build_bronze(spark, "s3a://landing/nyc_taxi/", bs.bronze_table("nyc_taxi_trips"))
    assert n > 0
