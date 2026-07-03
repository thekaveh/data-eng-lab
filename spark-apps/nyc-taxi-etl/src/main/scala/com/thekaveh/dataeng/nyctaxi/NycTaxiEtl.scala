package com.thekaveh.dataeng.nyctaxi

import com.thekaveh.dataeng.nyctaxi.transforms.TaxiTransforms
import org.apache.spark.sql.SparkSession

object NycTaxiEtl {
  def main(args: Array[String]): Unit = {
    val landing = if (args.length > 0) args(0) else "s3a://landing/nyc_taxi/"
    val table = if (args.length > 1) args(1) else "lakehouse.bronze.nyc_taxi_trips"

    val spark = SparkSession.builder().appName("nyc-taxi-etl").getOrCreate()
    try {
      val cleaned = TaxiTransforms.clean(spark.read.parquet(landing))
      cleaned.writeTo(table).using("iceberg").createOrReplace()
      // scalastyle:off println
      println(s"wrote $table: ${spark.table(table).count()} rows")
      // scalastyle:on println
    } finally spark.stop()
  }
}
