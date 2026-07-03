package com.thekaveh.dataeng.medallion

import com.thekaveh.dataeng.medallion.transforms.MedallionTransforms
import org.apache.spark.sql.SparkSession

object NycTaxiMedallion {
  def main(args: Array[String]): Unit = {
    val bronzeTable = if (args.length > 0) args(0) else "lakehouse.bronze.nyc_taxi_trips"
    val spark = SparkSession.builder().appName("nyc-taxi-medallion").getOrCreate()
    try {
      val bronze = spark.table(bronzeTable)
      val silver = MedallionTransforms.silver(bronze)
      spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
      silver.writeTo("lakehouse.silver.nyc_taxi_trips").using("iceberg").createOrReplace()
      val gold = MedallionTransforms.gold(silver)
      spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
      gold.writeTo("lakehouse.gold.nyc_taxi_daily").using("iceberg").createOrReplace()
    } finally spark.stop()
  }
}
