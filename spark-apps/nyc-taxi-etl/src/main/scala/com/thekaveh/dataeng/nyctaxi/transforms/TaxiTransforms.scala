package com.thekaveh.dataeng.nyctaxi.transforms

import org.apache.spark.sql.{DataFrame, functions => F}

object TaxiTransforms {

  /** Clean raw taxi trips: drop null pickups + non-positive passenger counts, add trip_date.
    * No dedup at bronze — deduplication lives at the silver layer (see the medallion scenario). */
  def clean(df: DataFrame): DataFrame =
    df.where(F.col("tpep_pickup_datetime").isNotNull && (F.col("passenger_count") > 0))
      .withColumn("trip_date", F.to_date(F.col("tpep_pickup_datetime")))
}
