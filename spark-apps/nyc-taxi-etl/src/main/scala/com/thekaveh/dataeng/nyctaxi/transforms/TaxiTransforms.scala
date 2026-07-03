package com.thekaveh.dataeng.nyctaxi.transforms

import org.apache.spark.sql.{DataFrame, functions => F}

object TaxiTransforms {

  /** Clean raw taxi trips: drop null pickups + non-positive passenger counts, dedupe, add trip_date. */
  def clean(df: DataFrame): DataFrame =
    df.where(F.col("pickup_datetime").isNotNull && (F.col("passenger_count") > 0))
      .dropDuplicates()
      .withColumn("trip_date", F.to_date(F.col("pickup_datetime")))
}
