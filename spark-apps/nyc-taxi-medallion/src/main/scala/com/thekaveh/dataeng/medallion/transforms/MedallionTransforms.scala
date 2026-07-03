package com.thekaveh.dataeng.medallion.transforms

import org.apache.spark.sql.{DataFrame, functions => F}

object MedallionTransforms {

  /** Silver: dedupe bronze on the natural trip key. */
  def silver(df: DataFrame): DataFrame =
    df.dropDuplicates("tpep_pickup_datetime", "tpep_dropoff_datetime")

  /** Gold: daily trip counts + average fare. */
  def gold(df: DataFrame): DataFrame =
    df.groupBy(F.col("trip_date"))
      .agg(F.count("*").as("trips"), F.avg(F.col("fare_amount")).as("avg_fare"))
}
