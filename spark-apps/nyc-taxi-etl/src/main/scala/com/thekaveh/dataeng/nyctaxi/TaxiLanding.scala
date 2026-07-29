package com.thekaveh.dataeng.nyctaxi

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

/** Selects and normalizes the NYC Taxi Parquet objects produced by `make datasets`.
  *
  * Individual monthly files can encode passenger_count differently.  Read and normalize
  * each file before the union so Spark never has to reconcile those physical schemas.
  */
object TaxiLanding {
  val DefaultScale = "small"

  private val filesByScale = Map(
    "tiny" -> Seq("yellow_tripdata_2023-01.parquet"),
    "small" -> Seq(
      "yellow_tripdata_2023-01.parquet",
      "yellow_tripdata_2023-02.parquet",
      "yellow_tripdata_2023-03.parquet"
    ),
    "medium" -> Seq(
      "yellow_tripdata_2023-01.parquet",
      "yellow_tripdata_2023-02.parquet",
      "yellow_tripdata_2023-03.parquet",
      "yellow_tripdata_2023-04.parquet",
      "yellow_tripdata_2023-05.parquet",
      "yellow_tripdata_2023-06.parquet"
    )
  )

  def pathsForScale(landingPrefix: String, scale: String = DefaultScale): Seq[String] = {
    val files = filesByScale.getOrElse(
      scale,
      throw new IllegalArgumentException(s"unsupported NYC Taxi dataset scale: $scale")
    )
    val prefix = landingPrefix.stripSuffix("/")
    files.map(file => s"$prefix/$file")
  }

  def read(spark: SparkSession, landingPrefix: String, scale: String = DefaultScale): DataFrame =
    pathsForScale(landingPrefix, scale)
      .map(path => spark.read.parquet(path).withColumn("passenger_count", F.col("passenger_count").cast("double")))
      .reduce(_.unionByName(_))
}
