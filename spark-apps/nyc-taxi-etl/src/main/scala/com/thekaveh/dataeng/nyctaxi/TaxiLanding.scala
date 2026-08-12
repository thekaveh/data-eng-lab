package com.thekaveh.dataeng.nyctaxi

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

/** Reads and normalizes an already-resolved NYC Taxi publication.
  *
  * Individual monthly files can encode passenger_count differently.  Read and normalize
  * each file before the union so Spark never has to reconcile those physical schemas.
  */
object TaxiLanding {
  def readResolved(spark: SparkSession, sparkUris: Seq[String]): DataFrame = {
    require(sparkUris.nonEmpty, "resolved NYC Taxi Spark URI arguments are required")
    sparkUris
      .map(path => spark.read.parquet(path).withColumn("passenger_count", F.col("passenger_count").cast("double")))
      .reduce(_.unionByName(_))
  }
}
