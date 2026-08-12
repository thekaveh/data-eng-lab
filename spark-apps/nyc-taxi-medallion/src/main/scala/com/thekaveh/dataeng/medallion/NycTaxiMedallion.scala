package com.thekaveh.dataeng.medallion

import com.thekaveh.dataeng.medallion.transforms.MedallionTransforms
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

object NycTaxiMedallion {
  final case class Arguments(uris: Seq[String], sparkUris: Seq[String])

  private val ImmutableUri =
    "^(s3://landing/nyc_taxi/_generations/[0-9a-f]{64}/[0-9a-f]{32}/)[A-Za-z0-9._-]+\\.parquet$".r

  def parseArguments(args: Array[String]): Arguments = {
    require(args.nonEmpty, "verified immutable NYC Taxi URI arguments are required")
    val uris = args.toSeq
    require(uris.nonEmpty, "verified immutable NYC Taxi URI arguments are required")
    require(uris.distinct.size == uris.size, "immutable NYC Taxi URI arguments must be unique")
    val generationPrefixes = uris.map {
      case ImmutableUri(prefix) => prefix
      case _ => throw new IllegalArgumentException("verified immutable NYC Taxi URI arguments are required")
    }
    require(generationPrefixes.distinct.size == 1, "immutable NYC Taxi URI arguments must share one generation")
    val sparkUris = uris.map(uri => "s3a://" + uri.stripPrefix("s3://"))
    Arguments(uris, sparkUris)
  }

  private[medallion] def readResolved(spark: SparkSession, sparkUris: Seq[String]): DataFrame = {
    require(sparkUris.nonEmpty, "resolved NYC Taxi Spark URI arguments are required")
    sparkUris
      .map(uri => spark.read.parquet(uri).withColumn("passenger_count", F.col("passenger_count").cast("double")))
      .reduce(_.unionByName(_))
      .filter(F.col("tpep_pickup_datetime").isNotNull && F.col("passenger_count") > 0)
      .withColumn("trip_date", F.to_date(F.col("tpep_pickup_datetime")))
  }

  def main(args: Array[String]): Unit = {
    val arguments = parseArguments(args)
    val spark = SparkSession.builder().appName("nyc-taxi-medallion").getOrCreate()
    try {
      val bronze = readResolved(spark, arguments.sparkUris)
      val silver = MedallionTransforms.silver(bronze)
      spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
      silver.writeTo("lakehouse.silver.nyc_taxi_trips").using("iceberg").createOrReplace()
      val gold = MedallionTransforms.gold(silver)
      spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
      gold.writeTo("lakehouse.gold.nyc_taxi_daily").using("iceberg").createOrReplace()
    } finally spark.stop()
  }
}
