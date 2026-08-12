package com.thekaveh.dataeng.medallion

import com.thekaveh.dataeng.medallion.transforms.MedallionTransforms
import org.apache.spark.sql.SparkSession

object NycTaxiMedallion {
  final case class Arguments(uris: Seq[String], bronzeTable: String)

  private val ImmutableUri =
    "^(s3://landing/nyc_taxi/_generations/[0-9a-f]{64}/[0-9a-f]{32}/)[A-Za-z0-9._-]+\\.parquet$".r

  def parseArguments(args: Array[String]): Arguments = {
    require(args.nonEmpty, "verified immutable NYC Taxi URI arguments are required")
    val optionIndex = args.indexOf("--bronze-table")
    require(
      optionIndex > 0 && optionIndex == args.length - 2,
      "--bronze-table must follow immutable URI arguments"
    )
    val uris = args.take(optionIndex).toSeq
    require(uris.nonEmpty, "verified immutable NYC Taxi URI arguments are required")
    require(uris.distinct.size == uris.size, "immutable NYC Taxi URI arguments must be unique")
    val generationPrefixes = uris.map {
      case ImmutableUri(prefix) => prefix
      case _ => throw new IllegalArgumentException("verified immutable NYC Taxi URI arguments are required")
    }
    require(generationPrefixes.distinct.size == 1, "immutable NYC Taxi URI arguments must share one generation")
    Arguments(uris, args.last)
  }

  def main(args: Array[String]): Unit = {
    val arguments = parseArguments(args)
    val spark = SparkSession.builder().appName("nyc-taxi-medallion").getOrCreate()
    try {
      val bronze = spark.table(arguments.bronzeTable)
      val silver = MedallionTransforms.silver(bronze)
      spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
      silver.writeTo("lakehouse.silver.nyc_taxi_trips").using("iceberg").createOrReplace()
      val gold = MedallionTransforms.gold(silver)
      spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
      gold.writeTo("lakehouse.gold.nyc_taxi_daily").using("iceberg").createOrReplace()
    } finally spark.stop()
  }
}
