package com.thekaveh.dataeng.nyctaxi

import com.thekaveh.dataeng.nyctaxi.transforms.TaxiTransforms
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

object NycTaxiEtl {
  final case class Arguments(uris: Seq[String], table: String)

  private val ImmutableUri =
    "^(s3://landing/nyc_taxi/_generations/[0-9a-f]{64}/[0-9a-f]{32}/)[A-Za-z0-9._-]+\\.parquet$".r

  def parseArguments(args: Array[String]): Arguments = {
    require(args.nonEmpty, "verified immutable NYC Taxi URI arguments are required")
    val optionIndex = args.indexOf("--table")
    require(optionIndex > 0 && optionIndex == args.length - 2, "--table must follow immutable URI arguments")
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

  private def readResolved(spark: SparkSession, uris: Seq[String]): DataFrame =
    uris
      .map(uri => spark.read.parquet(uri).withColumn("passenger_count", F.col("passenger_count").cast("double")))
      .reduce(_.unionByName(_))

  def main(args: Array[String]): Unit = {
    val arguments = parseArguments(args)

    val spark = SparkSession.builder().appName("nyc-taxi-etl").getOrCreate()
    try {
      val ns = arguments.table.substring(0, arguments.table.lastIndexOf('.'))  // e.g. lakehouse.bronze
      spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $ns")
      val cleaned = TaxiTransforms.clean(readResolved(spark, arguments.uris))
      cleaned.writeTo(arguments.table).using("iceberg").createOrReplace()
      // scalastyle:off println
      println(s"wrote ${arguments.table}: ${spark.table(arguments.table).count()} rows")
      // scalastyle:on println
    } finally spark.stop()
  }
}
