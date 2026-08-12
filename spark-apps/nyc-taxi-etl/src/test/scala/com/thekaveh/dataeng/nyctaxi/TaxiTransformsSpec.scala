package com.thekaveh.dataeng.nyctaxi

import java.nio.file.Files
import java.sql.Timestamp

import com.thekaveh.dataeng.nyctaxi.transforms.TaxiTransforms
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TaxiTransformsSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession.builder().appName("test").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def ts(s: String): Timestamp = Timestamp.valueOf(s)

  test("drops null pickup + non-positive passengers, adds trip_date") {
    val s = spark
    import s.implicits._
    val raw = Seq(
      (ts("2023-01-01 10:00:00"), 2, 5.0),
      (null.asInstanceOf[Timestamp], 1, 3.0),  // null pickup -> dropped
      (ts("2023-01-02 11:00:00"), 0, 4.0)      // passenger_count 0 -> dropped
    ).toDF("tpep_pickup_datetime", "passenger_count", "fare_amount")

    val out = TaxiTransforms.clean(raw)
    assert(out.count() == 1)
    val row = out.select("trip_date").as[java.sql.Date].collect().head
    assert(row.toString == "2023-01-01")
  }

  test("normalizes each selected Parquet file before unioning mixed passenger_count schemas") {
    val s = spark
    import s.implicits._
    val landing = Files.createTempDirectory("nyc-taxi-landing")
    Seq((ts("2023-01-01 10:00:00"), 2L, 5.0))
      .toDF("tpep_pickup_datetime", "passenger_count", "fare_amount")
      .write.parquet(landing.resolve("yellow_tripdata_2023-01.parquet").toString)
    Seq((ts("2023-02-01 10:00:00"), 3.0, 6.0))
      .toDF("tpep_pickup_datetime", "passenger_count", "fare_amount")
      .write.parquet(landing.resolve("yellow_tripdata_2023-02.parquet").toString)
    Seq((ts("2023-03-01 10:00:00"), 0L, 7.0))
      .toDF("tpep_pickup_datetime", "passenger_count", "fare_amount")
      .write.parquet(landing.resolve("yellow_tripdata_2023-03.parquet").toString)

    val raw = TaxiLanding.readResolved(
      s,
      Seq(
        landing.resolve("yellow_tripdata_2023-01.parquet").toString,
        landing.resolve("yellow_tripdata_2023-02.parquet").toString,
        landing.resolve("yellow_tripdata_2023-03.parquet").toString
      )
    )
    assert(raw.schema("passenger_count").dataType.typeName == "double")
    assert(raw.count() == 3)
    assert(TaxiTransforms.clean(raw).count() == 2)
  }

  test("entrypoint requires ordered immutable URI arguments and an explicit table") {
    val generation = "1" * 64
    val publication = "0123456789ab4def8123456789abcdef"
    val first = s"s3://landing/nyc_taxi/_generations/$generation/$publication/2023-01.parquet"
    val second = s"s3://landing/nyc_taxi/_generations/$generation/$publication/2023-02.parquet"
    val parsed = NycTaxiEtl.parseArguments(Array(first, second, "--table", "lakehouse.bronze.taxi"))
    assert(parsed.uris == Seq(first, second))
    assert(parsed.sparkUris == Seq(first, second).map(_.replace("s3://", "s3a://")))
    assert(parsed.table == "lakehouse.bronze.taxi")
    assertThrows[IllegalArgumentException](NycTaxiEtl.parseArguments(Array.empty))
    assertThrows[IllegalArgumentException](NycTaxiEtl.parseArguments(Array("s3://landing/nyc_taxi/file")))
    val genericHex = s"s3://landing/nyc_taxi/_generations/$generation/${"a" * 32}/2023-02.parquet"
    assertThrows[IllegalArgumentException](
      NycTaxiEtl.parseArguments(Array(first, genericHex, "--table", "lakehouse.bronze.taxi"))
    )
    val other =
      s"s3://landing/nyc_taxi/_generations/$generation/0123456789ab4def9123456789abcdef/2023-02.parquet"
    assertThrows[IllegalArgumentException](
      NycTaxiEtl.parseArguments(Array(first, other, "--table", "lakehouse.bronze.taxi"))
    )
  }
}
