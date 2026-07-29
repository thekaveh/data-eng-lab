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
    val prefix = landing.toUri.toString.stripSuffix("/")

    Seq((ts("2023-01-01 10:00:00"), 2L, 5.0))
      .toDF("tpep_pickup_datetime", "passenger_count", "fare_amount")
      .write.parquet(landing.resolve("yellow_tripdata_2023-01.parquet").toString)
    Seq((ts("2023-02-01 10:00:00"), 3.0, 6.0))
      .toDF("tpep_pickup_datetime", "passenger_count", "fare_amount")
      .write.parquet(landing.resolve("yellow_tripdata_2023-02.parquet").toString)
    Seq((ts("2023-03-01 10:00:00"), 0L, 7.0))
      .toDF("tpep_pickup_datetime", "passenger_count", "fare_amount")
      .write.parquet(landing.resolve("yellow_tripdata_2023-03.parquet").toString)

    val raw = TaxiLanding.read(s, prefix)
    assert(raw.schema("passenger_count").dataType.typeName == "double")
    assert(raw.count() == 3)
    assert(TaxiTransforms.clean(raw).count() == 2)
  }

  test("uses deterministic scale paths and rejects unsupported scales") {
    val prefix = "s3a://landing/nyc_taxi/"
    assert(TaxiLanding.pathsForScale(prefix, "tiny") == Seq(
      "s3a://landing/nyc_taxi/yellow_tripdata_2023-01.parquet"
    ))
    assert(TaxiLanding.pathsForScale(prefix) == Seq(
      "s3a://landing/nyc_taxi/yellow_tripdata_2023-01.parquet",
      "s3a://landing/nyc_taxi/yellow_tripdata_2023-02.parquet",
      "s3a://landing/nyc_taxi/yellow_tripdata_2023-03.parquet"
    ))
    assert(TaxiLanding.pathsForScale(prefix, "medium").size == 6)
    assertThrows[IllegalArgumentException](TaxiLanding.pathsForScale(prefix, "large"))
  }
}
