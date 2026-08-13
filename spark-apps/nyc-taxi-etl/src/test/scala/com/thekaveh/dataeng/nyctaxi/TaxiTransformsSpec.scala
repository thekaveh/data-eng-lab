package com.thekaveh.dataeng.nyctaxi

import java.nio.file.Files
import java.sql.Timestamp
import java.time.LocalDateTime

import com.thekaveh.dataeng.nyctaxi.transforms.TaxiTransforms
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
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

  test("producer transform preserves the exact TimestampNTZ Bronze contract consumed by quality") {
    val rawSchema = StructType(Seq(
      StructField("VendorID", LongType, nullable = true),
      StructField("tpep_pickup_datetime", TimestampNTZType, nullable = true),
      StructField("tpep_dropoff_datetime", TimestampNTZType, nullable = true),
      StructField("passenger_count", DoubleType, nullable = true),
      StructField("trip_distance", DoubleType, nullable = true),
      StructField("RatecodeID", DoubleType, nullable = true),
      StructField("store_and_fwd_flag", StringType, nullable = true),
      StructField("PULocationID", LongType, nullable = true),
      StructField("DOLocationID", LongType, nullable = true),
      StructField("payment_type", LongType, nullable = true),
      StructField("fare_amount", DoubleType, nullable = true),
      StructField("extra", DoubleType, nullable = true),
      StructField("mta_tax", DoubleType, nullable = true),
      StructField("tip_amount", DoubleType, nullable = true),
      StructField("tolls_amount", DoubleType, nullable = true),
      StructField("improvement_surcharge", DoubleType, nullable = true),
      StructField("total_amount", DoubleType, nullable = true),
      StructField("congestion_surcharge", DoubleType, nullable = true),
      StructField("airport_fee", DoubleType, nullable = true)
    ))
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(
        1L, LocalDateTime.parse("2023-01-01T10:00:00"), LocalDateTime.parse("2023-01-01T10:10:00"),
        2.0d, 1.5d, 1.0d, "N", 10L, 20L, 1L, 8.0d, 0.0d, 0.5d, 1.0d, 0.0d,
        0.3d, 9.8d, 0.0d, 0.0d
      ))),
      rawSchema
    )
    val output = TaxiTransforms.clean(input)
    assert(output.schema == StructType(rawSchema.fields :+
      StructField("trip_date", DateType, nullable = true)))
  }

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
