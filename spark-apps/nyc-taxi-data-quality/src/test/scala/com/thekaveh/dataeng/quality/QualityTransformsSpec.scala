package com.thekaveh.dataeng.quality

import java.sql.Date
import java.time.{Instant, LocalDateTime}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class QualityTransformsSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("nyc-quality-tests").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def row(
      fare: java.lang.Double,
      passenger: java.lang.Double,
      vendor: java.lang.Long = 1L,
      pickup: LocalDateTime = LocalDateTime.parse("2023-01-01T01:00:00")
  ): Row = Row(
    vendor, pickup, LocalDateTime.parse("2023-01-01T01:10:00"), passenger,
    1.0d, 1.0d, "N", 10L, 20L, 1L, fare, 0.0d, 0.5d, 1.0d, 0.0d, 0.3d,
    if (fare == null) null else Double.box(fare + 1.8d), 0.0d, 0.0d, Date.valueOf("2023-01-01")
  )

  private def frame(rows: Seq[Row], schema: StructType = QualityContract.bronzeSchema): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

  test("null-safe split quarantines every false, null, NaN, and infinite rule operand") {
    val duplicate = row(10.0d, 2.0d, 7L)
    val source = frame(Seq(
      row(10.0d, 1.0d),
      row(10.0d, 6.0d),
      row(10.0d, 0.0d),
      row(10.0d, 7.0d),
      row(0.0d, 2.0d),
      row(-1.0d, 2.0d),
      row(null, 2.0d),
      row(10.0d, null),
      row(Double.NaN, 2.0d),
      row(Double.PositiveInfinity, 2.0d),
      row(10.0d, Double.NaN),
      row(10.0d, Double.NegativeInfinity),
      duplicate,
      duplicate
    ))

    val split = QualityTransforms.split(source)
    assert(split.clean.count() == 4L)
    assert(split.quarantine.count() == 10L)
    assert(split.clean.filter("VendorID = 7").count() == 2L)
    assert(split.quarantine.filter("fare_amount IS NULL").count() == 1L)
    assert(split.quarantine.filter("passenger_count IS NULL").count() == 1L)
    QualityTransforms.assertPartition(source, split.clean, split.quarantine)
  }

  test("partition validation rejects omission, overlap, and predicate contamination") {
    val source = frame(Seq(row(10.0d, 2.0d), row(-1.0d, 2.0d)))
    val split = QualityTransforms.split(source)
    assertThrows[IllegalArgumentException](
      QualityTransforms.assertPartition(source, split.clean, split.quarantine.limit(0)))
    assertThrows[IllegalArgumentException](
      QualityTransforms.assertPartition(source, split.clean.union(split.clean), split.quarantine))
    assertThrows[IllegalArgumentException](
      QualityTransforms.assertPartition(source, source, split.quarantine))
  }

  test("exact schema validation rejects order, type, case, nullability, missing, and extra drift") {
    val source = frame(Seq(row(10.0d, 2.0d)))
    QualityTransforms.assertExactSchema(source)

    val reordered = StructType(QualityContract.bronzeSchema.fields.reverse)
    val wrongType = StructType(QualityContract.bronzeSchema.fields.updated(3,
      StructField("passenger_count", LongType, nullable = true)))
    val wrongCase = StructType(QualityContract.bronzeSchema.fields.updated(0,
      StructField("vendorid", LongType, nullable = true)))
    val wrongNullability = StructType(QualityContract.bronzeSchema.fields.updated(0,
      StructField("VendorID", LongType, nullable = false)))
    val missing = StructType(QualityContract.bronzeSchema.fields.dropRight(1))
    val extra = StructType(QualityContract.bronzeSchema.fields :+
      StructField("unexpected", StringType, nullable = true))
    val legacyUtcTimestamps = StructType(QualityContract.bronzeSchema.fields
      .updated(1, StructField("tpep_pickup_datetime", TimestampType, nullable = true))
      .updated(2, StructField("tpep_dropoff_datetime", TimestampType, nullable = true)))

    Seq(reordered, wrongType, wrongCase, wrongNullability, missing, extra, legacyUtcTimestamps).foreach { schema =>
      assertThrows[IllegalArgumentException](QualityTransforms.assertExactSchema(frame(Seq.empty, schema)))
    }
  }

  test("distributed fingerprint is stable across partition order and multiplicity-aware") {
    val rows = Seq(row(10.0d, 2.0d, 1L), row(-1.0d, 2.0d, 2L), row(null, 2.0d, 3L))
    val first = frame(rows).repartition(3)
    val second = frame(rows.reverse).repartition(1)
    val duplicated = frame(rows :+ rows.head).repartition(2)
    assert(QualityTransforms.fingerprint(first) == QualityTransforms.fingerprint(second))
    assert(QualityTransforms.fingerprint(first) != QualityTransforms.fingerprint(duplicated))
  }

  test("ratio policy freezes pass, warn, and fail threshold edges") {
    assert(QualityTransforms.ratioStatus(1L, 100L) == "pass")
    assert(QualityTransforms.ratioStatus(10000001L, 1000000000L) == "warn")
    assert(QualityTransforms.ratioStatus(5L, 100L) == "warn")
    assert(QualityTransforms.ratioStatus(50000001L, 1000000000L) == "fail")
    assertThrows[IllegalArgumentException](QualityTransforms.ratioStatus(0L, 0L))
    assertThrows[IllegalArgumentException](QualityTransforms.ratioStatus(-1L, 100L))
    assertThrows[IllegalArgumentException](QualityTransforms.ratioStatus(101L, 100L))
  }

  test("freshness is pass at the six-hour boundary and stale only beyond it") {
    val intervalEnd = Instant.parse("2026-08-14T00:00:00Z")
    assert(QualityTransforms.freshnessStatus(Instant.parse("2026-08-13T18:00:00Z"), intervalEnd) == "pass")
    assert(QualityTransforms.freshnessStatus(Instant.parse("2026-08-13T17:59:59Z"), intervalEnd) == "stale")
    assert(QualityTransforms.freshnessStatus(Instant.parse("2026-08-14T00:00:01Z"), intervalEnd) == "pass")
  }

  test("signal observation uses closed severity and diagnostic mappings") {
    val pass = QualityTransforms.ratioObservation("bronze.invalid_ratio.v1", 1L, 100L)
    val warn = QualityTransforms.ratioObservation("silver.quarantine_ratio.v1", 5L, 100L)
    val fail = QualityTransforms.ratioObservation("bronze.invalid_ratio.v1", 6L, 100L)
    assert((pass.status, pass.severity, pass.diagnosticCode, pass.metricValue.toString) ==
      ("pass", "info", "ok", "0.010000000"))
    assert((warn.status, warn.severity, warn.diagnosticCode) ==
      ("warn", "warning", "threshold_warn"))
    assert((fail.status, fail.severity, fail.diagnosticCode) ==
      ("fail", "error", "threshold_fail"))
    assertThrows[IllegalArgumentException](
      QualityTransforms.ratioObservation("bronze.source_available.v1", 1L, 100L))
  }
}
