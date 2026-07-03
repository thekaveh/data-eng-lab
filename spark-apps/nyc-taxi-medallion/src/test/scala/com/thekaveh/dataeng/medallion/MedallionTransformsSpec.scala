package com.thekaveh.dataeng.medallion

import java.sql.{Date, Timestamp}

import com.thekaveh.dataeng.medallion.transforms.MedallionTransforms
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class MedallionTransformsSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession.builder().appName("test").master("local[2]")
      .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def ts(s: String): Timestamp = Timestamp.valueOf(s)

  test("silver dedupes on pickup+dropoff; gold aggregates trips + avg_fare by trip_date") {
    val s = spark
    import s.implicits._
    val bronze = Seq(
      (ts("2023-01-01 10:00:00"), ts("2023-01-01 10:30:00"), 5.0, Date.valueOf("2023-01-01")),
      (ts("2023-01-01 10:00:00"), ts("2023-01-01 10:30:00"), 5.0, Date.valueOf("2023-01-01")), // dup
      (ts("2023-01-01 11:00:00"), ts("2023-01-01 11:20:00"), 7.0, Date.valueOf("2023-01-01"))
    ).toDF("tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount", "trip_date")

    val silver = MedallionTransforms.silver(bronze)
    assert(silver.count() == 2) // one duplicate collapsed

    val gold = MedallionTransforms.gold(silver)
    val row = gold.where($"trip_date" === Date.valueOf("2023-01-01")).collect().head
    assert(row.getAs[Long]("trips") == 2)
    assert(math.abs(row.getAs[Double]("avg_fare") - 6.0) < 1e-9)
  }
}
