package com.thekaveh.dataeng.nyctaxi

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
}
