package com.thekaveh.dataeng.quality

import java.time.{Duration, Instant}

import org.apache.spark.sql.{Column, DataFrame, functions => F}

final case class SplitResult(clean: DataFrame, quarantine: DataFrame)

final case class RowFingerprint(
    rowCount: Long,
    sumA: BigDecimal,
    xorA: Long,
    sumB: BigDecimal,
    xorB: Long
)

final case class SignalObservation(
    ruleId: String,
    numerator: Long,
    denominator: Long,
    metricValue: BigDecimal,
    status: String,
    severity: String,
    diagnosticCode: String
)

object QualityTransforms {
  private val WarnRatio = BigDecimal("0.010000000")
  private val FailRatio = BigDecimal("0.050000000")

  private def finite(column: Column): Column =
    column.isNotNull && !F.isnan(column) && F.abs(column) <= F.lit(Double.MaxValue)

  def validPredicate: Column = {
    val fare = F.col("fare_amount")
    val passengers = F.col("passenger_count")
    finite(fare) && finite(passengers) && fare > F.lit(0.0d) &&
      passengers.between(1.0d, 6.0d)
  }

  def assertExactSchema(frame: DataFrame): Unit = {
    val observed = frame.schema.fields.map(field => (field.name, field.dataType, field.nullable)).toSeq
    val expected = QualityContract.bronzeSchema.fields
      .map(field => (field.name, field.dataType, field.nullable)).toSeq
    require(observed == expected, "NYC Taxi source schema is invalid")
  }

  def split(bronze: DataFrame): SplitResult = {
    assertExactSchema(bronze)
    val projected = bronze.select(QualityContract.bronzeSchema.fieldNames.map(F.col): _*)
    val valid = F.coalesce(validPredicate, F.lit(false))
    SplitResult(projected.where(valid), projected.where(!valid))
  }

  def assertPartition(bronze: DataFrame, clean: DataFrame, quarantine: DataFrame): Unit = {
    assertExactSchema(bronze)
    assertExactSchema(clean)
    assertExactSchema(quarantine)
    val valid = F.coalesce(validPredicate, F.lit(false))
    require(clean.where(!valid).limit(1).count() == 0L, "clean output contains an invalid row")
    require(quarantine.where(valid).limit(1).count() == 0L, "quarantine output contains a valid row")
    val sourceCount = bronze.count()
    require(clean.count() + quarantine.count() == sourceCount, "quality partition count mismatch")
    val union = clean.unionByName(quarantine)
    require(union.exceptAll(bronze).limit(1).count() == 0L, "quality outputs contain an extra row")
    require(bronze.exceptAll(union).limit(1).count() == 0L, "quality outputs omit a source row")
  }

  def invalidCount(bronze: DataFrame): Long = {
    assertExactSchema(bronze)
    bronze.where(!F.coalesce(validPredicate, F.lit(false))).count()
  }

  def fingerprint(frame: DataFrame): RowFingerprint = {
    assertExactSchema(frame)
    val columns = QualityContract.bronzeSchema.fieldNames.map(F.col)
    val hashed = frame
      .withColumn("_quality_hash_a", F.xxhash64((Seq(F.lit("nyc-quality-a")) ++ columns): _*))
      .withColumn("_quality_hash_b", F.xxhash64((Seq(F.lit("nyc-quality-b")) ++ columns): _*))
    val aggregate = hashed.agg(
      F.count(F.lit(1)).as("row_count"),
      F.sum(F.col("_quality_hash_a").cast("decimal(38,0)")).as("sum_a"),
      F.expr("bit_xor(_quality_hash_a)").as("xor_a"),
      F.sum(F.col("_quality_hash_b").cast("decimal(38,0)")).as("sum_b"),
      F.expr("bit_xor(_quality_hash_b)").as("xor_b")
    ).head()
    val count = aggregate.getAs[Long]("row_count")
    def decimal(name: String): BigDecimal =
      Option(aggregate.getAs[java.math.BigDecimal](name)).map(BigDecimal(_)).getOrElse(BigDecimal(0))
    def long(name: String): Long = Option(aggregate.getAs[java.lang.Long](name)).fold(0L)(_.longValue())
    RowFingerprint(count, decimal("sum_a"), long("xor_a"), decimal("sum_b"), long("xor_b"))
  }

  def ratioStatus(numerator: Long, denominator: Long): String = {
    require(numerator >= 0 && denominator > 0 && numerator <= denominator,
      "quality ratio counts are invalid")
    val value = QualityContract.ratio(numerator, denominator)
    if (value > FailRatio) "fail"
    else if (value > WarnRatio) "warn"
    else "pass"
  }

  def freshnessStatus(committedAt: Instant, dataIntervalEnd: Instant): String =
    if (Duration.between(committedAt, dataIntervalEnd).getSeconds > 21600L) "stale" else "pass"

  def ratioObservation(ruleId: String, numerator: Long, denominator: Long): SignalObservation = {
    require(Set("bronze.invalid_ratio.v1", "silver.quarantine_ratio.v1").contains(ruleId),
      "ratio observation rule is invalid")
    val status = ratioStatus(numerator, denominator)
    val diagnostic = status match {
      case "pass" => "ok"
      case "warn" => "threshold_warn"
      case "fail" => "threshold_fail"
    }
    SignalObservation(ruleId, numerator, denominator, QualityContract.ratio(numerator, denominator),
      status, QualityContract.severity(status), diagnostic)
  }
}
