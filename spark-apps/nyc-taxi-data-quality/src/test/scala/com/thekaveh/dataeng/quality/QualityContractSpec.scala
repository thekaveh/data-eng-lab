package com.thekaveh.dataeng.quality

import java.time.Instant

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class QualityContractSpec extends AnyFunSuite {
  private val ValidArgs = Array(
    "--logical-date", "2026-08-13T01:00:00Z",
    "--data-interval-end", "2026-08-14T00:00:00Z",
    "--upstream-dag-id", "nyc_taxi_etl"
  )

  test("parses the one strict ordered production argument shape") {
    val parsed = QualityContract.parseArguments(ValidArgs)
    assert(parsed.logicalDate == Instant.parse("2026-08-13T01:00:00Z"))
    assert(parsed.dataIntervalEnd == Instant.parse("2026-08-14T00:00:00Z"))
    assert(parsed.upstreamDagId == "nyc_taxi_etl")
  }

  test("rejects malformed, ambiguous, reordered, and configurable arguments") {
    val invalid = Seq(
      Array.empty[String],
      ValidArgs.dropRight(2),
      ValidArgs ++ Array("--threshold", "0.5"),
      Array("--data-interval-end", ValidArgs(3), "--logical-date", ValidArgs(1),
        "--upstream-dag-id", "nyc_taxi_etl"),
      ValidArgs.updated(1, "2026-08-13T01:00:00.000Z"),
      ValidArgs.updated(1, "2026-08-13T01:00:00+00:00"),
      ValidArgs.updated(1, " 2026-08-13T01:00:00Z"),
      ValidArgs.updated(1, "2026-02-30T01:00:00Z"),
      ValidArgs.updated(5, "other_etl"),
      ValidArgs.updated(5, "nyc_taxi_etl\nsecret"),
      ValidArgs.updated(5, "x" * 129),
      ValidArgs.updated(5, "nyc_taxi_étl")
    )
    invalid.foreach(args => assertThrows[IllegalArgumentException](QualityContract.parseArguments(args)))
  }

  test("freezes the case-preserving optional Bronze and Silver schema") {
    val expected = Seq(
      "VendorID" -> LongType,
      "tpep_pickup_datetime" -> TimestampType,
      "tpep_dropoff_datetime" -> TimestampType,
      "passenger_count" -> DoubleType,
      "trip_distance" -> DoubleType,
      "RatecodeID" -> DoubleType,
      "store_and_fwd_flag" -> StringType,
      "PULocationID" -> LongType,
      "DOLocationID" -> LongType,
      "payment_type" -> LongType,
      "fare_amount" -> DoubleType,
      "extra" -> DoubleType,
      "mta_tax" -> DoubleType,
      "tip_amount" -> DoubleType,
      "tolls_amount" -> DoubleType,
      "improvement_surcharge" -> DoubleType,
      "total_amount" -> DoubleType,
      "congestion_surcharge" -> DoubleType,
      "airport_fee" -> DoubleType,
      "trip_date" -> DateType
    )
    assert(QualityContract.bronzeSchema.fields.map(field => field.name -> field.dataType).toSeq == expected)
    assert(QualityContract.bronzeSchema.fields.forall(_.nullable))
    assert(QualityContract.schemaSha256.matches("[0-9a-f]{64}"))
  }

  test("freezes the exact non-nullable and nullable facts schema") {
    val expectedNames = Seq(
      "quality_run_id", "logical_date", "data_interval_end", "dataset_id", "binding_type",
      "upstream_dag_id", "source_table", "source_snapshot_id", "source_snapshot_committed_at",
      "source_schema_sha256", "layer", "rule_id", "rule_version", "owner", "metric_name",
      "metric_numerator", "metric_denominator", "metric_value", "warn_threshold",
      "fail_threshold", "severity", "status", "diagnostic_code"
    )
    assert(QualityContract.factsSchema.fieldNames.toSeq == expectedNames)
    assert(QualityContract.factsSchema("metric_value").dataType == DecimalType(38, 9))
    val nullable = QualityContract.factsSchema.fields.filter(_.nullable).map(_.name).toSet
    assert(nullable == Set("source_snapshot_id", "source_snapshot_committed_at",
      "source_schema_sha256", "metric_numerator", "metric_denominator", "metric_value",
      "warn_threshold"))
  }

  test("derives deterministic bounded run identities without a random value") {
    val logical = Instant.parse("2026-08-13T01:00:00Z")
    assert(QualityContract.qualityRunId(logical, Some(6090932775096319165L)) ==
      "043748e6ef9131e97e1f84f5044bbc2b90d8760461c1dcd317217d7222ba0809")
    assert(QualityContract.qualityRunId(logical, None).matches("[0-9a-f]{64}"))
    assert(QualityContract.qualityRunId(logical, None) !=
      QualityContract.qualityRunId(logical, Some(6090932775096319165L)))
  }

  test("freezes eight governed rule definitions and exact threshold encodings") {
    val expected = Seq(
      ("bronze.source_available.v1", "Bronze", "Data Engineering", None, "rows=0"),
      ("bronze.schema.v1", "Bronze", "Data Engineering", None, "ratio<1.000000000"),
      ("bronze.snapshot_freshness.v1", "Bronze", "Data Engineering", None, "seconds>21600"),
      ("bronze.invalid_ratio.v1", "Bronze", "Data Quality Engineering",
        Some("ratio>0.010000000"), "ratio>0.050000000"),
      ("silver.partition_conservation.v1", "Silver", "Data Quality Engineering",
        None, "ratio!=1.000000000"),
      ("silver.clean_nonempty.v1", "Silver", "Data Quality Engineering", None, "rows=0"),
      ("silver.quarantine_ratio.v1", "Silver", "Data Quality Engineering",
        Some("ratio>0.010000000"), "ratio>0.050000000"),
      ("silver.output_readback.v1", "Silver", "Data Platform Engineering",
        None, "ratio<1.000000000")
    )
    assert(QualityContract.rules.map(rule =>
      (rule.ruleId, rule.layer, rule.owner, rule.warnThreshold, rule.failThreshold)) == expected)
  }

  test("uses half-up scale-nine ratios and closed status precedence") {
    assert(QualityContract.ratio(82414L, 8991502L).toString == "0.009165766")
    assert(QualityContract.ratio(1L, 6L).toString == "0.166666667")
    assertThrows[IllegalArgumentException](QualityContract.ratio(1L, 0L))
    assert(QualityContract.overallStatus(Seq("pass", "warn")) == "warn")
    assert(QualityContract.overallStatus(Seq("pass", "warn", "fail")) == "fail")
    assert(QualityContract.overallStatus(Seq("fail", "stale")) == "stale")
    assert(QualityContract.overallStatus(Seq("stale", "missing")) == "missing")
    assertThrows[IllegalArgumentException](QualityContract.overallStatus(Seq("unknown")))
  }
}
