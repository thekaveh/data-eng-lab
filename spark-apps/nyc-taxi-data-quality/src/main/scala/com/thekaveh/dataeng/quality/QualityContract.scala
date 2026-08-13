package com.thekaveh.dataeng.quality

import java.math.{BigDecimal => JBigDecimal}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.types._

final case class Arguments(
    logicalDate: Instant,
    dataIntervalEnd: Instant,
    upstreamDagId: String
)

final case class SourceSnapshot(id: Long, committedAt: Instant, schemaSha256: String)

final case class RuleDefinition(
    ruleId: String,
    layer: String,
    owner: String,
    metricName: String,
    warnThreshold: Option[String],
    failThreshold: String
)

final case class QualityFact(
    qualityRunId: String,
    logicalDate: Timestamp,
    dataIntervalEnd: Timestamp,
    datasetId: String,
    bindingType: String,
    upstreamDagId: String,
    sourceTable: String,
    sourceSnapshotId: java.lang.Long,
    sourceSnapshotCommittedAt: Timestamp,
    sourceSchemaSha256: String,
    layer: String,
    ruleId: String,
    ruleVersion: String,
    owner: String,
    metricName: String,
    metricNumerator: java.lang.Long,
    metricDenominator: java.lang.Long,
    metricValue: JBigDecimal,
    warnThreshold: String,
    failThreshold: String,
    severity: String,
    status: String,
    diagnosticCode: String
)

object QualityContract {
  val DatasetId = "nyc_taxi"
  val RuleVersion = "nyc_taxi_quality_v1"
  val SourceTable = "lakehouse.bronze.nyc_taxi_trips"
  val CleanTable = "lakehouse.silver.nyc_taxi_clean"
  val QuarantineTable = "lakehouse.silver.nyc_taxi_quarantine"
  val FactsTable = "lakehouse.gold.nyc_taxi_quality_facts"
  val UpstreamDagId = "nyc_taxi_etl"

  val rules: Seq[RuleDefinition] = Seq(
    RuleDefinition("bronze.source_available.v1", "Bronze", "Data Engineering",
      "source_row_count", None, "rows=0"),
    RuleDefinition("bronze.schema.v1", "Bronze", "Data Engineering",
      "schema_match_ratio", None, "ratio<1.000000000"),
    RuleDefinition("bronze.snapshot_freshness.v1", "Bronze", "Data Engineering",
      "snapshot_age_seconds", None, "seconds>21600"),
    RuleDefinition("bronze.invalid_ratio.v1", "Bronze", "Data Quality Engineering",
      "invalid_row_ratio", Some("ratio>0.010000000"), "ratio>0.050000000"),
    RuleDefinition("silver.partition_conservation.v1", "Silver", "Data Quality Engineering",
      "partition_row_ratio", None, "ratio!=1.000000000"),
    RuleDefinition("silver.clean_nonempty.v1", "Silver", "Data Quality Engineering",
      "clean_row_count", None, "rows=0"),
    RuleDefinition("silver.quarantine_ratio.v1", "Silver", "Data Quality Engineering",
      "quarantine_row_ratio", Some("ratio>0.010000000"), "ratio>0.050000000"),
    RuleDefinition("silver.output_readback.v1", "Silver", "Data Platform Engineering",
      "readback_check_ratio", None, "ratio<1.000000000")
  )

  val ExpectedRuleIds: Seq[String] = rules.map(_.ruleId)

  val DiagnosticCodes: Set[String] = Set(
    "ok",
    "threshold_warn",
    "threshold_fail",
    "source_missing",
    "source_stale",
    "schema_mismatch",
    "partition_mismatch",
    "output_empty",
    "readback_mismatch"
  )

  def requireDiagnosticCode(value: String): String = {
    require(DiagnosticCodes.contains(value), "quality diagnostic code is invalid")
    value
  }

  def requireStatusDiagnostic(status: String, diagnostic: String): Unit = {
    requireDiagnosticCode(diagnostic)
    val valid = status match {
      case "pass" => diagnostic == "ok"
      case "warn" => diagnostic == "threshold_warn"
      case "missing" => diagnostic == "source_missing"
      case "stale" => diagnostic == "source_stale"
      case "fail" => Set(
        "threshold_fail", "schema_mismatch", "partition_mismatch", "output_empty", "readback_mismatch"
      ).contains(diagnostic)
      case _ => false
    }
    require(valid, "quality status and diagnostic code do not match")
  }

  val bronzeSchema: StructType = StructType(Seq(
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
    StructField("airport_fee", DoubleType, nullable = true),
    StructField("trip_date", DateType, nullable = true)
  ))

  val factsSchema: StructType = StructType(Seq(
    StructField("quality_run_id", StringType, nullable = false),
    StructField("logical_date", TimestampType, nullable = false),
    StructField("data_interval_end", TimestampType, nullable = false),
    StructField("dataset_id", StringType, nullable = false),
    StructField("binding_type", StringType, nullable = false),
    StructField("upstream_dag_id", StringType, nullable = false),
    StructField("source_table", StringType, nullable = false),
    StructField("source_snapshot_id", LongType, nullable = true),
    StructField("source_snapshot_committed_at", TimestampType, nullable = true),
    StructField("source_schema_sha256", StringType, nullable = true),
    StructField("layer", StringType, nullable = false),
    StructField("rule_id", StringType, nullable = false),
    StructField("rule_version", StringType, nullable = false),
    StructField("owner", StringType, nullable = false),
    StructField("metric_name", StringType, nullable = false),
    StructField("metric_numerator", LongType, nullable = true),
    StructField("metric_denominator", LongType, nullable = true),
    StructField("metric_value", DecimalType(38, 9), nullable = true),
    StructField("warn_threshold", StringType, nullable = true),
    StructField("fail_threshold", StringType, nullable = false),
    StructField("severity", StringType, nullable = false),
    StructField("status", StringType, nullable = false),
    StructField("diagnostic_code", StringType, nullable = false)
  ))

  private val TimestampPattern =
    "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$".r
  private val PrintableAscii = "^[\\x20-\\x7e]{1,128}$".r
  private val ExpectedOptions = Seq("--logical-date", "--data-interval-end", "--upstream-dag-id")
  private val StatusPriority = Map("pass" -> 0, "warn" -> 1, "fail" -> 2, "stale" -> 3, "missing" -> 4)

  private def strictInstant(value: String): Instant = {
    require(TimestampPattern.pattern.matcher(value).matches(), "timestamp must be strict whole-second UTC")
    try {
      val parsed = Instant.parse(value)
      require(parsed.toString == value, "timestamp must be canonical whole-second UTC")
      parsed
    } catch {
      case _: RuntimeException => throw new IllegalArgumentException("timestamp must be strict whole-second UTC")
    }
  }

  def parseArguments(args: Array[String]): Arguments = {
    require(args.length == 6, "exact quality arguments are required")
    require(args.grouped(2).map(_.head).toSeq == ExpectedOptions, "quality arguments must use exact order")
    val values = args.grouped(2).map(_(1)).toSeq
    require(values.forall(value => PrintableAscii.pattern.matcher(value).matches()),
      "quality argument values must be bounded printable ASCII")
    require(values(2) == UpstreamDagId, "upstream DAG must be nyc_taxi_etl")
    Arguments(strictInstant(values(0)), strictInstant(values(1)), values(2))
  }

  def sha256(value: String): String =
    MessageDigest.getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

  private def jsonString(value: String): String =
    "\"" + value.flatMap {
      case '\\' => "\\\\"
      case '"' => "\\\""
      case character => character.toString
    } + "\""

  val canonicalSchemaJson: String = bronzeSchema.fields.map { field =>
    val canonicalType = field.dataType.typeName
    s"{\"name\":${jsonString(field.name)},\"nullable\":${field.nullable}," +
      s"\"type\":${jsonString(canonicalType)}}"
  }.mkString("[", ",", "]")

  val schemaSha256: String = sha256(canonicalSchemaJson)

  def qualityRunId(logicalDate: Instant, snapshot: Option[Long]): String = {
    snapshot.foreach(value => require(value > 0, "source snapshot must be positive"))
    val identity = snapshot.map(_.toString).getOrElse("missing")
    sha256(s"$DatasetId\n${logicalDate.toString}\n$identity\n$RuleVersion")
  }

  def ratio(numerator: Long, denominator: Long): BigDecimal = {
    require(denominator > 0, "quality ratio denominator must be positive")
    (BigDecimal(numerator) / BigDecimal(denominator)).setScale(9, BigDecimal.RoundingMode.HALF_UP)
  }

  def overallStatus(statuses: Seq[String]): String = {
    require(statuses.nonEmpty && statuses.forall(StatusPriority.contains), "quality status is invalid")
    statuses.maxBy(StatusPriority)
  }

  def severity(status: String): String = status match {
    case "pass" => "info"
    case "warn" => "warning"
    case "fail" | "stale" | "missing" => "error"
    case _ => throw new IllegalArgumentException("quality status is invalid")
  }
}
