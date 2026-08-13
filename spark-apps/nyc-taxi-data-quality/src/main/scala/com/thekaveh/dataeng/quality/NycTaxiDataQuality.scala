package com.thekaveh.dataeng.quality

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.{DataFrame, SparkSession}

final class QualityFailure(message: String) extends RuntimeException(message)

final case class RunResult(
    qualityRunId: String,
    status: String,
    sourceCount: Long,
    cleanCount: Long,
    quarantineCount: Long
)

final class QualityPipeline(store: QualityStore) {
  private def fact(
      arguments: Arguments,
      snapshot: Option[SourceSnapshot],
      ruleId: String,
      numerator: Option[Long],
      denominator: Option[Long],
      value: Option[BigDecimal],
      status: String,
      diagnostic: String
  ): QualityFact = {
    val rule = QualityContract.rules.find(_.ruleId == ruleId)
      .getOrElse(throw new QualityFailure("quality rule is unavailable"))
    QualityFact(
      QualityContract.qualityRunId(arguments.logicalDate, snapshot.map(_.id)),
      Timestamp.from(arguments.logicalDate), Timestamp.from(arguments.dataIntervalEnd),
      QualityContract.DatasetId, "iceberg_snapshot", arguments.upstreamDagId,
      QualityContract.SourceTable, snapshot.map(value => Long.box(value.id)).orNull,
      snapshot.map(value => Timestamp.from(value.committedAt)).orNull,
      snapshot.map(_.schemaSha256).orNull, rule.layer, rule.ruleId, QualityContract.RuleVersion,
      rule.owner, rule.metricName, numerator.map(Long.box).orNull, denominator.map(Long.box).orNull,
      value.map(_.bigDecimal).orNull, rule.warnThreshold.orNull, rule.failThreshold,
      QualityContract.severity(status), status, diagnostic
    )
  }

  private def persistDiagnostic(facts: Seq[QualityFact]): Unit = {
    if (facts.nonEmpty) {
      try store.mergeFacts(facts)
      catch { case _: Throwable => () }
    }
  }

  private def requireFactsReadback(frame: DataFrame, expected: Seq[QualityFact]): Unit = {
    require(frame.schema == QualityContract.factsSchema, "quality facts readback schema mismatch")
    val rows = frame.collect().toSeq
    if (rows.size != expected.size) throw new QualityFailure("quality facts readback count mismatch")
    val observedKeys = rows.map(row => row.getAs[String]("quality_run_id") -> row.getAs[String]("rule_id"))
    val expectedKeys = expected.map(value => value.qualityRunId -> value.ruleId)
    if (observedKeys.distinct.size != observedKeys.size || observedKeys.toSet != expectedKeys.toSet)
      throw new QualityFailure("quality facts readback keys mismatch")
    expected.foreach { value =>
      val row = rows.find(_.getAs[String]("rule_id") == value.ruleId).get
      val matches = row.getAs[String]("quality_run_id") == value.qualityRunId &&
        row.getAs[String]("status") == value.status &&
        row.getAs[String]("severity") == value.severity &&
        row.getAs[String]("diagnostic_code") == value.diagnosticCode &&
        row.getAs[String]("warn_threshold") == value.warnThreshold &&
        row.getAs[String]("fail_threshold") == value.failThreshold
      if (!matches) throw new QualityFailure("quality facts readback values mismatch")
    }
  }

  def run(arguments: Arguments): RunResult = {
    store.ensureFactsTable()
    val snapshot = store.captureSource()
    if (snapshot.isEmpty) {
      persistDiagnostic(Seq(fact(arguments, None, "bronze.source_available.v1",
        None, None, None, "missing", "source_missing")))
      throw new QualityFailure("Bronze source is missing")
    }
    val sourceSnapshot = snapshot.get
    val freshness = QualityTransforms.freshnessStatus(sourceSnapshot.committedAt, arguments.dataIntervalEnd)
    if (freshness == "stale") {
      val age = java.time.Duration.between(sourceSnapshot.committedAt, arguments.dataIntervalEnd).getSeconds
      persistDiagnostic(Seq(fact(arguments, snapshot, "bronze.snapshot_freshness.v1",
        Some(age), Some(21600L), Some(BigDecimal(age).setScale(9)), "stale", "source_stale")))
      throw new QualityFailure("Bronze source is stale")
    }

    val bronze = store.readBronze()
    val sourceCount = bronze.count()
    if (sourceCount <= 0L) {
      persistDiagnostic(Seq(fact(arguments, snapshot, "bronze.source_available.v1",
        Some(0L), None, Some(BigDecimal(0).setScale(9)), "fail", "threshold_fail")))
      throw new QualityFailure("Bronze source is empty")
    }
    val invalidCount = QualityTransforms.invalidCount(bronze)
    val invalid = QualityTransforms.ratioObservation("bronze.invalid_ratio.v1", invalidCount, sourceCount)

    val bronzeFacts = Seq(
      fact(arguments, snapshot, "bronze.source_available.v1", Some(sourceCount), None,
        Some(BigDecimal(sourceCount).setScale(9)), "pass", "ok"),
      fact(arguments, snapshot, "bronze.schema.v1", Some(20L), Some(20L),
        Some(BigDecimal("1.000000000")), "pass", "ok"),
      fact(arguments, snapshot, "bronze.snapshot_freshness.v1",
        Some(java.time.Duration.between(sourceSnapshot.committedAt, arguments.dataIntervalEnd).getSeconds),
        Some(21600L), Some(BigDecimal(java.time.Duration.between(
          sourceSnapshot.committedAt, arguments.dataIntervalEnd).getSeconds).setScale(9)), "pass", "ok"),
      fact(arguments, snapshot, "bronze.invalid_ratio.v1", Some(invalidCount), Some(sourceCount),
        Some(invalid.metricValue), invalid.status, invalid.diagnosticCode)
    )
    if (invalid.status == "fail") {
      persistDiagnostic(bronzeFacts)
      throw new QualityFailure("Bronze invalid ratio exceeds the fail threshold")
    }

    val split = QualityTransforms.split(bronze)
    QualityTransforms.assertPartition(bronze, split.clean, split.quarantine)
    val runId = QualityContract.qualityRunId(arguments.logicalDate, Some(sourceSnapshot.id))
    val properties = QualityProperties.forRun(runId, sourceSnapshot.id)

    store.replaceSilver(QualityContract.CleanTable, split.clean, properties)
    val clean = store.readSilver(QualityContract.CleanTable)
    store.replaceSilver(QualityContract.QuarantineTable, split.quarantine, properties)
    val quarantine = store.readSilver(QualityContract.QuarantineTable)
    QualityTransforms.assertPartition(bronze, clean, quarantine)
    val cleanCount = clean.count()
    val quarantineCount = quarantine.count()

    val postSnapshot = store.captureSource()
    if (!postSnapshot.contains(sourceSnapshot))
      throw new QualityFailure("Bronze source changed during the quality run")

    val quarantineObservation = QualityTransforms.ratioObservation(
      "silver.quarantine_ratio.v1", quarantineCount, sourceCount)
    val silverFacts = Seq(
      fact(arguments, snapshot, "silver.partition_conservation.v1",
        Some(cleanCount + quarantineCount), Some(sourceCount), Some(BigDecimal("1.000000000")), "pass", "ok"),
      fact(arguments, snapshot, "silver.clean_nonempty.v1", Some(cleanCount), Some(sourceCount),
        Some(BigDecimal(cleanCount).setScale(9)), if (cleanCount > 0) "pass" else "fail",
        if (cleanCount > 0) "ok" else "output_empty"),
      fact(arguments, snapshot, "silver.quarantine_ratio.v1", Some(quarantineCount), Some(sourceCount),
        Some(quarantineObservation.metricValue), quarantineObservation.status,
        quarantineObservation.diagnosticCode),
      fact(arguments, snapshot, "silver.output_readback.v1", Some(8L), Some(8L),
        Some(BigDecimal("1.000000000")), "pass", "ok")
    )
    val allFacts = bronzeFacts ++ silverFacts
    val status = QualityContract.overallStatus(allFacts.map(_.status))
    if (status == "fail") {
      persistDiagnostic(allFacts)
      throw new QualityFailure("Silver quality checks failed")
    }
    store.mergeFacts(allFacts)
    requireFactsReadback(store.readFacts(runId), allFacts)
    RunResult(runId, status, sourceCount, cleanCount, quarantineCount)
  }
}

object NycTaxiDataQuality {
  def main(args: Array[String]): Unit = {
    val arguments = QualityContract.parseArguments(args)
    val spark = SparkSession.builder().appName("nyc-taxi-data-quality")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    var primary: Throwable = null
    try {
      val result = new QualityPipeline(new SparkQualityStore(new SparkQualityStorageBackend(spark)))
        .run(arguments)
      // scalastyle:off println
      println(s"quality_run_id=${result.qualityRunId} status=${result.status} " +
        s"source=${result.sourceCount} clean=${result.cleanCount} quarantine=${result.quarantineCount}")
      // scalastyle:on println
    } catch {
      case error: Throwable => primary = error; throw error
    } finally {
      try spark.stop()
      catch {
        case cleanup: Throwable if primary != null => primary.addSuppressed(cleanup)
        case cleanup: Throwable => throw cleanup
      }
    }
  }
}
