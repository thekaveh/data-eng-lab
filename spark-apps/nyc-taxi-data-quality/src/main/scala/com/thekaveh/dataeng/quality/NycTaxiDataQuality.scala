package com.thekaveh.dataeng.quality

import java.sql.Timestamp
import java.time.Instant

import scala.util.control.NonFatal

import org.apache.spark.sql.{DataFrame, SparkSession}

final class QualityFailure(
    val category: String,
    val diagnosticCode: String,
    message: String,
    cause: Throwable = null
) extends RuntimeException(message, cause) {
  require(category.matches("[a-z0-9_]{1,64}"), "quality failure category is invalid")
  QualityContract.requireDiagnosticCode(diagnosticCode)
  require(message.nonEmpty && message.length <= 160, "quality failure message is invalid")
}

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
      .getOrElse(throw new IllegalArgumentException("Quality rule is unavailable"))
    QualityContract.requireRuleStatusDiagnostic(ruleId, status, diagnostic)
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
      catch { case NonFatal(_) => () }
    }
  }

  private def controlled[T](category: String, diagnostic: String, message: String)(operation: => T): T =
    try operation
    catch {
      case failure: QualityFailure => throw failure
      case NonFatal(_) => throw new QualityFailure(category, diagnostic, message)
    }

  private def diagnosticFailure(
      arguments: Arguments,
      snapshot: Option[SourceSnapshot],
      ruleId: String,
      category: String,
      diagnostic: String,
      message: String
  ): Nothing = {
    persistDiagnostic(Seq(fact(arguments, snapshot, ruleId, None, None, None, "fail", diagnostic)))
    throw new QualityFailure(category, diagnostic, message)
  }

  private def requireFactsReadback(frame: DataFrame, expected: Seq[QualityFact]): Unit = {
    if (frame.schema != QualityContract.factsSchema) throw new QualityFailure(
      "facts_readback", "readback_mismatch", "Quality facts readback does not match the intended rows")
    val intended = SparkQualityStorageBackend.factFrame(frame.sparkSession, expected)
    val observedCount = frame.count()
    val exact = observedCount == expected.size &&
      frame.exceptAll(intended).limit(1).count() == 0L &&
      intended.exceptAll(frame).limit(1).count() == 0L
    if (!exact) throw new QualityFailure(
      "facts_readback", "readback_mismatch", "Quality facts readback does not match the intended rows")
  }

  def run(arguments: Arguments): RunResult = {
    controlled("facts_store", "readback_mismatch", "Quality facts storage is unavailable") {
      store.ensureFactsTable()
    }
    val snapshot = try controlled(
      "source_metadata", "readback_mismatch", "Bronze snapshot metadata is unavailable") {
      store.captureSource()
    } catch {
      case failure: QualityFailure =>
        val ruleId = if (failure.category == "source_schema") "bronze.schema.v1"
        else "bronze.source_available.v1"
        persistDiagnostic(Seq(fact(arguments, None, ruleId,
          None, None, None, "fail", failure.diagnosticCode)))
        throw failure
    }
    if (snapshot.isEmpty) {
      persistDiagnostic(Seq(fact(arguments, None, "bronze.source_available.v1",
        None, None, None, "missing", "source_missing")))
      throw new QualityFailure("source_missing", "source_missing", "Bronze source is missing")
    }
    val sourceSnapshot = snapshot.get
    val freshness = QualityTransforms.freshnessStatus(sourceSnapshot.committedAt, arguments.dataIntervalEnd)
    if (freshness == "stale") {
      val age = java.time.Duration.between(sourceSnapshot.committedAt, arguments.dataIntervalEnd).getSeconds
      persistDiagnostic(Seq(fact(arguments, snapshot, "bronze.snapshot_freshness.v1",
        Some(age), Some(21600L), Some(BigDecimal(age).setScale(9)), "stale", "source_stale")))
      throw new QualityFailure("source_stale", "source_stale", "Bronze source is stale")
    }

    val bronze = try controlled("source_read", "readback_mismatch", "Bronze source read failed") {
      store.readBronze()
    } catch {
      case failure: QualityFailure =>
        persistDiagnostic(Seq(fact(arguments, snapshot, "bronze.source_available.v1",
          None, None, None, "fail", failure.diagnosticCode)))
        throw failure
    }
    try controlled("source_schema", "schema_mismatch", "Bronze source schema does not match") {
      QualityTransforms.assertExactSchema(bronze)
    } catch {
      case failure: QualityFailure =>
        persistDiagnostic(Seq(fact(arguments, snapshot, "bronze.schema.v1",
          Some(0L), Some(20L), Some(BigDecimal("0.000000000")), "fail", failure.diagnosticCode)))
        throw failure
    }
    val sourceCount = try controlled("source_read", "readback_mismatch", "Bronze source count failed") {
      bronze.count()
    } catch {
      case failure: QualityFailure =>
        persistDiagnostic(Seq(fact(arguments, snapshot, "bronze.source_available.v1",
          None, None, None, "fail", failure.diagnosticCode)))
        throw failure
    }
    if (sourceCount <= 0L) {
      persistDiagnostic(Seq(fact(arguments, snapshot, "bronze.source_available.v1",
        Some(0L), None, Some(BigDecimal(0).setScale(9)), "fail", "threshold_fail")))
      throw new QualityFailure("source_empty", "threshold_fail", "Bronze source is empty")
    }
    val invalidCount = try controlled("source_read", "readback_mismatch", "Bronze invalid-row count failed") {
      QualityTransforms.invalidCount(bronze)
    } catch {
      case failure: QualityFailure =>
        persistDiagnostic(Seq(fact(arguments, snapshot, "bronze.invalid_ratio.v1",
          None, Some(sourceCount), None, "fail", failure.diagnosticCode)))
        throw failure
    }
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
      throw new QualityFailure("source_threshold", "threshold_fail",
        "Bronze invalid ratio exceeds the fail threshold")
    }

    val split = controlled("partition_validation", "partition_mismatch", "Quality partition validation failed") {
      QualityTransforms.split(bronze)
    }
    try controlled("partition_validation", "partition_mismatch", "Quality partition validation failed") {
      QualityTransforms.assertPartition(bronze, split.clean, split.quarantine)
    } catch {
      case failure: QualityFailure =>
        persistDiagnostic(Seq(fact(arguments, snapshot, "silver.partition_conservation.v1",
          None, Some(sourceCount), None, "fail", failure.diagnosticCode)))
        throw failure
    }
    val runId = QualityContract.qualityRunId(arguments.logicalDate, Some(sourceSnapshot.id))
    val properties = QualityProperties.forRun(runId, sourceSnapshot.id)

    try controlled("silver_write", "readback_mismatch", "Clean Silver write failed") {
      store.replaceSilver(QualityContract.CleanTable, split.clean, properties)
    } catch {
      case failure: QualityFailure => diagnosticFailure(arguments, snapshot,
        "silver.output_readback.v1", failure.category, failure.diagnosticCode, failure.getMessage)
    }
    val clean = try controlled("silver_readback", "readback_mismatch", "Clean Silver readback failed") {
      store.readSilver(QualityContract.CleanTable)
    } catch {
      case failure: QualityFailure => diagnosticFailure(arguments, snapshot,
        "silver.output_readback.v1", failure.category, failure.diagnosticCode, failure.getMessage)
    }
    try controlled("silver_write", "readback_mismatch", "Quarantine Silver write failed") {
      store.replaceSilver(QualityContract.QuarantineTable, split.quarantine, properties)
    } catch {
      case failure: QualityFailure => diagnosticFailure(arguments, snapshot,
        "silver.output_readback.v1", failure.category, failure.diagnosticCode, failure.getMessage)
    }
    val quarantine = try controlled(
      "silver_readback", "readback_mismatch", "Quarantine Silver readback failed") {
      store.readSilver(QualityContract.QuarantineTable)
    } catch {
      case failure: QualityFailure => diagnosticFailure(arguments, snapshot,
        "silver.output_readback.v1", failure.category, failure.diagnosticCode, failure.getMessage)
    }
    try controlled("silver_readback", "readback_mismatch", "Silver partition readback failed") {
      QualityTransforms.assertPartition(bronze, clean, quarantine)
    } catch {
      case failure: QualityFailure => diagnosticFailure(arguments, snapshot,
        "silver.output_readback.v1", failure.category, failure.diagnosticCode, failure.getMessage)
    }
    val (cleanCount, quarantineCount) = try controlled(
      "silver_readback", "readback_mismatch", "Silver output count failed") {
      clean.count() -> quarantine.count()
    } catch {
      case failure: QualityFailure => diagnosticFailure(arguments, snapshot,
        "silver.output_readback.v1", failure.category, failure.diagnosticCode, failure.getMessage)
    }

    val postSnapshot = try controlled(
      "source_metadata", "readback_mismatch", "Bronze snapshot recheck failed") {
      store.captureSource()
    } catch {
      case failure: QualityFailure => diagnosticFailure(arguments, snapshot,
        "silver.output_readback.v1", failure.category, failure.diagnosticCode, failure.getMessage)
    }
    if (!postSnapshot.contains(sourceSnapshot))
      diagnosticFailure(arguments, snapshot, "silver.output_readback.v1",
        "source_changed", "readback_mismatch", "Bronze source changed during the quality run")

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
      throw new QualityFailure("silver_threshold", "threshold_fail", "Silver quality checks failed")
    }
    controlled("facts_write", "readback_mismatch", "Quality facts MERGE failed") {
      store.mergeFacts(allFacts)
    }
    val readback = controlled("facts_readback", "readback_mismatch", "Quality facts readback failed") {
      store.readFacts(runId)
    }
    controlled("facts_readback", "readback_mismatch",
      "Quality facts readback does not match the intended rows") {
      requireFactsReadback(readback, allFacts)
    }
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
