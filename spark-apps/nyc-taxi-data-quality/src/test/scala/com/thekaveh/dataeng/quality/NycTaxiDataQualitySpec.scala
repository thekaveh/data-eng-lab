package com.thekaveh.dataeng.quality

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDateTime}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class NycTaxiDataQualitySpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("nyc-quality-pipeline-tests").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val Arguments = com.thekaveh.dataeng.quality.Arguments(
    Instant.parse("2026-08-13T01:00:00Z"),
    Instant.parse("2026-08-13T06:00:00Z"),
    QualityContract.UpstreamDagId
  )
  private val Snapshot = SourceSnapshot(6090932775096319165L,
    Instant.parse("2026-08-13T00:10:00Z"), QualityContract.schemaSha256)

  private def row(fare: Double, passenger: Double, vendor: Long): Row = Row(
    vendor, LocalDateTime.parse("2023-01-01T01:00:00"), LocalDateTime.parse("2023-01-01T01:10:00"),
    passenger, 1.0d, 1.0d, "N", 10L, 20L, 1L, fare, 0.0d, 0.5d, 1.0d, 0.0d,
    0.3d, fare + 1.8d, 0.0d, 0.0d, Date.valueOf("2023-01-01")
  )

  private def bronze(valid: Int = 99, invalid: Int = 1): DataFrame = {
    val values = (1 to valid).map(index => row(10.0d, 2.0d, index.toLong)) ++
      (1 to invalid).map(index => row(-1.0d, 2.0d, (valid + index).toLong))
    spark.createDataFrame(spark.sparkContext.parallelize(values), QualityContract.bronzeSchema)
  }

  private def factsFrame(values: Seq[QualityFact]): DataFrame = {
    val rows = values.map { value => Row(
      value.qualityRunId, value.logicalDate, value.dataIntervalEnd, value.datasetId,
      value.bindingType, value.upstreamDagId, value.sourceTable, value.sourceSnapshotId,
      value.sourceSnapshotCommittedAt, value.sourceSchemaSha256, value.layer, value.ruleId,
      value.ruleVersion, value.owner, value.metricName, value.metricNumerator,
      value.metricDenominator, value.metricValue, value.warnThreshold, value.failThreshold,
      value.severity, value.status, value.diagnosticCode
    ) }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), QualityContract.factsSchema)
  }

  private class RecordingStore(
      var current: Option[SourceSnapshot] = Some(Snapshot),
      val source: DataFrame = bronze(),
      var failAt: String = ""
  ) extends QualityStore {
    val actions = ArrayBuffer.empty[String]
    var clean: DataFrame = null
    var quarantine: DataFrame = null
    var silverProperties = Map.empty[String, Map[String, String]]
    var facts = Seq.empty[QualityFact]

    private def action(name: String): Unit = {
      actions += name
      if (failAt == name) throw new IllegalStateException(s"failure:$name")
    }
    override def ensureFactsTable(): Unit = action("ensureFacts")
    override def captureSource(): Option[SourceSnapshot] = { action("captureSource"); current }
    override def readBronze(): DataFrame = { action("readBronze"); source }
    override def replaceSilver(identifier: String, frame: DataFrame, properties: Map[String, String]): Unit = {
      val name = if (identifier == QualityContract.CleanTable) "replaceClean" else "replaceQuarantine"
      action(name)
      if (identifier == QualityContract.CleanTable) clean = frame else quarantine = frame
      silverProperties += identifier -> properties
    }
    override def readSilver(identifier: String): DataFrame = {
      val name = if (identifier == QualityContract.CleanTable) "readClean" else "readQuarantine"
      action(name)
      if (identifier == QualityContract.CleanTable) clean else quarantine
    }
    override def mergeFacts(values: Seq[QualityFact]): Unit = { action("mergeFacts"); facts = values }
    override def readFacts(runId: String): DataFrame = {
      action("readFacts")
      factsFrame(facts)
    }
  }

  test("accepted run follows exact write and readback order and persists eight facts") {
    val store = new RecordingStore()
    val result = new QualityPipeline(store).run(Arguments)
    assert(store.actions == Seq("ensureFacts", "captureSource", "readBronze", "replaceClean", "readClean",
      "replaceQuarantine", "readQuarantine", "captureSource", "mergeFacts", "readFacts"))
    assert(result.status == "pass")
    assert(result.sourceCount == 100L)
    assert(result.cleanCount == 99L)
    assert(result.quarantineCount == 1L)
    assert(store.facts.map(_.ruleId) == QualityContract.ExpectedRuleIds)
    assert(store.facts.map(_.qualityRunId).distinct == Seq(result.qualityRunId))
    val readback = store.facts.find(_.ruleId == "silver.output_readback.v1").get
    assert((readback.metricNumerator, readback.metricDenominator, readback.metricValue.toString) ==
      (Long.box(8L), Long.box(8L), "1.000000000"))
  }

  test("warning succeeds only after exact facts readback") {
    val store = new RecordingStore(source = bronze(valid = 98, invalid = 2))
    val result = new QualityPipeline(store).run(Arguments)
    assert(result.status == "warn")
    assert(store.actions.takeRight(2) == Seq("mergeFacts", "readFacts"))
    assert(store.facts.filter(_.ruleId.endsWith("ratio.v1")).forall(_.status == "warn"))
  }

  test("missing, stale, schema, and hard threshold failures never mutate Silver") {
    val missing = new RecordingStore(current = None)
    assertThrows[QualityFailure](new QualityPipeline(missing).run(Arguments))
    assert(!missing.actions.exists(_.startsWith("replace")))

    val stale = new RecordingStore(current = Some(Snapshot.copy(
      committedAt = Instant.parse("2026-08-12T23:59:59Z"))))
    assertThrows[QualityFailure](new QualityPipeline(stale).run(Arguments))
    assert(!stale.actions.exists(_.startsWith("replace")))

    val fail = new RecordingStore(source = bronze(valid = 94, invalid = 6))
    assertThrows[QualityFailure](new QualityPipeline(fail).run(Arguments))
    assert(!fail.actions.exists(_.startsWith("replace")))
  }

  test("failure at each boundary prevents every later operation") {
    val boundaries = Seq(
      "ensureFacts", "captureSource", "readBronze", "replaceClean", "readClean",
      "replaceQuarantine", "readQuarantine", "mergeFacts", "readFacts"
    )
    boundaries.foreach { boundary =>
      val store = new RecordingStore(failAt = boundary)
      assertThrows[RuntimeException](new QualityPipeline(store).run(Arguments))
      assert(store.actions.contains(boundary))
      assert(!store.actions.dropWhile(_ != boundary).drop(1).exists(_.startsWith("replace")))
    }
  }

  test("source snapshot change after Silver readback fails before fact merge") {
    val store = new RecordingStore() {
      private var captures = 0
      override def captureSource(): Option[SourceSnapshot] = {
        captures += 1
        actions += "captureSource"
        if (captures == 1) Some(Snapshot) else Some(Snapshot.copy(id = Snapshot.id + 1L))
      }
    }
    assertThrows[QualityFailure](new QualityPipeline(store).run(Arguments))
    assert(store.facts.map(_.ruleId) == Seq("silver.output_readback.v1"))
    assert(store.facts.head.diagnosticCode == "readback_mismatch")
  }

  test("failure between Silver writes converges on same-snapshot retry without duplicate facts") {
    val store = new RecordingStore(failAt = "replaceQuarantine")
    assertThrows[RuntimeException](new QualityPipeline(store).run(Arguments))
    assert(store.clean != null && store.quarantine == null)
    assert(store.facts.map(_.ruleId) == Seq("silver.output_readback.v1"))
    assert(store.silverProperties.keySet == Set(QualityContract.CleanTable))
    store.failAt = ""
    val first = new QualityPipeline(store).run(Arguments)
    val second = new QualityPipeline(store).run(Arguments)
    assert(first.qualityRunId == second.qualityRunId)
    assert(store.facts.size == 8)
    assert(store.facts.map(fact => fact.qualityRunId -> fact.ruleId).distinct.size == 8)
    assert(store.silverProperties.keySet == Set(QualityContract.CleanTable, QualityContract.QuarantineTable))
    assert(store.silverProperties.values.toSet.size == 1)
  }

  test("fact readback rejects missing, duplicate, or mismatched accepted rows") {
    val missing = new RecordingStore() {
      override def readFacts(runId: String): DataFrame = {
        actions += "readFacts"
        factsFrame(facts.dropRight(1))
      }
    }
    assertThrows[QualityFailure](new QualityPipeline(missing).run(Arguments))

    val duplicate = new RecordingStore() {
      override def readFacts(runId: String): DataFrame = {
        actions += "readFacts"
        factsFrame(facts :+ facts.head)
      }
    }
    assertThrows[QualityFailure](new QualityPipeline(duplicate).run(Arguments))
  }

  test("fact readback compares all 23 fields including timestamps decimals nulls and lineage") {
    val corruptions: Seq[(Int, Any)] = Seq(
      0 -> ("f" * 64),
      1 -> Timestamp.from(Instant.parse("2026-08-13T01:00:01Z")),
      2 -> Timestamp.from(Instant.parse("2026-08-13T06:00:01Z")),
      3 -> "other_dataset",
      4 -> "other_binding",
      5 -> "other_upstream",
      6 -> "lakehouse.bronze.other",
      7 -> Long.box(Snapshot.id + 1L),
      8 -> null,
      9 -> ("f" * 64),
      10 -> "Other",
      11 -> "other.rule.v1",
      12 -> "other_rule_version",
      13 -> "Other Owner",
      14 -> "wrong_metric",
      15 -> Long.box(999L),
      16 -> Long.box(999L),
      17 -> new java.math.BigDecimal("0.999999999"),
      18 -> "ratio>0.999999999",
      19 -> "wrong_fail_threshold",
      20 -> "error",
      21 -> "fail",
      22 -> "wrong_diagnostic"
    )
    corruptions.foreach { case (index, replacement) =>
      val store = new RecordingStore() {
        override def readFacts(runId: String): DataFrame = {
          actions += "readFacts"
          val original = factsFrame(facts).collect().toSeq
          val values = original.head.toSeq.updated(index, replacement)
          spark.createDataFrame(
            spark.sparkContext.parallelize(Row.fromSeq(values) +: original.tail),
            QualityContract.factsSchema
          )
        }
      }
      val failure = intercept[QualityFailure](new QualityPipeline(store).run(Arguments))
      assert(failure.category == "facts_readback")
      assert(failure.diagnosticCode == "readback_mismatch")
    }
  }

  test("fact readback evaluation failures are bounded and categorized") {
    val store = new RecordingStore() {
      override def readFacts(runId: String): DataFrame = {
        actions += "readFacts"
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[Row],
          org.apache.spark.sql.types.StructType(QualityContract.factsSchema.fields.reverse)
        )
      }
    }
    val failure = intercept[QualityFailure](new QualityPipeline(store).run(Arguments))
    assert(failure.category == "facts_readback")
    assert(failure.diagnosticCode == "readback_mismatch")
    assert(failure.getMessage == "Quality facts readback does not match the intended rows")
  }

  test("controlled boundary failures expose bounded categories and persist only safe diagnostics") {
    val scenarios = Seq(
      "captureSource" -> "source_metadata",
      "readBronze" -> "source_read",
      "replaceClean" -> "silver_write",
      "readClean" -> "silver_readback",
      "replaceQuarantine" -> "silver_write",
      "readQuarantine" -> "silver_readback"
    )
    scenarios.foreach { case (boundary, category) =>
      val store = new RecordingStore(failAt = boundary)
      val failure = intercept[QualityFailure](new QualityPipeline(store).run(Arguments))
      assert(failure.category == category)
      assert(QualityContract.DiagnosticCodes.contains(failure.diagnosticCode))
      assert(failure.getMessage.length <= 160 && !failure.getMessage.contains("failure:"))
      assert(!store.actions.dropWhile(_ != boundary).drop(1).exists(_.startsWith("replace")))
      if (boundary != "captureSource") assert(store.facts.nonEmpty)
    }
  }

  test("Spark action and postcheck failures are controlled and persist safe diagnostics") {
    val invalidFailure = new RecordingStore(source = bronze().withColumn(
      "fare_amount", org.apache.spark.sql.functions.expr("raise_error('injected-invalid-count')").cast("double")
    ))
    val invalid = intercept[QualityFailure](new QualityPipeline(invalidFailure).run(Arguments))
    assert(invalid.diagnosticCode == "readback_mismatch")
    assert(!invalid.getMessage.contains("secret"))
    assert(invalidFailure.facts.map(_.ruleId) == Seq("bronze.invalid_ratio.v1"))

    val countFailure = new RecordingStore() {
      override def readSilver(identifier: String): DataFrame = {
        actions += (if (identifier == QualityContract.CleanTable) "readClean" else "readQuarantine")
        val frame = if (identifier == QualityContract.CleanTable) clean else quarantine
        frame.withColumn("fare_amount",
          org.apache.spark.sql.functions.expr("raise_error('injected-output-count')").cast("double"))
      }
    }
    val output = intercept[QualityFailure](new QualityPipeline(countFailure).run(Arguments))
    assert(output.diagnosticCode == "readback_mismatch")
    assert(!output.getMessage.contains("secret"))
    assert(countFailure.facts.map(_.ruleId) == Seq("silver.output_readback.v1"))

    val postcheckFailure = new RecordingStore() {
      private var captures = 0
      override def captureSource(): Option[SourceSnapshot] = {
        captures += 1
        actions += "captureSource"
        if (captures == 1) Some(Snapshot) else throw new IllegalStateException("injected-postcheck")
      }
    }
    val postcheck = intercept[QualityFailure](new QualityPipeline(postcheckFailure).run(Arguments))
    assert(postcheck.diagnosticCode == "readback_mismatch")
    assert(postcheckFailure.facts.map(_.ruleId) == Seq("silver.output_readback.v1"))
  }

  test("schema mismatch persists the schema diagnostic before any Silver mutation") {
    val wrong = spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      org.apache.spark.sql.types.StructType(QualityContract.bronzeSchema.fields.reverse)
    )
    val store = new RecordingStore(source = wrong)
    val failure = intercept[QualityFailure](new QualityPipeline(store).run(Arguments))
    assert(failure.category == "source_schema")
    assert(failure.diagnosticCode == "schema_mismatch")
    assert(store.facts.map(_.ruleId) == Seq("bronze.schema.v1"))
    assert(!store.actions.exists(_.startsWith("replace")))
  }
}
