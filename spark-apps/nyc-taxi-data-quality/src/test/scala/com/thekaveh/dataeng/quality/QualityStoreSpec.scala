package com.thekaveh.dataeng.quality

import java.sql.Timestamp
import java.time.Instant

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class QualityStoreSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("nyc-quality-store-tests").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def empty(schema: StructType): DataFrame =
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

  private def rows(schema: StructType, values: Seq[Row]): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(values), schema)

  private val MetadataSchema = StructType(Seq(
    StructField("snapshot_id", LongType, nullable = false),
    StructField("committed_at", TimestampType, nullable = false)
  ))
  private val PropertiesSchema = StructType(Seq(
    StructField("key", StringType, nullable = false),
    StructField("value", StringType, nullable = false)
  ))

  private final class RecordingBackend(
      var exists: Boolean = true,
      var source: DataFrame = null,
      var metadata: DataFrame = null,
      var properties: DataFrame = null,
      var facts: DataFrame = null
  ) extends QualityStorageBackend {
    val actions = ArrayBuffer.empty[String]
    var merged = Seq.empty[QualityFact]

    override def tableExists(identifier: String): Boolean = { actions += s"exists:$identifier"; exists }
    override def table(identifier: String): DataFrame = {
      actions += s"table:$identifier"
      if (identifier == QualityContract.SourceTable) source
      else if (identifier == QualityContract.FactsTable) facts
      else source
    }
    override def query(statement: String): DataFrame = {
      actions += s"query:$statement"
      if (statement.contains(".refs")) metadata else properties
    }
    override def createNamespace(identifier: String): Unit = actions += s"namespace:$identifier"
    override def createFactsTable(): Unit = actions += "create:facts"
    override def replace(identifier: String, frame: DataFrame): Unit = actions += s"replace:$identifier"
    override def setProperties(identifier: String, values: Map[String, String]): Unit =
      actions += s"properties:$identifier:${values.toSeq.sorted.mkString(",")}"
    override def mergeFacts(values: Seq[QualityFact]): Unit = { actions += "merge:facts"; merged = values }
  }

  private def sourceFrame: DataFrame = empty(QualityContract.bronzeSchema)

  private def metadataFrame(id: Long = 6090932775096319165L): DataFrame = rows(
    MetadataSchema,
    Seq(Row(id, Timestamp.from(Instant.parse("2026-08-13T00:10:00Z"))))
  )

  private def propertyFrame(values: Map[String, String]): DataFrame = rows(
    PropertiesSchema,
    values.toSeq.sorted.map { case (key, value) => Row(key, value) }
  )

  private def fact(runId: String, ruleId: String): QualityFact = {
    val rule = QualityContract.rules.find(_.ruleId == ruleId).get
    QualityFact(
      runId, Timestamp.from(Instant.parse("2026-08-13T01:00:00Z")),
      Timestamp.from(Instant.parse("2026-08-14T00:00:00Z")), "nyc_taxi",
      "iceberg_snapshot", "nyc_taxi_etl", QualityContract.SourceTable,
      6090932775096319165L, Timestamp.from(Instant.parse("2026-08-13T00:10:00Z")),
      QualityContract.schemaSha256, rule.layer, rule.ruleId, QualityContract.RuleVersion,
      rule.owner, rule.metricName, 1L, 1L, new java.math.BigDecimal("1.000000000"),
      rule.warnThreshold.orNull, rule.failThreshold, "info", "pass", "ok"
    )
  }

  test("quality properties bind both Silver tables only to snapshot, rule, and run") {
    val runId = "a" * 64
    val expected = Map(
      "data_eng_lab.quality.binding" -> "iceberg_snapshot",
      "data_eng_lab.quality.source_table" -> QualityContract.SourceTable,
      "data_eng_lab.quality.source_snapshot_id" -> "6090932775096319165",
      "data_eng_lab.quality.rule_version" -> QualityContract.RuleVersion,
      "data_eng_lab.quality.run_id" -> runId
    )
    assert(QualityProperties.forRun(runId, 6090932775096319165L) == expected)
    assert(expected.keys.forall(!_.startsWith("data_eng_lab.dataset")))
    assertThrows[IllegalArgumentException](QualityProperties.forRun("short", 1L))
    assertThrows[IllegalArgumentException](QualityProperties.forRun(runId, 0L))
  }

  test("captures exactly one positive current Bronze snapshot and exact schema") {
    val backend = new RecordingBackend(source = sourceFrame, metadata = metadataFrame())
    val store = new SparkQualityStore(backend)
    val captured = store.captureSource().get
    assert(captured == SourceSnapshot(6090932775096319165L,
      Instant.parse("2026-08-13T00:10:00Z"), QualityContract.schemaSha256))
    assert(backend.actions.head == s"exists:${QualityContract.SourceTable}")
    assert(backend.actions.exists(action => action.contains(".refs") && action.contains(".snapshots")))
  }

  test("missing source is explicit while malformed metadata and schema fail closed") {
    val missing = new RecordingBackend(exists = false)
    assert(new SparkQualityStore(missing).captureSource().isEmpty)

    val absentMain = rows(MetadataSchema, Seq.empty)
    assert(new SparkQualityStore(
      new RecordingBackend(source = sourceFrame, metadata = absentMain)).captureSource().isEmpty)

    val duplicateMetadata = rows(MetadataSchema, Seq(
      Row(1L, Timestamp.from(Instant.parse("2026-08-13T00:00:00Z"))),
      Row(2L, Timestamp.from(Instant.parse("2026-08-13T00:01:00Z")))
    ))
    val duplicate = intercept[QualityFailure](new SparkQualityStore(
      new RecordingBackend(source = sourceFrame, metadata = duplicateMetadata)).captureSource())
    assert(duplicate.diagnosticCode == "readback_mismatch")
    val malformed = intercept[QualityFailure](new SparkQualityStore(
      new RecordingBackend(source = sourceFrame, metadata = metadataFrame(0L))).captureSource())
    assert(malformed.diagnosticCode == "readback_mismatch")

    val drift = empty(StructType(QualityContract.bronzeSchema.fields.reverse))
    val schemaFailure = intercept[QualityFailure](new SparkQualityStore(
      new RecordingBackend(source = drift, metadata = metadataFrame())).captureSource())
    assert(schemaFailure.category == "source_schema")
    assert(schemaFailure.diagnosticCode == "schema_mismatch")
  }

  test("replaces one Silver table then requires the exact property readback") {
    val runId = "b" * 64
    val intended = QualityProperties.forRun(runId, 6090932775096319165L)
    val backend = new RecordingBackend(
      source = sourceFrame,
      metadata = metadataFrame(),
      properties = propertyFrame(intended)
    )
    val store = new SparkQualityStore(backend)
    store.replaceSilver(QualityContract.CleanTable, sourceFrame, intended)
    assert(backend.actions.indexOf(s"replace:${QualityContract.CleanTable}") <
      backend.actions.indexWhere(_.startsWith(s"properties:${QualityContract.CleanTable}:")))
    assert(backend.actions.contains(s"query:SHOW TBLPROPERTIES ${QualityContract.CleanTable}"))
    assert(!backend.actions.exists(_.contains(".properties")))

    val mismatch = new RecordingBackend(source = sourceFrame, metadata = metadataFrame(),
      properties = propertyFrame(intended.updated("data_eng_lab.quality.run_id", "c" * 64)))
    assertThrows[IllegalArgumentException](
      new SparkQualityStore(mismatch).replaceSilver(QualityContract.CleanTable, sourceFrame, intended))
    assertThrows[IllegalArgumentException](
      store.replaceSilver("lakehouse.silver.other", sourceFrame, intended))
  }

  test("Spark SHOW TBLPROPERTIES returns the key-value shape used by production readback") {
    val identifier = "default.quality_properties_syntax"
    spark.sql(s"DROP TABLE IF EXISTS $identifier")
    try {
      spark.sql(s"CREATE TABLE $identifier (id BIGINT) USING parquet")
      spark.sql(s"ALTER TABLE $identifier SET TBLPROPERTIES ('data_eng_lab.quality.run_id'='${"f" * 64}')")
      val backend = new SparkQualityStorageBackend(spark)
      val rows = backend.query(s"SHOW TBLPROPERTIES $identifier").collect().toSeq
      assert(rows.exists(row => row.getAs[String]("key") == "data_eng_lab.quality.run_id" &&
        row.getAs[String]("value") == "f" * 64))
      assertThrows[org.apache.spark.sql.AnalysisException](
        backend.query(s"SELECT key, value FROM $identifier.properties").collect())
    } finally spark.sql(s"DROP TABLE IF EXISTS $identifier")
  }

  test("facts table setup is exact and validates an existing schema") {
    val createBackend = new RecordingBackend(exists = false, facts = empty(QualityContract.factsSchema))
    new SparkQualityStore(createBackend).ensureFactsTable()
    assert(createBackend.actions == Seq(s"exists:${QualityContract.FactsTable}",
      "namespace:lakehouse.gold", "create:facts"))

    val existing = new RecordingBackend(exists = true, facts = empty(QualityContract.factsSchema))
    new SparkQualityStore(existing).ensureFactsTable()
    assert(existing.actions == Seq(s"exists:${QualityContract.FactsTable}",
      s"table:${QualityContract.FactsTable}"))

    val wrong = new RecordingBackend(exists = true,
      facts = empty(StructType(QualityContract.factsSchema.fields.reverse)))
    assertThrows[IllegalArgumentException](new SparkQualityStore(wrong).ensureFactsTable())
  }

  test("fact MERGE accepts one bounded run with unique governed composite keys") {
    val runId = "d" * 64
    val facts = QualityContract.ExpectedRuleIds.map(fact(runId, _))
    val backend = new RecordingBackend()
    val store = new SparkQualityStore(backend)
    store.mergeFacts(facts)
    assert(backend.merged.map(item => item.qualityRunId -> item.ruleId).distinct.size == 8)
    assertThrows[IllegalArgumentException](store.mergeFacts(facts :+ facts.head))
    assertThrows[IllegalArgumentException](store.mergeFacts(Seq.empty))
    assertThrows[IllegalArgumentException](store.mergeFacts(Seq(fact(runId, facts.head.ruleId),
      fact("e" * 64, facts(1).ruleId))))
    assertThrows[IllegalArgumentException](store.mergeFacts(Seq(
      fact(runId, facts.head.ruleId).copy(diagnosticCode = "source_missing"))))
    assertThrows[IllegalArgumentException](store.mergeFacts(Seq(
      fact(runId, facts.head.ruleId).copy(severity = "error"))))
    assertThrows[IllegalArgumentException](store.mergeFacts(Seq(
      fact(runId, "bronze.schema.v1").copy(
        status = "fail", severity = "error", diagnosticCode = "output_empty"))))
  }

  test("literal Iceberg MERGE matches only the composite run and rule key") {
    val sql = SparkQualityStorageBackend.MergeFactsSql
    assert(sql.contains("target.quality_run_id = source.quality_run_id"))
    assert(sql.contains("target.rule_id = source.rule_id"))
    assert(sql.contains("WHEN MATCHED THEN UPDATE SET"))
    assert(sql.contains("WHEN NOT MATCHED THEN INSERT"))
    assert(!sql.contains("data_eng_lab.dataset"))
  }

  test("production fact conversion binds exact snake-case schema before MERGE") {
    val runId = "9" * 64
    val values = QualityContract.ExpectedRuleIds.map(fact(runId, _))
    val frame = SparkQualityStorageBackend.factFrame(spark, values)
    assert(frame.schema == QualityContract.factsSchema)
    assert(frame.columns.toSeq == QualityContract.factsSchema.fieldNames.toSeq)
    assert(frame.count() == 8L)
    frame.createOrReplaceTempView("quality_fact_binding_probe")
    try {
      val keys = spark.sql(
        "SELECT quality_run_id, rule_id, metric_value, logical_date " +
          "FROM quality_fact_binding_probe ORDER BY rule_id"
      ).collect()
      assert(keys.length == 8)
      assert(keys.forall(_.getAs[String]("quality_run_id") == runId))
      assert(keys.forall(_.getAs[java.math.BigDecimal]("metric_value").scale() == 9))
      assert(keys.forall(_.getAs[Timestamp]("logical_date").toInstant ==
        Instant.parse("2026-08-13T01:00:00Z")))
    } finally spark.catalog.dropTempView("quality_fact_binding_probe")
  }
}
