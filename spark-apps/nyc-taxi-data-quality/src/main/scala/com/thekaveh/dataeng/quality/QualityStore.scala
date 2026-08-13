package com.thekaveh.dataeng.quality

import java.time.Instant

import org.apache.spark.sql.{DataFrame, SparkSession}

trait QualityStorageBackend {
  def tableExists(identifier: String): Boolean
  def table(identifier: String): DataFrame
  def query(statement: String): DataFrame
  def createNamespace(identifier: String): Unit
  def createFactsTable(): Unit
  def replace(identifier: String, frame: DataFrame): Unit
  def setProperties(identifier: String, values: Map[String, String]): Unit
  def mergeFacts(values: Seq[QualityFact]): Unit
}

trait QualityStore {
  def ensureFactsTable(): Unit
  def captureSource(): Option[SourceSnapshot]
  def readBronze(): DataFrame
  def replaceSilver(identifier: String, frame: DataFrame, properties: Map[String, String]): Unit
  def readSilver(identifier: String): DataFrame
  def mergeFacts(values: Seq[QualityFact]): Unit
  def readFacts(runId: String): DataFrame
}

object QualityProperties {
  private val RunId = "^[0-9a-f]{64}$".r

  def forRun(runId: String, snapshotId: Long): Map[String, String] = {
    require(RunId.pattern.matcher(runId).matches(), "quality run ID is invalid")
    require(snapshotId > 0, "source snapshot must be positive")
    Map(
      "data_eng_lab.quality.binding" -> "iceberg_snapshot",
      "data_eng_lab.quality.source_table" -> QualityContract.SourceTable,
      "data_eng_lab.quality.source_snapshot_id" -> snapshotId.toString,
      "data_eng_lab.quality.rule_version" -> QualityContract.RuleVersion,
      "data_eng_lab.quality.run_id" -> runId
    )
  }
}

final class SparkQualityStore(backend: QualityStorageBackend) extends QualityStore {
  private val SilverTables = Set(QualityContract.CleanTable, QualityContract.QuarantineTable)

  override def ensureFactsTable(): Unit = {
    if (!backend.tableExists(QualityContract.FactsTable)) {
      backend.createNamespace("lakehouse.gold")
      backend.createFactsTable()
    } else {
      val frame = backend.table(QualityContract.FactsTable)
      require(frame.schema == QualityContract.factsSchema, "quality facts schema is invalid")
    }
  }

  override def captureSource(): Option[SourceSnapshot] = {
    if (!backend.tableExists(QualityContract.SourceTable)) None
    else {
      val source = backend.table(QualityContract.SourceTable)
      QualityTransforms.assertExactSchema(source)
      val statement =
        s"""SELECT r.snapshot_id, s.committed_at
           |FROM ${QualityContract.SourceTable}.refs r
           |JOIN ${QualityContract.SourceTable}.snapshots s ON r.snapshot_id = s.snapshot_id
           |WHERE r.name = 'main'""".stripMargin
      val rows = backend.query(statement).collect().toSeq
      require(rows.size == 1, "Bronze main snapshot metadata is invalid")
      val id = rows.head.getAs[Long]("snapshot_id")
      val committedAt = rows.head.getAs[java.sql.Timestamp]("committed_at")
      require(id > 0 && committedAt != null, "Bronze main snapshot metadata is invalid")
      Some(SourceSnapshot(id, committedAt.toInstant, QualityContract.schemaSha256))
    }
  }

  override def readBronze(): DataFrame = {
    val frame = backend.table(QualityContract.SourceTable)
    QualityTransforms.assertExactSchema(frame)
    frame
  }

  override def replaceSilver(
      identifier: String,
      frame: DataFrame,
      properties: Map[String, String]
  ): Unit = {
    require(SilverTables.contains(identifier), "quality Silver table is invalid")
    QualityTransforms.assertExactSchema(frame)
    backend.replace(identifier, frame)
    backend.setProperties(identifier, properties)
    val observed = readProperties(identifier)
    require(observed == properties, "quality Silver properties mismatch")
  }

  private def readProperties(identifier: String): Map[String, String] = {
    require(SilverTables.contains(identifier), "quality Silver table is invalid")
    val rows = backend.query(s"SHOW TBLPROPERTIES $identifier").collect().toSeq
    val pairs = rows.map(row => row.getAs[String]("key") -> row.getAs[String]("value"))
    require(pairs.map(_._1).distinct.size == pairs.size, "quality Silver properties are duplicated")
    pairs.toMap.filter { case (key, _) => key.startsWith("data_eng_lab.quality.") }
  }

  override def readSilver(identifier: String): DataFrame = {
    require(SilverTables.contains(identifier), "quality Silver table is invalid")
    val frame = backend.table(identifier)
    QualityTransforms.assertExactSchema(frame)
    frame
  }

  override def mergeFacts(values: Seq[QualityFact]): Unit = {
    require(values.nonEmpty && values.size <= QualityContract.rules.size,
      "quality facts batch is invalid")
    val runIds = values.map(_.qualityRunId).distinct
    require(runIds.size == 1 && runIds.head.matches("[0-9a-f]{64}"),
      "quality facts must bind to one run")
    val keys = values.map(value => value.qualityRunId -> value.ruleId)
    require(keys.distinct.size == keys.size, "quality facts keys must be unique")
    require(values.forall(value => QualityContract.ExpectedRuleIds.contains(value.ruleId)),
      "quality fact rule is invalid")
    backend.mergeFacts(values)
  }

  override def readFacts(runId: String): DataFrame = {
    require(runId.matches("[0-9a-f]{64}"), "quality run ID is invalid")
    backend.query(s"SELECT * FROM ${QualityContract.FactsTable} WHERE quality_run_id = '$runId'")
  }
}

object SparkQualityStorageBackend {
  private val FactColumns = QualityContract.factsSchema.fieldNames.toSeq
  private val Assignments = FactColumns.filterNot(Set("quality_run_id", "rule_id"))
    .map(name => s"$name = source.$name").mkString(", ")

  val MergeFactsSql: String =
    s"""MERGE INTO ${QualityContract.FactsTable} target
       |USING quality_facts_source source
       |ON target.quality_run_id = source.quality_run_id
       |AND target.rule_id = source.rule_id
       |WHEN MATCHED THEN UPDATE SET $Assignments
       |WHEN NOT MATCHED THEN INSERT (${FactColumns.mkString(", ")})
       |VALUES (${FactColumns.map(name => s"source.$name").mkString(", ")})""".stripMargin
}

final class SparkQualityStorageBackend(spark: SparkSession) extends QualityStorageBackend {
  private val FactColumns = QualityContract.factsSchema.fieldNames.toSeq

  override def tableExists(identifier: String): Boolean = spark.catalog.tableExists(identifier)
  override def table(identifier: String): DataFrame = spark.table(identifier)
  override def query(statement: String): DataFrame = spark.sql(statement)
  override def createNamespace(identifier: String): Unit = spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $identifier")

  override def createFactsTable(): Unit = {
    val definitions = QualityContract.factsSchema.fields.map { field =>
      val required = if (field.nullable) "" else " NOT NULL"
      s"`${field.name}` ${field.dataType.sql}$required"
    }.mkString(", ")
    spark.sql(s"CREATE TABLE IF NOT EXISTS ${QualityContract.FactsTable} ($definitions) USING iceberg")
  }

  override def replace(identifier: String, frame: DataFrame): Unit =
    frame.writeTo(identifier).using("iceberg").createOrReplace()

  override def setProperties(identifier: String, values: Map[String, String]): Unit = {
    val properties = values.toSeq.sortBy(_._1).map { case (key, value) =>
      require(key.matches("[a-z0-9_.]+") && value.matches("[ -~]{1,256}"),
        "quality property is invalid")
      s"'$key'='${value.replace("'", "''")}'"
    }.mkString(", ")
    spark.sql(s"ALTER TABLE $identifier SET TBLPROPERTIES ($properties)")
  }

  override def mergeFacts(values: Seq[QualityFact]): Unit = {
    import spark.implicits._
    val frame = values.toDF().select(FactColumns.head, FactColumns.tail: _*)
    frame.createOrReplaceTempView("quality_facts_source")
    var primary: Throwable = null
    try spark.sql(SparkQualityStorageBackend.MergeFactsSql)
    catch {
      case error: Throwable => primary = error; throw error
    } finally {
      try spark.catalog.dropTempView("quality_facts_source")
      catch {
        case cleanup: Throwable if primary != null => primary.addSuppressed(cleanup)
        case cleanup: Throwable => throw cleanup
      }
    }
  }
}
