package com.thekaveh.dataeng.gharchive

import org.apache.spark.sql.{DataFrame, SparkSession}

trait TableWriter {
  def createNamespace(): Unit
  def replace(table: String, frame: DataFrame, provenance: Provenance): Unit
  def readFrame(table: String): DataFrame
  def readProperties(table: String): Map[String, String]
}

final class IcebergTableWriter(spark: SparkSession) extends TableWriter {
  def createNamespace(): Unit = spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

  def replace(table: String, frame: DataFrame, provenance: Provenance): Unit = {
    var builder = frame.writeTo(table).using("iceberg")
    provenance.properties.toSeq.sortBy(_._1).foreach { case (key, value) =>
      builder = builder.tableProperty(key, value)
    }
    builder.createOrReplace()
  }

  def readFrame(table: String): DataFrame = spark.table(table)

  def readProperties(table: String): Map[String, String] =
    spark.sql(s"SHOW TBLPROPERTIES $table").collect().map(row => row.getString(0) -> row.getString(1)).toMap
}

object IcebergTables {
  private val IdentityPrefix = "data_eng_lab.dataset"

  def requireProvenance(table: String, actual: Map[String, String], expected: Provenance): Unit = {
    val identity = actual.filter { case (key, _) => key.startsWith(IdentityPrefix) }
    if (identity != expected.properties)
      throw new IllegalStateException(s"$table provenance does not match the intended GitHub Archive generation")
  }

  def sameRows(expected: DataFrame, actual: DataFrame): Boolean =
    expected.exceptAll(actual).limit(1).count() == 0 && actual.exceptAll(expected).limit(1).count() == 0
}
