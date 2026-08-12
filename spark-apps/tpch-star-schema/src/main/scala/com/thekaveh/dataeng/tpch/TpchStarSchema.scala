package com.thekaveh.dataeng.tpch

import org.apache.spark.sql.{DataFrame, SparkSession}

final case class RunResult(dimensionRows: Long, factRows: Long, provenance: Provenance)

trait TableWriter {
  def createNamespace(): Unit
  def replace(table: String, frame: DataFrame, provenance: Provenance): Unit
  def readProperties(table: String): Map[String, String]
}

final class IcebergTableWriter(spark: SparkSession) extends TableWriter {
  def createNamespace(): Unit = spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

  def replace(table: String, frame: DataFrame, provenance: Provenance): Unit = {
    var builder = frame.writeTo(table).using("iceberg")
    provenance.properties.toSeq.sortBy(_._1).foreach { case (key, value) =>
      builder = builder.tableProperty(key, value)
    }
    builder.createOrReplace()
  }

  def readProperties(table: String): Map[String, String] =
    spark.sql(s"SHOW TBLPROPERTIES $table").collect().map(row => row.getString(0) -> row.getString(1)).toMap
}

object TpchStarSchema {
  val DimensionTable = "lakehouse.gold.dim_customer"
  val FactTable = "lakehouse.gold.fct_orders"

  def runResolved(sources: ResolvedSources, customer: DataFrame, orders: DataFrame, lineitem: DataFrame,
                  writer: TableWriter): RunResult = {
    StarSchemaTransforms.validateSources(customer, orders, lineitem)
    val dimension = StarSchemaTransforms.dimension(customer).cache()
    val fact = StarSchemaTransforms.fact(orders, lineitem).cache()
    try {
      val dimensionRows = dimension.count()
      val factRows = fact.count()
      require(dimensionRows > 0 && factRows > 0, "TPC-H star-schema outputs must be nonempty")
      writer.createNamespace()
      writer.replace(DimensionTable, dimension, sources.provenance)
      writer.replace(FactTable, fact, sources.provenance)
      Seq(DimensionTable, FactTable).foreach { table =>
        val actual = writer.readProperties(table).filter { case (key, _) => sources.provenance.properties.contains(key) }
        if (actual != sources.provenance.properties)
          throw new IllegalStateException(s"$table provenance does not match the intended TPC-H generation")
      }
      RunResult(dimensionRows, factRows, sources.provenance)
    } finally {
      dimension.unpersist()
      fact.unpersist()
    }
  }

  def main(args: Array[String]): Unit = {
    val sources = TpchSources.parse(args)
    val spark = SparkSession.builder().appName("tpch-star-schema").getOrCreate()
    try {
      val result = runResolved(
        sources,
        spark.read.parquet(sources.sparkUri("customer.parquet")),
        spark.read.parquet(sources.sparkUri("orders.parquet")),
        spark.read.parquet(sources.sparkUri("lineitem.parquet")),
        new IcebergTableWriter(spark)
      )
      // scalastyle:off println
      println(s"wrote $DimensionTable=${result.dimensionRows}, $FactTable=${result.factRows}, " +
        s"publication=${result.provenance.publicationId}")
      // scalastyle:on println
    } finally spark.stop()
  }
}
