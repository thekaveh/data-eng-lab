package com.thekaveh.dataeng.tpch

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.types._

object StarSchemaTransforms {
  private val CustomerColumns = Seq("c_custkey" -> LongType, "c_name" -> StringType,
    "c_nationkey" -> IntegerType, "c_mktsegment" -> StringType)
  private val OrderColumns = Seq("o_orderkey" -> LongType, "o_custkey" -> LongType, "o_orderdate" -> DateType)
  private val LineColumns = Seq("l_orderkey" -> LongType, "l_linenumber" -> LongType,
    "l_extendedprice" -> DecimalType(15, 2))

  private def requireColumns(frame: DataFrame, expected: Seq[(String, DataType)], source: String): Unit =
    expected.foreach { case (name, dataType) =>
      require(frame.schema.fieldNames.contains(name), s"$source is missing $name")
      require(frame.schema(name).dataType == dataType, s"$source has invalid type for $name")
    }

  private def requireKeys(frame: DataFrame, keys: Seq[String], source: String): Unit = {
    require(frame.where(keys.map(k => F.col(k).isNull).reduce(_ || _)).limit(1).count() == 0,
      s"$source has null keys")
    require(frame.groupBy(keys.map(F.col): _*).count().where(F.col("count") > 1).limit(1).count() == 0,
      s"$source has duplicate keys")
  }

  def validateSources(customer: DataFrame, orders: DataFrame, lineitem: DataFrame): Unit = {
    requireColumns(customer, CustomerColumns, "customer")
    requireColumns(orders, OrderColumns, "orders")
    requireColumns(lineitem, LineColumns, "lineitem")
    requireKeys(customer, Seq("c_custkey"), "customer")
    requireKeys(orders, Seq("o_orderkey"), "orders")
    requireKeys(lineitem, Seq("l_orderkey", "l_linenumber"), "lineitem")
    require(orders.join(customer.select("c_custkey"), orders("o_custkey") === customer("c_custkey"), "left_anti")
      .limit(1).count() == 0, "orders reference missing customers")
    require(lineitem.join(orders.select("o_orderkey"), lineitem("l_orderkey") === orders("o_orderkey"), "left_anti")
      .limit(1).count() == 0, "lineitems reference missing orders")
  }

  def dimension(customer: DataFrame): DataFrame = customer.select(CustomerColumns.map { case (n, _) => F.col(n) }: _*)

  def fact(orders: DataFrame, lineitem: DataFrame): DataFrame =
    orders.join(lineitem, orders("o_orderkey") === lineitem("l_orderkey"))
      .groupBy(orders("o_orderkey"), orders("o_custkey"), orders("o_orderdate"))
      .agg(F.sum(lineitem("l_extendedprice")).as("revenue"), F.count(F.lit(1)).as("line_count"))
}
