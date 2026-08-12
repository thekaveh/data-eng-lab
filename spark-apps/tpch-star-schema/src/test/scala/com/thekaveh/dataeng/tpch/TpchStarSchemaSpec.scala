package com.thekaveh.dataeng.tpch

import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TpchStarSchemaSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private val plan = "1" * 64
  private val publication = "0123456789ab4def8123456789abcdef"
  private val manifest = "2" * 64
  private val names = Seq("customer.parquet", "lineitem.parquet", "nation.parquet", "orders.parquet",
    "part.parquet", "partsupp.parquet", "region.parquet", "supplier.parquet")
  private def uri(name: String, p: String = plan, pub: String = publication) =
    s"s3://landing/tpch/_generations/$p/$pub/$name"
  private def args(uris: Seq[String] = names.map(name => uri(name)), scale: String = "tiny",
                   p: String = plan, pub: String = publication, m: String = manifest) =
    (uris ++ Seq("--dataset-scale", scale, "--plan-id", p, "--publication-id", pub,
      "--manifest-sha256", m)).toArray

  override def beforeAll(): Unit = spark = SparkSession.builder().appName("tpch-test").master("local[2]")
    .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").getOrCreate()
  override def afterAll(): Unit = if (spark != null) spark.stop()

  test("requires one complete ordered immutable publication and exact provenance") {
    val parsed = TpchSources.parse(args())
    assert(parsed.canonicalUris == names.map(name => uri(name)))
    assert(parsed.sparkUri("orders.parquet") == uri("orders.parquet").replace("s3://", "s3a://"))
    assert(parsed.provenance == Provenance("tiny", plan, publication, manifest))
    Seq(args(names.dropRight(1).map(name => uri(name))), args(names.reverse.map(name => uri(name))),
      args(names.map(name => uri(name)) :+ uri(names.head)),
      args(names.map(n => uri(n)).updated(1, uri(names(1), pub = "0123456789ab4def9123456789abcdef"))),
      args(names.map(name => uri(name)), p = "3" * 64), args(names.map(name => uri(name)), scale = "large"),
      args(names.map(name => uri(name)), m = "A" * 64))
      .foreach(a => assertThrows[IllegalArgumentException](TpchSources.parse(a)))
  }

  test("builds notebook-equivalent exact dimension and fact schemas and measures") {
    val s = spark; import s.implicits._
    val customer = Seq((1L, "Alice", 10, "BUILDING"), (2L, "Bob", 20, "AUTOMOBILE"))
      .toDF("c_custkey", "c_name", "c_nationkey", "c_mktsegment")
    val orders = Seq((100L, 1L, Date.valueOf("2026-01-02")), (101L, 2L, Date.valueOf("2026-01-03")))
      .toDF("o_orderkey", "o_custkey", "o_orderdate")
    val lineitem = Seq((100L, 1L, BigDecimal("10.25")), (100L, 2L, BigDecimal("2.75")),
      (101L, 1L, BigDecimal("9.00"))).toDF("l_orderkey", "l_linenumber", "l_extendedprice")
      .withColumn("l_extendedprice", org.apache.spark.sql.functions.col("l_extendedprice").cast(DecimalType(15, 2)))
    StarSchemaTransforms.validateSources(customer, orders, lineitem)
    val dim = StarSchemaTransforms.dimension(customer)
    val fact = StarSchemaTransforms.fact(orders, lineitem)
    assert(dim.columns.toSeq == Seq("c_custkey", "c_name", "c_nationkey", "c_mktsegment"))
    assert(dim.schema.fields.map(_.dataType) sameElements Array(LongType, StringType, IntegerType, StringType))
    assert(fact.columns.toSeq == Seq("o_orderkey", "o_custkey", "o_orderdate", "revenue", "line_count"))
    assert(fact.schema("revenue").dataType == DecimalType(25, 2))
    assert(fact.schema("line_count").dataType == LongType)
    assert(fact.orderBy("o_orderkey").select("revenue", "line_count").collect().map(r => (r.getDecimal(0).toString, r.getLong(1))).toSeq ==
      Seq(("13.00", 2L), ("9.00", 1L)))
  }

  test("rejects duplicate, null, and dangling TPC-H keys") {
    val s = spark; import s.implicits._
    val customer = Seq((1L, "Alice", 10, "BUILDING")).toDF("c_custkey", "c_name", "c_nationkey", "c_mktsegment")
    val orders = Seq((100L, 9L, Date.valueOf("2026-01-02"))).toDF("o_orderkey", "o_custkey", "o_orderdate")
    val lines = Seq((100L, 1L, BigDecimal("1.00"))).toDF("l_orderkey", "l_linenumber", "l_extendedprice")
      .withColumn("l_extendedprice", org.apache.spark.sql.functions.col("l_extendedprice").cast(DecimalType(15, 2)))
    assertThrows[IllegalArgumentException](StarSchemaTransforms.validateSources(customer, orders, lines))
    val duplicateCustomers = customer.union(customer)
    val validOrders = Seq((100L, 1L, Date.valueOf("2026-01-02"))).toDF("o_orderkey", "o_custkey", "o_orderdate")
    assertThrows[IllegalArgumentException](StarSchemaTransforms.validateSources(duplicateCustomers, validOrders, lines))
    val danglingLines = Seq((999L, 1L, BigDecimal("1.00"))).toDF("l_orderkey", "l_linenumber", "l_extendedprice")
      .withColumn("l_extendedprice", org.apache.spark.sql.functions.col("l_extendedprice").cast(DecimalType(15, 2)))
    assertThrows[IllegalArgumentException](StarSchemaTransforms.validateSources(customer, validOrders, danglingLines))
  }
}
