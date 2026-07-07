# Notebooks — star_schema-tpch-spark-iceberg
Auto-extracted from `jupyter/notebook.ipynb` and `zeppelin/notebook.zpln`.
Both notebooks implement identical logic in PySpark and Scala.

## 1. Section map

| Section | Scala (Zeppelin) | PySpark (Jupyter) |
|---|---|---|
| 2. Setup | ✓ | ✓ |
| 3. Read | ✓ | ✓ |
| 4. Transform | ✓ | ✓ |
| 5. Write | ✓ | ✓ |
| 6. Verify | ✓ | ✓ |

## 2. Walkthrough

### 2. Setup

**Scala (Zeppelin):**

```scala
import spark.implicits._
import org.apache.spark.sql.functions._
// spark pre-bound (Spark Connect + lakehouse catalog)
```

**PySpark (Jupyter):**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

### 3. Read

**Scala (Zeppelin):**

```scala
val orders = spark.read.parquet("s3a://landing/tpch/orders")
val customer = spark.read.parquet("s3a://landing/tpch/customer")
val lineitem = spark.read.parquet("s3a://landing/tpch/lineitem")
```

**PySpark (Jupyter):**

```python
orders = spark.read.parquet("s3a://landing/tpch/orders")
customer = spark.read.parquet("s3a://landing/tpch/customer")
lineitem = spark.read.parquet("s3a://landing/tpch/lineitem")
```

### 4. Transform

**Scala (Zeppelin):**

```scala
val dimCustomer = customer.select($"c_custkey", $"c_name", $"c_nationkey", $"c_mktsegment")
val fctOrders = orders.join(lineitem, orders("o_orderkey") === lineitem("l_orderkey"))
  .groupBy($"o_orderkey", $"o_custkey", $"o_orderdate")
  .agg(sum($"l_extendedprice").as("revenue"), count("*").as("line_count"))
```

**PySpark (Jupyter):**

```python
dimCustomer = customer.select(F.col("c_custkey"), F.col("c_name"), F.col("c_nationkey"), F.col("c_mktsegment"))
fctOrders = (orders.join(lineitem, orders["o_orderkey"] == lineitem["l_orderkey"])
    .groupBy(F.col("o_orderkey"), F.col("o_custkey"), F.col("o_orderdate"))
    .agg(F.sum("l_extendedprice").alias("revenue"), F.count("*").alias("line_count")))
```

### 5. Write

**Scala (Zeppelin):**

```scala
dimCustomer.writeTo("lakehouse.gold.dim_customer").using("iceberg").createOrReplace()
fctOrders.writeTo("lakehouse.gold.fct_orders").using("iceberg").createOrReplace()
```

**PySpark (Jupyter):**

```python
dimCustomer.writeTo("lakehouse.gold.dim_customer").using("iceberg").createOrReplace()
fctOrders.writeTo("lakehouse.gold.fct_orders").using("iceberg").createOrReplace()
```

### 6. Verify

**Scala (Zeppelin):**

```scala
spark.sql("SELECT c.c_mktsegment, sum(f.revenue) AS revenue FROM lakehouse.gold.fct_orders f JOIN lakehouse.gold.dim_customer c ON f.o_custkey = c.c_custkey GROUP BY c.c_mktsegment").show()
```

**PySpark (Jupyter):**

```python
spark.sql("SELECT c.c_mktsegment, sum(f.revenue) AS revenue FROM lakehouse.gold.fct_orders f JOIN lakehouse.gold.dim_customer c ON f.o_custkey = c.c_custkey GROUP BY c.c_mktsegment").show()
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
