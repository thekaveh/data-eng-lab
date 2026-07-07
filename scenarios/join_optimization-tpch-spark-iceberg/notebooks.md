<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — join_optimization-tpch-spark-iceberg
Auto-extracted from `jupyter/notebook.ipynb` and `zeppelin/notebook.zpln`.
Both notebooks implement identical logic in PySpark and Scala.

## 1. Section map

| Subsection | Scala (Zeppelin) | PySpark (Jupyter) |
|---|---|---|
| 2.1 Setup | ✓ | ✓ |
| 2.2 Read | ✓ | ✓ |
| 2.3 Transform | ✓ | ✓ |
| 2.4 Write | ✓ | ✓ |
| 2.5 Verify | ✓ | ✓ |

## 2. Walkthrough

### 2.1 Setup

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

### 2.2 Read

**Scala (Zeppelin):**

```scala
val orders = spark.read.parquet("s3a://landing/tpch/orders")
val customer = spark.read.parquet("s3a://landing/tpch/customer")
```

**PySpark (Jupyter):**

```python
orders = spark.read.parquet("s3a://landing/tpch/orders")
customer = spark.read.parquet("s3a://landing/tpch/customer")
```

### 2.3 Transform

**Scala (Zeppelin):**

```scala
val joined = orders.join(broadcast(customer), $"o_custkey" === $"c_custkey")
joined.explain()
val mart = joined.groupBy($"c_mktsegment").agg(sum($"o_totalprice").as("revenue"), count("*").as("orders"))
```

**PySpark (Jupyter):**

```python
joined = orders.join(F.broadcast(customer), F.col("o_custkey") == F.col("c_custkey"))
joined.explain()
mart = joined.groupBy("c_mktsegment").agg(F.sum("o_totalprice").alias("revenue"), F.count("*").alias("orders"))
```

### 2.4 Write

**Scala (Zeppelin):**

```scala
mart.writeTo("lakehouse.gold.tpch_segment_revenue").using("iceberg").createOrReplace()
```

**PySpark (Jupyter):**

```python
mart.writeTo("lakehouse.gold.tpch_segment_revenue").using("iceberg").createOrReplace()
```

### 2.5 Verify

**Scala (Zeppelin):**

```scala
println("AQE: " + spark.conf.get("spark.sql.adaptive.enabled"))
spark.table("lakehouse.gold.tpch_segment_revenue").orderBy($"revenue".desc).show(false)
```

**PySpark (Jupyter):**

```python
print("AQE:", spark.conf.get("spark.sql.adaptive.enabled"))
spark.table("lakehouse.gold.tpch_segment_revenue").orderBy(F.desc("revenue")).show(truncate=False)
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
