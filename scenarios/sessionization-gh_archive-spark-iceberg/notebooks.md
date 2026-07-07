<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — sessionization-gh_archive-spark-iceberg
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
import org.apache.spark.sql.expressions.Window
```

**PySpark (Jupyter):**

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

### 2.2 Read

**Scala (Zeppelin):**

```scala
val raw = spark.read.json("s3a://landing/gh_archive")
val events = raw.select($"actor.login".as("actor_login"), $"created_at".cast("timestamp").as("ts"))
```

**PySpark (Jupyter):**

```python
raw = spark.read.json("s3a://landing/gh_archive")
events = raw.select(F.col("actor.login").alias("actor_login"), F.col("created_at").cast("timestamp").alias("ts"))
```

### 2.3 Transform

**Scala (Zeppelin):**

```scala
val w = Window.partitionBy($"actor_login").orderBy($"ts")
val gaps = events.withColumn("prev_ts", lag($"ts", 1).over(w)).withColumn("new_session", when($"prev_ts".isNull || (unix_timestamp($"ts") - unix_timestamp($"prev_ts")) > 1800, 1).otherwise(0))
val sessions = gaps.withColumn("session_id", sum($"new_session").over(w))
```

**PySpark (Jupyter):**

```python
w = Window.partitionBy("actor_login").orderBy("ts")
gaps = events.withColumn("prev_ts", F.lag("ts", 1).over(w)).withColumn("new_session", F.when(F.col("prev_ts").isNull() | ((F.unix_timestamp("ts") - F.unix_timestamp("prev_ts")) > 1800), 1).otherwise(0))
sessions = gaps.withColumn("session_id", F.sum("new_session").over(w))
```

### 2.4 Write

**Scala (Zeppelin):**

```scala
sessions.writeTo("lakehouse.silver.gh_sessions").using("iceberg").createOrReplace()
```

**PySpark (Jupyter):**

```python
sessions.writeTo("lakehouse.silver.gh_sessions").using("iceberg").createOrReplace()
```

### 2.5 Verify

**Scala (Zeppelin):**

```scala
spark.sql("SELECT actor_login, count(distinct session_id) AS sessions FROM lakehouse.silver.gh_sessions GROUP BY actor_login ORDER BY sessions DESC").show(false)
```

**PySpark (Jupyter):**

```python
spark.sql("SELECT actor_login, count(distinct session_id) AS sessions FROM lakehouse.silver.gh_sessions GROUP BY actor_login ORDER BY sessions DESC").show(truncate=False)
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
