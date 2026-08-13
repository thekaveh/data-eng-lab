# 6.20. sessionization-gh_archive-spark-iceberg
Documents the scenario's paired Jupyter (`notebook.ipynb`) and Zeppelin (`notebook.zpln`) implementations.
Both notebooks implement identical logic in PySpark and Scala.

> **Production-risk warning:** these educational notebooks consume a typed table produced from an
> immutable generation but directly replace
> `lakehouse.silver.gh_sessions`, do not write production provenance, and are not serialized with
> the flatten stage. Production writes must use `gh_archive_flatten_sessionization`.

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

Both setup implementations use the preconfigured Spark session and window functions. They do not
resolve or validate a production generation.

### 2.2 Read

**Scala (Zeppelin):**

```scala
val events = spark.table("lakehouse.silver.gh_events").select(
  $"id", $"type", $"actor_login", $"repo_name", $"created_at"
)
```

**PySpark (Jupyter):**

```python
events = spark.table("lakehouse.silver.gh_events").select(
    "id", "type", "actor_login", "repo_name", "created_at"
)
```

### 2.3 Transform

**Scala (Zeppelin):**

```scala
val w = Window.partitionBy($"actor_login").orderBy($"created_at", $"id")
val cumulative = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
val gaps = events
  .withColumn("previous_created_at", lag($"created_at", 1).over(w))
  .withColumn("new_session", when(
    $"previous_created_at".isNull ||
      (unix_timestamp($"created_at") - unix_timestamp($"previous_created_at")) > 1800,
    lit(1)
  ).otherwise(lit(0)).cast("int"))
val sessions = gaps
  .withColumn("session_id", sum($"new_session").over(cumulative).cast("long"))
  .select($"id", $"type", $"actor_login", $"repo_name", $"created_at",
    $"previous_created_at", $"new_session", $"session_id")
```

**PySpark (Jupyter):**

```python
w = Window.partitionBy("actor_login").orderBy("created_at", "id")
cumulative = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
gaps = events.withColumn("previous_created_at", F.lag("created_at", 1).over(w)).withColumn(
    "new_session",
    F.when(
        F.col("previous_created_at").isNull()
        | ((F.unix_timestamp("created_at") - F.unix_timestamp("previous_created_at")) > 1800),
        1,
    ).otherwise(0).cast("int"),
)
sessions = gaps.withColumn("session_id", F.sum("new_session").over(cumulative).cast("long")).select(
    "id", "type", "actor_login", "repo_name", "created_at",
    "previous_created_at", "new_session", "session_id",
)
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

Both notebooks read the same typed five-column `gh_events` table, use `(created_at, id)` ordering,
and produce the same eight-column `gh_sessions` contract; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or
`jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom only in an
isolated educational environment. The required typed `lakehouse.silver.gh_events` table must exist.
