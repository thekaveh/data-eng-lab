<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — sessionization-gh_archive-spark-iceberg
Auto-extracted from `jupyter/notebook.ipynb` and `zeppelin/notebook.zpln`.
Both notebooks implement identical logic in PySpark and Scala.

## 2. Section map

| Section | Scala (Zeppelin) | PySpark (Jupyter) |
|---|---|---|
| 1. Overview | ✓ | ✓ |
| 2. Setup | ✓ | ✓ |
| 3. Read | ✓ | ✓ |
| 4. Transform | ✓ | ✓ |
| 5. Write | ✓ | ✓ |
| 6. Verify | ✓ | ✓ |

## 3. Walkthrough

### 1. Overview

## 1. Overview

### 2. Setup

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

## 2. Setup

### 3. Read

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
raw = spark.read.json("s3a://landing/gh_archive")
events = raw.select(F.col("actor.login").alias("actor_login"), F.col("created_at").cast("timestamp").alias("ts"))
```

## 3. Read

### 4. Transform

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
w = Window.partitionBy("actor_login").orderBy("ts")
gaps = events.withColumn("prev_ts", F.lag("ts", 1).over(w)).withColumn("new_session", F.when(F.col("prev_ts").isNull() | ((F.unix_timestamp("ts") - F.unix_timestamp("prev_ts")) > 1800), 1).otherwise(0))
sessions = gaps.withColumn("session_id", F.sum("new_session").over(w))
```

## 4. Transform

### 5. Write

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
sessions.writeTo("lakehouse.silver.gh_sessions").using("iceberg").createOrReplace()
```

## 5. Write

### 6. Verify

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.sql("SELECT actor_login, count(distinct session_id) AS sessions FROM lakehouse.silver.gh_sessions GROUP BY actor_login ORDER BY sessions DESC").show(truncate=False)
```

## 6. Verify

## 4. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 5. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
