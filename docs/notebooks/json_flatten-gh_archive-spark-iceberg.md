# Notebooks — json_flatten-gh_archive-spark-iceberg
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
val raw = spark.read.json("s3a://landing/gh_archive")
raw.printSchema()
```

**PySpark (Jupyter):**

```python
raw = spark.read.json("s3a://landing/gh_archive")
raw.printSchema()
```

### 4. Transform

**Scala (Zeppelin):**

```scala
val flat = raw.select($"id", $"type", $"actor.login".as("actor_login"), $"repo.name".as("repo_name"), $"created_at".cast("timestamp").as("created_at"))
```

**PySpark (Jupyter):**

```python
flat = raw.select(
    F.col("id"),
    F.col("type"),
    F.col("actor.login").alias("actor_login"),
    F.col("repo.name").alias("repo_name"),
    F.col("created_at").cast("timestamp").alias("created_at")
)
```

### 5. Write

**Scala (Zeppelin):**

```scala
flat.writeTo("lakehouse.silver.gh_events").using("iceberg").createOrReplace()
```

**PySpark (Jupyter):**

```python
flat.writeTo("lakehouse.silver.gh_events").using("iceberg").createOrReplace()
```

### 6. Verify

**Scala (Zeppelin):**

```scala
spark.sql("SELECT type, count(*) AS n FROM lakehouse.silver.gh_events GROUP BY type ORDER BY n DESC").show()
```

**PySpark (Jupyter):**

```python
spark.sql("SELECT type, count(*) AS n FROM lakehouse.silver.gh_events GROUP BY type ORDER BY n DESC").show()
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
