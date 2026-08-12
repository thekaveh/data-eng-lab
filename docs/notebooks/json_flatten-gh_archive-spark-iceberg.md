# 6.19. json_flatten-gh_archive-spark-iceberg
Documents the scenario's paired Jupyter (`notebook.ipynb`) and Zeppelin (`notebook.zpln`) implementations.
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

Both setup implementations choose an explicit notebook scale override before `DATASET_SCALE`, default to `small`, and call the internal resolver once. They validate and retain one verified immutable generation as `datasetSparkUris` / `dataset_spark_uris`.

### 2.2 Read

**Scala (Zeppelin):**

```scala
val raw = spark.read.json(datasetSparkUris: _*)
raw.printSchema()
```

**PySpark (Jupyter):**

```python
raw = spark.read.json(list(dataset_spark_uris))
raw.printSchema()
```

### 2.3 Transform

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

### 2.4 Write

**Scala (Zeppelin):**

```scala
flat.writeTo("lakehouse.silver.gh_events").using("iceberg").createOrReplace()
```

**PySpark (Jupyter):**

```python
flat.writeTo("lakehouse.silver.gh_events").using("iceberg").createOrReplace()
```

### 2.5 Verify

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
