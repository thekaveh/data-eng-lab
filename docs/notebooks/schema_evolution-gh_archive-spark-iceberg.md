# Notebooks — schema_evolution-gh_archive-spark-iceberg
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
// spark is pre-bound by the Atlas Zeppelin interpreter
```

**PySpark (Jupyter):**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

### 2.2 Read

**Scala (Zeppelin):**

```scala
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.gh_events_se (id string, type string, actor_login string) USING iceberg")
spark.sql("INSERT INTO lakehouse.silver.gh_events_se VALUES ('1','PushEvent','octocat')")
```

**PySpark (Jupyter):**

```python
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.gh_events_se (id string, type string, actor_login string) USING iceberg")
spark.sql("INSERT INTO lakehouse.silver.gh_events_se VALUES ('1','PushEvent','octocat')")
```

### 2.3 Transform

**Scala (Zeppelin):**

```scala
spark.sql("ALTER TABLE lakehouse.silver.gh_events_se ADD COLUMN repo_name string")
spark.sql("ALTER TABLE lakehouse.silver.gh_events_se RENAME COLUMN type TO event_type")
```

**PySpark (Jupyter):**

```python
spark.sql("ALTER TABLE lakehouse.silver.gh_events_se ADD COLUMN repo_name string")
spark.sql("ALTER TABLE lakehouse.silver.gh_events_se RENAME COLUMN type TO event_type")
```

### 2.4 Write

**Scala (Zeppelin):**

```scala
spark.sql("INSERT INTO lakehouse.silver.gh_events_se VALUES ('2','WatchEvent','torvalds','linux')")
```

**PySpark (Jupyter):**

```python
spark.sql("INSERT INTO lakehouse.silver.gh_events_se VALUES ('2','WatchEvent','torvalds','linux')")
```

### 2.5 Verify

**Scala (Zeppelin):**

```scala
spark.sql("SELECT id, event_type, actor_login, repo_name FROM lakehouse.silver.gh_events_se ORDER BY id").show()
```

**PySpark (Jupyter):**

```python
spark.sql("SELECT id, event_type, actor_login, repo_name FROM lakehouse.silver.gh_events_se ORDER BY id").show()
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
