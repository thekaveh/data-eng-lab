# Notebooks — streaming_ingest-gh_archive-spark-iceberg
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
import org.apache.spark.sql.types._
// spark is pre-bound by the Atlas Zeppelin interpreter
```

**PySpark (Jupyter):**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

### 3. Read

**Scala (Zeppelin):**

```scala
val schema = new StructType().add("id", StringType).add("type", StringType).add("created_at", StringType)
val stream = spark.readStream.schema(schema).json("s3a://landing/gh_archive")
```

**PySpark (Jupyter):**

```python
schema = StructType().add("id", StringType()).add("type", StringType()).add("created_at", StringType())
stream = spark.readStream.schema(schema).json("s3a://landing/gh_archive")
```

### 4. Transform

**Scala (Zeppelin):**

```scala
val events = stream.withColumn("created_at", $"created_at".cast("timestamp"))
```

**PySpark (Jupyter):**

```python
events = stream.withColumn("created_at", F.col("created_at").cast("timestamp"))
```

### 5. Write

**Scala (Zeppelin):**

```scala
val query = events.writeStream.format("iceberg").outputMode("append").option("checkpointLocation", "s3a://checkpoints/gh_events_file").toTable("lakehouse.bronze.gh_events_stream")
// query.awaitTermination() to keep stream running
```

**PySpark (Jupyter):**

```python
query = events.writeStream.format("iceberg").outputMode("append").option("checkpointLocation", "s3a://checkpoints/gh_events_file").toTable("lakehouse.bronze.gh_events_stream")
# query.awaitTermination() to keep stream running
```

### 6. Verify

**Scala (Zeppelin):**

```scala
spark.table("lakehouse.bronze.gh_events_stream").count()
```

**PySpark (Jupyter):**

```python
spark.table("lakehouse.bronze.gh_events_stream").count()
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
